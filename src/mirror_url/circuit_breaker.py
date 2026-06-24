"""Circuit-breaker state machines + per-domain manager.

Migrated verbatim from ``mirror_url.py``:
``CircuitBreaker`` (orig. 3098-3212), ``AsyncCircuitBreaker`` (orig. 3214-3336),
``ChunkCircuitBreaker`` (orig. 4161-4209), ``CircuitBreakerManager`` (orig. 8355-8412).
"""

from __future__ import annotations

import asyncio
import logging
import time
from threading import RLock
from typing import Any, Callable, Dict

from .enums import CircuitBreakerState
from .exceptions import MirrorConnectionError


class CircuitBreaker:
    """Thread-safe circuit breaker with proper HALF_OPEN semantics."""

    def __init__(
        self, failure_threshold: int = 5, recovery_timeout: float = 60.0, half_open_limit: int = 3
    ):
        # Input validation
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if recovery_timeout <= 0:
            raise ValueError(f"recovery_timeout must be > 0, got {recovery_timeout}")
        if half_open_limit < 1:
            raise ValueError(f"half_open_limit must be >= 1, got {half_open_limit}")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_limit = half_open_limit
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_successes = 0
        self.half_open_start = 0.0
        self.half_open_permits = 0
        self.lock = RLock()
        self.total_failures = 0
        self.total_successes = 0

    def can_execute(self) -> bool:
        """Thread-safe check if operation can proceed."""
        with self.lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True

            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    self.half_open_start = time.time()
                    logging.info("Circuit breaker half-open - testing recovery")
                    return True
                return False

            if self.state == CircuitBreakerState.HALF_OPEN:
                # Check timeout first
                if time.time() - self.half_open_start >= self.recovery_timeout:
                    self.state = CircuitBreakerState.OPEN
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    logging.warning("Circuit breaker half-open timeout, reopening")
                    return False

                # Reserve a permit atomically
                if self.half_open_permits < self.half_open_limit:
                    self.half_open_permits += 1
                    return True
                return False

            return False

    def record_success(self) -> None:
        """Record a successful operation."""
        with self.lock:
            self.total_successes += 1

            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_limit:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    logging.info("Circuit breaker closed - service recovered")

            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed operation."""
        with self.lock:
            self.total_failures += 1
            self.last_failure_time = time.time()

            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.half_open_successes = 0
                self.half_open_permits = 0
                logging.warning("Circuit breaker reopened - test request failed")

            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logging.warning(f"Circuit breaker opened - {self.failure_count} failures")

    def is_closed(self) -> bool:
        """Pure query: returns True if circuit is closed (no side effects)."""
        with self.lock:
            return self.state == CircuitBreakerState.CLOSED

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self.lock:
            return {
                "state": self.state.value,
                "failure_count": self.failure_count,
                "failure_threshold": self.failure_threshold,
                "total_failures": self.total_failures,
                "total_successes": self.total_successes,
                "last_failure_time": self.last_failure_time,
                "half_open_successes": self.half_open_successes,
                "half_open_permits": self.half_open_permits,
                "half_open_limit": self.half_open_limit,
                "recovery_timeout": self.recovery_timeout,
            }


class AsyncCircuitBreaker:
    """Circuit breaker specifically for async operations."""

    def __init__(self, failure_threshold: int = 10, recovery_timeout: float = 30.0):
        """
        Initialize async circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
        self.lock = asyncio.Lock()
        self.total_failures = 0
        self.total_successes = 0
        self.half_open_successes = 0  # NEW: Track successes in half-open state
        self.half_open_limit = 3  # NEW: Number of successes needed to close

    async def can_execute(self) -> bool:
        """
        Check if operation can be executed.

        FIXED: No side effects - state transitions happen in record_failure/record_success.
        """
        async with self.lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    # FIX: Actually transition to half-open here
                    self.state = "half-open"
                    self.half_open_successes = 0
                    logging.debug("Async circuit breaker half-open - testing recovery")
                    return True
                return False
            return True

    async def _try_transition_to_half_open(self) -> bool:
        """
        Atomically attempt to transition from OPEN to HALF-OPEN.

        Returns:
            True if transition was successful and execution is allowed
        """
        async with self.lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "half-open"
                    self.half_open_successes = 0
                    logging.debug("Async circuit breaker half-open - testing recovery")
                    return True
            return self.state != "open"

    async def execute_or_fallback(self, operation: Callable, fallback: Callable = None):
        """
        Execute an operation with circuit breaker protection.
        FIX: Safely handles both sync and async fallbacks.
        """
        can_proceed = await self._try_transition_to_half_open()
        if not can_proceed:
            if fallback:
                fb_result = fallback()
                # ✅ FIX: Only await if fallback returns a coroutine
                return await fb_result if asyncio.iscoroutine(fb_result) else fb_result
            raise MirrorConnectionError("Circuit breaker is open")

        try:
            result = await operation()
            await self.record_success()
            return result
        except Exception:
            await self.record_failure()
            if fallback:
                fb_result = fallback()
                return await fb_result if asyncio.iscoroutine(fb_result) else fb_result
            raise

    async def record_success(self) -> None:
        """Record a successful operation."""
        async with self.lock:
            self.total_successes += 1

            if self.state == "half-open":
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_limit:
                    self.state = "closed"
                    self.failures = 0
                    self.half_open_successes = 0
                    logging.info("Async circuit breaker closed - service recovered")
            elif self.state == "closed":
                # Gradually reduce failure count on successes
                self.failures = max(0, self.failures - 1)

    async def record_failure(self) -> None:
        """Record a failed operation."""
        async with self.lock:
            self.total_failures += 1
            self.failures += 1
            self.last_failure_time = time.time()

            if self.state == "half-open":
                # Half-open failure - reopen circuit
                self.state = "open"
                self.half_open_successes = 0
                logging.warning("Async circuit breaker reopened - test request failed")
            elif self.state == "closed" and self.failures >= self.failure_threshold:
                # Closed state reached threshold - open circuit
                self.state = "open"
                logging.warning(f"Async circuit breaker opened after {self.failures} failures")

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "state": self.state,
            "failures": self.failures,
            "failure_threshold": self.failure_threshold,
            "total_failures": self.total_failures,
            "total_successes": self.total_successes,
            "last_failure_time": self.last_failure_time,
            "half_open_successes": self.half_open_successes,
        }


# ============================================================================
# CHUNK-AWARE CIRCUIT BREAKER
# ============================================================================
class ChunkCircuitBreaker(CircuitBreaker):
    """Circuit breaker that aggregates chunk failures per file/server"""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_limit: int = 3,
        chunk_failure_threshold: int = 3,
    ):
        super().__init__(failure_threshold, recovery_timeout, half_open_limit)
        self.chunk_failure_threshold = chunk_failure_threshold
        self.file_chunk_failures: Dict[str, int] = {}
        self.server_chunk_failures: Dict[str, int] = {}
        self.chunk_lock = RLock()

    def record_chunk_failure(self, file_url: str, server: str) -> None:
        """
        Record a chunk failure.

        If too many chunks fail for a file, mark entire file as failed.
        If too many chunks fail for a server, open circuit for that server.
        """
        with self.chunk_lock:
            # Track per-file chunk failures
            self.file_chunk_failures[file_url] = self.file_chunk_failures.get(file_url, 0) + 1

            # Track per-server chunk failures
            self.server_chunk_failures[server] = self.server_chunk_failures.get(server, 0) + 1

            # Check file threshold
            if self.file_chunk_failures[file_url] >= self.chunk_failure_threshold:
                logging.warning(f"Too many chunk failures for {file_url}, marking file as failed")
                self.record_failure()  # This will affect overall circuit

            # Check server threshold
            if self.server_chunk_failures[server] >= self.chunk_failure_threshold * 2:
                logging.warning(f"Too many chunk failures for server {server}, opening circuit")
                self.record_failure()  # This will affect overall circuit

    def record_chunk_success(self, file_url: str, server: str) -> None:
        """Record a successful chunk download"""
        with self.chunk_lock:
            # Reset file failure count on success
            self.file_chunk_failures.pop(file_url, None)

            # Gradually reduce server failure count
            current = self.server_chunk_failures.get(server, 0)
            if current > 0:
                self.server_chunk_failures[server] = max(0, current - 1)

        self.record_success()


# ============================================================================
# PER-DOMAIN CIRCUIT BREAKER MANAGER
# ============================================================================
class CircuitBreakerManager:
    """Manages per-domain circuit breakers to prevent cascade failures."""

    def __init__(
        self, failure_threshold: int = 5, recovery_timeout: float = 30.0, half_open_limit: int = 2
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_limit = half_open_limit
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.lock = RLock()

    def get_breaker(self, domain: str) -> CircuitBreaker:
        with self.lock:
            if domain not in self.breakers:
                self.breakers[domain] = CircuitBreaker(
                    failure_threshold=self.failure_threshold,
                    recovery_timeout=self.recovery_timeout,
                    half_open_limit=self.half_open_limit,
                )
            return self.breakers[domain]

    def record_success(self, domain: str) -> None:
        # FIX (production-readiness pass): previously this only updated a
        # breaker that already existed in self.breakers, but nothing on the
        # real request path ever called get_breaker() to create one — only
        # an explicit, manual get_breaker() call did. Since real callers
        # only ever call record_success()/record_failure()/can_execute(),
        # the per-domain breaker for a real domain was NEVER created, which
        # made can_execute() return True unconditionally forever (see its
        # old "if domain not in self.breakers: return True" short-circuit)
        # and made record_failure() a permanent no-op. The circuit breaker
        # never actually tripped for a single domain in real usage. Lazily
        # creating the breaker here (get_breaker() is idempotent) fixes
        # that: the first call for a domain registers it, and all three
        # methods now operate on the same persistent per-domain breaker.
        with self.lock:
            self.get_breaker(domain).record_success()

    def record_failure(self, domain: str) -> None:
        with self.lock:
            self.get_breaker(domain).record_failure()

    def can_execute(self, domain: str) -> bool:
        with self.lock:
            return self.get_breaker(domain).can_execute()

    def reset_domain(self, domain: str) -> None:
        with self.lock:
            if domain in self.breakers:
                del self.breakers[domain]

    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {domain: breaker.get_stats() for domain, breaker in self.breakers.items()}

    def reset_all(self) -> None:
        with self.lock:
            self.breakers.clear()


__all__ = [
    "CircuitBreaker",
    "AsyncCircuitBreaker",
    "ChunkCircuitBreaker",
    "CircuitBreakerManager",
]
