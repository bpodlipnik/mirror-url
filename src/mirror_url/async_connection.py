"""Async connection managers and task runner.

Migrated verbatim from ``mirror_url.py``:
``AsyncConnectionManager`` (orig. 6690-7103), ``AdaptiveAsyncManager`` (orig.
7108-7743), ``AsyncTaskManager`` (orig. 7749-7937).
"""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set
from urllib.parse import urlparse

import httpx

from .circuit_breaker import CircuitBreakerManager
from .constants import (
    ADAPTIVE_COOLDOWN_SECONDS,
    ADAPTIVE_ERROR_THRESHOLD,
    ADAPTIVE_MAX_CONCURRENCY,
    ADAPTIVE_RTT_THRESHOLD_MS,
    ADAPTIVE_START_CONCURRENCY,
    ASYNC_SEMAPHORE_LIMIT,
    CONTENT_HASH_LIMIT,
    KNOWN_THROTTLED_DOMAINS,
    PROFILE_SAMPLE_SIZE,
)
from .models import ServerProfile
from .rate_limiter import PerIPRateLimiter
from .transport import SecureAsyncTransport
from .utils import exponential_backoff

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig
    from .metrics import MetricsCollector


class AsyncConnectionManager:
    """Long-lived async client with exponential backoff"""

    def __init__(self, config: MirrorConfig, metrics: MetricsCollector):
        """
        Initialize async connection manager.

        Args:
            config: MirrorConfig instance
            metrics: MetricsCollector instance
        """
        self.config = config
        self.metrics = metrics
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()
        self._closed = False
        self.rate_limiter = PerIPRateLimiter() if config.security_validation else None

    async def __aenter__(self):
        """Context manager entry"""
        if self._closed:
            raise RuntimeError("Cannot reuse closed AsyncConnectionManager")
        self._build_client()
        self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
        return self

    def _build_client(self) -> None:
        """Create the underlying httpx.AsyncClient (idempotent-ish helper)."""
        limits = httpx.Limits(
            max_connections=self.config.async_workers,
            max_keepalive_connections=self.config.async_workers // 3,
            keepalive_expiry=60.0,
        )
        timeout = httpx.Timeout(self.config.timeout, connect=6.0, read=self.config.timeout * 1.5)

        self._client = httpx.AsyncClient(
            http2=self.config.http2,
            limits=limits,
            timeout=timeout,
            follow_redirects=True,
            transport=SecureAsyncTransport(rate_limiter=self.rate_limiter),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    async def _ensure_client(self) -> bool:
        """Ensure the async client exists before use.

        FIX: head() called self._ensure_client(), but the method did not exist
        on this class (only on AdaptiveAsyncManager) — so any HEAD that actually
        reached this point raised AttributeError, breaking the non-adaptive
        async metadata path. This lazily builds the client if the manager was
        used without `async with` (and rebuilds it if it was closed), mirroring
        the lifecycle __aenter__ provides.
        """
        if self._closed:
            return False
        if self._client is None or getattr(self._client, "is_closed", False):
            try:
                self._build_client()
            except Exception as e:
                logging.error(f"Failed to ensure async client: {e}")
                return False
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
        return True

    def record_result(self, url: str, success: bool, rtt_ms: float, duration: float) -> None:
        """No-op result recorder.

        FIX: head() calls self.record_result(...) on every request, but the
        real implementation lives on AdaptiveAsyncManager — it drives adaptive
        concurrency scaling using state (_get_profile, _current_concurrency,
        _fallback_to_sync, self.lock, ...) that THIS non-adaptive manager does
        not have. Calling it here previously raised AttributeError, breaking
        every async HEAD. AsyncConnectionManager has no concurrency profile to
        adjust, so recording a result is genuinely a no-op for it (mirrors the
        `if hasattr(self, 'record_result')` guards elsewhere, which already
        treat its absence as "do nothing").
        """
        return None

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._closed = True
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as e:
                logging.debug(f"Error closing async client: {e}")
        self._client = None
        self._semaphore = None

    def is_available(self) -> bool:
        """Check if async connection manager is available for use."""
        return (
            self._client is not None
            and not self._closed
            and not getattr(self._client, "is_closed", False)
        )

    async def warm_up(self, urls: List[str]) -> None:
        """Pre-warm async connections with proper error handling."""
        if not urls or self._closed:
            return

        if not self.is_available():
            logging.debug("Async connection manager not available for warm-up")
            return

        logging.info("🔥 Pre-warming async connections")

        # Limit to first 10 URLs to avoid overwhelming
        warm_urls = urls[:10]
        tasks = []

        for url in warm_urls:
            tasks.append(self._warm_single_connection(url))

        # Use asyncio.gather with return_exceptions to handle failures
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = 0
        for r in results:
            if r is True:
                success_count += 1
            elif isinstance(r, Exception):
                logging.debug(f"Warm-up connection error: {r}")

        logging.info(f"✅ Warmed up {success_count}/{len(tasks)} async connections")

    async def _warm_single_connection(self, url: str) -> bool:
        """Warm up a single connection"""
        try:
            if self.rate_limiter:
                parsed = urlparse(url)
                try:
                    ip = socket.gethostbyname(parsed.hostname)
                    self.rate_limiter.wait(ip)
                except Exception:
                    pass

            # FIX: Previously this used `asyncio.Semaphore(self._current_concurrency)`,
            # but (a) AsyncConnectionManager has no `_current_concurrency`
            # attribute (it lives on AdaptiveAsyncManager), so this always
            # raised AttributeError under load, and (b) creating a fresh
            # Semaphore per-call never actually limits anything. Use the
            # shared instance semaphore set up in __aenter__.
            sem = self._semaphore or asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
            async with sem:
                # Python 3.10 fix: replaced asyncio.timeout() with asyncio.wait_for()
                resp = await asyncio.wait_for(
                    self._client.head(
                        url, timeout=httpx.Timeout(3.0, connect=2.0), follow_redirects=False
                    ),
                    timeout=5.0,
                )
                await resp.aclose()
                return True
        except asyncio.TimeoutError:
            logging.debug(f"Warm-up timeout for {url}")
            return False
        except Exception as e:
            logging.debug(f"Warm-up failed for {url}: {e}")
            return False

    async def head(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> Optional[httpx.Response]:
        """
        Async HEAD request with simplified timeout handling and clean retry logic.

        Optimized version with DNS caching, proper semaphore initialization,
        and non-blocking rate limiting.
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        # Circuit breaker check
        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment("circuit_breaker_trips")
            self.metrics.add_error(f"Circuit breaker open for domain {domain}", "circuit_breaker")
            return None

        # Ensure client is initialized
        if not await self._ensure_client() or self._client is None or self._client.is_closed:
            self.metrics.add_error(
                f"Async client not available for {url}", "async_client_unavailable"
            )
            return None

        # Configuration
        per_request_timeout = 12.0
        max_retries = self.config.max_retries
        retry_delay_base = self.config.retry_delay * 0.7
        start_time = time.time()

        # Semaphore is guaranteed by _ensure_client()/__aenter__ above.
        # FIX: the previous code here referenced self._semaphore_lock and
        # self._current_concurrency — neither exists on this (non-adaptive)
        # class; they live on AdaptiveAsyncManager. That raised AttributeError
        # whenever _semaphore happened to be None. Fall back to a shared-limit
        # semaphore if somehow still unset.
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)

        # DNS cache for performance
        if not hasattr(self, "_dns_cache"):
            self._dns_cache = {}
            self._dns_cache_ttl = 300  # 5 minutes

        for attempt in range(max_retries + 1):
            try:
                # 1️⃣ RATE LIMIT FIRST (async, non-blocking, with DNS caching)
                if self.rate_limiter:
                    try:
                        # Check DNS cache
                        cache_key = parsed_url.hostname
                        now = time.time()
                        ip = None

                        if cache_key in self._dns_cache:
                            cached_ip, timestamp = self._dns_cache[cache_key]
                            if now - timestamp < self._dns_cache_ttl:
                                ip = cached_ip
                            else:
                                del self._dns_cache[cache_key]

                        if ip is None:
                            loop = asyncio.get_running_loop()
                            ip = await loop.run_in_executor(
                                None, socket.gethostbyname, parsed_url.hostname
                            )
                            self._dns_cache[cache_key] = (ip, now)

                        await self.rate_limiter.async_wait(ip)
                    except Exception as e:
                        logging.debug(f"Rate limiter error for {url}: {e}")

                # 2️⃣ ACQUIRE SEMAPHORE & MAKE REQUEST
                async with self._semaphore:
                    resp = await self._client.head(
                        url,
                        headers=headers or {},
                        timeout=httpx.Timeout(per_request_timeout, connect=4.0),
                    )

                    # Success path
                    duration = time.time() - start_time
                    self.metrics.increment("async_metadata_checks")
                    self.metrics.add_request_time(duration)

                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_success(domain)

                    self.record_result(url, True, duration * 1000, duration)

                    if getattr(resp, "status_code", None) == 404:
                        logging.debug(f"Async HEAD 404 for {url}")
                        return None

                    return resp

            # 3️⃣ TIMEOUT HANDLING
            except (httpx.TimeoutException, asyncio.TimeoutError):
                duration = time.time() - start_time
                logging.debug(
                    f"Async HEAD timeout for {url} (attempt {attempt + 1}/{max_retries + 1})"
                )

                if attempt == max_retries:
                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_failure(domain)
                    self.metrics.add_error(
                        f"Async HEAD timeout after {max_retries + 1} attempts", "async_head_timeout"
                    )
                    self.record_result(url, False, duration * 1000, duration)
                    return None

                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue

            # 4️⃣ NETWORK ERRORS
            except (httpx.ConnectError, httpx.ReadError) as e:
                duration = time.time() - start_time
                logging.debug(
                    f"Async HEAD network error: {type(e).__name__} (attempt {attempt + 1}/{max_retries + 1})"
                )

                if attempt == max_retries:
                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_failure(domain)
                    self.metrics.add_error(f"Async HEAD failed: {e}", "async_head_error")
                    self.record_result(url, False, duration * 1000, duration)
                    return None

                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue

            # 5️⃣ HTTP STATUS ERRORS
            except httpx.HTTPStatusError as e:
                duration = time.time() - start_time
                status = e.response.status_code if e.response else 0

                if status == 404:
                    logging.debug(f"Async HEAD 404 for {url}")
                    self.record_result(url, True, duration * 1000, duration)
                    return None

                logging.debug(f"Async HEAD HTTP {status} (attempt {attempt + 1}/{max_retries + 1})")

                if self.circuit_breaker_manager:
                    self.circuit_breaker_manager.record_failure(domain)
                self.metrics.add_error(
                    f"Async HEAD HTTP {status} for {url}", "async_head_http_error"
                )
                self.record_result(url, False, duration * 1000, duration)

                # Don't retry 4xx client errors (except 404 handled above)
                if 400 <= status < 500:
                    return None

                if attempt == max_retries:
                    return None

                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue

            # 6️⃣ UNEXPECTED ERRORS (fail fast)
            except Exception as e:
                duration = time.time() - start_time
                logging.error(f"Async HEAD unexpected error: {type(e).__name__}: {e}")

                if self.circuit_breaker_manager:
                    self.circuit_breaker_manager.record_failure(domain)
                self.metrics.add_error(f"Async HEAD exception: {e}", "async_head_exception")
                self.record_result(url, False, duration * 1000, duration)
                return None

        return None

    async def get_small_content(self, url: str) -> Optional[bytes]:
        """Get small content via async GET."""
        # Check if we have a client
        if self._client is None or self._client.is_closed:
            self.metrics.add_error(
                f"Async client not available for {url}", "async_client_unavailable"
            )
            return None

        # FIX: Add fallback for tests where circuit_breaker may not be initialized
        if hasattr(self, "circuit_breaker") and self.circuit_breaker:
            can_execute = await self.circuit_breaker.can_execute()
            if not can_execute:
                self.metrics.increment("circuit_breaker_trips")
                return None

        # Apply per-IP rate limiting
        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                # Use thread pool for DNS resolution to avoid blocking
                loop = asyncio.get_running_loop()
                ip = await loop.run_in_executor(None, socket.gethostbyname, parsed.hostname)
                await self.rate_limiter.async_wait(ip)
            except Exception:
                pass

        start = time.time()
        try:
            # FIX: previously this was `async with asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT):`
            # which created a fresh single-permit-batch semaphore on every
            # call — it never actually limited concurrent requests. Reuse
            # the shared instance semaphore initialized in __aenter__.
            sem = self._semaphore or asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
            async with sem:
                # Handle mock clients that don't support async context manager properly
                try:
                    # Try streaming first (preferred)
                    async with self._client.stream("GET", url, timeout=15.0) as resp:
                        if resp.status_code != 200:
                            return None
                        content_chunks = []
                        async for chunk in resp.aiter_bytes():
                            content_chunks.append(chunk)
                            if sum(len(c) for c in content_chunks) >= CONTENT_HASH_LIMIT:
                                break
                        content = b"".join(content_chunks)
                except AttributeError:
                    # Mock client fallback - use direct get
                    resp = await self._client.get(url, timeout=15.0)
                    if resp.status_code != 200:
                        return None
                    content = resp.content
                    if len(content) > CONTENT_HASH_LIMIT:
                        content = content[:CONTENT_HASH_LIMIT]

                rtt = (time.time() - start) * 1000

                # Record result if method exists
                if hasattr(self, "record_result"):
                    self.record_result(url, True, rtt, time.time() - start)

                # Record success in circuit breaker
                if hasattr(self, "circuit_breaker") and self.circuit_breaker:
                    await self.circuit_breaker.record_success()

                return content

        except Exception as e:
            rtt = (time.time() - start) * 1000
            logging.debug(f"Async GET failed for {url}: {e}")

            # Record failure if method exists
            if hasattr(self, "record_result"):
                self.record_result(url, False, rtt, time.time() - start)

            # Record failure in circuit breaker
            if hasattr(self, "circuit_breaker") and self.circuit_breaker:
                await self.circuit_breaker.record_failure()

            return None


class AdaptiveAsyncManager:
    """Manages adaptive async behavior with performance tracking"""

    def __init__(self, config: MirrorConfig, metrics: MetricsCollector):
        """Initialize adaptive async manager."""
        self.config = config
        self.metrics = metrics
        self.profiles: Dict[str, ServerProfile] = {}
        self.lock = RLock()
        self._client: Optional[httpx.AsyncClient] = None
        self._current_concurrency = ADAPTIVE_START_CONCURRENCY
        self._fallback_to_sync = False
        self._profile_complete = False
        self._closed = False
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()
        self._pending_concurrency: Optional[int] = None
        self._last_concurrency_change: float = 0.0
        self.rate_limiter = PerIPRateLimiter() if config.security_validation else None
        self._client_initialized = False  # Add this flag
        # FIX: shared concurrency-limit semaphore. Previously each call
        # created its own asyncio.Semaphore(self._current_concurrency),
        # which never actually limited concurrent operations. Lazily
        # (re)initialized in _init_client() so concurrency changes take
        # effect on subsequent operations without requiring an event loop
        # at construction time.
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._semaphore_lock = asyncio.Lock()

    async def __aenter__(self):
        """Context manager entry"""
        if self._closed:
            raise RuntimeError("Cannot reuse closed AdaptiveAsyncManager")
        await self._init_client()  # Initialize client on entry
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._closed = True
        if self._client and not self._client.is_closed:
            try:
                await self._client.aclose()
            except Exception as e:
                logging.debug(f"Error closing adaptive client: {e}")
        self._client = None
        self._client_initialized = False  # Reset for potential reuse

        # Reset semaphore
        self._semaphore = None

        # Clear profiles if they're stale
        if not self._profile_complete:
            self.profiles.clear()

    def is_available(self) -> bool:
        """
        Check if adaptive async manager is available for use.

        Returns:
            True if manager is not closed and can be used
        """
        if self._closed:
            return False

        # Don't require _client_initialized here - it will be initialized
        # when __aenter__ is called. The check in _check_files_async happens
        # BEFORE the async with block, so the client hasn't been created yet.
        # If _closed is False, the manager IS available - it just needs
        # __aenter__ to initialize the client.
        return True

    async def _init_client(self) -> None:
        """Initialize the async client if not already done — PRODUCTION HARDENED."""
        # ✅ FIX: Use async lock to prevent race condition during initialization
        async with self._semaphore_lock:  # Reuse existing lock for simplicity
            if self._client_initialized and self._client and not self._client.is_closed:
                return
            try:
                # Close old client if it exists and is still open
                if self._client and not self._client.is_closed:
                    try:
                        await self._client.aclose()
                    except Exception as e:
                        logging.debug(f"Error closing old async client: {e}")

                # Create new client with updated concurrency limits
                limits = httpx.Limits(
                    max_connections=self._current_concurrency,
                    max_keepalive_connections=max(2, self._current_concurrency // 3),
                    keepalive_expiry=60.0,
                )
                timeout = httpx.Timeout(
                    self.config.timeout, connect=6.0, read=self.config.timeout * 1.5
                )
                self._client = httpx.AsyncClient(
                    http2=self.config.http2,
                    limits=limits,
                    timeout=timeout,
                    follow_redirects=True,
                    transport=SecureAsyncTransport(rate_limiter=self.rate_limiter),
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    },
                )
                # ✅ FIX: Recreate semaphore under same lock to ensure atomic update
                # This guarantees no operation can observe a mismatched client/semaphore pair.
                self._semaphore = asyncio.Semaphore(self._current_concurrency)
                self._client_initialized = True
                logging.debug(f"Async client initialized: concurrency={self._current_concurrency}")
            except Exception as e:
                logging.error(f"Failed to initialize async client: {e}")
                self._client = None
                self._client_initialized = False
                # Do NOT reset _semaphore here — leave it for caller to handle or retry
                raise

    async def _ensure_client(self) -> bool:
        """Ensure client is initialized before use."""
        if not self._client_initialized or not self._client or self._client.is_closed:
            try:
                await self._init_client()
            except Exception as e:
                logging.error(f"Failed to ensure async client: {e}")
                return False
        return True

    async def profile_server(self, test_urls: List[str]) -> bool:
        """Profile server performance."""
        if self._profile_complete:
            return True
        if not test_urls:
            logging.warning("No URLs provided for profiling, using default settings")
            self._profile_complete = True
            return True

        # Ensure client is initialized
        if not await self._ensure_client():
            logging.warning("Failed to initialize async client for profiling, falling back to sync")
            self._fallback_to_sync = True
            self._profile_complete = True
            return False

        profile = self._get_profile(test_urls[0] if test_urls else str(self.config.base_url))
        logging.info(
            f"🔍 Profiling server {profile.domain} with {min(PROFILE_SAMPLE_SIZE, len(test_urls))} samples..."
        )

        test_batch = test_urls[:PROFILE_SAMPLE_SIZE]
        successful_samples = 0
        total_samples = len(test_batch)

        for i, url in enumerate(test_batch):
            try:
                req_start = time.time()
                if self.rate_limiter:
                    parsed = urlparse(url)
                    try:
                        ip = socket.gethostbyname(parsed.hostname)
                        self.rate_limiter.wait(ip)
                    except Exception:
                        pass
                try:
                    # Python 3.10 fix: replaced async with asyncio.timeout(5.0) with asyncio.wait_for
                    resp = await asyncio.wait_for(
                        self._client.head(
                            url, timeout=httpx.Timeout(3.0, connect=2.0), follow_redirects=False
                        ),
                        timeout=5.0,
                    )
                    rtt = (time.time() - req_start) * 1000
                    success = resp.status_code < 400
                    if success:
                        successful_samples += 1
                    # Always record the sample so error rate reflects HTTP failures, not just timeouts
                    profile.add_sample(rtt, success, time.time() - req_start)
                except asyncio.TimeoutError:
                    logging.debug(f"Profile sample {i + 1} timed out")
                    profile.add_sample(5000.0, False, 5.0)
            except Exception as e:
                profile.add_sample(5000.0, False, 0)
                logging.debug(f"Profile sample {i + 1} failed: {e}")

            if (i + 1) % 5 == 0:
                logging.debug(f"Profile progress: {i + 1}/{total_samples} samples complete")
                await asyncio.sleep(0.1)

        profile._update_metrics()
        success_rate = (successful_samples / total_samples * 100) if total_samples > 0 else 0
        logging.info(
            f"Profile complete: {successful_samples}/{total_samples} successful ({success_rate:.1f}%), "
            f"avg RTT={profile.avg_rtt_ms:.0f}ms, errors={profile.error_rate:.1%}"
        )

        if profile.error_rate > ADAPTIVE_ERROR_THRESHOLD:
            logging.warning(
                f"⚠️ Server {profile.domain} error rate {profile.error_rate:.1%} > threshold, disabling async"
            )
            logging.info("📝 Falling back to sync metadata checks (GET requests will still work)")
            self._fallback_to_sync = True
            self._profile_complete = True
            return False

        if profile.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS * 2:
            logging.warning(
                f"⚠️ Server {profile.domain} high RTT {profile.avg_rtt_ms:.0f}ms, reducing concurrency"
            )
            profile.recommended_concurrency = max(1, profile.recommended_concurrency // 3)
            self._current_concurrency = profile.recommended_concurrency
            await self._init_client()  # Reinitialize with new concurrency

        self._profile_complete = True
        logging.info(
            f"✅ Server profile complete: concurrency={self._current_concurrency}, "
            f"RTT={profile.avg_rtt_ms:.0f}ms, errors={profile.error_rate:.1%}"
        )
        return not self._fallback_to_sync

    def should_fallback(self) -> bool:
        """Check if should fallback to sync"""
        return self._fallback_to_sync

    def get_concurrency(self) -> int:
        """Get current concurrency level"""
        return self._current_concurrency

    def record_result(self, url: str, success: bool, rtt_ms: float, duration: float) -> None:
        """Record operation result for adaptive adjustment — THREAD SAFE."""
        profile = self._get_profile(url)
        profile.add_sample(rtt_ms, success, duration)

        # ✅ FIX: Protect all state mutations with self.lock
        with self.lock:
            # Check if we need to fall back to sync
            if profile.error_rate > ADAPTIVE_ERROR_THRESHOLD and not self._fallback_to_sync:
                logging.warning(
                    f"⚠️ Error rate {profile.error_rate:.1%} exceeded, falling back to sync"
                )
                self._fallback_to_sync = True
                self.metrics.increment("adaptive_fallback_events")  # ✅ Track fallbacks
                return

            # Cooldown period: don't change concurrency too frequently
            now = time.time()
            # ✅ FIX: Initialize _last_concurrency_change in __init__, so hasattr not needed
            if now - self._last_concurrency_change < ADAPTIVE_COOLDOWN_SECONDS:
                return

            # Scale up logic
            if profile.should_scale_up() and self._current_concurrency < ADAPTIVE_MAX_CONCURRENCY:
                new_concurrency = min(ADAPTIVE_MAX_CONCURRENCY, self._current_concurrency + 2)
                if new_concurrency != self._current_concurrency:
                    logging.debug(
                        f"⚡ Adaptive scale up: {self._current_concurrency} → {new_concurrency}"
                    )
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment("adaptive_scale_up_events")  # ✅ Track scale-ups

            # NEW: Scale down logic for moderate error rates (between 2.5% and 5%)
            elif (
                profile.error_rate > ADAPTIVE_ERROR_THRESHOLD * 0.5
                and self._current_concurrency > ADAPTIVE_START_CONCURRENCY
            ):
                new_concurrency = max(ADAPTIVE_START_CONCURRENCY, self._current_concurrency // 2)
                if new_concurrency != self._current_concurrency:
                    logging.warning(
                        f"⚠️ Adaptive scale down (moderate errors {profile.error_rate:.1%}): "
                        f"{self._current_concurrency} → {new_concurrency}"
                    )
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment(
                        "adaptive_scale_down_error_events"
                    )  # ✅ Track error-based scale-downs

            # Also scale down if RTT is very high
            elif (
                profile.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS * 2
                and self._current_concurrency > ADAPTIVE_START_CONCURRENCY
            ):
                new_concurrency = max(ADAPTIVE_START_CONCURRENCY, self._current_concurrency - 1)
                if new_concurrency != self._current_concurrency:
                    logging.debug(
                        f"📉 Adaptive scale down (high RTT {profile.avg_rtt_ms:.0f}ms): "
                        f"{self._current_concurrency} → {new_concurrency}"
                    )
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment(
                        "adaptive_scale_down_rtt_events"
                    )  # ✅ Track RTT-based scale-downs

    def get_circuit_breaker_stats(self) -> Optional[Dict[str, Any]]:
        """Get circuit breaker statistics."""
        if self.circuit_breaker_manager:
            return self.circuit_breaker_manager.get_stats()
        return None

    async def _do_head_request(
        self, url: str, headers: Optional[Dict[str, str]]
    ) -> Optional[httpx.Response]:
        """Internal method to perform the actual HEAD request."""
        if not await self._ensure_client() or self._fallback_to_sync:
            return None

        if self._client is None:
            return None

        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                ip = socket.gethostbyname(parsed.hostname)
                self.rate_limiter.wait(ip)
            except Exception:
                pass

        # FIX: previously `semaphore = asyncio.Semaphore(self._current_concurrency)`
        # was constructed here per call — that never limited concurrency.
        # Reuse the shared instance semaphore from _init_client.
        sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
        start_time = time.time()

        async with sem:
            for attempt in range(self.config.max_retries + 1):
                try:
                    resp = await asyncio.wait_for(
                        self._client.head(
                            url, headers=headers or {}, timeout=httpx.Timeout(12.0, connect=4.0)
                        ),
                        timeout=12.0,
                    )

                    self.metrics.increment("async_metadata_checks")
                    duration = time.time() - start_time
                    self.metrics.add_request_time(duration)
                    self.record_result(url, True, duration * 1000, duration)
                    # Treat 404 as "no resource" rather than a usable response
                    if getattr(resp, "status_code", None) == 404:
                        return None
                    return resp

                except asyncio.TimeoutError:
                    if attempt == self.config.max_retries:
                        duration = time.time() - start_time
                        self.record_result(url, False, 12000.0, duration)
                        return None
                    await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError):
                    if attempt == self.config.max_retries:
                        duration = time.time() - start_time
                        self.record_result(url, False, duration * 1000, duration)
                        return None
                    await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

        return None

    async def head(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> Optional[httpx.Response]:
        """Make async HEAD request with adaptive behavior and proper timeout handling."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        # ✅ FIX 1: Single circuit breaker check (removed duplicate)
        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment("circuit_breaker_trips")
            self.metrics.add_error(f"Circuit breaker open for domain {domain}", "circuit_breaker")
            return None

        if not await self._ensure_client():
            self.metrics.add_error(
                f"Async client not available for {url}", "async_client_unavailable"
            )
            return None
        if self._client is None or self._client.is_closed:
            return None

        # ✅ FIX 2: Non-blocking rate limiting with async DNS resolution
        if self.rate_limiter and parsed_url.hostname:
            try:
                loop = asyncio.get_running_loop()
                # Lazy-init DNS cache to avoid blocking event loop with repeated lookups
                if not hasattr(self, "_dns_cache"):
                    self._dns_cache = {}
                cache_key = parsed_url.hostname
                now = time.time()
                cached = self._dns_cache.get(cache_key)
                if cached and (now - cached[1]) < 300:  # 5 min TTL
                    ip = cached[0]
                else:
                    ip = await loop.run_in_executor(None, socket.gethostbyname, parsed_url.hostname)
                    self._dns_cache[cache_key] = (ip, now)
                # Use async_wait() instead of blocking wait()
                await self.rate_limiter.async_wait(ip)
            except Exception as e:
                logging.debug(f"Async rate limiter error for {url}: {e}")

        sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
        start_time = time.time()

        async def _do_request_with_retries():
            async with sem:
                for attempt in range(self.config.max_retries + 1):
                    attempt_start = time.time()  # ✅ FIX 3: Track per-attempt duration
                    try:
                        resp = await self._client.head(
                            url, headers=headers or {}, timeout=httpx.Timeout(12.0, connect=4.0)
                        )
                        rtt_ms = (time.time() - attempt_start) * 1000
                        self.metrics.increment("async_metadata_checks")
                        self.metrics.add_request_time(time.time() - start_time)

                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_success(domain)
                        self.record_result(url, True, rtt_ms, time.time() - attempt_start)

                        if getattr(resp, "status_code", None) == 404:
                            return None
                        return resp

                    except (httpx.TimeoutException, asyncio.TimeoutError):
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.debug(f"Async HEAD timeout for {url} (attempt {attempt + 1})")
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            self.metrics.add_error(
                                f"Async HEAD timeout for {url}", "async_head_timeout"
                            )
                            self.record_result(url, False, 12000.0, time.time() - start_time)
                            return None
                        await asyncio.sleep(
                            exponential_backoff(attempt, self.config.retry_delay * 0.7)
                        )

                    except (httpx.ConnectError, httpx.ReadError) as e:
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.debug(f"Async HEAD error for {url}: {e} (attempt {attempt + 1})")
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            self.metrics.add_error(
                                f"Async HEAD failed for {url}: {e}", "async_head_error"
                            )
                            self.record_result(url, False, rtt_ms, time.time() - start_time)
                            return None
                        await asyncio.sleep(
                            exponential_backoff(attempt, self.config.retry_delay * 0.7)
                        )

                    except httpx.HTTPStatusError as e:
                        status = e.response.status_code if e.response else 0
                        rtt_ms = (time.time() - attempt_start) * 1000
                        if status == 404:
                            self.record_result(url, True, rtt_ms, time.time() - attempt_start)
                            return e.response
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_failure(domain)
                        self.metrics.add_error(
                            f"Async HEAD HTTP {status} for {url}", "async_head_http_error"
                        )
                        self.record_result(url, False, rtt_ms, time.time() - start_time)
                        return None

                    except Exception as e:
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.error(f"Async HEAD unexpected error for {url}: {e}")
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_failure(domain)
                        self.metrics.add_error(
                            f"Async HEAD exception for {url}: {e}", "async_head_exception"
                        )
                        self.record_result(url, False, rtt_ms, time.time() - start_time)
                        return None

        try:
            # Hard cap for the entire retry sequence (prevents runaway backoff)
            return await asyncio.wait_for(_do_request_with_retries(), timeout=30.0)
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            logging.warning(f"Async HEAD overall timeout for {url} after {duration:.1f}s")
            if self.circuit_breaker_manager:
                self.circuit_breaker_manager.record_failure(domain)
            self.metrics.add_error(
                f"Async HEAD overall timeout for {url}", "async_head_overall_timeout"
            )
            self.record_result(url, False, duration * 1000, duration)
            return None

    async def get_small_content(self, url: str) -> Optional[bytes]:
        """Get small content via async GET."""
        if not await self._ensure_client() or self._fallback_to_sync:
            return None

        if self._client is None:
            return None

        # FIX: await the coroutine
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment("circuit_breaker_trips")
            return None

        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                # Use thread pool for DNS resolution to avoid blocking
                loop = asyncio.get_running_loop()
                ip = await loop.run_in_executor(None, socket.gethostbyname, parsed.hostname)
                await self.rate_limiter.async_wait(ip)
            except Exception:
                pass

        start = time.time()
        try:
            # FIX: use the shared instance semaphore. Previously a fresh
            # asyncio.Semaphore(self._current_concurrency) was constructed
            # per-call and so never actually limited concurrent operations.
            sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
            async with sem:
                # Handle mock clients that don't support async context manager properly
                try:
                    # Try streaming first (preferred for large files)
                    async with self._client.stream("GET", url, timeout=15.0) as resp:
                        if resp.status_code != 200:
                            return None

                        content_chunks = []
                        async for chunk in resp.aiter_bytes():
                            content_chunks.append(chunk)
                            if sum(len(c) for c in content_chunks) >= CONTENT_HASH_LIMIT:
                                break

                        content = b"".join(content_chunks)
                except AttributeError:
                    # Mock client fallback - use direct get
                    resp = await self._client.get(url, timeout=15.0)
                    if resp.status_code != 200:
                        return None
                    content = resp.content
                    if len(content) > CONTENT_HASH_LIMIT:
                        content = content[:CONTENT_HASH_LIMIT]

                rtt = (time.time() - start) * 1000

                self.record_result(url, True, rtt, time.time() - start)

                if self.circuit_breaker_manager:
                    self.circuit_breaker_manager.record_success(domain)

                return content

        except Exception as e:
            rtt = (time.time() - start) * 1000
            logging.debug(f"Async GET failed for {url}: {e}")
            self.record_result(url, False, rtt, time.time() - start)

            if self.circuit_breaker_manager:
                self.circuit_breaker_manager.record_failure(domain)

            return None

    async def apply_pending_concurrency_change(self) -> None:
        """Apply pending concurrency changes at safe points"""
        if self._pending_concurrency is not None:
            if self._current_concurrency != self._pending_concurrency:
                logging.info(
                    f"⚡ Applying adaptive concurrency change: {self._current_concurrency} → {self._pending_concurrency}"
                )
                self._current_concurrency = self._pending_concurrency
                await self._init_client()  # Reinitialize with new concurrency
                self._pending_concurrency = None

    def get_stats(self) -> Dict[str, Any]:
        """Get adaptive async manager statistics"""
        stats = {
            "current_concurrency": self._current_concurrency,
            "fallback_to_sync": self._fallback_to_sync,
            "profile_complete": self._profile_complete,
            "profiles": {domain: profile.__dict__ for domain, profile in self.profiles.items()},
            "rate_limiter": self.rate_limiter.get_stats() if self.rate_limiter else None,
            "client_initialized": self._client_initialized,
        }

        # Add circuit breaker stats if available
        if self.circuit_breaker_manager:
            stats["circuit_breaker"] = self.circuit_breaker_manager.get_stats()

        return stats

    async def warm_up(self, urls: List[str]) -> None:
        """Pre-warm adaptive async connections."""
        if not urls or self._closed or self._fallback_to_sync:
            return

        # Initialize client first
        if not await self._ensure_client():
            logging.warning("Cannot warm up - async client initialization failed")
            return

        if not self._profile_complete:
            # Profile first with a subset of URLs
            await self.profile_server(urls[:10])

        await self._warm_up_connections(urls[:10])

    async def _warm_up_connections(self, urls: List[str]) -> None:
        """Internal warm-up implementation"""
        if not self._client or self._client.is_closed:
            return

        logging.info("🔥 Pre-warming adaptive async connections")

        tasks = []
        for url in urls:
            tasks.append(self._warm_single_connection(url))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        logging.info(f"✅ Warmed up {success_count}/{len(urls)} async connections")

    async def _warm_single_connection(self, url: str) -> bool:
        """Warm up a single connection"""
        try:
            if self.rate_limiter:
                parsed = urlparse(url)
                ip = socket.gethostbyname(parsed.hostname)
                self.rate_limiter.wait(ip)

            # FIX: use the shared instance semaphore (initialized in
            # _init_client). The previous per-call Semaphore did not limit
            # concurrent warm-ups at all.
            sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
            async with sem:
                resp = await self._client.head(
                    url, timeout=httpx.Timeout(3.0, connect=2.0), follow_redirects=False
                )
                await resp.aclose()
            return True
        except Exception as e:
            logging.debug(f"Warm-up failed for {url}: {e}")
            return False

    def _get_profile(self, url: str) -> ServerProfile:
        """Get or create server profile for URL"""
        with self.lock:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain not in self.profiles:
                is_throttled = any(kd in domain for kd in KNOWN_THROTTLED_DOMAINS)
                start_conc = 3 if is_throttled else ADAPTIVE_START_CONCURRENCY
                self.profiles[domain] = ServerProfile(
                    domain=domain, is_throttled=is_throttled, recommended_concurrency=start_conc
                )
                if is_throttled:
                    # Also set current concurrency to conservative value
                    if not self._fallback_to_sync:
                        self._current_concurrency = start_conc
                    logging.info(f"🔍 Known throttled domain: {domain}, starting conservative")
            return self.profiles[domain]


class AsyncTaskManager:
    """Manages async tasks with proper cleanup and cancellation."""

    def __init__(self):
        """Initialize async task manager."""
        self.tasks: Set[asyncio.Task] = set()
        self.lock = asyncio.Lock()
        self._shutdown = False
        self.total_created = 0
        self.total_completed = 0

    async def create_task(self, coro) -> asyncio.Task:
        """
        Create a task with automatic cleanup.

        Args:
            coro: Coroutine to wrap

        Returns:
            asyncio.Task instance
        """
        task = asyncio.create_task(coro)
        async with self.lock:
            self.tasks.add(task)
            self.total_created += 1
        task.add_done_callback(self._task_done)
        return task

    def _task_done(self, task: asyncio.Task) -> None:
        """Remove completed task (callback)."""
        # Schedule removal in the event loop
        if not self._shutdown:
            try:
                loop = asyncio.get_running_loop()
                # Use call_soon_threadsafe if needed, but here we're likely in the same loop
                if loop.is_running():
                    # Create task but ensure it's tracked
                    removal_task = loop.create_task(self._remove_task(task))

                    # Add to tracked tasks
                    async def track_and_remove():
                        async with self.lock:
                            self.tasks.add(removal_task)
                        await self._remove_task(task)
                        async with self.lock:
                            self.tasks.discard(removal_task)

                    loop.create_task(track_and_remove())
                else:
                    self._sync_remove_task(task)
            except RuntimeError:
                # No running loop, use sync removal
                self._sync_remove_task(task)
        else:
            # During shutdown, remove synchronously
            self._sync_remove_task(task)

    async def _remove_task(self, task: asyncio.Task) -> None:
        """Remove task from set."""
        async with self.lock:
            if task in self.tasks:
                self.tasks.discard(task)
                self.total_completed += 1

    def _sync_remove_task(self, task: asyncio.Task) -> None:
        """Synchronous removal for shutdown."""
        try:
            # Use asyncio.run_coroutine_threadsafe if possible
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._remove_task(task))
            else:
                # No loop running, just remove from set (safe during shutdown)
                if task in self.tasks:
                    self.tasks.discard(task)
                    self.total_completed += 1
        except RuntimeError:
            # No event loop, just remove
            if task in self.tasks:
                self.tasks.discard(task)
                self.total_completed += 1

    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown all tasks gracefully with proper cleanup.

        Args:
            timeout: Maximum time to wait for tasks to complete
        """
        if self._shutdown:
            logging.debug("AsyncTaskManager already shutting down")
            return

        logging.debug(f"AsyncTaskManager shutdown initiated (active tasks: {len(self.tasks)})")
        self._shutdown = True

        # Take a snapshot of tasks to cancel
        async with self.lock:
            tasks_to_cancel = list(self.tasks)
            logging.debug(f"Cancelling {len(tasks_to_cancel)} async tasks")

        if not tasks_to_cancel:
            logging.debug("No tasks to cancel")
            return

        # Cancel each task
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete with timeout
        try:
            # Use asyncio.wait with timeout for graceful shutdown
            done, pending = await asyncio.wait(
                tasks_to_cancel, timeout=timeout, return_when=asyncio.ALL_COMPLETED
            )

            if pending:
                logging.warning(
                    f"Async shutdown timeout: {len(pending)} tasks still pending after {timeout}s"
                )

                # Force cancel remaining tasks
                for task in pending:
                    if not task.done():
                        task.cancel()
                        try:
                            # Give each task a brief moment to clean up
                            await asyncio.wait_for(task, timeout=1.0)
                        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                            pass

        except asyncio.TimeoutError:
            logging.error(f"AsyncTaskManager shutdown timed out after {timeout}s")
        except Exception as e:
            logging.error(f"Error during async shutdown: {e}")

        # Clear remaining tasks
        async with self.lock:
            self.tasks.clear()

        # Log final statistics
        logging.info(
            f"AsyncTaskManager shutdown complete: "
            f"created={self.total_created}, completed={self.total_completed}, "
            f"cancelled={self.total_created - self.total_completed}"
        )

        # Reset counters for potential reuse
        self.total_created = 0
        self.total_completed = 0

    async def force_shutdown(self, timeout: float = 5.0) -> None:
        """
        Force shutdown all tasks immediately without waiting for completion.
        Use this only when graceful shutdown fails or times out.

        Args:
            timeout: Maximum time per task for forced cancellation
        """
        logging.warning(f"Force shutting down AsyncTaskManager (active tasks: {len(self.tasks)})")
        self._shutdown = True

        async with self.lock:
            tasks_to_cancel = list(self.tasks)
            self.tasks.clear()

        # Force cancel all tasks
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()

        # Brief wait for cancellation to propagate
        if tasks_to_cancel:
            try:
                await asyncio.wait(tasks_to_cancel, timeout=min(timeout, 5.0))
            except Exception:
                pass

        logging.info("AsyncTaskManager force shutdown complete")

    def get_stats(self) -> Dict[str, Any]:
        """Get task manager statistics."""
        return {
            "active_tasks": len(self.tasks),
            "shutdown": self._shutdown,
            "total_created": self.total_created,
            "total_completed": self.total_completed,
        }


__all__ = ["AsyncConnectionManager", "AdaptiveAsyncManager", "AsyncTaskManager"]
