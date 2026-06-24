"""Bandwidth and request-rate limiters.

Migrated verbatim from ``mirror_url.py``:
``BandwidthLimiter`` (orig. 3341-3395), ``RateLimiter`` (orig. 3918-4006),
``PerIPRateLimiter`` (orig. 4011-4081), ``ChunkAwareRateLimiter`` (orig. 4086-4156).
"""

from __future__ import annotations

import asyncio
import logging
import time
from threading import Lock, RLock
from typing import Any, Dict, Optional

from .constants import DEFAULT_RATE_LIMIT, REQUEST_DELAY


class BandwidthLimiter:
    """Limit download bandwidth with smoothing"""

    def __init__(self, max_bytes_per_second: Optional[float] = None):
        """
        Initialize bandwidth limiter.

        Args:
            max_bytes_per_second: Maximum bytes per second
        """
        self.max_bytes_per_second = max_bytes_per_second
        self.bytes_downloaded = 0
        self.last_check = time.time()
        self.lock = RLock()
        self.peak_rate = 0.0
        self.average_rate = 0.0

    def throttle(self, bytes_count: int) -> None:
        """
        Throttle download speed.

        Args:
            bytes_count: Number of bytes just downloaded
        """
        if not self.max_bytes_per_second:
            return
        with self.lock:
            self.bytes_downloaded += bytes_count
            now = time.time()
            elapsed = now - self.last_check

            if elapsed >= 1.0:
                current_rate = self.bytes_downloaded / elapsed
                self.peak_rate = max(self.peak_rate, current_rate)
                self.average_rate = self.average_rate * 0.9 + current_rate * 0.1
                self.bytes_downloaded = 0
                self.last_check = now
            elif self.bytes_downloaded > self.max_bytes_per_second:
                sleep_time = (self.bytes_downloaded / self.max_bytes_per_second) - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

    def get_stats(self) -> Dict[str, float]:
        """
        Get bandwidth limiter statistics.

        Returns:
            Dictionary with bandwidth statistics
        """
        with self.lock:
            return {
                "limit_bps": self.max_bytes_per_second,
                "peak_bps": self.peak_rate,
                "average_bps": self.average_rate,
            }


class RateLimiter:
    """Rate limiter for HTTP requests with per-IP option"""

    def __init__(
        self,
        requests_per_second: float = DEFAULT_RATE_LIMIT,
        delay: float = REQUEST_DELAY,
        per_ip: bool = False,
    ):
        """
        Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests per second
            delay: Minimum delay between requests
            per_ip: Whether to rate limit per IP
        """
        self.min_interval = max(1.0 / requests_per_second, delay)
        self.last_request = 0
        self.per_ip = per_ip
        self.ip_last_requests: Dict[str, float] = {}
        self.lock = RLock()
        self.total_delays = 0

        # FIX: Periodic cleanup tracking
        self._cleanup_counter = 0
        self._cleanup_interval = 500  # Clean every 500 requests

    def wait(self, ip: Optional[str] = None) -> None:
        """
        Wait if necessary to respect rate limit.

        Args:
            ip: IP address for per-IP limiting
        """
        with self.lock:
            if self.per_ip and ip:
                last = self.ip_last_requests.get(ip, 0)
                elapsed = time.time() - last
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    time.sleep(sleep_time)
                    self.total_delays += 1
                self.ip_last_requests[ip] = time.time()

                # FIX: Periodic cleanup of stale IP entries
                self._cleanup_counter += 1
                if self._cleanup_counter >= self._cleanup_interval:
                    self._cleanup_stale_entries()
                    self._cleanup_counter = 0
            else:
                elapsed = time.time() - self.last_request
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
                    self.total_delays += 1
                self.last_request = time.time()

    # FIX: Add cleanup method
    def _cleanup_stale_entries(self) -> None:
        """Remove IP entries older than 1 hour."""
        if not self.per_ip:
            return

        now = time.time()
        max_age = 3600  # 1 hour

        stale_ips = [
            ip for ip, last_time in self.ip_last_requests.items() if now - last_time > max_age
        ]

        for ip in stale_ips:
            del self.ip_last_requests[ip]

        if stale_ips and len(stale_ips) > 100:
            logging.debug(
                f"RateLimiter: cleaned {len(stale_ips)} stale IP entries "
                f"(remaining: {len(self.ip_last_requests)})"
            )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get rate limiter statistics.

        Returns:
            Dictionary with rate limiter statistics
        """
        with self.lock:
            return {
                "min_interval_ms": self.min_interval * 1000,
                "per_ip": self.per_ip,
                "active_ips": len(self.ip_last_requests) if self.per_ip else 0,
                "total_delays": self.total_delays,
            }


# ============================================================================
# PER-IP RATE LIMITER (REPLACES STANDALONE DEFINITION)
# ============================================================================
class PerIPRateLimiter(RateLimiter):
    """Rate limiter that tracks and limits requests per IP address with async support."""

    def __init__(self, requests_per_second: float = DEFAULT_RATE_LIMIT):
        # Initialize base class (delay=0 ensures min_interval is purely rate-based)
        super().__init__(requests_per_second=requests_per_second, delay=0.0, per_ip=True)
        # Use explicit per-IP tracking for better cleanup control
        self.last_requests: Dict[str, float] = {}
        # self.lock and self.total_delays are inherited from RateLimiter

    def wait(self, ip: str) -> None:
        """Synchronous wait for rate limiting (blocks thread)."""
        sleep_time = 0.0
        with self.lock:
            now = time.time()
            last = self.last_requests.get(ip, 0.0)
            elapsed = now - last
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                # ATOMIC: Reserve next allowed timestamp BEFORE sleeping
                self.last_requests[ip] = now + sleep_time
                self.total_delays += 1
            else:
                self.last_requests[ip] = now

            # Periodic cleanup to prevent memory leaks
            if len(self.last_requests) % 500 == 0:
                self._cleanup_old_entries_unlocked()

        # Sleep OUTSIDE the lock to prevent blocking concurrent threads for other IPs
        if sleep_time > 0:
            time.sleep(sleep_time)

    async def async_wait(self, ip: str) -> None:
        """Non-blocking async wait for rate limiting (does NOT block event loop)."""
        sleep_time = 0.0
        with self.lock:
            now = time.time()
            last = self.last_requests.get(ip, 0.0)
            elapsed = now - last
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                # ATOMIC: Reserve next allowed timestamp
                self.last_requests[ip] = now + sleep_time
                self.total_delays += 1
            else:
                self.last_requests[ip] = now

        # Yield control back to event loop instead of blocking
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    def _cleanup_old_entries_unlocked(self, max_age_seconds: float = 3600) -> None:
        """Internal cleanup. Assumes self.lock is already held."""
        now = time.time()
        expired = [ip for ip, last in self.last_requests.items() if now - last > max_age_seconds]
        for ip in expired:
            del self.last_requests[ip]

    def cleanup_old_entries(self, max_age_seconds: float = 3600) -> None:
        """Public cleanup method."""
        with self.lock:
            self._cleanup_old_entries_unlocked(max_age_seconds)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self.lock:
            return {
                "active_ips": len(self.last_requests),
                "total_delays": self.total_delays,
                "requests_per_second": 1.0 / self.min_interval if self.min_interval > 0 else 0,
            }


# ============================================================================
# CHUNK-AWARE RATE LIMITER
# ============================================================================
class ChunkAwareRateLimiter(RateLimiter):
    """Rate limiter that accounts for parallel chunk connections - OPTIMIZED"""

    def __init__(
        self,
        requests_per_second: float = DEFAULT_RATE_LIMIT,
        delay: float = REQUEST_DELAY,
        per_ip: bool = False,
        chunk_multiplier: float = 0.5,
        disable_scaling: bool = False,
    ):  # NEW PARAM
        super().__init__(requests_per_second, delay, per_ip)
        self.chunk_multiplier = chunk_multiplier
        self.active_chunks_per_ip: Dict[str, int] = {}
        self.chunk_lock = RLock()
        self.disable_scaling = disable_scaling  # NEW FLAG
        self._wait_lock = Lock()

    def register_chunk_start(self, ip: str) -> None:
        """Register that a chunk download is starting for an IP with max limit"""
        with self.chunk_lock:
            current = self.active_chunks_per_ip.get(ip, 0)
            # Add safety limit to prevent overloading a single IP
            max_per_ip = 20  # Conservative limit
            if current >= max_per_ip:
                logging.warning(
                    f"IP {ip} has {current} active chunks, max {max_per_ip} - throttling"
                )
                return
            self.active_chunks_per_ip[ip] = current + 1

    def register_chunk_complete(self, ip: str) -> None:
        """Register that a chunk download completed for an IP"""
        with self.chunk_lock:
            current = self.active_chunks_per_ip.get(ip, 0)
            if current > 1:
                self.active_chunks_per_ip[ip] = current - 1
            else:
                self.active_chunks_per_ip.pop(ip, None)

    def wait(self, ip: Optional[str] = None) -> None:
        """Wait with optimized scaling - FIXED DEADLOCK & RACE CONDITION"""
        # Get active chunk count WITHOUT nested locks
        active_chunks = 1
        if self.per_ip and ip:
            with self.chunk_lock:
                active_chunks = self.active_chunks_per_ip.get(ip, 1)

        # Calculate delay inside lock, sleep outside to prevent blocking other IPs
        sleep_time = 0
        with self._wait_lock:
            now = time.time()
            if self.per_ip and ip:
                if self.disable_scaling:
                    effective_delay = self.min_interval
                else:
                    multiplier = 1.0 + (active_chunks - 1) * 0.1
                    effective_delay = self.min_interval * min(1.5, multiplier)

                last = self.ip_last_requests.get(ip, 0)
                elapsed = now - last
                if elapsed < effective_delay:
                    sleep_time = effective_delay - elapsed
                    # FIX: Update timestamp BEFORE releasing lock to atomically reserve the slot
                    self.ip_last_requests[ip] = now + sleep_time
                    self.total_delays += 1
            else:
                elapsed = now - self.last_request
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    # FIX: Update timestamp BEFORE releasing lock
                    self.last_request = now + sleep_time
                    self.total_delays += 1

        # Sleep OUTSIDE the lock (prevents blocking concurrent threads for other IPs)
        if sleep_time > 0:
            time.sleep(sleep_time)


__all__ = [
    "BandwidthLimiter",
    "RateLimiter",
    "PerIPRateLimiter",
    "ChunkAwareRateLimiter",
]
