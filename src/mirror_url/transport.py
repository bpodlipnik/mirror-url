"""Hardened httpx transports (SSRF / private-IP guards).

Migrated verbatim from ``mirror_url.py`` (orig. lines 1379-1651):
``SecureTransport``, ``SecureAsyncTransport``.

These resolve and validate the target IP before connecting, pin the connection
to that validated IP while preserving the ``Host`` header / SNI, and apply
optional per-IP rate limiting.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import time
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

import httpx

from .exceptions import SecurityError
from .security import SecurityValidator

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .rate_limiter import PerIPRateLimiter


class SecureTransport(httpx.HTTPTransport):
    """Transport that validates resolved IP before connecting"""

    IP_CACHE_TTL_SECONDS = 300
    IP_CACHE_MAX_SIZE = 1000  # FIX: Prevent unbounded growth
    IP_CACHE_CLEANUP_INTERVAL = 60  # FIX: Cleanup every 60 seconds

    def __init__(self, rate_limiter: Optional[PerIPRateLimiter] = None, test_mode: bool = False):
        super().__init__()
        self._test_mode = test_mode  # ✅ Instance-level flag (eliminates global test pollution)
        self._resolved_ips: Dict[str, Tuple[str, float]] = {}
        self._ip_lock = RLock()
        self.rate_limiter = rate_limiter

        # FIX: Track cleanup timing
        self._last_cleanup = time.time()
        self._request_count = 0  # For periodic cleanup triggering

    def _get_cached_ip(self, hostname: str) -> Optional[str]:
        """Get cached IP if still valid"""
        with self._ip_lock:
            if hostname in self._resolved_ips:
                ip, timestamp = self._resolved_ips[hostname]
                if time.time() - timestamp < self.IP_CACHE_TTL_SECONDS:
                    return ip
                del self._resolved_ips[hostname]
            return None

    def _cache_ip(self, hostname: str, ip: str) -> None:
        """Cache resolved IP with timestamp and bounded size"""
        with self._ip_lock:
            # FIX: If cache is full, remove oldest entries before adding
            if len(self._resolved_ips) >= self.IP_CACHE_MAX_SIZE:
                # Remove 25% oldest entries to make room
                entries = sorted(self._resolved_ips.items(), key=lambda x: x[1][1])
                to_remove = len(entries) // 4
                for old_hostname, _ in entries[:to_remove]:
                    del self._resolved_ips[old_hostname]
                logging.debug(f"IP cache pruned: removed {to_remove} oldest entries")

            self._resolved_ips[hostname] = (ip, time.time())

    def _cleanup_stale_ips(self, force: bool = False) -> int:
        """Remove stale IP entries.

        Args:
            force: If True, clean regardless of interval

        Returns:
            Number of entries removed
        """
        now = time.time()
        if not force and now - self._last_cleanup < self.IP_CACHE_CLEANUP_INTERVAL:
            return 0

        with self._ip_lock:
            stale = [
                hostname
                for hostname, (_, timestamp) in self._resolved_ips.items()
                if now - timestamp > self.IP_CACHE_TTL_SECONDS
            ]
            for hostname in stale:
                del self._resolved_ips[hostname]

            self._last_cleanup = now

            if stale:
                logging.debug(
                    f"Cleaned {len(stale)} stale IP entries (cache size: {len(self._resolved_ips)})"
                )
            return len(stale)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # Skip IP resolution in test mode
        if self._test_mode:
            return super().handle_request(request)

        hostname = request.url.host
        hostname = hostname.strip("[]")
        try:
            ipaddress.ip_address(hostname)
            raise SecurityError(f"Direct IP connection attempted: {hostname}")
        except ValueError:
            pass

        # Apply per-IP rate limiting
        if self.rate_limiter:
            self.rate_limiter.wait(hostname)

        # FIX: Periodic cleanup of stale IPs
        self._request_count += 1
        if self._request_count % 100 == 0:  # Every 100 requests
            self._cleanup_stale_ips()

        safe_ip = self._get_cached_ip(hostname)
        if safe_ip is None:
            with self._ip_lock:
                safe_ip = self._get_cached_ip(hostname)
                if safe_ip is None:
                    safe_ip = SecurityValidator.resolve_and_validate_hostname(hostname)
                    self._cache_ip(hostname, safe_ip)

        # Create new request with IP address instead of hostname
        # But preserve the Host header for virtual hosting
        new_url = request.url.copy_with(host=safe_ip)
        new_headers = request.headers.copy()
        new_headers["Host"] = hostname
        new_extensions = dict(request.extensions) if request.extensions else {}
        new_extensions["sni_hostname"] = hostname
        new_request = httpx.Request(
            method=request.method,
            url=new_url,
            headers=new_headers,
            stream=request.stream,
            extensions=new_extensions,
        )
        try:
            return super().handle_request(new_request)
        except httpx.ConnectError:
            # DNS rotation or dead IP: clear cache and retry once
            with self._ip_lock:
                self._resolved_ips.pop(hostname, None)
            safe_ip_retry = SecurityValidator.resolve_and_validate_hostname(hostname)
            self._cache_ip(hostname, safe_ip_retry)
            new_url_retry = request.url.copy_with(host=safe_ip_retry)
            new_request_retry = httpx.Request(
                method=request.method,
                url=new_url_retry,
                headers=request.headers,
                extensions={"sni_hostname": hostname},
            )
            return super().handle_request(new_request_retry)

    def clear_ip_cache(self) -> None:
        """Clear IP cache (for testing/shutdown)."""
        with self._ip_lock:
            self._resolved_ips.clear()
            logging.debug("IP cache cleared")

    def get_ip_cache_stats(self) -> Dict[str, Any]:
        """Get IP cache statistics."""
        with self._ip_lock:
            return {
                "size": len(self._resolved_ips),
                "max_size": self.IP_CACHE_MAX_SIZE,
                "ttl_seconds": self.IP_CACHE_TTL_SECONDS,
                "entries": list(self._resolved_ips.keys())[:10],  # Show first 10
            }


class SecureAsyncTransport(httpx.AsyncHTTPTransport):
    """Async Transport that validates resolved IP before connecting with non-blocking rate limiting"""

    IP_CACHE_TTL_SECONDS = 300
    IP_CACHE_MAX_SIZE = 1000  # FIX: Prevent unbounded growth
    IP_CACHE_CLEANUP_INTERVAL = 60  # FIX: Cleanup every 60 seconds

    def __init__(self, rate_limiter: Optional[PerIPRateLimiter] = None, test_mode: bool = False):
        super().__init__()
        self._resolved_ips: Dict[str, Tuple[str, float]] = {}
        self._ip_lock = asyncio.Lock()
        self.rate_limiter = rate_limiter
        # FIX: handle_async_request() reads self._test_mode, but it was never
        # initialized here (only SecureTransport set it). That raised
        # AttributeError on EVERY async request, breaking all async metadata /
        # download paths. Mirror the sync transport's behavior.
        self._test_mode = test_mode

        # FIX: Track cleanup timing
        self._last_cleanup = time.time()
        self._request_count = 0

    async def _get_cached_ip(self, hostname: str) -> Optional[str]:
        """Get cached IP if still valid"""
        async with self._ip_lock:
            if hostname in self._resolved_ips:
                ip, timestamp = self._resolved_ips[hostname]
                if time.time() - timestamp < self.IP_CACHE_TTL_SECONDS:
                    return ip
                del self._resolved_ips[hostname]
            return None

    async def _cache_ip(self, hostname: str, ip: str) -> None:
        """Cache resolved IP with timestamp and bounded size"""
        async with self._ip_lock:
            # FIX: If cache is full, remove oldest entries before adding
            if len(self._resolved_ips) >= self.IP_CACHE_MAX_SIZE:
                entries = sorted(self._resolved_ips.items(), key=lambda x: x[1][1])
                to_remove = len(entries) // 4
                for old_hostname, _ in entries[:to_remove]:
                    del self._resolved_ips[old_hostname]
                logging.debug(f"Async IP cache pruned: removed {to_remove} oldest entries")

            self._resolved_ips[hostname] = (ip, time.time())

    async def _cleanup_stale_ips(self, force: bool = False) -> int:
        """Remove stale IP entries."""
        now = time.time()
        if not force and now - self._last_cleanup < self.IP_CACHE_CLEANUP_INTERVAL:
            return 0

        async with self._ip_lock:
            stale = [
                hostname
                for hostname, (_, timestamp) in self._resolved_ips.items()
                if now - timestamp > self.IP_CACHE_TTL_SECONDS
            ]
            for hostname in stale:
                del self._resolved_ips[hostname]

            self._last_cleanup = now

            if stale:
                logging.debug(
                    f"Cleaned {len(stale)} stale async IP entries "
                    f"(cache size: {len(self._resolved_ips)})"
                )
            return len(stale)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._test_mode:
            return await super().handle_async_request(request)
        hostname = request.url.host
        hostname = hostname.strip("[]")

        # Block direct IP connections (SSRF protection)
        try:
            ipaddress.ip_address(hostname)
            raise SecurityError(f"Direct IP connection attempted: {hostname}")
        except ValueError:
            pass

        # ✅ Apply per-IP rate limiting NON-BLOCKING
        if self.rate_limiter:
            if hasattr(self.rate_limiter, "async_wait"):
                await self.rate_limiter.async_wait(hostname)
            else:
                self.rate_limiter.wait(hostname)

        # FIX: Periodic cleanup of stale IPs
        self._request_count += 1
        if self._request_count % 100 == 0:
            await self._cleanup_stale_ips()

        safe_ip = await self._get_cached_ip(hostname)
        if safe_ip is None:
            async with self._ip_lock:
                safe_ip = await self._get_cached_ip(hostname)
                if safe_ip is None:
                    safe_ip = SecurityValidator.resolve_and_validate_hostname(hostname)
                    await self._cache_ip(hostname, safe_ip)

        new_url = request.url.copy_with(host=safe_ip)
        new_headers = request.headers.copy()
        new_headers["Host"] = hostname
        new_extensions = dict(request.extensions) if request.extensions else {}
        new_extensions["sni_hostname"] = hostname

        new_request = httpx.Request(
            method=request.method,
            url=new_url,
            headers=new_headers,
            stream=request.stream,
            extensions=new_extensions,
        )

        return await super().handle_async_request(new_request)

    async def clear_ip_cache(self) -> None:
        """Clear IP cache (for testing/shutdown)."""
        async with self._ip_lock:
            self._resolved_ips.clear()
            logging.debug("Async IP cache cleared")

    def get_ip_cache_stats(self) -> Dict[str, Any]:
        """Get IP cache statistics (sync for convenience)."""
        return {
            "size": len(self._resolved_ips),
            "max_size": self.IP_CACHE_MAX_SIZE,
            "ttl_seconds": self.IP_CACHE_TTL_SECONDS,
        }


__all__ = ["SecureTransport", "SecureAsyncTransport"]
