"""Synchronous connection pool + manager.

Migrated verbatim from ``mirror_url.py``:
``ConnectionPool`` (orig. 5645-5985), ``ConnectionManager`` (orig. 6260-6685).

One behavior-preserving fix to a migration hazard: ``_is_url_within_scope``
imported ``MirrorURL`` from the top-level ``mirror_url`` module (the monolith's
self-import). That now resolves to the intra-package ``.core`` module instead.
The import remains lazy (inside the method) so it works once ``core`` is
populated and avoids an import cycle.
"""

from __future__ import annotations

import logging
import random
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import RLock, Semaphore
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from urllib.parse import quote, unquote, urljoin, urlparse

import httpx

from .circuit_breaker import CircuitBreakerManager
from .compat import Str
from .concurrency import UnifiedConcurrencyManager
from .constants import DEFAULT_TIMEOUT, MAX_CONNECTION_POOLS
from .enums import ConcurrencyType
from .exceptions import (
    ConcurrencyLimitError,
    MirrorConnectionError,
    SecurityError,
    URLScopeError,
)
from .rate_limiter import PerIPRateLimiter, RateLimiter
from .security import SecurityValidator
from .transport import SecureTransport
from .utils import exponential_backoff, sanitize_url_for_log, trim_url

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig
    from .metrics import MetricsCollector


class ConnectionPool:
    """Manage connection pools for better resource usage with proper reuse"""

    def __init__(self, max_pools: int = MAX_CONNECTION_POOLS, config: MirrorConfig = None):
        """
        Initialize connection pool.

        Args:
            max_pools: Maximum number of connection pools
            config: MirrorConfig instance
        """
        self.pools: Dict[str, httpx.Client] = {}
        self.max_pools = max_pools
        self.lock = RLock()
        self.config = config
        self._session_counter = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._creation_count = 0
        self.rate_limiter = PerIPRateLimiter() if config and config.security_validation else None

        # FIX: Track connection usage per pool
        self.pool_usage: Dict[str, int] = {}
        self.last_used: Dict[str, float] = {}

        logging.debug(f"ConnectionPool initialized: max_pools={max_pools}")

    def get_client(self, base_url: str) -> httpx.Client:
        """
        Get or create HTTP client for base URL with proper connection reuse.
        Uses simplified double-checked locking to prevent thread explosion and deadlocks.
        Args:
            base_url: Base URL for connection pool
        Returns:
            HTTP client instance with connection pooling
        """
        parsed = urlparse(base_url)
        pool_key = f"{parsed.scheme}://{parsed.netloc}"

        # Fast path - check without lock for read-heavy workloads
        if pool_key in self.pools:
            with self.lock:
                # Double-check under lock for thread safety and stats accuracy
                if pool_key in self.pools:
                    self._hits += 1
                    self.pool_usage[pool_key] = self.pool_usage.get(pool_key, 0) + 1
                    self.last_used[pool_key] = time.time()
                    return self.pools[pool_key]

        # Slow path - create new pool under lock
        with self.lock:
            # Re-check after acquiring lock (another thread may have created it)
            if pool_key in self.pools:
                self._hits += 1
                self.pool_usage[pool_key] = self.pool_usage.get(pool_key, 0) + 1
                self.last_used[pool_key] = time.time()
                return self.pools[pool_key]

            # Pool definitely doesn't exist
            self._misses += 1

            # Evict oldest pool if we're at capacity
            if len(self.pools) >= self.max_pools:
                self._evict_oldest_pool()

            # Create new client with optimized settings
            client = self._create_client()
            self.pools[pool_key] = client
            self.pool_usage[pool_key] = 1
            self.last_used[pool_key] = time.time()
            self._creation_count += 1
            self._session_counter += 1

            logging.debug(f"Created new connection pool for {pool_key} (total: {len(self.pools)})")
            return client

    def _create_client(self) -> httpx.Client:
        """Create new HTTP client with optimized connection pooling"""
        # Much larger connection limits for parallel downloads
        limits = httpx.Limits(
            max_connections=100,  # Increased from 20
            max_keepalive_connections=50,  # Increased from 10
            keepalive_expiry=120.0,  # Increased from 60
        )

        # Longer timeouts for large files
        timeout = httpx.Timeout(
            self.config.timeout if self.config else DEFAULT_TIMEOUT,
            connect=30.0,  # Increased from 10
            read=self.config.timeout * 3
            if self.config
            else DEFAULT_TIMEOUT * 3,  # Increased multiplier
        )

        # Create client with keep-alive and connection reuse
        client = httpx.Client(
            http2=self.config.http2 if self.config else True,
            limits=limits,
            timeout=timeout,
            follow_redirects=True,
            transport=SecureTransport(rate_limiter=self.rate_limiter),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Connection": "keep-alive",
                "Keep-Alive": "timeout=120, max=1000",  # Explicit keep-alive
                "Upgrade-Insecure-Requests": "1",
            },
        )

        return client

    def _evict_oldest_pool(self) -> None:
        """Evict the oldest or least used connection pool"""
        if not self.pools:
            return

        # Smart eviction - remove least recently used pool
        if self.last_used:
            oldest_key = min(self.last_used.items(), key=lambda x: x[1])[0]
        else:
            oldest_key = next(iter(self.pools))

        try:
            # Close all connections in the pool
            old_client = self.pools[oldest_key]
            old_client.close()
            logging.debug(f"Closed connection pool for {oldest_key}")
        except Exception as e:
            logging.debug(f"Error closing pool {oldest_key}: {e}")

        # Clean up tracking. By construction every key in pool_usage /
        # last_used is also in self.pools (see get_client), so popping here
        # keeps the three dicts in sync and bounded at max_pools.
        del self.pools[oldest_key]
        self.pool_usage.pop(oldest_key, None)
        self.last_used.pop(oldest_key, None)
        self._evictions += 1

        logging.debug(f"Evicted connection pool for {oldest_key} (evictions: {self._evictions})")

    def warm_up(self, urls: List[str]) -> None:
        """
        Pre-warm connections for a list of URLs.

        This establishes connections before downloads start to avoid
        connection setup overhead during critical download time.

        Args:
            urls: List of URLs to warm up connections for
        """
        if not urls:
            return

        # Group by domain
        domains = {}
        for url in urls[:20]:  # Limit to 20 URLs for warm-up
            try:
                parsed = urlparse(url)
                domain = f"{parsed.scheme}://{parsed.netloc}"
                if domain not in domains:
                    domains[domain] = []
                domains[domain].append(url)
            except Exception:
                continue

        if not domains:
            return

        logging.info(f"🔥 Pre-warming {len(domains)} connection pools")

        # Warm up each domain with a few connections
        with ThreadPoolExecutor(max_workers=min(len(domains), 10)) as executor:
            futures = []
            for domain, domain_urls in domains.items():
                futures.append(executor.submit(self._warm_domain, domain, domain_urls[:3]))

            for future in as_completed(futures):
                try:
                    future.result(timeout=15)
                except Exception as e:
                    logging.debug(f"Warm-up failed for a domain: {e}")

        logging.info("✅ Connection pool warm-up complete")

    def _warm_domain(self, domain: str, urls: List[str]) -> None:
        """
        Warm up a specific domain by establishing connections.

        Args:
            domain: Domain to warm up
            urls: Sample URLs from this domain
        """
        try:
            # Get or create client for this domain
            client = self.get_client(urls[0])

            # Establish connections by making HEAD requests
            for url in urls:
                try:
                    # Actually send HEAD request to establish connection
                    response = client.head(url, timeout=5.0)
                    response.close()  # Close response but keep connection alive
                    logging.debug(f"Warmed up connection to {url}")
                except Exception as e:
                    logging.debug(f"Failed to warm up {url}: {e}")
                    continue

        except Exception as e:
            logging.debug(f"Failed to warm up domain {domain}: {e}")

    def close_all(self) -> None:
        """Close all connection pools and clean up resources"""
        with self.lock:
            pool_count = len(self.pools)
            for domain, client in self.pools.items():
                try:
                    client.close()
                    logging.debug(f"Closed connection pool for {domain}")
                except Exception as e:
                    logging.debug(f"Error closing pool for {domain}: {e}")

            self.pools.clear()
            self.pool_usage.clear()
            self.last_used.clear()
            logging.info(f"Closed all {pool_count} connection pools")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive connection pool statistics.

        Returns:
            Dictionary with connection pool statistics
        """
        with self.lock:
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0

            # Get pool-specific stats
            pool_details = {}
            for domain, client in self.pools.items():
                pool_details[domain] = {
                    "usage_count": self.pool_usage.get(domain, 0),
                    "last_used": self.last_used.get(domain, 0),
                    "age_seconds": time.time() - self.last_used.get(domain, time.time())
                    if domain in self.last_used
                    else 0,
                }

            return {
                "pools": len(self.pools),
                "max_pools": self.max_pools,
                "hits": self._hits,
                "misses": self._misses,
                "total_requests": total_requests,
                "hit_rate": f"{hit_rate:.1f}%",
                "evictions": self._evictions,
                "creations": self._creation_count,
                "active_sessions": self._session_counter,
                "pool_details": pool_details,
                "rate_limiter": self.rate_limiter.get_stats() if self.rate_limiter else None,
            }

    def get_pool(self, domain: str) -> Optional[httpx.Client]:
        """
        Get a specific pool by domain without creating a new one.

        Args:
            domain: Domain to get pool for (e.g., 'https://example.com')

        Returns:
            HTTP client or None if not found
        """
        with self.lock:
            return self.pools.get(domain)

    def has_pool(self, domain: str) -> bool:
        """
        Check if a pool exists for the given domain.

        Args:
            domain: Domain to check

        Returns:
            True if pool exists
        """
        with self.lock:
            return domain in self.pools

    def clear_idle_pools(self, idle_timeout: float = 300.0) -> int:
        """
        Clear pools that have been idle for too long.

        Args:
            idle_timeout: Maximum idle time in seconds

        Returns:
            Number of pools cleared
        """
        cleared = 0
        now = time.time()

        with self.lock:
            idle_domains = [
                domain for domain, last in self.last_used.items() if now - last > idle_timeout
            ]

            for domain in idle_domains:
                if domain in self.pools:
                    try:
                        self.pools[domain].close()
                        del self.pools[domain]
                        self.pool_usage.pop(domain, None)
                        self.last_used.pop(domain, None)
                        cleared += 1
                        logging.debug(f"Cleared idle pool for {domain}")
                    except Exception as e:
                        logging.debug(f"Error clearing idle pool {domain}: {e}")

        if cleared > 0:
            logging.info(f"Cleared {cleared} idle connection pools")

        return cleared

    def resize_pools(self, new_max_pools: int) -> None:
        """
        Dynamically resize the maximum number of pools.

        Args:
            new_max_pools: New maximum number of pools
        """
        with self.lock:
            old_max = self.max_pools
            self.max_pools = new_max_pools

            # If we're reducing size, evict excess pools
            if new_max_pools < len(self.pools):
                excess = len(self.pools) - new_max_pools
                for _ in range(excess):
                    self._evict_oldest_pool()

            logging.info(f"Resized connection pools: {old_max} → {new_max_pools}")


# ============================================================================
# CONNECTION MANAGER
# ============================================================================
class ConnectionManager:
    """Manages HTTP connections with security and rate limiting"""

    def __init__(
        self,
        config: MirrorConfig,
        metrics: MetricsCollector,
        concurrency_manager: Optional[UnifiedConcurrencyManager] = None,
    ):
        """
        Initialize connection manager.

        Args:
            config: MirrorConfig instance
            metrics: MetricsCollector instance
            concurrency_manager: Optional concurrency manager (created if not provided)
        """
        self.config = config
        self.metrics = metrics

        # FIX: Create concurrency manager if not provided
        if concurrency_manager is None:
            self.concurrency_manager = UnifiedConcurrencyManager()
            self.concurrency_manager.start()
        else:
            self.concurrency_manager = concurrency_manager

        self.connection_pool = ConnectionPool(config=config)
        self.rate_limiter = RateLimiter(
            delay=config.request_delay, per_ip=config.security_validation
        )
        self.request_semaphore = Semaphore(20)
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        self.base_url = config.base_url
        self.base_parsed = urlparse(str(config.base_url))
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()

    def _validate_url_scheme(self, url: str) -> bool:
        """Validate URL scheme using fast method."""
        # Use the fast version from MirrorURL if available, or implement here
        url_sz = Str(url)
        if url_sz.startswith(Str("http://")):
            return True
        if url_sz.startswith(Str("https://")):
            return True
        return False

    def _get_url_path_fast(self, url: str) -> Str:
        """Fast path extraction using StringZilla."""
        url_sz = Str(url)
        # Find the path part after the domain
        after_protocol = url_sz.find("://")
        if after_protocol < 0:
            return Str("")

        path_start = url_sz.find("/", after_protocol + 3)
        if path_start < 0:
            return Str("")

        return url_sz[path_start:]

    def _is_url_within_scope(self, url: str, check_base: bool = True) -> bool:
        """
        Optimized URL scope checking using StringZilla.

        Validates that a URL is within the configured base scope.
        Prevents path traversal and ensures security boundaries.
        """
        try:
            # Use the static method from MirrorURL for fast scheme validation
            from .core import MirrorURL

            if not MirrorURL._validate_url_scheme_fast(url):
                logging.debug(f"URL scope check failed: invalid scheme for {url}")
                return False

            # Fast path extraction using StringZilla
            url_path = self._get_url_path_fast(url)
            if not url_path:
                logging.debug(f"URL scope check failed: no path for {url}")
                return False

            # Get scope path
            if check_base:
                scope_path = self.base_parsed.path
            else:
                if not self.target_parsed:
                    return False
                scope_path = self.target_parsed.path

            # Normalize scope path: ensure it's not None and handle root
            if not scope_path:
                scope_path = "/"

            # Ensure scope_path ends with / for proper prefix matching
            # This prevents /files matching /files_secure
            if not scope_path.endswith("/"):
                scope_path = scope_path + "/"

            # Convert to string for comparison
            url_path_str = str(url_path)

            # Check if url_path starts with scope_path
            if not url_path_str.startswith(scope_path):
                # Special case: root scope matches everything
                if scope_path != "/":
                    logging.debug(f"URL {url} outside scope {scope_path}")
                    return False

            # Get remaining path after scope for security checks
            remaining = (
                url_path_str[len(scope_path) :] if len(scope_path) < len(url_path_str) else ""
            )

            # Fast path traversal detection using StringZilla
            remaining_sz = Str(remaining)
            if remaining_sz.find("..") >= 0:
                logging.warning(f"Path traversal attempt in URL: {sanitize_url_for_log(url)}")
                self.metrics.increment("security_blocks")
                return False

            # Check for dot segments (current directory references)
            if remaining_sz.find("/.") >= 0 or remaining_sz.find("./") >= 0:
                logging.warning(f"Current directory reference in URL: {sanitize_url_for_log(url)}")
                self.metrics.increment("security_blocks")
                return False

            # Check for encoded path traversal
            remaining_str = str(remaining_sz)
            if "%2e" in remaining_str.lower() or "%2f" in remaining_str.lower():
                try:
                    decoded = unquote(remaining_str)
                    if ".." in decoded or "/." in decoded:
                        logging.warning(
                            f"Encoded path traversal in URL: {sanitize_url_for_log(url)}"
                        )
                        self.metrics.increment("security_blocks")
                        return False
                except Exception:
                    pass

            return True

        except Exception as e:
            logging.debug(f"Error in URL scope check: {e}")
            return False

    def _normalize_url(self, url: str) -> str:
        """Normalize URL"""
        parsed = urlparse(url)
        path = parsed.path
        if "%" in path:
            try:
                path = unquote(path)
            except Exception:
                pass
        path = quote(path, safe="/%")
        normalized = parsed._replace(path=path, fragment="").geturl()
        return normalized

    # Maximum number of HTTP redirects we'll follow in one request() call.
    # The previous implementation followed redirects via unbounded recursion
    # (`return self.request(...)`), so a malicious or misconfigured server
    # could trigger RecursionError or stack exhaustion. Bound it explicitly.
    _MAX_REDIRECTS = 10

    def request(
        self,
        url: str,
        method: str = "GET",
        allow_redirects: bool = True,
        _redirect_depth: int = 0,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Make HTTP request with security and retry logic.
        Args:
            url: URL to request
            method: HTTP method
            allow_redirects: Whether to follow redirects
            _redirect_depth: Internal counter — number of redirects already
                followed for this logical request. Callers should not set
                this; it's incremented when the manual redirect handler
                re-enters request().
            **kwargs: Additional arguments for request
        Returns:
            HTTP response
        Raises:
            SecurityError: If security validation fails
            MirrorConnectionError: If circuit breaker is open
            URLScopeError: If URL is outside scope
        """

        # Acquire thread slot from concurrency manager
        thread_acquired = False
        try:
            if self.concurrency_manager:
                thread_acquired = self.concurrency_manager.acquire_thread(ConcurrencyType.SYNC)
                if not thread_acquired:
                    raise ConcurrencyLimitError("Thread limit reached, cannot process request")
            if self.config.security_validation:
                is_safe, error_msg = SecurityValidator.validate_url_security(
                    url, str(self.base_url)
                )
                if not is_safe:
                    self.metrics.increment("security_blocks")
                    raise SecurityError(f"Security validation failed: {error_msg}")
            # Get domain for circuit breaker
            parsed_url = urlparse(url)
            domain = parsed_url.netloc

            if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(
                domain
            ):
                self.metrics.increment("circuit_breaker_trips")
                raise MirrorConnectionError(f"Circuit breaker is open for domain {domain}")
            normalized_url = trim_url(self._normalize_url(url))

            # Call the scope check
            is_within = self._is_url_within_scope(normalized_url)

            if not is_within:
                # FIX: Check for path traversal specifically
                if ".." in url or "%2e" in url.lower():
                    raise URLScopeError(f"Path traversal detected: {sanitize_url_for_log(url)}")
                else:
                    raise URLScopeError("Attempted to access URL outside configured base URL scope")

            with self.request_semaphore:
                if self.consecutive_failures >= self.max_consecutive_failures:
                    wait_time = exponential_backoff(
                        self.consecutive_failures - self.max_consecutive_failures
                    )
                    logging.warning(
                        f"Too many consecutive failures ({self.consecutive_failures}). Waiting {wait_time:.1f}s..."
                    )
                    time.sleep(wait_time)
                    self.consecutive_failures = 0

                # Get IP for rate limiting with error handling
                parsed = urlparse(normalized_url)
                try:
                    if parsed.hostname:
                        ip = socket.gethostbyname(parsed.hostname)
                    else:
                        ip = "unknown"
                except Exception:
                    ip = "unknown"
                if ip != "unknown":
                    self.rate_limiter.wait(ip)
                time.sleep(random.uniform(0, 0.02))

                # FIX: Preserve custom timeout if provided
                custom_timeout = kwargs.pop("timeout", None)
                # Capture the CALLER-supplied headers once, before the retry
                # loop pops them out of kwargs. Used to correctly forward
                # Range / If-None-Match etc. across a redirect (see below).
                caller_headers = dict(kwargs.get("headers") or {})

                for attempt in range(self.config.max_retries + 1):
                    try:
                        client = self.connection_pool.get_client(normalized_url)
                        try:
                            request_headers = client.headers.copy()
                        except (AttributeError, TypeError):
                            request_headers = {}
                        if "headers" in kwargs:
                            try:
                                request_headers.update(kwargs.pop("headers"))
                            except Exception:
                                request_headers = dict(kwargs.pop("headers"))

                        # FIX: Use custom timeout or default
                        if custom_timeout:
                            timeout = custom_timeout
                        else:
                            timeout = httpx.Timeout(
                                self.config.timeout, connect=10.0, read=self.config.timeout * 2
                            )

                        logging.debug(
                            f"HTTP Request: {method} {sanitize_url_for_log(normalized_url)} (attempt {attempt + 1})"
                        )
                        start = time.time()
                        # We always disable httpx auto-follow so we can validate the
                        # redirect target ourselves (scope + security) before fetching it.
                        try:
                            headers_to_send = dict(request_headers)
                        except Exception:
                            headers_to_send = {}
                        response = client.request(
                            method,
                            normalized_url,
                            timeout=timeout,
                            follow_redirects=False,
                            headers=headers_to_send,
                            **kwargs,
                        )
                        self.metrics.add_request_time(time.time() - start)

                        # Manual redirect handling so we can enforce scope/security on Location
                        status_code = getattr(response, "status_code", None)
                        if (
                            allow_redirects
                            and isinstance(status_code, int)
                            and 300 <= status_code < 400
                        ):
                            redirect_url = (
                                response.headers.get("Location")
                                if hasattr(response, "headers")
                                else None
                            )
                            if redirect_url:
                                # FIX (unbounded recursion): cap how many
                                # times we'll follow Location: . The old code
                                # did `return self.request(...)` which could
                                # blow the Python stack on a redirect loop.
                                if _redirect_depth >= self._MAX_REDIRECTS:
                                    self.metrics.increment("redirect_loop_aborted")
                                    raise MirrorConnectionError(
                                        f"Too many redirects ({_redirect_depth}) for "
                                        f"{sanitize_url_for_log(url)}"
                                    )
                                resolved_url = urljoin(normalized_url, redirect_url)
                                resolved_normalized = trim_url(self._normalize_url(resolved_url))
                                if self.config.security_validation:
                                    is_safe, error_msg = SecurityValidator.validate_url_security(
                                        resolved_normalized, str(self.base_url)
                                    )
                                    if not is_safe:
                                        self.metrics.increment("security_blocks")
                                        raise SecurityError(f"Redirect blocked: {error_msg}")
                                if not self._is_url_within_scope(resolved_normalized):
                                    raise URLScopeError(
                                        f"Redirect outside scope: {sanitize_url_for_log(resolved_normalized)}"
                                    )
                                logging.debug(
                                    f"Following redirect to: {sanitize_url_for_log(resolved_normalized)}"
                                )
                                # FIX: `timeout` (popped into custom_timeout at
                                # the top of the method) and `headers` (popped
                                # into request_headers inside this loop) are no
                                # longer in **kwargs, so the recursive redirect
                                # call previously dropped BOTH — a redirected
                                # ranged/conditional request lost its Range /
                                # If-None-Match headers and reverted to the
                                # default timeout. Re-thread them explicitly.
                                redirect_kwargs = dict(kwargs)
                                if custom_timeout is not None:
                                    redirect_kwargs["timeout"] = custom_timeout
                                # Forward only the caller's original headers
                                # (Range, If-None-Match, ...) — NOT the source
                                # client's default headers, which the recursive
                                # call re-derives from the redirect target's own
                                # client.
                                if caller_headers:
                                    redirect_kwargs["headers"] = dict(caller_headers)
                                return self.request(
                                    resolved_normalized,
                                    method,
                                    allow_redirects,
                                    _redirect_depth=_redirect_depth + 1,
                                    **redirect_kwargs,
                                )

                        # Retry on 5xx server errors
                        if isinstance(status_code, int) and 500 <= status_code < 600:
                            self.consecutive_failures += 1
                            if attempt == self.config.max_retries:
                                if self.circuit_breaker_manager:
                                    self.circuit_breaker_manager.record_failure(domain)
                                self.metrics.add_error(
                                    f"HTTP {status_code} for {sanitize_url_for_log(url)}",
                                    "request_error",
                                )
                                raise MirrorConnectionError(
                                    f"Request failed after {attempt + 1} attempts with HTTP {status_code}"
                                )
                            wait_time = exponential_backoff(attempt, self.config.retry_delay)
                            logging.warning(
                                f"HTTP {status_code} (attempt {attempt + 1}), retrying in {wait_time:.1f}s"
                            )
                            time.sleep(wait_time)
                            continue

                        # 4xx and other final responses - raise_for_status to surface.
                        #
                        # Exceptions: 416 Range Not Satisfiable and 304 Not
                        # Modified are special — they're expected, actionable
                        # outcomes, not errors.
                        #   - 416 carries Content-Range with the actual file
                        #     size, which the resume code path needs to decide
                        #     whether the local partial is already complete.
                        #   - 304 is the normal "up to date" result of a
                        #     conditional GET/HEAD (If-None-Match /
                        #     If-Modified-Since). httpx's raise_for_status()
                        #     treats any non-2xx, including 304, as an error
                        #     (is_success is only true for 2xx), so without
                        #     this carve-out every conditional revalidation
                        #     that correctly finds an up-to-date file raises
                        #     HTTPStatusError instead of returning the 304
                        #     response to the caller (see
                        #     file_exists_and_up_to_date / the async metadata
                        #     pipeline, which both check `status_code == 304`
                        #     directly on the returned response).
                        if status_code != 416 and status_code != 304:
                            try:
                                response.raise_for_status()
                            except httpx.HTTPStatusError as e:
                                self.consecutive_failures += 1
                                if self.circuit_breaker_manager:
                                    self.circuit_breaker_manager.record_failure(domain)
                                self.metrics.add_error(str(e), "request_error")
                                raise

                        self.consecutive_failures = 0
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_success(domain)
                        return response
                    # replaced commented block
                    except (
                        httpx.ConnectError,
                        httpx.TimeoutException,
                        httpx.ReadError,
                        httpx.RequestError,
                    ) as e:
                        self.consecutive_failures += 1
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            logging.error(
                                f"Request failed after {self.config.max_retries} retries: {e}"
                            )
                            self.metrics.add_error(str(e), "request_error")
                            raise MirrorConnectionError(f"Request failed: {e}")
                        wait_time = exponential_backoff(attempt, self.config.retry_delay)
                        logging.warning(
                            f"Request failed (attempt {attempt + 1}), retrying in {wait_time:.1f}s: {e}"
                        )
                        time.sleep(wait_time)
        finally:
            # Release thread slot if acquired
            if thread_acquired and self.concurrency_manager:
                self.concurrency_manager.release_thread()

    def close(self) -> None:
        """Close all connections"""
        self.connection_pool.close_all()


__all__ = ["ConnectionPool", "ConnectionManager"]
