"""HTTP health-check server and checker.

Migrated from ``mirror_url.py``:
``HealthCheckHandler`` (orig. 9371-9480), ``HealthCheckServer`` (orig. 9482-9533),
``HealthChecker`` (orig. 9655-9711).

Post-migration fixes (circuit-breaker dead API + HTTP protocol + shared state):
- Report circuit-breaker state from ``circuit_breaker_manager`` (the live
  per-domain path), not the deprecated always-``None`` ``circuit_breaker``.
- Build the response body *before* sending headers so error paths never call
  ``send_response`` after ``end_headers`` for JSON-serialization failures.
  Follow-up: that alone didn't cover ``wfile.write()`` itself failing after
  headers were already sent (e.g. a client disconnect mid-response) -- the
  except handlers in ``_handle_health``/``_handle_metrics`` would retry
  ``_send_json``, reintroducing the same double-response bug one layer
  deeper. ``_response_started`` now tracks whether headers already went out,
  and ``_send_error_unless_response_started`` closes the connection instead
  of sending a second status line when they have.
- Bind the mirror instance on the ``HTTPServer`` subclass, not on the handler
  class attribute, so concurrent MirrorURL / test instances do not stomp each
  other.
- Make ``/metrics`` 429 responses consistent with ``/health`` (JSON body +
  Retry-After).
- ``is_healthy()`` reads ``AtomicCounter.value()`` explicitly.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import TYPE_CHECKING, Any, Dict, Optional

from .enums import CircuitBreakerState
from .models import HealthStatus
from .utils import sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .core import MirrorURL


# Default failure threshold for the quick is_healthy() probe. Configurable
# via HealthChecker(..., failure_threshold=N) if callers need a different bar.
DEFAULT_HEALTHY_FAILURE_THRESHOLD = 10


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health-check endpoints with rate limiting.

    The mirror instance is read from ``self.server.mirror_instance`` (set by
    ``HealthCheckServer``), not from a class attribute, so multiple servers
    do not share mutable global state.
    """

    # Class-level rate limiting shared across handler instances of one process.
    # Acceptable for a single health server; multi-server setups still share the
    # budget, which is the safer default (global cap) for localhost probes.
    _rate_limit_lock = threading.Lock()
    _request_times: deque = deque(maxlen=100)
    MAX_REQUESTS_PER_SECOND = 5

    # Class-level default; do_GET() resets this per-request (the same
    # handler instance serves multiple requests on a keep-alive connection).
    # Only meaningful as a fallback if _send_json is ever called before
    # do_GET has run once.
    _response_started = False

    @classmethod
    def check_rate_limit(cls) -> bool:
        """Return True if the request is allowed under the rate limit."""
        now = time.time()
        with cls._rate_limit_lock:
            while cls._request_times and now - cls._request_times[0] > 1.0:
                cls._request_times.popleft()

            if len(cls._request_times) >= cls.MAX_REQUESTS_PER_SECOND:
                return False

            cls._request_times.append(now)
            return True

    @property
    def mirror_instance(self) -> Any:
        """Mirror bound to this server instance (may be None)."""
        return getattr(self.server, "mirror_instance", None)

    def _send_json(
        self,
        status_code: int,
        payload: Dict[str, Any],
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        """Serialize ``payload`` and send a complete HTTP response in one shot.

        Building the body first avoids a mid-write exception trying to call
        ``send_response(500)`` after ``end_headers()`` had already been
        issued for a 200 -- but only for failures in ``json.dumps()``, which
        happens before any header is sent. It does NOT by itself protect
        against ``self.wfile.write(body)`` failing (e.g. the client
        disconnecting mid-response) after headers are already on the wire.
        Callers that wrap a ``_send_json`` call in a try/except and retry
        with a different status on failure must check ``_response_started``
        first (see ``_handle_health`` / ``_handle_metrics``) rather than
        calling ``_send_json`` again unconditionally, or they reintroduce
        the same double-response bug this docstring describes fixing.
        """
        body = json.dumps(payload, indent=2).encode()
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        # From here on, any failure (e.g. a broken pipe from wfile.write())
        # must NOT be followed by another send_response() call -- the status
        # line and headers are already written to the socket.
        self._response_started = True
        self.wfile.write(body)

    def _send_error_unless_response_started(
        self, status_code: int, payload: Dict[str, Any]
    ) -> None:
        """Send an error response, unless a response was already started.

        Used from exception handlers that wrap a successful-path
        ``_send_json`` call: if that call already got past ``end_headers()``
        before failing (e.g. ``wfile.write`` hit a broken pipe), the status
        line and headers are already on the wire and a second
        ``send_response`` would corrupt the HTTP response. In that case we
        can only give up on this response and close the connection instead
        of sending a fabricated second one.
        """
        if self._response_started:
            logging.error(
                "Cannot send %s error response: a response was already "
                "started for this request; closing connection instead",
                status_code,
            )
            self.close_connection = True
            return
        self._send_json(status_code, payload)

    def do_GET(self) -> None:
        """Handle GET requests with rate limiting."""
        self._response_started = False
        if self.path not in ("/health", "/metrics"):
            self.send_response(404)
            self.end_headers()
            return

        if not self.check_rate_limit():
            self._send_json(
                429,
                {"error": "Rate limit exceeded", "retry_after": 1},
                extra_headers={"Retry-After": "1"},
            )
            return

        if self.path == "/health":
            self._handle_health()
        else:
            self._handle_metrics()

    def _handle_health(self) -> None:
        mirror = self.mirror_instance
        if not mirror or not hasattr(mirror, "health_checker"):
            self._send_json(
                503,
                {"status": "unavailable", "error": "Mirror instance not available"},
            )
            return

        try:
            status = mirror.health_checker.get_status()
            # Don't expose internal details beyond the intentional safe subset.
            safe_status = {
                "status": status.status,
                "timestamp": status.timestamp,
                "connection": status.connection if isinstance(status.connection, dict) else {},
                "system": {
                    "memory_usage_mb": (
                        status.system.get("memory_usage_mb", 0)
                        if isinstance(status.system, dict)
                        else 0
                    ),
                    "platform": (
                        status.system.get("platform", "unknown")
                        if isinstance(status.system, dict)
                        else "unknown"
                    ),
                },
            }
            self._send_json(200, safe_status)
        except Exception:
            logging.exception("Health check handler failed")
            self._send_error_unless_response_started(
                500, {"status": "error", "message": "Health check failed"}
            )

    def _handle_metrics(self) -> None:
        mirror = self.mirror_instance
        if not mirror or not hasattr(mirror, "metrics"):
            self._send_json(503, {"status": "unavailable", "error": "Metrics not available"})
            return

        try:
            summary = mirror.metrics.get_summary()
            safe_metrics = {
                "files_downloaded": summary.get("files_downloaded", 0),
                "files_failed": summary.get("files_failed", 0),
                "files_skipped": summary.get("files_skipped", 0),
                "bytes_downloaded": summary.get("bytes_downloaded", 0),
                "elapsed_seconds": summary.get("elapsed_seconds", 0),
            }
            self._send_json(200, safe_metrics)
        except Exception:
            logging.exception("Metrics handler failed")
            self._send_error_unless_response_started(
                500, {"status": "error", "message": "Metrics collection failed"}
            )

    def log_message(self, format: str, *args) -> None:
        """Route BaseHTTPRequestHandler logs through the package logger."""
        try:
            message = format % args
        except Exception:
            message = f"{format} {args!r}"
        logging.debug("HealthCheck: %s", message)


class _MirrorHTTPServer(HTTPServer):
    """HTTPServer that carries the MirrorURL instance and allows port reuse."""

    allow_reuse_address = True

    def __init__(self, server_address, RequestHandlerClass, mirror_instance):
        self.mirror_instance = mirror_instance
        super().__init__(server_address, RequestHandlerClass)


class HealthCheckServer:
    """Simple HTTP server for health checks (bound to localhost)."""

    def __init__(self, mirror_instance, port: int = 8080):
        """
        Args:
            mirror_instance: MirrorURL instance
            port: Port to listen on (localhost only)
        """
        self.mirror_instance = mirror_instance
        self.port = port
        self.server: Optional[_MirrorHTTPServer] = None
        self.thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """Start health check server in a background daemon thread.

        Wraps server creation so a port collision (common in test suites where
        a previous test's daemon thread has not released the port yet) does not
        surface as an unhandled thread exception.
        """

        def run_server() -> None:
            try:
                self.server = _MirrorHTTPServer(
                    ("localhost", self.port),
                    HealthCheckHandler,
                    self.mirror_instance,
                )
                logging.info("Health check server started on port %s", self.port)
                self.server.serve_forever()
            except OSError as e:
                # Most commonly EADDRINUSE.
                logging.warning("Health check server could not start on port %s: %s", self.port, e)

        self.thread = threading.Thread(target=run_server, daemon=True, name="health-check-server")
        self.thread.start()

    def stop(self) -> None:
        """Stop the health check server if it is running."""
        if self.server:
            try:
                self.server.shutdown()
                self.server.server_close()
            except Exception as e:
                logging.debug("Health check server stop error: %s", e)
            finally:
                self.server = None


def _circuit_breaker_summary(connection_manager) -> str:
    """Summarize the live per-domain circuit-breaker manager for health output.

    Returns:
        ``"disabled"`` if no manager is present;
        ``"open"`` if any domain breaker is OPEN;
        ``"half_open"`` if any is HALF_OPEN and none are OPEN;
        ``"closed"`` if all registered breakers are CLOSED (or none registered yet).
    """
    manager = getattr(connection_manager, "circuit_breaker_manager", None)
    if manager is None:
        return "disabled"

    try:
        stats = manager.get_stats()
    except Exception:
        return "disabled"

    if not stats:
        # Manager exists but no domain has been seen yet — treat as closed.
        return "closed"

    states = {info.get("state") for info in stats.values() if isinstance(info, dict)}
    if CircuitBreakerState.OPEN.value in states:
        return "open"
    if CircuitBreakerState.HALF_OPEN.value in states:
        return "half_open"
    return "closed"


def _counter_value(counter) -> int:
    """Read an AtomicCounter or plain int uniformly."""
    if hasattr(counter, "value") and callable(counter.value):
        return int(counter.value())
    return int(counter)


class HealthChecker:
    """Provide health check information for a MirrorURL instance."""

    def __init__(
        self,
        mirror: MirrorURL,
        failure_threshold: int = DEFAULT_HEALTHY_FAILURE_THRESHOLD,
    ):
        """
        Args:
            mirror: MirrorURL instance
            failure_threshold: Max ``files_failed`` still considered healthy
                by ``is_healthy()``
        """
        self.mirror = mirror
        self.start_time = time.time()
        self.check_count = 0
        self.failure_threshold = failure_threshold

    def get_status(self) -> HealthStatus:
        """Get current health status."""
        self.check_count += 1
        memory_monitor = getattr(self.mirror, "memory_monitor", None)
        disk_manager = getattr(self.mirror, "disk_manager", None)
        performance_monitor = getattr(self.mirror, "performance_monitor", None)
        connection_manager = getattr(self.mirror, "connection_manager", None)

        files_processed = _counter_value(self.mirror.files_processed)
        files_failed = _counter_value(self.mirror.files_failed)
        files_skipped = _counter_value(self.mirror.files_skipped)
        total_downloaded = _counter_value(self.mirror.total_downloaded_size)

        return HealthStatus(
            status="healthy" if self.mirror.connection_ok else "degraded",
            timestamp=datetime.now().isoformat(),
            metrics={
                "files_processed": files_processed,
                "files_failed": files_failed,
                "files_skipped": files_skipped,
                "total_downloaded_mb": total_downloaded / (1024 * 1024),
                "uptime_seconds": time.time() - self.mirror.start_time,
                "health_checks": self.check_count,
            },
            connection={
                "base_url": sanitize_url_for_log(self.mirror.base_url),
                "ok": self.mirror.connection_ok,
                "circuit_breaker": _circuit_breaker_summary(connection_manager),
            },
            cache=(
                self.mirror.cache_manager.lru_file_cache.get_stats()
                if hasattr(self.mirror.cache_manager, "lru_file_cache")
                else {}
            ),
            errors=self.mirror.metrics.metrics.get("errors", [])[-10:],
            system={
                "memory_usage_mb": memory_monitor.get_usage_mb() if memory_monitor else 0,
                "disk_usage": disk_manager.get_usage_stats() if disk_manager else {},
                "performance": performance_monitor.get_summary() if performance_monitor else {},
                "python_version": sys.version.split()[0],
                "platform": sys.platform,
            },
        )

    def is_healthy(self) -> bool:
        """Quick health check: connection OK and failures under threshold."""
        return (
            self.mirror.connection_ok
            and _counter_value(self.mirror.files_failed) < self.failure_threshold
        )


__all__ = ["HealthCheckHandler", "HealthCheckServer", "HealthChecker"]
