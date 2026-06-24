"""HTTP health-check server and checker.

Migrated verbatim from ``mirror_url.py``:
``HealthCheckHandler`` (orig. 9371-9480), ``HealthCheckServer`` (orig. 9482-9533),
``HealthChecker`` (orig. 9655-9711).
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
from typing import TYPE_CHECKING

from .models import HealthStatus
from .utils import sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .core import MirrorURL


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoint - WITH RATE LIMITING"""

    mirror_instance = None

    # Class-level rate limiting
    _rate_limit_lock = threading.Lock()
    _request_times = deque(maxlen=100)
    MAX_REQUESTS_PER_SECOND = 5

    @classmethod
    def check_rate_limit(cls) -> bool:
        """Check if request should be rate limited."""
        now = time.time()
        with cls._rate_limit_lock:
            # Remove old entries
            while cls._request_times and now - cls._request_times[0] > 1.0:
                cls._request_times.popleft()

            if len(cls._request_times) >= cls.MAX_REQUESTS_PER_SECOND:
                return False

            cls._request_times.append(now)
            return True

    def do_GET(self):
        """Handle GET requests with rate limiting."""
        if self.path == "/health":
            # Apply rate limiting
            if not self.check_rate_limit():
                self.send_response(429)  # Too Many Requests
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"error": "Rate limit exceeded", "retry_after": 1}).encode()
                )
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.end_headers()

            if self.mirror_instance and hasattr(self.mirror_instance, "health_checker"):
                try:
                    status = self.mirror_instance.health_checker.get_status()
                    # Don't expose internal details in health check
                    safe_status = {
                        "status": status.status,
                        "timestamp": status.timestamp,
                        "connection": status.connection
                        if isinstance(status.connection, dict)
                        else {},
                        "system": {
                            "memory_usage_mb": status.system.get("memory_usage_mb", 0)
                            if isinstance(status.system, dict)
                            else 0,
                            "platform": status.system.get("platform", "unknown")
                            if isinstance(status.system, dict)
                            else "unknown",
                        },
                    }
                    self.wfile.write(json.dumps(safe_status, indent=2).encode())
                except Exception:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"status": "error", "message": "Health check failed"}).encode()
                    )
            else:
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps(
                        {"status": "unavailable", "error": "Mirror instance not available"}
                    ).encode()
                )
        elif self.path == "/metrics":
            # Simple metrics endpoint (if needed)
            if not self.check_rate_limit():
                self.send_response(429)
                self.end_headers()
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()

            if self.mirror_instance and hasattr(self.mirror_instance, "metrics"):
                try:
                    summary = self.mirror_instance.metrics.get_summary()
                    safe_metrics = {
                        "files_downloaded": summary.get("files_downloaded", 0),
                        "files_failed": summary.get("files_failed", 0),
                        "files_skipped": summary.get("files_skipped", 0),
                        "bytes_downloaded": summary.get("bytes_downloaded", 0),
                        "elapsed_seconds": summary.get("elapsed_seconds", 0),
                    }
                    self.wfile.write(json.dumps(safe_metrics, indent=2).encode())
                except Exception:
                    self.send_response(500)
                    self.end_headers()
            else:
                self.send_response(503)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Override to use our logging instead of stderr."""
        logging.debug(f"HealthCheck: {args[0]}" % args[1:] if len(args) > 1 else format % args)


class HealthCheckServer:
    """Simple HTTP server for health checks"""

    def __init__(self, mirror_instance, port: int = 8080):
        """
        Initialize health check server.

        Args:
            mirror_instance: MirrorURL instance
            port: Port to listen on
        """
        self.mirror_instance = mirror_instance
        self.port = port
        self.server = None
        self.thread = None

        # Set the mirror instance for the handler
        HealthCheckHandler.mirror_instance = mirror_instance

    def start(self):
        """Start health check server in background thread.

        Wraps server creation in try/except so a port collision (common in
        test suites where a previous test's daemon thread hasn't released
        port 8080 yet) doesn't surface as an unhandled thread exception.
        Sets SO_REUSEADDR to make the port bindable again sooner after a
        previous instance shuts down.
        """

        def run_server():
            try:
                # Allow re-bind shortly after a prior shutdown
                class _ReusableHTTPServer(HTTPServer):
                    allow_reuse_address = True

                self.server = _ReusableHTTPServer(("localhost", self.port), HealthCheckHandler)
                logging.info(f"Health check server started on port {self.port}")
                self.server.serve_forever()
            except OSError as e:
                # Most commonly EADDRINUSE — port already bound by another
                # instance (likely a leftover from a previous test). Log and
                # bail rather than killing the test process with an
                # uncaught thread exception.
                logging.warning(f"Health check server could not start on port {self.port}: {e}")

        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()

    def stop(self):
        """Stop health check server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()


class HealthChecker:
    """Provide health check information"""

    def __init__(self, mirror: MirrorURL):
        """
        Initialize health checker.

        Args:
            mirror: MirrorURL instance
        """
        self.mirror = mirror
        self.start_time = time.time()
        self.check_count = 0

    def get_status(self) -> HealthStatus:
        """Get current health status"""
        self.check_count += 1
        memory_monitor = getattr(self.mirror, "memory_monitor", None)
        disk_manager = getattr(self.mirror, "disk_manager", None)
        performance_monitor = getattr(self.mirror, "performance_monitor", None)

        return HealthStatus(
            status="healthy" if self.mirror.connection_ok else "degraded",
            timestamp=datetime.now().isoformat(),
            metrics={
                "files_processed": self.mirror.files_processed.value()
                if hasattr(self.mirror.files_processed, "value")
                else self.mirror.files_processed,
                "files_failed": self.mirror.files_failed.value()
                if hasattr(self.mirror.files_failed, "value")
                else self.mirror.files_failed,
                "files_skipped": self.mirror.files_skipped.value()
                if hasattr(self.mirror.files_skipped, "value")
                else self.mirror.files_skipped,
                "total_downloaded_mb": (
                    self.mirror.total_downloaded_size.value()
                    if hasattr(self.mirror.total_downloaded_size, "value")
                    else self.mirror.total_downloaded_size
                )
                / (1024 * 1024),
                "uptime_seconds": time.time() - self.mirror.start_time,
                "health_checks": self.check_count,
            },
            connection={
                "base_url": sanitize_url_for_log(self.mirror.base_url),
                "ok": self.mirror.connection_ok,
                "circuit_breaker": (
                    self.mirror.connection_manager.circuit_breaker.state.value
                    if (
                        self.mirror.connection_manager
                        and self.mirror.connection_manager.circuit_breaker
                    )
                    else "disabled"
                ),
            },
            cache=self.mirror.cache_manager.lru_file_cache.get_stats()
            if hasattr(self.mirror.cache_manager, "lru_file_cache")
            else {},
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
        """Quick health check"""
        return self.mirror.connection_ok and self.mirror.files_failed < 10


__all__ = ["HealthCheckHandler", "HealthCheckServer", "HealthChecker"]
