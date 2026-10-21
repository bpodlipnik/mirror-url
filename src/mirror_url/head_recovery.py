"""Reporting for HEAD connections closed before any HTTP response arrives."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import httpx

from .utils import sanitize_url_for_log

if TYPE_CHECKING:
    from .metrics import MetricsCollector

HEAD_RECOVERY_METRICS = (
    ("head_early_disconnects", "HEAD early disconnects"),
    ("head_disconnect_recoveries", "HEAD requests recovered after disconnect"),
    ("head_disconnect_exhausted", "HEAD requests exhausted after disconnect"),
)


class HeadDisconnectRecovery:
    """Track one request without labelling every protocol error as an idle close.

    HTTPX does not expose the TLS alert or whether the connection was reused.
    Only httpcore's explicit no-response EOF is eligible for the prompt retry;
    malformed responses and other transport errors retain their normal policy.
    """

    def __init__(self, metrics: MetricsCollector, url: str, method: str = "HEAD") -> None:
        self.metrics = metrics
        self.url = sanitize_url_for_log(url)
        self.is_head = method.upper() == "HEAD"
        self.disconnects = 0
        self.finished = False

    def observe(self, error: BaseException) -> bool:
        disconnect = (
            self.is_head
            and isinstance(error, httpx.RemoteProtocolError)
            and str(error) == "Server disconnected without sending a response."
        )
        if disconnect:
            self.disconnects += 1
            self.metrics.increment("head_early_disconnects")
        return disconnect

    def log_retry(self, attempt: int, delay: float) -> None:
        logging.log(
            logging.INFO if attempt == 0 else logging.WARNING,
            "Request failed for HEAD %s (attempt %d, RemoteProtocolError; "
            "connection closed before response), retrying in %.1fs",
            self.url,
            attempt + 1,
            delay,
        )

    def recovered(self, status: int, attempts: int) -> bool:
        if not self.disconnects or self.finished:
            return False
        self.finished = True
        self.metrics.increment("head_disconnect_recoveries")
        logging.info(
            "Request recovered: HEAD %s returned HTTP %d after %d attempts",
            self.url,
            status,
            attempts,
        )
        return True

    def exhausted(self, attempts: int) -> bool:
        if not self.disconnects or self.finished:
            return False
        self.finished = True
        self.metrics.increment("head_disconnect_exhausted")
        logging.error(
            "HEAD request exhausted retry budget after an early disconnect: %s (%d attempts)",
            self.url,
            attempts,
        )
        return True
