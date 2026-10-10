"""Per-sync reporting, independent of transfer and freshness policy."""

from __future__ import annotations

import time
from collections import Counter
from contextlib import contextmanager
from threading import RLock
from typing import Dict, Iterator, Optional


class RunReport:
    """Count final file decisions once, including after async/sync fallback."""

    def __init__(self) -> None:
        self._lock = RLock()
        self.started: Optional[float] = None
        self.finished: Optional[float] = None
        self.status = "not started"
        self.reasons = ""
        self.checks: Dict[str, tuple[str, bool]] = {}
        self.downloads: set[str] = set()
        self.phases: Dict[str, float] = {}
        self._active_phase: Optional[tuple[str, float]] = None
        self.baseline: Dict[str, float] = {}
        self.remote_files = 0
        self.selected_configured = 0
        self.selected_found = 0

    def start(self, metrics: dict, selected_configured: int) -> None:
        with self._lock:
            self.started = time.perf_counter()
            self.finished = None
            self.status = "running"
            self.reasons = ""
            self.checks.clear()
            self.downloads.clear()
            self.phases.clear()
            self._active_phase = None
            self.baseline = {
                key: value for key, value in metrics.items() if isinstance(value, (int, float))
            }
            self.remote_files = self.selected_found = 0
            self.selected_configured = selected_configured

    def finish(self, status: str, reasons: str = "") -> None:
        with self._lock:
            if self.finished is None:
                self.end_phase()
                self.finished = time.perf_counter()
                self.status, self.reasons = status, reasons

    def begin_phase(self, name: str) -> None:
        """Start a coordinator phase without restructuring transfer control flow."""
        with self._lock:
            self.end_phase()
            self._active_phase = (name, time.perf_counter())

    def end_phase(self) -> None:
        with self._lock:
            if self._active_phase is not None:
                name, started = self._active_phase
                self.phases[name] = self.phases.get(name, 0.0) + time.perf_counter() - started
                self._active_phase = None

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        start = time.perf_counter()
        try:
            yield
        finally:
            with self._lock:
                self.phases[name] = self.phases.get(name, 0.0) + time.perf_counter() - start

    def record_check(self, url: str, outcome: str, selected: bool = False) -> None:
        with self._lock:
            self.checks[url] = (outcome, selected)

    def record_publication(self, url: str) -> None:
        """Called only when a complete downloaded file has been published."""
        with self._lock:
            self.downloads.add(url)

    def delta(self, metrics: dict, name: str) -> float:
        return metrics.get(name, 0) - self.baseline.get(name, 0)

    def snapshot(self, downloaded_bytes: int = 0, skipped: int = 0) -> dict:
        with self._lock:
            checks = Counter(outcome for outcome, _ in self.checks.values())
            downloads = Counter(
                self.checks.get(url, ("unclassified", False))[0] for url in self.downloads
            )
            elapsed = (
                (self.finished if self.finished is not None else time.perf_counter()) - self.started
                if self.started is not None
                else 0.0
            )
            download_seconds = self.phases.get("downloads", 0.0)
            return {
                "status": self.status,
                "reasons": self.reasons,
                "remote_files_found": self.remote_files,
                "selected_paths_configured": self.selected_configured,
                "selected_paths_found": self.selected_found,
                "selected_existing_files_checked": sum(
                    selected and outcome in ("current", "changed", "uncertain")
                    for outcome, selected in self.checks.values()
                ),
                "existing_files_current": checks["current"],
                "freshness_checks_skipped": checks["unchecked"],
                "other_files_skipped": max(0, skipped - checks["current"] - checks["unchecked"]),
                "missing_files_downloaded": downloads["missing"],
                "changed_files_downloaded": downloads["changed"],
                "uncertain_freshness_downloads": downloads["uncertain"],
                "unclassified_downloads": downloads["unclassified"],
                "metadata_checks_unresolved": checks["uncertain"],
                "downloads_planned": checks["missing"] + checks["changed"] + checks["uncertain"],
                "phase_seconds": dict(self.phases),
                "elapsed_seconds": elapsed,
                "download_throughput": (
                    downloaded_bytes / download_seconds
                    if self.downloads and download_seconds > 0
                    else None
                ),
            }
