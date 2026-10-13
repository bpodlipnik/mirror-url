"""Per-sync reporting, independent of transfer and freshness policy."""

from __future__ import annotations

import time
from collections import Counter
from contextlib import contextmanager
from threading import RLock
from typing import Dict, Iterator, Optional

TRANSFER_STAGES = (
    ("range_metadata", "Range metadata probes"),
    ("transfer", "Data transfer/write"),
    ("assembly", "Chunk assembly"),
    ("flush", "Disk flushing"),
    ("hash", "SHA-256 hashing"),
)


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
        self.hashed_downloads: set[str] = set()
        self.verify_content: Optional[bool] = None
        self.phases: Dict[str, float] = {}
        self.transfer_seconds: Dict[str, float] = {}
        self.transfer_operations: Counter[str] = Counter()
        self._active_phase: Optional[tuple[str, float]] = None
        self.baseline: Dict[str, float] = {}
        self.remote_files = 0
        self.selected_configured = 0
        self.selected_found = 0
        self.discovery_mode: Optional[str] = None
        self.discovery_workers = 0
        self.directories_examined = 0
        self.closed_connection_pools = 0
        self.peak_parallel_chunks = 0

    def start(
        self, metrics: dict, selected_configured: int, *, verify_content: Optional[bool] = None
    ) -> None:
        with self._lock:
            self.started = time.perf_counter()
            self.finished = None
            self.status = "running"
            self.reasons = ""
            self.checks.clear()
            self.downloads.clear()
            self.hashed_downloads.clear()
            self.verify_content = verify_content
            self.phases.clear()
            self.transfer_seconds.clear()
            self.transfer_operations.clear()
            self._active_phase = None
            self.baseline = {
                key: value for key, value in metrics.items() if isinstance(value, (int, float))
            }
            self.remote_files = self.selected_found = 0
            self.selected_configured = selected_configured
            self.discovery_mode = None
            self.discovery_workers = self.directories_examined = 0
            self.closed_connection_pools = 0
            self.peak_parallel_chunks = 0

    def record_chunk_workers(self, active: int) -> None:
        """Observe admitted chunk workers, rather than executor capacity."""
        with self._lock:
            self.peak_parallel_chunks = max(self.peak_parallel_chunks, active)

    def begin_discovery(self, mode: str, workers: int) -> None:
        """Record the actual BFS strategy, separately from parser activity."""
        with self._lock:
            self.discovery_mode = mode
            self.discovery_workers = workers
            self.directories_examined = 0

    def record_directory_examined(self) -> None:
        """Count each directory admitted by the BFS coordinator once."""
        with self._lock:
            self.directories_examined += 1

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

    @contextmanager
    def transfer_stage(self, name: str) -> Iterator[None]:
        """Sum measured worker intervals, including failed/cancelled operations.

        These intervals can overlap across workers and do not replace elapsed
        coordinator phases or the throughput denominator. Observe operations,
        rather than measuring entire transfer methods with nested stages.
        """
        started = time.perf_counter()
        try:
            yield
        finally:
            elapsed = time.perf_counter() - started
            with self._lock:
                self.transfer_seconds[name] = self.transfer_seconds.get(name, 0.0) + elapsed
                self.transfer_operations[name] += 1

    def record_publication(self, url: str, *, sha256: Optional[str] = None) -> None:
        """Called only when a complete downloaded file has been published."""
        with self._lock:
            self.downloads.add(url)
            if sha256 is not None:
                self.hashed_downloads.add(url)

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
            discovery_seconds = self.phases.get("discovery", 0.0)
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
                "peak_parallel_chunks": self.peak_parallel_chunks,
                "verify_content": self.verify_content,
                "sha256_hashes_recorded": len(self.hashed_downloads),
                "phase_seconds": dict(self.phases),
                "transfer_stage_seconds": dict(self.transfer_seconds),
                "transfer_stage_operations": dict(self.transfer_operations),
                "elapsed_seconds": elapsed,
                "discovery_mode": self.discovery_mode,
                "discovery_workers": self.discovery_workers,
                "directories_examined": self.directories_examined,
                "discovery_throughput": (
                    self.directories_examined / discovery_seconds
                    if self.discovery_mode is not None and discovery_seconds > 0
                    else None
                ),
                "download_throughput": (
                    downloaded_bytes / download_seconds
                    if self.downloads and download_seconds > 0
                    else None
                ),
            }
