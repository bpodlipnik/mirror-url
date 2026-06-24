"""Plain dataclasses passed between subsystems.

Migrated verbatim from ``mirror_url.py`` (orig. lines 505-651).
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from typing import Any, Deque, Dict, List, Optional

from .constants import (
    ADAPTIVE_ERROR_THRESHOLD,
    ADAPTIVE_MAX_CONCURRENCY,
    ADAPTIVE_RTT_THRESHOLD_MS,
    ADAPTIVE_START_CONCURRENCY,
    ADAPTIVE_THROUGHPUT_MIN,
    ADAPTIVE_WINDOW_SIZE,
)
from .enums import DownloadPriority


@dataclass
class DownloadTask:
    """Represents a file download task"""

    remote_url: str
    local_path: Path
    priority: DownloadPriority = DownloadPriority.NORMAL
    size: Optional[int] = None
    retries: int = 0
    max_retries: int = 3
    etag: Optional[str] = None
    timestamp: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    # NEW v2.0.0 fields
    partial_path: Optional[Path] = None
    resume_from: int = 0
    # NEW v3.0.0 fields
    chunk_info: Optional[Dict[str, Any]] = None
    is_chunk: bool = False


@dataclass
class ServerProfile:
    """Tracks server performance characteristics for adaptive async.

    Thread-safety: ``add_sample`` is called from concurrent HTTP completion
    callbacks (one per finished request). Without a lock, two callers could
    interleave ``deque.append`` with ``_update_metrics`` iterating the same
    deque, raising ``RuntimeError: deque mutated during iteration`` and
    crashing the request thread. ``_lock`` guards the deque + derived metric
    fields. The lock is excluded from ``repr`` so debug dumps stay readable.
    """

    domain: str
    avg_rtt_ms: float = 0.0
    error_rate: float = 0.0
    throughput_files_per_sec: float = 100.0  # Changed from 0.0 to 100.0 while fixing error
    last_adjustment: float = 0.0
    recommended_concurrency: int = ADAPTIVE_START_CONCURRENCY
    is_throttled: bool = False
    samples: Deque[Dict] = field(default_factory=lambda: deque(maxlen=ADAPTIVE_WINDOW_SIZE))
    # Reentrant — _update_metrics is called from inside add_sample which
    # already holds the lock. RLock lets the same thread re-enter without
    # deadlocking.
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)

    def add_sample(self, rtt_ms: float, success: bool, duration_sec: float = 0) -> None:
        """Add a performance sample. Safe to call from multiple threads."""
        with self._lock:
            self.samples.append(
                {
                    "rtt_ms": rtt_ms,
                    "success": success,
                    "duration": duration_sec,
                    "timestamp": time.time(),
                }
            )
            self._update_metrics()

    def _update_metrics(self) -> None:
        """Recalculate metrics from samples.

        Caller MUST hold ``self._lock``. Re-iterating the deque without the
        lock against a concurrent ``append`` raises
        ``RuntimeError: deque mutated during iteration``.
        """
        with self._lock:
            if not self.samples:
                return
            # Snapshot under lock so the iteration below is safe even if a
            # future caller forgets the outer lock.
            samples_snapshot = list(self.samples)
            successful = [s for s in samples_snapshot if s["success"]]
            self.error_rate = 1.0 - (len(successful) / len(samples_snapshot))
            if successful:
                self.avg_rtt_ms = statistics.mean(s["rtt_ms"] for s in successful)
                total_time = sum(s["duration"] for s in successful if s["duration"] > 0)
                if total_time > 0:
                    self.throughput_files_per_sec = len(successful) / total_time
            if (
                self.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS
                or self.error_rate > ADAPTIVE_ERROR_THRESHOLD
            ):
                self.is_throttled = True
                self.recommended_concurrency = max(1, self.recommended_concurrency // 2)
            elif self.error_rate < 0.01 and self.throughput_files_per_sec > ADAPTIVE_THROUGHPUT_MIN:
                self.recommended_concurrency = min(
                    ADAPTIVE_MAX_CONCURRENCY, self.recommended_concurrency + 2
                )
            self.last_adjustment = time.time()

    def should_scale_up(self) -> bool:
        """Check if we can increase concurrency.

        Decision is based on error_rate and avg_rtt_ms only.
        throughput_files_per_sec is intentionally excluded: it equals
        (sample_count / total_duration) which is dominated by individual
        request duration, not server health. Ten samples at 0.5s each
        yields 2.0 files/sec regardless of RTT or errors, so a threshold
        of ADAPTIVE_THROUGHPUT_MIN (10) would never be reached even when
        the server is perfectly healthy.
        """
        with self._lock:
            return self.error_rate < 0.02 and self.avg_rtt_ms < ADAPTIVE_RTT_THRESHOLD_MS * 0.7


@dataclass
class HealthStatus:
    """Health check status"""

    status: str
    timestamp: str
    metrics: Dict[str, Any]
    connection: Dict[str, Any]
    cache: Dict[str, Any]
    errors: List[Dict[str, Any]]
    # NEW v2.0.0 fields
    system: Dict[str, Any] = field(default_factory=dict)


# NEW v3.0.0 data classes
@dataclass
class ChunkInfo:
    """Information about a file chunk"""

    file_url: str
    final_path: Path
    chunk_id: int
    start_byte: int
    end_byte: int
    total_chunks: int
    temp_path: Path
    size: int = 0
    downloaded: int = 0
    status: str = "pending"  # pending, downloading, completed, failed
    retries: int = 0
    max_retries: int = 3
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    direct_write: bool = False  # Write directly to final file


@dataclass
class ParallelFileDownload:
    """Manages parallel chunk downloads for a single file"""

    url: str
    final_path: Path
    file_size: int
    chunks: List[ChunkInfo] = field(default_factory=list)
    temp_dir: Optional[Path] = None
    start_time: float = field(default_factory=time.time)
    completed_chunks: int = 0
    failed_chunks: int = 0
    status: str = "initializing"  # initializing, downloading, assembling, completed, failed
    supports_range: bool = True
    server_etag: Optional[str] = None
    server_last_modified: Optional[float] = None
    lock: RLock = field(default_factory=RLock)


__all__ = [
    "DownloadTask",
    "ServerProfile",
    "HealthStatus",
    "ChunkInfo",
    "ParallelFileDownload",
]
