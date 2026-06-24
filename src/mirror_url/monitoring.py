"""Host resource monitors.

Migrated verbatim from ``mirror_url.py``:
``MemoryMonitor`` (orig. 8941-9027), ``DiskSpaceManager`` (orig. 9032-9121),
``PerformanceMonitor`` (orig. 9126-9199).
"""

from __future__ import annotations

import logging
import shutil
import statistics
import time
from collections import defaultdict, deque
from pathlib import Path
from threading import RLock
from typing import Any, Deque, Dict, Optional, Tuple

from .compat import PSUTIL_AVAILABLE, psutil
from .constants import (
    DISK_SPACE_CRITICAL_THRESHOLD,
    DISK_SPACE_WARNING_THRESHOLD,
    MEMORY_CHECK_INTERVAL,
    MEMORY_CRITICAL_THRESHOLD_MB,
    MEMORY_WARNING_THRESHOLD_MB,
    MIN_FREE_SPACE_BYTES,
)
from .enums import MemoryPressure


class MemoryMonitor:
    """Monitor memory usage and trigger cleanup when needed"""

    def __init__(
        self,
        warning_threshold_mb: int = MEMORY_WARNING_THRESHOLD_MB,
        critical_threshold_mb: int = MEMORY_CRITICAL_THRESHOLD_MB,
        check_interval: int = MEMORY_CHECK_INTERVAL,
    ):
        """
        Initialize memory monitor.

        Args:
            warning_threshold_mb: Warning threshold in MB
            critical_threshold_mb: Critical threshold in MB
            check_interval: Check interval in seconds
        """
        self.warning_threshold = warning_threshold_mb * 1024 * 1024
        self.critical_threshold = critical_threshold_mb * 1024 * 1024
        self.check_interval = check_interval
        self.last_check = 0
        self.high_water_mark = 0
        self.lock = RLock()
        self.psutil_available = PSUTIL_AVAILABLE
        self._last_reset = time.time()

        if not PSUTIL_AVAILABLE:
            logging.warning("psutil not available, memory monitoring disabled")

    def check_pressure(self) -> MemoryPressure:
        """
        Check current memory pressure level.

        Returns:
            MemoryPressure level
        """
        if not self.psutil_available:
            return MemoryPressure.NORMAL

        now = time.time()
        with self.lock:
            if now - self.last_check < self.check_interval:
                return MemoryPressure.NORMAL

            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                rss = memory_info.rss

                if rss > self.high_water_mark:
                    self.high_water_mark = rss

                # Add periodic reset of high water mark to avoid unbounded growth
                if hasattr(self, "_last_reset"):
                    if time.time() - self._last_reset > 3600:  # Reset hourly
                        self.high_water_mark = rss
                        self._last_reset = time.time()
                else:
                    self._last_reset = time.time()

                self.last_check = now

                if rss > self.critical_threshold:
                    logging.warning(f"Critical memory pressure: {rss / (1024 * 1024):.1f}MB")
                    return MemoryPressure.CRITICAL
                elif rss > self.warning_threshold:
                    logging.debug(f"Memory pressure warning: {rss / (1024 * 1024):.1f}MB")
                    return MemoryPressure.WARNING

                return MemoryPressure.NORMAL
            except Exception as e:
                logging.debug(f"Memory check failed: {e}")
                return MemoryPressure.NORMAL

    def get_usage_mb(self) -> float:
        """
        Get current memory usage in MB.

        Returns:
            Memory usage in MB
        """
        if not self.psutil_available:
            return 0.0

        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0


class DiskSpaceManager:
    """Manage disk space and check availability"""

    def __init__(self, target_dir: Path):
        """
        Initialize disk space manager.

        Args:
            target_dir: Target directory to monitor
        """
        self.target_dir = target_dir
        self.warnings_issued = 0
        # FIX: Only create directory if target_dir is provided AND not None
        if self.target_dir is not None:
            try:
                self.target_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logging.debug(f"Could not create target directory: {e}")

    def check_available(self, required_bytes: int) -> Tuple[bool, Optional[str]]:
        """
        Check if enough disk space is available for *this operation*.

        Returns False ONLY when there isn't enough free space to do the work
        (either below the absolute floor or below ``required_bytes``).
        Overall disk-fullness percentages are warnings, not blockers — a host
        that happens to have a 96%-full disk can still legitimately download
        a few MB of data, and refusing to operate there made the library
        (and its tests) unusable on real hosts.

        Args:
            required_bytes: Required bytes

        Returns:
            Tuple of (ok, error_message)
        """
        # FIX: Return True if no target_dir (dry-run or connection failed)
        if self.target_dir is None:
            return True, None

        try:
            usage = shutil.disk_usage(self.target_dir)
            free_bytes = usage.free

            if free_bytes < MIN_FREE_SPACE_BYTES:
                return (
                    False,
                    f"Only {free_bytes / (1024 * 1024):.1f}MB free, need at least {MIN_FREE_SPACE_BYTES / (1024 * 1024):.1f}MB",
                )

            if free_bytes < required_bytes:
                return (
                    False,
                    f"Insufficient space: need {required_bytes / (1024 * 1024):.1f}MB, have {free_bytes / (1024 * 1024):.1f}MB",
                )

            # Fullness percentages are advisory warnings only — they DO NOT
            # block operations that have enough free space for themselves.
            usage_percent = usage.used / usage.total
            if usage_percent > DISK_SPACE_CRITICAL_THRESHOLD:
                if self.warnings_issued % 10 == 0:
                    logging.warning(
                        f"Disk usage critical: {usage_percent * 100:.1f}% "
                        f"({free_bytes / (1024 * 1024):.1f}MB free) — proceeding anyway"
                    )
                self.warnings_issued += 1
            elif usage_percent > DISK_SPACE_WARNING_THRESHOLD:
                if self.warnings_issued % 10 == 0:
                    logging.warning(f"Disk usage high: {usage_percent * 100:.1f}%")
                self.warnings_issued += 1

            return True, None
        except Exception as e:
            logging.warning(f"Failed to check disk space: {e}")
            return True, None

    def get_usage_stats(self) -> Dict[str, float]:
        """
        Get disk usage statistics.

        Returns:
            Dictionary with disk usage stats
        """
        # FIX: Return empty dict if target_dir is None
        if self.target_dir is None:
            return {}
        try:
            usage = shutil.disk_usage(self.target_dir)
            return {
                "total_gb": usage.total / (1024**3),
                "used_gb": usage.used / (1024**3),
                "free_gb": usage.free / (1024**3),
                "usage_percent": usage.used / usage.total * 100,
            }
        except Exception:
            return {}


class PerformanceMonitor:
    """Track detailed performance metrics"""

    def __init__(self, window_size: int = 1000):
        """
        Initialize performance monitor.

        Args:
            window_size: Number of samples to keep per operation
        """
        self.operations: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window_size))
        self.counters: Dict[str, int] = defaultdict(int)
        self.lock = RLock()
        self.start_time = time.time()

    def record(self, operation: str, duration: float, success: bool = True) -> None:
        """
        Record operation duration.

        Args:
            operation: Operation name
            duration: Duration in seconds
            success: Whether operation succeeded
        """
        with self.lock:
            self.operations[operation].append(duration)
            self.counters[f"{operation}_{'success' if success else 'failure'}"] += 1

    def record_bytes(self, bytes_count: int) -> None:
        """Record bytes downloaded"""
        with self.lock:
            self.counters["bytes_downloaded"] += bytes_count

    def get_stats(self, operation: str) -> Dict[str, float]:
        """
        Get statistics for an operation.

        Args:
            operation: Operation name

        Returns:
            Dictionary with operation statistics
        """
        with self.lock:
            times = list(self.operations.get(operation, []))
            if not times:
                return {}

            return {
                "avg_ms": statistics.mean(times) * 1000,
                "p95_ms": sorted(times)[int(len(times) * 0.95)] * 1000,
                "max_ms": max(times) * 1000,
                "min_ms": min(times) * 1000,
                "count": len(times),
                "success_count": self.counters.get(f"{operation}_success", 0),
                "failure_count": self.counters.get(f"{operation}_failure", 0),
            }

    def get_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive performance summary.

        Returns:
            Dictionary with performance summary
        """
        with self.lock:
            return {
                "uptime_seconds": time.time() - self.start_time,
                "operations": {op: self.get_stats(op) for op in self.operations},
                "counters": dict(self.counters),
                "total_operations": sum(len(v) for v in self.operations.values()),
            }


__all__ = ["MemoryMonitor", "DiskSpaceManager", "PerformanceMonitor"]
