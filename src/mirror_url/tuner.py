"""Auto concurrency tuning.

Migrated verbatim from ``mirror_url.py`` (orig. lines 9717-9818): ``AutoConcurrencyTuner``.
"""

from __future__ import annotations

from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

from .constants import (
    AUTO_CONCURRENCY_MAX,
    AUTO_CONCURRENCY_START,
    AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD,
)


class AutoConcurrencyTuner:
    """Automatically tune concurrency based on measured throughput"""

    def __init__(
        self,
        start_concurrency: int = AUTO_CONCURRENCY_START,
        max_concurrency: int = AUTO_CONCURRENCY_MAX,
    ):
        """
        Initialize auto-concurrency tuner.

        Args:
            start_concurrency: Starting concurrency level
            max_concurrency: Maximum concurrency level
        """
        self._start = start_concurrency
        self.current = start_concurrency
        self.max = max_concurrency
        self._start = start_concurrency
        self.samples: List[Tuple[int, float]] = []  # (concurrency, throughput)
        self.last_throughput = 0.0
        self.improvement_count = 0
        self.adjustments = 0
        self.lock = RLock()

    def record_throughput(self, concurrency: int, throughput: float) -> Optional[int]:
        """
        Record throughput and return new concurrency if tuning needed.

        Args:
            concurrency: Current concurrency level
            throughput: Measured throughput in MB/s

        Returns:
            New concurrency value if adjustment needed, None otherwise
        """
        with self.lock:
            self.samples.append((concurrency, throughput))

            # Keep only last 20 samples
            if len(self.samples) > 20:
                self.samples.pop(0)

            if self.last_throughput > 0:
                improvement = (throughput - self.last_throughput) / self.last_throughput
                if improvement > AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD:
                    self.improvement_count += 1
                elif improvement < -AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD:
                    self.improvement_count -= 1
                else:
                    self.improvement_count = max(0, min(0, self.improvement_count))

            self.last_throughput = throughput

            # If we've seen improvement in last 3 samples, increase concurrency
            if self.improvement_count >= 2 and self.current < self.max:
                self.current = min(self.max, self.current + 2)
                self.improvement_count = 0
                self.adjustments += 1
                return self.current

            # If no improvement and we're above start, decrease
            if self.improvement_count <= -2 and self.current > self.start:
                self.current = max(self.start, self.current - 2)
                self.improvement_count = 0
                self.adjustments += 1
                return self.current

            return None

    def get_concurrency(self) -> int:
        """Get current recommended concurrency"""
        with self.lock:
            return self.current

    def get_stats(self) -> Dict[str, Any]:
        """Get tuner statistics"""
        with self.lock:
            return {
                "current_concurrency": self.current,
                "start_concurrency": self.start,
                "max_concurrency": self.max,
                "samples": self.samples[-10:],
                "last_throughput": self.last_throughput,
                "adjustments": self.adjustments,
                "total_samples": len(self.samples),
            }

    def reset(self) -> None:
        """Reset tuner to starting values"""
        with self.lock:
            self.current = self.start
            self.samples.clear()
            self.last_throughput = 0.0
            self.improvement_count = 0

    @property
    def start(self) -> int:
        return self._start

    @start.setter
    def start(self, value: int):
        self._start = value


__all__ = ["AutoConcurrencyTuner"]
