"""Terminal progress reporting.

Migrated verbatim from ``mirror_url.py`` (orig. lines 8637-8936):
``ProgressTracker``, ``MultiLevelProgress``.
"""

from __future__ import annotations

import logging
import time
from threading import RLock
from typing import TYPE_CHECKING, Callable, Dict, Optional

from .compat import TQDM_AVAILABLE
from .constants import (
    PROGRESS_MEDIUM_JOB_SECONDS,
    PROGRESS_MIN_FILES_FOR_PCT,
    PROGRESS_PCT_MILESTONES,
    PROGRESS_SHORT_JOB_SECONDS,
    PROGRESS_UPDATE_LONG,
    PROGRESS_UPDATE_MEDIUM,
    PROGRESS_UPDATE_SHORT,
)
from .utils import format_duration

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig


class ProgressTracker:
    """Track and report progress for long operations"""

    def __init__(
        self,
        total: int,
        prefix: str = "",
        name: str = "items",
        use_tqdm: bool = True,
        config: Optional[MirrorConfig] = None,
        level: str = "default",
    ):
        """
        Initialize progress tracker.

        Args:
            total: Total items to process
            prefix: Prefix for log messages
            name: Name of items being tracked
            use_tqdm: Whether to use tqdm progress bar
            config: MirrorConfig instance
            level: Progress level name
        """
        try:
            self.total = max(0, int(total) if total is not None else 0)
        except (TypeError, ValueError):
            self.total = 0
            logging.debug(f"Invalid total value for ProgressTracker: {total}, using 0")

        self.prefix = prefix
        self.name = name
        self.level = level
        self.completed = 0
        self.lock = RLock()
        self.start_time = time.time()
        self.last_report = 0
        self.callbacks = []
        self._last_logged_completed = -1
        self._last_logged_pct = -1
        self.use_tqdm = (
            use_tqdm and TQDM_AVAILABLE and config and config.progress_bar and self.total > 0
        )
        self.tqdm_bar = None
        self._fallback_mode = False
        self._use_percentage_mode = self.total >= PROGRESS_MIN_FILES_FOR_PCT
        self._milestone_index = 0
        self._next_milestone = PROGRESS_PCT_MILESTONES[0] if self._use_percentage_mode else None
        self._pending_updates = 0
        self._update_threshold = 10 if not self.use_tqdm else 1

        if self.use_tqdm:
            try:
                from tqdm import tqdm

                self.tqdm_bar = tqdm(
                    total=self.total, desc=f"{prefix}{name}", unit=name, leave=True, position=0
                )
            except Exception as e:
                logging.debug(f"Failed to initialize tqdm: {e}")
                self.use_tqdm = False

    def reset_rate_after_fallback(self):
        """Reset rate timer after fallback"""
        with self.lock:
            self._fallback_mode = True
            self.start_time = time.time()
            self._last_logged_completed = self.completed
            logging.debug("Progress rate timer reset after fallback to sync")

    def _should_report(self) -> bool:
        """Check if progress should be reported"""
        if self.completed >= self.total:
            return False

        now = time.time()
        elapsed = now - self.start_time

        if elapsed < PROGRESS_SHORT_JOB_SECONDS:
            if self._use_percentage_mode:
                current_pct = self.completed / self.total * 100
                if current_pct >= self._next_milestone:
                    while (
                        self._milestone_index < len(PROGRESS_PCT_MILESTONES) - 1
                        and current_pct >= PROGRESS_PCT_MILESTONES[self._milestone_index + 1]
                    ):
                        self._milestone_index += 1
                    self._next_milestone = (
                        PROGRESS_PCT_MILESTONES[self._milestone_index + 1]
                        if self._milestone_index < len(PROGRESS_PCT_MILESTONES) - 1
                        else 101
                    )
                    return True
            else:
                return (now - self.last_report) >= PROGRESS_UPDATE_SHORT
        elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
            return (now - self.last_report) >= PROGRESS_UPDATE_MEDIUM
        else:
            return (now - self.last_report) >= PROGRESS_UPDATE_LONG

        return False

    def update(self, n: int = 1) -> None:
        """
        Update progress by n items.

        Args:
            n: Number of items completed
        """
        with self.lock:
            old_completed = self.completed
            self.completed = min(self.completed + n, self.total)

            if (
                logging.root.level <= logging.DEBUG
                and hasattr(self, "config")
                and self.config
                and self.config.debug
            ):
                logging.debug(
                    f"Progress.update({n}): {old_completed} -> {self.completed}/{self.total}"
                )

            if self.use_tqdm and self.tqdm_bar:
                try:
                    self.tqdm_bar.update(n)
                except Exception:
                    pass

            if self._should_report():
                report_msg = self._generate_report()
                elapsed = time.time() - self.start_time

                if elapsed < PROGRESS_SHORT_JOB_SECONDS:
                    logging.info(f"[short][{self.level}] {report_msg}")
                elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
                    logging.info(f"[medium][{self.level}] {report_msg}")
                else:
                    logging.info(f"[long][{self.level}] {report_msg}")

                self.last_report = time.time()
                self._last_logged_completed = self.completed

                if self._use_percentage_mode:
                    self._last_logged_pct = self.completed / self.total * 100

    def report_final(self) -> str:
        """Report final progress"""
        with self.lock:
            logging.debug(f"report_final: before - completed={self.completed}, total={self.total}")

            if self.completed < self.total:
                logging.debug("report_final: completed < total, setting to total")
                self.completed = self.total

            self.last_report = time.time()

            if self.use_tqdm and self.tqdm_bar:
                try:
                    self.tqdm_bar.n = self.tqdm_bar.total
                    self.tqdm_bar.refresh()
                    self.tqdm_bar.close()
                except Exception:
                    pass

            report_msg = self._generate_report(force_total=True)

            if not self.use_tqdm:
                elapsed = time.time() - self.start_time
                if elapsed < PROGRESS_SHORT_JOB_SECONDS:
                    logging.info(f"[final-short][{self.level}] {report_msg}")
                elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
                    logging.info(f"[final-medium][{self.level}] {report_msg}")
                else:
                    logging.info(f"[final-long][{self.level}] {report_msg}")

            self._last_logged_completed = self.completed
            logging.debug(f"report_final: after - completed={self.completed}, total={self.total}")

            return report_msg

    def _generate_report(self, force_total: bool = False) -> str:
        """Generate progress report message"""
        now = time.time()
        total_elapsed = now - self.start_time

        if force_total:
            percentage = (self.completed / self.total * 100) if self.total > 0 else 100
            rate = self.completed / total_elapsed if total_elapsed > 0 else 0
            report = (
                f"{self.prefix}Progress [{self.level}]: {self.completed}/{self.total} {self.name} "
                f"({percentage:.1f}%) - Complete! (Overall rate: {rate:.1f}/s)"
            )
        else:
            rate = self.completed / total_elapsed if total_elapsed > 0 else 0
            remaining_items = self.total - self.completed

            if rate > 0:
                remaining_time = remaining_items / rate
            else:
                remaining_time = float("inf")

            eta_str = (
                format_duration(remaining_time)
                if remaining_time > 0 and remaining_time != float("inf")
                else "unknown"
            )
            percentage = (self.completed / self.total * 100) if self.total > 0 else 0
            elapsed_str = format_duration(total_elapsed)

            report = (
                f"{self.prefix}Progress [{self.level}]: {self.completed}/{self.total} {self.name} "
                f"({percentage:.1f}%) - Rate: {rate:.1f}/s - "
                f"Elapsed: {elapsed_str} - ETA: {eta_str}"
            )

        return report

    def add_callback(self, callback: Callable) -> None:
        """Add progress callback"""
        self.callbacks.append(callback)

    def _trigger_callbacks(self) -> None:
        """Trigger progress callbacks"""
        for callback in self.callbacks:
            try:
                callback(self.completed, self.total)
            except Exception as e:
                logging.debug(f"Callback error: {e}")


class MultiLevelProgress:
    """Track progress across multiple levels/operations"""

    def __init__(self):
        """Initialize multi-level progress tracker"""
        self.levels: Dict[str, ProgressTracker] = {}
        self.lock = RLock()
        self.start_time = time.time()

    def add_level(
        self,
        name: str,
        total: int,
        prefix: str = "",
        use_tqdm: bool = True,
        config: Optional[MirrorConfig] = None,
    ):
        """
        Add a new progress level.

        Args:
            name: Level name
            total: Total items for this level
            prefix: Prefix for log messages
            use_tqdm: Whether to use tqdm
            config: MirrorConfig instance
        """
        with self.lock:
            self.levels[name] = ProgressTracker(
                total=total, prefix=prefix, name=name, use_tqdm=use_tqdm, config=config, level=name
            )

    def update(self, level: str, n: int = 1):
        """
        Update progress for a level.

        Args:
            level: Level name
            n: Number of items completed
        """
        with self.lock:
            if level in self.levels:
                self.levels[level].update(n)

    def set_total(self, level: str, total: int):
        """
        Set total for a level.

        Args:
            level: Level name
            total: New total
        """
        with self.lock:
            if level in self.levels:
                self.levels[level].total = total

    def report_final(self, level: str) -> str:
        """
        Get final report for a level.

        Args:
            level: Level name

        Returns:
            Final report string
        """
        with self.lock:
            if level in self.levels:
                return self.levels[level].report_final()
            return ""

    def get_status(self) -> str:
        """
        Get overall progress status.

        Returns:
            Status string for all levels
        """
        with self.lock:
            status = []
            for name, tracker in self.levels.items():
                if tracker.total > 0:
                    pct = tracker.completed / tracker.total * 100
                    status.append(f"{name}: {tracker.completed}/{tracker.total} ({pct:.1f}%)")

            elapsed = format_duration(time.time() - self.start_time)
            return f"Elapsed: {elapsed} | " + " | ".join(status)

    def reset_rate_after_fallback(self, level: str):
        """Reset rate after fallback for a level"""
        with self.lock:
            if level in self.levels:
                self.levels[level].reset_rate_after_fallback()


__all__ = ["ProgressTracker", "MultiLevelProgress"]
