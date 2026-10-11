"""Terminal progress reporting.

Migrated verbatim from ``mirror_url.py`` (orig. lines 8637-8936):
``ProgressTracker``, ``MultiLevelProgress``. Known-URL byte/stage observations
share this module via ``DownloadProgress``.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

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
from .utils import format_bytes, format_duration

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig


def _create_progress_bar(**settings):
    """Use the same terminal renderer for item and byte progress."""
    try:
        from tqdm import tqdm

        return tqdm(**settings)
    except Exception as error:
        logging.debug("Failed to initialize tqdm: %s", error)
        return None


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
        self.last_report: float = 0
        self.callbacks: List[Callable[[int, int], None]] = []
        self.use_tqdm = (
            use_tqdm and TQDM_AVAILABLE and config and config.progress_bar and self.total > 0
        )
        self.tqdm_bar = None
        self._use_percentage_mode = self.total >= PROGRESS_MIN_FILES_FOR_PCT
        self._milestone_index = 0
        self._next_milestone = PROGRESS_PCT_MILESTONES[0] if self._use_percentage_mode else None

        if self.use_tqdm:
            self.tqdm_bar = _create_progress_bar(
                total=self.total, desc=f"{prefix}{name}", unit=name, leave=True, position=0
            )
            self.use_tqdm = self.tqdm_bar is not None

    def reset_rate_after_fallback(self):
        """Reset rate timer after fallback"""
        with self.lock:
            self.start_time = time.time()
            logging.debug("Progress rate timer reset after fallback to sync")

    def _should_report(self) -> bool:
        """Check if progress should be reported"""
        if self.completed >= self.total:
            return False

        now = time.time()
        elapsed = now - self.start_time

        if elapsed < PROGRESS_SHORT_JOB_SECONDS:
            if self._use_percentage_mode and self._next_milestone is not None:
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

        self._trigger_callbacks()

    def report_final(self) -> str:
        """Report final progress"""
        with self.lock:
            logging.debug(f"report_final: before - completed={self.completed}, total={self.total}")

            self.last_report = time.time()

            if self.use_tqdm and self.tqdm_bar:
                try:
                    self.tqdm_bar.n = self.completed
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
                f"({percentage:.1f}%) - Finished (Overall rate: {rate:.1f} {self.name}/s)"
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
                f"({percentage:.1f}%) - Rate: {rate:.1f} {self.name}/s - "
                f"Elapsed: {elapsed_str} - ETA: {eta_str}"
            )

        return report

    def add_callback(self, callback: Callable) -> None:
        """Add progress callback"""
        with self.lock:
            self.callbacks.append(callback)

    def _trigger_callbacks(self) -> None:
        """Trigger progress callbacks"""
        with self.lock:
            callbacks = tuple(self.callbacks)
            completed, total = self.completed, self.total
        for callback in callbacks:
            try:
                callback(completed, total)
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


PROGRESS_INTERVAL_SECONDS = 10.0


class ConsoleProgressFilter(logging.Filter):
    """Keep periodic file-log observations off the console when bars show them."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not getattr(record, "progress_bar_active", False)


@dataclass
class _ActiveFile:
    started: float
    received: int = 0
    expected: Optional[int] = None
    stage: str = "preparing"
    bar: Any = None
    position: int = 0


class DownloadProgress:
    """One joined timer per visible run, independent of blocking disk/hash work.

    Only active file observations are retained. Published/failed counts change
    after the worker's result and receipt hook; receiving a full body alone is
    not completion. Received bytes include discarded whole-file retry attempts.
    """

    def __init__(self, enabled: bool, config: Optional[MirrorConfig] = None):
        self.enabled = enabled
        requested = bool(config and config.progress_bar and not config.quiet)
        self.bars = requested and TQDM_AVAILABLE
        if requested and not TQDM_AVAILABLE:
            logging.warning(
                "Progress bars require tqdm; install tqdm (a required mirror-url dependency)"
            )
        self.started = time.perf_counter()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.active: Dict[Path, _ActiveFile] = {}
        self.total: Optional[int] = None
        self.completed = self.failed = self.received_bytes = 0
        self.batch_stage = "preparing downloads"

    def __enter__(self):
        if self.enabled or self.bars:
            self.thread = threading.Thread(
                target=self._watch, name="mirror-url-download-progress", daemon=True
            )
            self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
        with self.lock:
            for file in self.active.values():
                self._close_bar(file)

    @staticmethod
    def _close_bar(file: _ActiveFile) -> None:
        if file.bar is not None:
            try:
                file.bar.close()
            except Exception as error:
                logging.debug("Failed to close progress bar: %s", error)
            file.bar = None

    def _update_bar(self, file: _ActiveFile, *, reset: bool = False, refresh: bool = False) -> None:
        if file.bar is None:
            return
        try:
            changed_total = file.bar.total != file.expected
            file.bar.total = file.expected
            if reset:
                file.bar.reset()
            file.bar.set_postfix_str(file.stage, refresh=False)
            file.bar.update(file.received - file.bar.n)
            if reset or refresh or changed_total:
                file.bar.refresh()
        except Exception as error:
            # A terminal renderer failure must not change transfer outcomes.
            logging.debug("Failed to update progress bar: %s", error)
            self._close_bar(file)

    def plan(self, count: int) -> None:
        with self.lock:
            self.total = count

    def begin(self, path: Path, stage: str = "preparing") -> None:
        if self.enabled or self.bars:
            with self.lock:
                previous = self.active.get(path)
                file = _ActiveFile(time.perf_counter(), stage=stage)
                if previous is not None:
                    file.bar, file.position = previous.bar, previous.position
                elif self.bars:
                    positions = {active.position for active in self.active.values()}
                    while file.position in positions:
                        file.position += 1
                    file.bar = _create_progress_bar(
                        total=None,
                        desc=path.name.replace("\n", "\\n").replace("\r", "\\r"),
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        leave=False,
                        position=file.position,
                    )
                self.active[path] = file
                self._update_bar(file, reset=True)

    def received(self, path: Path, size: int, expected: Optional[int]) -> None:
        if self.enabled or self.bars:
            with self.lock:
                file = self.active[path]
                self.received_bytes += size - file.received
                file.received, file.expected, file.stage = size, expected, "downloading"
                self._update_bar(file)

    def stage(self, path: Path, stage: str) -> None:
        if self.enabled or self.bars:
            with self.lock:
                file = self.active[path]
                file.stage = stage
                self._update_bar(file, refresh=True)

    def finish(self, path: Path, success: bool) -> None:
        if self.enabled or self.bars:
            with self.lock:
                self._close_bar(self.active.pop(path))
                self.completed += int(success)
                self.failed += int(not success)

    def saving_receipts(self) -> None:
        with self.lock:
            self.batch_stage = "saving receipts"

    def _watch(self) -> None:
        while not self.stop.wait(PROGRESS_INTERVAL_SECONDS):
            self.log_progress()

    def log_progress(self) -> None:
        now = time.perf_counter()
        with self.lock:
            for file in self.active.values():
                self._update_bar(file, refresh=True)
            elapsed = max(0.0, now - self.started)
            rate = self.received_bytes / elapsed if elapsed else 0.0
            total = str(self.total) if self.total is not None else "?"
            details = []
            for path, file in list(self.active.items())[:3]:
                name = str(path).replace("\n", "\\n").replace("\r", "\\r")
                amount = format_bytes(file.received, binary_units=True)
                if file.expected is not None:
                    amount += "/" + format_bytes(file.expected, binary_units=True)
                eta = ""
                age = now - file.started
                if file.stage == "downloading" and file.expected and file.received and age > 0:
                    remaining = max(0, file.expected - file.received)
                    eta = f", ETA {remaining / (file.received / age):.0f}s"
                details.append(f"{name}: {file.stage} ({amount}{eta})")
            if len(self.active) > 3:
                details.append(f"and {len(self.active) - 3} more active files")
            detail = "; ".join(details) or self.batch_stage
            message = (
                f"Download progress: {self.completed}/{total} files completed, "
                f"{self.failed} failed; received {format_bytes(self.received_bytes, binary_units=True)}; "
                f"elapsed {elapsed:.1f}s; average {format_bytes(rate, binary_units=True)}/s; {detail}"
            )
            # A disabled or failed renderer must retain the console fallback.
            visible_bars = bool(self.active) and all(
                file.bar is not None and not getattr(file.bar, "disable", False)
                for file in self.active.values()
            )
        if self.enabled:
            logging.info(message, extra={"progress_bar_active": visible_bars})


__all__ = ["ProgressTracker", "MultiLevelProgress", "DownloadProgress", "ConsoleProgressFilter"]
