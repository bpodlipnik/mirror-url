"""Human-readable reporting for exact-URL whole-file downloads."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Optional

if TYPE_CHECKING:
    from .config import MirrorConfig


PROGRESS_INTERVAL_SECONDS = 10.0


def transfer_bytes(value: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if value < 1024:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TiB"


@dataclass
class _ActiveFile:
    started: float
    received: int = 0
    expected: Optional[int] = None
    stage: str = "preparing"


class DownloadProgress:
    """One joined timer per visible run, independent of blocking disk/hash work.

    Only active file observations are retained. Published/failed counts change
    after the worker's result and receipt hook; receiving a full body alone is
    not completion. Received bytes include discarded whole-file retry attempts.
    """

    def __init__(self, enabled: bool):
        self.enabled = enabled
        self.started = time.perf_counter()
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.active: Dict[Path, _ActiveFile] = {}
        self.total: Optional[int] = None
        self.completed = self.failed = self.received_bytes = 0
        self.batch_stage = "preparing downloads"

    def __enter__(self):
        if self.enabled:
            self.thread = threading.Thread(
                target=self._watch, name="mirror-url-download-progress", daemon=True
            )
            self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        if self.thread is not None:
            self.thread.join()

    def plan(self, count: int) -> None:
        with self.lock:
            self.total = count

    def begin(self, path: Path, stage: str = "preparing") -> None:
        if self.enabled:
            with self.lock:
                self.active[path] = _ActiveFile(time.perf_counter(), stage=stage)

    def received(self, path: Path, size: int, expected: Optional[int]) -> None:
        if self.enabled:
            with self.lock:
                file = self.active[path]
                self.received_bytes += size - file.received
                file.received, file.expected, file.stage = size, expected, "downloading"

    def stage(self, path: Path, stage: str) -> None:
        if self.enabled:
            with self.lock:
                self.active[path].stage = stage

    def finish(self, path: Path, success: bool) -> None:
        if self.enabled:
            with self.lock:
                self.active.pop(path)
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
            elapsed = max(0.0, now - self.started)
            rate = self.received_bytes / elapsed if elapsed else 0.0
            total = str(self.total) if self.total is not None else "?"
            details = []
            for path, file in list(self.active.items())[:3]:
                name = str(path).replace("\n", "\\n").replace("\r", "\\r")
                amount = transfer_bytes(file.received)
                if file.expected is not None:
                    amount += "/" + transfer_bytes(file.expected)
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
                f"{self.failed} failed; received {transfer_bytes(self.received_bytes)}; "
                f"elapsed {elapsed:.1f}s; average {transfer_bytes(rate)}/s; {detail}"
            )
        logging.info(message)


def log_download_summary(config: MirrorConfig, summary: dict) -> None:
    target = config.dest_path / config.dir_suffix if config.dir_suffix else config.dest_path
    lines = [
        "DOWNLOAD SUMMARY:",
        "  Result: " + ("SUCCESS" if summary["success"] else "FAILED"),
        f"  Backend: {config.backend}",
        f"  Destination: {target.resolve()}",
    ]
    if summary.get("dry_run"):
        lines.extend(["  Dry run: no downloads", f"  Files planned: {summary['files']}"])
    else:
        size = summary["bytes_downloaded"]
        elapsed = summary["elapsed_seconds"]
        throughput = transfer_bytes(size / elapsed) + "/s" if elapsed > 0 else "n/a"
        lines.extend(
            [
                f"  Files downloaded: {summary['files_downloaded']}",
                f"  Files failed: {summary['files_failed']}",
                f"  Downloaded bytes: {transfer_bytes(size)} ({size} bytes)",
                f"  Elapsed time: {elapsed:.3f}s",
                f"  Average throughput: {throughput}",
                "  SHA-256: "
                + ("enabled (local receipts)" if config.verify_content else "disabled"),
                f"  Disk flush time: {summary['stage_seconds']['fsync']:.3f}s",
            ]
        )
        if config.verify_content:
            lines.append(f"  SHA-256 time: {summary['stage_seconds']['hash']:.3f}s")
        lines.append(f"  Ownership receipts: {summary['receipts']}")
    logging.info("\n".join(lines))
