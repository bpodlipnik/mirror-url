"""Reclaim recorded parallel-transfer scratch after the previous owner exits."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import stat
import threading
import uuid
from pathlib import Path
from typing import Dict, List, Optional, TextIO

import portalocker

from .destination_lock import DestinationLock, destination_operation


class OwnedScratch:
    """Flat private workspaces, recovered only with exclusive state ownership.

    A manifest is written before transfer bytes. Unknown entries, links and
    invalid markers are preserved. Current work is excluded from recovery even
    when a bounded shutdown leaves a writer alive.
    """

    def __init__(self, target: Path, state: Path, assembly: Optional[Path], guard: DestinationLock):
        self._destination_lock = guard
        self.target = str(target.resolve())
        self.stage_root = state / "parallel"
        identity = hashlib.sha256(self.target.encode("utf-8")).hexdigest()
        self.chunk_root = (
            assembly / (".mirror-url-scratch-" + identity) if assembly else state / "chunks"
        )
        self._active: Dict[Path, TextIO] = {}
        self._lock = threading.RLock()
        # The configured assembly tree is already reserved by MirrorURL.
        guard.add_paths([self.stage_root, self.chunk_root])
        self._ensure_root(self.stage_root, "staging")
        self._ensure_root(self.chunk_root, "chunks")
        self.recover()

    @staticmethod
    def _regular(path: Path) -> bool:
        info = path.lstat()
        return (
            stat.S_ISREG(info.st_mode)
            and info.st_nlink == 1
            and (not hasattr(os, "getuid") or info.st_uid == os.getuid())
            and (path.name != "owner.json" or info.st_size <= 64 * 1024)
        )

    def _ensure_root(self, root: Path, kind: str) -> None:
        expected = {"format": 1, "target": self.target, "kind": kind}
        if not root.exists() and not root.is_symlink():
            root.mkdir(mode=0o700, parents=True)
            self._write_marker(root / "owner.json", expected)
        if root.is_symlink() or not root.is_dir():
            raise ValueError("Refusing an unsafe parallel scratch directory")
        marker = root / "owner.json"
        if not self._regular(marker) or json.loads(marker.read_text(encoding="utf-8")) != expected:
            raise ValueError("Refusing an unowned parallel scratch directory")

    @staticmethod
    def _write_marker(path: Path, data: Dict) -> None:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(data, stream)
            stream.flush()
            os.fsync(stream.fileno())

    @destination_operation
    def create(self, kind: str, names: List[str]) -> Path:
        if kind not in ("staging", "chunks") or not self._valid_names(kind, names):
            raise ValueError("Invalid parallel scratch manifest")
        root = self.stage_root if kind == "staging" else self.chunk_root
        with self._lock:
            self._ensure_root(root, kind)
            token = uuid.uuid4().hex
            work = root / ("work_" + token)
            work.mkdir(mode=0o700)
            # A per-work lease also protects a still-running suffix writer
            # when another CLI child borrows the same parent ownership scope.
            stream = (work / "owner.json").open("x+", encoding="utf-8")
            try:
                portalocker.lock(stream, portalocker.LOCK_EX | portalocker.LOCK_NB)
                json.dump(
                    {
                        "format": 1,
                        "target": self.target,
                        "kind": kind,
                        "token": token,
                        "files": names,
                    },
                    stream,
                )
                stream.flush()
                os.fsync(stream.fileno())
            except BaseException:
                stream.close()
                raise
            self._active[work] = stream
            return work

    @staticmethod
    def _valid_names(kind: str, names: object) -> bool:
        if not isinstance(names, list) or not names or not all(isinstance(n, str) for n in names):
            return False
        if len(set(names)) != len(names):
            return False
        pattern = (
            r"staging\.(?:streaming|assembling)"
            if kind == "staging"
            else r"chunk_\d{4,}_[0-9a-f]{16}\.part"
        )
        return all(re.fullmatch(pattern, name) is not None for name in names)

    @destination_operation
    def staging(self, final: Path, kind: str) -> Path:
        # Publication must stay atomic, including for a nested mount point.
        if final.parent.stat().st_dev != self.stage_root.stat().st_dev:
            raise OSError("Parallel staging and destination must use the same filesystem")
        name = "staging." + kind
        return self.create("staging", [name]) / name

    def _remove(self, work: Path, root: Path, kind: str) -> bool:
        """Validate the entire flat workspace before unlinking any bytes."""
        token = work.name.removeprefix("work_")
        if work.parent != root or re.fullmatch(r"work_[0-9a-f]{32}", work.name) is None:
            return False
        if work.is_symlink() or not work.is_dir():
            return False
        marker = work / "owner.json"
        if not self._regular(marker):
            return False
        descriptor = os.open(marker, os.O_RDWR | getattr(os, "O_NOFOLLOW", 0))
        with os.fdopen(descriptor, "r+", encoding="utf-8") as stream:
            try:
                portalocker.lock(stream, portalocker.LOCK_EX | portalocker.LOCK_NB)
            except portalocker.LockException:
                return False
            data = json.load(stream)
        if not isinstance(data, dict) or not self._valid_names(kind, data.get("files")):
            return False
        expected = {
            "format": 1,
            "target": self.target,
            "kind": kind,
            "token": token,
            "files": data["files"],
        }
        if data != expected:
            return False
        entries = list(work.iterdir())
        if any(
            p.name not in {"owner.json", *data["files"]} or not self._regular(p) for p in entries
        ):
            return False
        # Keep the manifest until transfer-byte deletion succeeds, so a later
        # owner can retry after a permission/IO failure.
        for entry in entries:
            if entry.name != "owner.json":
                entry.unlink()
        marker.unlink()
        work.rmdir()
        return True

    def release(self, work: Path) -> bool:
        """Retire own leased work after IO, including during bounded shutdown."""
        with self._lock:
            if work not in self._active:
                return False
            self._active.pop(work).close()
            root = work.parent
            kind = "staging" if root == self.stage_root else "chunks"
            try:
                self._ensure_root(root, kind)
                removed = self._remove(work, root, kind)
                if not removed:
                    logging.warning("Preserving unrecognized parallel scratch: %s", work)
                return removed
            except (OSError, ValueError) as error:
                logging.warning("Could not reclaim parallel scratch %s: %s", work, error)
                return False

    @destination_operation
    def recover(self) -> int:
        removed = 0
        with self._lock:
            for root, kind in ((self.stage_root, "staging"), (self.chunk_root, "chunks")):
                self._ensure_root(root, kind)
                for work in root.iterdir():
                    if work.name == "owner.json" or work in self._active:
                        continue
                    try:
                        if self._remove(work, root, kind):
                            removed += 1
                        else:
                            logging.warning("Preserving unrecognized parallel scratch: %s", work)
                    except (OSError, ValueError) as error:
                        logging.warning("Could not reclaim parallel scratch %s: %s", work, error)
        if removed:
            logging.info("Reclaimed %s abandoned parallel scratch workspaces", removed)
        return removed
