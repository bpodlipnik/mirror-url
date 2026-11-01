"""Portable, cooperative capacity accounting. No physical reservation is implied."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, Optional
from uuid import uuid4

import portalocker

from .destination_lock import _lock_directory, _open_lock


class CapacityError(OSError):
    """Admission failed; preserve any validated resumable prefix."""


class _CapacityClaim:
    """Per-user, same-host filesystem claims with kernel-owned crash leases.

    An epoch starts with one free-space sample. Retained bytes stay charged
    until every lease in the epoch ends, so later jobs do not count written
    bytes twice. Abandoned claims remain conservatively charged until then.
    Non-cooperating writers and shared-pool/remote quotas still require I/O
    failure handling; this registry is deliberately not an allocation API.
    """

    def __init__(self, target: Path):
        directory = _lock_directory()
        identity = f"{os.name}:{target.stat().st_dev}:{target.resolve().anchor}"
        digest = hashlib.sha256(identity.encode()).hexdigest()
        self.state = directory / ("capacity-" + digest + ".json")
        self.mutex = directory / ("capacity-" + digest + ".lock")
        self.token = uuid4().hex
        self.lease_path = directory / ("capacity-lease-" + self.token + ".lock")
        self.lease = _open_lock(self.lease_path)
        try:
            portalocker.lock(self.lease, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except BaseException:
            self.lease.close()
            raise
        self.funded = 0

    def _load(self, free: int) -> dict:
        if self.state.exists():
            with _open_lock(self.state) as file:
                raw = file.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise OSError("Capacity registry is too large")
            data = json.loads(raw)
            if (
                not isinstance(data, dict)
                or set(data) != {"free", "spent", "claims"}
                or any(type(data[key]) is not int or data[key] < 0 for key in ("free", "spent"))
                or not isinstance(data["claims"], dict)
            ):
                raise OSError("Invalid capacity registry")
        else:
            data = {"free": free, "spent": 0, "claims": {}}
        for token, charge in list(data["claims"].items()):
            if (
                not isinstance(token, str)
                or len(token) != 32
                or any(c not in "0123456789abcdef" for c in token)
                or type(charge) is not int
                or charge < 0
            ):
                raise OSError("Invalid capacity claim")
            if token == self.token:
                continue
            path = self.state.parent / ("capacity-lease-" + token + ".lock")
            with _open_lock(path) as file:
                try:
                    portalocker.lock(file, portalocker.LOCK_EX | portalocker.LOCK_NB)
                except portalocker.exceptions.LockException:
                    continue
                data["spent"] += data["claims"].pop(token)
            path.unlink(missing_ok=True)
        if not data["claims"]:
            data = {"free": free, "spent": 0, "claims": {}}
        return data

    def _save(self, data: dict) -> None:
        descriptor, name = tempfile.mkstemp(prefix="capacity-write-", dir=self.state.parent)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as file:
                json.dump(data, file)
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary, self.state)
        finally:
            temporary.unlink(missing_ok=True)

    def add(self, minimum: int, preferred: int, free: int) -> int:
        with _open_lock(self.mutex) as file:
            portalocker.lock(file, portalocker.LOCK_EX)
            data = self._load(free)
            if data["claims"].get(self.token, 0) != self.funded:
                raise OSError("Capacity claim changed unexpectedly")
            available = min(free, data["free"] - data["spent"] - sum(data["claims"].values()))
            if available < minimum:
                raise OSError(errno.ENOSPC, "Insufficient capacity for cooperating transfers")
            amount = min(available, preferred)
            data["claims"][self.token] = data["claims"].get(self.token, 0) + amount
            self._save(data)
            self.funded += amount
            return amount

    def close(self, retained: int) -> None:
        try:
            if self.funded:
                with _open_lock(self.mutex) as file:
                    portalocker.lock(file, portalocker.LOCK_EX)
                    data = self._load(0)
                    if (
                        data["claims"].get(self.token) != self.funded
                        or not 0 <= retained <= self.funded
                    ):
                        raise OSError("Capacity claim changed before retirement")
                    data["claims"].pop(self.token, None)
                    data["spent"] += retained
                    self._save(data)
        finally:
            self.lease.close()
            self.lease_path.unlink(missing_ok=True)
            self.funded = 0


class CapacityBudget:
    """Thread-safe batch credit shared by whole-file and chunk transfers.

    Query at first admission and at most every 30 seconds thereafter, never
    per block. Fund unknown-length work in coarse windows. Only proven removal
    of owned staging returns consumed credit. Published bytes remain charged.
    """

    WINDOW = 8 * 1024 * 1024

    def __init__(
        self,
        target: Path,
        concurrency: int,
        *,
        total_hint: Optional[int] = None,
        sample: Optional[Callable] = None,
    ):
        self.target, self.total_hint, self.sample = target, total_hint, sample or shutil.disk_usage
        self.headroom = max(1, concurrency) * 1024 * 1024
        self._lock = RLock()
        self._claim: Optional[_CapacityClaim] = None
        self._remaining = 0
        self._funded = 0
        self._free: Optional[int] = None
        self._sampled = 0.0
        self._failed: Optional[Exception] = None
        self._closed = False
        self._reserved: Dict[Path, int] = {}

    def _fund(self, minimum: int) -> None:
        if self._failed is not None:
            raise CapacityError(f"Capacity admission unavailable: {self._failed}")
        try:
            now = time.monotonic()
            if self._free is None or now - self._sampled >= 30:
                self._free = self.sample(self.target).free
                self._sampled = now
            if self._claim is None:
                self._claim = _CapacityClaim(self.target)
            first = self._funded == 0
            minimum += self.headroom if first else 0
            preferred = max(
                minimum, (self.total_hint or self.WINDOW) + self.headroom if first else self.WINDOW
            )
            amount = self._claim.add(minimum, preferred, self._free)
            self._funded += amount
            self._remaining += amount - (self.headroom if first else 0)
        except Exception as error:
            self._failed = error
            raise CapacityError(str(error)) from error

    def reserve(self, local: Path, size: int) -> None:
        if size < 0:
            raise ValueError("Negative capacity request")
        with self._lock:
            if self._closed:
                raise CapacityError("Capacity budget is closed")
            if self._failed is not None:
                raise CapacityError(f"Capacity admission unavailable: {self._failed}")
            if self._free is None or size > self._remaining:
                self._fund(max(0, size - self._remaining))
            elif time.monotonic() - self._sampled >= 30:
                # An external writer may have consumed space since admission.
                try:
                    self._free = self.sample(self.target).free
                    self._sampled = time.monotonic()
                    if self._free < size + self.headroom:
                        raise OSError(
                            errno.ENOSPC, "Destination capacity decreased during transfers"
                        )
                except Exception as error:
                    self._failed = error
                    raise CapacityError(str(error)) from error
            self._remaining -= size
            self._reserved[local] = self._reserved.get(local, 0) + size

    def ensure(self, local: Path, needed: int) -> None:
        with self._lock:
            if self._closed:
                raise CapacityError("Capacity budget is closed")
            extra = needed - self._reserved.get(local, 0)
            if extra > 0:
                # Local bookkeeping is cheap; shared funding occurs only when
                # the prepaid window is exhausted.
                self.reserve(local, extra)
            elif time.monotonic() - self._sampled >= 30:
                self.reserve(local, 0)

    def release(self, local: Path, retained: int = 0) -> None:
        with self._lock:
            reserved = self._reserved.pop(local, 0)
            self._remaining += max(0, reserved - retained)

    def close(self) -> None:
        with self._lock:
            try:
                if self._claim is not None:
                    self._claim.close(max(0, self._funded - self._remaining - self.headroom))
            finally:
                self._claim = None
                self._closed = True
