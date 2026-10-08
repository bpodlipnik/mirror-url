"""Cooperative, per-user ownership of local mirror trees and shared state.

Shared ancestor locks plus exclusive resource locks reject overlapping trees
without serializing unrelated destinations. Keep lock files permanently: unlinking
one while a process holds it could create two separately locked inodes. Kernel
locks disappear on process exit, including a hard kill; PID files are not used.
"""

from __future__ import annotations

import hashlib
import logging
import os
import stat
import unicodedata
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path
from threading import RLock
from typing import BinaryIO, Callable, Dict, Iterable, Iterator, List, Optional

import portalocker

from .exceptions import DestinationLockError


class MirrorFileHandler(logging.FileHandler):
    """Ignore delayed records after close instead of reopening an append log.

    Handler.handle and FileHandler.close hold the same handler lock. A record
    that selected this handler before its removal therefore cannot reopen it
    after cleanup releases filesystem ownership.
    """

    # Python 3.9's logging.Handler has no _closed attribute. Keep our own
    # immutable class default, replaced with an instance flag when closing.
    _mirror_closed = False

    def emit(self, record: logging.LogRecord) -> None:
        if not self._mirror_closed:
            super().emit(record)

    def close(self) -> None:
        self.acquire()
        try:
            self._mirror_closed = True
            super().close()
        finally:
            self.release()


def _check_directory(path: Path, posix: bool = os.name == "posix") -> None:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise OSError(f"Lock directory is not a real directory: {path}")
    if posix and (info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise OSError(f"Lock directory must be owned by this user and private: {path}")


def _lock_directory() -> Path:
    # HOME is stable across jobs with different TMPDIR settings. Each cooperating
    # process must use the same home directory; network filesystems are unsupported.
    directory = Path.home() / ".mirror-url" / "locks-v1"
    for path in (directory.parent, directory):
        path.mkdir(mode=0o700, exist_ok=True)
        _check_directory(path)
    return directory


def _key(path: Path) -> str:
    # Conservative on case-sensitive volumes; also protects aliases on Windows
    # and the usual case-insensitive macOS volume before a destination exists.
    return unicodedata.normalize("NFC", path.resolve().as_posix()).casefold()


def _requirements(paths: Iterable[Path]) -> Dict[str, bool]:
    required: Dict[str, bool] = {}
    for path in paths:
        resolved = path.resolve()
        for parent in resolved.parents:
            required.setdefault(_key(parent), False)
        required[_key(resolved)] = True
    return required


def _open_lock(path: Path) -> BinaryIO:
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        info = os.fstat(descriptor)
        named = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or info.st_nlink != 1
            or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise OSError(f"Unsafe lock file: {path}")
        return os.fdopen(descriptor, "r+b", buffering=0)
    except BaseException:
        os.close(descriptor)
        raise


class DestinationLock:
    """Own resources until cleanup and every active writing operation finish."""

    def __init__(self, paths: Iterable[Path], parent: Optional[DestinationLock] = None):
        self._mutex = RLock()
        self._parent = parent
        self._handles: Dict[str, BinaryIO] = {}
        self._exclusive: Dict[str, bool] = {}
        self._active = 0
        self._closing = False
        try:
            if parent is None:
                self._directory = _lock_directory()
            self.add_paths(paths)
        except OSError as error:
            self.close()
            raise DestinationLockError(f"Cannot initialize destination locks: {error}") from error
        except BaseException:
            self.close()
            raise

    def add_paths(self, paths: Iterable[Path]) -> None:
        """Acquire additional state paths before their first filesystem access."""
        with self._mutex:
            if self._closing:
                raise DestinationLockError("MirrorURL is closed")
            if self._parent is not None:
                self._parent.add_paths(paths)
                return
            acquired: List[str] = []
            try:
                for key, exclusive in sorted(_requirements(paths).items()):
                    if key in self._handles:
                        if exclusive and not self._exclusive[key]:
                            raise DestinationLockError(f"State path overlaps an ancestor: {key}")
                        continue
                    digest = hashlib.sha256(key.encode("utf-8", "surrogatepass")).hexdigest()
                    handle = _open_lock(self._directory / (digest + ".lock"))
                    try:
                        flags = portalocker.LOCK_EX if exclusive else portalocker.LOCK_SH
                        portalocker.lock(handle, flags | portalocker.LOCK_NB)
                    except BaseException:
                        handle.close()
                        raise
                    self._handles[key] = handle
                    self._exclusive[key] = exclusive
                    acquired.append(key)
            except (OSError, portalocker.exceptions.LockException) as error:
                for key in reversed(acquired):
                    self._handles.pop(key).close()
                    self._exclusive.pop(key)
                raise DestinationLockError(
                    "Destination or shared state is already in use, or cannot be locked: "
                    f"{error}. Close the other mirror and retry. Do not delete lock files."
                ) from error
            except BaseException:
                for key in reversed(acquired):
                    self._handles.pop(key).close()
                    self._exclusive.pop(key)
                raise

    @contextmanager
    def operation(self) -> Iterator[None]:
        with ExitStack() as leases:
            if self._parent is not None:
                leases.enter_context(self._parent.operation())
            with self._mutex:
                if self._closing:
                    raise DestinationLockError("MirrorURL is closed")
                self._active += 1
            try:
                yield
            finally:
                self._leave()

    def _leave(self) -> None:
        with self._mutex:
            self._active -= 1
            if self._closing and self._active == 0:
                for handle in reversed(list(self._handles.values())):
                    handle.close()  # Closing releases the OS lock on all platforms.
                self._handles.clear()
                self._exclusive.clear()

    def close(self, cleanup: Optional[Callable[[], None]] = None) -> None:
        """Reject new work; keep ownership while cleanup or old workers run.

        Cleanup runs once, even when another thread calls close concurrently.
        Timed-out workers retain their operation leases until their finally blocks.
        """
        with self._mutex:
            if self._closing:
                return
            self._closing = True
            self._active += 1
        try:
            with ExitStack() as leases:
                if self._parent is not None:
                    leases.enter_context(self._parent.operation())
                if cleanup is not None:
                    cleanup()
        finally:
            self._leave()


_RUN_LOCK: ContextVar[Optional[DestinationLock]] = ContextVar("mirror_url_run_lock", default=None)


def current_run_lock() -> Optional[DestinationLock]:
    return _RUN_LOCK.get()


@contextmanager
def run_ownership(paths: Iterable[Path]) -> Iterator[None]:
    """Reserve a CLI run's roots and log tree before creating shared logging."""
    guard = DestinationLock(paths)
    token = _RUN_LOCK.set(guard)
    try:
        yield
    finally:
        _RUN_LOCK.reset(token)
        guard.close()


def destination_operation(function):
    """Keep ownership for a complete sync, direct download, scan or cleanup."""

    @wraps(function)
    def wrapped(self, *args, **kwargs):
        guard = getattr(self, "_destination_lock", None)
        if guard is None:
            # Bare mixins used in tests and stand-alone low-level managers
            # have no MirrorURL lifecycle.
            if getattr(self, "_closed", False):
                raise DestinationLockError("MirrorURL is closed")
            return function(self, *args, **kwargs)
        with guard.operation():
            return function(self, *args, **kwargs)

    return wrapped
