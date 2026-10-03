"""Native lock semantics, ownership lifetime, and fail-closed IO errors."""

from __future__ import annotations

import logging
import os
import stat
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import portalocker
import pytest

from mirror_url import DestinationLockError, MirrorConfig, MirrorURL
from mirror_url import destination_lock as locks
from mirror_url.destination_lock import DestinationLock, destination_operation


@pytest.fixture
def directory(tmp_path, monkeypatch):
    directory = tmp_path / "locks"
    directory.mkdir(mode=0o700)
    monkeypatch.setattr(locks, "_lock_directory", lambda: directory)
    return directory


@pytest.mark.parametrize("other", ["same", "child", "parent", "case", "unicode"])
def test_overlapping_trees_are_exclusive_and_released(directory, tmp_path, other):
    destination = tmp_path / ("café" if other == "unicode" else "tree")
    contender = {
        "same": destination / ".",
        "child": destination / "sub",
        "parent": tmp_path,
        "case": tmp_path / "TREE",
        "unicode": tmp_path / "cafe\u0301",
    }[other]
    first = DestinationLock([destination])
    try:
        with pytest.raises(DestinationLockError, match="already in use"):
            DestinationLock([contender])
        assert not destination.exists()
    finally:
        first.close()
    second = DestinationLock([contender])
    second.close()


def test_siblings_can_run_together_and_lock_files_keep_their_inodes(directory, tmp_path):
    first = DestinationLock([tmp_path / "a"])
    second = DestinationLock([tmp_path / "b"])
    files = {path: path.stat().st_ino for path in directory.iterdir()}
    first.close()
    first.close()
    second.close()
    third = DestinationLock([tmp_path / "a", tmp_path / "a" / "nested"])
    third.close()
    assert all(path.stat().st_ino == inode for path, inode in files.items())


def test_operation_leases_keep_lock_after_cleanup_returns(directory, tmp_path):
    root = tmp_path / "tree"
    owner = DestinationLock([root])
    started, finish = threading.Event(), threading.Event()

    def abandoned_writer():
        with owner.operation():
            started.set()
            assert finish.wait(5)

    worker = threading.Thread(target=abandoned_writer)
    worker.start()
    assert started.wait(5)
    cleanup = Mock()
    try:
        owner.close(cleanup)
        owner.close(cleanup)
        cleanup.assert_called_once()
        with pytest.raises(DestinationLockError):
            DestinationLock([root])
        with pytest.raises(DestinationLockError, match="closed"):
            with owner.operation():
                pytest.fail("New work started after cleanup")
        with pytest.raises(DestinationLockError, match="closed"):
            owner.add_paths([root / "new"])
    finally:
        finish.set()
        worker.join(timeout=5)
    assert not worker.is_alive()
    DestinationLock([root]).close()


def test_exception_paths_release_operation_and_cleanup_ownership(directory, tmp_path):
    owner = DestinationLock([tmp_path])
    with pytest.raises(ValueError):
        with owner.operation():
            raise ValueError("download failed")
    with pytest.raises(OSError):
        owner.close(Mock(side_effect=OSError("cleanup failed")))
    DestinationLock([tmp_path]).close()


def test_failed_extension_rolls_back_new_handles_and_preserves_old_lock(directory, tmp_path):
    destination = tmp_path / "z" / "tree"
    owner = DestinationLock([destination])
    try:
        with pytest.raises(DestinationLockError, match="ancestor"):
            owner.add_paths([tmp_path / "a", tmp_path])
        independent = DestinationLock([tmp_path / "a"])
        independent.close()
        with pytest.raises(DestinationLockError):
            DestinationLock([destination])
    finally:
        owner.close()


@pytest.mark.parametrize("error", [OSError("disk failed"), RuntimeError("unexpected")])
def test_failed_lock_acquisition_closes_every_new_handle(directory, tmp_path, monkeypatch, error):
    actual = portalocker.lock
    opened = []
    open_lock = locks._open_lock

    def record(path):
        handle = open_lock(path)
        opened.append(handle)
        return handle

    def fail_second(handle, flags):
        if len(opened) == 2:
            raise error
        actual(handle, flags)

    monkeypatch.setattr(locks, "_open_lock", record)
    monkeypatch.setattr(portalocker, "lock", fail_second)
    expected = DestinationLockError if isinstance(error, OSError) else RuntimeError
    with pytest.raises(expected):
        DestinationLock([tmp_path / "tree"])
    assert len(opened) == 2 and all(handle.closed for handle in opened)
    monkeypatch.setattr(portalocker, "lock", actual)
    DestinationLock([tmp_path / "tree"]).close()


def test_directory_failure_is_reported_as_public_lock_error(monkeypatch):
    monkeypatch.setattr(locks, "_lock_directory", Mock(side_effect=PermissionError("denied")))
    with pytest.raises(DestinationLockError, match="initialize"):
        DestinationLock([Path("unused")])


@pytest.mark.parametrize("kind", ["file", "symlink", "permission", "owner"])
def test_lock_directory_rejects_unsafe_existing_state(tmp_path, monkeypatch, kind):
    path = tmp_path / "directory"
    if kind == "file":
        path.write_text("important")
    elif kind == "symlink":
        path.symlink_to(tmp_path, target_is_directory=True)
    else:
        path.mkdir(mode=0o700)
        info = path.lstat()
        changed = SimpleNamespace(
            st_mode=(info.st_mode | 0o020) if kind == "permission" else info.st_mode,
            st_uid=123 if kind == "owner" else 42,
        )
        monkeypatch.setattr(Path, "lstat", lambda _: changed)
        monkeypatch.setattr(locks.os, "getuid", lambda: 42, raising=False)
    with pytest.raises(OSError):
        locks._check_directory(path, posix=True)


def test_stable_home_directory_ignores_temporary_directory_settings(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("TMPDIR", str(tmp_path / "unrelated"))
    directory = locks._lock_directory()
    assert directory == tmp_path / ".mirror-url" / "locks-v1"
    assert locks._lock_directory() == directory
    locks._check_directory(directory, posix=False)  # Windows uses the user's directory ACL.


@pytest.mark.parametrize("fault", ["descriptor_type", "named_type", "links", "identity", "fdopen"])
def test_unsafe_lock_file_or_failed_stream_wrapper_closes_descriptor(tmp_path, monkeypatch, fault):
    path = tmp_path / "resource.lock"
    original_fstat, original_lstat = os.fstat, Path.lstat
    closed = []
    original_close = os.close

    def close(descriptor):
        original_close(descriptor)
        closed.append(descriptor)

    def modified(info, source):
        return SimpleNamespace(
            st_mode=stat.S_IFDIR if fault == source + "_type" else info.st_mode,
            st_nlink=2 if fault == "links" else info.st_nlink,
            st_dev=info.st_dev,
            st_ino=info.st_ino + (fault == "identity" and source == "named"),
        )

    monkeypatch.setattr(os, "close", close)
    monkeypatch.setattr(os, "fstat", lambda fd: modified(original_fstat(fd), "descriptor"))
    monkeypatch.setattr(Path, "lstat", lambda self: modified(original_lstat(self), "named"))
    if fault == "fdopen":
        monkeypatch.setattr(os, "fdopen", Mock(side_effect=OSError("cannot wrap descriptor")))
    with pytest.raises(OSError):
        locks._open_lock(path)
    assert len(closed) == 1
    with pytest.raises(OSError):
        original_fstat(closed[0])


def test_symbolic_lock_file_never_modifies_its_target(tmp_path):
    important = tmp_path / "important"
    important.write_bytes(b"preserve")
    path = tmp_path / "resource.lock"
    path.symlink_to(important)
    with pytest.raises(OSError):
        locks._open_lock(path)
    assert important.read_bytes() == b"preserve"


def test_decorator_guards_work_and_rejects_use_after_close(directory, tmp_path):
    class Writer:
        @destination_operation
        def write(self, value):
            return value

    bare = Writer()
    assert bare.write(1) == 1
    bare._closed = True
    with pytest.raises(DestinationLockError, match="closed"):
        bare.write(2)
    writer = Writer()
    writer._destination_lock = DestinationLock([tmp_path])
    assert writer.write(3) == 3
    writer._destination_lock.close()
    with pytest.raises(DestinationLockError, match="closed"):
        writer.write(4)


@pytest.mark.parametrize("error", [RuntimeError("init failed"), KeyboardInterrupt()])
def test_failed_constructor_releases_destination_ownership(directory, tmp_path, monkeypatch, error):
    config = MirrorConfig(
        base_url="https://example.com/", dest_path=tmp_path / "mirror", log_path=tmp_path / "logs"
    )
    monkeypatch.setattr(MirrorURL, "_initialize", Mock(side_effect=error))
    with pytest.raises(type(error)):
        MirrorURL(config)
    DestinationLock([config.dest_path]).close()


def test_cli_scope_allows_sequential_children_but_keeps_external_ownership(directory, tmp_path):
    assert locks.current_run_lock() is None
    with locks.run_ownership([tmp_path / "tree", tmp_path / "logs"]):
        parent = locks.current_run_lock()
        child = DestinationLock([tmp_path / "tree"], parent=parent)
        with child.operation():
            child.add_paths([tmp_path / "logs" / "cache.json"])
        child.close()
        child.close()
        with pytest.raises(DestinationLockError):
            DestinationLock([tmp_path / "tree"])
        with pytest.raises(DestinationLockError, match="closed"):
            child.add_paths([tmp_path / "new"])
        with pytest.raises(DestinationLockError, match="closed"):
            with child.operation():
                pass
        DestinationLock([tmp_path / "tree"], parent=parent).close()
    assert locks.current_run_lock() is None
    DestinationLock([tmp_path / "tree"]).close()


def test_parent_close_retains_a_child_worker_lease(directory, tmp_path):
    parent = DestinationLock([tmp_path])
    child = DestinationLock([tmp_path / "tree"], parent=parent)
    with child.operation():
        child.close()
        parent.close()
        with pytest.raises(DestinationLockError):
            DestinationLock([tmp_path / "tree"])
    DestinationLock([tmp_path]).close()


def test_failed_cli_scope_restores_context_and_releases_ownership(directory, tmp_path):
    with pytest.raises(ValueError):
        with locks.run_ownership([tmp_path]):
            raise ValueError("CLI failed")
    assert locks.current_run_lock() is None
    DestinationLock([tmp_path]).close()


def test_main_thread_can_restore_signals_after_background_cleanup(directory, tmp_path):
    import signal

    mirror = MirrorURL.__new__(MirrorURL)
    mirror._destination_lock = DestinationLock([tmp_path])
    mirror._previous_signal_handlers = {}
    mirror._cleanup_resources = Mock()
    previous = {signum: signal.getsignal(signum) for signum in (signal.SIGINT, signal.SIGTERM)}
    mirror.install_signal_handlers()
    try:
        worker = threading.Thread(target=mirror.cleanup)
        worker.start()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert signal.getsignal(signal.SIGINT) == mirror._signal_handler
        mirror.cleanup()
        assert all(signal.getsignal(signum) == handler for signum, handler in previous.items())
        mirror._cleanup_resources.assert_called_once()
    finally:
        mirror.cleanup()


def test_delayed_log_record_cannot_reopen_a_closed_file(tmp_path):
    path = tmp_path / "mirror.log"
    handler = locks.MirrorFileHandler(path, mode="a")
    handler.handle(logging.LogRecord("open", logging.INFO, "", 0, "owned", (), None))
    handler.close()
    before = path.read_bytes()
    handler.handle(logging.LogRecord("closed", logging.INFO, "", 0, "too late", (), None))
    assert path.read_bytes() == before and handler.stream is None
