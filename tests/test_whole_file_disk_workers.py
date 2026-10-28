"""Whole-file disk work yields the loop without abandoning owned operations."""

import asyncio
import hashlib
import os
import threading
from contextlib import asynccontextmanager
from pathlib import PureWindowsPath

import httpx
import pytest

from mirror_url import MirrorConfig, download_integrity, transfers
from mirror_url.destination_lock import DestinationLock
from mirror_url.download import PartialDownloadManager
from mirror_url.scratch import OwnedScratch
from test_destination_lock import directory as directory

BASE = "http://files.example/root/"
PAYLOAD = b"complete downloaded bytes"


@pytest.fixture
def engine(directory, tmp_path, monkeypatch):
    class Backend:
        retryable = (httpx.TransportError,)

        def __init__(self, *_):
            pass

        async def close(self):
            pass

        @asynccontextmanager
        async def open(self, _):
            async def blocks():
                yield PAYLOAD

            yield (
                200,
                {
                    "Content-Length": str(len(PAYLOAD)),
                    "Last-Modified": "Mon, 19 Oct 2026 00:00:00 GMT",
                },
                blocks(),
            )

    monkeypatch.setattr(transfers, "_HTTPXBackend", Backend)
    target = tmp_path / "dest"
    target.mkdir()
    config = MirrorConfig(
        base_url=BASE,
        dest_path=target,
        log_path=tmp_path / "logs",
        verify_content=True,
        max_concurrent_downloads=2,
        request_delay=0,
        requests_per_second=0,
    )
    guard = DestinationLock([target])
    scratch = OwnedScratch(target, PartialDownloadManager(target)._state_directory(), None, guard)
    owned = transfers.AsyncTransfers(config, target, BASE, scratch)
    yield owned
    guard.close()


def gate_stage(engine, stage, monkeypatch, entered, release, observed_threads):
    def pause():
        observed_threads.append(threading.get_ident())
        entered.set()
        assert release.wait(5), "test must release the owned disk operation"

    if stage == "prepare":
        original = engine.scratch.staging

        def staging(*args):
            path = original(*args)
            pause()
            return path

        monkeypatch.setattr(engine.scratch, "staging", staging)
    elif stage == "flush":
        original = transfers.os.fsync

        def flush(fd):
            if os.fstat(fd).st_size == len(PAYLOAD):
                pause()
            return original(fd)

        monkeypatch.setattr(transfers.os, "fsync", flush)
    elif stage == "hash":
        original = transfers.file_sha256

        def hashing(*args):
            pause()
            return original(*args)

        monkeypatch.setattr(transfers, "file_sha256", hashing)
    elif stage == "publish":
        original = engine._publish

        def publish(*args, **kwargs):
            pause()
            return original(*args, **kwargs)

        monkeypatch.setattr(engine, "_publish", publish)
    else:
        assert stage == "cleanup"
        original = engine.scratch.release

        def cleanup(*args):
            pause()
            return original(*args)

        monkeypatch.setattr(engine.scratch, "release", cleanup)


@pytest.mark.parametrize("stage", ["prepare", "flush", "hash", "publish", "cleanup"])
async def test_disk_stages_leave_event_loop_responsive(engine, monkeypatch, stage):
    entered, release = threading.Event(), threading.Event()
    observed_threads, published = [], []
    gate_stage(engine, stage, monkeypatch, entered, release, observed_threads)
    local = engine.target / "a"
    task = asyncio.create_task(engine.run([(BASE + "a", local)], published.append))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        assert len(observed_threads) == 1
        assert observed_threads[0] != threading.get_ident()
        assert not task.done()
        # The assertion above and this callback must execute before disk IO resumes.
        await asyncio.sleep(0)
    finally:
        release.set()
        results = await task
    assert results[0].success and published == results
    assert local.read_bytes() == PAYLOAD
    assert published[0].sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    assert not engine.scratch._active and not list(engine.target.rglob("work_*"))


@pytest.mark.parametrize("stage", ["prepare", "flush", "hash", "publish", "receipt", "cleanup"])
async def test_repeated_cancellation_drains_disk_work_and_keeps_publication_hook(
    engine, monkeypatch, stage
):
    entered, release = threading.Event(), threading.Event()
    observed_threads, published = [], []
    if stage != "receipt":
        gate_stage(engine, stage, monkeypatch, entered, release, observed_threads)

    def record(result):
        published.append(result)
        if stage == "receipt":
            observed_threads.append(threading.get_ident())
            entered.set()
            assert release.wait(5)

    local = engine.target / "a"
    local.write_bytes(b"old file")
    original = transfers._identity(local)
    task = asyncio.create_task(engine.run([(BASE + "a", local)], record))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        assert observed_threads[0] != threading.get_ident()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done(), "the disk worker still owns its file and workspace"
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    if stage in ("receipt", "cleanup"):
        assert local.read_bytes() == PAYLOAD and len(published) == 1
        assert published[0].sha256 == hashlib.sha256(PAYLOAD).hexdigest()
    else:
        assert local.read_bytes() == b"old file" and transfers._identity(local) == original
        assert not published
    assert not engine.scratch._active and not list(engine.target.rglob("work_*"))
    assert engine.closed_connection_pools == 1
    engine.scratch._destination_lock.close()
    next_owner = DestinationLock([engine.target])
    next_owner.close()


async def test_another_file_can_finish_while_one_file_hash_is_blocked(engine, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = transfers.file_sha256
    blocked = engine.target / "a"
    other = engine.target / "b"
    # Staging filenames are deliberately anonymous; identify the first admitted hash.
    lock = threading.Lock()
    first = True

    def hashing(*args):
        nonlocal first
        with lock:
            wait = first
            first = False
        if wait:
            entered.set()
            assert release.wait(5)
        return original(*args)

    monkeypatch.setattr(transfers, "file_sha256", hashing)
    loop = asyncio.get_running_loop()
    completed = asyncio.Event()
    published = []

    def record(result):
        published.append(result)
        loop.call_soon_threadsafe(completed.set)

    task = asyncio.create_task(engine.run([(BASE + "a", blocked), (BASE + "b", other)], record))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        await asyncio.wait_for(completed.wait(), 3)
        assert len(published) == 1 and not task.done()
        assert published[0].path.read_bytes() == PAYLOAD
    finally:
        release.set()
        results = await task
    assert all(result.success for result in results) and len(published) == 2


async def test_payload_handle_is_closed_before_atomic_publication(engine, monkeypatch):
    streams = []
    original_open = transfers.Path.open
    original_publish = engine._publish

    def opening(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        if path.name == "staging.streaming" and args == ("xb",):
            streams.append(stream)
        return stream

    def publish(*args, **kwargs):
        assert len(streams) == 1 and streams[0].closed
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(transfers.Path, "open", opening)
    monkeypatch.setattr(engine, "_publish", publish)
    results = await engine.run([(BASE + "a", engine.target / "a")])
    assert results[0].success


async def test_flush_failure_keeps_old_file_and_cleans_owned_workspace(engine, monkeypatch):
    original_fsync = transfers.os.fsync

    def failing(fd):
        if os.fstat(fd).st_size == len(PAYLOAD):
            raise OSError("disk flush failed")
        return original_fsync(fd)

    monkeypatch.setattr(transfers.os, "fsync", failing)
    local = engine.target / "a"
    local.write_bytes(b"old file")
    original = transfers._identity(local)
    published = []
    results = await engine.run([(BASE + "a", local)], published.append)
    assert not results[0].success and results[0].error == "disk flush failed"
    assert local.read_bytes() == b"old file" and transfers._identity(local) == original
    assert not published and not engine.scratch._active
    assert not list(engine.target.rglob("work_*"))


@pytest.mark.parametrize("payload", [b"", b"downloaded bytes"])
async def test_receipt_hook_transport_error_never_repeats_a_published_get(
    engine, monkeypatch, payload
):
    requests = []

    @asynccontextmanager
    async def opened(self, url):
        requests.append(url)

        async def blocks():
            yield payload

        yield 200, {"Content-Length": str(len(payload))}, blocks()

    monkeypatch.setattr(transfers._HTTPXBackend, "open", opened)
    engine.config.max_retries = 1
    engine.config.retry_delay = 0
    calls = []

    def published(result):
        calls.append(result)
        raise httpx.ReadError("receipt hook failed")

    url, local = BASE + "a", engine.target / "a"
    results = await engine.run([(url, local)], published)
    assert requests == [url], "A publication must never be repeated as a network retry"
    assert len(calls) == 1 and local.read_bytes() == payload
    assert not results[0].success and results[0].error == "receipt hook failed"
    assert not engine.scratch._active and not list(engine.target.rglob("work_*"))


@pytest.mark.parametrize(
    "planned,checked",
    [
        ("C:/mirror/c2/a.fts", "//?/C:/mirror/c2/a.fts"),
        ("//?/C:/mirror/c2/a.fts", "C:/mirror/c2/a.fts"),
        ("//server/share/mirror/c2/a.fts", "//?/UNC/server/share/mirror/c2/a.fts"),
        ("//?/UNC/server/share/mirror/c2/a.fts", "//server/share/mirror/c2/a.fts"),
    ],
)
def test_publication_accepts_equivalent_resolved_windows_spellings(
    engine, monkeypatch, planned, checked
):
    staging = engine.scratch.staging(engine.target / "a", "streaming")
    staging.write_bytes(PAYLOAD)
    inode = staging.stat()
    planned, checked = PureWindowsPath(planned), PureWindowsPath(checked)
    engine.target = planned.parent
    original_identity = download_integrity.file_identity
    monkeypatch.setattr(
        download_integrity,
        "file_identity",
        lambda path, **kwargs: None if path is planned else original_identity(path, **kwargs),
    )
    monkeypatch.setattr(PureWindowsPath, "is_symlink", lambda path: False, raising=False)
    monkeypatch.setattr(transfers.PathSafety, "safe_join", lambda *args, **kwargs: checked)
    replacements = []
    monkeypatch.setattr(transfers.os, "replace", lambda *paths: replacements.append(paths))
    try:
        result = engine._publish(
            BASE + "a",
            planned,
            staging,
            len(PAYLOAD),
            None,
            None,
            None,
            (inode.st_dev, inode.st_ino),
        )
        assert result.success and replacements == [(staging, planned)]
    finally:
        assert engine.scratch.release(staging.parent)
