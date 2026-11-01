"""Terminal storage errors stop new work without publishing incomplete files."""

from __future__ import annotations

import asyncio
import builtins
import errno
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mirror_url import transfers
from mirror_url.capacity import CapacityBudget, CapacityError, is_capacity_error
from mirror_url.download import PartialDownloadManager
from test_capacity_budget import private_home as private_home
from test_destination_lock import directory as directory
from test_download_failure_contract import manager as manager
from test_download_failure_contract import range_handler
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_preservation_backend_matrix import configuration, execute
from test_preservation_backend_matrix import remote as remote
from test_transfer_storage_lifecycle import make_download
from test_whole_file_disk_workers import engine as engine

MIB = 1024**2
PAYLOAD = b"R" * (2 * MIB + 17)
ORIGINAL = b"previous complete scientific file"


class StorageFault:
    """Inject at the real payload handle after bytes have already been written."""

    def __init__(self, target, stage, code):
        self.target, self.stage, self.code = target, stage, code
        self.lock = threading.RLock()
        self.written = self.failures = 0
        self.descriptors = set()

    def wrap(self, stream, path):
        path = Path(path)
        payload = path.is_relative_to(self.target) and (
            path.name.endswith(".mirror-partial")
            or path.name.startswith("chunk_")
            and path.suffix == ".part"
            or path.name in ("staging.streaming", "staging.assembling")
        )
        if not payload:
            return stream
        fault = self
        with self.lock:
            self.descriptors.add(stream.fileno())

        class Stream:
            def __getattr__(self, name):
                return getattr(stream, name)

            def __enter__(self):
                stream.__enter__()
                return self

            def __exit__(self, *args):
                with fault.lock:
                    if not stream.closed:
                        fault.descriptors.discard(stream.fileno())
                return stream.__exit__(*args)

            def close(self):
                with fault.lock:
                    if not stream.closed:
                        fault.descriptors.discard(stream.fileno())
                return stream.close()

            def write(self, data):
                with fault.lock:
                    if fault.stage == "write" and fault.written >= 65536:
                        fault.failures += 1
                        raise OSError(fault.code, "injected storage exhaustion")
                    count = stream.write(data)
                    fault.written += count
                    return count

        return Stream()

    def install(self, monkeypatch):
        import mirror_url._core.downloads as whole
        import mirror_url.download as chunks

        owned_open, path_open, ordinary_open, sync = (
            whole.open_owned_partial,
            Path.open,
            builtins.open,
            os.fsync,
        )
        monkeypatch.setattr(
            whole,
            "open_owned_partial",
            lambda path, *args: self.wrap(owned_open(path, *args), path),
        )
        monkeypatch.setattr(
            Path,
            "open",
            lambda path, *args, **kwargs: self.wrap(path_open(path, *args, **kwargs), path),
        )
        monkeypatch.setattr(
            chunks,
            "open",
            lambda path, *args, **kwargs: self.wrap(ordinary_open(path, *args, **kwargs), path),
            raising=False,
        )

        def fsync(fd):
            with self.lock:
                if self.stage == "flush" and fd in self.descriptors and self.written >= 65536:
                    self.failures += 1
                    raise OSError(self.code, "injected final flush exhaustion")
            return sync(fd)

        monkeypatch.setattr(os, "fsync", fsync)
        monkeypatch.setattr(chunks, "exponential_backoff", lambda *args: 0)


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("method", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize("stage", ["write", "flush"])
@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
def test_exhaustion_preserves_files_and_stops_later_requests(
    tmp_path, private_home, remote, monkeypatch, mode, backend, method, stage, code
):
    remote.files = dict.fromkeys(("a.bin", "b.bin", "c.bin"), PAYLOAD)
    config = configuration(tmp_path, remote, mode, backend, method, remote.files)
    config.max_concurrent_downloads = 1
    config.dest_path.mkdir()
    for name in remote.files:
        (config.dest_path / name).write_bytes(ORIGINAL)
    fault = StorageFault(config.dest_path, stage, code)
    fault.install(monkeypatch)
    assert not execute(config)
    assert fault.written >= 65536 and fault.failures
    assert all((config.dest_path / name).read_bytes() == ORIGINAL for name in remote.files)
    gets = [
        (name, headers.get("Range"))
        for verb, name, headers in remote.requests
        if verb == "GET" and name in remote.files
    ]
    assert len({name for name, _ in gets}) == 1
    # A terminal capacity error must neither retry chunks nor fall back to a GET.
    assert len(gets) == len(set(gets))
    if method != "sequential":
        assert all(byte_range is not None for _, byte_range in gets)
    if mode == "mirror" and backend == "httpx" and method == "sequential":
        partial = PartialDownloadManager(config.dest_path).get_partial_path(
            config.dest_path / gets[0][0]
        )
        assert partial.exists()
        assert PAYLOAD.startswith(partial.read_bytes())


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("known", [True, False])
@pytest.mark.parametrize("expired", [True, False])
async def test_stream_capacity_checks_follow_interval(
    engine, private_home, monkeypatch, backend, known, expired
):
    sample = Mock(side_effect=[SimpleNamespace(free=20 * MIB), SimpleNamespace(free=0)])
    budget = CapacityBudget(engine.target, 1, sample=sample)
    engine.capacity, engine._owns_capacity = budget, False
    engine.config.backend = backend
    local = engine.target / "a"
    local.write_bytes(ORIGINAL)
    payload = b"complete downloaded bytes"

    @asynccontextmanager
    async def opened(self, url):
        async def blocks():
            yield payload[:4]
            if expired:
                budget._sampled -= 31
            yield payload[4:]

        yield 200, {"Content-Length": str(len(payload))} if known else {}, blocks()

    monkeypatch.setattr(transfers._HTTPXBackend, "open", opened)
    monkeypatch.setattr(transfers._AIOHTTPBackend, "open", opened)
    try:
        results = await engine.run([(engine.scope + "a", local)])
        assert results[0].success is not expired
        assert sample.call_count == (2 if expired else 1)
        assert local.read_bytes() == (ORIGINAL if expired else payload)
    finally:
        budget.close()


@pytest.mark.parametrize(
    "error",
    [OSError(errno.ENOSPC, "full"), OSError(errno.EDQUOT, "quota"), CapacityError("closed")],
)
def test_failure_latch_rejects_already_funded_writes(private_home, error):
    sample = Mock(return_value=SimpleNamespace(free=20 * MIB))
    budget = CapacityBudget(private_home, 1, sample=sample)
    key = private_home / "a"
    try:
        budget.reserve(key, MIB)
        assert budget.fail(error)
        assert budget.fail(OSError(errno.EDQUOT, "later failure"))
        for check in (budget.check, lambda: budget.ensure(key, 1), lambda: budget.reserve(key, 1)):
            with pytest.raises(CapacityError) as stopped:
                check()
            assert str(error) in str(stopped.value)
        sample.assert_called_once()
    finally:
        budget.close()


@pytest.mark.parametrize(
    "error", [OSError(errno.EIO, "other I/O failure"), ValueError("invalid response")]
)
def test_non_capacity_failure_does_not_close_batch(private_home, error):
    budget = CapacityBudget(private_home, 1)
    try:
        assert not is_capacity_error(error) and not budget.fail(error)
        budget.check()
    finally:
        budget.close()
    with pytest.raises(CapacityError, match="closed"):
        budget.check()


def test_terminal_failure_prevents_chunk_recovery(manager, private_home, monkeypatch):
    download = make_download(manager)
    budget = CapacityBudget(private_home, 1)
    manager.capacity = budget
    budget.fail(OSError(errno.ENOSPC, "full"))
    request = Mock(side_effect=AssertionError("no chunk request after exhaustion"))
    monkeypatch.setattr(manager.connection_manager, "request", request)
    try:
        assert not manager._retry_failed_chunks(download)
        request.assert_not_called()
        assert download.final_path.read_bytes() == b"original"
    finally:
        budget.close()


def test_streaming_preallocation_exhaustion_cannot_fall_back(manager, private_home, monkeypatch):
    budget = CapacityBudget(private_home, 1)
    manager.capacity, manager.use_streaming = budget, True
    manager.connection_manager.request.side_effect = range_handler
    final = manager.mirror.target_dir / "a"
    final.write_bytes(ORIGINAL)
    real_fdopen = os.fdopen

    def fdopen(*args, **kwargs):
        stream = real_fdopen(*args, **kwargs)
        stream.truncate = Mock(side_effect=OSError(errno.EDQUOT, "preallocation quota"))
        return stream

    monkeypatch.setattr("mirror_url.download.os.fdopen", fdopen)
    try:
        with pytest.raises(OSError, match="preallocation quota"):
            manager.create_chunks("https://example.com/root/a", final, 6)
        with pytest.raises(CapacityError, match="preallocation quota"):
            budget.check()
        assert final.read_bytes() == ORIGINAL
        assert not list(final.parent.glob("*.streaming"))
    finally:
        budget.close()


def test_final_assembly_exhaustion_closes_shared_budget(manager, private_home, monkeypatch):
    download = make_download(manager)
    manager.MMAP_MAX_FILE_SIZE = 0
    budget = CapacityBudget(private_home, 1)
    manager.capacity = budget
    budget.reserve(download.final_path, 12)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(
                "mirror_url.download.os.fsync",
                Mock(side_effect=[None, OSError(errno.ENOSPC, "assembly full")]),
            )
            assert not manager.assemble_file(download)
        assert download.final_path.read_bytes() == b"original"
        with pytest.raises(CapacityError, match="assembly full"):
            budget.check()
    finally:
        budget.release(download.final_path)
        budget.close()


@pytest.mark.asyncio
async def test_cleanup_error_cannot_hide_terminal_write_failure(engine, private_home, monkeypatch):
    budget = CapacityBudget(engine.target, 1)
    engine.capacity, engine._owns_capacity = budget, False
    local = engine.target / "a"
    local.write_bytes(ORIGINAL)

    @asynccontextmanager
    async def opened(self, url):
        async def blocks():
            yield b"prefix"
            raise OSError(errno.ENOSPC, "original payload full")

        yield 200, {"Content-Length": "10"}, blocks()

    release = engine.scratch.release

    def failed_cleanup(work):
        release(work)
        return False

    monkeypatch.setattr(transfers._HTTPXBackend, "open", opened)
    monkeypatch.setattr(engine.scratch, "release", failed_cleanup)
    try:
        results = await engine.run([(engine.scope + "a", local)])
        assert not results[0].success and "cleanup failed" in results[0].error
        assert local.read_bytes() == ORIGINAL
        with pytest.raises(CapacityError, match="original payload full"):
            budget.check()
    finally:
        budget.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
async def test_new_run_gets_a_fresh_capacity_budget(engine, private_home, monkeypatch, backend):
    engine.config.backend = backend
    local = engine.target / "a"
    local.write_bytes(ORIGINAL)
    attempts = []

    @asynccontextmanager
    async def opened(self, url):
        attempts.append(url)

        async def blocks():
            if len(attempts) == 1:
                yield PAYLOAD[:4]
                raise OSError(errno.ENOSPC, "first run full")
            yield PAYLOAD

        yield 200, {"Content-Length": str(len(PAYLOAD))}, blocks()

    monkeypatch.setattr(transfers._HTTPXBackend, "open", opened)
    monkeypatch.setattr(transfers._AIOHTTPBackend, "open", opened)
    first = await engine.run([(engine.scope + "a", local)])
    assert not first[0].success and local.read_bytes() == ORIGINAL
    second = await engine.run([(engine.scope + "a", local)])
    assert second[0].success and local.read_bytes() == PAYLOAD
    assert len(attempts) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("code", [errno.ENOSPC, errno.EDQUOT])
async def test_active_funded_peer_stops_and_pending_file_never_opens(
    engine, private_home, monkeypatch, backend, code
):
    budget = CapacityBudget(engine.target, 2)
    engine.capacity, engine._owns_capacity = budget, False
    engine.config.backend = backend
    both_open, failed = asyncio.Event(), asyncio.Event()
    opened_urls = []
    report_error = budget.fail

    def fail(error):
        terminal = report_error(error)
        if terminal:
            failed.set()
        return terminal

    @asynccontextmanager
    async def opened(self, url):
        opened_urls.append(url)
        if len(opened_urls) == 2:
            both_open.set()

        async def blocks():
            await asyncio.wait_for(both_open.wait(), 5)
            yield b"part"
            if url.endswith("a"):
                raise OSError(code, "active writer exhausted capacity")
            await asyncio.wait_for(failed.wait(), 5)
            yield b"tail"

        yield 200, {"Content-Length": "8"}, blocks()

    monkeypatch.setattr(budget, "fail", fail)
    monkeypatch.setattr(transfers._HTTPXBackend, "open", opened)
    monkeypatch.setattr(transfers._AIOHTTPBackend, "open", opened)
    items = [(engine.scope + name, engine.target / name) for name in ("a", "b", "c")]
    for _, local in items:
        local.write_bytes(ORIGINAL)
    try:
        results = await asyncio.wait_for(engine.run(items), 10)
        assert len(results) == 3 and all(not result.success for result in results)
        assert opened_urls == [engine.scope + "a", engine.scope + "b"]
        assert all(local.read_bytes() == ORIGINAL for _, local in items)
        assert not engine.scratch._active
    finally:
        budget.close()


def test_chunk_admission_os_error_latches_batch(manager, mirror, private_home, monkeypatch):
    budget = CapacityBudget(mirror.target_dir, 1)
    mirror._capacity_budget = budget
    mirror.parallel_manager = manager
    local = mirror.target_dir / "a"
    local.write_bytes(ORIGINAL)
    monkeypatch.setattr(
        budget, "reserve", Mock(side_effect=OSError(errno.ENOSPC, "admission full"))
    )
    try:
        assert not mirror.download_file_with_resume("https://example.com/root/a", local, 6)
        assert local.read_bytes() == ORIGINAL
        with pytest.raises(CapacityError, match="admission full"):
            budget.check()
    finally:
        budget.close()


def test_partial_creation_quota_error_latches_batch(mirror, private_home, monkeypatch):
    budget = CapacityBudget(mirror.target_dir, 1)
    mirror._capacity_budget = budget
    local = mirror.target_dir / "a"
    local.write_bytes(ORIGINAL)
    monkeypatch.setattr(
        mirror.partial_manager,
        "register_partial",
        Mock(side_effect=OSError(errno.EDQUOT, "partial quota")),
    )
    try:
        assert not mirror._download_file_single("https://example.com/root/a", local)
        mirror.connection_manager.request.assert_not_called()
        assert local.read_bytes() == ORIGINAL
        with pytest.raises(CapacityError, match="partial quota"):
            budget.check()
    finally:
        budget.close()
