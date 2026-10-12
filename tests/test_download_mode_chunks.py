"""Known-URL modes reuse verified range transfers without discovery or freshness."""

from __future__ import annotations

import asyncio
import hashlib
import json
import socket
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

from mirror_url import MirrorConfig, cli, download_url_list, transfers
from mirror_url.destination_lock import DestinationLock
from mirror_url.download import PartialDownloadManager
from mirror_url.exceptions import ConfigError
from mirror_url.progress import DownloadProgress
from mirror_url.scratch import OwnedScratch
from mirror_url.security import SecurityValidator
from mirror_url.transfers import AsyncTransfers, _identity
from test_transfer_backends import assert_no_scratch, job


@pytest.fixture(params=["parallel_downloads", "streaming_parallel"])
def chunk_mode(request):
    return request.param


@pytest.fixture
def ranges(monkeypatch):
    payload = b"a" * 1024**2 + b"b" * 1024**2 + b"c" * 1024**2
    state = SimpleNamespace(
        payload=payload,
        requests=[],
        counts=Counter(),
        fault=None,
        active=0,
        maximum=0,
        together=threading.Event(),
        entered=threading.Event(),
        release=threading.Event(),
        hold=False,
        threshold=3,
    )
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_):
            pass

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body):
            range_header = self.headers.get("Range")
            with lock:
                state.requests.append((self.command, self.path, dict(self.headers)))
                state.counts[(self.command, self.path, range_header)] += 1
                attempt = state.counts[(self.command, self.path, range_header)]
            code, data = 200, payload
            headers = {
                "ETag": '"v1"',
                "Accept-Ranges": "bytes",
                "Last-Modified": "Wed, 07 Oct 2026 00:00:00 GMT",
            }
            if state.fault == "weak":
                headers["ETag"] = 'W/"v1"'
            if state.fault == "no-ranges":
                headers.pop("Accept-Ranges")
            if not body and state.fault == "no-head":
                code = 405
            if state.fault in ("redirect", "outside", "loop") and self.path.endswith("/a"):
                code, data = 302, b""
                headers["Location"] = {"redirect": "renamed", "outside": "/outside/a", "loop": "a"}[
                    state.fault
                ]
            elif body and range_header:
                start, end = map(int, range_header.removeprefix("bytes=").split("-"))
                data = payload[start : end + 1]
                code = 206
                headers["Content-Range"] = f"bytes {start}-{end}/{len(payload)}"
                if state.fault == "status":
                    code = 200
                elif state.fault == "etag":
                    headers["ETag"] = '"v2"'
                elif state.fault == "range":
                    headers["Content-Range"] = f"bytes {start + 1}-{end}/{len(payload)}"
                elif state.fault == "encoding":
                    headers["Content-Encoding"] = "gzip"
                if state.fault == "truncate" or state.fault == "recover" and attempt == 1:
                    headers["Content-Length"] = str(len(data))
                    headers["Connection"] = "close"
                    data = data[:31]
            headers.setdefault("Content-Length", str(len(data)))
            self.send_response(code)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            if body:
                try:
                    if range_header:
                        with lock:
                            state.active += 1
                            state.maximum = max(state.maximum, state.active)
                            if state.active >= state.threshold:
                                state.together.set()
                        state.entered.set()
                        if state.hold:
                            assert state.release.wait(5)
                        elif state.fault is None:
                            assert state.together.wait(5)
                    self.wfile.write(data)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
                finally:
                    if range_header:
                        with lock:
                            state.active -= 1
            if headers.get("Connection") == "close":
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.base = f"http://files.example:{server.server_port}/root/"
    monkeypatch.setattr(SecurityValidator, "resolve_and_validate_hostname", lambda _: "127.0.0.1")
    monkeypatch.setattr(socket, "gethostbyname", lambda _: "127.0.0.1")
    monkeypatch.setattr("mirror_url.download.exponential_backoff", lambda *_: 0)
    yield state
    state.release.set()
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)
    assert not thread.is_alive() and state.active == 0


def configuration(tmp_path, ranges, chunk_mode, **options):
    return job(
        tmp_path,
        ranges,
        **{chunk_mode: True, "min_chunk_size_mb": 1, "max_chunks_per_file": 3, **options},
    )


def receipt(result, local):
    return json.loads(Path(result["receipts"]).read_text())["files"].get(str(local))


@pytest.mark.parametrize("direct", [False, True])
def test_exact_downloads_have_concurrent_ranges_and_owned_receipts(
    tmp_path, ranges, chunk_mode, direct, monkeypatch
):
    config = configuration(tmp_path, ranges, chunk_mode)
    if direct:
        config.url_list, config.download_url = None, ranges.base + "a"
    usage = []
    actual_usage = transfers.shutil.disk_usage

    def disk_usage(path):
        usage.append(path)
        return actual_usage(path)

    monkeypatch.setattr(transfers.shutil, "disk_usage", disk_usage)
    observations, reporters, managers = [], [], []
    actual_manager = transfers.ParallelDownloadManager

    def owned_manager(*args, **kwargs):
        manager = actual_manager(*args, **kwargs)
        managers.append(manager)
        return manager

    class Observer(DownloadProgress):
        def __init__(self, *_):
            super().__init__(True)
            reporters.append(self)

        def received_chunk(self, path, chunk, size, expected):
            observations.append((path, chunk, size, expected))
            super().received_chunk(path, chunk, size, expected)

    monkeypatch.setattr(transfers, "DownloadProgress", Observer)
    monkeypatch.setattr(transfers, "ParallelDownloadManager", owned_manager)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 1
    assert result["bytes_downloaded"] == len(ranges.payload)
    assert ranges.maximum == 3 and ranges.together.is_set()
    local = config.dest_path / "a"
    assert local.read_bytes() == ranges.payload
    proof = receipt(result, local)
    assert proof["size"] == len(ranges.payload)
    assert proof["sha256"] == hashlib.sha256(ranges.payload).hexdigest()
    assert proof["identity"] == list(_identity(local)) and proof["etag"] == '"v1"'
    assert local.stat().st_mtime == pytest.approx(1791331200, abs=0.001)
    assert len(usage) == 1
    assert [method for method, _, _ in ranges.requests].count("HEAD") == 1
    requests = [headers for method, _, headers in ranges.requests if method == "GET"]
    assert len(requests) == 3
    assert {headers["Range"] for headers in requests} == {
        "bytes=0-1048575",
        "bytes=1048576-2097151",
        "bytes=2097152-3145727",
    }
    assert all(
        headers["If-Range"] == '"v1"' and headers["Accept-Encoding"] == "identity"
        for headers in requests
    )
    assert result["stage_seconds"]["fsync"] > 0 and result["stage_seconds"]["hash"] > 0
    assert {path for path, _, _, _ in observations} == {local}
    assert {chunk for _, chunk, _, _ in observations} == {0, 1, 2}
    assert all(expected == len(ranges.payload) for _, _, _, expected in observations)
    assert reporters[0].completed == 1 and not reporters[0].active
    assert not reporters[0].thread.is_alive()
    assert_no_scratch(config.dest_path)
    assert len(managers) == 1 and not managers[0]._cleanup_thread.is_alive()
    assert all(not thread.is_alive() for thread in managers[0].executor._threads)
    # The same owned URL is downloaded again rather than freshness-skipped.
    assert download_url_list(config)["success"]


@pytest.mark.parametrize("fault", ["weak", "no-ranges", "no-head", "small"])
def test_ineligible_files_fall_back_to_whole_file(tmp_path, ranges, chunk_mode, fault):
    ranges.fault = fault
    config = configuration(tmp_path, ranges, chunk_mode)
    if fault == "small":
        config.min_chunk_size_mb = 4
    result = download_url_list(config)
    assert result["success"]
    assert (config.dest_path / "a").read_bytes() == ranges.payload
    assert [method for method, _, _ in ranges.requests] == ["HEAD", "GET"]
    assert all("Range" not in headers for _, _, headers in ranges.requests)
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("fault", ["status", "etag", "range", "encoding", "truncate"])
def test_invalid_ranges_fail_without_publication_or_whole_file_fallback(
    tmp_path, ranges, chunk_mode, fault
):
    ranges.fault = fault
    config = configuration(tmp_path, ranges, chunk_mode, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"old")
    before = _identity(local)
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 1
    assert local.read_bytes() == b"old" and _identity(local) == before
    assert receipt(result, local) is None
    assert len(ranges.requests) == 4
    assert all("Range" in headers for method, _, headers in ranges.requests if method == "GET")
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("recover", [False, True])
def test_chunk_retry_budget_restarts_exact_range(tmp_path, ranges, chunk_mode, recover):
    ranges.fault = "recover" if recover else "truncate"
    config = configuration(tmp_path, ranges, chunk_mode, max_retries=1)
    result = download_url_list(config)
    assert result["success"] is recover
    gets = [key for key in ranges.counts if key[0] == "GET"]
    assert len(gets) == 3 and all(ranges.counts[key] == 2 for key in gets)
    assert_no_scratch(config.dest_path)


def test_disk_budget_reserves_peak_before_any_chunk_write(
    tmp_path, ranges, chunk_mode, monkeypatch
):
    config = configuration(tmp_path, ranges, chunk_mode, max_concurrent_downloads=1)
    calls = []

    def usage(path):
        calls.append(path)
        return SimpleNamespace(free=len(ranges.payload) + 1024**2 + 1)

    monkeypatch.setattr(transfers.shutil, "disk_usage", usage)
    result = download_url_list(config)
    assert result["success"] is (chunk_mode == "streaming_parallel")
    assert len(calls) == 1
    if chunk_mode == "parallel_downloads":
        assert [method for method, _, _ in ranges.requests] == ["HEAD"]
    assert_no_scratch(config.dest_path)


def test_multiple_files_share_chunk_cap_and_one_disk_sample(
    tmp_path, ranges, chunk_mode, monkeypatch
):
    ranges.threshold = 10
    config = configuration(
        tmp_path,
        ranges,
        chunk_mode,
        paths=("a", "b", "c", "d"),
        max_concurrent_downloads=4,
        max_parallel_chunks_total=10,
        trusted_server=True,
    )
    calls = []
    actual_usage = transfers.shutil.disk_usage
    monkeypatch.setattr(
        transfers.shutil, "disk_usage", lambda p: (calls.append(p), actual_usage(p))[1]
    )
    config.dest_path.mkdir()
    (config.dest_path / "unlisted").write_bytes(b"preserved")
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 4
    assert ranges.maximum == 10 and len(calls) == 1
    assert result["bytes_downloaded"] == len(ranges.payload) * 4
    for name in ("a", "b", "c", "d"):
        local = config.dest_path / name
        assert local.read_bytes() == ranges.payload
        assert receipt(result, local)["sha256"] == hashlib.sha256(ranges.payload).hexdigest()
    assert (config.dest_path / "unlisted").read_bytes() == b"preserved"
    assert_no_scratch(config.dest_path)


def test_published_files_stay_charged_in_batch_budget(tmp_path, ranges, chunk_mode, monkeypatch):
    config = configuration(
        tmp_path, ranges, chunk_mode, paths=("a", "b"), max_concurrent_downloads=1
    )
    multiplier = 2 if chunk_mode == "parallel_downloads" else 1
    free = int(len(ranges.payload) * (multiplier + 0.5)) + 1024**2
    calls = []
    monkeypatch.setattr(
        transfers.shutil, "disk_usage", lambda p: (calls.append(p), SimpleNamespace(free=free))[1]
    )
    result = download_url_list(config)
    assert not result["success"] and result["files_downloaded"] == result["files_failed"] == 1
    assert (config.dest_path / "a").read_bytes() == ranges.payload
    assert not (config.dest_path / "b").exists() and len(calls) == 1
    assert receipt(result, config.dest_path / "a") is not None
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("tight", [False, True])
def test_streaming_preallocation_fallback_reserves_assembly_space(
    tmp_path, ranges, monkeypatch, tight
):
    config = configuration(tmp_path, ranges, "streaming_parallel", max_concurrent_downloads=1)
    free = len(ranges.payload) * (1 if tight else 2) + 1024**2 + 1
    calls = []
    monkeypatch.setattr(
        transfers.shutil, "disk_usage", lambda p: (calls.append(p), SimpleNamespace(free=free))[1]
    )
    actual_open = Path.open

    def open_file(path, *args, **kwargs):
        file = actual_open(path, *args, **kwargs)
        if path.name == "staging.streaming" and args == ("xb",):

            def fail(_):
                raise OSError("preallocation failed")

            file.truncate = fail
        return file

    monkeypatch.setattr(Path, "open", open_file)
    result = download_url_list(config)
    assert result["success"] is not tight and len(calls) == 1
    if tight:
        assert [method for method, _, _ in ranges.requests] == ["HEAD"]
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("fault", ["redirect", "outside", "loop"])
def test_redirects_preserve_range_headers_and_remain_scoped(tmp_path, ranges, chunk_mode, fault):
    ranges.fault = fault
    config = configuration(tmp_path, ranges, chunk_mode)
    result = download_url_list(config)
    assert result["success"] is (fault == "redirect")
    assert all(path.startswith("/root/") for _, path, _ in ranges.requests)
    if fault == "redirect":
        gets = [
            headers
            for method, path, headers in ranges.requests
            if method == "GET" and path.endswith("/renamed")
        ]
        assert len(gets) == 3 and all(headers["If-Range"] == '"v1"' for headers in gets)
        assert (config.dest_path / "a").read_bytes() == ranges.payload
    assert_no_scratch(config.dest_path)


def test_destination_change_during_transfer_is_preserved(tmp_path, ranges, chunk_mode, monkeypatch):
    config = configuration(tmp_path, ranges, chunk_mode, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"old")
    original = transfers.file_sha256

    def changed(path, enabled=True):
        if path.name.startswith("staging."):
            local.write_bytes(b"external edit")
        return original(path, enabled)

    monkeypatch.setattr(transfers, "file_sha256", changed)
    result = download_url_list(config)
    assert not result["success"]
    assert local.read_bytes() == b"external edit" and receipt(result, local) is None
    assert_no_scratch(config.dest_path)


async def test_cancelled_chunk_writers_drain_before_lock_release(tmp_path, ranges, chunk_mode):
    ranges.hold = True
    config = configuration(tmp_path, ranges, chunk_mode, overwrite=True)
    target = config.dest_path
    target.mkdir()
    local = target / "a"
    local.write_bytes(b"old")
    guard = DestinationLock([target])
    state = PartialDownloadManager(target)._state_directory()
    scratch = OwnedScratch(target, state, None, guard)
    with DownloadProgress(False, config) as progress:
        engine = AsyncTransfers(config, target, ranges.base, scratch, progress)
        task = asyncio.create_task(engine.run([(ranges.base + "a", local)]))
        try:
            assert await asyncio.to_thread(ranges.entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done() and local.read_bytes() == b"old"
            ranges.release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert local.read_bytes() == b"old" and engine.closed_connection_pools == 1
            assert_no_scratch(target)
        finally:
            ranges.release.set()
            guard.close()
    next_owner = DestinationLock([target])
    next_owner.close()


async def test_cancellation_after_publication_keeps_its_receipt_hook(
    tmp_path, ranges, chunk_mode, monkeypatch
):
    config = configuration(tmp_path, ranges, chunk_mode)
    target = config.dest_path
    target.mkdir()
    guard = DestinationLock([target])
    scratch = OwnedScratch(target, PartialDownloadManager(target)._state_directory(), None, guard)
    published, entered, release = [], threading.Event(), threading.Event()
    actual = transfers.ParallelDownloadManager.download_parallel

    def after_publication(manager, download):
        accepted = actual(manager, download)
        entered.set()
        assert release.wait(5)
        return accepted

    monkeypatch.setattr(transfers.ParallelDownloadManager, "download_parallel", after_publication)
    engine = AsyncTransfers(config, target, ranges.base, scratch)
    local = target / "a"
    task = asyncio.create_task(engine.run([(ranges.base + "a", local)], published.append))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        assert (
            len(published) == 1
            and published[0].sha256 == hashlib.sha256(ranges.payload).hexdigest()
        )
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert local.read_bytes() == ranges.payload and engine.closed_connection_pools == 1
        assert_no_scratch(target)
    finally:
        release.set()
        guard.close()


async def test_cancellation_during_hash_preserves_destination(
    tmp_path, ranges, chunk_mode, monkeypatch
):
    config = configuration(tmp_path, ranges, chunk_mode, overwrite=True)
    target = config.dest_path
    target.mkdir()
    local = target / "a"
    local.write_bytes(b"old")
    guard = DestinationLock([target])
    scratch = OwnedScratch(target, PartialDownloadManager(target)._state_directory(), None, guard)
    entered, release = threading.Event(), threading.Event()
    original = transfers.file_sha256

    def hashing(path, enabled=True):
        entered.set()
        assert release.wait(5)
        return original(path, enabled)

    monkeypatch.setattr(transfers, "file_sha256", hashing)
    engine = transfers.AsyncTransfers(config, target, ranges.base, scratch)
    published = []
    task = asyncio.create_task(engine.run([(ranges.base + "a", local)], published.append))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.02)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert local.read_bytes() == b"old" and not published
        assert_no_scratch(target)
    finally:
        release.set()
        guard.close()


def test_failed_owned_assembly_cleanup_preserves_evidence_and_reports_failure(
    tmp_path, ranges, monkeypatch
):
    config = configuration(tmp_path, ranges, "parallel_downloads")
    actual = OwnedScratch._remove

    def retain(self, work, root, kind):
        if kind == "staging":
            return False
        return actual(self, work, root, kind)

    monkeypatch.setattr(OwnedScratch, "_remove", retain)
    result = download_url_list(config)
    assert not result["success"]
    # Publication is real even if cleanup fails; its ownership proof survives.
    assert receipt(result, config.dest_path / "a") is not None
    work = list(config.dest_path.rglob("work_*"))
    assert len(work) == 1 and (work[0] / "owner.json").exists()


def test_chunk_flags_validate_and_aiohttp_remains_explicit(tmp_path, chunk_mode):
    values = dict(
        base_url="https://files.example/root/",
        dest_path=tmp_path,
        log_path=tmp_path,
        mode="download",
        download_url="https://files.example/root/a",
        **{chunk_mode: True},
    )
    assert getattr(MirrorConfig(**values), chunk_mode)
    with pytest.raises(ConfigError, match="require httpx"):
        MirrorConfig(**values, backend="aiohttp")
    with pytest.raises(ConfigError, match="chunk scratch inside"):
        MirrorConfig(**values, chunk_assembly_dir=tmp_path / "outside")


@pytest.mark.parametrize("configured", [False, True])
def test_cli_chunk_flags_use_shared_summary_and_truthful_banner(
    tmp_path, ranges, chunk_mode, monkeypatch, capsys, configured
):
    flag = "--parallel-downloads" if chunk_mode == "parallel_downloads" else "--streaming-parallel"
    arguments = [
        "mirror-url",
        "--mode",
        "download",
        ranges.base + "a",
        flag,
        "--dest-path",
        str(tmp_path / "dest"),
        "--log-path",
        str(tmp_path / "logs"),
        "--min-chunk-size",
        "1",
        "--max-chunks",
        "3",
        "--print-logs",
        "--request-delay",
        "0",
        "--requests-per-second",
        "0",
    ]
    if configured:
        settings = tmp_path / "config.json"
        settings.write_text(
            json.dumps(
                {
                    "base_url": ranges.base,
                    "dest_path": str(tmp_path / "dest"),
                    "log_path": str(tmp_path / "logs"),
                    chunk_mode: True,
                    "min_chunk_size_mb": 1,
                    "max_chunks_per_file": 3,
                    "requests_per_second": 0,
                    "request_delay": 0,
                }
            )
        )
        arguments = [
            "mirror-url",
            "--config",
            str(settings),
            "--mode",
            "download",
            ranges.base + "a",
            "--print-logs",
        ]
    monkeypatch.setattr("sys.argv", arguments)
    cli.main()
    output = capsys.readouterr()
    assert output.out == ""
    assert "parallel chunks" in output.err and "Range metadata probes enabled" in output.err
    assert "SUMMARY:" in output.err and "Sync completed successfully" in output.err
    assert "Closed all 1 connection pools" in output.err
    assert "Remote files found: not performed" in output.err
    assert "Metadata time: not performed" in output.err
    assert (tmp_path / "dest/a").read_bytes() == ranges.payload
