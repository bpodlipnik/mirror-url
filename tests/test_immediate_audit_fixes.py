"""Regression coverage for immediate audit findings F01--F13."""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from urllib.parse import unquote, urlparse

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.async_connection import AsyncTaskManager
from mirror_url.cache import NullCacheManager
from mirror_url.download import ParallelDownloadManager, PartialDownloadManager
from mirror_url.enums import CleanupPolicy
from mirror_url.metrics import MetricsCollector
from mirror_url.parsing import extract_links_fast
from mirror_url.primitives import AtomicCounter, AtomicSize
from mirror_url.transport import SecureAsyncTransport

BASE = "https://example.com/root/"


def response(status=200, headers=None, content=b""):
    return httpx.Response(
        status, headers=headers, content=content, request=httpx.Request("GET", BASE + "a")
    )


@pytest.fixture
def mirror(tmp_path):
    m = MirrorURL.__new__(MirrorURL)
    m.config = MirrorConfig(
        base_url=BASE, dest_path=tmp_path / "mirror", log_path=tmp_path / "logs", max_retries=0
    )
    m.target_dir = tmp_path / "mirror"
    m.target_dir.mkdir()
    m._target_dir_path = m.target_dir
    m.target_parsed = urlparse(BASE)
    m.target_base_url = BASE
    m.metrics = MetricsCollector()
    m.scanner = SimpleNamespace(cached_signatures={}, fresh_dir_signatures={})
    m.cache_manager = NullCacheManager()
    m.performance_monitor = Mock()
    m.symlink_tracker = None
    m.files_skipped = AtomicCounter()
    m.files_failed = AtomicCounter()
    m.files_processed = AtomicCounter()
    m.total_downloaded_size = AtomicSize()
    m._speed_samples = deque(maxlen=20)
    m.async_task_manager = AsyncTaskManager()
    m._meta_check_executor = ThreadPoolExecutor(max_workers=2)
    m.partial_manager = PartialDownloadManager(m.target_dir)
    m.bandwidth_limiter = None
    m.connection_ok = True
    m.scan_incomplete = False
    m.suffix_index = 0
    m.total_suffixes = 1
    m.get_remote_timestamp = lambda url: None
    m._get_filename_fast = lambda url: unquote(urlparse(url).path.rsplit("/", 1)[-1])
    m.connection_manager = Mock()
    yield m
    m._meta_check_executor.shutdown()


class AsyncManager:
    def __init__(self, result=None):
        self.result = result or response(headers={"Content-Length": "3"})
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def is_available(self):
        return True

    async def profile_server(self, urls):
        return True

    async def head(self, url, headers=None):
        self.calls.append((url, headers))
        return self.result


def check_async(m, items, result=None):
    manager = AsyncManager(result)
    m.adaptive_async_manager = manager
    planned = asyncio.run(m._check_files_async(items))
    return planned, manager


@pytest.mark.parametrize(
    "policy", [CleanupPolicy.DELETE, CleanupPolicy.MOVE, CleanupPolicy.PREVIEW]
)
def test_cleanup_never_follows_local_symlinks(mirror, tmp_path, policy):
    mirror.config.cleanup_policy = policy
    outside = tmp_path / "outside"
    outside.mkdir()
    important = outside / "important.dat"
    important.write_bytes(b"keep")
    (mirror.target_dir / "linked").symlink_to(outside, target_is_directory=True)
    (mirror.target_dir / "file-link").symlink_to(important)
    mirror.clean_obsolete(set())
    assert important.read_bytes() == b"keep"
    assert (mirror.target_dir / "linked").is_symlink()
    assert (mirror.target_dir / "file-link").is_symlink()
    assert mirror._scan_local_tree() == ([], [])


def test_cleanup_refuses_symlink_root(mirror, tmp_path):
    mirror.config.cleanup_policy = CleanupPolicy.DELETE
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "important").write_bytes(b"keep")
    mirror.target_dir.rmdir()
    mirror.target_dir.symlink_to(outside, target_is_directory=True)
    mirror.clean_obsolete(set())
    assert (outside / "important").read_bytes() == b"keep"


def test_archive_creation_failure_never_becomes_delete(mirror, monkeypatch):
    mirror.config.cleanup_policy = CleanupPolicy.MOVE
    important = mirror.target_dir / "important"
    important.write_bytes(b"keep")
    archive = mirror.target_dir.with_name("mirror_obsolete")
    archive.write_bytes(b"blocking file")
    monkeypatch.setattr(
        "builtins.input", lambda _: pytest.fail("MOVE must not request DELETE approval")
    )
    mirror.clean_obsolete(set())
    assert important.read_bytes() == b"keep"
    assert mirror.config.cleanup_policy == CleanupPolicy.MOVE


def test_failed_directory_move_leaves_directory(mirror, monkeypatch):
    mirror.config.cleanup_policy = CleanupPolicy.MOVE
    empty = mirror.target_dir / "empty"
    empty.mkdir()
    monkeypatch.setattr(
        "mirror_url._core.cleanup.shutil.move", Mock(side_effect=OSError("archive unavailable"))
    )
    mirror.clean_obsolete(set())
    assert empty.is_dir()


@pytest.mark.parametrize(
    "policy", [CleanupPolicy.DELETE, CleanupPolicy.MOVE, CleanupPolicy.PREVIEW]
)
def test_cleanup_preserves_unselected_paths_and_metadata(mirror, policy):
    mirror.config.cleanup_policy = policy
    mirror.config.file_filters = [".dat"]
    mirror.config.exclude_dirs = ["excluded"]
    mirror.config.max_depth = 2
    files = [
        "unselected.txt",
        "excluded/keep.dat",
        "depth/boundary/keep.dat",
        "duplicate/keep.dat",
        "gone.dat",
    ]
    for name in files:
        path = mirror.target_dir / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"keep")
    empty_excluded = mirror.target_dir / "excluded" / "empty"
    empty_excluded.mkdir()
    mirror.cleanup_protected_prefixes = {BASE + "duplicate/"}
    retained = []
    mirror.cache_manager = SimpleNamespace(
        cleanup_file_metadata=lambda _: None,
        cleanup_stale_metadata=lambda expected: retained.extend(expected) or 0,
    )
    mirror.clean_obsolete(set())
    for name in files[:-1]:
        assert (mirror.target_dir / name).read_bytes() == b"keep"
    assert empty_excluded.is_dir()
    if policy == CleanupPolicy.PREVIEW:
        assert mirror.metrics.metrics["files_would_delete"] == 1
    else:
        assert not (mirror.target_dir / "gone.dat").exists()
        assert set(retained) >= {mirror.target_dir / name for name in files[:-1]}


def test_async_cold_dns_cache_completes_without_blocking_loop(monkeypatch):
    calls = []

    def resolve(host):
        time.sleep(0.02)
        calls.append(host)
        return "8.8.8.8"

    send = AsyncMock(return_value=response())
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolve
    )
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", send)

    async def run():
        transport = SecureAsyncTransport()
        request = httpx.Request("HEAD", "https://example.com:8443/root/a")
        ticks = []

        async def tick():
            await asyncio.sleep(0.005)
            ticks.append(len(calls))

        try:
            await asyncio.wait_for(
                asyncio.gather(
                    transport.handle_async_request(request),
                    transport.handle_async_request(request),
                    tick(),
                ),
                1,
            )
            assert ticks == [0]
            assert calls == ["example.com"]
            sent = send.call_args.args[0]
            assert sent.url.host == "8.8.8.8"
            assert sent.headers["Host"] == "example.com:8443"
            assert sent.extensions["sni_hostname"] == "example.com"
        finally:
            await transport.aclose()

    asyncio.run(run())


@pytest.mark.parametrize("fallback", [False, True])
def test_async_visits_every_input_once(mirror, monkeypatch, fallback):
    import mirror_url._core.compare as module

    if fallback:
        monkeypatch.setattr(module, "ASYNC_TEST_MIN_SPEED", 10**12)
        monkeypatch.setattr(module, "ASYNC_TEST_MAX_SECONDS", 10000)
        monkeypatch.setattr(module, "ASYNC_TEST_MIN_FILES", 10000)
    items = [(BASE + f"f{i}", mirror.target_dir / f"f{i}") for i in range(400)]
    fallback_items = []
    mirror._check_files_sync = lambda remaining, progress: (
        fallback_items.extend(remaining) or remaining
    )
    progress = Mock()
    manager = AsyncManager()
    mirror.adaptive_async_manager = manager
    planned = asyncio.run(mirror._check_files_async(items, progress))
    assert planned == items
    assert len(set(planned)) == 400
    if fallback:
        assert fallback_items
    assert sum(call.args[0] for call in progress.update.call_args_list) == (
        400 - len(fallback_items)
    )


def test_async_speed_samples_support_deque(mirror, monkeypatch):
    import mirror_url._core.compare as module

    monkeypatch.setattr(module, "ASYNC_TEST_MIN_FILES", 100000)
    monkeypatch.setattr(module, "ASYNC_TEST_MAX_SECONDS", 100000)
    monkeypatch.setattr(module, "ASYNC_TEST_MIN_SPEED", 0)
    items = [(BASE + f"f{i}", mirror.target_dir / f"f{i}") for i in range(560)]
    planned, _ = check_async(mirror, items)
    assert planned == items
    assert len(mirror._speed_samples) == 5


def test_directory_signature_never_hides_missing_async_file(mirror):
    mirror.scanner.cached_signatures = {BASE: "etag:same"}
    mirror.scanner.fresh_dir_signatures = dict(mirror.scanner.cached_signatures)
    item = (BASE + "missing", mirror.target_dir / "missing")
    assert check_async(mirror, [item])[0] == [item]


@pytest.mark.parametrize("async_mode", [False, True])
@pytest.mark.parametrize(
    "case",
    [
        "size-before-time",
        "size-before-etag",
        "no-etag",
        "no-cache",
        "unknown-headers",
        "directory-validator",
        "invalid-304",
        "local-modified",
    ],
)
def test_consistent_file_freshness(mirror, async_mode, case):
    path = mirror.target_dir / "a"
    path.write_bytes(b"abc")
    os.utime(path, (2000, 2000))
    stat = path.stat()
    stored = {
        "etag": '"old"',
        "size": 3,
        "local_mtime_ns": stat.st_mtime_ns,
        "local_ctime_ns": stat.st_ctime_ns,
    }
    headers = {"Content-Length": "3", "Last-Modified": "Thu, 01 Jan 1970 00:16:40 GMT"}
    expected = False
    status = 200
    if case.startswith("size-before"):
        headers["Content-Length"] = "100"
        if case == "size-before-etag":
            headers["ETag"] = '"old"'
    elif case == "no-etag":
        mirror.config.no_etag = True
        headers["ETag"] = '"new"'
        expected = True
    elif case == "no-cache":
        mirror.config.no_cache = True
        headers["Last-Modified"] = "Thu, 01 Jan 1970 00:50:00 GMT"
    elif case == "unknown-headers":
        headers = {}
    elif case == "directory-validator":
        mirror.scanner.cached_signatures = {BASE: "etag:directory"}
        mirror.scanner.fresh_dir_signatures = dict(mirror.scanner.cached_signatures)
        headers["ETag"] = '"new"'
    elif case == "invalid-304":
        stored = None
        status = 304
    elif case == "local-modified":
        path.write_bytes(b"x")
        headers = {"ETag": '"old"'}
        status = 304
    mirror.cache_manager = SimpleNamespace(get_file_metadata=lambda _: stored)
    result = response(status, headers)
    mirror.connection_manager.request.return_value = result
    if async_mode:
        planned, manager = check_async(mirror, [(BASE + "a", path)], result)
        current = not planned
        sent_headers = manager.calls[0][1]
    else:
        current = mirror.file_exists_and_up_to_date(path, BASE + "a")
        sent_headers = mirror.connection_manager.request.call_args.kwargs["headers"]
    assert current is expected
    if case in ("no-etag", "no-cache", "local-modified"):
        assert "If-None-Match" not in sent_headers


def set_partial(mirror, *, data=b"ABC", total=6, etag='"v1"'):
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    partial = mirror.partial_manager.get_partial_path(path)
    partial.write_bytes(data)
    partial.with_name(partial.name + ".json").write_text(
        json.dumps({"url": BASE + "a", "etag": etag, "size": total})
    )
    return path, partial


@pytest.mark.parametrize(
    "defect",
    [
        "wrong-offset",
        "wrong-total",
        "changed-etag",
        "missing-etag",
        "short-body",
        "long-body",
        "unsolicited",
        "encoded",
    ],
)
def test_invalid_resume_never_replaces_existing_file(mirror, defect):
    path, partial = set_partial(mirror)
    headers = {"Content-Range": "bytes 3-5/6", "Content-Length": "3", "ETag": '"v1"'}
    body = b"DEF"
    if defect == "wrong-offset":
        headers["Content-Range"] = "bytes 0-2/6"
    elif defect == "wrong-total":
        headers["Content-Range"] = "bytes 3-5/9"
    elif defect == "changed-etag":
        headers["ETag"] = '"v2"'
    elif defect == "missing-etag":
        del headers["ETag"]
    elif defect == "short-body":
        body = b"D"
    elif defect == "long-body":
        body = b"DEFG"
    elif defect == "unsolicited":
        partial.with_name(partial.name + ".json").unlink()
    elif defect == "encoded":
        headers["Content-Encoding"] = "gzip"
        body = b""  # don't ask httpx's constructor to decode invalid gzip
    mirror.connection_manager.request.return_value = response(206, headers, body)
    assert mirror._download_file_single(BASE + "a", path) is False
    assert path.read_bytes() == b"old final"
    assert mirror.files_processed.value() == 0


def test_valid_resume_has_if_range_and_publishes_exact_bytes(mirror):
    path, partial = set_partial(mirror)
    mirror.connection_manager.request.return_value = response(
        206, {"Content-Range": "bytes 3-5/6", "Content-Length": "3", "ETag": '"v1"'}, b"DEF"
    )
    assert mirror._download_file_single(BASE + "a", path)
    assert path.read_bytes() == b"ABCDEF"
    headers = mirror.connection_manager.request.call_args.kwargs["headers"]
    assert headers["Range"] == "bytes=3-"
    assert headers["If-Range"] == '"v1"'
    assert not partial.exists()
    assert not partial.with_name(partial.name + ".json").exists()


@pytest.mark.parametrize("case", ["legacy", "oversized", "weak-etag", "ignored-range", "416"])
def test_unsafe_partial_restarts_from_full_representation(mirror, case):
    path, partial = set_partial(mirror)
    if case == "legacy":
        partial.with_name(partial.name + ".json").unlink()
    elif case == "oversized":
        partial.write_bytes(b"TOO-LONG")
    elif case == "weak-etag":
        set_partial(mirror, etag='W/"v1"')
    fresh = response(200, {"Content-Length": "3", "ETag": '"v2"'}, b"NEW")
    mirror.connection_manager.request.side_effect = (
        [response(416, {"Content-Range": "bytes */1"}), fresh] if case == "416" else [fresh]
    )
    assert mirror._download_file_single(BASE + "a", path)
    assert path.read_bytes() == b"NEW"
    if case in ("legacy", "oversized", "weak-etag"):
        assert "Range" not in mirror.connection_manager.request.call_args_list[0].kwargs["headers"]
    if case == "416":
        assert mirror.connection_manager.request.call_count == 2
        assert "Range" not in mirror.connection_manager.request.call_args.kwargs["headers"]


@pytest.fixture
def parallel(tmp_path, mirror):
    cfg = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "parallel",
        log_path=tmp_path / "log",
        streaming_parallel=True,
        chunk_assembly_dir=tmp_path / "chunks",
    )
    manager = ParallelDownloadManager(
        cfg, mirror.metrics, mirror.connection_manager, None, mirror=mirror
    )
    manager.min_chunk_size = 1
    manager.max_chunks_per_file = 2
    yield manager
    manager.shutdown(timeout=2)


def range_handler(url, method="GET", headers=None, **kwargs):
    headers = headers or {}
    if method == "HEAD":
        return response(200, {"Accept-Ranges": "bytes", "Content-Length": "6", "ETag": '"v1"'})
    assert headers["If-Range"] == '"v1"'
    start, end = map(int, headers["Range"][6:].split("-"))
    return response(
        206,
        {
            "Content-Range": f"bytes {start}-{end}/6",
            "Content-Length": str(end - start + 1),
            "ETag": '"v1"',
        },
        b"ABCDEF"[start : end + 1],
    )


@pytest.mark.parametrize("streaming", [False, True])
def test_parallel_verified_chunks_publish_atomically(parallel, mirror, streaming):
    parallel.use_streaming = streaming
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    mirror.connection_manager.request.side_effect = range_handler
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    assert path.read_bytes() == b"old final"
    if streaming:
        assert download.staging_path != path
    assert parallel.download_parallel(download)
    assert path.read_bytes() == b"ABCDEF"
    assert mirror.files_processed.value() == 1
    assert download.status == "completed"
    assert download.staging_path is None


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "defect", ["200", "etag", "offset", "total", "overflow", "short", "redirect-block"]
)
def test_bad_parallel_response_preserves_old_file(parallel, mirror, monkeypatch, streaming, defect):
    from mirror_url.exceptions import URLScopeError

    monkeypatch.setattr("mirror_url.download.time.sleep", lambda _: None)
    parallel.use_streaming = streaming
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")

    def handler(url, method="GET", headers=None, **kwargs):
        good = range_handler(url, method, headers, **kwargs)
        if method == "HEAD":
            return good
        if defect == "redirect-block":
            raise URLScopeError("outside configured root")
        if defect == "200":
            return response(200, {"Content-Length": "3", "ETag": '"v1"'}, b"BAD")
        changed_headers = dict(good.headers)
        body = good.content
        if defect == "etag":
            changed_headers["etag"] = '"v2"'
        elif defect == "offset":
            changed_headers["content-range"] = "bytes 1-3/6"
        elif defect == "total":
            changed_headers["content-range"] = "bytes 0-2/9"
        elif defect == "overflow":
            body = b"XXXX"
        elif defect == "short":
            body = b"X"
        return response(206, changed_headers, body)

    mirror.connection_manager.request.side_effect = handler
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    staging = download.staging_path
    assert parallel.download_parallel(download) is False
    assert path.read_bytes() == b"old final"
    if staging:
        assert not staging.exists()


def test_parallel_retry_uses_same_atomic_completion(parallel, mirror, monkeypatch):
    monkeypatch.setattr("mirror_url.download.time.sleep", lambda _: None)
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    mirror.connection_manager.request.side_effect = range_handler
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    attempts = {}
    real_download = parallel._download_chunk_with_semaphore

    def fail_once(chunk):
        attempts[chunk.chunk_id] = attempts.get(chunk.chunk_id, 0) + 1
        if chunk.chunk_id == 0 and attempts[0] == 1:
            chunk.status = "failed"
            return False
        return real_download(chunk)

    monkeypatch.setattr(parallel, "_download_chunk_with_semaphore", fail_once)
    assert parallel.download_parallel(download)
    assert path.read_bytes() == b"ABCDEF"
    assert mirror.files_processed.value() == 1


def test_lightweight_parser_handles_html_attribute_syntax():
    html = '<A HREF = "upper.bin">a</A><a href = spaced.bin>b</a><a href=unquoted.bin>c</a><a href="x?a=1&amp;b=2">d</a><!-- <a href="ghost"> --><script>var x = \'<a href="script">\';</script><img href="image"><a href="MAILTO:x">mail</a>'
    expected = ["upper.bin", "spaced.bin", "unquoted.bin", "x?a=1&b=2"]
    assert extract_links_fast(html) == expected
    assert extract_links_fast(html.encode()) == expected


def test_streaming_redirect_is_rejected_before_outside_request(parallel, mirror, monkeypatch):
    from mirror_url.connection import ConnectionManager

    monkeypatch.setattr("mirror_url.download.time.sleep", lambda _: None)
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    cfg = mirror.config.model_copy(update={"security_validation": False})
    cm = ConnectionManager(cfg, mirror.metrics)
    visited = []

    def handle(request):
        visited.append((request.method, str(request.url)))
        if request.method == "HEAD":
            return httpx.Response(
                200, headers={"Accept-Ranges": "bytes", "Content-Length": "6", "ETag": '"v1"'}
            )
        return httpx.Response(307, headers={"Location": "/outside/a"})

    client = httpx.Client(transport=httpx.MockTransport(handle))
    cm.connection_pool.get_client = lambda _: client
    parallel.connection_manager = cm
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    try:
        download = parallel.create_chunks(BASE + "a", path, 6)
        assert download is not None
        assert not parallel.download_parallel(download)
        assert path.read_bytes() == b"old final"
        assert all(url.startswith(BASE) for _, url in visited)
        assert any(method == "GET" for method, _ in visited)
    finally:
        client.close()
        cm.close()


def test_connection_retry_preserves_range_and_validator(mirror, monkeypatch):
    from mirror_url.connection import ConnectionManager

    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    cfg = mirror.config.model_copy(update={"security_validation": False, "max_retries": 1})
    cm = ConnectionManager(cfg, mirror.metrics)
    sent_headers = []

    def handle(request):
        sent_headers.append(dict(request.headers))
        if len(sent_headers) == 1:
            raise httpx.ConnectError("temporary connection failure", request=request)
        return httpx.Response(206, headers={"Content-Range": "bytes 3-5/6"}, content=b"DEF")

    client = httpx.Client(transport=httpx.MockTransport(handle))
    cm.connection_pool.get_client = lambda _: client
    try:
        assert (
            cm.request(BASE + "a", headers={"Range": "bytes=3-5", "If-Range": '"v1"'}).status_code
            == 206
        )
        assert len(sent_headers) == 2
        assert all(h["range"] == "bytes=3-5" and h["if-range"] == '"v1"' for h in sent_headers)
    finally:
        client.close()
        cm.close()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize(
    "headers",
    [
        {"Accept-Ranges": "bytes", "Content-Length": "6"},
        {"Accept-Ranges": "bytes", "Content-Length": "6", "ETag": 'W/"v1"'},
        {"Accept-Ranges": "bytes", "Content-Length": "9", "ETag": '"v1"'},
    ],
)
def test_parallel_without_stable_representation_falls_back(parallel, mirror, streaming, headers):
    parallel.use_streaming = streaming
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    mirror.connection_manager.request.return_value = response(200, headers)
    assert parallel.create_chunks(BASE + "a", path, 6) is None
    assert path.read_bytes() == b"old final"


def test_missing_file_never_published_during_streaming_allocation(parallel, mirror):
    path = mirror.target_dir / "new"
    mirror.connection_manager.request.side_effect = range_handler
    download = parallel.create_chunks(BASE + "new", path, 6)
    assert download is not None
    assert not path.exists()
    assert download.staging_path.exists()
    parallel.cleanup_chunks(download)
    assert not path.exists()


def test_preallocation_failure_selects_real_temp_chunk_mode(parallel, mirror, monkeypatch):
    monkeypatch.setattr(
        "mirror_url.download.tempfile.mkstemp", Mock(side_effect=OSError("no allocation"))
    )
    mirror.connection_manager.request.side_effect = range_handler
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    assert download.status == "downloading"
    assert all(not chunk.direct_write for chunk in download.chunks)
    assert path.read_bytes() == b"old final"
    assert parallel.download_parallel(download)
    assert path.read_bytes() == b"ABCDEF"


def test_trusted_metadata_304_is_current_and_local_edit_is_not(mirror):
    from mirror_url.cache import CacheManager

    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    mirror.cache_manager = CacheManager(
        mirror.target_dir / "cache.json", mirror.config, mirror.metrics
    )
    mirror.cache_manager.save_file_metadata(path, '"v1"', path.stat().st_mtime, 3)
    mirror.connection_manager.request.return_value = response(304)
    assert mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert mirror.connection_manager.request.call_args.kwargs["headers"]["If-None-Match"] == '"v1"'
    path.write_bytes(b"XYZ")
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert "If-None-Match" not in mirror.connection_manager.request.call_args.kwargs["headers"]


def test_explicit_no_cache_checks_newer_timestamp(mirror):
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    os.utime(path, (1000, 1000))
    mirror.connection_manager.request.return_value = response(
        200, {"Content-Length": "3", "Last-Modified": "Thu, 01 Jan 1970 00:50:00 GMT"}
    )
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a", use_cache=False)
    assert mirror.connection_manager.request.call_count == 1


def test_resolver_rejects_unsafe_address_and_releases_locks(monkeypatch):
    from mirror_url.exceptions import SecurityError

    dns = Mock(side_effect=[SecurityError("private address"), "8.8.8.8"])
    send = AsyncMock(return_value=response())
    monkeypatch.setattr("mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", dns)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", send)

    async def run():
        transport = SecureAsyncTransport()
        request = httpx.Request("HEAD", BASE)
        try:
            with pytest.raises(SecurityError):
                await asyncio.wait_for(transport.handle_async_request(request), 1)
            assert not send.called
            await asyncio.wait_for(transport.handle_async_request(request), 1)
            assert send.call_count == 1
        finally:
            await transport.aclose()

    asyncio.run(run())


@pytest.mark.parametrize("etag", ['"old"', '"new"'])
def test_legacy_cache_needs_verified_file_metadata(mirror, etag):
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    mirror.cache_manager = SimpleNamespace(get_file_metadata=lambda _: {"etag": '"old"', "size": 3})
    mirror.connection_manager.request.return_value = response(
        200, {"ETag": etag, "Content-Length": "3"}
    )
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert "If-None-Match" not in mirror.connection_manager.request.call_args.kwargs["headers"]


def test_download_uses_supplied_safe_local_path(mirror):
    path = mirror.target_dir / "sanitized-name"
    mirror._get_cached_filename = lambda _: "raw-name"
    mirror.connection_manager.request.return_value = response(200, {"Content-Length": "3"}, b"NEW")
    assert mirror._download_file_single(BASE + "raw-name", path)
    assert path.read_bytes() == b"NEW"
    assert not (mirror.target_dir / "raw-name").exists()


def test_bfs_records_skipped_subtree_for_cleanup(mirror, monkeypatch):
    mirror.config.handle_symlinks = True
    mirror.config.symlink_mode = "skip"
    mirror.per_ip_limiter = SimpleNamespace(wait=lambda _: None)
    child = BASE + "duplicate/"
    mirror.scanner = SimpleNamespace(
        scan_directory_sequential=lambda url: ([], [child]) if url == BASE else ([], [])
    )
    mirror._check_directory_symlink = lambda url, *args: (url == child, BASE)
    mirror._symlink_confidence_note = lambda *args: ""
    mirror._is_within_target_scope = lambda _: True
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda _: "8.8.8.8")
    assert list(mirror._discover_directories_bfs()) == [BASE]
    assert mirror.cleanup_protected_prefixes == {child}
    path = mirror.target_dir / "duplicate" / "keep"
    path.parent.mkdir()
    path.write_bytes(b"keep")
    mirror.config.cleanup_policy = CleanupPolicy.DELETE
    mirror.clean_obsolete(set())
    assert path.read_bytes() == b"keep"


@pytest.mark.parametrize("streaming", [False, True])
def test_atomic_replacement_failure_keeps_existing_file(parallel, mirror, monkeypatch, streaming):
    parallel.use_streaming = streaming
    path = mirror.target_dir / "a"
    path.write_bytes(b"old final")
    mirror.connection_manager.request.side_effect = range_handler
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    monkeypatch.setattr(
        "mirror_url.download.os.replace", Mock(side_effect=OSError("replace failed"))
    )
    assert parallel.download_parallel(download) is False
    assert path.read_bytes() == b"old final"


def test_cache_failure_after_streaming_publication_is_nonfatal(parallel, mirror):
    mirror.cache_manager = SimpleNamespace(
        save_file_metadata=Mock(side_effect=OSError("cache unavailable"))
    )
    path = mirror.target_dir / "a"
    mirror.connection_manager.request.side_effect = range_handler
    download = parallel.create_chunks(BASE + "a", path, 6)
    assert download is not None
    assert parallel.download_parallel(download)
    assert path.read_bytes() == b"ABCDEF"
    assert download.status == "completed"


def test_cancelled_async_verification_requires_download(mirror):
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    manager = AsyncManager()

    async def cancelled_head(*args):
        raise asyncio.CancelledError

    manager.head = cancelled_head
    mirror.adaptive_async_manager = manager
    item = (BASE + "a", path)
    assert asyncio.run(mirror._check_files_async([item])) == [item]
