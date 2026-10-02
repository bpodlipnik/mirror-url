"""Regression tests for the v3.1.76 audit fixes."""

import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.async_connection import AdaptiveAsyncManager
from mirror_url.connection import ConnectionManager
from mirror_url.download_integrity import save_resume_metadata
from mirror_url.exceptions import ParsingError, URLScopeError
from mirror_url.health import HealthChecker
from mirror_url.metrics import MetricsCollector
from mirror_url.scanner import DirectoryScanner
from test_immediate_audit_fixes import BASE, response
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel


@pytest.fixture
def public_dns(monkeypatch):
    monkeypatch.setattr("socket.gethostbyname", lambda _: "93.184.216.34")
    monkeypatch.setattr(
        "socket.getaddrinfo", lambda *a, **kw: [(2, 1, 6, "", ("93.184.216.34", 443))]
    )


@pytest.mark.parametrize(
    "target,security",
    [
        ("http://example.com/root/a", True),
        ("https://example.com:8443/root/a", True),
        ("https://cdn.example.com/root/a", True),
        ("https://outside.example/root/a", False),
    ],
)
def test_sync_blocks_outside_origin_redirect(tmp_path, monkeypatch, public_dns, target, security):
    config = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path,
        log_path=tmp_path,
        security_validation=security,
        max_retries=0,
    )
    manager = ConnectionManager(config, MetricsCollector())
    contacted = []

    def handle(request):
        contacted.append(str(request.url))
        return (
            httpx.Response(302, headers={"Location": target})
            if len(contacted) == 1
            else httpx.Response(200, content=b"ok")
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
        with pytest.raises(URLScopeError):
            manager.request(BASE + "a")
        assert contacted == [BASE + "a"]
    manager.close()


def test_pool_warmup_blocks_unrelated_host_and_path(tmp_path, monkeypatch):
    cfg = MirrorConfig(base_url=BASE, dest_path=tmp_path, log_path=tmp_path)
    manager = ConnectionManager(cfg, MetricsCollector())
    contacted = []
    target = "https://outside.example/elsewhere/"

    def handle(request):
        contacted.append(str(request.url))
        return (
            httpx.Response(302, headers={"Location": target})
            if len(contacted) == 1
            else httpx.Response(200)
        )

    with httpx.Client(transport=httpx.MockTransport(handle), follow_redirects=True) as client:
        monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
        manager.connection_pool.warm_up([BASE + "a"])
        assert contacted == [BASE + "a"]
    manager.close()


def test_connection_close_stops_owned_coordinator(tmp_path):
    manager = ConnectionManager(
        MirrorConfig(base_url=BASE, dest_path=tmp_path, log_path=tmp_path), MetricsCollector()
    )
    coordinator = manager.concurrency_manager
    try:
        manager.close()
        assert not coordinator.monitor_thread.is_alive()
        assert coordinator._shutdown
    finally:
        coordinator.shutdown()


def test_auto_mode_without_parallel_manager_falls_back_to_download(mirror):
    mirror.config.async_metadata = False
    mirror.config.no_cache = True
    mirror.parallel_manager = None
    mirror.auto_tuner = None
    mirror.multi_progress = Mock()
    mirror.performance_monitor.get_summary.return_value = {"total_operations": 0}
    mirror.get_remote_files = Mock(return_value=[BASE + "a"])
    mirror._check_files_sync = Mock(return_value=[(BASE + "a", mirror.target_dir / "a")])
    mirror._get_file_size = lambda _: 3
    mirror.check_disk_space = lambda _: True
    mirror.clean_obsolete = Mock()
    mirror.download_file_with_resume = Mock()
    assert mirror.sync() is True
    mirror.download_file_with_resume.assert_called_once_with(BASE + "a", mirror.target_dir / "a", 3)
    assert not (mirror.target_dir / "a").exists()


def test_real_parallel_initialization_failure_falls_back_without_thread_leak(tmp_path, monkeypatch):
    from mirror_url.download import ParallelDownloadManager

    allocated = []

    class RecordingManager(ParallelDownloadManager):
        def __init__(self, *args, **kwargs):
            allocated.append(self)
            super().__init__(*args, **kwargs)

    monkeypatch.setattr("mirror_url._core._base.ParallelDownloadManager", RecordingManager)
    monkeypatch.setattr(MirrorURL, "test_connection", lambda _: True)
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    blocking = tmp_path / "assembly"
    blocking.write_bytes(b"regular file")
    cfg = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        chunk_assembly_dir=blocking,
        async_metadata=False,
        no_cache=True,
        connection_pool_prewarm=False,
    )
    try:
        with MirrorURL(cfg) as m:
            assert m.parallel_manager is None
            m.get_remote_files = Mock(return_value=[BASE + "a"])
            m._check_files_sync = Mock(return_value=[(BASE + "a", m.target_dir / "a")])
            m._get_file_size = lambda _: 3
            m.check_disk_space = lambda _: True
            m.download_file_with_resume = Mock()
            assert m.sync() is True
            m.download_file_with_resume.assert_called_once_with(BASE + "a", m.target_dir / "a", 3)
            assert not (m.target_dir / "a").exists()
        assert allocated[0]._cleanup_thread is None
        assert allocated[0]._shutdown
    finally:
        for child in allocated:
            child.shutdown()


def test_list_files_boundary_scan_failure_marks_incomplete(mirror):
    mirror.config.max_depth = 1
    mirror.scanner.scan_directory_sequential = Mock(
        side_effect=lambda url: (
            ([BASE + "a"], [BASE + "child/"])
            if url == BASE
            else (_ for _ in ()).throw(ParsingError("child unavailable"))
        )
    )
    assert mirror.list_files() is False
    assert mirror.scan_incomplete is True


def test_enabled_parser_fallback_extracts_listing(mirror, monkeypatch):
    mirror.base_url = BASE
    mirror.config.fast_parsing_fallback = True
    mirror.config.no_cache = True
    mirror.connection_manager.request.return_value = response(
        200, content=b'<html><a href="a">a</a></html>'
    )
    scanner = DirectoryScanner(mirror)
    ParserError = pytest.importorskip("lxml.etree").ParserError

    monkeypatch.setattr(
        "mirror_url.scanner.html.fromstring", Mock(side_effect=ParserError("parser failure"))
    )
    files, subdirs = scanner.scan_directory_sequential(BASE)
    assert files == [BASE + "a"]
    assert subdirs == []
    assert scanner.fast_parse_count == 1
    mirror.config.fast_parsing_fallback = False
    with pytest.raises(ParsingError, match="parser failure"):
        scanner.scan_directory_sequential(BASE)


def test_connect_failure_preserves_resumable_partial(mirror, monkeypatch, public_dns):
    partial = mirror.partial_manager.get_partial_path(mirror.target_dir / "a")
    partial.write_bytes(b"abc")
    save_resume_metadata(partial, BASE + "a", '"v1"', 6)
    manager = ConnectionManager(
        mirror.config.model_copy(update={"security_validation": False}), mirror.metrics
    )

    def handle(request):
        raise httpx.ConnectError("connection refused", request=request)

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
        mirror.connection_manager = manager
        assert mirror._download_file_single(BASE + "a", mirror.target_dir / "a") is False
        assert partial.read_bytes() == b"abc"
        assert partial.with_name(partial.name + ".json").exists()
    manager.close()


@pytest.mark.parametrize("streaming", [False, True])
def test_valid_255_character_filename_can_stage_and_publish(parallel, monkeypatch, streaming):
    local = parallel.mirror.target_dir / ("a" * 255)
    local.write_bytes(b"old")
    size = 4
    parallel.use_streaming = streaming
    monkeypatch.setattr(parallel, "get_chunk_count", lambda _: 2)
    parallel.connection_manager.request.return_value = response(
        200, {"Content-Length": str(size), "Accept-Ranges": "bytes", "ETag": '"v1"'}
    )
    download = parallel.create_chunks(BASE + "a", local, size)
    assert local.read_bytes() == b"old"
    if streaming:
        download.staging_path.write_bytes(b"abcd")
    for chunk in download.chunks:
        chunk.status = "completed"
        if not streaming:
            chunk.temp_path.write_bytes(b"ab" if chunk.chunk_id == 0 else b"cd")
    assert (
        parallel._finish_streaming(download) if streaming else parallel.assemble_file(download)
    ) is True
    assert local.read_bytes() == b"abcd"


@pytest.mark.parametrize("streaming,traditional", [(False, False), (True, False), (False, True)])
def test_auto_concurrency_created_after_explicit_or_auto_mode_selection(
    tmp_path, monkeypatch, streaming, traditional
):
    monkeypatch.setattr(MirrorURL, "test_connection", lambda _: True)
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    cfg = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        auto_concurrency=True,
        parallel_downloads=traditional,
        streaming_parallel=streaming,
        async_metadata=False,
        no_cache=True,
        connection_pool_prewarm=False,
    )
    with MirrorURL(cfg) as m:
        assert m.parallel_manager is not None
        if not streaming and not traditional:
            from mirror_url.enums import DownloadMethod

            m.get_remote_files = Mock(return_value=[BASE + "a"])
            m._check_files_sync = Mock(return_value=[(BASE + "a", m.target_dir / "a")])
            m._get_file_size = lambda _: 3
            m.check_disk_space = lambda _: True
            m.download_file_with_resume = Mock(return_value=True)
            m.parallel_manager.auto_select_method = Mock(
                return_value=DownloadMethod.TRADITIONAL_PARALLEL
            )
            assert m.sync()
            m.parallel_manager.auto_select_method.assert_called_once()
        assert m.auto_tuner is not None
        tuner = m.auto_tuner
        m._initialize_auto_tuner()
        assert m.auto_tuner is tuner


def test_adaptive_changes_applied_between_metadata_batches(mirror, monkeypatch):
    mirror.config.security_validation = False
    mirror.config.adaptive_async = True
    mirror.config.adaptive_start_concurrency = 2
    mirror.config.async_workers = 10
    manager = AdaptiveAsyncManager(mirror.config, mirror.metrics)
    manager.profile_server = AsyncMock(return_value=True)
    profile = manager._get_profile(BASE + "a")
    monkeypatch.setattr(profile, "should_scale_up", lambda: True)
    observed = []

    def handle(request):
        observed.append((manager.get_concurrency(), manager._pending_concurrency))
        return httpx.Response(200, headers={"Content-Length": "3"})

    original_init = manager._init_client

    async def init():
        await original_init()
        await manager._client.aclose()
        manager._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))

    monkeypatch.setattr(manager, "_init_client", init)
    mirror.adaptive_async_manager = manager
    urls = [BASE + str(i) for i in range(200)]
    for url in urls:
        (mirror.target_dir / url.rsplit("/", 1)[-1]).write_bytes(b"abc")
    asyncio.run(mirror._check_files_async(urls))
    assert len(observed) == 200
    assert observed[0][0] == 2
    assert any(current > 2 for current, pending in observed)
    assert any(pending == 4 for current, pending in observed)
    assert manager.get_concurrency() > 2


def test_suffix_preserves_remote_component(mirror):
    mirror.config.dir_suffix = "CON"
    assert mirror._get_target_base_url() == BASE + "CON/"


def test_health_status_reflects_failed_files_and_recorded_errors(mirror):
    mirror.base_url = BASE
    mirror.start_time = 0
    mirror.connection_manager.circuit_breaker_manager = None
    mirror.files_failed.increment(20)
    mirror.metrics.add_error("download failed", "download_failed")
    checker = HealthChecker(mirror)
    assert checker.is_healthy() is False
    status = checker.get_status()
    assert status.status == "degraded"
    assert status.errors[-1]["message"] == "download failed"


def test_trusted_server_keeps_default_50ms_pacing(tmp_path):
    cfg = MirrorConfig(base_url=BASE, dest_path=tmp_path, log_path=tmp_path, trusted_server=True)
    manager = ConnectionManager(cfg, MetricsCollector())
    try:
        assert manager.rate_limiter.min_interval == 0.05
        assert manager.connection_pool.rate_limiter.min_interval == 0.05
    finally:
        manager.close()
        manager.concurrency_manager.shutdown()


@pytest.mark.parametrize("streaming", [False, True])
def test_chunk_publication_restores_mtime_and_detects_newer_remote_version(
    parallel, monkeypatch, streaming
):
    from email.utils import formatdate

    parallel.use_streaming = streaming
    monkeypatch.setattr(parallel, "get_chunk_count", lambda _: 2)
    parallel.connection_manager.request.return_value = response(
        200,
        {
            "Content-Length": "4",
            "Accept-Ranges": "bytes",
            "ETag": '"v1"',
            "Last-Modified": formatdate(1000, usegmt=True),
        },
    )
    final = parallel.mirror.target_dir / "a"
    download = parallel.create_chunks(BASE + "a", final, 4)
    if streaming:
        download.staging_path.write_bytes(b"abcd")
    for chunk in download.chunks:
        chunk.status = "completed"
        if not streaming:
            chunk.temp_path.write_bytes(b"ab" if chunk.chunk_id == 0 else b"cd")
    assert (
        parallel._finish_streaming(download) if streaming else parallel.assemble_file(download)
    ) is True
    assert final.stat().st_mtime == 1000
    parallel.mirror.config.no_etag = True
    parallel.mirror.connection_manager.request.return_value = response(
        200, {"Content-Length": "4", "ETag": '"v2"', "Last-Modified": formatdate(2000, usegmt=True)}
    )
    assert parallel.mirror.file_exists_and_up_to_date(final, BASE + "a") is False


@pytest.mark.parametrize("last_modified", ["invalid date", "Fri, 02 Oct 2026 12:00:00 GMT"])
def test_chunk_timestamp_invalid_header_or_filesystem_failure_is_nonfatal(
    parallel, monkeypatch, last_modified
):
    monkeypatch.setattr(parallel, "get_chunk_count", lambda _: 2)
    parallel.connection_manager.request.return_value = response(
        200,
        {
            "Content-Length": "4",
            "Accept-Ranges": "bytes",
            "ETag": '"v1"',
            "Last-Modified": last_modified,
        },
    )
    monkeypatch.setattr(
        "mirror_url.download.os.utime", Mock(side_effect=OSError("timestamp unsupported"))
    )
    download = parallel.create_chunks(BASE + "a", parallel.mirror.target_dir / "a", 4)
    download.staging_path.write_bytes(b"abcd")
    for chunk in download.chunks:
        chunk.status = "completed"
    assert parallel._finish_streaming(download)
    assert download.final_path.read_bytes() == b"abcd"


@pytest.mark.parametrize("use_mmap", [False, True])
def test_assembly_reads_multiple_bounded_blocks(parallel, monkeypatch, use_mmap):
    from test_transfer_storage_lifecycle import make_download

    download = make_download(parallel)
    parallel.MMAP_MAX_FILE_SIZE = 100 if use_mmap else 0
    monkeypatch.setattr("mirror_url.download.STREAMING_WRITE_BUFFER_SIZE", 2)
    original = parallel._read_chunk_data
    reads = []

    def blocks(path):
        for data in original(path):
            reads.append(len(data))
            yield data

    monkeypatch.setattr(parallel, "_read_chunk_data", blocks)
    assert parallel.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"
    assert reads == [2, 1, 2, 1]


def test_assembly_rejects_overlong_chunk_before_publication(parallel):
    from test_transfer_storage_lifecycle import make_download

    download = make_download(parallel)
    download.chunks[0].temp_path.write_bytes(b"too many bytes")
    assert not parallel.assemble_file(download)
    assert download.final_path.read_bytes() == b"original"


@pytest.mark.parametrize("healthy, expected", [(True, 200), (False, 503)])
def test_health_endpoint_reports_http_status_from_health_checker(mirror, healthy, expected):
    from types import SimpleNamespace

    from mirror_url.health import HealthCheckHandler

    mirror.base_url = BASE
    mirror.start_time = 0
    mirror.connection_manager.circuit_breaker_manager = None
    mirror.connection_ok = healthy
    mirror.health_checker = HealthChecker(mirror)
    handler = SimpleNamespace(mirror_instance=mirror, _send_json=Mock())
    HealthCheckHandler._handle_health(handler)
    code, payload = handler._send_json.call_args.args
    assert code == expected
    assert payload["status"] == ("healthy" if healthy else "degraded")
