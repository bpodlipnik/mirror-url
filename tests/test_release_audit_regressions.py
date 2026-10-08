"""Regression tests for the 3.1.69 line-by-line audit findings."""

import asyncio
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from pydantic import ValidationError

from mirror_url import MirrorConfig
from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager
from mirror_url.cache import CacheManager
from mirror_url.circuit_breaker import CircuitBreaker
from mirror_url.connection import ConnectionManager, ConnectionPool
from mirror_url.constants import DOWNLOAD_CHUNK_SIZE, PARTIAL_SUFFIX
from mirror_url.enums import CleanupPolicy
from mirror_url.metrics import MetricsCollector
from mirror_url.parsing import extract_links_fast
from mirror_url.progress import ProgressTracker
from mirror_url.rate_limiter import ChunkAwareRateLimiter
from mirror_url.scanner import DirectoryScanner
from mirror_url.storage import DiskBackedSet
from mirror_url.transport import SecureTransport
from test_immediate_audit_fixes import BASE, response
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel


@pytest.mark.parametrize("symlink", [False, True])
def test_disk_probe_preserves_existing_files(parallel, tmp_path, monkeypatch, symlink):
    target = parallel.mirror.target_dir / ".speed_test"
    outside = tmp_path / "outside-important"
    outside.write_bytes(b"important")
    if symlink:
        target.symlink_to(outside)
    else:
        target.write_bytes(b"important")
    import sys

    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace(disk_partitions=list))
    monkeypatch.setattr("mirror_url.download.random.randint", lambda *args: 0)
    parallel._detect_ssd()
    assert target.read_bytes() == b"important"
    if symlink:
        assert outside.read_bytes() == b"important"


def test_partial_storage_preserves_remote_suffix_files(mirror):
    legitimate = mirror.target_dir / ("a" + PARTIAL_SUFFIX)
    legitimate.write_bytes(b"other remote file")
    mirror.connection_manager.request.return_value = response(
        200, {"Content-Length": "3", "ETag": '"v1"'}, b"NEW"
    )
    final = mirror.target_dir / "a"
    assert mirror._download_file_single(BASE + "a", final)
    assert final.read_bytes() == b"NEW"
    assert legitimate.exists()


def test_stale_cleanup_preserves_completed_remote_files(mirror):
    final = mirror.target_dir / ("science" + PARTIAL_SUFFIX)
    mirror.connection_manager.request.return_value = response(
        200,
        {"Content-Length": "3", "ETag": '"v1"', "Last-Modified": "Thu, 01 Jan 1970 00:16:40 GMT"},
        b"NEW",
    )
    assert mirror._download_file_single(BASE + "science" + PARTIAL_SUFFIX, final)
    count = mirror.partial_manager.cleanup_stale_partials()
    assert count == 0
    assert final.read_bytes() == b"NEW"


def test_resume_metadata_preserves_remote_json_files(mirror):
    legitimate = mirror.target_dir / ("a" + PARTIAL_SUFFIX + ".json")
    legitimate.write_bytes(b"legitimate server resource")
    mirror.connection_manager.request.return_value = response(200, {"Content-Length": "3"}, b"NEW")
    assert mirror._download_file_single(BASE + "a", mirror.target_dir / "a")
    assert legitimate.exists()


def test_owned_partial_symlink_is_rejected(mirror, tmp_path):
    outside = tmp_path / "outside"
    outside.write_bytes(b"important")
    partial = mirror.partial_manager.get_partial_path(mirror.target_dir / "a")
    partial.symlink_to(outside)
    mirror.connection_manager.request.return_value = response(200, {"Content-Length": "3"}, b"NEW")
    final = mirror.target_dir / "a"
    assert not mirror._download_file_single(BASE + "a", final)
    assert outside.read_bytes() == b"important"
    assert not final.exists()


def test_disk_set_probe_preserves_existing_file(tmp_path):
    probe = tmp_path / ".write_test"
    probe.write_bytes(b"important")
    store = DiskBackedSet(tmp_path)
    assert probe.read_bytes() == b"important"
    store.clear()


def test_cache_failure_does_not_fail_published_download(mirror):
    mirror.cache_manager = SimpleNamespace(
        save_file_metadata=Mock(side_effect=OSError("cache unavailable"))
    )
    mirror.connection_manager.request.return_value = response(
        200, {"Content-Length": "3", "ETag": '"v1"'}, b"NEW"
    )
    final = mirror.target_dir / "a"
    final.write_bytes(b"old")
    assert mirror._download_file_single(BASE + "a", final)
    assert final.read_bytes() == b"NEW"
    assert mirror.files_failed.value() == 0
    assert mirror.files_processed.value() == 1


def test_adaptive_configuration_resizes_live_gate(mirror):
    mirror.config.adaptive_start_concurrency = 17
    manager = AdaptiveAsyncManager(mirror.config, mirror.metrics)

    async def run():
        async with manager:
            initial = manager._semaphore._value
            manager._pending_concurrency = 9
            await manager.apply_pending_concurrency_change()
            actual = manager._semaphore._value
            assert initial == 17
            assert actual == 9
            assert manager.get_concurrency() == 9

    asyncio.run(run())


@pytest.mark.parametrize("kind", ["fixed", "adaptive"])
def test_async_429_retries_without_success_accounting(mirror, kind, monkeypatch):
    from unittest.mock import AsyncMock

    monkeypatch.setattr("mirror_url.async_connection.asyncio.sleep", AsyncMock())
    cls = AdaptiveAsyncManager if kind == "adaptive" else AsyncConnectionManager
    manager = cls(
        mirror.config.model_copy(update={"max_retries": 2, "security_validation": False}),
        mirror.metrics,
    )
    calls = []

    def handle(request):
        calls.append(str(request.url))
        return httpx.Response(429, headers={"Retry-After": "1"})

    async def run():
        async with manager:
            await manager._client.aclose()
            manager._client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
            result = await manager.head(BASE + "a")
            assert result is None
            assert len(calls) == 3
            assert (
                manager.circuit_breaker_manager.get_stats()["example.com"]["total_successes"] == 0
            )

    asyncio.run(run())


def test_async_redirect_is_blocked_before_out_of_scope_request(mirror):
    manager = AdaptiveAsyncManager(mirror.config, mirror.metrics)
    visited = []

    def handle(request):
        visited.append(str(request.url))
        if len(visited) == 1:
            return httpx.Response(307, headers={"Location": "https://outside.example/secret"})
        return httpx.Response(200, headers={"Content-Length": "3"})

    async def run():
        async with manager:
            await manager._client.aclose()
            manager._client = httpx.AsyncClient(
                transport=httpx.MockTransport(handle), follow_redirects=True
            )
            assert await manager.head(BASE + "a") is None
            assert visited == [BASE + "a"]

    asyncio.run(run())


def test_secure_transport_uses_http2_and_pool_settings(mirror):
    pool = ConnectionPool(config=mirror.config)
    client = pool._create_client()
    try:
        actual = client._transport._pool
        assert actual._http2 is True
        assert actual._max_keepalive_connections == 50
        assert actual._keepalive_expiry == 120
    finally:
        client.close()


def test_transport_retry_preserves_port_body_and_timeout(monkeypatch):
    sent = []

    def handle(self, request):
        sent.append(request)
        if len(sent) == 1:
            raise httpx.ConnectError("retry", request=request)
        return response()

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", handle)
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", lambda _: "8.8.8.8"
    )
    transport = SecureTransport()
    try:
        transport.handle_request(
            httpx.Request(
                "POST",
                "https://example.com:8443/root/a",
                content=b"payload",
                extensions={"timeout": {"read": 1}},
            )
        )
        assert sent[0].headers["host"] == "example.com:8443"
        assert sent[1].headers["host"] == "example.com:8443"
        assert sent[1].extensions["timeout"] == {"read": 1}
        assert b"".join(sent[1].stream) == b"payload"
    finally:
        transport.close()


def test_download_throttles_before_network_body_finishes(mirror, monkeypatch):
    events = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            events.append("network first")
            yield b"A" * DOWNLOAD_CHUNK_SIZE
            events.append("network last")
            yield b"B" * DOWNLOAD_CHUNK_SIZE

        def close(self):
            events.append("network closed")

    manager = ConnectionManager(
        mirror.config.model_copy(update={"security_validation": False}), mirror.metrics
    )
    client = httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200, headers={"Content-Length": str(2 * DOWNLOAD_CHUNK_SIZE)}, stream=Body()
            )
        )
    )
    manager.connection_pool.get_client = lambda _: client
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    mirror.connection_manager = manager
    mirror.bandwidth_limiter = SimpleNamespace(throttle=lambda n: events.append("throttle"))
    try:
        assert mirror._download_file_single(BASE + "a", mirror.target_dir / "a")
        assert events.index("throttle") < events.index("network last")
        assert events[-1] == "network closed"
    finally:
        client.close()
        manager.concurrency_manager.shutdown()


@pytest.mark.parametrize("per_ip", [False, True])
def test_chunk_limiter_reserves_each_request(monkeypatch, per_ip):
    limiter = ChunkAwareRateLimiter(per_ip=per_ip)
    sleeps = []
    monkeypatch.setattr("mirror_url.rate_limiter.time.time", lambda: 1000)
    monkeypatch.setattr("mirror_url.rate_limiter.time.sleep", sleeps.append)
    for _ in range(10):
        limiter.wait("8.8.8.8")
    assert len(sleeps) == 9
    assert (limiter.ip_last_requests["8.8.8.8"] if per_ip else limiter.last_request) > 1000


def test_one_chunk_maximum_is_honored(parallel):
    parallel.enabled = True
    parallel.max_chunks_per_file = 1
    assert parallel.get_chunk_count(30 * 1024 * 1024) == 1


def test_scope_rejects_wrong_origin_and_path(mirror):
    mirror.base_parsed = mirror.target_parsed
    results = [
        mirror._is_url_within_scope("https://outside.example/root/a"),
        mirror._is_url_within_scope("https://example.com/outside/a"),
    ]
    assert results == [False, False]


def test_no_cache_scans_are_fresh(mirror):
    mirror.config.no_cache = True
    mirror.config.cache_html = False
    mirror.config.refresh_cache = True
    scanner = DirectoryScanner(mirror)
    scanner._perform_scan = Mock(side_effect=[([BASE + "old"], []), ([BASE + "new"], [])])
    first = scanner.scan_directory_sequential(BASE)
    second = scanner.scan_directory_sequential(BASE)
    assert first != second
    assert scanner._perform_scan.call_count == 2


def test_parser_preserves_declared_non_utf8_filename():
    links = extract_links_fast(b'<meta charset="iso-8859-1"><a href="caf\xe9.dat">x</a>')
    assert links == ["café.dat"]


def prepare_empty_sync(mirror):
    mirror.config.async_metadata = False
    mirror.check_disk_space = Mock(return_value=True)
    mirror.get_remote_files = Mock(return_value=[])
    mirror._check_files_sync = Mock(return_value=[])
    mirror.parallel_manager = None
    mirror.performance_monitor.get_summary.return_value = {"total_operations": 0}


def test_complete_empty_scan_runs_requested_cleanup(mirror):
    prepare_empty_sync(mirror)
    mirror.config.cleanup_policy = CleanupPolicy.DELETE
    path = mirror.target_dir / "obsolete"
    path.write_bytes(b"old")
    mirror.clean_obsolete = Mock(wraps=mirror.clean_obsolete)
    assert mirror.sync()
    assert mirror.clean_obsolete.call_count == 1
    assert not path.exists()


def test_incomplete_scan_reports_failure(mirror, caplog):
    prepare_empty_sync(mirror)
    mirror.scan_incomplete = True
    assert not mirror.sync()
    assert "Sync failed: incomplete remote scan" in caplog.text
    assert "Sync completed with 0 failures" not in caplog.text


def test_cleanup_failure_reports_failure(mirror, caplog):
    prepare_empty_sync(mirror)
    mirror.get_remote_files.return_value = [BASE + "keep"]
    mirror.multi_progress = Mock()
    mirror.config.cleanup_policy = CleanupPolicy.MOVE
    (mirror.target_dir / "obsolete").write_bytes(b"old")
    archive = mirror.target_dir.with_name(mirror.target_dir.name + "_obsolete")
    archive.write_bytes(b"blocks archive directory creation")
    assert not mirror.sync()
    assert mirror.metrics.metrics["cleanup_failed_operations"] > 0
    assert "cleanup failures" in caplog.text
    assert "Sync completed with 0 failures" not in caplog.text


def test_parse_samples_appear_in_summary(monkeypatch):
    metrics = MetricsCollector()
    times = iter([10.0, 11.0])
    monkeypatch.setattr("mirror_url.metrics.time.time", lambda: next(times, 12.0))
    metrics.start_parse_timer()
    metrics.stop_parse_timer()
    summary = metrics.get_summary()
    assert summary["parse_times"] == [1.0]
    assert metrics.metrics["parse_times"] == []


def test_progress_callbacks_and_actual_final_count():
    tracker = ProgressTracker(2, use_tqdm=False)
    callback = Mock()
    tracker.add_callback(callback)
    tracker.update(1)
    tracker.report_final()
    callback.assert_called_once_with(1, 2)
    assert tracker.completed == 1


def test_half_open_admits_only_configured_permits(monkeypatch):
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=1, half_open_limit=2)
    monkeypatch.setattr("mirror_url.circuit_breaker.time.time", lambda: 100.0)
    breaker.record_failure()
    monkeypatch.setattr("mirror_url.circuit_breaker.time.time", lambda: 102.0)
    permits = [breaker.can_execute() for _ in range(4)]
    assert permits == [True, True, False, False]


@pytest.mark.parametrize(
    "field,value",
    [
        ("max_retries", -1),
        ("retry_delay", 0),
        ("adaptive_error_threshold", 1.1),
        ("adaptive_start_concurrency", 0),
    ],
)
def test_invalid_runtime_controls_rejected(tmp_path, field, value):
    with pytest.raises(ValidationError):
        MirrorConfig(base_url=BASE, dest_path=tmp_path, log_path=tmp_path, **{field: value})


def test_collision_preflight_rejects_before_publication(mirror):
    mirror.config.max_filename_len = 5
    with pytest.raises(ValueError, match="same local filename"):
        mirror._validate_remote_paths([BASE + "long-one.txt", BASE + "long-two.txt"])
    assert list(mirror.target_dir.iterdir()) == []


def test_reserved_or_unowned_partial_directory_is_preserved(mirror):
    state = mirror.target_dir / ".mirror-url-state"
    state.mkdir()
    (state / "important").write_bytes(b"keep")
    with pytest.raises(ValueError, match="unowned"):
        mirror.partial_manager.get_partial_path(mirror.target_dir / "a")
    assert (state / "important").read_bytes() == b"keep"
    with pytest.raises(ValueError, match="reserved"):
        mirror._validate_remote_paths([BASE + ".mirror-url-state/important"])


def test_directory_depth_and_regex_ignore_query(mirror):
    mirror.config.max_depth = 1
    assert mirror._get_local_path_from_url(BASE + "child/a") == mirror.target_dir / "child/a"
    mirror.config.file_filters = [r".*\.fits$"]
    from mirror_url._core.urls import UrlMixin

    mirror._get_filename_fast = UrlMixin._get_filename_fast.__get__(mirror)
    assert mirror.matches_filter(BASE + "a.fits?download=1")


def test_quick_refresh_updates_expiry_timestamp_and_keeps_metadata(mirror):
    import json

    cache = CacheManager(mirror.target_dir / "cache.json", mirror.config, mirror.metrics)
    cache.save_file_metadata(mirror.target_dir / "a", '"v1"', 1, 3)
    cache.save({BASE: "signature"}, 1)
    data = json.loads(cache.cache_file.read_text())
    data["_meta"]["last_full_run"] = "2000-01-01T00:00:00"
    cache.cache_file.write_text(json.dumps(data))
    assert cache.refresh_timestamp()
    refreshed = json.loads(cache.cache_file.read_text())
    assert refreshed["_meta"]["last_full_run"] != "2000-01-01T00:00:00"
    assert refreshed["_files"] == data["_files"]


def test_final_sync_persists_file_metadata(mirror):
    prepare_empty_sync(mirror)
    mirror.get_remote_files.return_value = [BASE + "a"]
    path = mirror.target_dir / "a"
    mirror._check_files_sync.return_value = [(BASE + "a", path)]
    mirror.multi_progress = Mock()
    mirror.config.sequential_downloads = True
    mirror.cache_manager = CacheManager(
        mirror.target_dir / "cache.json", mirror.config, mirror.metrics
    )
    mirror.connection_manager.request.return_value = response(
        200, {"Content-Length": "3", "ETag": '"v1"'}, b"NEW"
    )
    assert mirror.sync()
    fresh = CacheManager(mirror.cache_manager.cache_file, mirror.config, mirror.metrics)
    assert fresh.load()[0]
    assert fresh.get_file_metadata(path)["etag"] == '"v1"'


def test_command_logging_redacts_url_credentials():
    from mirror_url.utils import sanitize_command_line

    text = sanitize_command_line(
        ["mirror-url", "--url=https://alice:secret@example.com/root?token=hidden"]
    )
    assert "secret" not in text and "hidden" not in text and "alice" not in text


@pytest.mark.parametrize("mode", ["fixed", "adaptive"])
def test_async_head_retries_503_then_succeeds(mirror, monkeypatch, mode):
    from unittest.mock import AsyncMock

    sleep = AsyncMock()
    monkeypatch.setattr("mirror_url.async_connection.asyncio.sleep", sleep)
    calls = []

    def handler(request):
        calls.append(request)
        return (
            httpx.Response(503, headers={"Retry-After": "7"})
            if len(calls) == 1
            else httpx.Response(200)
        )

    cls = AsyncConnectionManager if mode == "fixed" else AdaptiveAsyncManager
    manager = cls(
        mirror.config.model_copy(update={"security_validation": False, "max_retries": 1}),
        mirror.metrics,
    )

    async def run():
        async with manager:
            await manager._client.aclose()
            manager._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            assert (await manager.head(BASE + "a")).status_code == 200

    asyncio.run(run())
    assert len(calls) == 2
    assert any(call.args[0] >= 7 for call in sleep.await_args_list)


def test_fixed_async_available_before_initialization_and_honors_workers(mirror):
    mirror.config.async_workers = 3
    manager = AsyncConnectionManager(mirror.config, mirror.metrics)
    assert manager.is_available()

    async def run():
        async with manager:
            assert manager._semaphore._value == 3

    asyncio.run(run())
    assert not manager.is_available()


def test_adaptive_shrink_keeps_old_waiters_in_same_gate():
    from mirror_url.async_primitives import ResizableSemaphore

    async def run():
        gate = ResizableSemaphore(2)
        entered = asyncio.Event()
        async with gate:
            async with gate:

                async def wait_for_gate():
                    async with gate:
                        entered.set()

                waiter = asyncio.create_task(wait_for_gate())
                await asyncio.sleep(0)
                await gate.resize(1)
            await asyncio.sleep(0)
            assert not entered.is_set()
        await asyncio.wait_for(waiter, 1)
        assert entered.is_set()

    asyncio.run(run())


def test_live_file_autotuning_changes_admission(mirror, monkeypatch):
    import threading
    import time

    prepare_empty_sync(mirror)
    mirror.multi_progress = Mock()
    mirror.config.parallel_downloads = True
    mirror.config.max_concurrent_downloads = 3
    mirror.download_queue = []
    mirror.auto_tuner = Mock()
    mirror.auto_tuner.get_concurrency.return_value = 1
    mirror.auto_tuner.record_throughput.side_effect = [3, None, None, None, None, None]
    mirror.auto_tuner.get_stats.return_value = {
        "adjustments": 1,
        "current_concurrency": 3,
        "start_concurrency": 1,
        "last_throughput": 1,
    }
    items = [(BASE + str(i), mirror.target_dir / str(i)) for i in range(6)]
    mirror.get_remote_files.return_value = [url for url, _ in items]
    mirror._check_files_sync.return_value = items
    mirror._get_file_size = Mock(return_value=1)
    lock = threading.Lock()
    active = peak = 0

    def download(*args):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.03)
        with lock:
            active -= 1
        mirror.files_processed.increment(1)
        return True

    mirror.download_file_with_resume = download
    monkeypatch.setattr("mirror_url._core.report.AUTO_CONCURRENCY_SAMPLES", 1)
    assert mirror.sync()
    assert peak == 3
    assert mirror.files_processed.value() == 6


def test_config_validation_uses_runtime_schema_and_cli_merges_required_paths(tmp_path, monkeypatch):
    import sys

    from mirror_url import cli
    from mirror_url.config import validate_config_file

    cfg = tmp_path / "config.yaml"
    cfg.write_text("base_url: https://example.com/root/\nworkers: 3\n")
    assert not validate_config_file(cfg)[0]  # standalone validation needs all paths
    captured = []

    class Stop(Exception):
        pass

    class Capture:
        def __init__(self, config, **kwargs):
            captured.append(config)
            raise Stop()

    monkeypatch.setattr(cli, "MirrorURL", Capture)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--config",
            str(cfg),
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
    )
    try:
        cli.main()
    except SystemExit:
        pass
    assert captured and captured[0].workers == 3


def test_benchmark_uses_merged_config_and_returns_failure_status(tmp_path, monkeypatch):
    import sys

    from mirror_url import cli

    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f"base_url: {BASE}\ndest_path: {tmp_path / 'dest'}\nlog_path: {tmp_path / 'logs'}\nworkers: 3\nasync_workers: 17\n"
    )
    captured = []

    class Capture:
        def __init__(self, config):
            captured.append(config)
            self.connection_manager = object()
            self.scanner = object()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def install_signal_handlers(self):
            pass

        def benchmark(self):
            return {"connection_test": False}

    monkeypatch.setattr(cli, "MirrorURL", Capture)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--config", str(cfg), "--benchmark", "--workers", "4"]
    )
    with pytest.raises(SystemExit) as exited:
        cli.main()
    assert exited.value.code == 1
    assert captured[0].workers == 4 and captured[0].async_workers == 17


def test_streamed_request_releases_slot_only_on_close(mirror, monkeypatch):
    events = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            events.append("read")
            yield b"data"

        def close(self):
            events.append("closed")

    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Body()))
    )
    manager = ConnectionManager(
        mirror.config.model_copy(update={"security_validation": False}), mirror.metrics
    )
    old_coordinator = manager.concurrency_manager
    release = Mock()
    manager.concurrency_manager = SimpleNamespace(
        acquire_thread=lambda _: True, release_thread=release
    )
    manager.connection_pool.get_client = lambda _: client
    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    try:
        streamed = manager.request(BASE + "a", stream=True)
        assert events == [] and release.call_count == 0
        assert streamed.read() == b"data"
        assert release.call_count == 1
        streamed.close()
        assert release.call_count == 1
    finally:
        client.close()
        old_coordinator.shutdown()


def test_streamed_redirect_works_with_one_request_permit(mirror, monkeypatch):
    import threading

    calls = []

    def handle(request):
        calls.append(request)
        return (
            httpx.Response(307, headers={"Location": "b"})
            if len(calls) == 1
            else httpx.Response(200, content=b"ok")
        )

    client = httpx.Client(transport=httpx.MockTransport(handle))
    manager = ConnectionManager(
        mirror.config.model_copy(update={"security_validation": False}), mirror.metrics
    )
    manager.request_semaphore = threading.Semaphore(1)
    manager.connection_pool.get_client = lambda _: client
    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    try:
        result = manager.request(BASE + "a", stream=True, headers={"Range": "bytes=0-1"})
        assert result.read() == b"ok"
        assert [request.headers["Range"] for request in calls] == ["bytes=0-1"] * 2
    finally:
        client.close()
        manager.concurrency_manager.shutdown()


def test_shared_logging_creates_directory_and_bounds_utf8_names(tmp_path):
    import argparse
    import logging

    from mirror_url.cli import setup_shared_logging

    saved = list(logging.root.handlers)
    saved_level = logging.root.level
    args = argparse.Namespace(
        dir_suffix=["L1/v1", "é" * 300],
        log_file="run/name",
        log_path=tmp_path / "nested/logs",
        debug=False,
        verbose=False,
        quiet=False,
        print_logs=False,
        cleanup_policy=CleanupPolicy.SAFE_NO_DELETE,
        no_cache=False,
        refresh_cache=False,
        confirm_delete=False,
        max_depth=50,
        max_filename_len=255,
        max_concurrent_downloads=1,
        progress_bar=False,
        async_metadata=False,
        content_hash_small_files=False,
        request_delay=0.05,
        trusted_server=False,
        cache_html=False,
        bandwidth_limit=None,
        handle_symlinks=False,
        adaptive_batch_processing=False,
        use_disk_backed_sets=False,
        fast_parsing_fallback=False,
        connection_pool_prewarm=False,
        metrics_json=None,
    )
    try:
        setup_shared_logging(args)
        files = list(args.log_path.glob("*.log"))
        assert len(files) == 1 and len(files[0].name.encode("utf-8")) <= 255
    finally:
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            if handler not in saved:
                handler.close()
        logging.root.handlers[:] = saved
        logging.root.setLevel(saved_level)


def test_signal_handlers_are_opt_in_and_restored(mirror):
    import signal

    mirror.is_dry_run = True
    mirror._previous_signal_handlers = {}
    previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM)}
    mirror.install_signal_handlers()
    assert signal.getsignal(signal.SIGINT) == mirror._signal_handler
    # Restore at the beginning of cleanup even if unrelated fixture managers
    # cannot perform the rest of the full lifecycle.
    mirror.cleanup()
    assert all(signal.getsignal(s) == handler for s, handler in previous.items())


def test_incomplete_dry_run_reports_failure(mirror):
    prepare_empty_sync(mirror)
    mirror.config.dry_run = True
    mirror.scan_incomplete = True
    assert not mirror.sync()


def test_unicode_equivalent_mapping_is_rejected(mirror):
    with pytest.raises(ValueError, match="same local filename"):
        mirror._validate_remote_paths([BASE + "가.dat", BASE + "가.dat"])


@pytest.mark.parametrize(
    "field,value",
    [("max_chunks_per_file", 0), ("max_parallel_chunks_total", 0), ("min_chunk_size_mb", 0)],
)
def test_auto_mode_chunk_controls_are_validated(tmp_path, field, value):
    with pytest.raises(ValidationError):
        MirrorConfig(base_url=BASE, dest_path=tmp_path, log_path=tmp_path, **{field: value})


def test_default_connection_pool_can_build_without_explicit_config():
    pool = ConnectionPool()
    client = pool._create_client()
    try:
        assert client._transport._pool._http2 is True
    finally:
        client.close()


def test_failed_unowned_state_download_cannot_delete_existing_state(mirror):
    prepare_empty_sync(mirror)
    mirror.multi_progress = Mock()
    mirror.config.sequential_downloads = True
    mirror.config.cleanup_policy = CleanupPolicy.DELETE
    state = mirror.target_dir / ".mirror-url-state"
    state.mkdir()
    important = state / "important"
    important.write_bytes(b"keep")
    mirror.get_remote_files.return_value = [BASE + "a"]
    mirror._check_files_sync.return_value = [(BASE + "a", mirror.target_dir / "a")]
    assert not mirror.sync()
    assert important.read_bytes() == b"keep"
    assert not (mirror.target_dir / "a").exists()


@pytest.mark.parametrize("name", [".mirror-url-state", ".MIRROR-URL-STATE"])
def test_cleanup_protects_reserved_state_case_variants(mirror, name):
    mirror.config.cleanup_policy = CleanupPolicy.DELETE
    state = mirror.target_dir / name
    state.mkdir()
    important = state / "important"
    important.write_bytes(b"keep")
    mirror.clean_obsolete(set())
    assert important.read_bytes() == b"keep"
    with pytest.raises(ValueError, match="reserved"):
        mirror._validate_remote_paths([BASE + name + "/owner.json"])


def test_bandwidth_budget_paces_every_chunk_across_counter_resets(monkeypatch):
    from mirror_url.rate_limiter import BandwidthLimiter

    elapsed = [0.0]
    monkeypatch.setattr("mirror_url.rate_limiter.time.monotonic", lambda: elapsed[0])
    monkeypatch.setattr("mirror_url.rate_limiter.time.time", lambda: 1000 + elapsed[0])
    monkeypatch.setattr(
        "mirror_url.rate_limiter.time.sleep",
        lambda duration: elapsed.__setitem__(0, elapsed[0] + duration),
    )
    limiter = BandwidthLimiter(100)
    for _ in range(4):
        limiter.throttle(100)
    assert elapsed[0] == pytest.approx(4.0)


@pytest.mark.parametrize("status", [200, 206])
def test_speed_probe_is_bounded_and_closes_ignored_ranges(parallel, status):
    reads = []
    closed = []

    class Body(httpx.SyncByteStream):
        def __iter__(self):
            for _ in range(100):
                reads.append(64 * 1024)
                yield b"x" * (64 * 1024)

        def close(self):
            closed.append(True)

    parallel.connection_manager.request.return_value = httpx.Response(
        status, stream=Body(), request=httpx.Request("GET", BASE + "a")
    )
    assert parallel._estimate_network_speed([BASE + "a"]) > 0
    assert sum(reads) == (1024 * 1024 if status == 206 else 0)
    assert closed == [True]
    assert parallel.connection_manager.request.call_args.kwargs["stream"] is True
