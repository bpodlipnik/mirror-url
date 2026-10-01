"""Resource bounds, shutdown, and download method selection contracts."""

from __future__ import annotations

import sys
import time
from collections import OrderedDict
from threading import Semaphore
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from mirror_url.download import ParallelDownloadManager
from mirror_url.enums import DownloadMethod
from mirror_url.models import ParallelFileDownload
from test_download_failure_contract import manager as manager
from test_immediate_audit_fixes import BASE
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel


def test_sequential_initialization_disables_parallel_and_auto_selection(manager):
    manager.config.sequential_downloads = True
    child = ParallelDownloadManager(
        manager.config, manager.metrics, manager.connection_manager, None
    )
    try:
        assert not child.enabled and not child.use_streaming and not child.auto_mode
        assert not child.should_use_parallel(1024**3)
    finally:
        child.shutdown()


@pytest.mark.parametrize("cleanup_error", [False, True])
def test_shared_executor_is_borrowed_and_atexit_captures_only_directory(
    manager, tmp_path, monkeypatch, cleanup_error
):
    manager.config.streaming_parallel = False
    manager.config.parallel_downloads = True
    manager.config.use_shared_thread_pool = True
    manager.config.chunk_assembly_dir = None
    manager.config.circuit_breaker_enabled = False
    shared = Mock()
    callbacks = []
    monkeypatch.setattr("mirror_url.download.atexit.register", callbacks.append)
    monkeypatch.setattr("mirror_url.download.tempfile.gettempdir", lambda: str(tmp_path))
    child = ParallelDownloadManager(
        manager.config,
        manager.metrics,
        manager.connection_manager,
        None,
        concurrency_manager=SimpleNamespace(shared_pool=shared),
    )
    try:
        assert child.executor is shared and not child.own_executor
        assert child.enabled and not child.use_streaming and not child.auto_mode
        assert child.circuit_breaker is None
        assert callbacks and all(
            cell.cell_contents is not child for cell in callbacks[0].__closure__
        )
        if cleanup_error:
            monkeypatch.setattr(
                "mirror_url.download.shutil.rmtree", Mock(side_effect=OSError("busy"))
            )
        callbacks[0]()
        assert child.assembly_dir.exists() is cleanup_error
    finally:
        child.shutdown()
    shared.shutdown.assert_not_called()


@pytest.mark.parametrize("error", [False, True])
def test_periodic_cleanup_recovers_errors_and_exits_on_shutdown(manager, error):
    manager._shutdown = False
    manager._shutdown_event = Mock()
    manager._shutdown_event.wait.return_value = False

    def cleanup():
        manager._shutdown = True
        if error:
            raise OSError("maintenance failed")

    manager._cleanup_idle_resources = Mock(side_effect=cleanup)
    manager._periodic_cleanup()
    manager._cleanup_idle_resources.assert_called_once()
    manager._shutdown_event.wait.assert_called_once_with(timeout=30)


def test_idle_cleanup_removes_terminal_entries_and_trims_oldest_first(manager):
    now = time.time()
    paths = [manager.assembly_dir / str(i) for i in range(7)]
    manager.max_active_downloads = 4
    manager.active_downloads = OrderedDict(
        (path, SimpleNamespace(status="completed", start_time=now - 50 + i))
        for i, path in enumerate(paths[:5])
    )
    manager.active_downloads[paths[5]] = SimpleNamespace(status="failed", start_time=now - 4000)
    manager.active_downloads[paths[6]] = SimpleNamespace(
        status="downloading", start_time=now - 5000, final_path=paths[6]
    )
    manager._cleanup_idle_resources()
    assert list(manager.active_downloads) == paths[3:5] + paths[6:]


@pytest.mark.parametrize("stale", [False, True])
def test_idle_cleanup_prunes_only_expired_ip_entries_and_keeps_active_downloads(manager, stale):
    now = time.time()
    manager.max_active_downloads = 1
    manager.active_downloads = OrderedDict(
        (
            manager.assembly_dir / str(i),
            SimpleNamespace(
                status="downloading", start_time=1, final_path=manager.assembly_dir / str(i)
            ),
        )
        for i in range(3)
    )
    manager._last_ip_semaphore_cleanup = 0
    manager._ip_semaphores = {str(i): Semaphore(1) for i in range(65)}
    manager._ip_semaphores_last_used = {str(i): now for i in range(65)}
    if stale:
        manager._ip_semaphores_last_used["0"] = 1
    manager._cleanup_idle_resources()
    assert len(manager.active_downloads) == 3
    assert ("0" in manager._ip_semaphores) is not stale
    assert ("0" in manager._ip_semaphores_last_used) is not stale
    assert len(manager._ip_semaphores) == 65 - stale
    assert manager._last_ip_semaphore_cleanup >= now


def test_scheduled_cleanup_does_not_prune_small_ip_table(manager):
    manager._last_ip_semaphore_cleanup = 0
    permit = Semaphore(1)
    manager._ip_semaphores = {"old": permit}
    manager._ip_semaphores_last_used = {"old": 1}
    manager._cleanup_idle_resources()
    assert manager._ip_semaphores["old"] is permit


@pytest.mark.parametrize("trusted", [False, True])
@pytest.mark.parametrize("stale", [False, True])
def test_per_ip_permits_refresh_heartbeat_prune_idle_entries_and_enforce_limit(
    manager, trusted, stale
):
    manager.config.trusted_server = trusted
    now = time.time()
    manager._ip_semaphores = {str(i): Semaphore(1) for i in range(64)}
    manager._ip_semaphores_last_used = {str(i): now for i in range(64)}
    if stale:
        manager._ip_semaphores_last_used["0"] = 1
    permit = manager._get_ip_semaphore("current")
    assert manager._get_ip_semaphore("current") is permit
    assert ("0" in manager._ip_semaphores) is not stale
    assert manager._ip_semaphores_last_used["current"] >= now
    limit = manager.max_parallel_chunks if trusted else 4
    for _ in range(limit):
        assert permit.acquire(blocking=False)
    assert not permit.acquire(blocking=False)
    for _ in range(limit):
        permit.release()


@pytest.mark.parametrize("fault", ["executor", "cleanup", "stuck-thread", "normal"])
def test_shutdown_clears_tracking_despite_resource_failure(manager, monkeypatch, fault):
    final = manager.assembly_dir / "a"
    download = ParallelFileDownload(BASE + "a", final, 6, status="downloading")
    completed = ParallelFileDownload(BASE + "b", final.with_name("b"), 6, status="completed")
    manager.active_downloads[final] = download
    manager.active_downloads[completed.final_path] = completed
    manager._get_ip_semaphore("192.0.2.1")
    # Stop the actual executor before simulating a broken shutdown API.
    manager.executor.shutdown(wait=True)
    shutdown = Mock(side_effect=OSError("executor") if fault == "executor" else None)
    monkeypatch.setattr("mirror_url.download.bounded_executor_shutdown", shutdown)
    manager.cleanup_stale_chunks = Mock(
        side_effect=OSError("cleanup") if fault == "cleanup" else None, return_value=1
    )
    if fault == "stuck-thread":
        manager._cleanup_thread = Mock()
        manager._cleanup_thread.is_alive.return_value = True
    manager.shutdown(timeout=0.01)
    assert download.status == "cancelled" and completed.status == "completed"
    assert not manager.active_downloads
    assert not manager._ip_semaphores and not manager._ip_semaphores_last_used
    assert manager._shutdown and manager._shutdown_event.is_set()
    shutdown.assert_called_once_with(manager.executor, 0.01, "Download executor")
    if fault == "stuck-thread":
        manager._cleanup_thread.join.assert_called_once_with(timeout=5.0)


def test_shutdown_tolerates_missing_optional_resources_and_repeated_calls(manager):
    manager._shutdown_event = None
    manager._cleanup_thread = None
    manager.own_executor = False
    manager.shutdown()
    manager.shutdown()
    assert manager._shutdown and not manager.active_downloads


@pytest.mark.parametrize("error", [None, OSError("already gone"), RuntimeError("unexpected")])
def test_destructor_suppresses_cleanup_errors_and_is_idempotent(error):
    manager = ParallelDownloadManager.__new__(ParallelDownloadManager)
    manager.__del__()  # A failed constructor may not have initialized anything.
    manager._shutdown = False
    manager.shutdown = Mock(side_effect=error)
    manager.__del__()
    manager.shutdown.assert_called_once()
    manager._shutdown = True
    manager.__del__()
    manager.shutdown.assert_called_once()


def test_statistics_work_without_rate_limiter_counters(manager):
    manager.rate_limiter = SimpleNamespace()
    assert manager.get_stats()["rate_limiter"] == {}


@pytest.mark.parametrize(
    "flag, method",
    [
        ("parallel_downloads", DownloadMethod.TRADITIONAL_PARALLEL),
        ("streaming_parallel", DownloadMethod.STREAMING_PARALLEL),
        ("sequential_downloads", DownloadMethod.SEQUENTIAL),
    ],
)
def test_explicit_download_method_skips_runtime_probes(manager, flag, method):
    manager.config.parallel_downloads = False
    manager.config.streaming_parallel = False
    manager.config.sequential_downloads = False
    setattr(manager.config, flag, True)
    manager._detect_ssd = Mock(side_effect=AssertionError("unnecessary probe"))
    assert manager.auto_select_method([1], 1, []) == method


@pytest.mark.parametrize(
    "sizes, ssd, speed, ranges, method",
    [
        ([1], True, 200, True, DownloadMethod.SEQUENTIAL),
        ([1, 1, 1], True, 200, True, DownloadMethod.TRADITIONAL_PARALLEL),
        ([200] * 4, True, 200, True, DownloadMethod.STREAMING_PARALLEL),
        ([100] * 3, False, 200, True, DownloadMethod.TRADITIONAL_PARALLEL),
        ([100] * 3, True, 200, True, DownloadMethod.SEQUENTIAL),
        ([200] * 4, True, 100, True, DownloadMethod.SEQUENTIAL),
        ([200] * 4, True, 200, False, DownloadMethod.SEQUENTIAL),
        ([1, 1], True, 200, True, DownloadMethod.SEQUENTIAL),
        ([], True, 100, False, DownloadMethod.SEQUENTIAL),
    ],
)
def test_auto_selection_decision_matrix(manager, sizes, ssd, speed, ranges, method):
    manager.config.streaming_parallel = False
    manager.config.parallel_downloads = False
    manager.config.sequential_downloads = False
    manager._detect_ssd = Mock(return_value=ssd)
    manager._estimate_network_speed = Mock(return_value=speed)
    manager._check_range_support = Mock(return_value=ranges)
    urls = [BASE + str(i) for i in range(7)] if sizes else []
    assert (
        manager.auto_select_method([size * 1024**2 for size in sizes], len(sizes), urls) == method
    )
    if len(sizes) != 1:
        manager._estimate_network_speed.assert_called_once_with(urls[:5])
        manager._check_range_support.assert_called_once_with(urls[0] if urls else None)


@pytest.mark.parametrize("override, result", [("SSD", True), ("hdd", False)])
def test_disk_type_override_does_not_probe(manager, override, result):
    manager.config.force_disk_type = override
    assert manager._detect_ssd() is result


@pytest.mark.parametrize("context", [None, SimpleNamespace(target_dir=None)])
def test_disk_probe_without_destination_assumes_ssd(manager, monkeypatch, context):
    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace())
    manager.mirror = context
    assert manager._detect_ssd()


@pytest.mark.parametrize("opts", ["rota=0", "nonrot"])
def test_disk_probe_respects_nonrotating_partition(manager, monkeypatch, opts):
    partition = SimpleNamespace(mountpoint=str(manager.mirror.target_dir), opts=opts)
    monkeypatch.setitem(sys.modules, "psutil", SimpleNamespace(disk_partitions=lambda: [partition]))
    assert manager._detect_ssd()


@pytest.mark.parametrize("partition", ["other", "hdd", "no-options", "none"])
@pytest.mark.parametrize("duration", [0.1, 1.0])
def test_disk_probe_random_write_fallback(manager, monkeypatch, partition, duration):
    part = SimpleNamespace(mountpoint=str(manager.mirror.target_dir))
    if partition == "other":
        part.mountpoint = "/unrelated"
    if partition != "no-options":
        part.opts = "rota=1"
    monkeypatch.setitem(
        sys.modules,
        "psutil",
        SimpleNamespace(disk_partitions=lambda: [] if partition == "none" else [part]),
    )
    monkeypatch.setattr(
        "mirror_url.download.time", SimpleNamespace(time=Mock(side_effect=[0, duration]))
    )
    monkeypatch.setattr("mirror_url.download.random.randint", lambda *_: 0)
    before = set(manager.mirror.target_dir.iterdir())
    assert manager._detect_ssd() is (duration < 0.5)
    assert set(manager.mirror.target_dir.iterdir()) == before


@pytest.mark.parametrize("fault", ["import", "partitions", "temporary-file"])
def test_disk_detection_failure_returns_safe_default(manager, monkeypatch, fault):
    if fault == "import":
        monkeypatch.setitem(sys.modules, "psutil", None)
    else:
        monkeypatch.setitem(
            sys.modules,
            "psutil",
            SimpleNamespace(
                disk_partitions=Mock(
                    side_effect=OSError("disk") if fault == "partitions" else None, return_value=[]
                )
            ),
        )
        if fault == "temporary-file":
            monkeypatch.setattr(
                "mirror_url.download.tempfile.TemporaryFile", Mock(side_effect=OSError("denied"))
            )
    assert manager._detect_ssd()


@pytest.mark.parametrize(
    "urls, override, expected", [([], None, 100), ([], 42, 42), ([BASE], 42, 42)]
)
def test_network_speed_defaults_and_override(manager, urls, override, expected):
    manager.config.manual_network_speed_mbps = override
    assert manager._estimate_network_speed(urls) == expected
    manager.connection_manager.request.assert_not_called()


@pytest.mark.parametrize("fault", ["short", "empty", "zero-time", "status", "request", "read"])
def test_network_probe_closes_response_and_falls_back_on_failure(manager, monkeypatch, fault):
    result = httpx.Response(206 if fault != "status" else 200, stream=httpx.ByteStream(b"probe"))
    assert not result.is_closed
    result.iter_raw = Mock(return_value=iter([b"a" * 1024]))
    if fault == "empty":
        result.iter_raw.return_value = iter([])
    elif fault == "read":
        result.iter_raw.side_effect = httpx.ReadError("probe interrupted")
    manager.connection_manager.request.return_value = result
    if fault == "request":
        manager.connection_manager.request.side_effect = httpx.ConnectError("unavailable")
    clock = Mock(side_effect=[0, 0 if fault == "zero-time" else 1])
    monkeypatch.setattr("mirror_url.download.time", SimpleNamespace(time=clock))
    assert manager._estimate_network_speed([BASE]) == (
        0.008192 if fault == "short" else 0 if fault == "empty" else 100
    )
    if fault != "request":
        assert result.is_closed
    manager.connection_manager.request.assert_called_once_with(
        BASE,
        method="GET",
        headers={"Range": "bytes=0-1048575", "Accept-Encoding": "identity"},
        timeout=10,
        stream=True,
    )


@pytest.mark.parametrize("url, result", [(None, False), (BASE, True), (BASE, False)])
def test_range_support_probe(manager, url, result):
    manager.connection_manager.request.return_value = httpx.Response(
        200, headers={"Accept-Ranges": "BYTES" if result else "none"}
    )
    assert manager._check_range_support(url) is result
    assert manager.connection_manager.request.call_count == bool(url)


def test_range_support_probe_failure(manager):
    manager.connection_manager.request.side_effect = httpx.ConnectError("unavailable")
    assert not manager._check_range_support(BASE)
