"""Bounded listing concurrency, deterministic scope decisions and owned teardown."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorURL
from mirror_url._core.scan import _directory_work
from mirror_url.enums import ScanMode
from mirror_url.transport import SecureTransport
from safety_helpers import ROOT, SafetyMirror
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote


@pytest.fixture(autouse=True)
def dns(monkeypatch, request):
    if request.node.get_closest_marker("integration") is None:
        monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda _: "8.8.8.8")


@pytest.mark.parametrize(
    "mode,workers,expected",
    [
        ("adaptive", 8, 1),
        ("sequential", 8, 1),
        ("async", 8, 1),
        ("parallel", 1, 1),
        ("parallel", 2, 2),
        ("parallel", 50, 4),
    ],
)
def test_only_explicit_parallel_mode_overlaps_bounded_listing_requests(
    tmp_path, mode, workers, expected
):
    mirror = SafetyMirror(tmp_path, no_cache=False, scan_mode=mode, workers=workers)
    barrier = threading.Barrier(expected)
    lock = threading.Lock()
    active = peak = 0
    calls = []

    def request(url, method="GET", **kwargs):
        nonlocal active, peak
        assert method == "GET"
        calls.append(url)
        if url == ROOT:
            body = "".join(f'<a href="d{i}/">d{i}</a>' for i in range(8))
        else:
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                barrier.wait(timeout=5)
                body = '<a href="file">file</a>'
            finally:
                with lock:
                    active -= 1
        return httpx.Response(200, content=body.encode())

    mirror.connection_manager.request.side_effect = request
    assert mirror.get_remote_files() == [ROOT + f"d{i}/file" for i in range(8)]
    assert peak == expected and active == 0
    assert len(calls) == len(set(calls)) == 9
    summary = mirror.metrics.get_summary()
    assert summary["directory_signatures_from_get"] == 9
    assert summary["directory_signature_head_calls"] == 0
    assert summary["directories_scanned_parallel"] == (9 if mode == "parallel" else 0)
    assert summary["directories_scanned_sequential"] == (0 if mode == "parallel" else 9)
    assert mirror.scanner.scan_count == 9
    assert mirror.scanner.fast_parse_count + mirror.scanner.lxml_parse_count == 9
    assert not mirror.scan_incomplete
    assert not any(t.name.startswith("directory_scan") for t in threading.enumerate())


@pytest.mark.parametrize("depth", [1, 2, 50])
def test_parallel_bfs_keeps_depth_exclusion_duplicate_and_scope_guards(tmp_path, depth):
    mirror = SafetyMirror(tmp_path, scan_mode="parallel", max_depth=depth, exclude_dirs=["omit"])
    calls = []

    def scan(url, **kwargs):
        calls.append(url)
        return [], {
            ROOT: [
                ROOT + "a/",
                ROOT + "%61/",
                ROOT + "omit/",
                "https://elsewhere.test/",
                ROOT + "../escape/",
            ],
            ROOT + "a/": [ROOT + "a/sub/", ROOT],
        }.get(url, [])

    mirror.scanner.scan_directory_sequential = scan
    result = list(mirror._discover_directories_bfs())
    assert result == [ROOT] + ([ROOT + "a/"] if depth else []) + (
        [ROOT + "a/sub/"] if depth > 1 else []
    )
    assert len(calls) == len(set(calls))
    assert not any(
        "elsewhere" in url or "escape" in url or "omit" in url or "%61" in url for url in calls
    )
    expected = (
        []
        if depth == 0
        else [ROOT]
        if depth == 1
        else [ROOT, ROOT + "a/"]
        if depth == 2
        else result
    )
    assert calls == expected


def test_prefetch_cannot_bypass_already_processed_directory(tmp_path):
    scan = Mock(return_value=([], []))
    queue = deque([(ROOT + "old/", 1), (ROOT + "new/", 1), (ROOT + "new/", 1)])
    with ThreadPoolExecutor(max_workers=4) as executor:
        result = list(_directory_work(queue, {"old"}, ROOT, 5, executor, 4, scan))
        assert result[0][2] is None and result[2][2] is None
        assert result[1][2].result() == ([], [])
    scan.assert_called_once_with(ROOT + "new/")


def test_prefetch_checks_scope_before_any_request_is_submitted():
    scan = Mock(return_value=([], []))
    queue = deque(
        [
            ("https://elsewhere.test/", 1),
            (ROOT + "../escape/", 1),
            (ROOT + "safe/", 1),
        ]
    )
    with ThreadPoolExecutor(max_workers=4) as executor:
        result = list(_directory_work(queue, set(), ROOT, 5, executor, 4, scan))
        assert result[0][2] is None and result[1][2] is None
        assert result[2][2].result() == ([], [])
    scan.assert_called_once_with(ROOT + "safe/")


def test_only_one_worker_sized_batch_is_submitted_while_requests_are_blocked(tmp_path, monkeypatch):
    mirror = SafetyMirror(tmp_path, scan_mode="parallel", workers=50)
    four_started = threading.Event()
    coordinator_waiting = threading.Event()
    release = threading.Event()
    lock = threading.Lock()
    submissions = []
    entered = 0

    class ObservedExecutor(ThreadPoolExecutor):
        def submit(self, fn, *args, **kwargs):
            submissions.append(args[0])
            future = super().submit(fn, *args, **kwargs)
            original_result = future.result

            def result(*result_args, **result_kwargs):
                if args[0] != ROOT:
                    coordinator_waiting.set()
                return original_result(*result_args, **result_kwargs)

            future.result = result
            return future

    monkeypatch.setattr("mirror_url._core.scan.ThreadPoolExecutor", ObservedExecutor)

    def scan(url, **kwargs):
        nonlocal entered
        if url == ROOT:
            return [], [ROOT + f"d{i}/" for i in range(8)]
        with lock:
            entered += 1
            if entered == 4:
                four_started.set()
        assert release.wait(timeout=5)
        return [], []

    mirror.scanner.scan_directory_sequential = scan
    result = []
    errors = []

    def discover():
        try:
            result.extend(mirror._discover_directories_bfs())
        except Exception as error:
            errors.append(error)

    coordinator = threading.Thread(target=discover)
    coordinator.start()
    try:
        assert four_started.wait(timeout=3)
        assert coordinator_waiting.wait(timeout=3)
        assert submissions == [ROOT] + [ROOT + f"d{i}/" for i in range(4)]
    finally:
        release.set()
        coordinator.join(timeout=5)
    assert not coordinator.is_alive() and not errors
    assert result == [ROOT] + [ROOT + f"d{i}/" for i in range(8)]
    assert len(submissions) == 9


@pytest.mark.parametrize("mode", ["skip", "treat-as-file", "follow", "detect"])
def test_out_of_order_responses_cannot_reverse_symlink_decisions(tmp_path, mode):
    mirror = SafetyMirror(
        tmp_path, no_cache=False, scan_mode="parallel", handle_symlinks=True, symlink_mode=mode
    )
    second_finished = threading.Event()
    calls = []

    def request(url, method="GET", **kwargs):
        calls.append(url)
        if url == ROOT:
            body = '<a href="a/">a</a><a href="b/">b</a>'
        elif url == ROOT + "a/":
            assert second_finished.wait(timeout=5)
            body = '<a href="keep">keep</a><a href="sub/">sub</a>'
        elif url == ROOT + "b/":
            body = '<a href="keep">keep</a><a href="sub/">sub</a>'
            second_finished.set()
        else:
            body = ""
        return httpx.Response(200, content=body.encode())

    mirror.connection_manager.request.side_effect = request
    result = list(mirror._discover_directories_bfs())
    assert result[:2] == [ROOT, ROOT + "a/"]
    skipped = mode in {"skip", "treat-as-file"}
    assert (ROOT + "b/" not in result) == skipped
    assert (ROOT + "b/sub/" not in calls) == skipped
    assert mirror.cleanup_protected_prefixes == ({ROOT + "b/"} if skipped else set())
    assert mirror.metrics.get_summary()["symlinks_detected"] == 1


def test_closing_discovery_waits_for_owned_running_requests(tmp_path):
    mirror = SafetyMirror(tmp_path, scan_mode="parallel")
    blocked = threading.Event()
    release = threading.Event()
    closed = threading.Event()

    def scan(url, **kwargs):
        if url == ROOT:
            return [], [ROOT + "a/", ROOT + "b/"]
        if url.endswith("b/"):
            blocked.set()
            assert release.wait(timeout=5)
        return [], []

    mirror.scanner.scan_directory_sequential = scan
    discovery = mirror._discover_directories_bfs()
    assert next(discovery) == ROOT
    assert next(discovery) == ROOT + "a/"
    assert blocked.wait(timeout=2)

    def close():
        discovery.close()
        closed.set()

    closer = threading.Thread(target=close)
    closer.start()
    try:
        assert not closed.wait(timeout=0.05)
    finally:
        release.set()
        closer.join(timeout=5)
    assert closed.is_set() and not closer.is_alive()
    assert not any(t.name.startswith("directory_scan") for t in threading.enumerate())


def test_discovery_progress_includes_network_wait_without_false_percentage_or_eta(
    tmp_path, monkeypatch, caplog
):
    mirror = SafetyMirror(tmp_path, no_cache=False)
    clock = 100.0
    monkeypatch.setattr("mirror_url._core.scan.time.perf_counter", lambda: clock)

    def request(url, method="GET", **kwargs):
        nonlocal clock
        assert any("Discovering directories: sequential" in r.getMessage() for r in caplog.records)
        clock += 3
        body = '<a href="a/">a</a><a href="b/">b</a>' if url == ROOT else '<a href="file">file</a>'
        return httpx.Response(200, content=body.encode())

    mirror.connection_manager.request.side_effect = request
    with caplog.at_level(logging.INFO):
        assert mirror.get_remote_files() == [ROOT + "a/file", ROOT + "b/file"]
    messages = [r.getMessage() for r in caplog.records if "Directory discovery" in r.getMessage()]
    assert messages == [
        "Directory discovery: 2 directories examined, 6.000s elapsed",
        "Directory discovery finished: 3 directories examined in 9.000s (0.3 dirs/s)",
    ]
    assert not any("ETA:" in message or "%" in message for message in messages)
    mirror.multi_progress.add_level.assert_not_called()
    mirror.multi_progress.update.assert_not_called()


@pytest.mark.parametrize("url", ["file:///no-host", "https://example.test/"])
def test_directory_pacing_retains_dns_failure_handling(tmp_path, monkeypatch, url):
    mirror = SafetyMirror(tmp_path, scan_mode=ScanMode.PARALLEL)
    monkeypatch.setattr(
        "mirror_url._core.scan.socket.gethostbyname", Mock(side_effect=OSError("DNS failed"))
    )
    mirror._pace_directory_request(url)
    mirror.per_ip_limiter.wait.assert_not_called()


@pytest.mark.integration
@pytest.mark.parametrize("delay", [0.0, 0.015])
def test_real_http_listings_overlap_without_bypassing_request_spacing(
    remote, config, monkeypatch, delay
):
    remote.files = {f"d{i}/file": b"payload" for i in range(8)}
    remote.directory_delay_seconds = 0.06
    config.scan_mode = ScanMode.PARALLEL
    config.requests_per_second = 0
    config.request_delay = delay
    monkeypatch.setattr("mirror_url.connection.random.uniform", lambda *args: 0)
    lock = threading.Lock()
    starts = []
    active = peak = 0

    def tracked_request(transport, request):
        nonlocal active, peak
        with lock:
            starts.append(time.perf_counter())
            active += 1
            peak = max(peak, active)
        try:
            return httpx.HTTPTransport.handle_request(transport, request)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(SecureTransport, "handle_request", tracked_request)
    with MirrorURL(config) as mirror:
        starts.clear()
        peak = 0
        remote.requests.clear()
        assert set(mirror.get_remote_files()) == {remote.url + name for name in remote.files}
        assert len(remote.requests) == 9 and all(
            method == "GET" for method, _, _ in remote.requests
        )
        assert 1 < peak <= 4 and active == 0
        assert len(starts) == 9
        assert all(b - a >= delay - 0.002 for a, b in zip(starts, starts[1:]))
        assert mirror.metrics.get_summary()["directory_signature_head_calls"] == 0
    assert not any(t.name.startswith("directory_scan") for t in threading.enumerate())
