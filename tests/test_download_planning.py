"""Small-file planning avoids probes and measures the representation it downloads."""

from __future__ import annotations

import gzip
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.enums import DownloadMethod
from test_download_failure_contract import manager as manager
from test_http_mirror_workflows import remote as remote
from test_immediate_audit_fixes import BASE
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_transfer_batch_logging import scope as scope


@pytest.mark.parametrize("count", [11, 72])
def test_lasco_sized_batches_never_probe_disk_speed_or_range(manager, count):
    manager.config.parallel_downloads = False
    manager.config.streaming_parallel = False
    manager.config.sequential_downloads = False
    for name in ("_detect_ssd", "_estimate_network_speed", "_check_range_support"):
        setattr(manager, name, Mock(side_effect=AssertionError("unnecessary small-file probe")))
    assert manager.auto_select_method([2 * 1024**2] * count, count, [BASE + "a.fts"]) == (
        DownloadMethod.TRADITIONAL_PARALLEL
    )
    assert manager.metrics.run_report.snapshot()["transfer_stage_operations"] == {}


@pytest.mark.parametrize(
    "status,headers,expected",
    [
        (200, {"Content-Length": "605"}, 605),
        (200, {"Content-Length": "0", "Content-Encoding": "IDENTITY"}, 0),
        (200, {"Content-Length": "146", "Content-Encoding": "gzip"}, None),
        (200, {}, None),
        (200, {"Content-Length": "invalid"}, None),
        (200, {"Content-Length": "-1"}, None),
        (404, {"Content-Length": "605"}, None),
    ],
)
def test_size_probes_measure_one_identity_head_and_close_it(
    mirror, monkeypatch, status, headers, expected
):
    now = [10.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    response = httpx.Response(status, headers=headers)
    response.close = Mock(wraps=response.close)

    def request(*args, **kwargs):
        now[0] += 2
        return response

    mirror.connection_manager.request.side_effect = request
    assert mirror._get_file_size(BASE + "img_hdr.txt") == expected
    mirror.connection_manager.request.assert_called_once_with(
        BASE + "img_hdr.txt", method="HEAD", headers={"Accept-Encoding": "identity"}
    )
    response.close.assert_called_once()
    run = mirror.metrics.run_report.snapshot()
    assert run["transfer_stage_seconds"] == {"range_metadata": 2.0}
    assert run["transfer_stage_operations"] == {"range_metadata": 1}


def test_failed_size_probe_retains_timing_without_a_known_size(mirror, monkeypatch):
    now = [0.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])

    def fail(*args, **kwargs):
        now[0] = 3.0
        raise httpx.ConnectError("owned fixture failure")

    mirror.connection_manager.request.side_effect = fail
    assert mirror._get_file_size(BASE + "a.fts") is None
    assert mirror.metrics.run_report.snapshot()["transfer_stage_seconds"] == {"range_metadata": 3.0}


def test_lazy_size_fallback_does_not_count_the_same_probe_twice(mirror, manager):
    mirror.parallel_manager = manager
    manager.enabled = True
    manager.min_chunk_size = 10 * 1024**2
    mirror.connection_manager.request.return_value = httpx.Response(
        200, headers={"Content-Length": "605"}
    )
    mirror._download_file_single = Mock(return_value=True)
    assert mirror.download_file_with_resume(BASE + "a.fts", mirror.target_dir / "a.fts")
    assert mirror.metrics.run_report.snapshot()["transfer_stage_operations"] == {
        "range_metadata": 1
    }
    mirror._download_file_single.assert_called_once()


def test_compressible_header_plans_the_uncompressed_get_length(mirror):
    payload = b"LASCO header\n" * 50
    encoded = gzip.compress(payload)
    seen = []

    def serve(request):
        seen.append((request.method, request.headers["Accept-Encoding"]))
        body = payload if request.headers["Accept-Encoding"] == "identity" else encoded
        headers = {"Content-Length": str(len(body))}
        if body is encoded:
            headers["Content-Encoding"] = "gzip"
        return httpx.Response(
            200, headers=headers, content=b"" if request.method == "HEAD" else body
        )

    with httpx.Client(transport=httpx.MockTransport(serve)) as client:
        mirror.connection_manager.request.side_effect = lambda url, *, method, **kwargs: (
            client.request(method, url, **kwargs)
        )
        planned = mirror._get_file_size(BASE + "img_hdr.txt")
        response = client.get(BASE + "img_hdr.txt", headers={"Accept-Encoding": "identity"})
        assert planned == len(response.content) == len(payload)
        assert response.content == payload
    assert seen == [("HEAD", "identity"), ("GET", "identity")]


def test_selection_work_and_capability_head_have_separate_timings(manager, monkeypatch):
    now = [0.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    manager.config.parallel_downloads = False
    manager.config.streaming_parallel = False
    manager.config.sequential_downloads = False

    def disk():
        now[0] += 1
        return True

    def speed(urls):
        now[0] += 2
        return 200

    def request(*args, **kwargs):
        now[0] += 3
        return httpx.Response(200, headers={"Accept-Ranges": "bytes"})

    manager._detect_ssd = disk
    manager._estimate_network_speed = speed
    manager.connection_manager.request.side_effect = request
    assert manager.auto_select_method([200 * 1024**2] * 4, 4, [BASE + "large"]) == (
        DownloadMethod.STREAMING_PARALLEL
    )
    manager.connection_manager.request.assert_called_once_with(
        BASE + "large", method="HEAD", headers={"Accept-Encoding": "identity"}, timeout=10
    )
    run = manager.metrics.run_report.snapshot()
    assert run["transfer_stage_seconds"] == {"method_selection": 3.0, "range_metadata": 3.0}
    assert run["transfer_stage_operations"] == {"method_selection": 1, "range_metadata": 1}


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("sequential", [False, True])
def test_real_small_file_mirror_reports_planning_heads_and_only_whole_file_gets(
    tmp_path, remote, scope, backend, sequential
):
    remote.files = {f"c2/{i}.fts": bytes([i]) * 1024 for i in range(10)}
    remote.files["c2/img_hdr.txt"] = b"header\n" * 100
    config = MirrorConfig(
        base_url=scope,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        backend=backend,
        sequential_downloads=sequential,
        missing_files=True,
        security_validation=False,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
    )
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        run = mirror.metrics.get_summary()["run"]
        assert run["transfer_stage_operations"]["range_metadata"] == 11
        assert "method_selection" not in run["transfer_stage_seconds"]
        assert "Transfer metadata probes (sum): not performed" not in mirror.metrics.report()
        assert "Automatic selection work (sum): not performed" in mirror.metrics.report()
        assert run["missing_files_downloaded"] == 11
        assert run["sha256_hashes_recorded"] == 0
        for name, payload in remote.files.items():
            assert (config.dest_path / name).read_bytes() == payload
        requests = [
            (method, name, headers)
            for method, name, headers in remote.requests
            if name in remote.files
        ]
        assert sum(method == "HEAD" for method, _, _ in requests) == 11
        assert sum(method == "GET" for method, _, _ in requests) == 11
        assert all(headers["Accept-Encoding"] == "identity" for _, _, headers in requests)
        assert all("Range" not in headers for _, _, headers in requests)
        assert not list(config.dest_path.rglob("*.part"))
        assert not list(config.dest_path.rglob("workspace-*"))
        assert mirror.sync()
        repeated = mirror.metrics.get_summary()["run"]
        assert repeated["freshness_checks_skipped"] == 11
        assert repeated["transfer_stage_seconds"] == {}
