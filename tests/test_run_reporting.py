"""User-visible outcomes, per-run accounting and phase timing."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, local

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.enums import ScanMode
from mirror_url.metrics import MetricsCollector
from mirror_url.run_report import RunReport
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote
from test_scanner_header_capture import URL, _make_scanner
from test_transfer_backends import job
from test_transfer_backends import origin as origin


def test_fallback_counts_the_final_decision_once_and_only_published_downloads():
    report = RunReport()
    report.record_check("changed", "uncertain", True)
    report.record_check("changed", "changed", True)
    report.record_check("missing", "missing")
    report.record_check("failed", "uncertain", True)
    report.record_check("kept", "unchecked")
    report.record_publication("changed")
    report.record_publication("changed")
    report.record_publication("missing")
    result = report.snapshot(skipped=2)
    assert result["changed_files_downloaded"] == 1
    assert result["missing_files_downloaded"] == 1
    assert result["uncertain_freshness_downloads"] == 0
    assert result["metadata_checks_unresolved"] == 1
    assert result["selected_existing_files_checked"] == 2
    assert result["freshness_checks_skipped"] == result["other_files_skipped"] == 1


def test_phase_wall_time_is_the_download_denominator_and_completion_is_frozen(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start({}, 0)
    with report.phase("discovery"):
        now[0] += 16
    with report.phase("downloads"):
        report.record_check("a", "missing")
        report.record_check("b", "missing")
        report.record_publication("a")
        report.record_publication("b")
        now[0] += 2  # Two overlapping transfers, one wall-clock interval.
    report.finish("success")
    now[0] += 500
    result = report.snapshot(downloaded_bytes=10_000_000)
    assert result["elapsed_seconds"] == 18
    assert result["download_throughput"] == 5_000_000
    assert result["phase_seconds"] == {"discovery": 16, "downloads": 2}
    report.start({}, 0)
    assert report.snapshot()["missing_files_downloaded"] == 0
    assert report.snapshot()["download_throughput"] is None


def test_failed_coordinator_phase_is_closed_and_reported(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start({}, 0)
    report.begin_phase("downloads")
    now[0] = 13
    report.finish("failed", "transfer error")
    now[0] = 100
    assert report.snapshot()["phase_seconds"] == {"downloads": 3}
    assert report.snapshot()["status"] == "failed"
    assert report.snapshot()["elapsed_seconds"] == 3
    report.finish("success")
    assert report.snapshot()["status"] == "failed"


@pytest.mark.integration
@pytest.mark.parametrize("status", [404, 503])
def test_http_skip_or_failed_transfer_never_receives_download_credit(remote, config, status):
    remote.files = {"file": b"unavailable"}
    remote.failures["file"] = status
    config.use_shared_log = True
    config.sequential_downloads = True
    with MirrorURL(config) as mirror:
        assert mirror.sync() is (status == 404)
        run = mirror.metrics.get_summary()["run"]
        assert run["missing_files_downloaded"] == 0
        assert run["download_throughput"] is None
        assert run["other_files_skipped"] == (1 if status == 404 else 0)
        assert run["status"] == ("success" if status == 404 else "failed")


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_repeat_run_reports_selected_current_and_unchecked_files_separately(
    remote, config, mode, caplog
):
    remote.files = {"selected": b"original", "keep": b"original"}
    config.cache_html = False
    config.use_shared_log = True
    config.missing_files = True
    config.check_files = ["selected", "not-discovered"]
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    with MirrorURL(config) as mirror:
        logging.getLogger().addHandler(caplog.handler)
        assert mirror.sync(), mirror.metrics.get_summary()
        first = mirror.metrics.get_summary()["run"]
        assert first["missing_files_downloaded"] == 2
        assert first["selected_existing_files_checked"] == 0
        caplog.clear()
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        current = mirror.metrics.get_summary()["run"]
        assert current["status"] == "success"
        assert current["selected_paths_configured"] == 2
        assert current["selected_paths_found"] == 1
        assert current["selected_existing_files_checked"] == 1
        assert current["existing_files_current"] == 1
        assert current["freshness_checks_skipped"] == 1
        assert current["missing_files_downloaded"] == 0
        assert current["download_throughput"] is None
        assert "HTTP 304 Not Modified" in caplog.text
        assert "Selected paths not discovered: 1" in caplog.text
        assert caplog.text.count("SUMMARY:") == 1
        assert "Download throughput: n/a" in caplog.text
        assert "Total duration:" in caplog.text
        assert "Download speed:" not in caplog.text


@pytest.mark.integration
def test_changed_file_and_missing_file_have_separate_publication_counts(remote, config, caplog):
    remote.files = {"selected": b"original", "keep": b"original"}
    config.cache_html = False
    config.use_shared_log = True
    config.missing_files = True
    config.check_files = ["selected"]
    config.sequential_downloads = True
    with MirrorURL(config) as mirror:
        logging.getLogger().addHandler(caplog.handler)
        assert mirror.sync(), mirror.metrics.get_summary()
        remote.files["selected"] = b"modified"  # Same size, changed ETag.
        remote.files["new"] = b"new payload"
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        result = mirror.metrics.get_summary()["run"]
        assert result["missing_files_downloaded"] == result["changed_files_downloaded"] == 1
        assert result["uncertain_freshness_downloads"] == 0
        assert "Updating: selected — ETag changed" in caplog.text


@pytest.mark.integration
def test_aiohttp_publication_and_repeat_run_reporting(tmp_path, origin, monkeypatch, caplog):
    pytest.importorskip("aiohttp")
    values = job(tmp_path, origin, "aiohttp").model_dump()
    values.update(
        mode="mirror",
        url_list=None,
        overwrite=False,
        missing_files=True,
        verify_content=False,
        check_files=["a"],
        async_metadata=False,
        connection_pool_prewarm=False,
        use_shared_log=True,
    )
    config = MirrorConfig(**values)
    origin.routes["/root/a"] = (200, {"ETag": '"same"'}, b"mirror bytes")
    origin.routes["/root/b"] = (200, {"ETag": '"same"'}, b"kept bytes")
    with MirrorURL(config) as mirror:
        monkeypatch.setattr(
            mirror, "get_remote_files", lambda: [origin.base + "a", origin.base + "b"]
        )
        assert mirror.sync(), mirror.metrics.get_summary()
        assert mirror.metrics.get_summary()["run"]["missing_files_downloaded"] == 2
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        result = mirror.metrics.get_summary()["run"]
        assert result["existing_files_current"] == result["freshness_checks_skipped"] == 1
        assert result["missing_files_downloaded"] == 0
        assert result["download_throughput"] is None
        assert "Current: a — ETag unchanged" in caplog.text


@pytest.mark.integration
def test_failed_head_is_uncertain_and_successful_get_is_not_reported_as_change(
    remote, config, monkeypatch, caplog
):
    remote.files = {"selected": b"original"}
    config.cache_html = False
    config.use_shared_log = True
    config.sequential_downloads = True
    with MirrorURL(config) as mirror:
        logging.getLogger().addHandler(caplog.handler)
        assert mirror.sync(), mirror.metrics.get_summary()
        request = mirror.connection_manager.request

        def head_unavailable(url, **kwargs):
            if kwargs.get("method") == "HEAD" and url.endswith("/selected"):
                return httpx.Response(503)
            return request(url, **kwargs)

        monkeypatch.setattr(mirror.connection_manager, "request", head_unavailable)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        result = mirror.metrics.get_summary()["run"]
        assert result["changed_files_downloaded"] == 0
        assert result["uncertain_freshness_downloads"] == 1
        assert "Revalidating: selected — metadata returned HTTP 503" in caplog.text


def test_cache_sources_are_separate_and_legacy_metrics_remain_available():
    metrics = MetricsCollector()
    metrics.increment("parsed_listing_reuses", 9)
    metrics.run_report.start(metrics.metrics, 0)
    metrics.increment("parsed_listing_reuses", 2)
    metrics.increment("html_cache_hits", 1)
    metrics.increment("html_cache_misses", 3)
    metrics.increment("html_cache_bypass_refresh", 4)
    metrics.run_report.finish("success")
    text = metrics.report()
    assert "In-memory parsed listing reuses: 2" in text
    assert "HTML listing cache: 1 hits, 3 misses" in text
    assert "Listing cache bypasses (--refresh-cache): 4" in text
    assert "Cache hits:" not in text
    assert metrics.get_summary()["parsed_listing_reuses"] == 11


def test_cache_gauges_and_feature_flags_are_not_subtracted_between_runs():
    metrics = MetricsCollector()
    metrics.metrics.update(
        cache_signatures=42, adaptive_async_enabled=True, adaptive_current_concurrency=8
    )
    metrics.run_report.start(metrics.metrics, 0)
    metrics.run_report.finish("success")
    report = metrics.report()
    assert "Cache signatures: 42" in report
    assert "Adaptive async: concurrency=8" in report


def test_concurrent_listing_durations_remain_independent_and_retain_json_samples(monkeypatch):
    scanner, client = _make_scanner({})
    request = client.request
    barrier = Barrier(2)
    clocks = local()

    def clock():
        if not hasattr(clocks, "times"):
            clocks.times = iter([0.0, 1.0, 4.0, 5.0])
        return next(clocks.times)

    monkeypatch.setattr("mirror_url.scanner.time.perf_counter", clock)

    def overlapping_request(*args, **kwargs):
        barrier.wait(timeout=5)
        return request(*args, **kwargs)

    client.request = overlapping_request
    with ThreadPoolExecutor(max_workers=2) as workers:
        results = [workers.submit(scanner._perform_scan, URL + str(index)) for index in range(2)]
        assert [future.result(timeout=5) for future in results] == [([], []), ([], [])]
    summary = scanner.metrics.get_summary()
    assert len(summary["parse_times"]) == 2
    assert sum(summary["parse_times"]) == summary["parse_time_seconds"] == 10
    assert summary["listing_fetches"] == 2
    assert summary["listing_get_seconds"] == 6


@pytest.mark.parametrize("mode,workers", [("sequential", 1), ("parallel", 4)])
def test_discovery_throughput_uses_elapsed_time_and_preserves_summed_work(
    monkeypatch, mode, workers
):
    now = [100.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    metrics = MetricsCollector()
    # Earlier runs must not contribute to either this run's numerator or work.
    metrics.increment("directories_processed", 50)
    metrics.add_parse_time(80.0)
    metrics.run_report.start(metrics.metrics, 0)
    with metrics.run_report.phase("discovery"):
        metrics.run_report.begin_discovery(mode, workers)
        for _ in range(3):
            metrics.run_report.record_directory_examined()
            metrics.increment("directories_processed")
            metrics.add_parse_time(4.0)
        now[0] += 6.0
    metrics.run_report.finish("success")
    now[0] += 1000  # Rendering after completion must not change throughput.
    report = metrics.report()
    assert "Directories examined: 3" in report
    assert "Discovery elapsed time: 6.000s" in report
    assert "Discovery throughput: 0.5 dirs/s" in report
    assert "Listing fetch/parse work (sum): 12.000s" in report
    assert "Listing processing rate:" not in report
    summary = metrics.get_summary()
    assert summary["parse_time_seconds"] == 92.0
    assert summary["directories_processed"] == 53
    assert summary["run"]["discovery_throughput"] == 0.5
    assert summary["run"]["directories_examined"] == 3
    assert summary["run"]["discovery_workers"] == workers
    metrics.run_report.start(metrics.metrics, 0)
    reset = metrics.get_summary()["run"]
    assert reset["discovery_mode"] is None
    assert reset["discovery_workers"] == reset["directories_examined"] == 0
    assert reset["discovery_throughput"] is None


def test_failed_discovery_reports_partial_work_without_success_credit(monkeypatch):
    now = [0.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    metrics = MetricsCollector()
    metrics.run_report.start(metrics.metrics, 0)
    with pytest.raises(RuntimeError), metrics.run_report.phase("discovery"):
        metrics.run_report.begin_discovery("parallel", 4)
        metrics.run_report.record_directory_examined()
        now[0] = 2.0
        raise RuntimeError("listing failed")
    metrics.run_report.finish("failed", "listing failed")
    report = metrics.report()
    assert "Result: FAILED" in report
    assert "Directories examined: 1" in report
    assert "Discovery elapsed time: 2.000s" in report
    assert "Discovery throughput: 0.5 dirs/s" in report


@pytest.mark.parametrize(
    "started,seconds,expected", [(False, 2.0, None), (True, 0.0, None), (True, 2.0, 0.0)]
)
def test_unavailable_or_empty_discovery_never_invents_throughput(
    monkeypatch, started, seconds, expected
):
    now = [0.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    metrics = MetricsCollector()
    metrics.run_report.start(metrics.metrics, 0)
    with metrics.run_report.phase("discovery"):
        if started:
            metrics.run_report.begin_discovery("sequential", 1)
        now[0] = seconds
    metrics.run_report.finish("success")
    assert metrics.get_summary()["run"]["discovery_throughput"] == expected
    rendered = "n/a" if expected is None else "0.0 dirs/s"
    assert f"Discovery throughput: {rendered}" in metrics.report()


@pytest.mark.integration
@pytest.mark.parametrize("mode,workers", [("sequential", 1), ("parallel", 4)])
def test_repeat_sync_discovery_text_and_export_match_real_local_http_work(
    remote, config, tmp_path, mode, workers
):
    remote.files = {"a/file": b"a", "b/file": b"b", "file": b"root"}
    config.scan_mode = ScanMode(mode)
    config.cache_html = False
    config.use_shared_log = True
    config.sequential_downloads = True
    with MirrorURL(config) as mirror:
        for _ in range(2):
            assert mirror.sync(), mirror.metrics.get_summary()
            run = mirror.metrics.get_summary()["run"]
            assert run["discovery_mode"] == mode
            assert run["discovery_workers"] == workers
            assert run["directories_examined"] == 3
            assert run["discovery_throughput"] == 3 / run["phase_seconds"]["discovery"]
            text = mirror.metrics.report()
            assert f"Discovery mode: {mode} ({workers} worker" in text
            assert "Directories examined: 3" in text
            assert f"Discovery throughput: {run['discovery_throughput']:.1f} dirs/s" in text
            assert "Listing processing rate:" not in text
            exported = tmp_path / "metrics.json"
            assert mirror.metrics.export_json(exported, config)
            assert json.loads(exported.read_text())["metrics"]["run"] == run


@pytest.mark.integration
def test_dry_run_after_publication_does_not_report_previous_downloads(remote, config, caplog):
    remote.files = {"selected": b"original", "keep": b"original"}
    config.cache_html = False
    config.use_shared_log = True
    config.sequential_downloads = True
    config.missing_files = True
    config.check_files = ["selected"]
    with MirrorURL(config) as mirror:
        assert mirror.sync(), mirror.metrics.get_summary()
        assert mirror.metrics.get_summary()["bytes_downloaded"] > 0
        config.dry_run = True
        caplog.clear()
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        summary = mirror.metrics.get_summary()
        assert summary["run"]["status"] == "dry run"
        assert summary["bytes_downloaded"] == summary["files_downloaded"] == 0
        assert summary["files_failed"] == 0
        assert summary["run"]["existing_files_current"] == 1
        assert summary["run"]["freshness_checks_skipped"] == 1
        assert "Downloaded bytes: 0.00 B" in caplog.text
        assert caplog.text.count("SUMMARY:") == 1


@pytest.mark.integration
def test_failure_during_finalization_does_not_emit_a_success_summary(
    remote, config, monkeypatch, caplog
):
    remote.files = {"file": b"original"}
    config.use_shared_log = True
    config.sequential_downloads = True
    with MirrorURL(config) as mirror:

        def fail_finalization():
            raise RuntimeError("finalization failed")

        monkeypatch.setattr(mirror.performance_monitor, "get_summary", fail_finalization)
        with caplog.at_level(logging.INFO):
            assert not mirror.sync()
        assert mirror.metrics.get_summary()["run"]["status"] == "failed"
        assert "Result: FAILED" in caplog.text
        assert "Result: SUCCESS" not in caplog.text
        assert caplog.text.count("SUMMARY:") == 1
