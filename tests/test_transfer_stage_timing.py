"""Shared, per-run worker timings describe real operations without changing policy."""

from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from mirror_url import MirrorConfig, MirrorURL, download_url_list
from mirror_url.metrics import MetricsCollector, format_download_metrics
from mirror_url.run_report import TRANSFER_STAGES, RunReport
from test_http_mirror_workflows import remote as remote
from test_transfer_backends import backend as backend
from test_transfer_batch_logging import scope as scope


def test_overlapping_worker_sums_do_not_replace_elapsed_download_time(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start({}, 0)
    report.begin_phase("downloads")
    first = report.transfer_stage("transfer")
    second = report.transfer_stage("transfer")
    first.__enter__()
    now[0] = 101.0
    second.__enter__()
    now[0] = 104.0
    first.__exit__(None, None, None)
    now[0] = 105.0
    second.__exit__(None, None, None)
    report.record_publication("a")
    report.finish("success")
    run = report.snapshot(downloaded_bytes=50)
    assert run["transfer_stage_seconds"] == {"transfer": 8.0}
    assert run["transfer_stage_operations"] == {"transfer": 2}
    assert run["phase_seconds"] == {"downloads": 5.0}
    assert run["elapsed_seconds"] == 5.0
    assert run["download_throughput"] == 10.0
    metrics = MetricsCollector()
    metrics.run_report = report
    text = metrics.report(run=run)
    assert "Data transfer/write (sum): 8.000s" in text
    assert "worker sums; concurrent work overlaps" in text
    assert "Downloads time: 5.000s" in text
    assert "SHA-256 hashing (sum): not performed" in text
    report.start({}, 0)
    assert report.snapshot()["transfer_stage_seconds"] == {}
    assert report.snapshot()["transfer_stage_operations"] == {}


@pytest.mark.parametrize("error", [OSError, asyncio.CancelledError])
def test_failed_and_cancelled_attempts_are_retained_but_never_receive_publication_credit(
    monkeypatch, error
):
    now = [0.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start({}, 0)
    for _ in range(2):
        with pytest.raises(error), report.transfer_stage("transfer"):
            now[0] += 3.0
            raise error("injected failure")
    report.finish("failed")
    run = report.snapshot(downloaded_bytes=123)
    assert run["transfer_stage_seconds"] == {"transfer": 6.0}
    assert run["transfer_stage_operations"] == {"transfer": 2}
    assert run["download_throughput"] is None
    assert run["sha256_hashes_recorded"] == 0


def test_worker_updates_are_atomic_and_snapshots_are_independent():
    report = RunReport()
    barrier = Barrier(8)

    def worker():
        barrier.wait(timeout=5)
        for _ in range(100):
            with report.transfer_stage("transfer"):
                pass

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: worker(), range(8)))
    snapshot = report.snapshot()
    assert snapshot["transfer_stage_operations"] == {"transfer": 800}
    assert snapshot["transfer_stage_seconds"]["transfer"] > 0
    snapshot["transfer_stage_seconds"]["transfer"] = -1
    assert report.snapshot()["transfer_stage_seconds"]["transfer"] > 0


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("strategy", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize("verify", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_both_modes_backends_and_strategies_report_measured_stages(
    tmp_path, remote, scope, backend, mode, strategy, verify, failed
):
    large = b"large" * (500 * 1024)
    remote.files = {"large.bin": large, "small.bin": b"small"}
    if failed:
        remote.files["bad.bin"] = b"bad"
        remote.failures["bad.bin"] = 500
    config = MirrorConfig(
        mode=mode,
        backend=backend,
        base_url=scope,
        download_files=list(remote.files) if mode == "download" else [],
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        min_chunk_size_mb=1,
        max_chunks_per_file=2,
        sequential_downloads=strategy == "sequential",
        parallel_downloads=strategy == "parallel",
        streaming_parallel=strategy == "streaming",
        verify_content=verify,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
        retry_delay=1,
    )
    metrics = MetricsCollector()
    if mode == "download":
        result = download_url_list(config, metrics=metrics)
        assert result["success"] is not failed
        run = metrics.run_report.snapshot(result["bytes_downloaded"])
        assert result["transfer_stage_seconds"] == run["transfer_stage_seconds"]
        text = format_download_metrics(config, result, config.dest_path, run, metrics=metrics)
        # The legacy result-only formatter retains the measured values too.
        assert (
            f"Data transfer/write (sum): {run['transfer_stage_seconds']['transfer']:.3f}s"
            in format_download_metrics(config, result, config.dest_path)
        )
        receipts = json.loads(
            next(config.dest_path.glob(".mirror-url-state/url-list-*.json")).read_text(
                encoding="utf-8"
            )
        )
        for name, payload in remote.files.items():
            if name == "bad.bin":
                continue
            receipt = receipts["files"][str(config.dest_path / name)]
            assert receipt["sha256"] == (hashlib.sha256(payload).hexdigest() if verify else None)
    else:
        with MirrorURL(config) as mirror:
            assert mirror.sync() is not failed
            run = mirror.metrics.get_summary()["run"]
            text = mirror.metrics.report()
            assert mirror.sync() is not failed
            # Unchanged files on the second sync perform no payload stage work.
            repeated = mirror.metrics.get_summary()["run"]
            assert "flush" not in repeated["transfer_stage_seconds"]
            assert "hash" not in repeated["transfer_stage_seconds"]
            assert "assembly" not in repeated["transfer_stage_seconds"]
            if not failed:
                assert repeated["transfer_stage_seconds"] == {}
    stages = run["transfer_stage_seconds"]
    assert stages["transfer"] > 0 and stages["flush"] > 0
    assert ("hash" in stages) is verify
    assert ("assembly" in stages) is (strategy == "parallel")
    assert ("range_metadata" in stages) is (mode == "mirror" or strategy != "sequential")
    assert all(seconds >= 0 for seconds in stages.values())
    for _, label in TRANSFER_STAGES:
        assert text.count(f"{label} (sum):") == 1
    assert (config.dest_path / "large.bin").read_bytes() == large
    assert (config.dest_path / "small.bin").read_bytes() == b"small"
    assert not (config.dest_path / "bad.bin").exists()
    assert not list(config.dest_path.rglob("*.part"))
    assert not list(config.dest_path.rglob("workspace-*"))
    assert run["sha256_hashes_recorded"] == (2 if verify else 0)
    # Each successful file receives one whole-file GET or exactly two ranges;
    # timing instrumentation does not introduce another payload request.
    successful = [
        request
        for request in remote.requests
        if request[0] == "GET" and request[1] in ("large.bin", "small.bin")
    ]
    assert sum(request[1] == "large.bin" for request in successful) == (
        1 if strategy == "sequential" else 2
    )
    assert sum(request[1] == "small.bin" for request in successful) == 1


@pytest.mark.integration
@pytest.mark.parametrize("strategy", ["sequential", "parallel", "streaming"])
def test_paired_modes_use_identical_roots_backends_and_payloads(
    tmp_path, remote, scope, backend, strategy, record_property
):
    # Match scope, backend, worker limits, payloads and relative destination layout.
    # Record timings without asserting a speed advantage from a short local run.
    payloads = {
        "nested/a.bin": b"a" * (3 * 1024 * 1024),
        "nested/b.bin": b"b" * (3 * 1024 * 1024),
        "nested/small.bin": b"c" * 100_000,
    }
    remote.files = payloads
    comparisons = {}
    for mode in ("mirror", "download"):
        config = MirrorConfig(
            mode=mode,
            backend=backend,
            base_url=scope,
            download_files=list(payloads) if mode == "download" else [],
            dest_path=tmp_path / mode,
            log_path=tmp_path / (mode + "-logs"),
            max_concurrent_downloads=3,
            min_chunk_size_mb=1,
            max_chunks_per_file=2,
            sequential_downloads=strategy == "sequential",
            parallel_downloads=strategy == "parallel",
            streaming_parallel=strategy == "streaming",
            verify_content=False,
            connection_pool_prewarm=False,
            request_delay=0,
            requests_per_second=0,
            max_retries=0,
            retry_delay=1,
        )
        start = len(remote.requests)
        if mode == "mirror":
            with MirrorURL(config) as mirror:
                assert mirror.sync()
                summary = mirror.metrics.get_summary()
                run = summary["run"]
        else:
            metrics = MetricsCollector()
            summary = download_url_list(config, metrics=metrics)
            assert summary["success"]
            run = metrics.run_report.snapshot(summary["bytes_downloaded"])
        assert summary["bytes_downloaded"] == sum(map(len, payloads.values()))
        assert summary["files_failed"] == 0
        assert run["missing_files_downloaded"] == 3
        assert run["sha256_hashes_recorded"] == 0
        for name, payload in payloads.items():
            assert (config.dest_path / name).read_bytes() == payload
        payload_requests = [
            (method, path, headers.get("Range"))
            for method, path, headers in remote.requests[start:]
            if method == "GET" and path in payloads
        ]
        comparisons[mode] = {
            "elapsed_seconds": run["elapsed_seconds"],
            "download_seconds": run["phase_seconds"]["downloads"],
            "transfer_stage_seconds": run["transfer_stage_seconds"],
            "payload_requests": sorted(payload_requests),
        }
    assert comparisons["mirror"]["payload_requests"] == comparisons["download"]["payload_requests"]
    record_property(
        "comparison",
        json.dumps({"backend": backend, "strategy": strategy, "runs": comparisons}, sort_keys=True),
    )
