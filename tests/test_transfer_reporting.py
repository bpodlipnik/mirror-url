"""Reporting remains accurate during waits and before publication."""

from __future__ import annotations

import logging
import threading

import pytest

from mirror_url import MirrorConfig
from mirror_url import transfer_report as report


def test_progress_tracks_retry_bytes_without_crediting_unpublished_files(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    path = tmp_path / "large.pdf"
    progress = report.DownloadProgress(True)
    progress.plan(1)
    progress.begin(path)
    progress.received(path, 1024, 2048)
    progress.log_progress()
    assert "0/1 files completed" in caplog.text
    assert "1.00 KiB/2.00 KiB" in caplog.text and "ETA " in caplog.text
    progress.begin(path, "waiting for response")
    progress.received(path, 2048, 2048)
    progress.stage(path, "hashing (SHA-256)")
    caplog.clear()
    progress.log_progress()
    assert "received 3.00 KiB" in caplog.text
    assert "0/1 files completed" in caplog.text and "hashing (SHA-256)" in caplog.text
    assert "ETA " not in caplog.text
    progress.finish(path, True)
    progress.saving_receipts()
    caplog.clear()
    progress.log_progress()
    assert "1/1 files completed" in caplog.text and "saving receipts" in caplog.text
    assert not progress.active


def test_unknown_lengths_failed_files_and_bounded_active_display(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    progress = report.DownloadProgress(True)
    progress.log_progress()
    assert "0/? files completed" in caplog.text and "preparing downloads" in caplog.text
    progress.plan(4)
    for number in range(4):
        path = tmp_path / str(number)
        progress.begin(path)
        progress.received(path, 10, None)
    caplog.clear()
    progress.log_progress()
    assert "and 1 more active files" in caplog.text
    assert "ETA" not in caplog.text and "/0.00 B" not in caplog.text
    progress.finish(tmp_path / "0", False)
    caplog.clear()
    progress.log_progress()
    assert "0/4 files completed, 1 failed" in caplog.text


def test_timer_stops_and_joins_on_exception_and_repeated_runs(monkeypatch):
    monkeypatch.setattr(report, "PROGRESS_INTERVAL_SECONDS", 0.02)
    for _ in range(3):
        observed = threading.Event()
        progress = report.DownloadProgress(True)
        monkeypatch.setattr(progress, "log_progress", observed.set)
        with pytest.raises(RuntimeError, match="test interruption"):
            with progress:
                assert observed.wait(3)
                raise RuntimeError("test interruption")
        assert progress.thread is not None and not progress.thread.is_alive()
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


def test_disabled_progress_has_no_thread_state_or_output(tmp_path, caplog):
    with report.DownloadProgress(False) as progress:
        progress.plan(1)
        path = tmp_path / "a"
        progress.begin(path)
        progress.received(path, 10, 20)
        progress.stage(path, "flushing file")
        progress.finish(path, True)
        progress.saving_receipts()
    assert progress.thread is None and not progress.active and progress.received_bytes == 0
    assert not caplog.records


@pytest.mark.parametrize("verify", [False, True])
def test_readable_summary_explains_real_hash_status(tmp_path, caplog, verify):
    caplog.set_level(logging.INFO)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        verify_content=verify,
    )
    summary = {
        "success": True,
        "files_downloaded": 1,
        "files_failed": 0,
        "bytes_downloaded": 9543236657,
        "elapsed_seconds": 90.83098622504622,
        "stage_seconds": {"fsync": 3.5375502910465, "hash": 0.00011729821562767029},
        "receipts": str(tmp_path / "receipts.json"),
    }
    report.log_download_summary(config, summary)
    assert "DOWNLOAD SUMMARY:" in caplog.text and "Result: SUCCESS" in caplog.text
    assert "8.89 GiB (9543236657 bytes)" in caplog.text
    assert "100.20 MiB/s" in caplog.text and "90.831s" in caplog.text
    assert "Disk flush time: 3.538s" in caplog.text
    assert ("SHA-256 time:" in caplog.text) is verify
    assert ("SHA-256: disabled" in caplog.text) is not verify
    assert "{'" not in caplog.text


def test_failed_zero_duration_and_dry_run_summaries(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    config = MirrorConfig(
        base_url="https://files.example/root", dest_path=tmp_path, log_path=tmp_path
    )
    report.log_download_summary(
        config,
        {
            "success": False,
            "files_downloaded": 0,
            "files_failed": 1,
            "bytes_downloaded": 0,
            "elapsed_seconds": 0,
            "stage_seconds": {"fsync": 0, "hash": 0},
            "receipts": "receipts.json",
        },
    )
    assert "Result: FAILED" in caplog.text and "Average throughput: n/a" in caplog.text
    caplog.clear()
    report.log_download_summary(config, {"success": True, "dry_run": True, "files": 2})
    assert "Dry run: no downloads" in caplog.text and "Files planned: 2" in caplog.text
    assert "Files downloaded:" not in caplog.text


@pytest.mark.parametrize("size, expected", [(0, "0.00 B"), (1024**4, "1.00 TiB")])
def test_binary_units(size, expected):
    assert report.transfer_bytes(size) == expected
