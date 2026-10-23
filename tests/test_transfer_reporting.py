"""Reporting remains accurate during waits and before publication."""

from __future__ import annotations

import builtins
import io
import logging
import sys
import threading

import pytest

from mirror_url import MirrorConfig
from mirror_url import progress as report
from mirror_url.metrics import (
    MetricsCollector,
    format_download_metrics,
    format_download_summary,
    log_download_report,
)
from mirror_url.run_report import RunReport
from mirror_url.utils import format_bytes


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
def test_download_summary_has_no_separate_details_footer(tmp_path, caplog, verify):
    caplog.set_level(logging.INFO)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        verify_content=verify,
        metrics=True,
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
    run = RunReport().snapshot()
    run.update(
        {
            "status": "success",
            "remote_files_found": 1,
            "missing_files_downloaded": 1,
            "unclassified_downloads": 0,
            "phase_seconds": {
                "download preparation": 1.0,
                "downloads": 80.0,
                "cleanup": 9.83098622504622,
            },
            "download_throughput": summary["bytes_downloaded"] / 80.0,
            "elapsed_seconds": summary["elapsed_seconds"],
        }
    )
    log_download_report(config, summary, tmp_path, run)
    messages = [record.getMessage() for record in caplog.records]
    text = "\n".join(messages)
    assert messages.count("SUMMARY:") == 1
    assert messages.count("METRICS SUMMARY:") == 1
    assert all("\n" not in message for message in messages)
    assert "Result: SUCCESS" in text
    assert "  Downloaded: 1\n  Skipped: 0\n  Failed: 0\n  Size: 8.89 GiB" in text
    assert "  Speed: 113.76 MiB/s\n  Duration: 0h 1m 30s" in text
    assert "On disk" not in text
    assert "Downloaded bytes: 8.89 GiB" in text
    assert "Download throughput: 113.76 MiB/s" in text and "90.831s" in text
    for extra in (
        "Backend:",
        "Destination:",
        "SHA-256:",
        "Disk flush time",
        "SHA-256 time",
        "Ownership receipts:",
    ):
        assert extra not in text
    assert text.rstrip("\n- ").endswith(
        "  Cache signatures: 0\n  RGET-LIST used: False\n  Fast parses: 0\n  LXML parses: 0"
    )
    assert "Download preparation time: 1.000s" in text
    assert "Downloads time: 80.000s" in text
    assert "Cleanup time: 9.831s" in text
    assert "Missing files downloaded: 1" in text
    assert "Existing files downloaded: 0" in text
    assert "Discovery elapsed time: not performed" in text
    assert "Metadata time: not performed" in text
    assert "Freshness checks skipped: 0" in text
    assert "Directories scanned: 0" in text
    assert "Directory listing GET calls: 0" in text
    assert "DOWNLOAD SUMMARY:" not in text
    assert "{'" not in text


def test_failed_zero_duration_and_dry_run_summaries(tmp_path, caplog):
    caplog.set_level(logging.INFO)
    config = MirrorConfig(
        base_url="https://files.example/root", dest_path=tmp_path, log_path=tmp_path
    )
    logging.info(
        format_download_metrics(
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
            tmp_path,
        )
    )
    assert "Result: FAILED" in caplog.text and "Download throughput: n/a" in caplog.text
    assert "Missing files downloaded: n/a" in caplog.text
    assert "Existing files downloaded: n/a" in caplog.text
    caplog.clear()
    logging.info(
        format_download_metrics(config, {"success": True, "dry_run": True, "files": 2}, tmp_path)
    )
    assert "Result: DRY RUN" in caplog.text and "Files that would be downloaded: 2" in caplog.text
    assert "Files downloaded:" not in caplog.text


def test_download_summary_retains_mirror_field_order_and_labels(tmp_path):
    config = MirrorConfig(
        base_url="https://files.example/root", dest_path=tmp_path, log_path=tmp_path
    )
    metrics = MetricsCollector()
    metrics.run_report.start({}, 0)
    metrics.run_report.finish("success")
    run = metrics.run_report.snapshot()
    mirror = metrics.report()
    download = format_download_metrics(
        config,
        {
            "success": True,
            "files_downloaded": 0,
            "files_failed": 0,
            "bytes_downloaded": 0,
            "elapsed_seconds": run["elapsed_seconds"],
            "stage_seconds": {"fsync": 0, "hash": 0},
            "receipts": str(tmp_path / "receipts.json"),
        },
        tmp_path,
        run,
    )

    def labels(text):
        return [line.split(":", 1)[0].strip() for line in text.splitlines()]

    mirror_labels, download_labels = labels(mirror), labels(download)
    direct_counts = {"Files requested", "Existing files downloaded", "Files downloaded"}
    assert [label for label in download_labels if label not in direct_counts] == mirror_labels
    footer = "  In-memory parsed listing reuses:"
    assert download[download.index(footer) :] == mirror[mirror.index(footer) :]
    for label in (
        "Selected paths",
        "Existing files checked and current",
        "Missing files downloaded",
        "Files failed",
        "Downloaded bytes",
        "Download throughput",
        "Total duration",
        "Directories scanned",
        "Directory listing GET calls",
        "Cache signatures",
    ):
        mirror_line = next(
            line for line in mirror.splitlines() if line.strip().startswith(label + ":")
        )
        assert mirror_line in download
    assert "Discovery elapsed time: not performed" in download
    assert "Metadata time: not performed" in download
    assert "Download preparation time: n/a" in download


@pytest.mark.parametrize("size, expected", [(0, "0.00 B"), (1024**4, "1.00 TiB")])
def test_binary_units(size, expected):
    assert format_bytes(size, binary_units=True) == expected


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize(
    "size,amount,rate",
    [
        (8 * 1024**2, "8.00 MiB", "1.00 MiB/s"),
        (8 * 1024**3, "8.00 GiB", "1.00 GiB/s"),
        (268509609, "256.07 MiB", "32.01 MiB/s"),
    ],
)
def test_both_summaries_use_iec_labels_without_changing_bytes_or_throughput(
    tmp_path, monkeypatch, mode, size, amount, rate
):
    clock = [20.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: clock[0])
    metrics = MetricsCollector()
    metrics.run_report.start(metrics.metrics, 0)
    metrics.run_report.record_check("https://files.example/a", "missing")
    metrics.run_report.record_publication("https://files.example/a")
    metrics.increment("files_downloaded")
    metrics.add_bytes(size)
    metrics.run_report.begin_phase("downloads")
    clock[0] = 28.0
    metrics.run_report.finish("success")
    before = metrics.get_summary()
    config = MirrorConfig(
        mode=mode,
        base_url="https://files.example/",
        dest_path=tmp_path,
        log_path=tmp_path,
        download_files=["a"] if mode == "download" else [],
    )
    text = (
        metrics.report()
        if mode == "mirror"
        else format_download_metrics(config, before, tmp_path, before["run"], metrics=metrics)
    )
    assert f"Downloaded bytes: {amount}" in text
    assert f"Download throughput: {rate}" in text
    compact = (
        metrics.summary()
        if mode == "mirror"
        else format_download_summary(config, before, tmp_path, before["run"], metrics=metrics)
    )
    assert f"  Size: {amount}\n  Speed: {rate}\n  Duration: 0h 0m 8s" in compact
    assert "METRICS SUMMARY:" not in compact
    assert "\nSUMMARY:" not in text
    after = metrics.get_summary()
    assert after["bytes_downloaded"] == before["bytes_downloaded"] == size
    assert after["run"] == before["run"]
    assert after["run"]["download_throughput"] == size / 8


def test_cli_help_labels_chunk_and_bandwidth_units(monkeypatch, capsys):
    from mirror_url import cli

    monkeypatch.setattr(sys, "argv", ["mirror-url", "--help"])
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 0
    help_text = capsys.readouterr().out
    assert "--min-chunk-size MiB" in help_text
    assert "Minimum chunk size in MiB" in help_text
    assert "--bandwidth-limit MiB/S" in help_text
    assert "Limit download bandwidth (MiB/s)" in help_text


@pytest.mark.parametrize("total", [None, 4 * 1024**2, 4 * 1024**3])
def test_actual_byte_bars_use_iec_labels_for_amounts_and_rates(total, monkeypatch):
    scale = 1024**3 if total == 4 * 1024**3 else 1024**2
    label = "GiB" if scale == 1024**3 else "MiB"
    bar = report._create_progress_bar(
        total=total,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        mininterval=float("inf"),
        file=io.StringIO(),
    )
    assert bar is not None
    try:
        monkeypatch.setattr(bar, "_time", lambda: bar.start_t + 2)
        bar.update(2 * scale)
        text = str(bar)
        assert f"2.00 {label}" in text and f"1.00 {label}/s" in text
        if total is None:
            assert "/?" in text and "<" not in text
        else:
            assert f"/4.00 {label}" in text and "50%" in text
        assert bar.n == 2 * scale and bar.total == total
    finally:
        bar.close()


class RecordingBar:
    def __init__(self, **settings):
        self.settings = settings
        self.total, self.n = settings["total"], 0
        self.closed = False
        self.stage = ""
        self.refreshes = 0

    def update(self, size):
        self.n += size

    def reset(self):
        self.n = 0

    def set_postfix_str(self, stage, refresh=False):
        self.stage = stage

    def refresh(self):
        self.refreshes += 1

    def close(self):
        self.closed = True


@pytest.mark.parametrize("renderer", ["active", "missing", "failed", "disabled"])
def test_bars_replace_console_ticks_but_keep_file_log_and_fallback(tmp_path, monkeypatch, renderer):
    monkeypatch.setattr(report, "TQDM_AVAILABLE", renderer != "missing")

    def create(**settings):
        if renderer == "failed":
            return None
        bar = RecordingBar(**settings)
        bar.disable = renderer == "disabled"
        return bar

    monkeypatch.setattr(report, "_create_progress_bar", create)
    console, file_log = io.StringIO(), io.StringIO()
    console_handler, file_handler = logging.StreamHandler(console), logging.StreamHandler(file_log)
    console_handler.addFilter(report.ConsoleProgressFilter())
    previous_level = logging.root.level
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(console_handler)
    logging.root.addHandler(file_handler)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
    )
    try:
        with report.DownloadProgress(True, config) as progress:
            path = tmp_path / "a.pdf"
            progress.plan(1)
            progress.begin(path)
            announcement = (
                "live byte bars; periodic status saved in the log file"
                if renderer == "active"
                else "console status approximately every 10 seconds"
            )
            assert announcement in console.getvalue()
            progress.received(path, 10, 20)
            for _ in range(3):
                progress.log_progress()
            assert console.getvalue().count("Progress display:") == 1
            assert file_log.getvalue().count("Download progress:") == 3
            assert console.getvalue().count("Download progress:") == (
                0 if renderer == "active" else 3
            )
            logging.warning("A transfer warning")
            assert "A transfer warning" in console.getvalue()
            progress.finish(path, True)
            progress.saving_receipts()
            progress.log_progress()
            assert "saving receipts" in console.getvalue()
            logging.info("SUMMARY:\n  Result: SUCCESS")
            assert "SUMMARY:" in console.getvalue()
    finally:
        logging.root.removeHandler(console_handler)
        logging.root.removeHandler(file_handler)
        console_handler.close()
        file_handler.close()
        logging.root.setLevel(previous_level)


def test_mirror_and_known_url_use_shared_terminal_renderer(tmp_path, monkeypatch):
    bars = []

    def create(**settings):
        bars.append(RecordingBar(**settings))
        return bars[-1]

    monkeypatch.setattr(report, "_create_progress_bar", create)
    monkeypatch.setattr(report, "TQDM_AVAILABLE", True)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
    )
    tracker = report.ProgressTracker(1, name="files", config=config)
    tracker.update()
    tracker.report_final()
    assert bars[0].n == 1 and bars[0].closed
    with report.DownloadProgress(False, config) as progress:
        path = tmp_path / "large.pdf"
        progress.begin(path)
        progress.received(path, 1024, 2048)
        assert bars[1].n == 1024 and bars[1].total == 2048
        progress.begin(path, "waiting for response")
        assert bars[1].n == 0 and bars[1].total is None
        progress.received(path, 2048, None)
        assert bars[1].n == 2048 and bars[1].total is None
        assert progress.received_bytes == 3072 and progress.completed == 0
        progress.stage(path, "flushing file")
        assert bars[1].stage == "flushing file"
        progress.finish(path, True)
        assert progress.completed == 1 and bars[1].closed
    assert progress.thread is not None and not progress.thread.is_alive()


def test_parallel_bars_reuse_slots_and_close_on_exception(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(report, "_create_progress_bar", RecordingBar)
    monkeypatch.setattr(report, "TQDM_AVAILABLE", True)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
    )
    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError):
        with report.DownloadProgress(False, config) as progress:
            a, b, c = [tmp_path / name for name in ("a", "b", "c")]
            progress.begin(a)
            progress.begin(b)
            first = progress.active[a].bar
            second = progress.active[b].bar
            assert first.settings["position"] == 0 and second.settings["position"] == 1
            progress.finish(a, False)
            progress.begin(c)
            third = progress.active[c].bar
            assert third.settings["position"] == 0
            before = second.refreshes
            progress.log_progress()
            assert second.refreshes > before and not caplog.records
            raise RuntimeError("interrupted")
    assert first.closed and second.closed and third.closed
    assert progress.thread is not None and not progress.thread.is_alive()


def test_missing_renderer_and_quiet_behavior(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(report, "TQDM_AVAILABLE", False)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
    )
    with report.DownloadProgress(False, config) as progress:
        assert progress.thread is None and not progress.bars
    assert "install tqdm (a required mirror-url dependency)" in caplog.text
    caplog.clear()
    config.quiet = True
    with report.DownloadProgress(False, config) as progress:
        assert progress.thread is None and not progress.bars
    assert not caplog.records


def test_terminal_renderer_failures_do_not_fail_download_reporting(tmp_path, monkeypatch):
    class BrokenBar(RecordingBar):
        def update(self, size):
            raise OSError("terminal disconnected")

        def close(self):
            raise OSError("terminal disconnected")

    monkeypatch.setattr(report, "_create_progress_bar", BrokenBar)
    monkeypatch.setattr(report, "TQDM_AVAILABLE", True)
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
    )
    with report.DownloadProgress(False, config) as progress:
        path = tmp_path / "a"
        progress.begin(path)
        assert progress.active[path].bar is None
        progress.received(path, 10, 10)
        progress.finish(path, True)
        assert progress.completed == 1


def test_shared_terminal_renderer_handles_missing_import(monkeypatch):
    original = builtins.__import__

    def unavailable(name, *args, **kwargs):
        if name == "tqdm":
            raise ImportError("dependency unavailable")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", unavailable)
    assert report._create_progress_bar(total=1) is None


def test_resumed_and_concurrent_chunk_progress_counts_only_new_traffic(tmp_path, monkeypatch):
    monkeypatch.setattr(report, "_create_progress_bar", RecordingBar)
    config = MirrorConfig(
        base_url="https://files.example/", dest_path=tmp_path, log_path=tmp_path, progress_bar=True
    )
    with report.DownloadProgress(True, config) as progress:
        path = tmp_path / "resume"
        progress.begin(path)
        progress.resume(path, 1024, 4096)
        assert progress.received_bytes == 0
        assert progress.active[path].bar.n == 1024
        assert progress.active[path].bar.initial == 1024
        progress.received(path, 2048, 4096)
        assert progress.received_bytes == 1024
        progress.finish(path, True)
        path = tmp_path / "chunked"
        progress.begin(path)
        progress.received_chunk(path, 0, 1024, 4096)
        progress.received_chunk(path, 1, 2048, 4096)
        assert progress.active[path].bar.n == 3072
        progress.received_chunk(path, 0, 0, 4096)
        assert progress.active[path].bar.n == 2048
        progress.received_chunk(path, 0, 2048, 4096)
        assert progress.active[path].bar.n == 4096
        assert progress.received_bytes == 6144
        assert progress.completed == 1  # Receiving the body doesn't publish chunked.
        progress.stage(path, "assembling chunks")
        assert progress.active[path].bar.stage == "assembling chunks"
        progress.finish(path, True)
    assert progress.completed == 2
    disabled = report.get_download_progress(None)
    disabled.resume(path, 1, None)
    disabled.received_chunk(None, 0, 1, 2)
    assert not disabled.active and disabled.thread is None


@pytest.mark.parametrize("quiet,broken", [(True, False), (False, False), (False, True)])
def test_metadata_bar_closes_once_even_on_renderer_failure(tmp_path, monkeypatch, quiet, broken):
    class Bar(RecordingBar):
        closes = 0

        def refresh(self):
            if broken:
                raise OSError("terminal unavailable")
            super().refresh()

        def close(self):
            self.closes += 1
            super().close()

    bars = []

    def create(**settings):
        bars.append(Bar(**settings))
        return bars[-1]

    monkeypatch.setattr(report, "_create_progress_bar", create)
    config = MirrorConfig(
        base_url="https://files.example/",
        dest_path=tmp_path,
        log_path=tmp_path,
        progress_bar=True,
        quiet=quiet,
    )
    tracker = report.ProgressTracker(2, config=config)
    tracker.update()
    tracker.report_final()
    tracker.close()
    assert tracker.completed == 1 and tracker.tqdm_bar is None
    if quiet:
        assert not bars
    else:
        assert bars[0].n == 1 and bars[0].closed and bars[0].closes == 1
        assert bars[0].settings["leave"] is False


@pytest.mark.parametrize("available", [False, True])
def test_console_logging_coordinates_with_renderer(monkeypatch, available):
    import tqdm

    monkeypatch.setattr(report, "TQDM_AVAILABLE", available)
    writes = []
    original = tqdm.tqdm.write

    def write(message, **kwargs):
        writes.append(message)
        original(message, **kwargs)

    monkeypatch.setattr(tqdm.tqdm, "write", write)
    stream = io.StringIO()
    handler = report.ProgressConsoleHandler(stream)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "ordinary log", (), None)
    handler.handle(record)
    assert stream.getvalue() == "[INFO] ordinary log\n"
    assert len(writes) == int(available)
    record.progress_bar_active = True
    handler.handle(record)
    assert stream.getvalue().count("ordinary log") == 1


def test_console_logging_falls_back_when_tqdm_write_fails(monkeypatch):
    import tqdm

    def fail(*args, **kwargs):
        raise OSError("terminal renderer failed")

    monkeypatch.setattr(tqdm.tqdm, "write", fail)
    stream = io.StringIO()
    handler = report.ProgressConsoleHandler(stream)
    record = logging.LogRecord("test", logging.WARNING, __file__, 1, "retained warning", (), None)
    handler.handle(record)
    assert stream.getvalue() == "retained warning\n"


def test_actual_tqdm_retry_resets_resumed_rate_baseline(tmp_path, monkeypatch):
    from tqdm import tqdm

    stream = io.StringIO()
    monkeypatch.setattr(
        report, "_create_progress_bar", lambda **settings: tqdm(file=stream, **settings)
    )
    config = MirrorConfig(
        base_url="https://files.example/", dest_path=tmp_path, log_path=tmp_path, progress_bar=True
    )
    with report.DownloadProgress(False, config) as progress:
        path = tmp_path / "resume"
        progress.begin(path)
        progress.resume(path, 1024, 4096)
        progress.received(path, 2048, 4096)
        bar = progress.active[path].bar
        assert bar.format_dict["initial"] == 1024
        progress.begin(path, "waiting for response")
        assert bar.n == 0 and bar.format_dict["initial"] == 0
        progress.received(path, 4096, 4096)
        assert bar.n == 4096 and progress.received_bytes == 5120
        progress.finish(path, True)


def test_existing_console_adaptation_preserves_settings_and_custom_handlers():
    stream = io.StringIO()
    plain = logging.StreamHandler(stream)
    plain.setLevel(logging.WARNING)
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    plain.setFormatter(formatter)
    filter_ = logging.Filter("allowed")
    plain.addFilter(filter_)
    coordinated = report.ProgressConsoleHandler.adapt(plain)
    assert isinstance(coordinated, report.ProgressConsoleHandler)
    assert coordinated.stream is stream and coordinated.formatter is formatter
    assert coordinated.level == logging.WARNING and filter_ in coordinated.filters
    assert any(isinstance(f, report.ConsoleProgressFilter) for f in coordinated.filters)
    assert report.ProgressConsoleHandler.adapt(coordinated) is coordinated

    class CustomHandler(logging.StreamHandler):
        pass

    custom = CustomHandler(stream)
    assert report.ProgressConsoleHandler.adapt(custom) is custom


def test_metadata_ticks_keep_file_logs_without_duplicating_visible_bar(tmp_path, monkeypatch):
    monkeypatch.setattr(report, "_create_progress_bar", RecordingBar)
    config = MirrorConfig(
        base_url="https://files.example/", dest_path=tmp_path, log_path=tmp_path, progress_bar=True
    )
    console, file_log = io.StringIO(), io.StringIO()
    console_handler = report.ProgressConsoleHandler(console)
    file_handler = logging.StreamHandler(file_log)
    previous_level = logging.root.level
    logging.root.setLevel(logging.INFO)
    logging.root.addHandler(console_handler)
    logging.root.addHandler(file_handler)
    tracker = report.ProgressTracker(2, name="files examined", config=config)
    monkeypatch.setattr(tracker, "_should_report", lambda: True)
    try:
        tracker.update()
        assert "1/2 files examined" in file_log.getvalue()
        assert "1/2 files examined" not in console.getvalue()
        logging.warning("metadata warning")
        assert "metadata warning" in console.getvalue()
    finally:
        tracker.close()
        for handler in (console_handler, file_handler):
            logging.root.removeHandler(handler)
            handler.close()
        logging.root.setLevel(previous_level)


def test_chunk_concurrency_peak_resets_for_each_run():
    metrics = MetricsCollector()
    metrics.run_report.start(metrics.metrics, 0)
    metrics.increment("chunk_downloads", 8)
    metrics.increment("total_chunks", 8)
    assert metrics.chunk_summary()["total_chunks"] == 8
    for _ in range(4):
        metrics.chunk_started()
    assert metrics.chunk_summary()["peak_parallel_chunks"] == 4
    for _ in range(4):
        metrics.chunk_finished()
    assert metrics.metrics["active_chunks"] == 0
    metrics.run_report.start(metrics.metrics, 0)
    assert metrics.chunk_summary()["total_chunks"] == 0
    assert metrics.chunk_summary()["chunk_downloads"] == 0
    metrics.chunk_started()
    metrics.chunk_finished()
    assert metrics.chunk_summary()["peak_parallel_chunks"] == 1
    assert "Peak parallel chunks: 1" in metrics.report()
    metrics.run_report.start(metrics.metrics, 0)
    assert metrics.chunk_summary()["peak_parallel_chunks"] == 0
    assert "Peak parallel chunks: 4" not in metrics.report()


def test_completed_or_disabled_progress_ignores_late_chunk_observations(tmp_path):
    path = tmp_path / "a"
    progress = report.DownloadProgress(True)
    progress.begin(path)
    progress.chunk_plan(path, 8)
    progress.chunk_activity(path, 0, True)
    progress.chunk_activity(path, 0, False, True)
    progress.finish(path, False)
    progress.chunk_plan(path, 8)
    progress.chunk_activity(path, 0, False)
    assert not progress.active
    disabled = report.DownloadProgress(False)
    disabled.chunk_plan(path, 8)
    disabled.chunk_activity(path, 0, True)
    assert not disabled.active


def test_summary_reuses_collector_without_overwriting_lifetime_counters(tmp_path):
    config = MirrorConfig(base_url="https://files.example/", dest_path=tmp_path, log_path=tmp_path)
    metrics = MetricsCollector()
    metrics.increment("chunk_downloads", 12)
    before = dict(metrics.metrics)
    summary = {
        "success": True,
        "files_downloaded": 1,
        "files_failed": 0,
        "bytes_downloaded": 1,
        "chunk_metrics": {
            "chunk_downloads": 8,
            "chunk_assemblies": 1,
            "parallel_files": 1,
            "total_chunks": 8,
            "peak_parallel_chunks": 4,
        },
    }
    text = format_download_metrics(config, summary, tmp_path, metrics=metrics)
    assert "Chunk downloads: 8" in text and "Peak parallel chunks: 4" in text
    assert metrics.metrics == before


@pytest.mark.parametrize("dry_run,success", [(False, True), (False, False), (True, True)])
def test_known_url_compact_and_metrics_formatters_are_independent(tmp_path, dry_run, success):
    config = MirrorConfig(base_url="https://example.com/", dest_path=tmp_path, log_path=tmp_path)
    result = {
        "success": success,
        "dry_run": dry_run,
        "files": 2,
        "files_downloaded": 0 if dry_run else 1,
        "files_failed": int(not success),
        "bytes_downloaded": 0 if dry_run else 1024,
        "elapsed_seconds": 90,
    }
    compact = format_download_summary(config, result, tmp_path)
    detail = format_download_metrics(config, result, tmp_path)
    assert compact.splitlines() == [
        "SUMMARY:",
        f"  Downloaded: {result['files_downloaded']}",
        "  Skipped: 0",
        f"  Failed: {int(not success)}",
        f"  Size: {'0.00 B' if dry_run else '1.00 KiB'}",
        "  Speed: n/a",
        "  Duration: 0h 1m 30s",
    ]
    assert detail.splitlines()[0] == "METRICS SUMMARY:"
    assert "SUMMARY:" not in detail.splitlines()[1:]
    assert "On disk" not in compact and "METRICS SUMMARY:" not in compact
