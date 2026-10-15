"""CLI totals include initialization and drained shutdown in both modes."""

from __future__ import annotations

import json
import socket

import pytest

from mirror_url import cli
from mirror_url._core._base import _MirrorBase
from mirror_url.destination_lock import DestinationLock
from mirror_url.run_report import RunReport
from test_transfer_backends import backend as backend
from test_transfer_backends import origin as origin


def test_job_clock_includes_setup_and_shutdown_without_changing_throughput(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start_job(now[0])
    now[0] += 3
    report.start({}, 0)
    with report.phase("downloads"):
        report.record_publication("a")
        now[0] += 5
    report.finish("success")
    now[0] += 7
    report.finish_job()
    now[0] += 100
    run = report.snapshot(50)
    assert run["elapsed_seconds"] == 15
    assert run["duration_scope"] == "job"
    assert run["download_throughput"] == 10
    assert run["phase_seconds"] == {"downloads": 5}
    report.finish_job(RuntimeError("too late"))
    assert report.snapshot(50) == run
    report.start({}, 0)
    assert report.snapshot()["duration_scope"] == "operation"
    assert report.snapshot()["elapsed_seconds"] == 0


def test_shutdown_failure_cannot_retain_success_or_erase_transfer_failure(monkeypatch):
    now = [10.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    report = RunReport()
    report.start_job(now[0])
    report.start({}, 0)
    report.finish("failed", "transfer failure")
    now[0] += 4
    report.finish_job(OSError("close failed"))
    run = report.snapshot()
    assert run["status"] == "failed"
    assert run["reasons"] == "transfer failure; job finalization: OSError"
    assert run["elapsed_seconds"] == 4
    report.finish("success")
    assert report.snapshot() == run


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("shutdown_failure", [False, True])
def test_real_cli_total_finishes_after_owned_shutdown(
    tmp_path, origin, backend, mode, shared, shutdown_failure, monkeypatch, capsys
):
    payload = b"exact fixture bytes"
    origin.routes["/root/"] = (200, {"Content-Type": "text/html"}, b'<a href="a">a</a>')
    origin.routes["/root/a"] = (200, {}, payload)
    now = [100.0]
    monkeypatch.setattr("mirror_url.run_report.time.perf_counter", lambda: now[0])
    initialize = _MirrorBase._initialize
    setup_logging = cli.setup_shared_logging
    end_phase = RunReport.end_phase
    close = DestinationLock.close
    finish = RunReport.finish_job
    finished = []

    def initialize_mirror(self, *args):
        now[0] += 3
        return initialize(self, *args)

    def initialize_download(*args):
        if mode == "download":
            now[0] += 3
        return setup_logging(*args)

    def end_downloads(self):
        if self._active_phase is not None and self._active_phase[0] == "downloads":
            now[0] += 5
        return end_phase(self)

    def close_owned_job(self, *args):
        owned = self._parent is not None and not self._closing
        result = close(self, *args)
        if owned:
            now[0] += 7
            if shutdown_failure:
                raise OSError("injected post-shutdown failure")
        return result

    def freeze(self, *args):
        result = finish(self, *args)
        finished.append(self.snapshot(len(payload)))
        now[0] += 100  # Terminal reporting must not extend the frozen total.
        return result

    monkeypatch.setattr(_MirrorBase, "_initialize", initialize_mirror)
    monkeypatch.setattr(cli, "setup_shared_logging", initialize_download)
    monkeypatch.setattr(RunReport, "end_phase", end_downloads)
    monkeypatch.setattr(DestinationLock, "close", close_owned_job)
    monkeypatch.setattr(RunReport, "finish_job", freeze)
    destination, logs = tmp_path / "dest", tmp_path / "logs"
    args = [
        "mirror-url",
        "--mode",
        mode,
        "--backend",
        backend,
        "--url",
        origin.base,
        "--dest-path",
        str(destination),
        "--log-path",
        str(logs),
        "--print-logs",
        "--sequential-downloads",
        "--no-connection-pool-prewarm",
        "--no-async-metadata",
        "--request-delay",
        "0",
        "--requests-per-second",
        "0",
        "--max-retries",
        "0",
    ]
    if mode == "download":
        args.append("a")
    if shared:
        args += ["--log-file", "compare"]
    monkeypatch.setattr("sys.argv", args)
    if mode == "mirror" or shutdown_failure:
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        assert stopped.value.code == int(shutdown_failure)
    else:
        cli.main()
    output = capsys.readouterr()
    assert len(finished) == 1
    run = finished[0]
    assert run["duration_scope"] == "job"
    assert run["elapsed_seconds"] == 15
    assert run["phase_seconds"]["downloads"] == 5
    assert run["download_throughput"] == len(payload) / 5
    assert run["status"] == ("failed" if shutdown_failure else "success")
    assert (destination / "a").read_bytes() == payload
    assert not list(destination.rglob("*.part")) and not list(destination.rglob("work_*"))
    if not shutdown_failure:
        assert output.err.count("SUMMARY:") == 1
        assert "Total duration: 15.000s" in output.err
        assert "Downloads time: 5.000s" in output.err
        assert output.err.index("SUMMARY:") < output.err.index("Closed all 1 connection pools")
    else:
        assert "Result: SUCCESS" not in output.err
        assert "Successfully processed:" not in output.err
    if mode == "download" and not shutdown_failure:
        assert output.out == ""


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["mirror", "download"])
def test_final_json_and_logged_summary_use_the_frozen_job_clock(
    tmp_path, origin, backend, mode, monkeypatch, capsys
):
    payload = b"exact JSON fixture bytes"
    origin.routes["/root/"] = (200, {"Content-Type": "text/html"}, b'<a href="a">a</a>')
    origin.routes["/root/a"] = (200, {}, payload)
    captured = []
    finish = RunReport.finish_job

    def freeze(self, *args):
        finish(self, *args)
        captured.append(self.snapshot(len(payload)))

    monkeypatch.setattr(RunReport, "finish_job", freeze)
    destination, logs, exported = tmp_path / "dest", tmp_path / "logs", tmp_path / "metrics.json"
    args = [
        "mirror-url",
        "--mode",
        mode,
        "--backend",
        backend,
        "--url",
        origin.base,
        "--dest-path",
        str(destination),
        "--log-path",
        str(logs),
        "--sequential-downloads",
        "--no-connection-pool-prewarm",
        "--no-async-metadata",
        "--request-delay",
        "0",
        "--requests-per-second",
        "0",
        "--max-retries",
        "0",
    ]
    if mode == "download":
        args.append("a")
    else:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        args += ["--metrics-json", str(exported), "--health-check-port", str(port)]
    monkeypatch.setattr("sys.argv", args)
    if mode == "mirror":
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        assert stopped.value.code == 0
    else:
        cli.main()
    output = capsys.readouterr()
    assert len(captured) == 1
    run = captured[0]
    result = (
        json.loads(exported.read_text())["metrics"]["run"]
        if mode == "mirror"
        else json.loads(output.out)
    )
    assert result["duration_scope"] == "job"
    assert result["elapsed_seconds"] == run["elapsed_seconds"]
    assert run["elapsed_seconds"] > run["phase_seconds"]["downloads"]
    log = "\n".join(path.read_text() for path in logs.glob("*.log"))
    assert f"Total duration: {run['elapsed_seconds']:.3f}s" in log
    assert log.count("SUMMARY:") == 1
    assert (destination / "a").read_bytes() == payload
