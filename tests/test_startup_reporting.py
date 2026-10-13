"""Mirror and download CLI runs expose the same startup fields in the same order."""

from __future__ import annotations

import logging
import re
from datetime import datetime

import pytest

from mirror_url import __version__, cli
from mirror_url.metrics import log_run_start
from test_download_mode_chunks import ranges as ranges
from test_transfer_backends import backend as backend


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("verify", [False, True])
def test_cli_startup_fields_and_file_plan_match(
    tmp_path, ranges, backend, mode, shared, verify, monkeypatch, capsys
):
    destination, logs = tmp_path / "dest", tmp_path / "logs"
    arguments = [
        "mirror-url",
        "--url",
        ranges.base,
        "--mode",
        mode,
        "--backend",
        backend,
        "--dest-path",
        str(destination),
        "--log-path",
        str(logs),
        "--print-logs",
        "--progress-bar",
        "--streaming-parallel",
        "--min-chunk-size",
        "1",
        "--max-chunks",
        "3",
        "--request-delay",
        "0",
        "--requests-per-second",
        "0",
    ]
    if mode == "download":
        arguments.append("a")
    if shared:
        arguments.extend(["--log-file", "compare"])
    if verify:
        arguments.append("--verify-content")
    monkeypatch.setattr("sys.argv", arguments)
    if mode == "mirror":
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        assert stopped.value.code == 0
    else:
        cli.main()
    output = capsys.readouterr()
    assert output.out == ""
    messages = [
        line.split(" [INFO] ", 1)[1] for line in output.err.splitlines() if " [INFO] " in line
    ]
    expected = [
        "Starting MirrorURL",
        "Job started at:",
        "Command:",
        "Processing directory suffix:",
        "Remote root:",
        "Destination path:",
        "Log file:",
        "Backend:",
        "SHA-256:",
    ]
    header = [message for message in messages if any(message.startswith(key) for key in expected)]
    assert len(header) == len(expected), header
    assert all(message.startswith(key) for key, message in zip(expected, header)), header
    assert header[0] == f"Starting MirrorURL v{__version__} mode: {mode}"
    assert re.fullmatch(r"Job started at: \d{4}-\d\d-\d\d \d\d:\d\d:\d\d", header[1])
    assert "mirror-url --url" in header[2]
    assert header[3] == "Processing directory suffix: 'ROOT'"
    assert header[4].rstrip("/") == "Remote root: " + ranges.base.rstrip("/")
    assert header[5] == f"Destination path: {destination.resolve()}"
    actual_logs = list(logs.glob("*.log"))
    assert len(actual_logs) == 1
    assert header[6] == f"Log file: {actual_logs[0]}"
    assert header[7] == f"Backend: {backend}"
    assert header[8] == "SHA-256: " + ("enabled (local receipts)" if verify else "disabled")
    assert messages.count("File download plan: 1 files; 1 file workers") == 1
    assert "KNOWN-URL DOWNLOAD" not in output.err and "SHARED LOG" not in output.err
    assert "Mirroring from:" not in output.err and "Command line used:" not in output.err
    assert (destination / "a").read_bytes() == ranges.payload
    for fragment in ("SAFE MODE", "URL sanitization enabled", "Path safety:"):
        assert output.err.count(fragment) == 1
    assert "Workers: 8, Max Retries:" not in output.err


def test_startup_redacts_command_and_root_and_identifies_multiple_scopes(
    tmp_path, caplog, monkeypatch
):
    caplog.set_level(logging.INFO)
    root = "https://user:password@files.example/root/?token=secret#private"
    monkeypatch.setattr("sys.argv", ["mirror-url", "--url=" + root])
    log_run_start(
        mode="mirror",
        remote_root=root,
        destination=tmp_path,
        log_path=tmp_path / "run.log",
        backend="httpx",
        verify_content=False,
        suffixes=("one", "two"),
        started_at=datetime(2026, 10, 13, 13, 5, 46),
    )
    assert "Job started at: 2026-10-13 13:05:46" in caplog.text
    assert "Processing directory suffixes: 'one', 'two'" in caplog.text
    assert "Remote root: https://files.example/root/?<redacted>" in caplog.text
    for secret in ("user:password", "token=secret", "#private"):
        assert secret not in caplog.text
