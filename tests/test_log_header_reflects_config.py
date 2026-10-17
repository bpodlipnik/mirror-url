"""The ``--log-file`` header must describe the settings the run really uses.

``setup_shared_logging()`` used to read only the argparse namespace, so with
``--config`` every value that came from the YAML was reported as its parser
default. The worst case: ``cleanup_policy: delete`` in the YAML while the log
header said "SAFE MODE".
"""

from __future__ import annotations

import logging
import sys

import pytest

from mirror_url import cli


@pytest.fixture(autouse=True)
def _restore_logging():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    for h in handlers:
        root.addHandler(h)
    root.setLevel(level)


def _run(monkeypatch, tmp_path, yaml_extra, argv_extra=()):
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    cfg = tmp_path / "c.yaml"
    cfg.write_text(
        "base_url: https://example.com/files/\n"
        f"dest_path: {tmp_path / 'dest'}\n"
        f"log_path: {log_dir}\n" + yaml_extra
    )
    # Observe the real lifecycle's single configuration block, without making
    # a connection. CLI setup owns the header; MirrorURL owns its settings.
    monkeypatch.setattr("mirror_url._core._base._MirrorBase.test_connection", lambda _: False)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--config", str(cfg), "--log-file", "hdr", *argv_extra]
    )
    try:
        cli.main()
    except SystemExit:
        pass
    for h in logging.getLogger().handlers:
        h.flush()
    logs = list(log_dir.glob("hdr_*.log"))
    assert len(logs) == 1, logs
    return logs[0].read_text(encoding="utf-8")


def test_yaml_delete_policy_is_reported_not_safe_mode(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "cleanup_policy: delete\n")
    assert "DELETE MODE: Deletion ENABLED" in text
    assert "SAFE MODE" not in text


def test_cli_cleanup_overrides_yaml_in_header(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "cleanup_policy: delete\n", ["--cleanup", "preview"])
    assert "PREVIEW MODE" in text
    assert "DELETE MODE" not in text


def test_yaml_safe_urls_false_and_resume_false_not_claimed(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "safe_urls: false\nenable_resume: false\n")
    assert "URL sanitization enabled" not in text
    assert "Resume capability enabled" not in text


def test_defaults_still_reported_without_yaml_overrides(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "")
    assert "SAFE MODE" in text
    assert "URL sanitization enabled" in text
    assert "Resume capability enabled" in text


def test_yaml_value_shows_in_header(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "bandwidth_limit: 12.5\n")
    assert "Bandwidth limit: 12.5 MiB/s" in text


def test_yaml_backend_and_hash_setting_show_once_in_header(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "backend: aiohttp\nverify_content: true\n")
    assert text.count("Backend: aiohttp") == 1
    assert text.count("SHA-256: enabled (local receipts)") == 1
    assert text.count("SAFE MODE") == 1
    assert text.count("URL sanitization enabled") == 1


def test_cli_backend_and_hash_settings_override_yaml_in_header(monkeypatch, tmp_path):
    text = _run(
        monkeypatch,
        tmp_path,
        "backend: aiohttp\nverify_content: true\n",
        ["--backend", "httpx", "--no-verify-content"],
    )
    assert "Backend: httpx" in text and "Backend: aiohttp" not in text
    assert "SHA-256: disabled" in text and "SHA-256: enabled" not in text


def test_shared_header_reports_the_selected_destination(monkeypatch, tmp_path):
    text = _run(monkeypatch, tmp_path, "dir_suffix: nested\n")
    assert "Processing directory suffix: 'nested'" in text
    assert f"Destination path: {(tmp_path / 'dest/nested').resolve()}" in text
