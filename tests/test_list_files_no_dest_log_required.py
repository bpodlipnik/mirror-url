"""Regression test: ``--list-files`` must not require ``--dest-path`` /
``--log-path`` when ``--config`` is not used, and must correctly thread
both the boolean flag and the optional N value through ``main()``.

Same bug class as ``test_list_dirs_no_dest_log_required.py``: ``list_files()``
never writes to ``dest_path`` and only uses ``log_path`` for its own run
log / cache-file bookkeeping, so it must not force the user to invent
throwaway values for either.

Following the same convention, this stubs out ``MirrorURL`` entirely
rather than driving real HTTP, so the test only exercises argument
parsing/validation and never touches the network.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

from mirror_url import cli as cli_module


class _CapturingMirrorStub:
    """Stand-in for MirrorURL that records the config it was built with
    and then bails out immediately, so main() never touches the network
    or the filesystem beyond what argument validation already did."""

    captured_configs: list = []

    def __init__(self, config, suffix_index=1, total_suffixes=1):
        type(self).captured_configs.append(config)
        raise RuntimeError("stub: intentionally never connects")

    def __enter__(self):  # pragma: no cover
        return self

    def __exit__(self, *exc_info):  # pragma: no cover
        return False


@pytest.fixture(autouse=True)
def _reset_captured_configs():
    _CapturingMirrorStub.captured_configs = []
    yield
    _CapturingMirrorStub.captured_configs = []


def test_list_files_without_dest_or_log_path_does_not_exit_early(monkeypatch, capsys):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--url", "https://example.test/data/", "--list-files"]
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code != 2

    captured = capsys.readouterr()
    assert "--dest-path is required" not in captured.err
    assert "--log-path is required" not in captured.err

    assert len(_CapturingMirrorStub.captured_configs) == 1


def test_list_files_defaults_dest_and_log_path_to_temp_scratch_dir(monkeypatch):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--url", "https://example.test/data/", "--list-files"]
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    assert len(_CapturingMirrorStub.captured_configs) == 1
    config = _CapturingMirrorStub.captured_configs[0]

    expected = Path(tempfile.gettempdir()) / "mirror-url-list-dirs"
    assert config.dest_path == expected
    assert config.log_path == expected


def test_list_files_without_n_sets_list_files_true_and_n_zero(monkeypatch):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--url", "https://example.test/data/", "--list-files"]
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    config = _CapturingMirrorStub.captured_configs[0]
    assert config.list_files is True
    assert config.list_files_n == 0


def test_list_files_with_n_threads_n_through(monkeypatch):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys,
        "argv",
        ["mirror-url", "--url", "https://example.test/data/", "--list-files", "5"],
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    config = _CapturingMirrorStub.captured_configs[0]
    assert config.list_files is True
    assert config.list_files_n == 5


def test_list_files_still_honors_explicit_dest_and_log_path(monkeypatch, tmp_path):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            "https://example.test/data/",
            "--list-files",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
        ],
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    config = _CapturingMirrorStub.captured_configs[0]
    assert config.dest_path == dest.resolve() or config.dest_path == dest
    assert config.log_path == logs.resolve() or config.log_path == logs


def test_list_dirs_and_list_files_together_is_a_usage_error(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            "https://example.test/data/",
            "--list-dirs",
            "--list-files",
        ],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "mutually exclusive" in captured.err


def test_sync_mode_without_list_files_still_requires_dest_and_log_path(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mirror-url", "--url", "https://example.test/data/"])

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "--dest-path is required" in captured.err
