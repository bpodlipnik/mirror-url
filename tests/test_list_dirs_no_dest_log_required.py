"""Regression test: ``--list-dirs`` must not require ``--dest-path`` /
``--log-path`` when ``--config`` is not used.

Bug history: the plain-CLI (no ``--config``) validation branch in
``main()`` unconditionally called ``parser.error(...)`` for a missing
``--dest-path`` and a missing ``--log-path``, with no exception for
``--list-dirs`` -- even though ``list_directories()`` (see
``test_list_dirs.py``) never writes to ``dest_path`` and only uses
``log_path`` for its own run log / cache-file bookkeeping. This made
``mirror-url --url ... --list-dirs`` unusable without also inventing
throwaway ``--dest-path``/``--log-path`` values, contradicting the
flag's own stated purpose of a lightweight, read-only "what's out
there" probe.

Fixed by skipping both required-argument checks when ``args.list_dirs``
is set and defaulting the two paths to a scratch directory under the
system temp dir instead.

Following the convention in ``test_list_dirs_cli_threading.py``, this
stubs out ``MirrorURL`` entirely rather than driving real HTTP, so the
test only exercises argument parsing/validation and never touches the
network.
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

    def __enter__(self):  # pragma: no cover - never reached, raise happens in __init__
        return self

    def __exit__(self, *exc_info):  # pragma: no cover
        return False


@pytest.fixture(autouse=True)
def _reset_captured_configs():
    _CapturingMirrorStub.captured_configs = []
    yield
    _CapturingMirrorStub.captured_configs = []


def test_list_dirs_without_dest_or_log_path_does_not_exit_early(monkeypatch, capsys):
    """The old bug: this invocation used to die in argparse validation
    with 'error: --dest-path is required ...' / 'error: --log-path is
    required ...' before ever reaching MirrorConfig/MirrorURL."""
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--url", "https://example.test/data/", "--list-dirs"]
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    # main() still exits non-zero here because the stub raises for every
    # suffix (there's nothing real to connect to) -- SystemExit(2) is the
    # argparse failure we're guarding against; anything else means
    # argument validation was cleared and the run proceeded normally.
    assert exc_info.value.code != 2

    captured = capsys.readouterr()
    assert "--dest-path is required" not in captured.err
    assert "--log-path is required" not in captured.err

    # We must have actually reached MirrorConfig construction.
    assert len(_CapturingMirrorStub.captured_configs) == 1


def test_list_dirs_defaults_dest_and_log_path_to_temp_scratch_dir(monkeypatch):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--url", "https://example.test/data/", "--list-dirs"]
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    assert len(_CapturingMirrorStub.captured_configs) == 1
    config = _CapturingMirrorStub.captured_configs[0]

    expected = Path(tempfile.gettempdir()) / "mirror-url-list-dirs"
    assert config.dest_path == expected
    assert config.log_path == expected


def test_list_dirs_still_honors_explicit_dest_and_log_path(monkeypatch, tmp_path):
    """An explicitly supplied --dest-path/--log-path must win over the
    scratch-dir default, not be silently overridden."""
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
            "--list-dirs",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
        ],
    )

    with pytest.raises(SystemExit):
        cli_module.main()

    assert len(_CapturingMirrorStub.captured_configs) == 1
    config = _CapturingMirrorStub.captured_configs[0]
    assert config.dest_path == dest.resolve() or config.dest_path == dest
    assert config.log_path == logs.resolve() or config.log_path == logs


def test_missing_url_still_errors_regardless_of_list_dirs(monkeypatch, capsys):
    """--list-dirs must not accidentally waive the --url requirement."""
    monkeypatch.setattr(sys, "argv", ["mirror-url", "--list-dirs"])

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "--url is required" in captured.err


def test_sync_mode_without_list_dirs_still_requires_dest_and_log_path(monkeypatch, capsys):
    """Guard the other side of the fix: normal (non list-dirs) runs must
    still be rejected without --dest-path/--log-path."""
    monkeypatch.setattr(sys, "argv", ["mirror-url", "--url", "https://example.test/data/"])

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "--dest-path is required" in captured.err
