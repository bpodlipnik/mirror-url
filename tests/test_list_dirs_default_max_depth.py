"""Tests for --list-dirs' shallower default --max-depth.

Bug/UX report: ``mirror-url --url ... --list-dirs --quiet`` against a
tree with several levels (date directories, each containing further
c2/c3/c4 subdirectories) recursed the full MAX_DIRECTORY_DEPTH (50) by
default, printing every nested subdirectory instead of just the current
folder's immediate children -- surprising for what's overwhelmingly a
"what's in this folder" probe.

Fixed in cli.py: --max-depth's argparse default became the sentinel
None (instead of MAX_DIRECTORY_DEPTH) so we can tell whether the user
passed it explicitly. Right after parsing, args.max_depth is resolved
to LIST_DIRS_DEFAULT_MAX_DEPTH (1) when --list-dirs is set and no
explicit --max-depth was given, or MAX_DIRECTORY_DEPTH otherwise
(unchanged for real syncs and --list-files). An explicit --max-depth
always wins, for every mode including --list-dirs.

Following the convention in test_list_dirs_cli_threading.py /
test_list_dirs_no_dest_log_required.py, this stubs out MirrorURL
entirely rather than driving real HTTP.
"""

from __future__ import annotations

import sys

import pytest

from mirror_url import cli as cli_module
from mirror_url.constants import LIST_DIRS_DEFAULT_MAX_DEPTH, MAX_DIRECTORY_DEPTH


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


def _run(monkeypatch, argv):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit):
        cli_module.main()
    return _CapturingMirrorStub.captured_configs[0]


def test_list_dirs_without_explicit_max_depth_defaults_to_one(monkeypatch):
    config = _run(monkeypatch, ["mirror-url", "--url", "https://example.test/data/", "--list-dirs"])
    assert config.max_depth == LIST_DIRS_DEFAULT_MAX_DEPTH == 1


def test_list_dirs_with_explicit_max_depth_is_not_overridden(monkeypatch):
    config = _run(
        monkeypatch,
        [
            "mirror-url",
            "--url",
            "https://example.test/data/",
            "--list-dirs",
            "--max-depth",
            "5",
        ],
    )
    assert config.max_depth == 5


def test_list_dirs_n_without_explicit_max_depth_still_defaults_to_one(monkeypatch):
    """The shallow default applies regardless of whether --list-dirs is
    given bare or with an N."""
    config = _run(
        monkeypatch, ["mirror-url", "--url", "https://example.test/data/", "--list-dirs", "3"]
    )
    assert config.max_depth == LIST_DIRS_DEFAULT_MAX_DEPTH == 1


def test_list_files_without_explicit_max_depth_keeps_full_default(monkeypatch):
    """Only --list-dirs gets the shallower default -- --list-files is
    unaffected and keeps recursing the full tree by default, since it's
    typically used to enumerate everything matching --filter."""
    config = _run(
        monkeypatch, ["mirror-url", "--url", "https://example.test/data/", "--list-files"]
    )
    assert config.max_depth == MAX_DIRECTORY_DEPTH


def test_plain_sync_without_explicit_max_depth_keeps_full_default(monkeypatch, tmp_path):
    """A real (non-list) sync run is unaffected -- still defaults to the
    original MAX_DIRECTORY_DEPTH."""
    config = _run(
        monkeypatch,
        [
            "mirror-url",
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "log"),
        ],
    )
    assert config.max_depth == MAX_DIRECTORY_DEPTH
