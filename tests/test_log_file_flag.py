"""Tests for --log-file: the flag rename from --log_file, and the missing
separator bug in the resulting filename.

Two independent bugs, reported together from a real script
(mirror_url_lasco_ql.sh) trying to use this flag for the first time:

1. The flag was spelled ``--log_file`` (underscore) -- the only CLI flag in
   the whole tool that didn't use a hyphen -- and was undocumented in
   USER_GUIDE.md. Renamed to ``--log-file``; argparse's default dest
   derivation means ``args.log_file`` (the Python attribute) is unchanged,
   so nothing downstream needed to change.

2. ``setup_shared_logging()`` built the filename as
   ``f"{args.log_file}{suffixes_str}_{timestamp}.log"`` -- no separator
   between the custom prefix and the dir-suffix portion. With
   ``--log-file mirror_url_lasco_ql_nrl --dir-suffix 260727`` this produced
   ``mirror_url_lasco_ql_nrl260727_...log`` (prefix and suffix jammed
   together) instead of the intended
   ``mirror_url_lasco_ql_nrl_260727_...log``.

All tests here drive the real argument parser through ``main()`` (there is
no standalone parser-factory function to call directly) with ``MirrorURL``
stubbed out, so ``setup_shared_logging()`` runs against a fully realistic
``argparse.Namespace`` rather than a hand-built one that would need to
track every attribute the function happens to touch.
"""

from __future__ import annotations

import logging
import sys

import pytest

from mirror_url import cli as cli_module


class _CapturingMirrorStub:
    """Stand-in for MirrorURL that records the config it was built with
    and then bails out immediately -- see test_list_files_no_dest_log_required.py
    for the same convention used elsewhere in this suite."""

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


@pytest.fixture(autouse=True)
def _restore_logging_handlers():
    """setup_shared_logging() mutates the root logger's handlers as a
    side effect -- restore them so this test file doesn't leak state into
    whatever runs after it in the same pytest session."""
    original = logging.root.handlers[:]
    original_level = logging.root.level
    yield
    for h in logging.root.handlers[:]:
        logging.root.removeHandler(h)
        try:
            h.close()
        except Exception:
            pass
    for h in original:
        logging.root.addHandler(h)
    logging.root.setLevel(original_level)


def _run_main(monkeypatch, argv_tail):
    monkeypatch.setattr(cli_module, "MirrorURL", _CapturingMirrorStub)
    monkeypatch.setattr(sys, "argv", ["mirror-url"] + argv_tail)
    with pytest.raises(SystemExit):
        cli_module.main()


def test_log_file_documented_in_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["mirror-url", "--help"])

    with pytest.raises(SystemExit):
        cli_module.main()

    help_text = capsys.readouterr().out
    assert "--log-file NAME" in help_text
    assert "--log_file" not in help_text


def test_old_underscore_spelling_is_rejected(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["mirror-url", "--url", "https://example.test/", "--log_file", "custom_prefix"],
    )

    with pytest.raises(SystemExit) as exc_info:
        cli_module.main()

    assert exc_info.value.code == 2
    captured = capsys.readouterr()
    assert "unrecognized arguments" in captured.err or "unrecognized arguments" in captured.out


def test_single_dir_suffix_produces_correctly_separated_filename(monkeypatch, tmp_path):
    """The exact reported scenario: --log-file with one --dir-suffix must
    separate the custom prefix from the suffix with an underscore, not
    jam them together."""
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            "260727",
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )

    log_files = list(logs.glob("*.log"))
    assert len(log_files) == 1
    name = log_files[0].name
    assert name.startswith("mirror_url_lasco_ql_nrl_260727_")
    assert "nrl260727" not in name  # the original bug: no separator at all
    assert name.endswith(".log")

    config = _CapturingMirrorStub.captured_configs[0]
    assert config.use_shared_log is True


def test_multiple_dir_suffixes_all_underscore_joined(monkeypatch, tmp_path):
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            "260725",
            "260726",
            "260727",
            "--log-file",
            "mirror_url_lasco_ql_nascom",
            "--dry-run",
        ],
    )

    log_files = list(logs.glob("*.log"))
    assert len(log_files) == 1
    assert log_files[0].name.startswith("mirror_url_lasco_ql_nascom_260725_260726_260727_")


def test_no_dir_suffix_falls_back_to_all(monkeypatch, tmp_path):
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )

    log_files = list(logs.glob("*.log"))
    assert len(log_files) == 1
    assert log_files[0].name.startswith("mirror_url_lasco_ql_nrl_all_")


def test_without_log_file_uses_default_per_suffix_naming(monkeypatch, tmp_path):
    """Backward compatibility: omitting --log-file must not change
    anything about the existing default naming scheme, and must not
    trigger setup_shared_logging() at all."""
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            "260727",
            "--dry-run",
        ],
    )

    # setup_shared_logging() only runs when --log-file is given, so no log
    # file should exist yet at this point in the (stubbed-out) run.
    assert list(logs.glob("*.log")) == []
    config = _CapturingMirrorStub.captured_configs[0]
    assert config.use_shared_log is False


def test_many_dir_suffixes_do_not_produce_a_too_long_filename(monkeypatch, tmp_path):
    """The exact reported crash: a full month of daily --dir-suffix values
    (31 entries) joined with underscores, plus --log-file, previously
    produced a filename well over the filesystem's limit (typically 255
    bytes), crashing with OSError: [Errno 36] File name too long before
    the run even started. The summarized filename must stay well under
    that limit and the log file must actually get created."""
    from mirror_url.constants import MAX_FILENAME_LENGTH

    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    july_days = [f"2607{d:02d}" for d in range(1, 32)]  # 31 suffixes, the real scenario

    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            *july_days,
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )

    log_files = list(logs.glob("*.log"))
    assert len(log_files) == 1, "log file was never created -- the crash reproduced"
    name = log_files[0].name
    assert len(name) <= MAX_FILENAME_LENGTH
    assert name.startswith("mirror_url_lasco_ql_nrl_")


def test_summarized_filename_previews_first_suffixes_and_counts_the_rest(monkeypatch, tmp_path):
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()
    july_days = [f"2607{d:02d}" for d in range(1, 32)]

    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            *july_days,
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )

    name = list(logs.glob("*.log"))[0].name
    assert "260701_260702_260703" in name  # first 3 suffixes previewed
    assert "plus28more" in name  # 31 total - 3 previewed = 28 remaining
    assert "260731" not in name  # the tail of the list is summarized away, not spelled out


def test_different_large_suffix_sets_never_collide_on_filename(monkeypatch, tmp_path):
    """Two different large --dir-suffix sets sharing the same first-3
    preview must still produce different filenames -- the hash covers
    the *full* sorted suffix list, not just the previewed prefix."""
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()

    set_a = [f"2607{d:02d}" for d in range(1, 32)]  # July 1-31
    set_b = [f"2607{d:02d}" for d in range(1, 31)]  # July 1-30 (one fewer, same first 3)

    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            *set_a,
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )
    name_a = list(logs.glob("*.log"))[0].name

    for f in logs.glob("*.log"):
        f.unlink()

    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            *set_b,
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )
    name_b = list(logs.glob("*.log"))[0].name

    assert name_a != name_b


def test_small_suffix_lists_are_unaffected_by_the_length_guard(monkeypatch, tmp_path):
    """Sanity check that the fix is opt-in only when actually needed --
    a handful of suffixes must still produce the full, unsummarized,
    human-readable filename exactly as before (see
    test_multiple_dir_suffixes_all_underscore_joined above)."""
    dest = tmp_path / "dest"
    logs = tmp_path / "logs"
    logs.mkdir()

    _run_main(
        monkeypatch,
        [
            "--url",
            "https://example.test/data/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--dir-suffix",
            "260801",
            "260802",
            "260803",
            "--log-file",
            "mirror_url_lasco_ql_nrl",
            "--dry-run",
        ],
    )

    name = list(logs.glob("*.log"))[0].name
    assert name.startswith("mirror_url_lasco_ql_nrl_260801_260802_260803_")
    assert "plus" not in name
    assert "more" not in name
