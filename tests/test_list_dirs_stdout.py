"""Tests for ScanMixin.list_directories()'s clean stdout output.

Follow-up to test_list_dirs.py: in addition to the existing logging
(banner, per-directory "📁 <path>" lines, "Listed N of Total directories"
summary -- all subject to --print-logs/--quiet and routed to the log
file by default), list_directories() also prints each discovered
directory as a bare path to stdout, one per line, with no icon/prefix,
followed by a "# Directories N/total" comment line -- mirroring
--list-files' "# Files N/total" convention -- so the output is
pipeable/scriptable (drop the comment with ``grep -v '^#'``) without
needing --print-logs or having to filter log noise out of it.

Same stubbing convention as test_list_dirs.py: build a ScanMixin
instance directly and stub _discover_directories_bfs() rather than
driving real HTTP.
"""

from __future__ import annotations

from mirror_url._core.scan import ScanMixin


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what list_directories uses."""

    def __init__(
        self,
        target_base_url,
        connection_ok=True,
        has_connection_manager=True,
        total_suffixes=1,
        dir_suffix=None,
    ):
        self.target_base_url = target_base_url
        self.connection_ok = connection_ok
        if has_connection_manager:
            self.connection_manager = object()  # presence is all that's checked
        self._discovered = []
        self.total_suffixes = total_suffixes
        self.config = _StubConfig(dir_suffix)

    def _get_prefix(self) -> str:
        return f"[1/{self.total_suffixes}] " if self.total_suffixes > 1 else ""

    def _discover_directories_bfs(self):
        yield from self._discovered


class _StubConfig:
    def __init__(self, dir_suffix):
        self.dir_suffix = dir_suffix


def test_list_directories_prints_bare_paths_to_stdout(capsys):
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = [
        "https://example.test/data/",
        "https://example.test/data/L1/",
        "https://example.test/data/L1/v03/",
        "https://example.test/data/L2/",
    ]

    result = mirror.list_directories()

    assert result is True
    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == [".", "L1", "L1/v03", "L2", "# Directories 4/4"]


def test_list_directories_stdout_has_no_icons_or_suffix_prefix(capsys):
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = ["https://example.test/data/L1/"]

    mirror.list_directories()

    out = capsys.readouterr().out
    assert "📁" not in out
    assert "[1/1]" not in out
    assert "\t" not in out


def test_list_directories_stdout_summary_line_is_comment_prefixed(capsys):
    """The trailing summary must start with '#' so it can be dropped with
    grep -v '^#' for a pure one-directory-per-line stream, same as
    --list-files' '# Files N/total' convention."""
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = ["https://example.test/data/L1/"]

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines[-1].startswith("#")
    assert out_lines[-1] == "# Directories 1/1"


def test_list_directories_stdout_only_summary_when_no_directories(capsys):
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = []

    result = mirror.list_directories()

    assert result is True
    assert capsys.readouterr().out.splitlines() == ["# Directories 0/0"]


def test_list_directories_returns_false_without_connection_manager_prints_nothing(capsys):
    mirror = _StubMirror(target_base_url="https://example.test/data/", has_connection_manager=False)
    assert mirror.list_directories() is False
    assert capsys.readouterr().out == ""


def test_list_directories_returns_false_when_connection_not_ok_prints_nothing(capsys):
    mirror = _StubMirror(target_base_url="https://example.test/data/", connection_ok=False)
    assert mirror.list_directories() is False
    assert capsys.readouterr().out == ""


def test_list_directories_qualifies_stdout_paths_with_suffix_when_multiple_suffixes(capsys):
    """With a single --dir-suffix (or none), stdout stays a bare path.
    With more than one --dir-suffix mirrored in the same run, a bare path
    is ambiguous about which suffix it came from, so each line gets a
    tab-separated suffix column prepended. The trailing summary comment
    line is never suffix-qualified, since it isn't a directory path."""
    mirror = _StubMirror(
        target_base_url="https://example.test/data/L1/v03/",
        total_suffixes=2,
        dir_suffix="L1/v03",
    )
    mirror._discovered = [
        "https://example.test/data/L1/v03/",
        "https://example.test/data/L1/v03/sub/",
    ]

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == ["L1/v03\t.", "L1/v03\tsub", "# Directories 2/2"]


def test_list_directories_single_suffix_run_has_no_qualifier(capsys):
    """total_suffixes == 1 (the common case) must not be qualified, even
    if a --dir-suffix happens to be set."""
    mirror = _StubMirror(
        target_base_url="https://example.test/data/L1/v03/",
        total_suffixes=1,
        dir_suffix="L1/v03",
    )
    mirror._discovered = ["https://example.test/data/L1/v03/"]

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == [".", "# Directories 1/1"]
