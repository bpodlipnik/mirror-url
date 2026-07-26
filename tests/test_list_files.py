"""Tests for ScanMixin.list_files() (--list-files).

--list-files discovers the files under the target URL / --dir-suffix and
prints them, without comparing freshness or downloading/deleting anything.
Unlike --list-dirs it respects --filter (files already come pre-filtered
from scan_directory_sequential, exactly as a real sync would see them).

With --list-files N, only the last N files per directory are shown, sorted
lexicographically by relative path (a name sort, not a timestamp sort --
see the list_files() docstring for why).

Following the convention already used in test_list_dirs.py, this builds a
ScanMixin instance directly and stubs out both _discover_directories_bfs()
and the scanner, rather than driving real HTTP through the SSRF-guarded
connection layer (see test_integration.py's module docstring for why a
live loopback server isn't used here).
"""

from __future__ import annotations

import logging

import pytest

from mirror_url._core.scan import ScanMixin


class _StubConfig:
    def __init__(self, dir_suffix=None, list_files_n=0):
        self.dir_suffix = dir_suffix
        self.list_files_n = list_files_n


class _StubScanner:
    """Maps a directory URL to its (files, subdirs) tuple, like the real
    DirectoryScanner.scan_directory_sequential() but without any HTTP."""

    def __init__(self, tree):
        self.tree = tree

    def scan_directory_sequential(self, url):
        return self.tree.get(url, ([], []))


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what list_files() uses."""

    def __init__(
        self,
        target_base_url,
        tree,
        connection_ok=True,
        has_connection_manager=True,
        list_files_n=0,
        dir_suffix=None,
        total_suffixes=1,
    ):
        self.target_base_url = target_base_url
        self.connection_ok = connection_ok
        if has_connection_manager:
            self.connection_manager = object()  # presence is all that's checked
        self.config = _StubConfig(dir_suffix=dir_suffix, list_files_n=list_files_n)
        self.scanner = _StubScanner(tree)
        self.total_suffixes = total_suffixes
        self._discovered = list(tree.keys())

    def _get_prefix(self) -> str:
        return ""

    def _discover_directories_bfs(self):
        yield from self._discovered


ROOT = "https://example.test/data/"

TREE = {
    ROOT + "v03/orbit_0042/": (
        [
            ROOT + "v03/orbit_0042/file_20260720_001.fits",
            ROOT + "v03/orbit_0042/file_20260721_002.fits",
            ROOT + "v03/orbit_0042/file_20260722_003.fits",
        ],
        [],
    ),
    ROOT + "v03/orbit_0043/": (
        [
            ROOT + "v03/orbit_0043/file_20260724_010.fits",
            ROOT + "v03/orbit_0043/file_20260725_011.fits",
        ],
        [],
    ),
    ROOT + "v03/empty_dir/": ([], []),
}


def test_list_files_no_limit_prints_all_files_relative_paths(capsys, caplog):
    mirror = _StubMirror(ROOT, TREE, list_files_n=0)

    with caplog.at_level(logging.INFO):
        result = mirror.list_files()

    assert result is True
    out = capsys.readouterr().out.splitlines()
    assert "v03/orbit_0042/file_20260720_001.fits" in out
    assert "v03/orbit_0042/file_20260721_002.fits" in out
    assert "v03/orbit_0042/file_20260722_003.fits" in out
    assert "v03/orbit_0043/file_20260724_010.fits" in out
    assert "v03/orbit_0043/file_20260725_011.fits" in out
    # Comment line always present, even without a limit.
    assert "# Files 3/3" in out
    assert "# Files 2/2" in out
    messages = [r.message for r in caplog.records]
    assert any("Listed 5 of 5 files" in m for m in messages)


def test_list_files_limit_keeps_lexicographically_last_n_per_directory(capsys):
    mirror = _StubMirror(ROOT, TREE, list_files_n=2)

    result = mirror.list_files()
    assert result is True

    out = capsys.readouterr().out.splitlines()
    # orbit_0042 has 3 files -- only the last 2 (lexicographically) should show.
    assert "v03/orbit_0042/file_20260720_001.fits" not in out
    assert "v03/orbit_0042/file_20260721_002.fits" in out
    assert "v03/orbit_0042/file_20260722_003.fits" in out
    assert "# Files 2/3" in out
    # orbit_0043 has only 2 files -- limit of 2 shows both, unchanged.
    assert "v03/orbit_0043/file_20260724_010.fits" in out
    assert "v03/orbit_0043/file_20260725_011.fits" in out
    assert "# Files 2/2" in out


def test_list_files_ordering_is_lexicographic_not_discovery_order(capsys):
    """Files are sorted before truncation, regardless of the order the
    scanner happened to return them in (HTML link order is not guaranteed)."""
    shuffled_tree = {
        ROOT + "v03/orbit_0042/": (
            [
                ROOT + "v03/orbit_0042/file_20260722_003.fits",
                ROOT + "v03/orbit_0042/file_20260720_001.fits",
                ROOT + "v03/orbit_0042/file_20260721_002.fits",
            ],
            [],
        ),
    }
    mirror = _StubMirror(ROOT, shuffled_tree, list_files_n=1)
    mirror.list_files()

    out = capsys.readouterr().out.splitlines()
    assert out[0] == "v03/orbit_0042/file_20260722_003.fits"
    assert out[1] == "# Files 1/3"


def test_list_files_skips_empty_directories(capsys):
    mirror = _StubMirror(ROOT, TREE, list_files_n=0)
    mirror.list_files()

    out = capsys.readouterr().out
    # empty_dir contributes nothing -- no stray "# Files 0/0" block.
    assert "# Files 0/0" not in out


def test_list_files_multi_suffix_qualifies_stdout_lines(capsys):
    mirror = _StubMirror(ROOT, TREE, list_files_n=0, dir_suffix="L2/v03", total_suffixes=2)
    mirror.list_files()

    out = capsys.readouterr().out.splitlines()
    assert "L2/v03\tv03/orbit_0042/file_20260720_001.fits" in out
    # Comment lines are never suffix-qualified -- they're not file paths.
    assert "# Files 3/3" in out


def test_list_files_no_connection_manager_returns_false(caplog):
    mirror = _StubMirror(ROOT, TREE, has_connection_manager=False)

    with caplog.at_level(logging.WARNING):
        result = mirror.list_files()

    assert result is False
    assert any("Skipping --list-files" in r.message for r in caplog.records)


def test_list_files_connection_not_ok_returns_false(caplog):
    mirror = _StubMirror(ROOT, TREE, connection_ok=False)

    with caplog.at_level(logging.INFO):
        result = mirror.list_files()

    assert result is False
    assert any("Skipping --list-files" in r.message for r in caplog.records)


def test_list_files_scan_error_is_skipped_not_fatal(capsys, caplog):
    class _FailingScanner(_StubScanner):
        def scan_directory_sequential(self, url):
            if "orbit_0042" in url:
                raise RuntimeError("boom")
            return super().scan_directory_sequential(url)

    mirror = _StubMirror(ROOT, TREE, list_files_n=0)
    mirror.scanner = _FailingScanner(TREE)

    with caplog.at_level(logging.WARNING):
        result = mirror.list_files()

    assert result is True
    out = capsys.readouterr().out
    assert "orbit_0042" not in out
    assert "v03/orbit_0043/file_20260724_010.fits" in out
    messages = [r.message for r in caplog.records]
    assert any("Error scanning" in m for m in messages)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
