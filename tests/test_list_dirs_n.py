"""Tests for ScanMixin.list_directories()'s optional N truncation
(--list-dirs [N]).

Follow-up to test_list_dirs.py / test_list_dirs_stdout.py, which cover
the unrestricted (no-N) path and are left untouched by this feature --
those stub configs don't set list_dirs_n at all, so getattr(...,
"list_dirs_n", 0) falls back to 0 and the original streaming behavior
is exercised exactly as before.

Unlike --list-files [N], which ranks the last N files *per directory*
(files are naturally grouped by their containing directory),
--list-dirs [N] ranks across the *entire* discovered tree for a suffix,
since directories have no equivalent natural grouping -- and always
excludes the root ('.') from that ranking, since it isn't a real
--dir-suffix candidate.

Same stubbing convention as test_list_dirs.py: build a ScanMixin
instance directly and stub _discover_directories_bfs() rather than
driving real HTTP.
"""

from __future__ import annotations

import logging

from mirror_url._core.scan import ScanMixin


class _StubConfig:
    def __init__(self, dir_suffix=None, list_dirs_n=0):
        self.dir_suffix = dir_suffix
        self.list_dirs_n = list_dirs_n


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what list_directories uses."""

    def __init__(
        self,
        target_base_url,
        discovered,
        connection_ok=True,
        has_connection_manager=True,
        total_suffixes=1,
        dir_suffix=None,
        list_dirs_n=0,
    ):
        self.target_base_url = target_base_url
        self.connection_ok = connection_ok
        if has_connection_manager:
            self.connection_manager = object()  # presence is all that's checked
        self._discovered = discovered
        self.total_suffixes = total_suffixes
        self.config = _StubConfig(dir_suffix=dir_suffix, list_dirs_n=list_dirs_n)

    def _get_prefix(self) -> str:
        return f"[1/{self.total_suffixes}] " if self.total_suffixes > 1 else ""

    def _discover_directories_bfs(self):
        yield from self._discovered


ROOT = "https://example.test/data/"
ORBITS = [
    "https://example.test/data/",
    "https://example.test/data/orbit_0041/",
    "https://example.test/data/orbit_0042/",
    "https://example.test/data/orbit_0043/",
    "https://example.test/data/orbit_0044/",
    "https://example.test/data/orbit_0045/",
]


def test_list_dirs_n_keeps_lexicographically_last_n_directories(capsys):
    mirror = _StubMirror(ROOT, ORBITS, list_dirs_n=3)

    result = mirror.list_directories()

    assert result is True
    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == ["orbit_0043", "orbit_0044", "orbit_0045", "# Directories 3/5"]


def test_list_dirs_n_excludes_root_from_ranking(capsys):
    """With only 2 real subdirectories and N=3, the root ('.') must not
    be padded into the result -- it's not a real --dir-suffix candidate."""
    mirror = _StubMirror(
        ROOT,
        [
            "https://example.test/data/",
            "https://example.test/data/orbit_0001/",
            "https://example.test/data/orbit_0002/",
        ],
        list_dirs_n=3,
    )

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert "." not in out_lines
    assert out_lines == ["orbit_0001", "orbit_0002", "# Directories 2/2"]


def test_list_dirs_no_n_keeps_discovery_order_unrestricted(capsys):
    """N=0 (the default) must remain exactly the pre-existing behavior:
    every directory, in discovery order, root included."""
    mirror = _StubMirror(ROOT, ORBITS, list_dirs_n=0)

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == [
        ".",
        "orbit_0041",
        "orbit_0042",
        "orbit_0043",
        "orbit_0044",
        "orbit_0045",
        "# Directories 6/6",
    ]


def test_list_dirs_n_ordering_is_lexicographic_not_discovery_order(capsys):
    shuffled = [
        "https://example.test/data/",
        "https://example.test/data/orbit_0045/",
        "https://example.test/data/orbit_0041/",
        "https://example.test/data/orbit_0043/",
    ]
    mirror = _StubMirror(ROOT, shuffled, list_dirs_n=2)

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == ["orbit_0043", "orbit_0045", "# Directories 2/3"]


def test_list_dirs_n_larger_than_available_shows_all_non_root(capsys):
    mirror = _StubMirror(ROOT, ORBITS, list_dirs_n=100)

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == [
        "orbit_0041",
        "orbit_0042",
        "orbit_0043",
        "orbit_0044",
        "orbit_0045",
        "# Directories 5/5",
    ]


def test_list_dirs_n_logs_shown_of_total_summary(caplog):
    mirror = _StubMirror(ROOT, ORBITS, list_dirs_n=3)

    with caplog.at_level(logging.INFO):
        mirror.list_directories()

    messages = [r.message for r in caplog.records]
    assert any("Listed 3 of 5 directories" in m for m in messages)


def test_list_dirs_n_multi_suffix_qualifies_stdout_lines(capsys):
    mirror = _StubMirror(
        "https://example.test/data/L1/v03/",
        [
            "https://example.test/data/L1/v03/",
            "https://example.test/data/L1/v03/orbit_0041/",
            "https://example.test/data/L1/v03/orbit_0042/",
        ],
        total_suffixes=2,
        dir_suffix="L1/v03",
        list_dirs_n=1,
    )

    mirror.list_directories()

    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == ["L1/v03\torbit_0042", "# Directories 1/2"]


def test_list_dirs_n_only_summary_when_only_root_discovered(capsys):
    mirror = _StubMirror(ROOT, ["https://example.test/data/"], list_dirs_n=3)

    result = mirror.list_directories()

    assert result is True
    assert capsys.readouterr().out.splitlines() == ["# Directories 0/0"]
