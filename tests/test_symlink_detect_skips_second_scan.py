"""Regression test for ScanMixin.get_remote_files()'s --symlink-mode
detect short-circuit.

Background: the fix in test_symlink_detect_skips_compare.py stopped
sync() from running the per-file dry-run compare after detect mode's
directory survey finished -- but that fix sits one layer too high.
Borut reported that runs were still slow after it, and tracing the log
showed why: get_remote_files() itself does a SECOND full pass over
every discovered directory (scan.py's `for i, url in
enumerate(directories): files, subdirs =
self.scanner.scan_directory_sequential(url)` loop) to build the
complete remote_files list and per-directory content signatures for
the download/cache pipeline -- all of which detect mode discards
immediately, since sync() returns right after get_remote_files()
comes back. Every directory was therefore being fetched twice: once
inside _discover_directories_bfs() (where symlink detection and the
"🔗 Symlink detected" logging happens), and again in this second loop,
for a 968-directory real-world tree that meant two multi-minute
passes for a mode whose only output is the log lines from the first
one.

This locks in the real fix: get_remote_files() must call
scan_directory_sequential() exactly once per directory (not twice)
when handle_symlinks + symlink_mode="detect", and must still report a
files-found count via the files_discovered_during_scan metric tallied
during the (single) BFS pass, not by re-scanning to rebuild the full
list.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.scan import ScanMixin
from mirror_url.enums import MemoryPressure

ROOT = "https://example.test/data/"


class _FakeScanner:
    def __init__(self, tree: dict[str, tuple[list[str], list[str]]]):
        self.tree = tree
        self.call_count = 0

    def scan_directory_sequential(self, url: str):
        self.call_count += 1
        return self.tree.get(url, ([], []))


class _FakeMetrics:
    def __init__(self):
        self.metrics: dict[str, int] = {}

    def increment(self, metric: str, value: int = 1) -> None:
        self.metrics[metric] = self.metrics.get(metric, 0) + value

    def add_error(self, *args, **kwargs) -> None:
        pass


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what
    get_remote_files() and _discover_directories_bfs() touch."""

    def __init__(self, tree, handle_symlinks, symlink_mode="skip"):
        self.target_base_url = ROOT
        self.connection_ok = True
        self.scanner = _FakeScanner(tree)
        self.config = SimpleNamespace(
            max_depth=50,
            exclude_dirs=[],
            handle_symlinks=handle_symlinks,
            symlink_mode=symlink_mode,
            no_cache=True,
            dry_run=True,
            progress_bar=False,
        )
        self.per_ip_limiter = SimpleNamespace(wait=lambda ip: None)
        self.scan_incomplete = False
        self.metrics = _FakeMetrics()
        self.symlink_tracker = None
        self.cache_manager = SimpleNamespace(load=lambda: (False, {}), save=lambda *a, **kw: None)
        self.multi_progress = SimpleNamespace(
            add_level=lambda *a, **kw: None, update=lambda *a, **kw: None
        )
        self.memory_monitor = SimpleNamespace(check_pressure=lambda: MemoryPressure.NORMAL)

    def _get_prefix(self) -> str:
        return ""

    def _is_dir_excluded(self, url: str) -> bool:
        return False

    def _is_within_target_scope(self, url: str) -> bool:
        return True

    def get_directory_signature(self, url: str) -> str:
        return "sig"


def _flat_tree_with_files():
    """ROOT -> {A/, B/}, each with 2 files -- no symlink duplication,
    just enough shape to count scan calls precisely."""
    return {
        ROOT: ([], [ROOT + "A/", ROOT + "B/"]),
        ROOT + "A/": ([ROOT + "A/f1.txt", ROOT + "A/f2.txt"], []),
        ROOT + "B/": ([ROOT + "B/f1.txt", ROOT + "B/f2.txt"], []),
    }


def test_detect_mode_scans_each_directory_only_once():
    """The actual bug: without the fix, every directory gets scanned
    twice (once in BFS, once in get_remote_files()'s second loop)."""
    tree = _flat_tree_with_files()
    mirror = _StubMirror(tree, handle_symlinks=True, symlink_mode="detect")

    result = mirror.get_remote_files()

    assert result == []
    # 3 directories (ROOT, A/, B/) scanned exactly once each -- not twice.
    assert mirror.scanner.call_count == 3


def test_detect_mode_still_reports_a_files_found_count_via_metrics():
    """Even though the second pass is skipped, the files-found count
    must still be available for sync()'s SYMLINK DETECT SUMMARY --
    sourced from the tally _discover_directories_bfs() already keeps
    from data it fetched anyway, not from re-scanning."""
    tree = _flat_tree_with_files()
    mirror = _StubMirror(tree, handle_symlinks=True, symlink_mode="detect")

    mirror.get_remote_files()

    assert mirror.metrics.metrics.get("files_discovered_during_scan") == 4


def test_non_detect_mode_still_scans_each_directory_twice():
    """Negative control: normal (non-detect) runs still need the full
    remote_files list and per-directory signatures for the download/
    compare/cache pipeline, so the second pass is intentional there and
    must be unaffected by this fix."""
    tree = _flat_tree_with_files()
    mirror = _StubMirror(tree, handle_symlinks=False, symlink_mode="skip")

    result = mirror.get_remote_files()

    assert sorted(result) == sorted(
        [ROOT + "A/f1.txt", ROOT + "A/f2.txt", ROOT + "B/f1.txt", ROOT + "B/f2.txt"]
    )
    # 3 directories scanned twice each = 6.
    assert mirror.scanner.call_count == 6
