"""Regression test for ScanMixin._discover_directories_bfs()'s off-by-one
depth check.

Production report: ``mirror-url --url .../level_05/ --list-dirs --quiet``
(which now defaults --max-depth to 1, see test_list_dirs_default_max_depth.py)
took 42s for 271 directories, vs. ~2s for an equivalent single-page curl
fetch of the same listing.

Root cause: the loop popped ``(url, depth)`` and only skipped *scanning*
(an HTTP request via ``scanner.scan_directory_sequential()``) when
``depth > self.config.max_depth`` -- meaning a directory sitting exactly
*at* max_depth still got scanned, purely to discover its children. Those
children land at ``depth + 1``, which already exceeds max_depth, so they
get silently discarded the instant they're popped (via that same
``depth > max_depth`` check) without ever being scanned themselves. The
scan of the max_depth-level directory was therefore 100% wasted network
I/O: one real HTTP request per directory at the deepest listed level,
for a result that was thrown away unused. At the old default of
max_depth=50 this was invisible on typical (shallow) trees; at
--list-dirs's new max_depth=1 default it meant N+1 requests (fetch root,
then fetch every single immediate child's page too) for what should be a
one-request "what's in this folder" probe.

Fix: only scan a directory (and only apply the per-request rate-limit
wait) when ``depth < self.config.max_depth`` -- i.e. only when there's
still depth budget left to use whatever children it might yield. The
directory itself is still yielded (listed) regardless; only the
*network request needed to find its children* is skipped once nothing
would be done with those children anyway.

Building a ScanMixin instance directly with a fake `scanner` that
records every call, following the stubbing convention already used in
test_cleanup_partial_scan.py / test_list_dirs.py (see test_integration.py's
module docstring for why a live loopback server isn't used here).
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.scan import ScanMixin

ROOT = "https://example.test/data/"


class _FakeScanner:
    """Records every scan_directory_sequential() call and returns a
    canned (files, subdirs) tuple per URL from a tree map."""

    def __init__(self, tree: dict[str, list[str]]):
        # tree maps a directory URL -> list of immediate child directory URLs.
        # Every entry contributes no files (only directory structure matters
        # for this test).
        self.tree = tree
        self.calls: list[str] = []

    def scan_directory_sequential(self, url: str):
        self.calls.append(url)
        return [], list(self.tree.get(url, []))


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what
    _discover_directories_bfs() touches."""

    def __init__(self, target_base_url, tree, max_depth, exclude_dirs=None):
        self.target_base_url = target_base_url
        self.connection_ok = True
        self.scanner = _FakeScanner(tree)
        self.config = SimpleNamespace(max_depth=max_depth, exclude_dirs=exclude_dirs or [])
        self.per_ip_limiter = SimpleNamespace(wait=lambda ip: None)
        self.scan_incomplete = False

    def _get_prefix(self) -> str:
        return ""

    def _is_dir_excluded(self, url: str) -> bool:
        return False


def _three_level_tree():
    """root -> {A, B} -> each has one child (A/sub, B/sub) -> each of
    those has one grandchild (A/sub/leaf, B/sub/leaf)."""
    return {
        ROOT: [ROOT + "A/", ROOT + "B/"],
        ROOT + "A/": [ROOT + "A/sub/"],
        ROOT + "B/": [ROOT + "B/sub/"],
        ROOT + "A/sub/": [ROOT + "A/sub/leaf/"],
        ROOT + "B/sub/": [ROOT + "B/sub/leaf/"],
    }


def test_max_depth_1_scans_only_the_root_not_every_child(monkeypatch):
    """The exact bug: with max_depth=1, only the root should ever be
    fetched. Every immediate child must still be *yielded* (listed),
    but never scanned, since their would-be children (depth 2) are
    already past max_depth and get discarded regardless."""
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    mirror = _StubMirror(ROOT, _three_level_tree(), max_depth=1)

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/", ROOT + "B/"])
    assert mirror.scanner.calls == [ROOT], (
        f"expected exactly one scan (the root only), got {mirror.scanner.calls} -- "
        "children at depth == max_depth must be yielded without being fetched"
    )


def test_max_depth_2_scans_root_and_depth_1_not_depth_2(monkeypatch):
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    mirror = _StubMirror(ROOT, _three_level_tree(), max_depth=2)

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted(
        [ROOT, ROOT + "A/", ROOT + "B/", ROOT + "A/sub/", ROOT + "B/sub/"]
    )
    assert sorted(mirror.scanner.calls) == sorted([ROOT, ROOT + "A/", ROOT + "B/"]), (
        "depth-2 directories (A/sub, B/sub) must be yielded but not scanned"
    )


def test_max_depth_50_default_style_still_scans_every_yielded_directory_but_last(monkeypatch):
    """Sanity check against the old (pre-fix) default: with max_depth
    comfortably deeper than the tree, every directory except the ones at
    the deepest actually-reached level gets scanned -- i.e. this isn't
    just a max_depth=1 special case, the same rule applies generally."""
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    mirror = _StubMirror(ROOT, _three_level_tree(), max_depth=50)

    yielded = list(mirror._discover_directories_bfs())

    all_dirs = [
        ROOT,
        ROOT + "A/",
        ROOT + "B/",
        ROOT + "A/sub/",
        ROOT + "B/sub/",
        ROOT + "A/sub/leaf/",
        ROOT + "B/sub/leaf/",
    ]
    assert sorted(yielded) == sorted(all_dirs)
    # Every directory gets scanned since max_depth (50) is never reached --
    # including the leaves, since scanning them (finding no children) is
    # what proves the tree ends there.
    assert sorted(mirror.scanner.calls) == sorted(all_dirs)


def test_max_depth_0_boundary_scans_nothing_yields_only_root(monkeypatch):
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    mirror = _StubMirror(ROOT, _three_level_tree(), max_depth=0)

    yielded = list(mirror._discover_directories_bfs())

    assert yielded == [ROOT]
    assert mirror.scanner.calls == []


def test_scan_exception_at_boundary_depth_is_not_reached(monkeypatch):
    """If a directory isn't scanned at all (because it's at max_depth),
    a scan exception for it obviously can't fire either -- scan_incomplete
    must stay False."""
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")

    class _RaisingScanner(_FakeScanner):
        def scan_directory_sequential(self, url):
            if url == ROOT + "A/":
                raise RuntimeError("would only be reached if A/ got scanned")
            return super().scan_directory_sequential(url)

    mirror = _StubMirror(ROOT, _three_level_tree(), max_depth=1)
    mirror.scanner = _RaisingScanner(_three_level_tree())

    yielded = list(mirror._discover_directories_bfs())

    assert ROOT + "A/" in yielded
    assert mirror.scan_incomplete is False
