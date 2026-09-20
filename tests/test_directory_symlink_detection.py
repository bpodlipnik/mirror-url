"""Tests for directory-level symlink detection in ScanMixin._discover_directories_bfs().

Background: plain HTTP directory listings (Apache-style autoindex) give no
explicit "this is a symlink" signal -- the server just transparently
resolves the symlink server-side and serves the target directory's
listing under the link's own URL path. Confirmed against a real example
Borut reported: NASA's sohoftp SolarSoft archive has
.../lasco/lasco/ symlinked to .../lasco/idl/, and both URLs return
byte-for-byte equivalent entry listings with no metadata distinguishing
one from the other.

Detection here is therefore a heuristic: compare each newly-scanned
directory's (files, subdirs) basenames against every other non-empty
directory already scanned in the same run. A match is treated as "the
later one is a symlink to the first one seen" -- our only available proxy
given HTTP's lack of a real signal. See _check_directory_symlink()'s
docstring in scan.py for the full rationale.

Desired behavior (Borut's spec):
  - every detected symlink is reported to the log unconditionally
  - a symlink whose target resolves outside the current scan scope
    (--url / --dir-suffix) is always ignored (never descended into),
    regardless of --symlink-mode -- a safety boundary against symlink
    bombs, not a user-tunable choice
  - a symlink whose target is in scope is mirrored/created normally when
    --symlink-mode=follow; ignored when --symlink-mode=skip (the
    default) or treat-as-file (which has no meaningful reading for a
    directory, so it's handled the same as skip)

Stubbing convention follows test_bfs_depth_boundary_scan.py: a minimal
ScanMixin subclass with fakes for everything the method under test
touches, rather than a live server (see test_integration.py's module
docstring for why).
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.scan import ScanMixin
from mirror_url.security import SymlinkTracker

ROOT = "https://example.test/data/"


class _FakeScanner:
    """Returns a canned (files, subdirs) tuple per directory URL."""

    def __init__(self, tree: dict[str, tuple[list[str], list[str]]], headers=None):
        # tree maps a directory URL -> (file URLs, subdir URLs)
        self.tree = tree
        self.calls: list[str] = []
        # url -> (Last-Modified, ETag), mirroring DirectoryScanner's real
        # dir_response_headers -- see test_scanner_header_capture.py for
        # where this actually gets populated in production.
        self.dir_response_headers: dict[str, tuple] = headers or {}

    def scan_directory_sequential(self, url: str):
        self.calls.append(url)
        return self.tree.get(url, ([], []))


class _FakeMetrics:
    def __init__(self):
        self.counts: dict[str, int] = {}

    def increment(self, metric: str, value: int = 1) -> None:
        self.counts[metric] = self.counts.get(metric, 0) + value


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what
    _discover_directories_bfs() and its symlink-detection helpers touch."""

    def __init__(
        self,
        target_base_url,
        tree,
        max_depth=50,
        handle_symlinks=True,
        symlink_mode="skip",
        symlink_tracker=None,
        out_of_scope_urls=frozenset(),
        dir_response_headers=None,
    ):
        self.target_base_url = target_base_url
        self.connection_ok = True
        self.scanner = _FakeScanner(tree, headers=dir_response_headers)
        self.config = SimpleNamespace(
            max_depth=max_depth,
            exclude_dirs=[],
            handle_symlinks=handle_symlinks,
            symlink_mode=symlink_mode,
        )
        self.per_ip_limiter = SimpleNamespace(wait=lambda ip: None)
        self.scan_incomplete = False
        self.metrics = _FakeMetrics()
        self.symlink_tracker = symlink_tracker
        self._out_of_scope_urls = out_of_scope_urls

    def _get_prefix(self) -> str:
        return ""

    def _is_dir_excluded(self, url: str) -> bool:
        return False

    def _is_within_target_scope(self, url: str) -> bool:
        return url not in self._out_of_scope_urls


def _patch_dns(monkeypatch):
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")


def _tree_with_symlinked_sibling():
    """ROOT -> {A/, B/}; A and B contain identically-named entries, so B
    looks like a symlink to A (or vice versa -- BFS visits A first since
    it's discovered/scanned first here)."""
    return {
        ROOT: ([], [ROOT + "A/", ROOT + "B/"]),
        ROOT + "A/": ([ROOT + "A/f1.txt", ROOT + "A/f2.txt"], []),
        ROOT + "B/": ([ROOT + "B/f1.txt", ROOT + "B/f2.txt"], []),
    }


def _tree_with_nested_symlinked_sibling():
    """ROOT -> {A/, B/}; A and B are identical multi-level trees, mirroring
    the real lasco/lasco -> lasco/idl case: one symlink at the top, whose
    entire subtree necessarily duplicates the target's subtree at every
    level. Detection must flag only the top-level pair (B is a duplicate
    of A), not every corresponding descendant pair independently."""
    return {
        ROOT: ([], [ROOT + "A/", ROOT + "B/"]),
        ROOT + "A/": ([], [ROOT + "A/sub1/", ROOT + "A/sub2/"]),
        ROOT + "B/": ([], [ROOT + "B/sub1/", ROOT + "B/sub2/"]),
        ROOT + "A/sub1/": ([ROOT + "A/sub1/f1.txt"], [ROOT + "A/sub1/deep/"]),
        ROOT + "B/sub1/": ([ROOT + "B/sub1/f1.txt"], [ROOT + "B/sub1/deep/"]),
        ROOT + "A/sub1/deep/": ([ROOT + "A/sub1/deep/f2.txt"], []),
        ROOT + "B/sub1/deep/": ([ROOT + "B/sub1/deep/f2.txt"], []),
        ROOT + "A/sub2/": ([ROOT + "A/sub2/f3.txt"], []),
        ROOT + "B/sub2/": ([ROOT + "B/sub2/f3.txt"], []),
    }


# ---------------------------------------------------------------------------
# _dir_entry_signature / _check_directory_symlink unit tests
# ---------------------------------------------------------------------------


def test_signature_matches_regardless_of_url_prefix():
    mirror = _StubMirror(ROOT, {})
    sig_a = mirror._dir_entry_signature([ROOT + "A/f1.txt", ROOT + "A/f2.txt"], [ROOT + "A/sub/"])
    sig_b = mirror._dir_entry_signature([ROOT + "B/f1.txt", ROOT + "B/f2.txt"], [ROOT + "B/sub/"])
    assert sig_a == sig_b


def test_signature_differs_for_different_content():
    mirror = _StubMirror(ROOT, {})
    sig_a = mirror._dir_entry_signature([ROOT + "A/f1.txt"], [])
    sig_b = mirror._dir_entry_signature([ROOT + "A/f1.txt", ROOT + "A/f2.txt"], [])
    assert sig_a != sig_b


def test_check_directory_symlink_flags_second_occurrence():
    mirror = _StubMirror(ROOT, {})
    signatures: dict[str, str] = {}

    is_link_a, target_a = mirror._check_directory_symlink(
        ROOT + "A/", [ROOT + "A/f1.txt"], [], signatures
    )
    assert is_link_a is False
    assert target_a is None

    is_link_b, target_b = mirror._check_directory_symlink(
        ROOT + "B/", [ROOT + "B/f1.txt"], [], signatures
    )
    assert is_link_b is True
    assert target_b == ROOT + "A/"


def test_check_directory_symlink_ignores_empty_directories():
    """Two empty directories must never be flagged as symlinks of each
    other -- every empty dir would otherwise collide trivially."""
    mirror = _StubMirror(ROOT, {})
    signatures: dict[str, str] = {}

    is_link_a, _ = mirror._check_directory_symlink(ROOT + "A/", [], [], signatures)
    is_link_b, _ = mirror._check_directory_symlink(ROOT + "B/", [], [], signatures)

    assert is_link_a is False
    assert is_link_b is False
    assert signatures == {}


def test_confidence_note_high_when_both_headers_match():
    mirror = _StubMirror(
        ROOT,
        {},
        dir_response_headers={
            ROOT + "A/": ("Fri, 28 Feb 2020 12:00:00 GMT", '"abc"'),
            ROOT + "B/": ("Fri, 28 Feb 2020 12:00:00 GMT", '"abc"'),
        },
    )
    note = mirror._symlink_confidence_note(ROOT + "B/", ROOT + "A/")
    assert "high confidence" in note
    assert "Last-Modified & ETag both match" in note


def test_confidence_note_partial_when_only_last_modified_matches():
    mirror = _StubMirror(
        ROOT,
        {},
        dir_response_headers={
            ROOT + "A/": ("Fri, 28 Feb 2020 12:00:00 GMT", '"abc"'),
            ROOT + "B/": ("Fri, 28 Feb 2020 12:00:00 GMT", '"different"'),
        },
    )
    note = mirror._symlink_confidence_note(ROOT + "B/", ROOT + "A/")
    assert "Last-Modified matches" in note
    assert "high confidence" not in note


def test_confidence_note_warns_when_headers_disagree():
    """Basenames matched (that's why this function is even called), but
    if the headers that ARE present flatly disagree, that's worth
    surfacing -- this never suppresses the detection itself (see the
    function's docstring: headers are corroboration, not the decision),
    just flags it for a manual look."""
    mirror = _StubMirror(
        ROOT,
        {},
        dir_response_headers={
            ROOT + "A/": ("Fri, 28 Feb 2020 12:00:00 GMT", '"abc"'),
            ROOT + "B/": ("Mon, 01 Jan 2024 00:00:00 GMT", '"xyz"'),
        },
    )
    note = mirror._symlink_confidence_note(ROOT + "B/", ROOT + "A/")
    assert "differ" in note


def test_confidence_note_empty_string_without_target():
    mirror = _StubMirror(ROOT, {})
    assert mirror._symlink_confidence_note(ROOT + "B/", None) == ""


def test_confidence_note_reports_missing_header_data():
    """Neither side has header data (e.g. served from cache this run,
    so no request happened to read headers from) -- must degrade
    gracefully, not silently claim a match or crash."""
    mirror = _StubMirror(ROOT, {})  # no dir_response_headers at all
    note = mirror._symlink_confidence_note(ROOT + "B/", ROOT + "A/")
    assert "no header data" in note


# ---------------------------------------------------------------------------
# Full _discover_directories_bfs() integration tests
# ---------------------------------------------------------------------------


def test_handle_symlinks_disabled_mirrors_both_dirs_unchanged(monkeypatch):
    """Default/off behavior must be untouched: with --handle-symlinks not
    set, duplicate-content siblings are just mirrored as normal
    directories, exactly like before this feature existed."""
    _patch_dns(monkeypatch)
    mirror = _StubMirror(ROOT, _tree_with_symlinked_sibling(), handle_symlinks=False)

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/", ROOT + "B/"])
    # Symlink-specific metrics must stay untouched -- but
    # files_discovered_during_scan is an always-on, essentially-free
    # tally (see _discover_directories_bfs's docstring) that runs
    # regardless of --handle-symlinks, so it's expected here too.
    assert "symlinks_detected" not in mirror.metrics.counts
    assert "symlinks_skipped" not in mirror.metrics.counts
    assert "symlinks_followed" not in mirror.metrics.counts


def test_symlink_mode_skip_ignores_the_detected_duplicate(monkeypatch):
    """Default --symlink-mode=skip: the detected symlink (B) is reported
    but not yielded/descended, even though its target (A) is in scope."""
    _patch_dns(monkeypatch)
    mirror = _StubMirror(
        ROOT, _tree_with_symlinked_sibling(), handle_symlinks=True, symlink_mode="skip"
    )

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/"])
    assert ROOT + "B/" not in yielded
    assert mirror.metrics.counts.get("symlinks_detected") == 1
    assert mirror.metrics.counts.get("symlinks_skipped") == 1


def test_symlink_mode_treat_as_file_behaves_like_skip_for_directories(monkeypatch):
    _patch_dns(monkeypatch)
    mirror = _StubMirror(
        ROOT,
        _tree_with_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="treat-as-file",
    )

    yielded = list(mirror._discover_directories_bfs())

    assert ROOT + "B/" not in yielded


def test_symlink_mode_follow_creates_in_scope_target(monkeypatch):
    """--symlink-mode=follow with the target inside the current scope:
    the symlinked directory is mirrored/created like any other."""
    _patch_dns(monkeypatch)
    tracker = SymlinkTracker(max_depth=50, max_per_dir=50, bomb_threshold=1000)
    mirror = _StubMirror(
        ROOT,
        _tree_with_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="follow",
        symlink_tracker=tracker,
    )

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/", ROOT + "B/"])
    assert mirror.metrics.counts.get("symlinks_followed") == 1
    assert tracker.get_stats()["total_followed"] == 1


def test_symlink_target_outside_scope_always_ignored_even_in_follow_mode(monkeypatch):
    """Safety boundary: a symlink whose target resolves outside the
    current scan scope is ignored unconditionally, even with
    --symlink-mode=follow -- this is what actually prevents a symlink
    bomb from escaping --dir-suffix/--url scope."""
    _patch_dns(monkeypatch)
    mirror = _StubMirror(
        ROOT,
        _tree_with_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="follow",
        out_of_scope_urls={ROOT + "A/"},
    )

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/"])
    assert ROOT + "B/" not in yielded
    assert mirror.metrics.counts.get("symlinks_skipped") == 1
    assert "symlinks_followed" not in mirror.metrics.counts


def test_symlink_mode_detect_reports_but_mirrors_normally(monkeypatch):
    """--symlink-mode detect: purely observational. The detected symlink
    (B) is still mirrored/descended into exactly as if --handle-symlinks
    were unset, but the detection is still logged and counted -- meant
    for surveying a tree before choosing --exclude-dir or a stronger
    --symlink-mode."""
    _patch_dns(monkeypatch)
    mirror = _StubMirror(
        ROOT, _tree_with_symlinked_sibling(), handle_symlinks=True, symlink_mode="detect"
    )

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/", ROOT + "B/"])
    assert mirror.metrics.counts.get("symlinks_detected") == 1
    assert "symlinks_skipped" not in mirror.metrics.counts
    assert "symlinks_followed" not in mirror.metrics.counts


def test_symlink_mode_detect_does_not_apply_out_of_scope_safety_skip(monkeypatch):
    """detect mode never alters crawl behavior -- not even the
    out-of-scope safety boundary that follow mode enforces. It's purely
    "tell me", not "act for me"."""
    _patch_dns(monkeypatch)
    mirror = _StubMirror(
        ROOT,
        _tree_with_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="detect",
        out_of_scope_urls={ROOT + "A/"},
    )

    yielded = list(mirror._discover_directories_bfs())

    assert sorted(yielded) == sorted([ROOT, ROOT + "A/", ROOT + "B/"])
    assert mirror.metrics.counts.get("symlinks_detected") == 1
    assert "symlinks_skipped" not in mirror.metrics.counts


def test_symlink_bomb_threshold_blocks_follow(monkeypatch):
    """Even with target in scope and --symlink-mode=follow, the
    SymlinkTracker's bomb/loop guard can still veto following -- it's
    now actually wired to real symlink detections instead of being
    permanently unreachable dead code."""
    _patch_dns(monkeypatch)
    tracker = SymlinkTracker(max_depth=50, max_per_dir=50, bomb_threshold=0)
    mirror = _StubMirror(
        ROOT,
        _tree_with_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="follow",
        symlink_tracker=tracker,
    )

    yielded = list(mirror._discover_directories_bfs())

    assert ROOT + "B/" not in yielded
    assert mirror.metrics.counts.get("symlink_loops_detected") == 1


def test_nested_duplicate_subtree_is_one_detection_not_one_per_directory():
    """The real bug Borut hit: a single symlink (B -> A) whose subtree is
    multiple levels deep produced one 'Symlink detected' event per
    directory in that subtree (460 for one real symlink), because every
    descendant's content-signature match was treated as an independent
    new detection. It isn't -- if B is a duplicate of A, every
    directory under B necessarily duplicates the corresponding one
    under A too; that's implied by the top-level match, not new
    information. Only the top-level B/ should be flagged."""
    mirror = _StubMirror(
        ROOT,
        _tree_with_nested_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="detect",
    )

    yielded = list(mirror._discover_directories_bfs())

    # detect mode's crawl behavior is unchanged -- every directory is
    # still visited/yielded exactly as before this fix.
    assert sorted(yielded) == sorted(
        [
            ROOT,
            ROOT + "A/",
            ROOT + "B/",
            ROOT + "A/sub1/",
            ROOT + "B/sub1/",
            ROOT + "A/sub1/deep/",
            ROOT + "B/sub1/deep/",
            ROOT + "A/sub2/",
            ROOT + "B/sub2/",
        ]
    )
    # But only ONE detection is counted/logged -- not one per descendant
    # (which would be 4: B/, B/sub1/, B/sub1/deep/, B/sub2/).
    assert mirror.metrics.counts.get("symlinks_detected") == 1


def test_nested_duplicate_subtree_skip_mode_still_stops_at_the_top():
    """Sanity check that the new suppression logic doesn't interfere
    with skip mode's existing behavior: B/ is flagged and not
    descended into, so its children are never even reached (and were
    never the source of the redundant-counting bug in skip mode to
    begin with -- see this file's module docstring)."""
    mirror = _StubMirror(
        ROOT,
        _tree_with_nested_symlinked_sibling(),
        handle_symlinks=True,
        symlink_mode="skip",
    )

    yielded = list(mirror._discover_directories_bfs())

    assert ROOT + "B/" not in yielded
    assert ROOT + "B/sub1/" not in yielded
    assert mirror.metrics.counts.get("symlinks_detected") == 1
    assert mirror.metrics.counts.get("symlinks_skipped") == 1
