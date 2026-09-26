"""Tests for UrlMixin._is_dir_excluded()'s root-relative exact-path matching.

Background: the previous implementation matched each --exclude-dir
pattern as a suffix against the full URL path, at any depth in the
crawled tree. Borut identified this as a real, silent data-loss risk:
--exclude-dir lasco (meant to exclude the one specific directory
<root>/lasco/) would also match and silently exclude an entirely
unrelated <root>/setup/lasco/ anywhere else in the tree, with no
warning. Confirmed directly against the code before this fix (see the
prior conversation turn testing this exact scenario).

New behavior, per Borut's own spec: each pattern is matched as an EXACT
path relative to the scan root (--url), never a suffix match at
arbitrary depth.
  - --exclude-dir lasco excludes only <root>/lasco/, never
    <root>/setup/lasco/ or any other nested "lasco".
  - --exclude-dir lasco idl/beta (two patterns) excludes exactly
    <root>/lasco/ and <root>/idl/beta/ -- nothing else, not other
    occurrences of either name elsewhere in the tree.
A glob pattern (containing *) is the explicit escape hatch for
"anywhere" matching, since it isn't anchored to depth 0 the way a
plain pattern is.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.urls import UrlMixin

ROOT = "https://sohoftp.nascom.nasa.gov/solarsoft/soho/lasco/"


class _Stub(UrlMixin):
    _UNSET = object()

    def __init__(self, exclude_dirs, root_url=ROOT, base_url=_UNSET, target_base_url=None):
        # base_url models --url (what CHANGELOG.md/USER_GUIDE.md document
        # --exclude-dir as being relative to); target_base_url models
        # --url + --dir-suffix appended (see _get_target_base_url). The two
        # differ whenever --dir-suffix is used -- defaulting base_url to
        # root_url when not passed keeps every pre-existing test in this
        # file (which never exercises --dir-suffix) unchanged, while still
        # letting a test pass an explicit "" to model it being unset.
        resolved_base_url = root_url if base_url is _Stub._UNSET else base_url
        self.config = SimpleNamespace(exclude_dirs=exclude_dirs, base_url=resolved_base_url)
        self.target_base_url = target_base_url or root_url


def test_plain_pattern_excludes_only_the_direct_root_relative_path():
    stub = _Stub(["lasco"])
    assert stub._is_dir_excluded(ROOT + "lasco/") is True


def test_plain_pattern_does_not_match_the_same_name_nested_deeper():
    """The exact scenario Borut flagged as fatal: a plain pattern must
    never reach a same-named directory elsewhere in the tree."""
    stub = _Stub(["lasco"])
    assert stub._is_dir_excluded(ROOT + "setup/lasco/") is False


def test_plain_pattern_does_not_match_a_different_sibling():
    stub = _Stub(["lasco"])
    assert stub._is_dir_excluded(ROOT + "idl/") is False


def test_plain_pattern_does_not_match_the_root_itself():
    stub = _Stub(["lasco"])
    assert stub._is_dir_excluded(ROOT) is False


def test_multi_segment_pattern_matches_only_that_exact_relative_path():
    stub = _Stub(["idl/beta"])
    assert stub._is_dir_excluded(ROOT + "idl/beta/") is True
    assert stub._is_dir_excluded(ROOT + "idl/") is False
    assert stub._is_dir_excluded(ROOT + "idl/beta/gamma/") is False
    assert stub._is_dir_excluded(ROOT + "other/idl/beta/") is False


def test_multiple_patterns_each_match_only_their_own_root_relative_path():
    """Borut's second example: --exclude-dir lasco idl/beta excludes
    exactly those two root-relative paths, nothing else."""
    stub = _Stub(["lasco", "idl/beta"])
    assert stub._is_dir_excluded(ROOT + "lasco/") is True
    assert stub._is_dir_excluded(ROOT + "idl/beta/") is True
    assert stub._is_dir_excluded(ROOT + "idl/") is False
    assert stub._is_dir_excluded(ROOT + "setup/lasco/") is False
    assert stub._is_dir_excluded(ROOT + "setup/idl/beta/") is False


def test_glob_pattern_is_the_explicit_escape_hatch_for_any_depth():
    """A glob pattern is the intentional, visible way to opt back into
    'this name anywhere' -- not the default for a plain pattern.
    '*/lasco' requires at least one segment before 'lasco', so it
    matches nested occurrences but correctly leaves the bare root-level
    'lasco/' to the plain-pattern rule instead (covered by the first
    test in this file)."""
    stub = _Stub(["*/lasco"])
    assert stub._is_dir_excluded(ROOT + "setup/lasco/") is True
    assert stub._is_dir_excluded(ROOT + "a/b/lasco/") is True
    assert stub._is_dir_excluded(ROOT + "lasco/") is False
    assert stub._is_dir_excluded(ROOT + "lascoX/") is False


def test_glob_pattern_matching_any_depth_including_root():
    """A pattern that should also cover the zero-depth case needs its
    own explicit '(anything/)?' or a bare '*' segment -- '**' isn't
    special here (this is a simple '*' glob, not full extended-glob
    syntax), so '*lasco' (no required separator) is the way to cover
    both root and nested in one pattern."""
    stub = _Stub(["*lasco"])
    assert stub._is_dir_excluded(ROOT + "lasco/") is True
    assert stub._is_dir_excluded(ROOT + "setup/lasco/") is True


def test_glob_pattern_still_anchors_to_the_full_relative_path():
    stub = _Stub(["20260910*"])
    assert stub._is_dir_excluded(ROOT + "20260910_old/") is True
    assert stub._is_dir_excluded(ROOT + "not_20260910_old/") is False
    assert stub._is_dir_excluded(ROOT + "other/20260910_old/") is False


def test_no_exclude_dirs_configured_excludes_nothing():
    stub = _Stub([])
    assert stub._is_dir_excluded(ROOT + "lasco/") is False


def test_url_outside_root_is_never_excluded():
    """Fail closed: a candidate URL that isn't even under the scan root
    has no relative path to match against, so it's never excluded --
    matches nothing rather than falling back to a broad match."""
    stub = _Stub(["lasco"])
    assert stub._is_dir_excluded("https://example.test/somewhere-else/lasco/") is False


# --- --dir-suffix interaction -------------------------------------------
#
# v3.1.52 rooted matching at self.target_base_url, which already has
# --dir-suffix appended (see UrlMixin._get_target_base_url). CHANGELOG.md
# and USER_GUIDE.md both document --exclude-dir as relative to --url --
# never mentioning --dir-suffix -- so a pattern's meaning silently shifted
# whenever --dir-suffix was in play, exactly the kind of undocumented,
# unbounded rescoping this whole rewrite was meant to close. Root must be
# self.config.base_url (--url itself), independent of --dir-suffix.


def test_pattern_is_relative_to_url_not_dir_suffix():
    """With --dir-suffix 20260908, --exclude-dir lasco must still mean
    <url>/lasco/ (per the docs), not <url>/20260908/lasco/."""
    stub = _Stub(
        ["lasco"],
        base_url=ROOT,
        target_base_url=ROOT + "20260908/",
    )
    assert stub._is_dir_excluded(ROOT + "lasco/") is True
    assert stub._is_dir_excluded(ROOT + "20260908/lasco/") is False


def test_pattern_naming_the_dir_suffix_path_excludes_under_the_suffix():
    """To exclude something under the suffix, the pattern must spell out
    the suffix itself, since matching is relative to --url."""
    stub = _Stub(
        ["20260908/lasco"],
        base_url=ROOT,
        target_base_url=ROOT + "20260908/",
    )
    assert stub._is_dir_excluded(ROOT + "20260908/lasco/") is True


def test_glob_pattern_still_relative_to_url_with_dir_suffix_active():
    stub = _Stub(
        ["*/lasco"],
        base_url=ROOT,
        target_base_url=ROOT + "20260908/",
    )
    assert stub._is_dir_excluded(ROOT + "20260908/lasco/") is True
    assert stub._is_dir_excluded(ROOT + "lasco/") is False


def test_missing_base_url_fails_closed_rather_than_falling_back():
    """No self.config.base_url (shouldn't normally happen) must fail
    closed -- never fall back to target_base_url's broader scope."""
    stub = _Stub(["lasco"], base_url="", target_base_url=ROOT)
    assert stub._is_dir_excluded(ROOT + "lasco/") is False
