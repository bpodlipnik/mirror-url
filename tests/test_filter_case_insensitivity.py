"""Regression test for a case-insensitivity bug in matches_filter().

Reported symptom (real user command, real production target):

    mirror-url --url https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0685 \\
      --list-files 10 --filter fe --quiet
    -> 10 matches (of 266), as expected

    mirror-url --url https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0685 \\
      --list-files 10 --filter 20260619T073 --quiet
    -> no output at all, despite the directory visibly containing files
       like aspiics_fe_l3_20260619T073111_2AD040B10001_v03.png that
       obviously contain that exact substring.

Root cause: for both the extension-check branch (``filename.endswith(...)``)
and the plain-substring branch (``pattern_lower in filename``), only the
*pattern* side was lowercased -- the filename being searched was left in
its original case. Substring/suffix matching against a lowercased pattern
is not actually case-insensitive unless the other side is lowercased too.

This went unnoticed for patterns like the first example above (``fe``)
because the matching portion of the filename (``aspiics_fe_l3...``)
happens to already be lowercase -- there's no case mismatch to expose the
bug. It breaks for any pattern that matches a portion of the filename
containing an uppercase character, such as the ``T`` time separator in the
ISO-8601-style timestamps PROBA-3/STEREO filenames use
(``..._20260619T073111_...``): the lowercased pattern ``20260619t073``
was compared against the *original-case* filename, which contains
``20260619T073`` (uppercase T) -- and never matched.

Same root cause affects the extension-check branch: a filter like
``.FITS`` (or a filename that happens to end in an uppercase extension)
would be similarly broken. Covered here too for completeness, even though
it wasn't part of the originally reported symptom.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.scan import ScanMixin
from mirror_url._core.urls import UrlMixin


class _StubMirror(ScanMixin, UrlMixin):
    """Minimal stand-in exposing only what matches_filter() needs."""

    def __init__(self, file_filters):
        self.config = SimpleNamespace(file_filters=file_filters)


def test_reported_case_exact_repro():
    """The exact filename/pattern pair from the bug report."""
    mirror = _StubMirror(file_filters=["20260619T073"])
    url = (
        "https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0685/"
        "aspiics_fe_l3_20260619T073111_2AD040B10001_v03.png"
    )
    assert mirror.matches_filter(url) is True


def test_reported_case_lowercase_pattern_still_matches_uppercase_filename():
    """The reported pattern was typed in lowercase ('073' segment aside,
    a 't' would be the natural lowercase typing) -- confirm the lowercase
    spelling of the pattern still matches the uppercase 'T' in the
    filename, which is the actual bug: pattern-side lowercasing without
    filename-side lowercasing is not case-insensitive matching."""
    mirror = _StubMirror(file_filters=["20260619t073"])  # lowercase 't'
    url = (
        "https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0685/"
        "aspiics_fe_l3_20260619T073111_2AD040B10001_v03.png"
    )
    assert mirror.matches_filter(url) is True


def test_substring_match_is_case_insensitive_both_directions():
    """Case-insensitivity has to hold regardless of which side (pattern or
    filename) carries the differing case."""
    url = "https://example.test/x/AsPiIcS_FE_L3_File.png"
    assert _StubMirror(file_filters=["aspiics_fe"]).matches_filter(url) is True
    assert _StubMirror(file_filters=["ASPIICS_FE"]).matches_filter(url) is True
    assert _StubMirror(file_filters=["AsPiIcS_fe"]).matches_filter(url) is True


def test_substring_match_still_rejects_non_matches():
    """The fix must not turn matching into an always-true no-op."""
    url = "https://example.test/x/aspiics_wl_l3_file.png"
    assert _StubMirror(file_filters=["_fe_"]).matches_filter(url) is False


def test_extension_filter_is_case_insensitive():
    """Same underlying bug pattern applies to the extension-check branch:
    filename.endswith(pattern_lower) needs a lowercased filename too."""
    mirror_lower_pattern = _StubMirror(file_filters=[".fits"])
    assert mirror_lower_pattern.matches_filter("https://example.test/a/FILE.FITS") is True

    mirror_upper_pattern = _StubMirror(file_filters=[".FITS"])
    assert mirror_upper_pattern.matches_filter("https://example.test/a/file.fits") is True


def test_regex_filter_case_insensitivity_unaffected():
    """The regex branch already passed re.IGNORECASE and was never part of
    this bug -- confirm it's still unaffected by the fix."""
    mirror = _StubMirror(file_filters=[r"20260619t\d{3}"])
    url = "https://example.test/x/aspiics_fe_l3_20260619T073111_v03.png"
    assert mirror.matches_filter(url) is True
