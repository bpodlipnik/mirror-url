"""Regression test for a StringZilla type-mixing bug in matches_filter().

Reported symptom (real user command, real stringzilla installed):

    mirror-url ... --dir-suffix "L3_png/v03" --filter _fe_ --dry-run
    ERROR:root:Error scanning https://.../orbit_0273/: 'in <string>'
    requires string as left operand, not stringzilla.Str

Root cause: ``_get_url_path_fast()`` (``_core/urls.py``) explicitly converts
its StringZilla result back to a plain ``str`` before returning (see its own
``-> str`` type hint and docstring). That plain ``str`` flows unchanged
through ``_get_filename_fast()``, so despite that method's ``-> Str`` type
hint, its return value is actually a plain ``str`` whenever the real
``stringzilla`` package is installed. ``matches_filter()``'s plain-substring
branch then did ``Str(pattern) in filename_sz`` -- comparing a real
``stringzilla.Str`` against a plain ``str`` -- which raises exactly this
TypeError. This is not specific to any particular pattern text; it breaks
for *any* filter pattern that takes the plain-substring branch (no leading
``.``, no regex metacharacters).

The pure-Python fallback ``Str`` in ``compat.py`` (used when stringzilla
isn't installed) subclasses ``str``, so this bug is invisible unless the
real stringzilla package is present -- which is why it slipped through
until a live run against p3sc.oma.be hit it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mirror_url._core.scan import ScanMixin
from mirror_url._core.urls import UrlMixin
from mirror_url.compat import STRINGZILLA_AVAILABLE


class _StubMirror(ScanMixin, UrlMixin):
    """Minimal stand-in exposing only what matches_filter() needs."""

    def __init__(self, file_filters):
        self.config = SimpleNamespace(file_filters=file_filters)


# A handful of realistic PROBA-3-style filenames and patterns that take the
# plain-substring branch (no leading '.', no regex metacharacters).
SUBSTRING_CASES = [
    ("https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0273/some_fe_284.png", "_fe_", True),
    ("https://p3sc.oma.be/datarepfiles/L3_png/v03/orbit_0273/some_wl_284.png", "_fe_", False),
    ("https://p3sc.oma.be/datarepfiles/L1/v03/orbit_0273/aspiics_L1_284.fits", "aspiics", True),
]


@pytest.mark.parametrize("url,pattern,expected", SUBSTRING_CASES)
def test_plain_substring_filter_does_not_raise(url, pattern, expected):
    """This is the exact shape of the reported crash: a non-extension,
    non-regex --filter pattern (e.g. --filter _fe_) must not raise
    TypeError, and must match/reject correctly."""
    mirror = _StubMirror(file_filters=[pattern])
    assert mirror.matches_filter(url) is expected


def test_extension_filter_still_works():
    """Sanity check the leading-'.' branch (untouched by this fix) still
    works after the change."""
    mirror = _StubMirror(file_filters=[".fits"])
    assert mirror.matches_filter("https://example.test/a/file.fits") is True
    assert mirror.matches_filter("https://example.test/a/file.png") is False


def test_regex_filter_still_works():
    """Sanity check the regex branch (untouched by this fix) still works.
    Note matches_filter() only inspects the filename (last path segment),
    not the full URL -- so the pattern must be matchable within that."""
    mirror = _StubMirror(file_filters=[r"orbit_02\d{2}"])
    assert mirror.matches_filter("https://example.test/x/orbit_0273_file.png") is True
    assert mirror.matches_filter("https://example.test/x/orbit_1999_file.png") is False


def test_real_stringzilla_is_actually_installed():
    """This bug is invisible under the pure-Python compat.py fallback (a
    str subclass). Guard so this test file actually exercises the real
    bug in CI rather than silently passing for the wrong reason."""
    assert STRINGZILLA_AVAILABLE, (
        "stringzilla is not installed in this environment -- the other "
        "tests in this file pass regardless of the fix, since the "
        "compat.py fallback Str subclasses str and never hits this bug. "
        "Install stringzilla to get real coverage of this regression."
    )
