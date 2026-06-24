"""Unit tests for stateless helpers in ``mirror_url.utils``.

Currently skipped because ``utils`` is a placeholder. Remove the skip marker
once the helper functions are migrated out of ``mirror_url.py``.
"""

from __future__ import annotations

import pytest

utils = pytest.importorskip("mirror_url.utils")

pytestmark = pytest.mark.skipif(
    not hasattr(utils, "format_bytes"),
    reason="utils not yet migrated from mirror_url.py",
)


def test_format_bytes_human_readable():
    assert utils.format_bytes(0) == "0.00 B"
    assert utils.format_bytes(1536) == "1.50 KB"
    assert utils.format_bytes(1048576) == "1.00 MB"


def test_normalize_etag_strips_weak_prefix():
    assert utils.normalize_etag('W/"abc"') == "abc"
    assert utils.normalize_etag('"abc"') == "abc"


def test_is_reserved_windows_filename():
    assert utils.is_reserved_windows_filename("CON")
    assert utils.is_reserved_windows_filename("com1")
    assert not utils.is_reserved_windows_filename("readme.txt")
