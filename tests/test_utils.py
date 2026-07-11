"""Unit tests for stateless helpers in ``mirror_url.utils``."""

from __future__ import annotations

from mirror_url import utils


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
