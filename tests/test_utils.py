"""Unit tests for stateless helpers in ``mirror_url.utils``."""

from __future__ import annotations

import pytest

from mirror_url import utils


def test_format_bytes_human_readable():
    assert utils.format_bytes(0) == "0.00 B"
    assert utils.format_bytes(1536) == "1.50 KiB"
    assert utils.format_bytes(1048576) == "1.00 MiB"


@pytest.mark.parametrize(
    "size, label",
    [(0, "B"), (1536, "KiB"), (1024**2, "MiB"), (1024**4, "TiB"), (1024**5, "PiB")],
)
def test_shared_byte_formatter_supports_iec_labels(size, label):
    assert utils.format_bytes(size).endswith(" " + label)
    assert utils.format_bytes(size, binary_units=True).endswith(" " + label)


@pytest.mark.parametrize(
    "size,expected",
    [
        (1023, "1023.00 B"),
        (1024, "1.00 KiB"),
        (1024**2, "1.00 MiB"),
        (1024**3, "1.00 GiB"),
        (268509609, "256.07 MiB"),
        (9543236657, "8.89 GiB"),
    ],
)
def test_default_unit_labels_preserve_binary_values(size, expected):
    assert utils.format_bytes(size) == expected
    assert (
        utils.format_bytes(size).split()[0]
        == utils.format_bytes(size, binary_units=False).split()[0]
    )


def test_explicit_legacy_labels_remain_available_to_api_callers():
    assert utils.format_bytes(1024**2, binary_units=False) == "1.00 MB"


def test_normalize_etag_strips_weak_prefix():
    assert utils.normalize_etag('W/"abc"') == "abc"
    assert utils.normalize_etag('"abc"') == "abc"


def test_is_reserved_windows_filename():
    assert utils.is_reserved_windows_filename("CON")
    assert utils.is_reserved_windows_filename("com1")
    assert not utils.is_reserved_windows_filename("readme.txt")
