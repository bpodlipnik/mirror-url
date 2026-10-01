"""Range validation and atomic resume metadata contracts."""

from __future__ import annotations

import json
from unittest.mock import Mock

import httpx
import pytest

from mirror_url.download_integrity import (
    clear_resume_metadata,
    content_length,
    load_resume_metadata,
    resume_metadata_path,
    save_resume_metadata,
    strong_etag,
    validate_range,
)


@pytest.mark.parametrize(
    "value, expected",
    [
        ('"v1"', '"v1"'),
        ('  "v1"  ', '"v1"'),
        ('""', '""'),
        ('"é"', '"é"'),
        ('W/"v1"', None),
        ('"a b"', None),
        ('"a\x00b"', None),
        ('"a"b"', None),
        ('"Ā"', None),
        ("", None),
    ],
)
def test_strong_etag_accepts_only_quoted_opaque_validators(value, expected):
    assert strong_etag({"ETag": value}) == expected


@pytest.mark.parametrize("value, expected", [(None, None), ("0", 0), ("12", 12)])
def test_optional_content_length(value, expected):
    headers = {} if value is None else {"Content-Length": value}
    assert content_length(headers) == expected


@pytest.mark.parametrize("value", ["-1", "invalid", "1.5"])
def test_invalid_content_length_is_rejected(value):
    with pytest.raises(ValueError):
        content_length({"Content-Length": value})


@pytest.mark.parametrize(
    "status, changes, error",
    [
        (200, {}, "must be 206"),
        (206, {"Content-Range": "bytes */6"}, "complete Content-Range"),
        (206, {"Content-Range": "bytes 1-3/6"}, "offset or total"),
        (206, {"ETag": 'W/"v1"'}, "unverified representation"),
        (206, {"ETag": '"v2"'}, "different"),
        (206, {"Content-Length": "2"}, "byte count"),
        (206, {"Content-Encoding": "gzip"}, "Encoded range"),
    ],
)
def test_range_rejects_unverified_or_mismatched_representations(status, changes, error):
    headers = {"Content-Range": "bytes 0-2/6", "Content-Length": "3", "ETag": '"v1"'}
    headers.update(changes)
    with pytest.raises(ValueError, match=error):
        validate_range(httpx.Response(status, headers=headers), 0, 2, 6, '"v1"')


@pytest.mark.parametrize("length", [None, "3"])
def test_valid_range_with_optional_length(length):
    headers = {"Content-Range": "bytes 0-2/6", "ETag": '"v1"', "Content-Encoding": "IDENTITY"}
    if length is not None:
        headers["Content-Length"] = length
    validate_range(httpx.Response(206, headers=headers), 0, 2, 6, '"v1"')


@pytest.mark.parametrize(
    "changes, partial",
    [
        ({}, b"abc"),
        ({}, b"abcdef"),
        ({"url": "https://other.example/a"}, b"abc"),
        ({"size": "6"}, b"abc"),
        ({"size": True}, b"a"),
        ({"size": 2}, b"abc"),
        ({}, b""),
        ({"etag": 'W/"v1"'}, b"abc"),
        ({"etag": None}, b"abc"),
    ],
)
def test_resume_requires_same_url_strong_validator_and_bounded_nonempty_prefix(
    tmp_path, changes, partial
):
    path = tmp_path / "prefix"
    path.write_bytes(partial)
    data = {"url": "https://example.com/a", "etag": '"v1"', "size": 6}
    data.update(changes)
    resume_metadata_path(path).write_text(json.dumps(data))
    expected = data if not changes and partial else None
    assert load_resume_metadata(path, "https://example.com/a") == expected
    assert path.read_bytes() == partial


@pytest.mark.parametrize("data", [None, "broken", "{}", "[]", '{"size": 6}'])
def test_unreadable_or_malformed_metadata_does_not_destroy_partial(tmp_path, data):
    path = tmp_path / "prefix"
    path.write_bytes(b"abc")
    if data is not None:
        resume_metadata_path(path).write_text(data)
    assert load_resume_metadata(path, "https://example.com/a") is None
    assert path.read_bytes() == b"abc"


def test_metadata_atomic_round_trip_and_idempotent_clear(tmp_path):
    path = tmp_path / "prefix"
    path.write_bytes(b"abc")
    save_resume_metadata(path, "https://example.com/a", '"v1"', 6)
    assert load_resume_metadata(path, "https://example.com/a") == {
        "url": "https://example.com/a",
        "etag": '"v1"',
        "size": 6,
    }
    assert not list(tmp_path.glob("*.tmp"))
    clear_resume_metadata(path)
    clear_resume_metadata(path)
    assert not resume_metadata_path(path).exists()
    assert path.read_bytes() == b"abc"


@pytest.mark.parametrize("fault", ["json.dump", "os.fsync", "os.replace"])
def test_metadata_save_failure_preserves_previous_metadata_and_removes_temporary(
    tmp_path, monkeypatch, fault
):
    path = tmp_path / "prefix"
    path.write_bytes(b"abc")
    save_resume_metadata(path, "https://example.com/a", '"v1"', 6)
    before = resume_metadata_path(path).read_bytes()
    monkeypatch.setattr("mirror_url.download_integrity." + fault, Mock(side_effect=OSError(fault)))
    with pytest.raises(OSError, match=fault):
        save_resume_metadata(path, "https://example.com/a", '"v2"', 9)
    assert resume_metadata_path(path).read_bytes() == before
    assert path.read_bytes() == b"abc"
    assert not list(tmp_path.glob("*.tmp"))
