"""Representation and byte-range checks shared by download paths."""

from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Optional


def strong_etag(headers) -> Optional[str]:
    value = headers.get("ETag", "").strip()
    if re.fullmatch(r'"[\x21\x23-\x7e\x80-\xff]*"', value):
        return value
    return None


def content_length(headers) -> Optional[int]:
    value = headers.get("Content-Length")
    if value is None:
        return None
    size = int(value)
    if size < 0:
        raise ValueError("Negative Content-Length")
    return size


def validate_range(response, start: int, end: int, total: int, etag: str) -> None:
    """A successful range must cover exactly the requested representation."""
    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
    if response.status_code != 206 or not match:
        raise ValueError("Range response must be 206 with a complete Content-Range")
    if tuple(map(int, match.groups())) != (start, end, total):
        raise ValueError("Content-Range does not match the requested offset or total")
    if strong_etag(response.headers) != etag:
        raise ValueError("Range response belongs to a different or unverified representation")
    length = content_length(response.headers)
    if length is not None and length != end - start + 1:
        raise ValueError("Range Content-Length does not match the requested byte count")
    if response.headers.get("Content-Encoding", "identity").lower() != "identity":
        raise ValueError("Encoded range response cannot be safely assembled")


def resume_metadata_path(partial_path: Path) -> Path:
    return partial_path.with_name(partial_path.name + ".json")


def load_resume_metadata(partial_path: Path, url: str):
    """Legacy partials without a validator are deliberately not resumable."""
    try:
        data = json.loads(resume_metadata_path(partial_path).read_text(encoding="utf-8"))
        size = partial_path.stat().st_size
        total = data["size"]
        if (
            data["url"] == url
            and isinstance(total, int)
            and not isinstance(total, bool)
            and 0 < size <= total
            and strong_etag({"ETag": data["etag"]})
        ):
            return data
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pass
    return None


def save_resume_metadata(partial_path: Path, url: str, etag: str, size: int) -> None:
    path = resume_metadata_path(partial_path)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            json.dump({"url": url, "etag": etag, "size": size}, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def clear_resume_metadata(partial_path: Path) -> None:
    resume_metadata_path(partial_path).unlink(missing_ok=True)
