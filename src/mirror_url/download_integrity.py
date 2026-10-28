"""Representation and byte-range checks shared by download paths."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import stat
import tempfile
from pathlib import Path
from typing import BinaryIO, Optional, Tuple

from .filename_mapping import _stat_identity
from .security import PathSafety
from .utils import sanitize_log_text


def file_identity(path: Path, *, allow_symlink: bool = False) -> Optional[Tuple[int, ...]]:
    """Snapshot an owned entry without following links; staging must be regular."""
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not (stat.S_ISREG(info.st_mode) or (allow_symlink and stat.S_ISLNK(info.st_mode)))
        or info.st_nlink != 1
        or (hasattr(os, "getuid") and info.st_uid != os.getuid())
    ):
        raise ValueError("Download would replace an unsafe or unowned local entry")
    return _stat_identity(info)


def open_owned_partial(path: Path, expected: Optional[Tuple[int, ...]]) -> BinaryIO:
    """Open without truncation; verify the handle and pathname before any write."""
    flags = os.O_RDWR | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_BINARY", 0)
    if expected is None:
        flags |= os.O_CREAT | os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        opened = _stat_identity(info)
        named = file_identity(path)
        if named != opened or (expected is not None and opened != expected):
            raise ValueError("Partial download changed before opening; preserving it")
        return os.fdopen(descriptor, "r+b")
    except BaseException:
        os.close(descriptor)
        raise


def replace_verified_file(
    staging: Path,
    local: Path,
    *,
    target: Path,
    original: Optional[Tuple[int, ...]],
    staged_identity: Tuple[int, ...],
    max_depth: int,
    max_filename_len: int,
    cancelled=None,
) -> None:
    """Publish only the verified staging file over the original destination."""
    if file_identity(staging) != staged_identity:
        raise ValueError("Owned staging file changed before publication")
    relative = PathSafety._normalize_resolved_path(local).relative_to(
        PathSafety._normalize_resolved_path(target)
    )
    checked = PathSafety.safe_join(
        target,
        *relative.parts,
        create_base=False,
        max_depth=max_depth,
        max_filename_len=max_filename_len,
        allow_leaf_symlink=True,
    )
    replaced_symlink = local.is_symlink()
    if (
        not PathSafety.same_resolved_path(checked, local)
        or file_identity(local, allow_symlink=True) != original
    ):
        raise ValueError("Destination changed during transfer; preserving existing entry")
    if cancelled is not None and cancelled.is_set():
        raise ValueError("Chunk transfer cancelled before publication")
    os.replace(staging, local)
    if replaced_symlink:
        logging.info(
            "Replaced local symlink with downloaded file: %s",
            sanitize_log_text(str(local).replace("\n", "\\n").replace("\r", "\\r")),
        )


def file_sha256(path: Path, enabled: bool = True) -> Optional[str]:
    """Hash a regular file in bounded chunks, rejecting changes during the read.

    Downloaders call this on their completed staging file before publication.
    This detects observable races; callers still need exclusive destination
    ownership to exclude arbitrary concurrent writers.
    """
    if not enabled:
        return None
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode):
        raise OSError("Content verification requires a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while block := file.read(1024 * 1024):
            digest.update(block)
    after = path.lstat()
    identity = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")
    if any(getattr(before, field) != getattr(after, field) for field in identity):
        raise OSError("File changed during content verification")
    return digest.hexdigest()


def local_content_matches(path: Path, expected) -> bool:
    """A missing or malformed receipt cannot validate local bytes."""
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
        return False
    try:
        return file_sha256(path) == expected
    except OSError:
        return False


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
