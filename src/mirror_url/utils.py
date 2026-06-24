"""Stateless helpers: formatting, URL handling, hashing, cache validation.

Migrated verbatim from ``mirror_url.py`` (orig. lines 815-1150 and 1942-1955).

Also owns the process-wide ``_log_files`` handler registry and its atexit
cleanup (orig. lines 1942-1955); ``cli``/``core`` import these rather than
re-declaring them so there is a single shared list.
"""

from __future__ import annotations

import atexit
import hashlib
import logging
import random
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote, unquote, urlparse

from .constants import (
    BACKOFF_BASE,
    DEFAULT_RETRY_DELAY,
    JITTER_FACTOR,
    MAX_BACKOFF_DELAY,
    WINDOWS_RESERVED_NAMES,
)


def exponential_backoff(
    attempt: int, base_delay: float = DEFAULT_RETRY_DELAY, max_delay: float = MAX_BACKOFF_DELAY
) -> float:
    """
    Calculate delay with exponential backoff and jitter.

    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Delay time in seconds with jitter, guaranteed to be <= max_delay
    """
    # Calculate exponential delay
    exp_delay = base_delay * (BACKOFF_BASE**attempt)

    # Cap at max_delay
    capped_delay = min(exp_delay, max_delay)

    # Add jitter (10% max, but ensure we don't exceed max_delay)
    jitter = random.uniform(0, min(JITTER_FACTOR * capped_delay, max_delay - capped_delay))

    return capped_delay + jitter


def _validate_and_sanitize_cache(data: Any) -> Dict[str, Any]:
    """
    Long-term replacement for _clean_json_keys.
    Validates structure, safely handles minor key corruption,
    and prevents silent data loss or collisions.
    """
    if not isinstance(data, dict):
        raise ValueError("Cache root must be a dictionary")

    cleaned = {}
    seen_keys = set()
    collision_count = 0

    for k, v in data.items():
        if not isinstance(k, str):
            logging.warning(f"Skipping non-string cache key: {type(k)}")
            continue

        # Strip whitespace from keys
        safe_key = k.strip() if isinstance(k, str) else str(k)
        if not safe_key:
            logging.warning("Skipping empty cache key after whitespace strip")
            continue

        if safe_key in seen_keys:
            collision_count += 1
            logging.warning(
                f"Cache key collision detected: '{k}' → '{safe_key}'. "
                f"Keeping existing entry to prevent data loss."
            )
            continue

        seen_keys.add(safe_key)

        # IMPROVED: Safely handle metadata with proper error handling
        if k == "_meta" and isinstance(v, dict):
            meta = {}
            for mk, mv in v.items():
                if mk == "version" and isinstance(mv, int):
                    meta[mk] = mv
                elif mk == "last_full_run" and isinstance(mv, str):
                    try:
                        # Validate ISO format timestamp
                        datetime.fromisoformat(mv)
                        meta[mk] = mv
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Invalid timestamp in cache metadata: {mv}, error: {e}")
                        # Use current time as fallback
                        meta[mk] = datetime.now(timezone.utc).isoformat()
                elif mk == "schema":
                    meta[mk] = str(mv)
                elif mk == "file_count" and isinstance(mv, (int, float)):
                    meta[mk] = int(mv)
                elif mk == "version_code":
                    meta[mk] = str(mv)
                elif mk == "config" and isinstance(mv, dict):
                    # Sanitize config to avoid storing sensitive data
                    safe_config = {}
                    for ck, cv in mv.items():
                        if ck in ("base_url", "dir_suffix", "cache_max_age", "parallel_downloads"):
                            safe_config[ck] = str(cv)
                    meta[mk] = safe_config
                elif mk == "dir_signatures" and isinstance(mv, dict):
                    # Validate directory signatures
                    sigs = {}
                    for dk, dv in mv.items():
                        if isinstance(dk, str) and isinstance(dv, str):
                            sigs[dk] = dv
                    meta[mk] = sigs
                else:
                    # Preserve unknown fields but log them
                    logging.debug(f"Unknown cache metadata field: {mk}")
                    meta[mk] = mv
            cleaned[safe_key] = meta
        else:
            # Values are preserved exactly as written
            cleaned[safe_key] = v

    if collision_count > 0:
        logging.warning(f"Cache load: {collision_count} key collisions detected and skipped")

    return cleaned


def format_duration(seconds: float, show_ms: bool = False) -> str:
    """
    Format duration with optional millisecond precision.

    Args:
        seconds: Duration in seconds
        show_ms: Whether to show milliseconds for sub-second durations

    Returns:
        Formatted duration string

    Example:
        >>> format_duration(3665)
        '1h 1m 5s'
        >>> format_duration(0.5, show_ms=True)
        '500ms'
    """
    if seconds < 0:
        return "unknown"
    if seconds < 1.0 and show_ms:
        ms = seconds * 1000
        return f"{max(1, round(ms))}ms"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"


def format_bytes(bytes_count: float) -> str:
    """
    Format bytes to human-readable string.

    Args:
        bytes_count: Number of bytes

    Returns:
        Formatted string with appropriate unit

    Example:
        >>> format_bytes(1536)
        '1.50 KB'
        >>> format_bytes(1048576)
        '1.00 MB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if bytes_count < 1024.0:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"


def normalize_etag(etag: str) -> str:
    """
    Normalize ETag by removing quotes and weak prefix.

    Args:
        etag: Raw ETag header value

    Returns:
        Normalized ETag string

    Example:
        >>> normalize_etag('W/"12345"')
        '12345'
    """
    if not etag:
        return ""
    # Strip the literal weak-validator prefix "W/" (case-insensitive),
    # NOT any leading 'W'/'/' characters. str.lstrip('W/') would treat
    # the argument as a CHARACTER SET and mangle etags whose content
    # begins with W or /, so use an explicit prefix check.
    if etag[:2] in ("W/", "w/"):
        etag = etag[2:]
    etag = etag.strip('"')
    return etag


def safe_url_encode(path: str) -> str:
    """
    Safely encode URL path components.

    Args:
        path: URL path to encode (should be unencoded)

    Returns:
        Properly encoded URL path. Already-valid percent-sequences
        are preserved; invalid sequences may be partially encoded.

    Note:
        This function does NOT decode first. If you need to normalize
        pre-encoded input, use safe_url_encode(unquote(path)) explicitly.
    """
    if not path:
        return path
    parts = path.split("/")
    # quote() with default safe='/' preserves path separators
    # Valid percent-sequences like %20 are preserved; literal % becomes %25
    return "/".join(quote(part) if part else "" for part in parts)


def trim_url(url: str) -> str:
    """Trim whitespace from URLs"""
    return url.strip()


def sanitize_url_for_log(url: str) -> str:
    """
    Sanitize URL for logging by removing credentials AND the query string.

    Query strings routinely carry secrets — API keys (``?api_key=...``),
    OAuth/JWT tokens (``?access_token=...``), AWS pre-signed-URL
    signatures (``?X-Amz-Signature=...``), session IDs, etc. The previous
    version stripped only userinfo, so any of those secrets ended up in
    log files. This sanitizer now drops the query string and fragment as
    well; if a query was present it is replaced with ``?<redacted>`` so
    the existence of parameters is still visible.

    Args:
        url: URL that may contain credentials or query-string secrets

    Returns:
        URL safe for logging

    Example:
        >>> sanitize_url_for_log('https://user:pass@example.com/p?token=abc#frag')
        'https://example.com/p?<redacted>'
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc
        if netloc and "@" in netloc:
            netloc = netloc.split("@")[-1]
        # Replace the query string with a redaction marker (preserves the
        # signal that *some* parameters were present without leaking them)
        # and drop the fragment entirely.
        new_query = "<redacted>" if parsed.query else ""
        sanitized = parsed._replace(netloc=netloc, query=new_query, fragment="").geturl()
        return sanitized
    except Exception:
        return url


def compute_file_hash(file_path: Path, algorithm: str = "sha256") -> Optional[str]:
    """
    Compute hash of file content.

    Args:
        file_path: Path to file
        algorithm: Hash algorithm (default: sha256)

    Returns:
        Hex digest string or None on error

    Example:
        >>> hash = compute_file_hash(Path('file.txt'))
        >>> print(hash)
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    """
    try:
        hash_obj = hashlib.new(algorithm)
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()
    except Exception:
        return None


def is_reserved_windows_filename(filename: str) -> bool:
    """
    Check if filename is reserved on Windows.

    Args:
        filename: Filename to check

    Returns:
        True if filename is reserved

    Example:
        >>> is_reserved_windows_filename('CON.txt')
        True
        >>> is_reserved_windows_filename('file.txt')
        False
    """
    stem = Path(filename).stem.upper()
    return stem in WINDOWS_RESERVED_NAMES


def normalize_url_path(url_path: str) -> str:
    """
    Normalize a URL path for consistent comparison.

    A normalized URL path is always rooted (begins with ``/``), so callers
    don't need to remember whether the input had a leading slash. Trailing
    slashes are preserved.

    Args:
        url_path: URL path to normalize

    Returns:
        Normalized path beginning with ``/`` (or empty string if input was
        empty/falsy).
    """
    if not url_path:
        return ""

    # Decode percent-encoding
    try:
        decoded = unquote(url_path)
    except Exception:
        decoded = url_path

    # Handle trailing slash
    if decoded.endswith("/"):
        trailing = True
        decoded = decoded.rstrip("/")
    else:
        trailing = False

    # Use Path to collapse duplicate slashes etc.
    normalized = str(Path(decoded)) if decoded else ""

    # Restore trailing slash if needed
    if trailing and normalized:
        normalized = normalized + "/"

    # Always root the path with a leading slash so callers can rely on it.
    if normalized and not normalized.startswith("/"):
        normalized = "/" + normalized

    return normalized


# ============================================================================
# LOG FILE HANDLER REGISTRY (orig. lines 1942-1955)
# ============================================================================
# Process-wide registry of log handlers opened during the run. Shared by the
# logging setup in ``core``/``cli`` and cleaned up on interpreter exit.
_log_files: List[logging.Handler] = []


def cleanup_log_files() -> None:
    """Clean up log file handlers on exit"""
    # Iterate over a snapshot to prevent RuntimeError if list changes during shutdown
    for handler in list(_log_files):
        try:
            handler.close()
        except Exception as e:
            # SAFER: logging module may be partially torn down during atexit.
            # Use sys.stderr to guarantee the message is visible during teardown.
            sys.stderr.write(f"Cleanup log handler error: {e}\n")


atexit.register(cleanup_log_files)


__all__ = [
    "exponential_backoff",
    "format_duration",
    "format_bytes",
    "normalize_etag",
    "safe_url_encode",
    "trim_url",
    "sanitize_url_for_log",
    "compute_file_hash",
    "is_reserved_windows_filename",
    "normalize_url_path",
    "cleanup_log_files",
]
