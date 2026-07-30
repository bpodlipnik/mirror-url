"""Tests for ScanMixin.list_directories() (--list-dirs).

--list-dirs discovers the directory tree under the target URL /
--dir-suffix and logs each one, without scanning files, comparing
freshness, or downloading/deleting anything -- it's a read-only "what's
out there" probe, useful for picking a --dir-suffix before committing to
a full mirror run.

Following the convention already used in test_cleanup_partial_scan.py,
this builds a ScanMixin instance directly and stubs out
_discover_directories_bfs() rather than driving real HTTP through the
SSRF-guarded connection layer (see test_integration.py's module
docstring for why a live loopback server isn't used here).
"""

from __future__ import annotations

import logging

from mirror_url._core.scan import ScanMixin


class _StubMirror(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what list_directories uses."""

    def __init__(self, target_base_url, connection_ok=True, has_connection_manager=True):
        self.target_base_url = target_base_url
        self.connection_ok = connection_ok
        if has_connection_manager:
            self.connection_manager = object()  # presence is all that's checked
        self._discovered = []

    def _get_prefix(self) -> str:
        return ""

    def _discover_directories_bfs(self):
        yield from self._discovered


def test_list_directories_logs_relative_paths_and_returns_true(caplog):
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = [
        "https://example.test/data/",
        "https://example.test/data/L1/",
        "https://example.test/data/L1/v03/",
        "https://example.test/data/L2/",
    ]

    with caplog.at_level(logging.INFO):
        result = mirror.list_directories()

    assert result is True
    messages = [r.message for r in caplog.records]
    # Root itself is shown as "." rather than an empty string.
    assert any("📁 ." in m for m in messages)
    assert any("📁 L1" in m and "L1/v03" not in m for m in messages)
    assert any("📁 L1/v03" in m for m in messages)
    assert any("📁 L2" in m for m in messages)
    assert any("Listed 4 of 4 directories" in m for m in messages)


def test_list_directories_singular_count(caplog):
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = ["https://example.test/data/"]

    with caplog.at_level(logging.INFO):
        result = mirror.list_directories()

    assert result is True
    assert any("Listed 1 of 1 directory" in r.message for r in caplog.records)
    assert not any("directories" in r.message for r in caplog.records if "Listed" in r.message)


def test_list_directories_returns_false_without_connection_manager():
    mirror = _StubMirror(target_base_url="https://example.test/data/", has_connection_manager=False)
    assert mirror.list_directories() is False


def test_list_directories_returns_false_when_connection_not_ok():
    mirror = _StubMirror(target_base_url="https://example.test/data/", connection_ok=False)
    assert mirror.list_directories() is False


def test_list_directories_does_not_scan_files():
    """list_directories must never call scan_directory_sequential -- only
    _discover_directories_bfs (which is what stays a pure directory-URL
    generator; per-directory file scanning belongs to get_remote_files)."""
    mirror = _StubMirror(target_base_url="https://example.test/data/")
    mirror._discovered = ["https://example.test/data/L1/"]

    def _boom(*args, **kwargs):
        raise AssertionError("list_directories must not scan files")

    mirror.scanner = None  # would raise AttributeError if ever touched
    assert mirror.list_directories() is True
