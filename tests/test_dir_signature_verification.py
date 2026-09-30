"""Directory listing validators must never replace per-file verification.

A directory ETag can remain unchanged while an existing child's content
changes. Matching, mismatched, and fallback listing signatures therefore
all require the same file HEAD check.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.compare import CompareMixin


class _FakeMetrics:
    """Records increment() calls so tests can assert on which fired."""

    def __init__(self):
        self.counts: dict[str, int] = {}

    def increment(self, name: str) -> None:
        self.counts[name] = self.counts.get(name, 0) + 1

    def add_request_time(self, _seconds: float) -> None:
        pass


class _FakeResponse:
    def __init__(self, status_code: int, headers: dict | None = None):
        self.status_code = status_code
        self.headers = headers or {}


class _FakeConnectionManager:
    """Records whether a HEAD request was made, and what to return."""

    def __init__(self, response: _FakeResponse):
        self.response = response
        self.call_count = 0

    def request(self, *_args, **_kwargs):
        self.call_count += 1
        return self.response


class _StubMirror(CompareMixin):
    """Minimal stand-in for MirrorURL exposing only what
    file_exists_and_up_to_date uses."""

    def __init__(self, config, scanner, cache_manager, connection_manager):
        self.config = config
        self.scanner = scanner
        self.cache_manager = cache_manager
        self.connection_manager = connection_manager
        self.metrics = _FakeMetrics()
        self.performance_monitor = SimpleNamespace(record=lambda *a, **k: None)
        # Deliberately no self.fs_cache -- exercises the local_path.exists()
        # fallback branch, which is what real runs hit for a fresh local
        # mirror without a warm fs_cache.


def _make_config(**overrides):
    defaults = {"no_etag": False}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_scanner(cached_signatures, fresh_dir_signatures):
    return SimpleNamespace(
        cached_signatures=cached_signatures,
        fresh_dir_signatures=fresh_dir_signatures,
    )


def _make_cache_manager():
    return SimpleNamespace(get_file_metadata=lambda path: None)


def test_matching_signature_still_checks_child(tmp_path):
    """An unchanged listing says nothing about child content."""
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"unchanged content")
    dir_url = "http://example.test/data/"
    remote_url = dir_url + "file.fits"

    scanner = _make_scanner(
        cached_signatures={dir_url: "etag:abc123"},
        fresh_dir_signatures={dir_url: "etag:abc123"},
    )
    conn = _FakeConnectionManager(_FakeResponse(200, {"Content-Length": "999999"}))
    mirror = _StubMirror(_make_config(), scanner, _make_cache_manager(), conn)

    result = mirror.file_exists_and_up_to_date(local_file, remote_url, use_cache=True)

    assert result is False
    assert conn.call_count == 1
    assert mirror.metrics.counts.get("cache_misses") == 1


def test_mismatched_signature_forces_real_check(tmp_path):
    """Fresh signature != cached signature -> falls through to a real check
    that can actually detect the file needs re-downloading."""
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"stale local content")
    dir_url = "http://example.test/data/"
    remote_url = dir_url + "file.fits"

    scanner = _make_scanner(
        cached_signatures={dir_url: "etag:abc123"},  # what was true last run
        fresh_dir_signatures={dir_url: "etag:XYZ999"},  # directory changed this run
    )
    # Simulate the server reporting a different, larger file than what's
    # stored locally -- a real in-place content change.
    conn = _FakeConnectionManager(
        _FakeResponse(200, headers={"Content-Length": "999999", "ETag": '"XYZ999"'})
    )
    mirror = _StubMirror(_make_config(), scanner, _make_cache_manager(), conn)

    result = mirror.file_exists_and_up_to_date(local_file, remote_url, use_cache=True)

    assert conn.call_count == 1, "signature mismatch must trigger a real HEAD request"
    assert result is False, "server ETag differs from stored ETag -> needs download"


def test_timestamp_fallback_signature_never_trusted(tmp_path):
    """The url:...:timestamp fallback form must never satisfy the shortcut,
    even in the pathological case where cached and fresh values are
    byte-identical -- it carries no real change signal."""
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"content")
    dir_url = "http://example.test/data/"
    remote_url = dir_url + "file.fits"

    fallback_sig = f"url:{dir_url}:1783936850"
    scanner = _make_scanner(
        cached_signatures={dir_url: fallback_sig},
        fresh_dir_signatures={dir_url: fallback_sig},  # identical on purpose
    )
    conn = _FakeConnectionManager(_FakeResponse(304))
    mirror = _StubMirror(_make_config(), scanner, _make_cache_manager(), conn)

    mirror.file_exists_and_up_to_date(local_file, remote_url, use_cache=True)

    assert conn.call_count == 1, (
        "url:...:timestamp fallback signatures carry no real change signal "
        "and must always fall through to a real check, even when identical"
    )
