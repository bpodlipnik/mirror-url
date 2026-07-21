"""Tests for --missing-files: skip per-file freshness verification for
files that already exist locally, download only what's absent.

The sync path (file_exists_and_up_to_date, compare.py) gets full
behavioral coverage via a stub mirror instance -- this is also
confirmed to be the actual code path real cron usage exercises (a
production log showed "Using sync metadata checks", not async).

The async path (check_one, inside _check_files_async) is a nested
closure with a lot of surrounding setup (adaptive/async connection
managers) that isn't practical to drive in isolation the same way --
covered instead with a source-level check that the same fast path
exists there too, following the same pattern used for the CLI
stderr-stream fix.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

from mirror_url._core import compare as compare_module
from mirror_url._core.compare import CompareMixin


class _FakeMetrics:
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
    def __init__(self, response: _FakeResponse | None = None):
        self.response = response or _FakeResponse(200)
        self.call_count = 0

    def request(self, *_args, **_kwargs):
        self.call_count += 1
        return self.response


class _StubMirror(CompareMixin):
    def __init__(self, config, connection_manager=None, scanner=None):
        self.config = config
        self.scanner = scanner or SimpleNamespace(cached_signatures={}, fresh_dir_signatures={})
        self.cache_manager = SimpleNamespace(get_file_metadata=lambda path: None)
        self.connection_manager = connection_manager or _FakeConnectionManager()
        self.metrics = _FakeMetrics()
        self.performance_monitor = SimpleNamespace(record=lambda *a, **k: None)
        # Deliberately no self.fs_cache -- exercises local_path.exists() directly.


def _config(missing_files: bool, **overrides):
    defaults = {"no_etag": False, "missing_files": missing_files}
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_missing_files_skips_check_for_existing_file(tmp_path):
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"already downloaded content")
    conn = _FakeConnectionManager()
    mirror = _StubMirror(_config(missing_files=True), connection_manager=conn)

    result = mirror.file_exists_and_up_to_date(
        local_file, "http://example.test/data/file.fits", use_cache=True
    )

    assert result is True
    assert conn.call_count == 0, (
        "--missing-files must not make any network request for an existing file"
    )
    assert mirror.metrics.counts.get("missing_files_skipped_check") == 1


def test_missing_files_still_downloads_absent_file(tmp_path):
    missing_path = tmp_path / "not_downloaded_yet.fits"  # never created
    conn = _FakeConnectionManager()
    mirror = _StubMirror(_config(missing_files=True), connection_manager=conn)

    result = mirror.file_exists_and_up_to_date(
        missing_path, "http://example.test/data/not_downloaded_yet.fits", use_cache=True
    )

    assert result is False, "an absent file must still be reported as needing download"
    assert conn.call_count == 0, "existence alone determines this -- still no network call needed"


def test_missing_files_ignores_stale_or_mismatched_directory_signature(tmp_path):
    """The whole point of the flag: it must skip verification even in
    exactly the scenario the directory-signature fix cares about (a
    directory whose signature changed, meaning something in it may have
    been modified) -- that's the explicit, documented trade-off."""
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"content")
    dir_url = "http://example.test/data/"
    scanner = SimpleNamespace(
        cached_signatures={dir_url: "etag:OLD"},
        fresh_dir_signatures={dir_url: "etag:NEW"},  # deliberately mismatched
    )
    conn = _FakeConnectionManager()
    mirror = _StubMirror(_config(missing_files=True), connection_manager=conn, scanner=scanner)

    result = mirror.file_exists_and_up_to_date(local_file, dir_url + "file.fits", use_cache=True)

    assert result is True
    assert conn.call_count == 0


def test_missing_files_off_by_default_preserves_existing_behavior(tmp_path):
    """Sanity check: without the flag, an existing file with no cached
    directory signature still goes through a real check (existing,
    pre-flag behavior), proving the new code path is genuinely opt-in."""
    local_file = tmp_path / "file.fits"
    local_file.write_bytes(b"content")
    conn = _FakeConnectionManager(_FakeResponse(200, headers={"Content-Length": "999"}))
    mirror = _StubMirror(_config(missing_files=False), connection_manager=conn)

    mirror.file_exists_and_up_to_date(
        local_file, "http://example.test/data/file.fits", use_cache=True
    )

    assert conn.call_count == 1, "without --missing-files, a real check must still happen"
    assert "missing_files_skipped_check" not in mirror.metrics.counts


def test_async_check_one_has_the_same_missing_files_fast_path():
    """Source-level check for the async path -- see module docstring for
    why this isn't driven behaviorally like the sync tests above."""
    src = inspect.getsource(compare_module)
    assert "missing_files_skipped_check" in src
    # Confirm it appears twice: once in the sync path, once in async check_one.
    assert src.count('self.metrics.increment("missing_files_skipped_check")') == 2
