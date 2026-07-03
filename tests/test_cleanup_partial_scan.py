"""Regression test for the clean_obsolete partial-scan guard.

``clean_obsolete()`` (``src/mirror_url/_core/cleanup.py``) treats any local
file under ``target_dir`` that is not present in ``remote_files`` as
obsolete, and deletes/moves it. Nothing used to distinguish a complete
remote listing from a partial one.

``_discover_directories_bfs()`` (``src/mirror_url/_core/scan.py``) catches
per-directory scan exceptions; it used to silently substitute an empty
file/subdir list for that directory instead of propagating the failure or
flagging the overall result as incomplete. A single transient error while
scanning one subdirectory (timeout, connection reset, transient 5xx, ...)
was therefore enough to make every file under that subtree get reported to
``clean_obsolete()`` as obsolete -- and deleted -- even though it still
existed on the remote and simply wasn't re-listed that run.

Fix: ``get_remote_files()`` now resets a ``self.scan_incomplete`` flag at
the start of each run, and ``_discover_directories_bfs()`` sets it to
``True`` whenever a directory fails to list. ``clean_obsolete()`` checks the
flag first and refuses to delete/move/preview anything while it's set,
logging a warning instead.

This test builds a ``CleanupMixin`` instance directly (bypassing the network
layer) and simulates exactly that scenario: a ``remote_files`` set that is
missing an entire subtree because its scan failed, with ``scan_incomplete``
set accordingly (as the real scan now does). It asserts ``clean_obsolete()``
leaves every local file untouched.
"""

from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import urlparse

from mirror_url._core.cleanup import CleanupMixin
from mirror_url.enums import CleanupPolicy


class _StubMirror(CleanupMixin):
    """Minimal stand-in for MirrorURL exposing only what clean_obsolete uses."""

    def __init__(self, config, target_dir, target_base_url):
        self.config = config
        self.target_dir = target_dir
        self.target_parsed = urlparse(target_base_url)
        self.suffix_index = 0
        self.total_suffixes = 1
        self.metrics = SimpleNamespace(metrics={})
        self.cache_manager = SimpleNamespace(
            cleanup_file_metadata=lambda path: None,
            cleanup_stale_metadata=lambda expected: 0,
        )

    def _get_prefix(self) -> str:
        return ""


def _make_config(**overrides):
    defaults = {
        "cleanup_policy": CleanupPolicy.DELETE,
        "dry_run": False,
        "confirm_delete": False,
        "max_depth": 10,
        "max_filename_len": 255,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_clean_obsolete_skips_everything_when_scan_incomplete(tmp_path):
    base_url = "http://example.test/data/"
    target_dir = tmp_path / "mirror"
    target_dir.mkdir()

    # Two subdirectories were mirrored in a previous run. This run, "ok/"
    # scanned fine; "flaky/" raised mid-scan (e.g. a timeout), so it
    # contributed no files to remote_files and set scan_incomplete=True
    # (mirroring what ScanMixin._discover_directories_bfs now does).
    (target_dir / "ok").mkdir()
    (target_dir / "ok" / "keep.dat").write_bytes(b"still on the remote")
    (target_dir / "flaky").mkdir()
    (target_dir / "flaky" / "also_still_remote.dat").write_bytes(
        b"never re-listed this run, but still exists remotely"
    )

    # What get_remote_files() returned this run: only the directory that
    # scanned successfully. "flaky/" contributed nothing -- not because
    # it's empty on the remote, but because its scan errored.
    remote_files = {base_url + "ok/keep.dat"}

    mirror = _StubMirror(_make_config(), target_dir, base_url)
    mirror.scan_incomplete = True  # what the real BFS scan sets on error

    mirror.clean_obsolete(remote_files)

    assert (target_dir / "ok" / "keep.dat").exists()
    assert (target_dir / "flaky" / "also_still_remote.dat").exists(), (
        "file in a directory whose scan failed must survive: clean_obsolete() "
        "should refuse to run at all while scan_incomplete is set"
    )


def test_clean_obsolete_still_runs_when_scan_complete(tmp_path):
    """Sanity check: the guard doesn't just disable cleanup unconditionally."""
    base_url = "http://example.test/data/"
    target_dir = tmp_path / "mirror"
    target_dir.mkdir()

    (target_dir / "keep.dat").write_bytes(b"still on the remote")
    (target_dir / "gone.dat").write_bytes(b"no longer on the remote")

    remote_files = {base_url + "keep.dat"}

    mirror = _StubMirror(_make_config(), target_dir, base_url)
    mirror.scan_incomplete = False  # a full, successful scan

    mirror.clean_obsolete(remote_files)

    assert (target_dir / "keep.dat").exists()
    assert not (target_dir / "gone.dat").exists()
