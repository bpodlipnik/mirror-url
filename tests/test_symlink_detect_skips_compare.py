"""Regression test for ReportMixin.sync()'s --symlink-mode detect early exit.

Background: --symlink-mode detect's entire purpose is directory-level
symlink discovery so the user can decide what to --exclude-dir (or switch
to --symlink-mode follow/skip) afterward -- see the "Symlink handling"
section of docs/USER_GUIDE.md. Everything needed for that decision is
already fully known and logged by the time get_remote_files() returns:
every directory has been walked and every "🔗 Symlink detected" line
already written. Borut observed in a real run against NASA's sohoftp
SolarSoft archive (53,900 files, 460 detected symlinks) that sync() was
still going on afterward to do a full per-file dry-run compare (~36s of
extra work simulating "what would be downloaded") -- an answer detect
mode never needed, since that comparison can't change which directories
get excluded.

This locks in the fix: sync() must return True immediately after
get_remote_files() when handle_symlinks + symlink_mode="detect", never
reaching the dry-run/real per-file compare step (_check_files_sync /
_check_files_async / the download pipeline). Builds a ReportMixin
instance directly (bypassing the network layer), with the compare step
replaced by a spy that raises if ever called -- proving sync() never
reaches it, not exercising what it does. Follows the stubbing convention
of test_sync_connection_failure_guard.py.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.report import ReportMixin


class _SpyCalled(Exception):
    """Raised by a stub method to prove it was reached -- an unmistakable
    failure signal if sync() ever does per-file work it must skip in
    --symlink-mode detect."""


class _StubMirror(ReportMixin):
    """Minimal stand-in for MirrorURL exposing only what sync()'s early
    branches (quick mode, disk space, get_remote_files, the new detect
    short-circuit) touch, plus a spy on the per-file compare step that
    must never run in detect mode."""

    def __init__(self, handle_symlinks, symlink_mode, remote_files, symlinks_detected=0):
        self.connection_manager = object()
        self.connection_ok = True
        self.config = SimpleNamespace(
            quick=False,
            dry_run=True,
            async_metadata=False,
            handle_symlinks=handle_symlinks,
            symlink_mode=symlink_mode,
            dir_suffix=None,
            progress_bar=False,
        )
        self.metrics = SimpleNamespace(
            metrics={
                "files_downloaded": 0,
                "files_skipped": 0,
                "files_failed": 0,
                "symlinks_detected": symlinks_detected,
            },
            add_error=lambda *a, **kw: None,
        )
        self.multi_progress = SimpleNamespace(add_level=lambda *a, **kw: None)
        self.files_processed = SimpleNamespace(value=lambda: 0)
        self.files_skipped = SimpleNamespace(value=lambda: 0)
        self.files_failed = SimpleNamespace(value=lambda: 0)
        self.total_downloaded_size = SimpleNamespace(value=lambda: 0)
        self._remote_files = remote_files
        self.check_files_called = False

    def _get_prefix(self) -> str:
        return ""

    def get_remote_files(self):
        return self._remote_files

    def _check_files_sync(self, *args, **kwargs):
        self.check_files_called = True
        raise _SpyCalled("_check_files_sync() must not be called in --symlink-mode detect")

    def _check_files_async(self, *args, **kwargs):
        self.check_files_called = True
        raise _SpyCalled("_check_files_async() must not be called in --symlink-mode detect")


def test_symlink_mode_detect_skips_per_file_compare():
    """The scenario the user actually hit: detect mode with a large file
    list must return successfully without ever touching the per-file
    compare step."""
    mirror = _StubMirror(
        handle_symlinks=True,
        symlink_mode="detect",
        remote_files=[f"https://example.test/f{i}.dat" for i in range(53900)],
        symlinks_detected=460,
    )

    result = mirror.sync()

    assert result is True
    assert mirror.check_files_called is False


def test_symlink_mode_skip_still_runs_the_compare_step():
    """Only 'detect' gets the short-circuit -- 'skip'/'follow' still need
    the normal dry-run/real compare step to know what to download.
    sync() catches exceptions internally and returns False, so we check
    the spy flag directly rather than expecting the exception to
    propagate."""
    mirror = _StubMirror(
        handle_symlinks=True,
        symlink_mode="skip",
        remote_files=["https://example.test/f0.dat"],
    )

    mirror.sync()

    assert mirror.check_files_called is True


def test_handle_symlinks_off_still_runs_the_compare_step():
    """detect's short-circuit requires handle_symlinks=True -- leaving
    symlink_mode at 'detect' with handle_symlinks off (its default state)
    must not accidentally trigger the short-circuit."""
    mirror = _StubMirror(
        handle_symlinks=False,
        symlink_mode="detect",
        remote_files=["https://example.test/f0.dat"],
    )

    mirror.sync()

    assert mirror.check_files_called is True
