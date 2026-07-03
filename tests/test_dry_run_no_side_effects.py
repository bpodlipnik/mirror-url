"""Regression test: dry-run must not create the target directory on disk.

Bug: ``PathSafety.safe_join()`` used to call ``base.mkdir()`` unconditionally
whenever the base directory didn't exist yet, with no way for a caller to
opt out. ``ScanMixin._get_local_path_from_url()`` (called once per remote
file during the dry-run file-existence check) resolves every local path via
``safe_join(self.target_dir, ...)`` -- so the very first file checked during
a dry-run silently created the (empty) target directory as a side effect,
even though the dry-run log had already reported it as "not created".

Fix: ``safe_join()`` now takes a ``create_base`` flag; ``_get_local_path_
from_url()`` passes ``create_base=not self.config.dry_run``.

This test builds a ``ScanMixin`` instance directly (bypassing the network
layer) and asserts that resolving local paths for a batch of files in
dry-run mode leaves the target directory untouched.
"""

from __future__ import annotations

from types import SimpleNamespace
from urllib.parse import urlparse

from mirror_url._core.scan import ScanMixin


class _StubScanner(ScanMixin):
    """Minimal stand-in for MirrorURL exposing only what
    _get_local_path_from_url uses."""

    def __init__(self, config, target_dir, target_base_url):
        self.config = config
        self.target_dir = target_dir
        self.target_parsed = urlparse(target_base_url)
        self._target_dir_path = target_dir  # normally target_dir.resolve()

    def _parse_url_cached(self, url: str):
        return urlparse(url)


def _make_config(*, dry_run: bool) -> SimpleNamespace:
    return SimpleNamespace(dry_run=dry_run, max_depth=10, max_filename_len=255)


def test_dry_run_does_not_create_target_directory(tmp_path):
    base_url = "http://example.test/data/orbit_0677/"
    target_dir = tmp_path / "orbit_0677"
    assert not target_dir.exists()

    scanner = _StubScanner(_make_config(dry_run=True), target_dir, base_url)

    # Simulate checking a batch of remote files, as the dry-run
    # file-existence check does for every file it would (not) download.
    for i in range(38):
        local_path = scanner._get_local_path_from_url(f"{base_url}file_{i:03d}.fits")
        assert local_path is not None  # path still resolves correctly

    assert not target_dir.exists(), (
        "dry-run resolved 38 local paths and must not have created the "
        "target directory as a side effect"
    )


def test_real_run_creates_target_directory(tmp_path):
    """Sanity check: the guard doesn't break normal (non-dry-run) behavior."""
    base_url = "http://example.test/data/orbit_0677/"
    target_dir = tmp_path / "orbit_0677"
    assert not target_dir.exists()

    scanner = _StubScanner(_make_config(dry_run=False), target_dir, base_url)

    local_path = scanner._get_local_path_from_url(f"{base_url}file_000.fits")

    assert local_path is not None
    assert target_dir.exists()
