"""Tests for CleanupMixin._scan_local_tree() and its integration into
clean_obsolete().

Replaces what used to be up to 4 separate Path.rglob("*") calls per
clean_obsolete() invocation with a single os.scandir()-based walk, shared
across the confirm-delete count, preview mode, and the real DELETE/MOVE
run. This file covers:

- _scan_local_tree() in isolation: correct files/dirs collection, a
  symlink-cycle guard (new -- more explicit/robust than the previous
  code's bare `except RuntimeError` around rglob()), and graceful
  handling of an unreadable subdirectory (monkeypatched, since tests run
  as root in CI/sandboxes where real chmod-based permission denial
  doesn't apply)
- clean_obsolete() end-to-end in PREVIEW mode (single walk now serves
  both the file-check and the empty-dir-check, previously two separate
  rglob() calls)
- clean_obsolete() end-to-end in DELETE mode with --confirm-delete (the
  confirm-count and the actual deletion now come from the same walk --
  regression guard against them ever diverging)
- clean_obsolete() end-to-end in MOVE mode (not covered by any existing
  test before this file)

Same stubbing convention as test_cleanup_partial_scan.py: a CleanupMixin
built directly against a real tmp_path tree, bypassing the network layer
entirely.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

from mirror_url._core.cleanup import CleanupMixin
from mirror_url.enums import CleanupPolicy


class _StubMirror(CleanupMixin):
    """Minimal stand-in for MirrorURL exposing only what clean_obsolete/
    _scan_local_tree use."""

    def __init__(self, config, target_dir, target_base_url):
        self.config = config
        self.target_dir = target_dir
        self.target_parsed = urlparse(target_base_url)
        self.suffix_index = 0
        self.total_suffixes = 1
        self.scan_incomplete = False
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


# ---------------------------------------------------------------------------
# _scan_local_tree() in isolation
# ---------------------------------------------------------------------------


def test_scan_local_tree_collects_all_files_and_dirs(tmp_path):
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "a").mkdir()
    (target / "a" / "b").mkdir()
    (target / "top.dat").write_bytes(b"x")
    (target / "a" / "mid.dat").write_bytes(b"x")
    (target / "a" / "b" / "deep.dat").write_bytes(b"x")

    mirror = _StubMirror(_make_config(), target, "http://example.test/data/")
    files, dirs = mirror._scan_local_tree()

    assert {f.name for f in files} == {"top.dat", "mid.dat", "deep.dat"}
    assert {d.name for d in dirs} == {"a", "b"}


def test_scan_local_tree_empty_directory(tmp_path):
    target = tmp_path / "mirror"
    target.mkdir()

    mirror = _StubMirror(_make_config(), target, "http://example.test/data/")
    files, dirs = mirror._scan_local_tree()

    assert files == []
    assert dirs == []


def test_scan_local_tree_symlink_cycle_terminates(tmp_path):
    """A directory symlink pointing back to an ancestor must not cause an
    infinite loop -- the resolved-real-path visited-set guard should
    catch it and simply not descend a second time."""
    target = tmp_path / "mirror"
    target.mkdir()
    sub = target / "sub"
    sub.mkdir()
    (sub / "file.dat").write_bytes(b"x")

    cycle_link = sub / "back_to_root"
    try:
        cycle_link.symlink_to(target, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks not supported in this environment")

    mirror = _StubMirror(_make_config(), target, "http://example.test/data/")

    # Must complete (not hang) and must still find the real file.
    files, dirs = mirror._scan_local_tree()
    assert any(f.name == "file.dat" for f in files)


def test_scan_local_tree_skips_unreadable_directory_without_crashing(tmp_path, monkeypatch):
    """Simulated via monkeypatch rather than real chmod, since tests may
    run as root (common in CI/sandboxes), where permission bits don't
    actually block access."""
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "readable.dat").write_bytes(b"x")
    blocked = target / "blocked"
    blocked.mkdir()
    (blocked / "hidden.dat").write_bytes(b"x")

    real_scandir = os.scandir

    def _flaky_scandir(path):
        if str(path) == str(blocked):
            raise PermissionError(f"simulated: cannot list {path}")
        return real_scandir(path)

    monkeypatch.setattr("mirror_url._core.cleanup.os.scandir", _flaky_scandir)

    mirror = _StubMirror(_make_config(), target, "http://example.test/data/")
    files, dirs = mirror._scan_local_tree()  # must not raise

    assert {f.name for f in files} == {"readable.dat"}
    assert {d.name for d in dirs} == {
        "blocked"
    }  # still listed as an entry, just not descended into


# ---------------------------------------------------------------------------
# clean_obsolete() integration -- PREVIEW mode
# ---------------------------------------------------------------------------


def test_preview_mode_reports_obsolete_files_and_empty_dirs(tmp_path):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")
    (target / "gone.dat").write_bytes(b"no longer remote")
    (target / "empty_now").mkdir()

    remote_files = {base_url + "keep.dat"}

    mirror = _StubMirror(_make_config(cleanup_policy=CleanupPolicy.PREVIEW), target, base_url)
    mirror.clean_obsolete(remote_files)

    # PREVIEW must never touch the filesystem.
    assert (target / "keep.dat").exists()
    assert (target / "gone.dat").exists()
    assert (target / "empty_now").exists()

    assert mirror.metrics.metrics["files_would_delete"] == 1
    assert mirror.metrics.metrics["dirs_would_delete"] == 1


def test_dry_run_behaves_like_preview(tmp_path):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "gone.dat").write_bytes(b"no longer remote")

    mirror = _StubMirror(
        _make_config(cleanup_policy=CleanupPolicy.DELETE, dry_run=True), target, base_url
    )
    mirror.clean_obsolete(set())

    assert (target / "gone.dat").exists()
    assert mirror.metrics.metrics["files_would_delete"] == 1


# ---------------------------------------------------------------------------
# clean_obsolete() integration -- DELETE mode with --confirm-delete
# ---------------------------------------------------------------------------


def test_confirm_delete_count_matches_actual_deletion_count(tmp_path, monkeypatch):
    """Regression guard: the confirm-prompt count and the real deletion
    must come from the same walk and therefore always agree -- previously
    two entirely separate rglob() passes computed each of these, with no
    guarantee they'd see the same tree if anything changed between them."""
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")
    (target / "gone1.dat").write_bytes(b"obsolete")
    (target / "gone2.dat").write_bytes(b"obsolete")

    remote_files = {base_url + "keep.dat"}
    prompts = []

    def _fake_input(prompt):
        prompts.append(prompt)
        return "yes"

    monkeypatch.setattr("builtins.input", _fake_input)

    mirror = _StubMirror(
        _make_config(cleanup_policy=CleanupPolicy.DELETE, confirm_delete=True), target, base_url
    )
    mirror.clean_obsolete(remote_files)

    assert len(prompts) == 1
    assert "2 files" in prompts[0]
    assert (target / "keep.dat").exists()
    assert not (target / "gone1.dat").exists()
    assert not (target / "gone2.dat").exists()


def test_confirm_delete_declined_leaves_everything_untouched(tmp_path, monkeypatch):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "gone.dat").write_bytes(b"obsolete")

    monkeypatch.setattr("builtins.input", lambda prompt: "no")

    mirror = _StubMirror(
        _make_config(cleanup_policy=CleanupPolicy.DELETE, confirm_delete=True), target, base_url
    )
    mirror.clean_obsolete(set())

    assert (target / "gone.dat").exists()


def test_confirm_delete_skips_prompt_when_nothing_obsolete(tmp_path, monkeypatch):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")

    calls = []
    monkeypatch.setattr("builtins.input", lambda prompt: calls.append(prompt) or "yes")

    mirror = _StubMirror(
        _make_config(cleanup_policy=CleanupPolicy.DELETE, confirm_delete=True), target, base_url
    )
    mirror.clean_obsolete({base_url + "keep.dat"})

    assert calls == []
    assert (target / "keep.dat").exists()


# ---------------------------------------------------------------------------
# clean_obsolete() integration -- MOVE mode (previously untested)
# ---------------------------------------------------------------------------


def test_move_mode_relocates_obsolete_files(tmp_path):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")
    (target / "gone.dat").write_bytes(b"obsolete")

    mirror = _StubMirror(_make_config(cleanup_policy=CleanupPolicy.MOVE), target, base_url)
    mirror.clean_obsolete({base_url + "keep.dat"})

    assert (target / "keep.dat").exists()
    assert not (target / "gone.dat").exists()

    obsolete_dir = target.parent / f"{target.name}_obsolete"
    assert (obsolete_dir / "gone.dat").exists()
    assert mirror.metrics.metrics["files_moved"] == 1


def test_move_mode_relocates_empty_obsolete_directories(tmp_path):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")
    obsolete_subdir = target / "old_orbit"
    obsolete_subdir.mkdir()
    (obsolete_subdir / "gone.dat").write_bytes(b"obsolete")

    mirror = _StubMirror(_make_config(cleanup_policy=CleanupPolicy.MOVE), target, base_url)
    mirror.clean_obsolete({base_url + "keep.dat"})

    assert not obsolete_subdir.exists()
    obsolete_dir = target.parent / f"{target.name}_obsolete"
    assert (obsolete_dir / "old_orbit" / "gone.dat").exists()


# ---------------------------------------------------------------------------
# Real DELETE mode without confirmation (no prior direct test of this exact path)
# ---------------------------------------------------------------------------


def test_delete_mode_removes_obsolete_files_and_empty_dirs(tmp_path):
    base_url = "http://example.test/data/"
    target = tmp_path / "mirror"
    target.mkdir()
    (target / "keep.dat").write_bytes(b"still remote")
    obsolete_subdir = target / "old_orbit"
    obsolete_subdir.mkdir()
    (obsolete_subdir / "gone.dat").write_bytes(b"obsolete")

    mirror = _StubMirror(_make_config(cleanup_policy=CleanupPolicy.DELETE), target, base_url)
    mirror.clean_obsolete({base_url + "keep.dat"})

    assert (target / "keep.dat").exists()
    assert not obsolete_subdir.exists()
    assert mirror.metrics.metrics["files_deleted"] == 1
    assert mirror.metrics.metrics["dirs_deleted"] == 1
