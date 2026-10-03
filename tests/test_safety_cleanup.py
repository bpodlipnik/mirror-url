"""Obsolete-file mutations must preserve unrelated data at every failure boundary."""

from __future__ import annotations

import os
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mirror_url.enums import CleanupPolicy
from safety_helpers import ROOT, SafetyMirror


@pytest.mark.parametrize("field", ["target_dir", "target_parsed", "missing_root"])
def test_missing_cleanup_state_preserves_files(tmp_path, field):
    mirror = SafetyMirror(tmp_path)
    victim = tmp_path / "outside"
    victim.write_bytes(b"preserve")
    if field == "missing_root":
        mirror.target_dir.rmdir()
    else:
        setattr(mirror, field, None)
    mirror.clean_obsolete(set())
    assert not mirror._cleanup_path_selected(victim)
    assert victim.read_bytes() == b"preserve"


@pytest.mark.parametrize(
    "kind", ["reserved", "leaf_link", "root_link", "escape", "ancestor_link", "lossy", "outside"]
)
def test_cleanup_selection_protects_unowned_paths(tmp_path, kind):
    mirror = SafetyMirror(tmp_path, max_filename_len=64)
    root = mirror.target_dir
    path = root / "keep"
    if kind == "reserved":
        path = root / ".MIRROR-URL-state" / "keep"
    elif kind == "lossy":
        path = root / ("a" * 70)
    elif kind == "outside":
        path = tmp_path / "outside"
    elif kind == "root_link":
        root.rmdir()
        root.symlink_to(tmp_path, target_is_directory=True)
    elif kind in {"ancestor_link", "escape"}:
        location = tmp_path if kind == "escape" else root / "other"
        if location != tmp_path:
            location.mkdir()
        (root / "alias").symlink_to(location, target_is_directory=True)
        path = root / "alias" / "keep"
    elif kind == "leaf_link":
        path.symlink_to(tmp_path / "absent")
    assert not mirror._cleanup_path_selected(path)


def test_cleanup_exception_while_mapping_preserves_every_file(tmp_path, monkeypatch):
    mirror = SafetyMirror(tmp_path)
    source = mirror.target_dir / "keep"
    source.write_bytes(b"preserve")
    monkeypatch.setattr(
        mirror, "_get_local_path_from_url", Mock(side_effect=OSError("injected mapping failure"))
    )
    mirror.clean_obsolete({ROOT + "keep"})
    assert source.read_bytes() == b"preserve"
    assert mirror.scan_incomplete


@pytest.mark.parametrize(
    "fault", ["root_resolve", "child_resolve", "visited", "entry_stat", "special"]
)
def test_local_walk_handles_filesystem_boundaries(tmp_path, monkeypatch, fault):
    mirror = SafetyMirror(tmp_path)
    child = mirror.target_dir / "child"
    child.mkdir()
    file = child / "keep"
    file.write_bytes(b"preserve")
    resolve = Path.resolve

    def resolve_fault(path, *args, **kwargs):
        if (fault == "root_resolve" and path == mirror.target_dir) or (
            fault == "child_resolve" and path == child
        ):
            raise OSError("injected resolution failure")
        if fault == "visited" and path == child:
            return mirror.target_dir.resolve()
        return resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve_fault)
    if fault == "entry_stat":
        entry = SimpleNamespace(
            path=str(file), is_symlink=Mock(side_effect=OSError("injected stat failure"))
        )
        scandir = os.scandir
        monkeypatch.setattr(
            os,
            "scandir",
            lambda path: nullcontext([entry]) if Path(path) == child else scandir(path),
        )
    if fault == "special":
        if not hasattr(os, "mkfifo"):
            pytest.skip("Native named pipes require POSIX")
        os.mkfifo(mirror.target_dir / "pipe")
    files, dirs = mirror._scan_local_tree()
    assert dirs == [child]
    assert files == ([] if fault in {"visited", "entry_stat"} else [file])
    assert file.read_bytes() == b"preserve"


@pytest.mark.parametrize("policy", [CleanupPolicy.PREVIEW, CleanupPolicy.DELETE])
@pytest.mark.parametrize("fault", ["iterate", "remove", "disappear", "unexpected_root"])
def test_directory_cleanup_faults_preserve_unrelated_data(tmp_path, monkeypatch, policy, fault):
    mirror = SafetyMirror(tmp_path, cleanup_policy=policy)
    empty = mirror.target_dir / "empty"
    empty.mkdir()
    outside = tmp_path / "unrelated"
    outside.write_bytes(b"preserve")
    if fault == "iterate":
        iterdir = Path.iterdir

        def iterate(path):
            if path == empty:
                raise PermissionError("injected listing failure")
            return iterdir(path)

        monkeypatch.setattr(Path, "iterdir", iterate)
    elif fault == "remove":
        rmdir = Path.rmdir

        def remove(path):
            if path == empty:
                raise PermissionError("injected remove failure")
            return rmdir(path)

        monkeypatch.setattr(Path, "rmdir", remove)
    elif fault == "disappear":
        exists = Path.exists

        def disappeared(path):
            if path == empty:
                return False
            return exists(path)

        monkeypatch.setattr(Path, "exists", disappeared)
    else:
        monkeypatch.setattr(mirror, "_scan_local_tree", lambda: ([], [mirror.target_dir, empty]))
    mirror.clean_obsolete(set())
    assert outside.read_bytes() == b"preserve"
    if policy == CleanupPolicy.DELETE and fault in {"iterate", "remove"}:
        assert mirror.metrics.metrics["cleanup_failed_operations"] >= 1


def test_failed_unlink_and_metadata_cleanup_preserve_source(tmp_path, monkeypatch):
    mirror = SafetyMirror(tmp_path)
    source = mirror.target_dir / "gone"
    source.write_bytes(b"preserve on failure")
    unlink = Path.unlink

    def denied(path, *args, **kwargs):
        if path == source:
            raise PermissionError("injected unlink failure")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", denied)
    mirror.cache_manager.cleanup_stale_metadata.side_effect = OSError("injected cache failure")
    mirror.clean_obsolete(set())
    assert source.read_bytes() == b"preserve on failure"
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 1


def test_successful_cleanup_removes_stale_metadata(tmp_path):
    mirror = SafetyMirror(tmp_path)
    mirror.cache_manager.cleanup_stale_metadata.return_value = 2
    mirror.clean_obsolete(set())
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 0


def test_failed_archive_creation_cannot_move_source(tmp_path, monkeypatch):
    mirror = SafetyMirror(tmp_path, cleanup_policy=CleanupPolicy.MOVE)
    source = mirror.target_dir / "gone"
    source.write_bytes(b"preserve")
    mkdir = Path.mkdir

    def denied(path, *args, **kwargs):
        if path == tmp_path / "mirror_obsolete":
            raise PermissionError("injected archive creation failure")
        return mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", denied)
    mirror.clean_obsolete(set())
    assert source.read_bytes() == b"preserve"
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 1


def test_archive_root_link_is_rejected(tmp_path):
    mirror = SafetyMirror(tmp_path, cleanup_policy=CleanupPolicy.MOVE)
    source = mirror.target_dir / "gone"
    source.write_bytes(b"preserve")
    (tmp_path / "mirror_obsolete").symlink_to(tmp_path, target_is_directory=True)
    mirror.clean_obsolete(set())
    assert source.read_bytes() == b"preserve"
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 1


def test_timestamp_collision_preserves_both_archives(tmp_path, monkeypatch):
    mirror = SafetyMirror(tmp_path, cleanup_policy=CleanupPolicy.MOVE)
    source = mirror.target_dir / "gone.bin"
    source.write_bytes(b"current source")
    archive = tmp_path / "mirror_obsolete"
    archive.mkdir()
    (archive / "gone.bin").write_bytes(b"first archive")
    (archive / "gone_1000000.bin").write_bytes(b"second archive")
    monkeypatch.setattr("mirror_url._core.cleanup.time.time", lambda: 1000)
    mirror.clean_obsolete(set())
    assert source.read_bytes() == b"current source"
    assert (archive / "gone.bin").read_bytes() == b"first archive"
    assert (archive / "gone_1000000.bin").read_bytes() == b"second archive"
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 1


@pytest.mark.parametrize("directory", [False, True])
def test_archive_collision_cannot_redirect_move(tmp_path, monkeypatch, directory):
    mirror = SafetyMirror(tmp_path, cleanup_policy=CleanupPolicy.MOVE)
    source = mirror.target_dir / ("empty" if directory else "gone.bin")
    source.mkdir() if directory else source.write_bytes(b"obsolete")
    archive = tmp_path / "mirror_obsolete"
    archive.mkdir()
    old = archive / source.name
    old.mkdir() if directory else old.write_bytes(b"previous archive")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "unrelated").write_bytes(b"preserve")
    name = "empty_1000000" if directory else "gone_1000000.bin"
    collision = archive / name
    collision.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr("mirror_url._core.cleanup.time.time", lambda: 1000)
    mirror.clean_obsolete(set())
    assert source.exists()
    assert sorted(p.name for p in outside.iterdir()) == ["unrelated"]
    assert (outside / "unrelated").read_bytes() == b"preserve"
    assert collision.is_symlink()
    assert mirror.metrics.metrics["cleanup_failed_operations"] == 1


def test_archive_ancestor_link_cannot_redirect_move_within_archive(tmp_path):
    mirror = SafetyMirror(tmp_path, cleanup_policy=CleanupPolicy.MOVE)
    source = mirror.target_dir / "sub" / "gone.bin"
    source.parent.mkdir()
    source.write_bytes(b"obsolete")
    archive = tmp_path / "mirror_obsolete"
    archive.mkdir()
    unrelated = archive / "unrelated"
    unrelated.mkdir()
    (archive / "sub").symlink_to(unrelated, target_is_directory=True)
    mirror.clean_obsolete(set())
    assert source.read_bytes() == b"obsolete"
    assert list(unrelated.iterdir()) == []
    assert mirror.metrics.metrics["cleanup_failed_operations"] >= 1
