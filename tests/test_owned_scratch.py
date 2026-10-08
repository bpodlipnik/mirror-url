"""Destructive recovery requires recorded ownership and an inactive work lease."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mirror_url import DestinationLockError
from mirror_url.destination_lock import DestinationLock
from mirror_url.scratch import OwnedScratch
from test_destination_lock import directory as directory

CHUNK = "chunk_0000_" + "a" * 16 + ".part"


@pytest.fixture
def scratch(directory, tmp_path):
    target = tmp_path / "scratch-mirror"
    target.mkdir()
    state = target / ".mirror-url-state"
    state.mkdir()
    guard = DestinationLock([target, tmp_path / "assembly"])
    owned = OwnedScratch(target, state, tmp_path / "assembly", guard)
    yield owned
    for stream in owned._active.values():
        stream.close()
    guard.close()


def abandon(scratch, work):
    scratch._active.pop(work).close()


@pytest.mark.parametrize("kind,name", [("chunks", CHUNK), ("staging", "staging.streaming")])
def test_recovery_preserves_active_work_then_removes_recorded_abandoned_bytes(scratch, kind, name):
    work = scratch.create(kind, [name])
    (work / name).write_bytes(b"temporary bytes")
    assert scratch.recover() == 0
    assert (work / name).read_bytes() == b"temporary bytes"
    other = OwnedScratch(
        Path(scratch.target),
        scratch.stage_root.parent,
        scratch.chunk_root.parent,
        scratch._destination_lock,
    )
    assert work.exists(), "An independent manager must respect the live work lease"
    abandon(scratch, work)
    assert other.recover() == 1
    assert not work.exists()


def test_default_state_root_and_publication_leave_no_workspaces(directory, tmp_path):
    target = tmp_path / "mirror"
    target.mkdir()
    guard = DestinationLock([target])
    try:
        scratch = OwnedScratch(target, target / ".mirror-url-state", None, guard)
        final = target / "final.bin"
        stage = scratch.staging(final, "assembling")
        stage.write_bytes(b"verified")
        os.replace(stage, final)
        assert scratch.release(stage.parent)
        assert final.read_bytes() == b"verified"
        assert scratch.chunk_root == target / ".mirror-url-state/chunks"
        assert list(scratch.stage_root.iterdir()) == [scratch.stage_root / "owner.json"]
    finally:
        guard.close()


@pytest.mark.parametrize(
    "kind,names",
    [
        ("other", [CHUNK]),
        ("chunks", []),
        ("chunks", None),
        ("chunks", [1]),
        ("chunks", [CHUNK, CHUNK]),
        ("chunks", ["../user-file"]),
        ("staging", ["staging.unknown"]),
    ],
)
def test_invalid_creation_records_cannot_select_user_paths(scratch, kind, names):
    with pytest.raises(ValueError):
        scratch.create(kind, names)


@pytest.mark.parametrize(
    "fault",
    [
        "foreign-target",
        "wrong-token",
        "extra-field",
        "bad-files",
        "not-dict",
        "bad-json",
        "unknown-file",
        "nested-dir",
        "data-symlink",
        "marker-symlink",
        "marker-hardlink",
        "data-hardlink",
        "missing-marker",
        "oversized-marker",
    ],
)
def test_recovery_preserves_every_byte_when_ownership_is_ambiguous(scratch, tmp_path, fault):
    work = scratch.create("chunks", [CHUNK])
    payload = work / CHUNK
    payload.write_bytes(b"preserve transfer")
    marker = work / "owner.json"
    abandon(scratch, work)
    data = json.loads(marker.read_text())
    outside = tmp_path / "outside"
    outside.write_bytes(b"preserve external")
    if fault == "foreign-target":
        data["target"] = str(outside)
    elif fault == "wrong-token":
        data["token"] = "b" * 32
    elif fault == "extra-field":
        data["unexpected"] = True
    elif fault == "bad-files":
        data["files"] = ["../outside"]
    elif fault == "not-dict":
        data = []
    elif fault == "bad-json":
        marker.write_text("{")
    elif fault == "unknown-file":
        (work / "science.txt").write_bytes(b"user data")
    elif fault == "nested-dir":
        (work / CHUNK).unlink()
        (work / CHUNK).mkdir()
    elif fault == "data-symlink":
        payload.unlink()
        payload.symlink_to(outside)
    elif fault == "marker-symlink":
        marker.unlink()
        marker.symlink_to(outside)
    elif fault == "marker-hardlink":
        os.link(marker, tmp_path / "marker-copy")
    elif fault == "data-hardlink":
        os.link(payload, tmp_path / "payload-copy")
    elif fault == "missing-marker":
        marker.unlink()
    elif fault == "oversized-marker":
        marker.write_text(" " * (64 * 1024 + 1))
    if fault in ("foreign-target", "wrong-token", "extra-field", "bad-files", "not-dict"):
        marker.write_text(json.dumps(data))
    assert scratch.recover() == 0
    assert work.exists()
    assert outside.read_bytes() == b"preserve external"
    if payload.is_file() and not payload.is_symlink():
        assert payload.read_bytes() == b"preserve transfer"


@pytest.mark.parametrize("fault", ["root-link", "root-file", "marker-link", "wrong-marker"])
def test_unowned_root_is_rejected_before_recovery(scratch, tmp_path, fault):
    root = scratch.stage_root
    marker = root / "owner.json"
    if fault in ("root-link", "root-file"):
        marker.unlink()
        root.rmdir()
        if fault == "root-link":
            root.symlink_to(tmp_path, target_is_directory=True)
        else:
            root.write_bytes(b"user file")
    elif fault == "marker-link":
        marker.unlink()
        marker.symlink_to(tmp_path / "missing")
    else:
        marker.write_text("{}")
    with pytest.raises((ValueError, OSError)):
        scratch.recover()


@pytest.mark.parametrize("entry", ["file", "directory", "symlink"])
def test_unrecorded_names_and_paths_are_preserved(scratch, tmp_path, entry):
    path = scratch.chunk_root / "user-data"
    if entry == "file":
        path.write_bytes(b"user data")
    elif entry == "directory":
        path.mkdir()
    else:
        path.symlink_to(tmp_path, target_is_directory=True)
    assert scratch.recover() == 0
    assert path.exists()
    assert not scratch._remove(tmp_path, scratch.chunk_root, "chunks")
    named = scratch.chunk_root / ("work_" + "b" * 32)
    named.symlink_to(tmp_path, target_is_directory=True)
    assert scratch.recover() == 0
    named.unlink()
    named.write_bytes(b"also user data")
    assert scratch.recover() == 0


def test_failed_unlink_keeps_manifest_for_a_later_recovery(scratch, monkeypatch):
    work = scratch.create("chunks", [CHUNK])
    (work / CHUNK).write_bytes(b"recover later")
    unlink = Path.unlink

    def denied(path, *args, **kwargs):
        if path.name == CHUNK:
            raise PermissionError("denied")
        return unlink(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "unlink", denied)
        assert not scratch.release(work)
        assert scratch.recover() == 0
        assert (work / "owner.json").is_file()
    assert scratch.recover() == 1


def test_release_preserves_unknown_work_and_can_retire_own_work_after_shutdown(scratch):
    assert not scratch.release(scratch.stage_root)
    work = scratch.create("staging", ["staging.streaming"])
    (work / "unrelated").write_bytes(b"user bytes")
    assert not scratch.release(work)
    another = scratch.create("chunks", [CHUNK])
    (another / CHUNK).write_bytes(b"completed IO")
    scratch._destination_lock.close()
    with pytest.raises(DestinationLockError):
        scratch.recover()
    assert scratch.release(another)
    assert (work / "unrelated").read_bytes() == b"user bytes"


def test_cross_filesystem_staging_fails_before_allocating_work(scratch, monkeypatch):
    final = Path(scratch.target) / "file"
    stat = Path.stat

    def different(path, *args, **kwargs):
        if path == final.parent:
            return SimpleNamespace(st_dev=-1)
        return stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", different)
    with pytest.raises(OSError, match="same filesystem"):
        scratch.staging(final, "assembling")
    assert not scratch._active


def test_manifest_write_failure_closes_the_work_lease(scratch, monkeypatch):
    monkeypatch.setattr("mirror_url.scratch.json.dump", Mock(side_effect=OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        scratch.create("chunks", [CHUNK])
    assert not scratch._active
    assert scratch.recover() == 0


def test_regular_file_owner_and_nonposix_identity_checks(scratch, monkeypatch):
    marker = scratch.stage_root / "owner.json"
    if hasattr(os, "getuid"):
        uid = os.getuid()
        with monkeypatch.context() as patch:
            patch.setattr(os, "getuid", lambda: uid + 1)
            assert not scratch._regular(marker)
    with monkeypatch.context() as patch:
        patch.delattr(os, "getuid", raising=False)
        assert scratch._regular(marker)
