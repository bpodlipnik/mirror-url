"""Delete-mode housekeeping removes only idle, verified empty application state."""

import json
import os
from pathlib import Path

import pytest

from mirror_url import MirrorURL
from mirror_url.destination_lock import DestinationLock
from mirror_url.download import PartialDownloadManager
from mirror_url.enums import CleanupPolicy
from mirror_url.scratch import OwnedScratch
from test_destination_lock import directory as directory
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote


@pytest.mark.integration
def test_delete_mode_removes_marker_only_state_after_a_current_file_run(remote, config):
    remote.files = {"one.txt": b"already current"}
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        state = mirror.target_dir / ".mirror-url-state"
        payload = mirror.target_dir / "one.txt"
    assert state.is_dir()
    before = payload.stat()
    config.cleanup_policy = CleanupPolicy.DELETE
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert mirror.metrics.get_summary()["files_downloaded"] == 0
    assert not state.exists()
    after = payload.stat()
    assert payload.read_bytes() == b"already current"
    assert (before.st_ino, before.st_mtime_ns) == (after.st_ino, after.st_mtime_ns)


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_delete_mode_removes_state_after_publishing_verified_bytes(remote, config, mode):
    content = bytes(range(256)) * (8192 + 17)
    remote.files = {"large.bin": content}
    config.cleanup_policy = CleanupPolicy.DELETE
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        target = mirror.target_dir
        assert (target / ".mirror-url-state").is_dir(), "Keep state until resource shutdown"
    assert (target / "large.bin").read_bytes() == content
    assert not (target / ".mirror-url-state").exists()


@pytest.mark.integration
@pytest.mark.parametrize(
    "policy", [CleanupPolicy.SAFE_NO_DELETE, CleanupPolicy.PREVIEW, CleanupPolicy.MOVE]
)
def test_other_cleanup_modes_preserve_state(remote, config, policy):
    remote.files = {"one.txt": b"payload"}
    config.cleanup_policy = policy
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        state = mirror.target_dir / ".mirror-url-state"
    assert (state / "owner.json").is_file()


@pytest.mark.integration
def test_delete_dry_run_preserves_existing_state(remote, config):
    remote.files = {"one.txt": b"payload"}
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        state = mirror.target_dir / ".mirror-url-state"
    before = tree_bytes(state)
    config.cleanup_policy = CleanupPolicy.DELETE
    config.dry_run = True
    with MirrorURL(config) as mirror:
        assert mirror.sync()
    assert tree_bytes(state) == before


def tree_bytes(root):
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


@pytest.fixture
def owned(directory, tmp_path):
    target = tmp_path / "mirror"
    manager = PartialDownloadManager(target)
    state = manager._state_directory()
    guard = DestinationLock([target])
    scratch = OwnedScratch(target, state, None, guard)
    yield scratch, guard, state
    for stream in scratch._active.values():
        stream.close()
    guard.close()


def retire(scratch, guard):
    result = []
    guard.close(lambda: result.append(scratch.cleanup_empty_state()))
    return result == [True]


def test_cleanup_requires_shutdown_and_its_only_remaining_lease(owned):
    scratch, guard, state = owned
    assert not scratch.cleanup_empty_state()
    with guard.operation():
        assert not retire(scratch, guard)
        assert state.is_dir()
    assert not scratch.cleanup_empty_state(), "A closed owner must not touch released resources"
    assert state.is_dir()


def test_cleanup_preserves_unreleased_work(owned):
    scratch, guard, state = owned
    work = scratch.create("staging", ["staging.streaming"])
    payload = work / "staging.streaming"
    payload.write_bytes(b"live writer")
    assert not retire(scratch, guard)
    assert payload.read_bytes() == b"live writer"
    assert state.is_dir()


@pytest.mark.parametrize("busy_parent", [False, True])
def test_cli_parent_ownership_is_checked_before_retirement(directory, tmp_path, busy_parent):
    target = tmp_path / "mirror"
    state = PartialDownloadManager(target)._state_directory()
    with_parent = DestinationLock([target])
    child = DestinationLock([target], parent=with_parent)
    scratch = OwnedScratch(target, state, None, child)
    try:
        if busy_parent:
            with with_parent.operation():
                assert not retire(scratch, child)
            assert state.is_dir()
        else:
            assert retire(scratch, child)
            assert not state.exists()
        assert with_parent._handles, "The CLI run retains ownership after child shutdown"
    finally:
        child.close()
        with_parent.close()


def test_external_chunk_assembly_is_preserved(directory, tmp_path):
    target = tmp_path / "mirror"
    state = PartialDownloadManager(target)._state_directory()
    assembly = tmp_path / "assembly"
    guard = DestinationLock([target, assembly])
    scratch = OwnedScratch(target, state, assembly, guard)
    external = scratch.chunk_root / "science.bin"
    external.write_bytes(b"keep external data")
    assert retire(scratch, guard)
    assert not state.exists()
    assert external.read_bytes() == b"keep external data"
    assert (scratch.chunk_root / "owner.json").is_file()


def test_nonstandard_state_location_is_preserved(directory, tmp_path):
    target = tmp_path / "mirror"
    target.mkdir()
    state = target / "user-state"
    guard = DestinationLock([target])
    scratch = OwnedScratch(target, state, None, guard)
    assert not retire(scratch, guard)
    assert state.is_dir()


@pytest.mark.parametrize("root_name", [".", "parallel", "chunks"])
@pytest.mark.parametrize(
    "fault",
    [
        "unknown-file",
        "unfinished-partial",
        "unknown-directory",
        "root-symlink",
        "root-file",
        "missing-marker",
        "marker-symlink",
        "marker-hardlink",
        "foreign-target",
        "bad-json",
        "extra-marker-field",
        "oversized-marker",
    ],
)
def test_ambiguous_or_nonempty_state_is_preserved(owned, tmp_path, root_name, fault):
    scratch, guard, state = owned
    root = state / root_name
    marker = root / "owner.json"
    outside = tmp_path / "external"
    outside.mkdir()
    (outside / "science.dat").write_bytes(b"external science")
    if fault == "unknown-file":
        (root / "science.dat").write_bytes(b"local science")
    elif fault == "unfinished-partial":
        (root / ("a" * 64 + ".mirror-partial")).write_bytes(b"resume bytes")
        (root / ("a" * 64 + ".mirror-partial.json")).write_text('{"etag":"strong"}')
    elif fault == "unknown-directory":
        (root / "user-data").mkdir()
    elif fault in ("root-symlink", "root-file"):
        # Move the original state aside to also verify no preexisting bytes are lost.
        root.rename(tmp_path / "original-state")
        if fault == "root-symlink":
            root.symlink_to(outside, target_is_directory=True)
        else:
            root.write_bytes(b"local file")
    elif fault == "missing-marker":
        marker.unlink()
    elif fault == "marker-symlink":
        marker.unlink()
        marker.symlink_to(outside / "science.dat")
    elif fault == "marker-hardlink":
        os.link(marker, tmp_path / "marker-copy")
    elif fault == "foreign-target":
        data = json.loads(marker.read_text())
        data["target"] = str(outside)
        marker.write_text(json.dumps(data))
    elif fault == "extra-marker-field":
        data = json.loads(marker.read_text())
        data["unknown"] = True
        marker.write_text(json.dumps(data))
    elif fault == "bad-json":
        marker.write_text("{")
    elif fault == "oversized-marker":
        marker.write_text(" " * (64 * 1024 + 1))
    before = tree_bytes(tmp_path)
    assert not retire(scratch, guard)
    assert root.exists() or root.is_symlink()
    assert tree_bytes(tmp_path) == before


@pytest.mark.parametrize(
    "fault", ["unlink", "rmdir", "entry-arrived", "marker-arrived", "replaced-root"]
)
def test_io_failures_preserve_new_entries_and_restore_ownership(owned, monkeypatch, fault):
    scratch, guard, state = owned
    root = scratch.stage_root
    marker = root / "owner.json"
    original_unlink, original_rmdir = Path.unlink, Path.rmdir

    def unlink(path, *args, **kwargs):
        if path == marker and fault == "unlink":
            raise PermissionError("injected marker unlink failure")
        return original_unlink(path, *args, **kwargs)

    def rmdir(path):
        if path == root:
            if fault == "entry-arrived":
                (root / "science.dat").write_bytes(b"keep new entry")
            elif fault == "marker-arrived":
                marker.write_bytes(b"keep replacement marker")
            elif fault == "replaced-root":
                root.rename(state / "replacement-moved-root")
                root.mkdir()
                (root / "science.dat").write_bytes(b"keep replacement directory")
            raise PermissionError("injected directory removal failure")
        return original_rmdir(path)

    monkeypatch.setattr(Path, "unlink", unlink)
    monkeypatch.setattr(Path, "rmdir", rmdir)
    assert not retire(scratch, guard)
    if fault == "marker-arrived":
        assert marker.read_bytes() == b"keep replacement marker"
    elif fault == "replaced-root":
        assert not marker.exists()
        assert (root / "science.dat").read_bytes() == b"keep replacement directory"
    else:
        assert json.loads(marker.read_text()) == {
            "format": 1,
            "target": scratch.target,
            "kind": "staging",
        }
        if fault == "entry-arrived":
            assert (root / "science.dat").read_bytes() == b"keep new entry"


def test_entry_arriving_after_tree_validation_is_preserved(owned, monkeypatch):
    scratch, guard, state = owned
    original = scratch._empty_owned_root
    calls = 0

    def check(root, expected, entries):
        nonlocal calls
        if root == scratch.stage_root:
            calls += 1
            if calls == 2:
                (root / "science.dat").write_bytes(b"new data")
        return original(root, expected, entries)

    monkeypatch.setattr(scratch, "_empty_owned_root", check)
    assert not retire(scratch, guard)
    assert (scratch.stage_root / "science.dat").read_bytes() == b"new data"
    assert (state / "owner.json").is_file()
