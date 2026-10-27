"""File identities and batch preflight protect bytes before destructive operations."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Event
from unittest.mock import Mock

import pytest

from mirror_url.download_integrity import file_identity, open_owned_partial, replace_verified_file
from mirror_url.exceptions import ConfigError
from mirror_url.filename_mapping import validate_move_targets
from test_cache_lifecycle import cache as cache
from test_download_failure_contract import manager as manager
from test_immediate_audit_fixes import BASE, range_handler, response
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel


@pytest.mark.parametrize(
    "names",
    [
        ["data", "data_obsolete"],
        ["data", "data_obsolete/nested"],
        ["", "nested"],
        ["data/nested", "data/nested_obsolete"],
        ["caf\u00e9", "cafe\u0301_obsolete"],
    ],
)
def test_move_preflight_rejects_archive_intersections_without_creating_trees(tmp_path, names):
    targets = [tmp_path / name for name in names]
    with pytest.raises(ConfigError, match="MOVE archive"):
        validate_move_targets(targets)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("sensitive", [False, True])
def test_move_preflight_respects_actual_case_behavior(tmp_path, monkeypatch, sensitive):
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: sensitive)
    targets = [tmp_path / "data", tmp_path / "DATA_obsolete"]
    if sensitive:
        validate_move_targets(targets)
    else:
        with pytest.raises(ConfigError, match="MOVE archive"):
            validate_move_targets(targets)
    assert list(tmp_path.iterdir()) == []


def test_move_preflight_accepts_disjoint_trees_and_name_boundaries(tmp_path):
    validate_move_targets([])
    validate_move_targets([tmp_path / name for name in ["data", "other", "data_obsolete_backup"]])
    assert list(tmp_path.iterdir()) == []


def test_move_preflight_rejects_filesystem_root():
    with pytest.raises(ConfigError, match="MOVE archive"):
        validate_move_targets([Path(Path.cwd().anchor)])


@pytest.mark.parametrize("alias", ["symlink", "hardlink", "directory", "foreign"])
def test_file_identity_rejects_unowned_or_shared_entries(tmp_path, monkeypatch, alias):
    saved = tmp_path / "saved"
    saved.write_bytes(b"preserve")
    path = tmp_path / "candidate"
    if alias == "symlink":
        path.symlink_to(saved)
    elif alias == "hardlink":
        os.link(saved, path)
    elif alias == "directory":
        path.mkdir()
    else:
        path.write_bytes(b"other owner")
        monkeypatch.setattr(os, "getuid", lambda: path.stat().st_uid + 1, raising=False)
    with pytest.raises(ValueError, match="unsafe or unowned"):
        file_identity(path)
    assert saved.read_bytes() == b"preserve"


def test_file_identity_without_posix_user_ids(tmp_path, monkeypatch):
    path = tmp_path / "file"
    path.write_bytes(b"bytes")
    with monkeypatch.context() as context:
        context.delattr(os, "getuid", raising=False)
        assert file_identity(path)[:3] == (path.stat().st_dev, path.stat().st_ino, 5)
    assert file_identity(tmp_path / "missing") is None


@pytest.mark.parametrize("existing", [False, True])
def test_partial_opens_without_truncation_and_matches_its_handle(tmp_path, existing):
    path = tmp_path / "partial"
    if existing:
        path.write_bytes(b"prefix")
    identity = file_identity(path)
    with open_owned_partial(path, identity) as handle:
        assert handle.read() == (b"prefix" if existing else b"")
        assert (
            os.fstat(handle.fileno()).st_dev,
            os.fstat(handle.fileno()).st_ino,
        ) == file_identity(path)[:2]
    assert path.read_bytes() == (b"prefix" if existing else b"")


@pytest.mark.parametrize("change", ["content", "inode", "handle"])
def test_partial_open_refuses_changed_entry_and_closes_the_descriptor(
    tmp_path, monkeypatch, change
):
    path = tmp_path / "partial"
    path.write_bytes(b"prefix")
    expected = file_identity(path)
    other = tmp_path / "replacement"
    other.write_bytes(b"external bytes")
    if change == "content":
        path.write_bytes(b"external edit")
    elif change == "inode":
        os.replace(other, path)
    opened = []
    real_open = os.open

    def inspect(name, flags, mode=0o777):
        descriptor = real_open(other if change == "handle" else name, flags, mode)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(os, "open", inspect)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="changed before opening"):
        open_owned_partial(path, expected)
    with pytest.raises(OSError):
        os.fstat(opened[0])
    assert path.read_bytes() == before


def test_partial_descriptor_is_closed_when_wrapping_fails(tmp_path, monkeypatch):
    path = tmp_path / "partial"
    opened = []
    real_open = os.open

    def inspect(*args):
        descriptor = real_open(*args)
        opened.append(descriptor)
        return descriptor

    monkeypatch.setattr(os, "open", inspect)
    monkeypatch.setattr(os, "fdopen", Mock(side_effect=OSError("fd wrapper failed")))
    with pytest.raises(OSError, match="wrapper failed"):
        open_owned_partial(path, None)
    with pytest.raises(OSError):
        os.fstat(opened[0])


@pytest.mark.parametrize("existing", [False, True])
def test_partial_hardlink_race_is_rejected_before_truncation(mirror, tmp_path, existing):
    local = mirror.target_dir / "a"
    local.write_bytes(b"old final")
    partial = mirror.partial_manager.get_partial_path(local)
    if existing:
        partial.write_bytes(b"prefix")
    saved = tmp_path / "outside"
    saved.write_bytes(b"precious bytes")

    def replace_partial(*args, **kwargs):
        partial.unlink(missing_ok=True)
        os.link(saved, partial)
        return response(headers={"Content-Length": "3"}, content=b"new")

    mirror.connection_manager.request.side_effect = replace_partial
    assert not mirror._download_file_single(BASE + "a", local)
    assert saved.read_bytes() == b"precious bytes"
    assert partial.read_bytes() == b"precious bytes"
    assert local.read_bytes() == b"old final"
    assert not mirror.partial_manager.active_partials


@pytest.mark.parametrize("cancelled", [False, True])
def test_checked_publication_honors_cancellation_and_file_snapshots(tmp_path, cancelled):
    final = tmp_path / "final"
    final.write_bytes(b"original")
    staging = tmp_path / "staging"
    staging.write_bytes(b"new")
    event = Event()
    if cancelled:
        event.set()
    options = {
        "target": tmp_path,
        "original": file_identity(final),
        "staged_identity": file_identity(staging),
        "max_depth": 2,
        "max_filename_len": 255,
        "cancelled": event,
    }
    if cancelled:
        with pytest.raises(ValueError, match="cancelled"):
            replace_verified_file(staging, final, **options)
        assert final.read_bytes() == b"original" and staging.read_bytes() == b"new"
    else:
        replace_verified_file(staging, final, **options)
        assert final.read_bytes() == b"new" and not staging.exists()


@pytest.mark.parametrize("change", ["staging", "destination", "symlink-parent"])
def test_checked_publication_preserves_unexpected_files(tmp_path, change):
    root = tmp_path / "root"
    root.mkdir()
    directory = root / "sub"
    directory.mkdir()
    final = directory / "final"
    final.write_bytes(b"original")
    staging = tmp_path / "stage"
    staging.write_bytes(b"new")
    options = {
        "target": root,
        "original": file_identity(final),
        "staged_identity": file_identity(staging),
        "max_depth": 2,
        "max_filename_len": 255,
    }
    if change == "staging":
        staging.write_bytes(b"altered")
    elif change == "destination":
        final.write_bytes(b"external edit")
    else:
        moved = tmp_path / "moved"
        directory.rename(moved)
        directory.symlink_to(moved, target_is_directory=True)
    before = final.read_bytes()
    with pytest.raises(ValueError, match="changed"):
        replace_verified_file(staging, final, **options)
    assert final.read_bytes() == before
    assert staging.exists()


@pytest.mark.parametrize("change", ["edit", "hardlink"])
def test_failed_chunks_never_fall_back_over_a_changed_destination(
    manager, mirror, tmp_path, change
):
    mirror.parallel_manager = manager
    manager.use_streaming = False
    manager.connection_manager.request.side_effect = range_handler
    manager.should_use_parallel = lambda _: True
    final = mirror.target_dir / "a"
    final.write_bytes(b"old final")
    saved = tmp_path / "outside"
    saved.write_bytes(b"external edit")

    def failed(download):
        if change == "edit":
            final.write_bytes(b"external edit")
        else:
            final.unlink()
            os.link(saved, final)
        manager.cleanup_chunks(download)
        return False

    manager.download_parallel = failed
    fallback = Mock(side_effect=AssertionError("Unsafe fallback"))
    mirror._download_file_single = fallback
    assert not mirror.download_file_with_resume(BASE + "a", final, 6)
    assert final.read_bytes() == b"external edit"
    assert saved.read_bytes() == b"external edit"
    fallback.assert_not_called()


def test_failed_cache_staging_preserves_unknown_legacy_temporary(cache, monkeypatch):
    legacy = cache.cache_file.with_suffix(".json.tmp")
    legacy.write_bytes(b"unrelated existing temporary")
    monkeypatch.setattr("mirror_url.cache.tempfile.mkstemp", Mock(side_effect=OSError("full")))
    assert not cache.save({}, 0)
    assert legacy.read_bytes() == b"unrelated existing temporary"


def test_partial_publication_uses_closed_file_timestamps(mirror, monkeypatch):
    from mirror_url.filename_mapping import _stat_identity

    def deferred_write_time(info):
        identity = _stat_identity(info)
        return (*identity[:3], identity[3] - 1_000_000, identity[4])

    monkeypatch.setattr("mirror_url._core.downloads._stat_identity", deferred_write_time)
    mirror.connection_manager.request.return_value = response(
        headers={"Content-Length": "3"}, content=b"new"
    )
    local = mirror.target_dir / "a"
    local.write_bytes(b"old")
    assert mirror._download_file_single(BASE + "a", local)
    assert local.read_bytes() == b"new"


def test_missing_chunk_staging_cannot_replace_final(manager):
    from test_transfer_storage_lifecycle import make_download

    download = make_download(manager)
    with pytest.raises(ValueError, match="staging file is missing"):
        manager._publish_staging(download, download.final_path.with_name("missing"))
    assert download.final_path.read_bytes() == b"original"


@pytest.mark.parametrize("method", ["sequential", "parallel", "streaming"])
def test_changed_staging_after_hash_preserves_mirror_final(manager, mirror, monkeypatch, method):
    from mirror_url.download_integrity import file_sha256

    local = mirror.target_dir / "a"
    local.write_bytes(b"old final")
    mirror.config.verify_content = True
    manager.config.verify_content = True

    def changed(path, enabled=True):
        digest = file_sha256(path, enabled)
        path.write_bytes(b"UVWXYZ")
        return digest

    if method == "sequential":
        monkeypatch.setattr("mirror_url._core.downloads.file_sha256", changed)
        mirror.connection_manager.request.return_value = response(
            headers={"Content-Length": "6"}, content=b"ABCDEF"
        )
        assert not mirror._download_file_single(BASE + "a", local)
    else:
        monkeypatch.setattr("mirror_url.download.file_sha256", changed)
        manager.use_streaming = method == "streaming"
        mirror.connection_manager.request.side_effect = range_handler
        download = manager.create_chunks(BASE + "a", local, 6)
        assert download is not None
        assert not manager.download_parallel(download)
        assert download.publication_failed
    assert local.read_bytes() == b"old final"
