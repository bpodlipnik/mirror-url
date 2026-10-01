"""Filesystem faults must preserve published content and bound cleanup damage."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from mirror_url.download import PartialDownloadManager
from test_download_failure_contract import manager as manager
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_transfer_storage_lifecycle import make_download


def test_assembly_rejects_missing_chunk_list(manager):
    download = make_download(manager)
    download.chunks.clear()
    assert not manager.assemble_file(download)
    assert download.status == "failed"
    assert download.final_path.read_bytes() == b"original"


@pytest.mark.parametrize("size", [5, 6, 7])
def test_mmap_threshold_uses_standard_io_at_boundary(manager, monkeypatch, size):
    download = make_download(manager)
    manager.MMAP_MAX_FILE_SIZE = size
    original = __import__("mmap").mmap
    mmap = Mock(wraps=original)
    monkeypatch.setattr("mirror_url.download.mmap.mmap", mmap)
    assert manager.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"
    assert mmap.call_count == (size > 6)


def test_standard_io_flush_survives_unsupported_fsync(manager, monkeypatch):
    download = make_download(manager)
    manager.MMAP_MAX_FILE_SIZE = 0
    fsync = Mock(side_effect=[None, OSError("unsupported")])
    monkeypatch.setattr("mirror_url.download.os.fsync", fsync)
    assert manager.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"
    assert fsync.call_count == 2


@pytest.mark.parametrize("fault", ["preallocation", "size", "first-byte", "last-byte"])
def test_assembly_detects_filesystem_size_and_readability_faults(manager, monkeypatch, fault):
    download = make_download(manager)
    original_open, original_mmap = open, __import__("mmap").mmap

    def fault_open(path, mode, *args, **kwargs):
        file = original_open(path, mode, *args, **kwargs)
        if str(path).endswith(".assembling"):
            if fault == "preallocation" and mode == "r+b":
                file.truncate(2)
            elif fault == "first-byte" and mode == "rb":
                with original_open(path, "r+b") as truncate:
                    truncate.truncate(0)
            elif fault == "last-byte" and mode == "rb":
                with original_open(path, "r+b") as truncate:
                    truncate.truncate(1)
        return file

    class Mapping:
        def __init__(self, *args):
            self.mapping = original_mmap(*args)

        def __setitem__(self, key, value):
            self.mapping[key] = value

        def flush(self):
            self.mapping.flush()

        def close(self):
            self.mapping.close()
            for path in download.final_path.parent.glob("*.assembling"):
                with original_open(path, "r+b") as file:
                    file.truncate(2)

    monkeypatch.setattr("mirror_url.download.open", fault_open, raising=False)
    if fault == "size":
        monkeypatch.setattr("mirror_url.download.mmap.mmap", Mapping)
    assert not manager.assemble_file(download)
    assert download.final_path.read_bytes() == b"original"
    assert download.status == "failed"
    assert not list(download.final_path.parent.glob("*.assembling"))


def test_mapping_close_error_is_nonfatal_after_flush(manager, monkeypatch):
    download = make_download(manager)
    original = __import__("mmap").mmap

    class Mapping:
        def __init__(self, *args):
            self.mapping = original(*args)

        def __setitem__(self, key, value):
            self.mapping[key] = value

        def flush(self):
            self.mapping.flush()

        def close(self):
            self.mapping.close()
            raise OSError("close failed after flush")

    monkeypatch.setattr("mirror_url.download.mmap.mmap", Mapping)
    assert manager.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"


def test_assembly_cleanup_errors_do_not_mask_publication_failure(manager, monkeypatch):
    download = make_download(manager)
    original_unlink = Path.unlink

    def unlink(path, *args, **kwargs):
        if path.name.endswith(".assembling"):
            raise OSError("temporary file busy")
        return original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    monkeypatch.setattr("mirror_url.download.os.replace", Mock(side_effect=OSError("replace")))
    manager.cleanup_chunks = Mock(side_effect=OSError("cleanup failed"))
    assert not manager.assemble_file(download)
    assert download.final_path.read_bytes() == b"original"
    assert download.status == "failed"
    manager.cleanup_chunks.assert_called_once_with(download)


def test_chunk_cleanup_failure_still_releases_active_tracking(manager, monkeypatch):
    download = make_download(manager)
    manager.active_downloads[download.final_path] = download
    monkeypatch.setattr("mirror_url.download.shutil.rmtree", Mock(side_effect=OSError("busy")))
    manager.cleanup_chunks(download)
    assert download.final_path not in manager.active_downloads
    assert download.final_path.read_bytes() == b"original"


@pytest.mark.parametrize("fault", ["missing-directory", "entry-failure"])
def test_stale_chunk_cleanup_handles_directory_races(manager, monkeypatch, fault):
    if fault == "missing-directory":
        manager.assembly_dir.rmdir()
    else:
        old = manager.assembly_dir / "old"
        old.mkdir()
        os.utime(old, (1, 1))
        monkeypatch.setattr("mirror_url.download.shutil.rmtree", Mock(side_effect=OSError("busy")))
    assert manager.cleanup_stale_chunks() == 0


def test_partial_manager_without_destination_and_reserved_path(tmp_path):
    manager = PartialDownloadManager(None)
    assert not manager.owns_state_directory()
    assert manager.cleanup_stale_partials() == 0
    with pytest.raises(ValueError, match="target directory"):
        manager._state_directory()
    manager = PartialDownloadManager(tmp_path)
    with pytest.raises(ValueError, match="conflicts"):
        manager.get_partial_path(tmp_path / ".MIRROR-URL-STATE" / "a")


def test_partial_directory_creation_error_is_deferred_to_use(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "mkdir", Mock(side_effect=OSError("directory denied")))
    manager = PartialDownloadManager(tmp_path / "missing")
    assert manager.get_stats()["active_partials"] == 0
    assert not manager.owns_state_directory()


def test_partial_activity_ignores_unknown_path_and_stat_race(tmp_path, monkeypatch):
    manager = PartialDownloadManager(tmp_path)
    unknown = tmp_path / "unknown"
    manager.update_activity(unknown, 10)
    assert manager.get_stats()["active_partials"] == 0
    unknown.write_bytes(b"abc")
    original_stat = Path.stat

    def stat(path, *args, **kwargs):
        if path == unknown:
            raise OSError("disappeared")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat)
    assert manager.get_resume_offset(unknown) == 0


def test_partial_cleanup_expires_tracking_but_keeps_recent_file(tmp_path):
    manager = PartialDownloadManager(tmp_path)
    path = manager.register_partial(tmp_path / "a", "https://example.com/a")
    path.write_bytes(b"recent")
    manager.active_partials[path]["last_activity"] = 1
    assert manager.cleanup_stale_partials() == 1
    assert path.read_bytes() == b"recent"
    assert manager.get_stats()["active_partials"] == 0


def test_partial_cleanup_ignores_nondigest_and_survives_unlink_failure(tmp_path, monkeypatch):
    manager = PartialDownloadManager(tmp_path)
    path = manager.get_partial_path(tmp_path / "a")
    path.write_bytes(b"stale")
    os.utime(path, (1, 1))
    unknown = path.parent / ("z" * 64 + manager.partial_suffix)
    unknown.write_bytes(b"remote file")
    original_unlink = Path.unlink

    def unlink(item, *args, **kwargs):
        if item == path:
            raise OSError("permission denied")
        return original_unlink(item, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink)
    assert manager.cleanup_stale_partials() == 0
    assert path.read_bytes() == b"stale"
    assert unknown.read_bytes() == b"remote file"
