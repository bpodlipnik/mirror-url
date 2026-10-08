"""Atomic chunk assembly and owned partial-state retention rules."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from mirror_url.download import PartialDownloadManager
from mirror_url.download_integrity import resume_metadata_path
from mirror_url.models import ChunkInfo, ParallelFileDownload
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel


def make_download(manager, content=b"abcdef"):
    final = manager.mirror.target_dir / "result.bin"
    final.write_bytes(b"original")
    directory = manager.assembly_dir / "chunks"
    directory.mkdir()
    chunks = []
    for index, piece in enumerate((content[:3], content[3:])):
        path = directory / str(index)
        path.write_bytes(piece)
        chunks.append(
            ChunkInfo(
                file_url="https://example.com/root/result.bin",
                final_path=final,
                chunk_id=index,
                start_byte=index * 3,
                end_byte=index * 3 + len(piece) - 1,
                total_chunks=2,
                temp_path=path,
                size=len(piece),
                status="completed",
            )
        )
    return ParallelFileDownload(
        url="https://example.com/root/result.bin",
        final_path=final,
        file_size=len(content),
        chunks=chunks,
        temp_dir=directory,
        server_etag='"v1"',
    )


@pytest.mark.parametrize("fault", ["missing", "short", "overflow", "incomplete", "replace"])
def test_failed_chunk_assembly_preserves_previous_final_file(parallel, monkeypatch, fault):
    download = make_download(parallel)
    if fault == "missing":
        download.chunks[0].temp_path.unlink()
    elif fault == "short":
        download.chunks[0].temp_path.write_bytes(b"x")
    elif fault == "overflow":
        download.chunks[1].start_byte = 5
        download.chunks[1].end_byte = 7
    elif fault == "incomplete":
        download.chunks[0].status = "failed"
    else:
        monkeypatch.setattr(
            "mirror_url.download.os.replace", Mock(side_effect=OSError("publication failed"))
        )
    assert parallel.assemble_file(download) is False
    assert download.final_path.read_bytes() == b"original"
    assert download.status == "failed"
    assert not list(download.final_path.parent.glob("*.assembling"))


def test_chunk_assembly_falls_back_when_mmap_is_unavailable(parallel, monkeypatch):
    download = make_download(parallel)
    monkeypatch.setattr(
        "mirror_url.download.mmap.mmap", Mock(side_effect=OSError("mmap unavailable"))
    )
    assert parallel.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"
    assert download.status == "completed"
    assert not download.temp_dir.exists()


def test_empty_chunk_assembly_publishes_empty_file(parallel):
    download = make_download(parallel, b"")
    assert parallel.assemble_file(download)
    assert download.final_path.read_bytes() == b""


def test_successful_assembly_survives_cache_update_failure(parallel):
    download = make_download(parallel)
    parallel.mirror.cache_manager.save_file_metadata = Mock(side_effect=OSError("cache offline"))
    assert parallel.assemble_file(download)
    assert download.final_path.read_bytes() == b"abcdef"
    assert parallel.mirror.files_processed.value() == 1


def test_stale_chunk_cleanup_keeps_recent_directories_and_unrelated_files(parallel):
    old = parallel.assembly_dir / "old"
    old.mkdir()
    (old / "chunk").write_bytes(b"old")
    os.utime(old, (1, 1))
    fresh = parallel.assembly_dir / "fresh"
    fresh.mkdir()
    unrelated = parallel.assembly_dir / "unrelated"
    unrelated.write_bytes(b"keep")
    assert parallel.cleanup_stale_chunks() == 0
    assert (old / "chunk").read_bytes() == b"old" and fresh.is_dir()
    assert unrelated.read_bytes() == b"keep"


@pytest.fixture
def partials(tmp_path):
    return PartialDownloadManager(tmp_path / "mirror")


def test_partial_path_requires_a_destination(tmp_path):
    manager = PartialDownloadManager(None)
    with pytest.raises(ValueError, match="directory is unavailable"):
        manager.get_partial_path(tmp_path / "a")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("marker", ["invalid-json", "other-owner", "symlink"])
def test_unowned_partial_state_is_rejected_without_modification(partials, tmp_path, marker):
    state = partials.download_dir / partials.STATE_DIRECTORY
    state.mkdir()
    owner = state / "owner.json"
    if marker == "invalid-json":
        owner.write_text("broken")
    elif marker == "other-owner":
        owner.write_text(json.dumps({"format": 1, "target": "/another/mirror"}))
    else:
        outside = tmp_path / "outside.json"
        outside.write_text("important")
        owner.symlink_to(outside)
    before = owner.read_bytes()
    with pytest.raises(ValueError, match="unowned"):
        partials.get_partial_path(partials.download_dir / "a")
    assert owner.read_bytes() == before
    assert partials.cleanup_stale_partials() == 0


def test_stale_partial_cleanup_removes_only_owned_inactive_digest_files(partials, tmp_path):
    old = partials.get_partial_path(partials.download_dir / "old")
    old.write_bytes(b"partial")
    resume_metadata_path(old).write_text("{}")
    os.utime(old, (1, 1))
    active = partials.register_partial(
        partials.download_dir / "active", "https://example.com/active"
    )
    active.write_bytes(b"active")
    os.utime(active, (1, 1))
    unknown = old.parent / "notes.mirror-partial"
    unknown.write_bytes(b"unowned")
    os.utime(unknown, (1, 1))
    outside = tmp_path / "important"
    outside.write_bytes(b"important")
    link = old.parent / ("a" * 64 + ".mirror-partial")
    link.symlink_to(outside)
    assert partials.cleanup_stale_partials() == 1
    assert not old.exists() and not resume_metadata_path(old).exists()
    assert active.read_bytes() == b"active"
    assert unknown.read_bytes() == b"unowned"
    assert outside.read_bytes() == b"important"


def test_partial_activity_and_completion_preserve_final_path(partials):
    final = partials.download_dir / "a"
    partial = partials.register_partial(final, "https://example.com/a", 10)
    partial.write_bytes(b"prefix")
    partials.update_activity(partial, 6)
    assert partials.get_resume_offset(partial) == 6
    assert partials.complete_partial(partial) == final
    assert partials.complete_partial(partial) is None
    assert partials.get_stats()["total_resumes"] == 1
    assert partials.get_resume_offset(Path("does-not-exist")) == 0
