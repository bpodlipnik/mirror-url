"""Local content receipts, bounded hashing, and prepublication failure safety."""

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

from mirror_url.cache import CacheManager
from mirror_url.download_integrity import file_sha256, local_content_matches
from mirror_url.exceptions import ConfigError
from test_download_failure_contract import chunk_for
from test_download_failure_contract import manager as manager
from test_immediate_audit_fixes import BASE, AsyncManager, check_async, response
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_transfer_storage_lifecycle import make_download


@pytest.mark.parametrize(
    "content",
    [b"", b"hello", bytes(range(256)) * 8193],
    ids=["empty", "small", "multiple-chunks"],
)
def test_hash_matches_complete_bytes_with_bounded_reads(tmp_path, monkeypatch, content):
    path = tmp_path / "file"
    path.write_bytes(content)
    original = Path.open
    sizes = []

    def recording_open(self, *args, **kwargs):
        file = original(self, *args, **kwargs)
        read = file.read

        def recording_read(size=-1):
            sizes.append(size)
            return read(size)

        file.read = recording_read
        return file

    monkeypatch.setattr(Path, "open", recording_open)
    assert file_sha256(path) == hashlib.sha256(content).hexdigest()
    assert sizes and set(sizes) == {1024 * 1024}


def test_disabled_hash_does_not_access_file(tmp_path):
    assert file_sha256(tmp_path / "missing", enabled=False) is None


@pytest.mark.parametrize("kind", ["directory", "symlink"])
def test_hash_rejects_nonregular_files(tmp_path, kind):
    path = tmp_path / "file"
    if kind == "directory":
        path.mkdir()
    else:
        target = tmp_path / "target"
        target.write_bytes(b"original")
        path.symlink_to(target)
    with pytest.raises(OSError, match="regular file"):
        file_sha256(path)


def test_hash_rejects_file_changed_during_read(tmp_path, monkeypatch):
    path = tmp_path / "file"
    path.write_bytes(b"original")
    original = Path.open

    def changing_open(self, *args, **kwargs):
        file = original(self, *args, **kwargs)
        read = file.read

        def changing_read(size):
            result = read(size)
            os.utime(path, ns=(1_000_000_000, 1_000_000_000))
            return result

        file.read = changing_read
        return file

    monkeypatch.setattr(Path, "open", changing_open)
    with pytest.raises(OSError, match="changed during"):
        file_sha256(path)


@pytest.mark.parametrize("receipt", [None, 1, "", "a" * 63, "A" * 64, "z" * 64])
def test_invalid_receipt_cannot_validate_content(tmp_path, receipt):
    assert not local_content_matches(tmp_path / "missing", receipt)


def test_unreadable_or_mismatched_content_cannot_validate_receipt(tmp_path):
    path = tmp_path / "file"
    receipt = hashlib.sha256(b"original").hexdigest()
    assert not local_content_matches(path, receipt)
    path.write_bytes(b"modified")
    assert not local_content_matches(path, receipt)
    path.write_bytes(b"original")
    assert local_content_matches(path, receipt)


def cache_receipt(mirror, path, digest):
    mirror.cache_manager = CacheManager(path.parent / "cache.json", mirror.config, mirror.metrics)
    mirror.cache_manager.save_file_metadata(
        path, '"v1"', path.stat().st_mtime, path.stat().st_size, sha256=digest
    )


@pytest.mark.parametrize("status", [200, 304])
@pytest.mark.parametrize("etag", [True, False])
def test_same_size_edit_with_identical_cached_stat_fields_requires_download(mirror, status, etag):
    mirror.config.verify_content = True
    mirror.config.no_etag = not etag
    path = mirror.target_dir / "a"
    path.write_bytes(b"XYZ")
    # Emulate a coarse filesystem clock: every cached stat field matches the
    # current file, but the receipt belongs to different same-size bytes.
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    mirror.connection_manager.request.return_value = response(
        status, {"Content-Length": "3", "ETag": '"v1"'}
    )
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert "If-None-Match" not in mirror.connection_manager.request.call_args.kwargs["headers"]


@pytest.mark.parametrize("receipt", [None, "bad", "0" * 64])
def test_legacy_or_invalid_receipt_forces_download_despite_matching_head(mirror, receipt):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    cache_receipt(mirror, path, receipt)
    mirror.connection_manager.request.return_value = response(headers={"Content-Length": "3"})
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")


def test_verified_content_can_survive_timestamp_only_changes(mirror):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    os.utime(path, ns=(1_000_000_000, 1_000_000_000))
    mirror.connection_manager.request.return_value = response(304)
    assert mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a", use_cache=False)


def test_expired_receipt_requires_download(mirror):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    metadata = mirror.cache_manager.get_file_metadata(path)
    metadata["updated"] = (datetime.now() - timedelta(days=8)).isoformat()
    mirror.connection_manager.request.return_value = response(headers={"Content-Length": "3"})
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert mirror.cache_manager.get_file_metadata(path) is None


@pytest.mark.parametrize("verify_content", [False, True])
@pytest.mark.parametrize(
    "status, headers",
    [
        (404, {}),
        (500, {}),
        (200, {"Content-Length": "invalid"}),
        (200, {"Content-Length": "-1"}),
        (200, {"Content-Length": "3", "Last-Modified": "invalid"}),
    ],
)
def test_valid_local_receipt_cannot_override_invalid_remote_metadata(
    mirror, verify_content, status, headers
):
    mirror.config.verify_content = verify_content
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    mirror.connection_manager.request.return_value = response(status, headers)
    assert not mirror.file_exists_and_up_to_date(path, BASE + "a")
    assert path.read_bytes() == b"ABC"


def test_async_content_verification_rejects_same_size_corruption(mirror):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"XYZ")
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    planned, manager = check_async(mirror, [(BASE + "a", path)], response(304))
    assert planned == [(BASE + "a", path)]
    assert "If-None-Match" not in manager.calls[0][1]


def test_async_hashing_keeps_event_loop_responsive(mirror, monkeypatch):
    mirror.config.verify_content = True
    mirror.config.adaptive_async = False
    mirror.async_connection_manager = AsyncManager(response(304))
    mirror.adaptive_async_manager = None
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    cache_receipt(mirror, path, hashlib.sha256(b"ABC").hexdigest())
    started, release = threading.Event(), threading.Event()
    original = mirror._comparison_metadata
    wait_for = asyncio.wait_for

    async def accelerated_metadata_deadlines(awaitable, timeout):
        # Compress the legacy 30/120 second deadlines to catch accidental
        # cancellation of disk hashing without a slow wall-clock test.
        return await wait_for(awaitable, 0.01 if timeout in (30.0, 120.0) else timeout)

    def slow_hash(*args):
        started.set()
        assert release.wait(5), "hash blocked the event loop"
        time.sleep(0.05)
        return original(*args)

    monkeypatch.setattr(mirror, "_comparison_metadata", slow_hash)
    monkeypatch.setattr(asyncio, "wait_for", accelerated_metadata_deadlines)

    async def check():
        task = asyncio.create_task(mirror._check_files_async([(BASE + "a", path)]))
        try:

            async def heartbeat():
                while not started.is_set():
                    await asyncio.sleep(0.001)
                assert not task.done()

            await asyncio.wait_for(heartbeat(), 2)
        finally:
            release.set()
        assert await task == []

    asyncio.run(check())


def test_missing_only_and_content_verification_are_incompatible(mirror):
    with pytest.raises(ConfigError, match="verify_content with missing_files"):
        type(mirror.config).from_dict(
            {**mirror.config.model_dump(), "verify_content": True, "missing_files": True}
        )


@pytest.mark.parametrize("streaming", [False, True])
def test_hash_failure_before_parallel_publication_preserves_previous_file(
    manager, monkeypatch, streaming
):
    manager.config.verify_content = True
    if streaming:
        download, _ = chunk_for(manager, streaming=True)
        for chunk in download.chunks:
            assert manager.download_chunk_streaming(chunk)
    else:
        download = make_download(manager, b"ABCDEF")
    manager.mirror.cache_manager.save_file_metadata = Mock()
    monkeypatch.setattr("mirror_url.download.file_sha256", Mock(side_effect=OSError("hash read")))
    assert not (manager._finish_streaming if streaming else manager.assemble_file)(download)
    assert download.final_path.read_bytes() == b"original"
    manager.mirror.cache_manager.save_file_metadata.assert_not_called()


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("etag", [None, '"v1"'])
def test_parallel_receipt_is_bound_to_staging_bytes(manager, monkeypatch, streaming, etag):
    manager.config.verify_content = True
    if streaming:
        download, _ = chunk_for(manager, streaming=True)
        for chunk in download.chunks:
            assert manager.download_chunk_streaming(chunk)
    else:
        download = make_download(manager, b"ABCDEF")
    download.server_etag = etag
    manager.mirror.cache_manager.save_file_metadata = Mock()
    replace = os.replace

    def external_edit(source, destination):
        replace(source, destination)
        Path(destination).write_bytes(b"edited")

    monkeypatch.setattr("mirror_url.download.os.replace", external_edit)
    assert (manager._finish_streaming if streaming else manager.assemble_file)(download)
    assert manager.mirror.cache_manager.save_file_metadata.call_args.kwargs["sha256"] == (
        hashlib.sha256(b"ABCDEF").hexdigest()
    )
    assert download.final_path.read_bytes() == b"edited"


def test_single_hash_failure_preserves_previous_file(mirror, monkeypatch):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"original")
    mirror.connection_manager.request.return_value = response(
        headers={"Content-Length": "3", "ETag": '"v1"'}, content=b"ABC"
    )
    monkeypatch.setattr("mirror_url._core.downloads.file_sha256", Mock(side_effect=OSError("hash")))
    assert not mirror._download_file_single(BASE + "a", path)
    assert path.read_bytes() == b"original"


def test_single_receipt_is_bound_to_staging_bytes(mirror, monkeypatch):
    mirror.config.verify_content = True
    path = mirror.target_dir / "a"
    path.write_bytes(b"original")
    cache_receipt(mirror, path, None)
    mirror.connection_manager.request.return_value = response(
        headers={"Content-Length": "3"}, content=b"ABC"
    )
    replace = os.replace

    def external_edit(source, destination):
        replace(source, destination)
        if Path(destination) == path:
            path.write_bytes(b"XYZ")

    monkeypatch.setattr("mirror_url._core.downloads.os.replace", external_edit)
    assert mirror._download_file_single(BASE + "a", path)
    assert (
        mirror.cache_manager.get_file_metadata(path)["sha256"] == hashlib.sha256(b"ABC").hexdigest()
    )
    assert not mirror._comparison_metadata(path)[1]["_local_verified"]
