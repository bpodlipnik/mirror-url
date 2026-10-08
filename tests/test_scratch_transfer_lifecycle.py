"""Transfer cleanup must wait for every chunk writer and preserve publications."""

from concurrent.futures import Future, TimeoutError
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from test_download_failure_contract import manager as manager
from test_immediate_audit_fixes import BASE, range_handler
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_owned_scratch import abandon
from test_owned_scratch import directory as directory
from test_owned_scratch import scratch as scratch


def owned_download(manager, scratch, streaming):
    manager.scratch = scratch
    manager.use_streaming = streaming
    manager.connection_manager.request.side_effect = range_handler
    final = Path(scratch.target) / "a"
    final.write_bytes(b"original")
    return manager.create_chunks(BASE + "a", final, 6)


@pytest.mark.parametrize("streaming", [False, True])
def test_timed_out_chunk_writers_keep_work_leased_until_last_future_exits(
    manager, scratch, streaming
):
    download = owned_download(manager, scratch, streaming)
    work = download.staging_path.parent if streaming else download.temp_dir

    class StillWriting(Future):
        def result(self, timeout=None):
            raise TimeoutError("IO still running")

    futures = [StillWriting(), StillWriting()]
    manager.executor.submit = Mock(side_effect=futures)
    assert not manager.download_parallel(download)
    manager.cleanup_chunks(download)  # Repeated cleanup must not duplicate callbacks.
    assert download.status == "failed"
    assert work in scratch._active and scratch.recover() == 0
    assert download.final_path.read_bytes() == b"original"
    futures[0].set_result(False)
    assert work.exists() and work in scratch._active
    newer_download = SimpleNamespace(status="completed")
    manager.active_downloads[download.final_path] = newer_download
    # The last writer may still write after its wait timed out.
    if streaming:
        download.staging_path.write_bytes(b"late IO")
    else:
        download.chunks[1].temp_path.write_bytes(b"late IO")
    futures[1].set_result(False)
    assert not work.exists() and not scratch._active
    assert not download.futures
    assert manager.active_downloads[download.final_path] is newer_download
    assert download.final_path.read_bytes() == b"original"


def test_submission_failure_retains_an_already_submitted_writer(manager, scratch):
    download = owned_download(manager, scratch, False)
    future = Future()
    manager.executor.submit = Mock(side_effect=[future, RuntimeError("executor closed")])
    assert not manager.download_parallel(download)
    assert download.temp_dir.exists()
    assert scratch.recover() == 0
    future.set_result(False)
    assert not download.temp_dir.exists()
    assert download.final_path.read_bytes() == b"original"


def test_cancelled_future_retries_its_range_before_publication(manager, scratch):
    download = owned_download(manager, scratch, False)
    cancelled = Future()
    cancelled.cancel()

    def submit(function, chunk):
        if chunk.chunk_id == 0:
            return cancelled
        future = Future()
        future.set_result(function(chunk))
        return future

    manager.executor.submit = Mock(side_effect=submit)
    assert manager.download_parallel(download)
    assert download.final_path.read_bytes() == b"ABCDEF"
    assert not scratch._active


@pytest.mark.parametrize("empty", [False, True])
def test_invalid_assembly_state_retires_idle_owned_work(manager, scratch, empty):
    download = owned_download(manager, scratch, False)
    if empty:
        download.chunks.clear()
    assert not manager.assemble_file(download)
    assert not download.temp_dir.exists()
    assert not scratch._active
    assert download.final_path.read_bytes() == b"original"


def test_managed_streaming_preallocation_failure_retires_stage_before_chunk_fallback(
    manager, scratch, monkeypatch
):
    original = Path.open

    def denied(path, *args, **kwargs):
        if path.name == "staging.streaming":
            raise OSError("preallocation denied")
        return original(path, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "open", denied)
        download = owned_download(manager, scratch, True)
    assert download.status == "downloading" and download.staging_path is None
    assert list(scratch.stage_root.iterdir()) == [scratch.stage_root / "owner.json"]
    assert manager.download_parallel(download)
    assert download.final_path.read_bytes() == b"ABCDEF"
    assert not scratch._active


def test_maintenance_reclaims_recorded_abandoned_work_only_before_shutdown(manager, scratch):
    download = owned_download(manager, scratch, False)
    abandon(scratch, download.temp_dir)
    assert manager.cleanup_stale_chunks() == 1
    assert not download.temp_dir.exists()
    manager._shutdown = True
    assert manager.cleanup_stale_chunks() == 0
