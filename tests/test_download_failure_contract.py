"""Download retries, admission failures, and publication safety."""

from __future__ import annotations

import os
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from test_immediate_audit_fixes import BASE, range_handler
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_transfer_storage_lifecycle import make_download


@pytest.fixture
def manager(parallel, monkeypatch):
    # Stop the real maintenance thread before controlling time/faults in tests.
    parallel._shutdown_event.set()
    parallel._cleanup_thread.join(timeout=2)
    assert not parallel._cleanup_thread.is_alive()
    monkeypatch.setattr("mirror_url.download.socket.gethostbyname", Mock(return_value="192.0.2.1"))
    parallel.rate_limiter = Mock()
    return parallel


def chunk_for(manager, *, streaming=False):
    manager.use_streaming = streaming
    manager.connection_manager.request.side_effect = range_handler
    final = manager.mirror.target_dir / "a"
    final.write_bytes(b"original")
    download = manager.create_chunks(BASE + "a", final, 6)
    assert download is not None
    return download, download.chunks[0]


def streaming_response(result):
    response = httpx.Response(
        result.status_code,
        headers=result.headers,
        stream=httpx.ByteStream(result.content),
        request=result.request,
    )
    assert not response.is_closed
    return response


@pytest.mark.parametrize("validator, size", [(None, 6), ('"v1"', None)])
def test_chunk_without_representation_metadata_never_sends_request(manager, validator, size):
    download, chunk = chunk_for(manager)
    manager.connection_manager.request.reset_mock()
    chunk.etag, chunk.file_size = validator, size
    manager.circuit_breaker = None
    assert not manager.download_chunk(chunk)
    manager.connection_manager.request.assert_not_called()
    assert chunk.status == "failed"
    assert download.final_path.read_bytes() == b"original"
    manager.rate_limiter.register_chunk_complete.assert_called_once_with("192.0.2.1")


def test_missing_chunk_output_is_rejected_and_response_closed(manager):
    _, chunk = chunk_for(manager)
    chunk.temp_path = None
    result = streaming_response(
        range_handler(BASE + "a", headers={"Range": "bytes=0-2", "If-Range": '"v1"'})
    )
    manager.connection_manager.request.side_effect = None
    manager.connection_manager.request.return_value = result
    assert not manager.download_chunk(chunk)
    assert result.is_closed


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("fault", ["request", "read", "write"])
@pytest.mark.parametrize("recover", [False, True])
def test_io_retry_restarts_range_closes_responses_and_preserves_final(
    manager, monkeypatch, streaming, fault, recover
):
    download, chunk = chunk_for(manager, streaming=streaming)
    manager.bandwidth_limiter = Mock()
    manager.circuit_breaker = None
    sleeps = Mock()
    monkeypatch.setattr("mirror_url.download.time.sleep", sleeps)
    original_open = open
    results = []
    attempts = 0

    def request(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if fault == "request" and (not recover or attempts == 1):
            raise httpx.ConnectError("connect failed")
        result = streaming_response(range_handler(*args, **kwargs))
        if fault == "read" and (not recover or attempts == 1):

            def broken_read(*_):
                yield b"A"
                raise httpx.ReadError("body interrupted")

            result.iter_bytes = broken_read
        results.append(result)
        return result

    def faulty_open(path, mode, *args, **kwargs):
        if fault == "write" and (not recover or attempts == 1):
            raise OSError("write failed")
        return original_open(path, mode, *args, **kwargs)

    manager.connection_manager.request.side_effect = request
    monkeypatch.setattr("mirror_url.download.open", faulty_open, raising=False)
    ok = manager.download_chunk_streaming(chunk) if streaming else manager.download_chunk(chunk)
    assert ok is recover
    assert chunk.status == ("completed" if recover else "failed")
    assert chunk.retries == (1 if recover else 3)
    assert attempts == (2 if recover else 3)
    assert sleeps.call_count == (1 if recover else 2)
    assert all(result.is_closed for result in results)
    assert download.final_path.read_bytes() == b"original"
    if recover:
        path = chunk.final_path if streaming else chunk.temp_path
        assert path.read_bytes()[:3] == b"ABC"
        assert manager.stats["completed_chunks"] == 1
        manager.bandwidth_limiter.throttle.assert_called()
    else:
        assert manager.stats["failed_chunks"] == 1
    manager.rate_limiter.register_chunk_complete.assert_called_once_with("192.0.2.1")


def test_dns_failure_uses_hostname_for_chunk_accounting(manager, monkeypatch):
    _, chunk = chunk_for(manager)
    monkeypatch.setattr(
        "mirror_url.download.socket.gethostbyname", Mock(side_effect=OSError("DNS"))
    )
    assert manager.download_chunk(chunk)
    manager.rate_limiter.register_chunk_start.assert_called_once_with("example.com")
    manager.rate_limiter.register_chunk_complete.assert_called_once_with("example.com")


def test_invalid_hostname_fails_before_acquiring_permit(manager):
    _, chunk = chunk_for(manager)
    chunk.file_url = "/relative/a"
    manager._get_ip_semaphore = Mock()
    assert not manager._download_chunk_with_semaphore(chunk)
    assert chunk.status == "failed"
    manager._get_ip_semaphore.assert_not_called()


@pytest.mark.parametrize(
    "fault", ["wait-then-success", "timeout", "acquire", "download", "release"]
)
def test_semaphore_faults_release_only_acquired_permits(manager, monkeypatch, fault):
    _, chunk = chunk_for(manager)
    permit = Mock()
    permit.acquire.return_value = True
    manager._get_ip_semaphore = Mock(return_value=permit)
    manager.download_chunk = Mock(return_value=True)
    if fault == "wait-then-success":
        permit.acquire.side_effect = [False, True]
    elif fault == "timeout":
        permit.acquire.return_value = False
        monkeypatch.setattr(
            "mirror_url.download.time",
            SimpleNamespace(time=Mock(side_effect=[0, 0, 10, 121])),
        )
    elif fault == "acquire":
        permit.acquire.side_effect = OSError("permit failure")
    elif fault == "download":
        manager.download_chunk.side_effect = RuntimeError("worker failed")
    else:
        permit.release.side_effect = ValueError("already released")
    assert manager._download_chunk_with_semaphore(chunk) is (
        fault in {"wait-then-success", "release"}
    )
    if fault in {"timeout", "acquire"}:
        permit.release.assert_not_called()
        manager.download_chunk.assert_not_called()
    else:
        permit.release.assert_called_once()
    if fault in {"timeout", "acquire", "download"}:
        assert chunk.status == "failed"


def test_dns_fallback_in_semaphore_wrapper(manager, monkeypatch):
    _, chunk = chunk_for(manager)
    monkeypatch.setattr(
        "mirror_url.download.socket.gethostbyname", Mock(side_effect=OSError("DNS"))
    )
    permit = Mock()
    permit.acquire.return_value = True
    manager._get_ip_semaphore = Mock(return_value=permit)
    manager.download_chunk = Mock(return_value=True)
    assert manager._download_chunk_with_semaphore(chunk)
    manager._get_ip_semaphore.assert_called_once_with("example.com")
    permit.release.assert_called_once()


@pytest.mark.parametrize("streaming", [False, True])
def test_disk_admission_failure_cleans_staging_without_touching_final(manager, streaming):
    download, _ = chunk_for(manager, streaming=streaming)
    directory, staging = download.temp_dir, download.staging_path
    manager.mirror.disk_manager = Mock()
    manager.mirror.disk_manager.check_available.return_value = (False, "full")
    assert not manager.download_parallel(download)
    manager.mirror.disk_manager.check_available.assert_called_once_with(6 if streaming else 12)
    assert download.status == "failed"
    assert download.final_path.read_bytes() == b"original"
    assert not (directory or staging).exists()


@pytest.mark.parametrize("streaming", [False, True])
def test_disk_admission_success_downloads_and_publishes(manager, streaming):
    download, _ = chunk_for(manager, streaming=streaming)
    manager.mirror.disk_manager = Mock()
    manager.mirror.disk_manager.check_available.return_value = (True, None)
    assert manager.download_parallel(download)
    assert download.final_path.read_bytes() == b"ABCDEF"
    assert download.status == "completed"
    manager.mirror.disk_manager.check_available.assert_called_once_with(6 if streaming else 12)


def test_download_rejects_finished_state_without_disk_or_network_work(manager):
    download, _ = chunk_for(manager)
    download.status = "completed"
    manager.connection_manager.request.reset_mock()
    assert not manager.download_parallel(download)
    manager.connection_manager.request.assert_not_called()


def test_parallel_worker_exception_is_counted_and_recovered(manager, monkeypatch):
    download, _ = chunk_for(manager)
    manager.rate_limiter.wait.side_effect = RuntimeError("rate bookkeeping unavailable")
    monkeypatch.setattr(
        "mirror_url.download.socket.gethostbyname", Mock(side_effect=OSError("DNS"))
    )
    failed, ok = Future(), Future()
    failed.set_exception(RuntimeError("worker crashed"))
    ok.set_result(True)
    manager.executor.submit = Mock(side_effect=[failed, ok])
    manager._retry_failed_chunks = Mock(return_value=True)
    assert manager.download_parallel(download)
    assert download.completed_chunks == 1 and download.failed_chunks == 1
    manager._retry_failed_chunks.assert_called_once_with(download)


@pytest.mark.parametrize("fault", ["retry-failed", "exhausted"])
def test_failed_retry_removes_only_temporary_chunks(manager, monkeypatch, fault):
    download, chunk = chunk_for(manager)
    chunk.status = "failed"
    chunk.retries = 3 if fault == "exhausted" else 0
    monkeypatch.setattr("mirror_url.download.time.sleep", Mock())
    manager._download_chunk_with_semaphore = Mock(return_value=False)
    assert not manager._retry_failed_chunks(download)
    assert download.status == "failed"
    assert download.final_path.read_bytes() == b"original"
    assert not download.temp_dir.exists()
    assert manager._download_chunk_with_semaphore.call_count == (fault == "retry-failed")


def test_successful_traditional_retry_assembles_verified_bytes(manager, monkeypatch):
    download, chunk = chunk_for(manager)
    assert manager.download_chunk(download.chunks[1])
    chunk.status = "failed"
    download.completed_chunks, download.failed_chunks = 1, 1
    monkeypatch.setattr("mirror_url.download.time.sleep", Mock())
    assert manager._retry_failed_chunks(download)
    assert download.final_path.read_bytes() == b"ABCDEF"
    assert download.completed_chunks == 2 and download.failed_chunks == 0


@pytest.mark.parametrize("fault", ["no-staging", "incomplete", "size", "fsync", "replace"])
def test_streaming_publication_failure_preserves_original(manager, monkeypatch, fault):
    download, _ = chunk_for(manager, streaming=True)
    staging = download.staging_path
    for chunk in download.chunks:
        chunk.status = "completed"
    if fault == "no-staging":
        staging.unlink()
        download.staging_path = None
    elif fault == "incomplete":
        download.chunks[1].status = "pending"
    elif fault == "size":
        staging.write_bytes(b"short")
    else:
        monkeypatch.setattr("mirror_url.download.os." + fault, Mock(side_effect=OSError(fault)))
    assert not manager._finish_streaming(download)
    assert download.status == "failed"
    assert download.final_path.read_bytes() == b"original"
    assert not staging.exists()


@pytest.mark.parametrize("mode", ["no-mirror", "no-caches", "cache-error"])
@pytest.mark.parametrize("streaming", [False, True])
def test_optional_bookkeeping_does_not_prevent_publication(manager, mode, streaming):
    if streaming:
        download, _ = chunk_for(manager, streaming=True)
        for chunk in download.chunks:
            assert manager.download_chunk_streaming(chunk)
    else:
        download = make_download(manager, b"ABCDEF")
    if mode == "no-mirror":
        manager.mirror = None
    elif mode == "no-caches":
        manager.mirror = SimpleNamespace(
            files_processed=Mock(), total_downloaded_size=Mock(), fs_cache=Mock()
        )
        download.server_etag = None
    else:
        manager.mirror.cache_manager.save_file_metadata = Mock(side_effect=OSError("cache"))
    assert (manager._finish_streaming if streaming else manager.assemble_file)(download)
    assert download.final_path.read_bytes() == b"ABCDEF"
    assert download.status == "completed"


@pytest.mark.parametrize("fault", ["request", "truncate"])
def test_chunk_preparation_falls_back_safely(manager, monkeypatch, fault):
    manager.use_streaming = True
    final = manager.mirror.target_dir / "a"
    final.write_bytes(b"original")
    if fault == "request":
        manager.connection_manager.request.side_effect = httpx.ConnectError("metadata failed")
        assert manager.create_chunks(BASE + "a", final, 6) is None
    else:
        manager.connection_manager.request.side_effect = range_handler
        original_fdopen = os.fdopen

        def fdopen(*args, **kwargs):
            file = original_fdopen(*args, **kwargs)
            file.truncate = Mock(side_effect=OSError("truncate"))
            return file

        monkeypatch.setattr("mirror_url.download.os.fdopen", fdopen)
        download = manager.create_chunks(BASE + "a", final, 6)
        assert download.status == "downloading" and download.staging_path is None
        assert all(not chunk.direct_write and chunk.temp_path for chunk in download.chunks)
        assert not list(final.parent.glob("*.streaming"))
    assert final.read_bytes() == b"original"


def test_single_chunk_and_open_circuit_use_sequential_path(manager):
    assert manager.create_chunks(BASE + "a", manager.mirror.target_dir / "a", 1) is None
    manager.circuit_breaker = Mock()
    manager.circuit_breaker.can_execute.return_value = False
    assert not manager.should_use_parallel(6)
    manager.connection_manager.request.assert_not_called()


def test_small_file_uses_sequential_path_even_when_parallel_is_enabled(manager):
    assert manager.enabled
    assert not manager.should_use_parallel(0)
