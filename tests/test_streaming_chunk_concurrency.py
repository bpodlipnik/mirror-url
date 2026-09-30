"""Concurrent range writes are verified before atomic publication.

A local HTTP server drives real concurrent reads and writes. Only the
ConnectionManager client's SSRF transport is replaced for this loopback
fixture; requests still use ConnectionManager's scope and redirect checks.
"""

from __future__ import annotations

import http.server
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from mirror_url.config import MirrorConfig
from mirror_url.connection import ConnectionManager
from mirror_url.download import ParallelDownloadManager
from mirror_url.metrics import MetricsCollector
from mirror_url.models import ChunkInfo
from mirror_url.rate_limiter import BandwidthLimiter

# Deterministic, non-repeating-enough content so any byte written to the
# wrong offset (a real corruption) is detectable.
FILE_SIZE = 517 * 1024 + 3  # deliberately not chunk-aligned
CONTENT = bytes((i * 2654435761) & 0xFF for i in range(FILE_SIZE))
N_CHUNKS = 8


class _RangeHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):  # silence test output
        pass

    def do_GET(self):
        range_header = self.headers.get("Range")
        if range_header:
            unit, _, rng = range_header.partition("=")
            start_s, _, end_s = rng.partition("-")
            start = int(start_s)
            end = int(end_s) if end_s else FILE_SIZE - 1
            body = CONTENT[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{FILE_SIZE}")
            self.send_header("ETag", '"test-v1"')
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(FILE_SIZE))
            self.send_header("ETag", '"test-v1"')
            self.end_headers()
            self.wfile.write(CONTENT)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(FILE_SIZE))
        self.send_header("ETag", '"test-v1"')
        self.end_headers()


@pytest.fixture(scope="module")
def range_server():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RangeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    yield f"http://127.0.0.1:{port}/file.bin"
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def _make_manager(tmp_path: Path, config: MirrorConfig) -> ParallelDownloadManager:
    pdm = ParallelDownloadManager(
        config=config,
        metrics=MetricsCollector(),
        connection_manager=ConnectionManager(config, MetricsCollector()),
        bandwidth_limiter=BandwidthLimiter(),
        mirror=None,
    )
    # Replace only the transport-bearing pooled client for localhost.
    client = httpx.Client()
    pdm.connection_manager.connection_pool.get_client = lambda url: client
    return pdm


def _run_one_concurrent_streaming_download(tmp_path: Path, url: str) -> bytes:
    config = MirrorConfig(
        base_url=url.rsplit("/", 1)[0] + "/",
        dest_path=str(tmp_path / "dest"),
        log_path=str(tmp_path / "log"),
        no_cache=True,
        streaming_parallel=True,
        security_validation=False,
    )
    pdm = _make_manager(tmp_path, config)
    try:
        final_path = tmp_path / f"out_{threading.get_ident()}_{id(tmp_path)}.bin"

        # A staging file is fully allocated before concurrent writers start.
        final_path.parent.mkdir(parents=True, exist_ok=True)
        with open(final_path, "wb") as f:
            f.truncate(FILE_SIZE)

        chunk_size = FILE_SIZE // N_CHUNKS
        chunks = []
        for i in range(N_CHUNKS):
            start = i * chunk_size
            end = start + chunk_size - 1 if i < N_CHUNKS - 1 else FILE_SIZE - 1
            chunks.append(
                ChunkInfo(
                    file_url=url,
                    final_path=final_path,
                    chunk_id=i,
                    start_byte=start,
                    end_byte=end,
                    total_chunks=N_CHUNKS,
                    temp_path=None,
                    size=end - start + 1,
                    direct_write=True,
                    etag='"test-v1"',
                    file_size=FILE_SIZE,
                )
            )

        # Drive disjoint chunk ranges concurrently through real HTTP I/O.
        with ThreadPoolExecutor(max_workers=N_CHUNKS) as ex:
            results = list(ex.map(pdm.download_chunk_streaming, chunks))

        assert all(results), f"one or more chunks failed: {results}"
        return final_path.read_bytes()
    finally:
        pdm.shutdown(timeout=2.0)
        pdm.connection_manager.connection_pool.get_client(url).close()
        pdm.connection_manager.close()


def test_concurrent_streaming_chunks_produce_correct_file(tmp_path, range_server):
    """A single run: byte-for-byte correctness with real concurrent I/O."""
    result = _run_one_concurrent_streaming_download(tmp_path, range_server)
    assert result == CONTENT
    assert len(result) == FILE_SIZE


def test_concurrent_streaming_chunks_repeated_runs_no_corruption(tmp_path, range_server):
    """Races are timing-dependent -- repeat to catch intermittent corruption."""
    for run in range(10):
        run_dir = tmp_path / f"run_{run}"
        run_dir.mkdir()
        result = _run_one_concurrent_streaming_download(run_dir, range_server)
        assert result == CONTENT, f"corruption detected on run {run}"


# ---------------------------------------------------------------------------
# fsync consolidation (perf fix -- see CHANGELOG v3.1.64): a single
# whole-file fsync in download_parallel()'s streaming-completion branch
# replaces the old per-chunk os.fsync() in download_chunk_streaming().
# Drives the full real orchestration path (create_chunks + download_parallel,
# not just download_chunk_streaming directly) end to end against the same
# real local Range server, and counts actual os.fsync() calls.
# ---------------------------------------------------------------------------
def test_streaming_download_fsyncs_once_per_file_not_once_per_chunk(tmp_path, range_server):
    import mirror_url.download as download_mod
    from mirror_url.config import MirrorConfig
    from mirror_url.rate_limiter import BandwidthLimiter

    config = MirrorConfig(
        base_url=range_server.rsplit("/", 1)[0] + "/",
        dest_path=str(tmp_path / "dest"),
        log_path=str(tmp_path / "log"),
        no_cache=True,
        streaming_parallel=True,
        security_validation=False,
    )
    pdm = ParallelDownloadManager(
        config=config,
        metrics=MetricsCollector(),
        connection_manager=ConnectionManager(config, MetricsCollector()),
        bandwidth_limiter=BandwidthLimiter(),
        mirror=None,
    )
    try:
        client = httpx.Client()
        pdm.connection_manager.connection_pool.get_client = lambda url: client
        # Force multiple chunks out of a sub-MB test file without
        # transferring tens of MB over the local server.
        pdm.min_chunk_size = 64 * 1024
        pdm.max_chunks_per_file = N_CHUNKS

        final_path = tmp_path / "out.bin"
        download = pdm.create_chunks(range_server, final_path, FILE_SIZE)
        assert download is not None, "expected multiple chunks for this file/config"
        assert len(download.chunks) >= 2

        fsync_calls = []
        real_fsync = download_mod.os.fsync

        def counting_fsync(fd):
            fsync_calls.append(fd)
            return real_fsync(fd)

        download_mod.os.fsync = counting_fsync
        try:
            ok = pdm.download_parallel(download)
        finally:
            download_mod.os.fsync = real_fsync

        assert ok is True
        assert final_path.read_bytes() == CONTENT
        assert len(fsync_calls) == 1, (
            f"expected exactly 1 fsync (once per file), got {len(fsync_calls)} "
            f"-- fsync should no longer run once per chunk"
        )
    finally:
        pdm.shutdown(timeout=2.0)
        client.close()
        pdm.connection_manager.close()
