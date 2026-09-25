"""Regression test for the streaming chunk-lock narrowing (perf fix).

download_chunk_streaming() used to hold the per-file RLock for the entire
network read loop + fsync of every chunk, fully serializing all chunks of
the same file in "streaming parallel" mode -- defeating the point of
parallel chunk downloads for any single file. The lock is now held only
for the create/pre-allocate race; the actual read+write+fsync happens
outside it, relying on each chunk writing to a disjoint byte range.

This test drives download_chunk_streaming for real, concurrently, against
a local HTTP server that actually supports Range requests, and asserts
the assembled file is byte-for-byte identical to the source -- repeated
across several runs to catch races that don't show up every time.

ConnectionManager's SecureTransport intentionally refuses loopback targets
(SSRF guard -- see test_integration.py) and there's no test-mode bypass
wired through config yet, so this test patches only
ParallelDownloadManager._get_client_for_url to hand back a plain
httpx.Client() pointed at the local server. That's the one seam between
this manager and the security-hardened transport; everything downstream
of it (locking, seeking, writing, fsync, retries) is the real,
unmodified production code path.
"""

from __future__ import annotations

import http.server
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from mirror_url.config import MirrorConfig
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
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(FILE_SIZE))
            self.end_headers()
            self.wfile.write(CONTENT)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(FILE_SIZE))
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


def _make_manager(tmp_path: Path, config: MirrorConfig) -> ParallelDownloadManager:
    pdm = ParallelDownloadManager(
        config=config,
        metrics=MetricsCollector(),
        connection_manager=None,
        bandwidth_limiter=BandwidthLimiter(),
        mirror=None,
    )
    # See module docstring: bypass only the SecureTransport seam, not the
    # per-file locking / write / fsync logic under test.
    client = httpx.Client()
    pdm._get_client_for_url = lambda url: client
    return pdm


def _run_one_concurrent_streaming_download(tmp_path: Path, url: str) -> bytes:
    config = MirrorConfig(
        base_url=url,
        dest_path=str(tmp_path / "dest"),
        log_path=str(tmp_path / "log"),
        no_cache=True,
        streaming_parallel=True,
    )
    pdm = _make_manager(tmp_path, config)
    try:
        final_path = tmp_path / f"out_{threading.get_ident()}_{id(tmp_path)}.bin"

        # Mirror create_chunks()'s real streaming pre-allocation step: the
        # final file already exists at full size before any chunk starts.
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
                )
            )

        # Download all chunks truly concurrently, same as _download_chunk_
        # with_semaphore does via the executor -- this is what exercises
        # the narrowed lock.
        with ThreadPoolExecutor(max_workers=N_CHUNKS) as ex:
            results = list(ex.map(pdm.download_chunk_streaming, chunks))

        assert all(results), f"one or more chunks failed: {results}"
        return final_path.read_bytes()
    finally:
        pdm.shutdown(timeout=2.0)


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
