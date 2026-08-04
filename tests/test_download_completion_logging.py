"""Tests for the 'Downloaded: <path> (<size>)' log line on the two
chunked-download completion paths (assemble_file / streaming mode).

Production report: a run's summary said "Downloading 13 files" and
"Downloaded: 13", but only 12 "Downloaded: " lines actually appeared in
the log. The 13th file (stereo_kernels.dbx) was fetched via the parallel
chunked-download path, which only logged "Assembling..." /
"Successfully assembled..." -- never the plain "Downloaded: <path>
(<size>)" line every other file gets from _core/downloads.py's
single-shot path. The file WAS correctly downloaded and correctly
counted (files_processed, and by extension the summary's total, is
incremented identically on both paths) -- this was a logging-
completeness gap, not a counting bug: grepping a log for "^Downloaded: "
to get a file inventory silently missed chunk-assembled files.

Fixed by adding the same log line to both completion paths:
assemble_file() (non-streaming chunk assembly) and the streaming-mode
early-return branch inside download_parallel().

Drives the real production methods against real chunk files on disk
(mmap-based assembly for the non-streaming case; pre-completed chunks
that skip network I/O entirely for the streaming case) rather than
mocking them, since the point is to prove the actual code path logs
correctly.
"""

from __future__ import annotations

import logging

from mirror_url.config import MirrorConfig
from mirror_url.download import ParallelDownloadManager
from mirror_url.metrics import MetricsCollector
from mirror_url.models import ChunkInfo, ParallelFileDownload
from mirror_url.rate_limiter import BandwidthLimiter


def _make_manager(tmp_path):
    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path=str(tmp_path / "dest"),
        log_path=str(tmp_path / "log"),
        no_cache=True,
    )
    return ParallelDownloadManager(
        config=config,
        metrics=MetricsCollector(),
        connection_manager=None,
        bandwidth_limiter=BandwidthLimiter(),
        mirror=None,
    )


def test_assemble_file_logs_downloaded_line(tmp_path, caplog):
    """Non-streaming (traditional) chunk assembly: mmap-based assembly of
    real chunk files on disk into the final file."""
    mgr = _make_manager(tmp_path)
    final_path = tmp_path / "dest" / "stereo_kernels.dbx"
    url = "https://example.test/data/stereo_kernels.dbx"

    chunk_a_path = tmp_path / "chunk_a.tmp"
    chunk_a_path.write_bytes(b"A" * 5)
    chunk_b_path = tmp_path / "chunk_b.tmp"
    chunk_b_path.write_bytes(b"B" * 5)

    download = ParallelFileDownload(
        url=url,
        final_path=final_path,
        file_size=10,
        chunks=[
            ChunkInfo(
                file_url=url,
                final_path=final_path,
                chunk_id=0,
                start_byte=0,
                end_byte=4,
                total_chunks=2,
                temp_path=chunk_a_path,
                size=5,
                status="completed",
            ),
            ChunkInfo(
                file_url=url,
                final_path=final_path,
                chunk_id=1,
                start_byte=5,
                end_byte=9,
                total_chunks=2,
                temp_path=chunk_b_path,
                size=5,
                status="completed",
            ),
        ],
    )

    with caplog.at_level(logging.INFO):
        result = mgr.assemble_file(download)

    assert result is True
    assert final_path.read_bytes() == b"AAAAABBBBB"

    messages = [r.message for r in caplog.records]
    downloaded_lines = [m for m in messages if m.startswith("Downloaded: ")]
    assert len(downloaded_lines) == 1
    assert str(final_path) in downloaded_lines[0]
    assert any(m.startswith("✅ Successfully assembled") for m in messages)


def test_streaming_completion_logs_downloaded_line(tmp_path, caplog):
    """Streaming mode: chunks already write directly to final_path as
    they download, so completion is just bookkeeping + logging -- no
    assembly step. All chunks pre-marked 'completed' so
    download_parallel() skips real network I/O entirely (see the
    `if chunk.status == "completed": continue` guard before chunks are
    submitted to the executor) and goes straight to the completion path
    this test is checking."""
    mgr = _make_manager(tmp_path)
    final_path = tmp_path / "dest" / "streamed_file.dat"
    final_path.parent.mkdir(parents=True, exist_ok=True)
    final_path.write_bytes(b"X" * 10)  # streaming already wrote this directly
    url = "https://example.test/data/streamed_file.dat"

    chunk = ChunkInfo(
        file_url=url,
        final_path=final_path,
        chunk_id=0,
        start_byte=0,
        end_byte=9,
        total_chunks=1,
        temp_path=final_path,
        size=10,
        status="completed",
        direct_write=True,
    )
    download = ParallelFileDownload(
        url=url,
        final_path=final_path,
        file_size=10,
        chunks=[chunk],
        status="streaming",
    )

    with caplog.at_level(logging.INFO):
        result = mgr.download_parallel(download)

    assert result is True

    messages = [r.message for r in caplog.records]
    downloaded_lines = [m for m in messages if m.startswith("Downloaded: ")]
    assert len(downloaded_lines) == 1
    assert str(final_path) in downloaded_lines[0]
    assert any(m.startswith("✅ Streaming complete") for m in messages)


def test_non_chunked_single_shot_line_format_matches(tmp_path, caplog):
    """The new lines on both chunked paths must match the exact format
    _core/downloads.py's single-shot path already uses, so a log grep
    for '^Downloaded: ' treats every file uniformly regardless of which
    download method fetched it."""
    mgr = _make_manager(tmp_path)
    final_path = tmp_path / "dest" / "solo.dat"
    url = "https://example.test/data/solo.dat"
    chunk_path = tmp_path / "solo_chunk.tmp"
    chunk_path.write_bytes(b"Z" * 8)

    download = ParallelFileDownload(
        url=url,
        final_path=final_path,
        file_size=8,
        chunks=[
            ChunkInfo(
                file_url=url,
                final_path=final_path,
                chunk_id=0,
                start_byte=0,
                end_byte=7,
                total_chunks=1,
                temp_path=chunk_path,
                size=8,
                status="completed",
            )
        ],
    )

    with caplog.at_level(logging.INFO):
        mgr.assemble_file(download)

    downloaded_line = next(
        r.message for r in caplog.records if r.message.startswith("Downloaded: ")
    )
    # Matches _core/downloads.py's f"Downloaded: {local_path} ({format_bytes(size)})"
    assert downloaded_line == f"Downloaded: {final_path} (8.00 B)"
