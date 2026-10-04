"""Parallel + partial download engines.

Originally extracted from ``mirror_url.py``:
``ParallelDownloadManager`` (orig. 4215-5640), ``PartialDownloadManager`` (orig.
9204-9366).

Two provably-dead local assignments (``domain = parsed.netloc`` in
``download_chunk`` / ``download_chunk_streaming``, never read) were dropped to
keep the linter clean; behavior is unchanged.
"""

from __future__ import annotations

import atexit
import hashlib
import json
import logging
import mmap
import os
import random
import secrets
import shutil
import socket
import tempfile
import threading
import time
import uuid
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from pathlib import Path
from threading import RLock, Semaphore
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import httpx

from .circuit_breaker import ChunkCircuitBreaker
from .concurrency import UnifiedConcurrencyManager
from .connection import ConnectionManager
from .constants import (
    PARTIAL_MAX_AGE_HOURS,
    PARTIAL_SUFFIX,
    STREAMING_WRITE_BUFFER_SIZE,
)
from .destination_lock import destination_operation
from .download_integrity import (
    clear_resume_metadata,
    content_length,
    file_sha256,
    strong_etag,
    validate_range,
)
from .enums import DownloadMethod
from .exceptions import ChunkAssemblyError, ChunkDownloadError
from .models import ChunkInfo, ParallelFileDownload
from .rate_limiter import BandwidthLimiter, ChunkAwareRateLimiter
from .scratch import OwnedScratch
from .utils import bounded_executor_shutdown, exponential_backoff, format_bytes

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from ._core._typing import MirrorHost
    from .config import MirrorConfig
    from .metrics import MetricsCollector


class ParallelDownloadManager:
    """Manages parallel chunk downloads for multiple files"""

    MMAP_MAX_FILE_SIZE = 50 * 1024**3

    def __init__(
        self,
        config: MirrorConfig,
        metrics: MetricsCollector,
        connection_manager: ConnectionManager,
        bandwidth_limiter: BandwidthLimiter,
        concurrency_manager: UnifiedConcurrencyManager = None,
        mirror: Optional[MirrorHost] = None,
    ):
        """Initialize parallel download manager."""
        self.config = config
        self.metrics = metrics
        self.connection_manager = connection_manager
        self.bandwidth_limiter = bandwidth_limiter
        self.concurrency_manager = concurrency_manager
        self.mirror = mirror
        self._destination_lock = getattr(mirror, "_destination_lock", None)
        self.scratch: Optional[OwnedScratch] = None

        # Determine download mode from config
        self.enabled = False
        self.use_streaming = False
        self.auto_mode = False

        # Check for sequential mode first
        if hasattr(config, "sequential_downloads") and config.sequential_downloads:
            self.enabled = False
            self.use_streaming = False
            self.auto_mode = False
            logging.info("📥 Sequential mode selected")

        # Check for streaming parallel mode
        elif hasattr(config, "streaming_parallel") and config.streaming_parallel:
            self.enabled = True
            self.use_streaming = True
            self.auto_mode = False
            logging.info("🚀 Streaming parallel mode selected")

        # Check for traditional parallel mode
        elif config.parallel_downloads:
            self.enabled = True
            self.use_streaming = False
            self.auto_mode = False
            logging.info("📦 Traditional parallel mode (temp files)")

        # Auto-select mode (no arguments)
        else:
            self.enabled = False  # Start with disabled, auto-select will decide
            self.use_streaming = False
            self.auto_mode = True
            logging.info("🤖 Auto-select mode (will choose best method at runtime)")

        self.max_chunks_per_file = config.max_chunks_per_file
        self.min_chunk_size = config.min_chunk_size_mb * 1024 * 1024
        self.max_parallel_chunks = config.max_parallel_chunks_total

        # State tracking
        # self.active_downloads: Dict[Path, ParallelFileDownload] = {}
        self.lock = RLock()

        # Chunk I/O workers follow the validated configured limit.
        max_chunk_threads = self.max_parallel_chunks
        capped_workers = max_chunk_threads

        if (
            self.config.use_shared_thread_pool
            and concurrency_manager
            and concurrency_manager.shared_pool
        ):
            self.executor = concurrency_manager.shared_pool
            self.own_executor = False
            logging.info("📦 Using shared thread pool for parallel downloads")
        else:
            self.executor = ThreadPoolExecutor(
                max_workers=capped_workers, thread_name_prefix="mirror_download"
            )
            self.own_executor = True
            logging.info(
                f"📦 Using DEDICATED download thread pool: {capped_workers} threads (capped)"
            )

        # Per-IP semaphore tracking
        self._ip_semaphores: Dict[str, Semaphore] = {}
        # FIX (memory leak): _ip_semaphores previously grew without bound
        # (one Semaphore per unique IP, never removed). For mirrors that
        # touch many hosts this leaked over time. Track last-touched time
        # and prune entries idle longer than _IP_SEM_IDLE_TTL.
        self._ip_semaphores_last_used: Dict[str, float] = {}
        self._ip_semaphores_lock = RLock()
        self._IP_SEM_IDLE_TTL = 600.0  # seconds

        # IMPROVED: Periodic cleanup of per-IP semaphores and idle resources
        self._ip_semaphores_cleanup_interval = 300  # 5 minutes between cleanups
        self._last_ip_semaphore_cleanup = time.time()
        self._ip_semaphore_max_idle = 600  # 10 minutes idle timeout

        # IMPROVED: Download tracking with bounded size to prevent memory leaks
        self.active_downloads: OrderedDict[Path, ParallelFileDownload] = OrderedDict()
        self.max_active_downloads = 100  # Prevent unbounded growth

        # Interruptible wait for the periodic cleanup loop: lets shutdown()
        # wake it immediately instead of waiting out the rest of a
        # time.sleep(30) interval (see _periodic_cleanup / shutdown). Created
        # before the thread starts, since the thread reads it on its very
        # first loop iteration.
        self._shutdown_event = threading.Event()

        # Start maintenance only after all fallible setup has succeeded.
        self._cleanup_thread: Optional[threading.Thread] = None
        self._shutdown = False

        # Rate limiter
        disable_scaling = (
            config.trusted_server
            or config.auto_concurrency
            or config.disable_rate_scaling
            or config.parallel_optimization_mode == "aggressive"
        )

        self.rate_limiter = ChunkAwareRateLimiter(
            delay=config.request_delay,
            per_ip=config.security_validation,
            disable_scaling=disable_scaling,
        )

        # Circuit breaker
        self.circuit_breaker = ChunkCircuitBreaker() if config.circuit_breaker_enabled else None

        # Assembly directory
        if config.chunk_assembly_dir:
            self.assembly_dir = config.chunk_assembly_dir
        elif (
            self._destination_lock is not None
            and mirror is not None
            and mirror._computed_target_path is not None
        ):
            # Initialized after target connection/path checks, and discoverable
            # by the next exclusive owner after a crash.
            self.assembly_dir = mirror._computed_target_path / ".mirror-url-state" / "chunks"
        else:
            # Use secure temporary directory with unique name
            unique_id = secrets.token_hex(8)
            self.assembly_dir = Path(tempfile.gettempdir()) / f"mirrorurl_chunks_{unique_id}"
            # FIX (memory leak via atexit): the previous closure captured
            # `self` by reference (`self.assembly_dir` inside the function
            # body), which kept every PDM instance alive for the lifetime
            # of the process — atexit holds the closure, the closure holds
            # self, and explicit shutdown() couldn't release it. For
            # embedders that build/destroy MirrorURL instances repeatedly
            # this leaked one PDM (plus all its threads/locks/dicts) per
            # job. Capture only the Path so the closure no longer pins
            # `self`.
            _assembly_dir_for_cleanup = self.assembly_dir

            def cleanup_assembly_dir():
                try:
                    shutil.rmtree(_assembly_dir_for_cleanup, ignore_errors=True)
                except Exception:
                    pass

            atexit.register(cleanup_assembly_dir)

        try:
            if self._destination_lock is None or config.chunk_assembly_dir is not None:
                self.assembly_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            self.shutdown()
            raise

        # Statistics
        self.stats = {
            "total_chunks": 0,
            "completed_chunks": 0,
            "failed_chunks": 0,
            "start_time": time.time(),
        }
        self.stats_lock = RLock()
        self._shutdown = False

        logging.info(
            f"📦 Parallel download manager: {max_chunk_threads} threads, "
            f"max_chunks={self.max_chunks_per_file}, min_chunk={config.min_chunk_size_mb}MB, "
            f"total_chunks={self.max_parallel_chunks}"
        )

        self._cleanup_thread = threading.Thread(
            target=self._periodic_cleanup, daemon=True, name=f"pdm_cleanup_{id(self)}"
        )
        self._cleanup_thread.start()

    def _periodic_cleanup(self) -> None:
        """Periodically clean up stale resources to prevent memory leaks.

        This method runs in a background daemon thread and periodically:
        1. Removes idle per-IP semaphores that haven't been used recently
        2. Cleans up completed/failed download entries from tracking
        3. Prevents unbounded growth of internal data structures
        """
        while not getattr(self, "_shutdown", False):
            try:
                # wait() returns True immediately if shutdown() sets the
                # event, instead of blocking for the full 30s like
                # time.sleep() did.
                if self._shutdown_event.wait(timeout=30):
                    break
                self._cleanup_idle_resources()
            except Exception as e:
                # Don't let cleanup errors crash the thread
                logging.debug(f"Periodic cleanup error (non-critical): {e}")

    @destination_operation
    def _cleanup_idle_resources(self) -> None:
        """Clean up idle per-IP semaphores and stale download tracking entries."""
        now = time.time()

        # ========================================================================
        # 1. CLEAN UP DOWNLOAD TRACKING ENTRIES (ALWAYS RUN)
        # ========================================================================
        with self.lock:
            # Snapshot keys explicitly to prevent runtime errors during mutation
            stale_downloads = [
                path
                for path, download in list(self.active_downloads.items())
                if download.status in ("completed", "failed", "cancelled")
                and now - download.start_time > 3600
            ]
            for path in stale_downloads:
                self.active_downloads.pop(path, None)

            # 🔴 CRITICAL: Enforce hard limit to prevent unbounded memory growth
            if len(self.active_downloads) > self.max_active_downloads:
                # Find oldest completed/failed entries
                completed = sorted(
                    [
                        (p, d)
                        for p, d in list(self.active_downloads.items())
                        if d.status in ("completed", "failed", "cancelled")
                    ],
                    key=lambda x: x[1].start_time,
                )
                # Trim down to 50% of max limit
                target = self.max_active_downloads // 2
                to_remove = completed[: max(0, len(completed) - target)]
                for path, _ in to_remove:
                    self.active_downloads.pop(path, None)

        # ========================================================================
        # 2. CLEAN UP IP SEMAPHORES (TIME-BASED)
        # ========================================================================
        if now - self._last_ip_semaphore_cleanup >= self._ip_semaphores_cleanup_interval:
            with self._ip_semaphores_lock:
                self._last_ip_semaphore_cleanup = now

                # ⚠️ SAFETY GUARD: Only prune when the table grows large.
                # Since `_last_used` is only set on creation (not reuse),
                # pruning blindly would delete semaphores for active long-running downloads.
                # This threshold matches the cheap prune in `_get_ip_semaphore`.
                if len(self._ip_semaphores) > 64:
                    idle_threshold = self._ip_semaphore_max_idle
                    stale_ips = [
                        ip
                        for ip, last_used in self._ip_semaphores_last_used.items()
                        if now - last_used > idle_threshold
                    ]
                    for ip in stale_ips:
                        self._ip_semaphores.pop(ip, None)
                        self._ip_semaphores_last_used.pop(ip, None)

                    if stale_ips:
                        logging.debug(
                            f"Cleaned {len(stale_ips)} idle IP semaphores "
                            f"(remaining: {len(self._ip_semaphores)})"
                        )

    def should_use_parallel(self, file_size: int) -> bool:
        """Determine if parallel download should be used for a file."""
        if not self.enabled:
            return False
        if file_size < self.min_chunk_size:
            return False
        # ChunkCircuitBreaker is per-file, keep as is
        if self.circuit_breaker and not self.circuit_breaker.can_execute():
            return False
        return True

    def get_chunk_count(self, file_size: int) -> int:
        """Calculate optimal number of chunks for a file."""
        if not self.should_use_parallel(file_size):
            return 1
        chunks = max(1, file_size // self.min_chunk_size)
        chunks = min(chunks, self.max_chunks_per_file)
        return chunks

    @destination_operation
    def create_chunks(
        self, url: str, local_path: Path, file_size: int
    ) -> Optional[ParallelFileDownload]:
        """Create chunk tasks for a file, using appropriate mode."""
        chunk_count = self.get_chunk_count(file_size)
        if chunk_count <= 1:
            return None
        try:
            response = self.connection_manager.request(
                url, method="HEAD", headers={"Accept-Encoding": "identity"}
            )
            etag = strong_etag(response.headers)
            if (
                response.status_code != 200
                or response.headers.get("Accept-Ranges", "").lower() != "bytes"
                or content_length(response.headers) != file_size
                or not etag
            ):
                return None
        except Exception as e:
            logging.debug(f"Range metadata unavailable for {url}: {e}")
            return None
        download = ParallelFileDownload(
            url=url, final_path=local_path, file_size=file_size, server_etag=etag
        )
        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            try:
                download.server_last_modified = parsedate_to_datetime(last_modified).timestamp()
            except (ValueError, TypeError, OverflowError):
                logging.debug("Ignoring invalid Last-Modified header for %s", url)

        if self.use_streaming:
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                if self.scratch is not None:
                    download.staging_path = self.scratch.staging(local_path, "streaming")
                    with download.staging_path.open("xb") as f:
                        f.truncate(file_size)
                else:
                    fd, name = tempfile.mkstemp(
                        prefix=".mirror-url-", suffix=".streaming", dir=local_path.parent
                    )
                    download.staging_path = Path(name)
                    with os.fdopen(fd, "wb") as f:
                        f.truncate(file_size)
                download.status = "streaming"
            except OSError as e:
                if download.staging_path:
                    download.staging_path.unlink(missing_ok=True)
                    if self.scratch is not None:
                        self.scratch.release(download.staging_path.parent)
                    download.staging_path = None
                logging.warning(f"Streaming pre-allocation failed, using chunk files: {e}")
        names = [f"chunk_{i:04d}_{secrets.token_hex(8)}.part" for i in range(chunk_count)]
        if download.staging_path is None:
            if self.scratch is not None:
                download.temp_dir = self.scratch.create("chunks", names)
            else:
                download.temp_dir = self.assembly_dir / f"chunks_{uuid.uuid4().hex}"
                download.temp_dir.mkdir(parents=True, exist_ok=True)
            download.status = "downloading"

        # Calculate chunk sizes
        chunk_size = file_size // chunk_count
        chunks = []
        for i in range(chunk_count):
            start = i * chunk_size
            end = start + chunk_size - 1 if i < chunk_count - 1 else file_size - 1
            chunk = ChunkInfo(
                file_url=url,
                final_path=download.staging_path or local_path,
                chunk_id=i,
                start_byte=start,
                end_byte=end,
                total_chunks=chunk_count,
                temp_path=download.temp_dir / names[i] if download.temp_dir else None,
                size=end - start + 1,
                direct_write=download.staging_path is not None,
                etag=etag,
                file_size=file_size,
            )
            chunks.append(chunk)
        download.chunks = chunks
        with self.lock:
            self.active_downloads[local_path] = download
        with self.stats_lock:
            self.stats["total_chunks"] += chunk_count
            self.metrics.increment("parallel_files")
            self.metrics.increment("total_chunks", chunk_count)
        return download

    def download_chunk(self, chunk: ChunkInfo) -> bool:
        """Download a verified range into a temporary chunk file."""
        return self._download_verified_chunk(chunk, streaming=False)

    def download_chunk_streaming(self, chunk: ChunkInfo) -> bool:
        """Write a verified range into a preallocated, unpublished staging file."""
        return self._download_verified_chunk(chunk, streaming=True)

    @destination_operation
    def _download_verified_chunk(self, chunk: ChunkInfo, *, streaming: bool) -> bool:
        chunk.status = "downloading"
        parsed = urlparse(chunk.file_url)
        hostname = parsed.hostname
        if hostname is None:
            chunk.status = "failed"
            logging.error("Chunk URL has no hostname")
            with self.stats_lock:
                self.stats["failed_chunks"] += 1
            self.metrics.increment("chunk_failures")
            return False
        try:
            ip = socket.gethostbyname(hostname)
        except Exception:
            ip = hostname
        self.rate_limiter.register_chunk_start(ip)
        try:
            if not chunk.etag or chunk.file_size is None:
                raise ChunkDownloadError("Chunk has no representation validator or total size")
            headers = {
                "Range": f"bytes={chunk.start_byte}-{chunk.end_byte}",
                "If-Range": chunk.etag,
                "Accept-Encoding": "identity",
            }
            attempt = 0
            while True:
                response = None
                try:
                    # Both modes share scope checks, manual redirects, retry
                    # headers and circuit breaking through ConnectionManager.
                    response = self.connection_manager.request(
                        chunk.file_url,
                        method="GET",
                        stream=True,
                        headers=dict(headers),
                        allow_redirects=True,
                        timeout=httpx.Timeout(
                            self.config.timeout * 2, connect=10.0, read=self.config.timeout * 3
                        ),
                    )
                    validate_range(
                        response, chunk.start_byte, chunk.end_byte, chunk.file_size, chunk.etag
                    )
                    path = chunk.final_path if streaming else chunk.temp_path
                    if path is None:
                        raise ChunkDownloadError("Missing chunk output path")
                    # Retry the entire chunk, never append a possibly stale
                    # tail. Concurrent writers only touch disjoint ranges.
                    with open(path, "r+b" if streaming else "wb") as f:
                        if streaming:
                            f.seek(chunk.start_byte)
                        downloaded = 0
                        for data in response.iter_bytes(STREAMING_WRITE_BUFFER_SIZE):
                            if downloaded + len(data) > chunk.size:
                                raise ChunkDownloadError("Response exceeds chunk boundary")
                            f.write(data)
                            downloaded += len(data)
                            if self.bandwidth_limiter:
                                self.bandwidth_limiter.throttle(len(data))
                        f.flush()
                        if not streaming:
                            os.fsync(f.fileno())
                    if downloaded != chunk.size:
                        raise ChunkDownloadError(
                            f"Chunk length mismatch: {downloaded} != {chunk.size}"
                        )
                    chunk.downloaded = downloaded
                    chunk.status = "completed"
                    with self.stats_lock:
                        self.stats["completed_chunks"] += 1
                    self.metrics.increment("chunk_downloads")
                    self.metrics.add_bytes(chunk.size)
                    if self.circuit_breaker:
                        self.circuit_breaker.record_chunk_success(chunk.file_url, parsed.netloc)
                    return True
                except (httpx.RequestError, OSError) as e:
                    chunk.retries += 1
                    if attempt == 2:
                        raise
                    logging.debug(f"Chunk {chunk.chunk_id} retry: {e}")
                    time.sleep(exponential_backoff(attempt))
                    attempt += 1
                finally:
                    if response is not None:
                        response.close()
        except Exception as e:
            logging.error(f"Chunk {chunk.chunk_id} failed: {e}")
            chunk.status = "failed"
            with self.stats_lock:
                self.stats["failed_chunks"] += 1
            self.metrics.increment("chunk_failures")
            if self.circuit_breaker:
                self.circuit_breaker.record_chunk_failure(chunk.file_url, parsed.netloc)
            return False
        finally:
            self.rate_limiter.register_chunk_complete(ip)

    @destination_operation
    def download_parallel(self, download: ParallelFileDownload) -> bool:
        """Download all chunks of a file in parallel with batch rate limiting - FIXED"""
        if download.status != "downloading" and download.status != "streaming":
            return False

        # Disk space check.
        #
        # FIX: previously this always required ``file_size * 2``. The 2x
        # factor exists because traditional parallel mode keeps each chunk
        # as a temp file AND then assembles them into the final output, so
        # peak disk usage really is ~2x. Streaming mode writes directly
        # into a pre-allocated staging file once and never duplicates the
        # bytes anywhere, so the right factor is 1x. The old check
        # incorrectly rejected streaming downloads when the user had only
        # ~file_size of headroom.
        disk_manager = getattr(self.mirror, "disk_manager", None)
        if disk_manager is not None:
            multiplier = 1 if download.status == "streaming" else 2
            required_space = download.file_size * multiplier
            ok, error = disk_manager.check_available(required_space)
            if not ok:
                logging.error(f"Insufficient disk space for parallel download: {error}")
                download.status = "failed"
                self.cleanup_chunks(download)
                return False

        # OPTIMIZATION: Apply rate limit ONCE per file, not per chunk
        parsed = urlparse(download.url)
        ip: Optional[str]
        try:
            if parsed.hostname is None:
                raise ValueError("URL has no hostname")
            ip = socket.gethostbyname(parsed.hostname)
        except Exception:
            ip = parsed.hostname

        # Single rate limit wait for all chunks of this file
        try:
            self.rate_limiter.wait(ip)
        except Exception as e:
            logging.debug(f"Rate limiter wait failed: {e}")

        # Submit all chunks WITH SEMAPHORE WRAPPER
        futures = []
        try:
            for chunk in download.chunks:
                if chunk.status == "completed":
                    continue
                future = self.executor.submit(self._download_chunk_with_semaphore, chunk)
                download.futures.append(future)
                futures.append((future, chunk))
        except Exception as error:
            logging.error("Could not submit parallel chunks: %s", error)
            download.status = "failed"
            self.cleanup_chunks(download)
            return False

        # Wait for all chunks
        completed = sum(c.status == "completed" for c in download.chunks)
        failed = 0
        for future, chunk in futures:
            try:
                result = future.result(timeout=120)
                if result:
                    completed += 1
                else:
                    failed += 1
                    logging.error(f"Chunk {chunk.chunk_id} failed")
            except Exception as e:
                failed += 1
                if future.done():
                    chunk.status = "failed"
                logging.error(f"Chunk {chunk.chunk_id} exception: {e}")

        with download.lock:
            download.completed_chunks = completed
            download.failed_chunks = failed

        if any(not future.done() for future, _ in futures):
            # A wait timeout does not stop IO. Keep scratch leased until the
            # last writer exits, and do not publish or retry its ranges.
            download.status = "failed"
            self.cleanup_chunks(download)
            return False

        if failed > 0:
            # Try to recover failed chunks
            if failed <= len(download.chunks) // 2:
                logging.warning(f"Retrying {failed} failed chunks for {download.final_path.name}")
                return self._retry_failed_chunks(download)
            else:
                logging.error(f"Too many chunk failures ({failed}) for {download.final_path.name}")
                download.status = "failed"
                self.cleanup_chunks(download)
                return False

        if download.status == "streaming":
            return self._finish_streaming(download)
        return self.assemble_file(download)

    @destination_operation
    def _finish_streaming(self, download: ParallelFileDownload) -> bool:
        """Publish only after all validated chunks are complete and flushed."""
        staging_work = download.staging_path.parent if download.staging_path else None
        try:
            if not download.staging_path or any(c.status != "completed" for c in download.chunks):
                raise ChunkAssemblyError("Streaming download is incomplete")
            if download.staging_path.stat().st_size != download.file_size:
                raise ChunkAssemblyError("Streaming size mismatch")
            with open(download.staging_path, "r+b") as f:
                os.fsync(f.fileno())
            digest = file_sha256(
                download.staging_path, enabled=getattr(self.config, "verify_content", False)
            )
            os.replace(download.staging_path, download.final_path)
            download.staging_path = None
            self._restore_server_timestamp(download)
            download.status = "completed"
            try:
                self.metrics.increment("chunk_assemblies")
                if self.mirror:
                    self.mirror.files_processed.increment(1)
                    self.mirror.total_downloaded_size.add(download.file_size)
                    if hasattr(self.mirror, "cache_manager") and (
                        download.server_etag or digest is not None
                    ):
                        self.mirror.cache_manager.save_file_metadata(
                            download.final_path,
                            download.server_etag,
                            time.time(),
                            download.file_size,
                            sha256=digest,
                        )
                    if hasattr(self.mirror, "fs_cache"):
                        self.mirror.fs_cache.invalidate(download.final_path)
            except Exception as e:
                logging.warning(f"Cache/metrics update failed after streaming publication: {e}")
            logging.info(f"Downloaded: {download.final_path} ({format_bytes(download.file_size)})")
            logging.info(f"✅ Streaming complete: {download.final_path.name}")
            return True
        except (OSError, ChunkAssemblyError) as e:
            logging.error(f"Streaming completion failed: {e}")
            download.status = "failed"
            return False
        finally:
            self.cleanup_chunks(download)
            if self.scratch is not None and staging_work is not None:
                self.scratch.release(staging_work)

    def _get_ip_semaphore(self, ip: str) -> Semaphore:
        """Atomically fetch-or-create the per-IP semaphore."""
        now = time.time()
        with self._ip_semaphores_lock:
            # ✅ FIX: Update heartbeat on EVERY access, not just creation
            self._ip_semaphores_last_used[ip] = now

            sem = self._ip_semaphores.get(ip)
            if sem is None:
                per_ip_limit = self.max_parallel_chunks if self.config.trusted_server else 4
                sem = Semaphore(per_ip_limit)
                self._ip_semaphores[ip] = sem

                # Cheap idle-prune when table grows beyond 64 entries
                if len(self._ip_semaphores) > 64:
                    cutoff = now - self._IP_SEM_IDLE_TTL
                    stale = [
                        k
                        for k, t in self._ip_semaphores_last_used.items()
                        if t < cutoff and k != ip
                    ]
                    for k in stale:
                        self._ip_semaphores.pop(k, None)
                        self._ip_semaphores_last_used.pop(k, None)
            return sem

    @destination_operation
    def _download_chunk_with_semaphore(self, chunk: ChunkInfo) -> bool:
        parsed = urlparse(chunk.file_url)

        if parsed.hostname is None:
            logging.error(f"Cannot download chunk: no hostname in URL {chunk.file_url}")
            chunk.status = "failed"
            return False

        try:
            ip = socket.gethostbyname(parsed.hostname)
        except Exception as e:
            logging.debug(f"DNS resolution failed for {parsed.hostname}: {e}")
            ip = parsed.hostname

        ip_semaphore = self._get_ip_semaphore(ip)
        acquired = False
        max_wait = 120
        start = time.time()

        while not acquired and (time.time() - start) < max_wait:
            try:
                # Use per-IP semaphore instead of global
                acquired = ip_semaphore.acquire(timeout=10)
                if not acquired:
                    logging.debug(
                        f"Waiting for per-IP semaphore for chunk {chunk.chunk_id}... "
                        f"({int(time.time() - start)}s elapsed)"
                    )
                    continue
            except Exception as e:
                logging.error(f"Semaphore acquire exception for chunk {chunk.chunk_id}: {e}")
                chunk.status = "failed"
                return False

        if not acquired:
            logging.error(
                f"Timeout acquiring per-IP semaphore for chunk {chunk.chunk_id} after {max_wait}s"
            )
            chunk.status = "failed"
            return False

        try:
            if chunk.direct_write:
                return self.download_chunk_streaming(chunk)  # Uses direct file write
            else:
                return self.download_chunk(chunk)  # Uses temp file

        except Exception as e:
            logging.error(f"Chunk download failed for chunk {chunk.chunk_id}: {e}")
            chunk.status = "failed"
            return False
        finally:
            try:
                ip_semaphore.release()
            except ValueError:
                pass

    def _retry_failed_chunks(self, download: ParallelFileDownload) -> bool:
        """Retry failed chunks sequentially to avoid deadlocks."""
        max_retries = 3
        for chunk in download.chunks:
            if chunk.status == "failed":
                chunk.retries += 1
                chunk.status = "pending"
                if chunk.retries <= max_retries:
                    wait_time = exponential_backoff(chunk.retries - 1)
                    time.sleep(wait_time)
                    # FIX (NoneType crash): streaming chunks have temp_path=None.
                    # Calling download_chunk() unconditionally dereferenced
                    # chunk.temp_path and crashed any streaming retry. Dispatch
                    # on chunk.direct_write so streaming chunks go through the
                    # streaming path. Both paths still go through the per-IP
                    # semaphore wrapper to keep concurrency control consistent.
                    retry_ok = self._download_chunk_with_semaphore(chunk)
                    if retry_ok:
                        with download.lock:
                            download.completed_chunks += 1
                            download.failed_chunks -= 1
                    else:
                        download.status = "failed"
                        self.cleanup_chunks(download)
                        return False
                else:
                    logging.error(f"Chunk {chunk.chunk_id} exceeded max retries")
                    download.status = "failed"
                    self.cleanup_chunks(download)
                    return False
        if download.status == "streaming" or any(c.direct_write for c in download.chunks):
            return self._finish_streaming(download)
        return self.assemble_file(download)

    @destination_operation
    def assemble_file(self, download: ParallelFileDownload) -> bool:
        """Assemble chunks into final file using memory-mapped I/O - PRODUCTION HARDENED v3.1.

        Critical guarantees:
        1. Thread-safe state transitions via download.lock
        2. Zero resource leaks (proper mmap lifecycle)
        3. Graceful fallback to standard I/O for files >50GB
        4. Atomic replacement + size verification
        """
        # ====================================================================
        # PHASE 1: VALIDATE INPUT STATE & SNAPSHOT
        # ====================================================================
        with download.lock:
            if not download.chunks:
                logging.error(f"No chunks to assemble for {download.final_path}")
                download.status = "failed"
                self.cleanup_chunks(download)
                return False

            incomplete = [c for c in download.chunks if c.status != "completed"]
            if incomplete:
                logging.error(
                    f"Cannot assemble {download.final_path}: "
                    f"{len(incomplete)} chunks not completed "
                    f"(ids: {[c.chunk_id for c in incomplete]})"
                )
                download.status = "failed"
                self.cleanup_chunks(download)
                return False

            # Snapshot critical state under lock to avoid holding it during I/O
            file_size = download.file_size
            chunks = sorted(download.chunks, key=lambda c: c.chunk_id)
            download.status = "assembling"

        logging.info(
            f"🔧 Assembling {download.final_path.name} from {len(chunks)} chunks "
            f"({format_bytes(file_size)})"
        )

        # ====================================================================
        # PHASE 2: PREPARE TEMPORARY FILE
        # ====================================================================
        unique_id = secrets.token_hex(8)
        temp_assembly = download.final_path.parent / f".mirror-url-{unique_id}.assembling"
        temp_file_moved = False

        try:
            download.final_path.parent.mkdir(parents=True, exist_ok=True)
            if self.scratch is not None:
                temp_assembly = self.scratch.staging(download.final_path, "assembling")

            # ====================================================================
            # PHASE 3: HANDLE 0-BYTE FILES
            # ====================================================================
            if file_size == 0:
                with open(temp_assembly, "wb") as f:
                    f.flush()
                    os.fsync(f.fileno())
            else:
                # ====================================================================
                # PHASE 4: PRE-ALLOCATE AND ASSEMBLE
                # ====================================================================
                # Pre-allocate to prevent fragmentation
                with open(temp_assembly, "wb") as f:
                    f.seek(file_size - 1)
                    f.write(b"\0")
                    f.flush()
                    os.fsync(f.fileno())

                # Decide whether to use mmap (failsafe for >50GB files)
                USE_MMAP = file_size < self.MMAP_MAX_FILE_SIZE
                mm = None

                with open(temp_assembly, "r+b") as f:
                    if f.seek(0, 2) != file_size:
                        raise ChunkAssemblyError(
                            f"Pre-allocation failed: file size is {f.tell()}, expected {file_size}"
                        )

                    # Create mmap with proper fallback
                    if USE_MMAP:
                        try:
                            mm = mmap.mmap(f.fileno(), 0)
                        except (ValueError, OSError) as e:
                            logging.warning(f"mmap failed ({e}), falling back to standard I/O")
                            USE_MMAP = False

                    try:
                        for chunk in chunks:
                            if not chunk.temp_path or not chunk.temp_path.exists():
                                raise ChunkAssemblyError(
                                    f"Chunk {chunk.chunk_id} file missing: {chunk.temp_path}"
                                )

                            expected_size = chunk.end_byte - chunk.start_byte + 1
                            written = 0
                            for data in self._read_chunk_data(chunk.temp_path):
                                if written + len(data) > expected_size:
                                    raise ChunkAssemblyError(
                                        f"Chunk {chunk.chunk_id} exceeds {expected_size} bytes"
                                    )
                                target_start = chunk.start_byte + written
                                target_end = target_start + len(data)
                                if target_end > file_size:
                                    raise ChunkAssemblyError(
                                        f"Chunk {chunk.chunk_id} would write past end of file: "
                                        f"target_end={target_end}, file_size={file_size}"
                                    )
                                if mm is not None:
                                    mm[target_start:target_end] = data
                                else:
                                    f.seek(target_start)
                                    f.write(data)
                                written += len(data)
                            if written != expected_size:
                                raise ChunkAssemblyError(
                                    f"Chunk {chunk.chunk_id} size mismatch: "
                                    f"expected {expected_size} bytes, got {written} bytes"
                                )

                        # Flush to disk
                        if mm is not None:
                            mm.flush()
                        else:
                            f.flush()
                            try:
                                os.fsync(f.fileno())
                            except OSError:
                                pass  # fsync unsupported on some FS
                    finally:
                        # Always close mmap safely
                        if mm is not None:
                            try:
                                mm.close()
                            except Exception as e:
                                logging.debug(f"Error closing memory mapping: {e}")

            # ====================================================================
            # PHASE 5: VERIFY ASSEMBLED FILE
            # ====================================================================
            actual_size = temp_assembly.stat().st_size
            if actual_size != file_size:
                raise ChunkAssemblyError(
                    f"Assembled file size mismatch: expected {file_size}, got {actual_size}"
                )

            if file_size > 0:
                with open(temp_assembly, "rb") as vf:
                    vf.seek(0)
                    if not vf.read(1):
                        raise ChunkAssemblyError("Cannot read first byte")
                    vf.seek(file_size - 1)
                    if not vf.read(1):
                        raise ChunkAssemblyError("Cannot read last byte")

            # ====================================================================
            # PHASE 6: ATOMIC REPLACEMENT
            # ====================================================================
            # The assembly file is in the destination directory, so a
            # failed replace must leave the old file intact. A copy/move
            # fallback could destroy it before that fallback completes.
            digest = file_sha256(
                temp_assembly, enabled=getattr(self.config, "verify_content", False)
            )
            os.replace(temp_assembly, download.final_path)
            temp_file_moved = True
            self._restore_server_timestamp(download)

            # ====================================================================
            # PHASE 7: UPDATE METRICS AND CACHE (Non-fatal)
            # ====================================================================
            try:
                self.metrics.increment("chunk_assemblies")
                if self.mirror:
                    self.mirror.files_processed.increment(1)
                    self.mirror.total_downloaded_size.add(file_size)
                    if hasattr(self.mirror, "cache_manager") and (
                        download.server_etag or digest is not None
                    ):
                        self.mirror.cache_manager.save_file_metadata(
                            download.final_path,
                            download.server_etag,
                            time.time(),
                            file_size,
                            sha256=digest,
                        )
                    if hasattr(self.mirror, "fs_cache"):
                        self.mirror.fs_cache.invalidate(download.final_path)
            except Exception as cache_err:
                logging.warning(f"Cache/metrics update failed (assembly succeeded): {cache_err}")

            # ====================================================================
            # SUCCESS
            # ====================================================================
            # Same log line the single-shot (non-chunked) download path emits
            # (_core/downloads.py) -- previously only the "Assembling.../
            # Successfully assembled..." lines above were logged here, so a
            # chunk-assembled file never got the plain "Downloaded: <path>
            # (<size>)" line every other file does, even though it's counted
            # identically in the run summary. Kept as its own line (not
            # merged into "Successfully assembled") so grepping logs for
            # "^Downloaded: " reliably finds every downloaded file,
            # regardless of which code path fetched it.
            logging.info(f"Downloaded: {download.final_path} ({format_bytes(file_size)})")
            logging.info(
                f"✅ Successfully assembled {download.final_path.name} ({format_bytes(file_size)})"
            )
            with download.lock:
                download.status = "completed"
            return True

        except ChunkAssemblyError as e:
            logging.error(f"Assembly error for {download.final_path.name}: {e}")
            with download.lock:
                download.status = "failed"
            return False
        except Exception as e:
            logging.error(
                f"Unexpected assembly error for {download.final_path.name}: {type(e).__name__}: {e}",
                exc_info=True,
            )
            with download.lock:
                download.status = "failed"
            return False
        finally:
            # ====================================================================
            # PHASE 8: CLEANUP
            # ====================================================================
            if self.scratch is not None:
                self.scratch.release(temp_assembly.parent)
            elif not temp_file_moved and temp_assembly.exists():
                try:
                    temp_assembly.unlink()
                    logging.debug(f"Removed temp assembly file: {temp_assembly}")
                except OSError as e:
                    logging.warning(f"Failed to remove temp file {temp_assembly}: {e}")
            try:
                self.cleanup_chunks(download)
            except Exception as e:
                logging.warning(f"Chunk cleanup error (non-fatal): {e}")

    def _restore_server_timestamp(self, download: ParallelFileDownload) -> None:
        """Restore mtime before saving the cache's local file identity."""
        if download.server_last_modified is not None:
            try:
                timestamp = download.server_last_modified
                os.utime(download.final_path, times=(timestamp, timestamp))
            except (OSError, ValueError, TypeError, OverflowError) as error:
                logging.warning(
                    "Could not restore server timestamp for %s: %s", download.final_path, error
                )

    def _read_chunk_data(self, chunk_path: Path) -> Iterator[bytes]:
        """Read bounded blocks, including when mmap is unavailable."""
        with open(chunk_path, "rb") as f:
            while True:
                data = f.read(STREAMING_WRITE_BUFFER_SIZE)
                if not data:
                    break
                yield data

    def cleanup_chunks(self, download: ParallelFileDownload) -> None:
        """Remove temporary chunk files."""
        with download.lock:
            pending = [future for future in download.futures if not future.done()]
            if pending:
                if download.cleanup_deferred:
                    return
                download.cleanup_deferred = True
            else:
                download.futures.clear()
        if pending:

            def retire(_future):
                if all(future.done() for future in download.futures):
                    self.cleanup_chunks(download)

            for future in pending:
                future.add_done_callback(retire)
            return
        try:
            if download.staging_path:
                if self.scratch is not None:
                    self.scratch.release(download.staging_path.parent)
                else:
                    download.staging_path.unlink(missing_ok=True)
            if download.temp_dir and download.temp_dir.exists():
                if self.scratch is not None:
                    self.scratch.release(download.temp_dir)
                else:
                    shutil.rmtree(download.temp_dir)
        except Exception as e:
            logging.debug(f"Cleanup error: {e}")

        with self.lock:
            if self.active_downloads.get(download.final_path) is download:
                self.active_downloads.pop(download.final_path)

    def cleanup_stale_chunks(self) -> int:
        """Recover recorded work only; age/name alone never establish ownership."""
        if self.scratch is not None and not self._shutdown:
            return self.scratch.recover()
        return 0

    def get_stats(self) -> Dict[str, Any]:
        """Get parallel download statistics"""
        with self.lock:
            active = len(self.active_downloads)
            total_chunks = sum(len(d.chunks) for d in self.active_downloads.values())
            return {
                "enabled": self.enabled,
                "active_files": active,
                "active_chunks": total_chunks,  # ✅ KEY FIX: This key was missing
                "max_chunks_per_file": self.max_chunks_per_file,
                "min_chunk_size_mb": self.min_chunk_size / (1024 * 1024),
                "max_parallel_chunks": self.max_parallel_chunks,
                "assembly_dir": str(self.assembly_dir),
                "rate_limiter": {
                    "active_chunks_per_ip": dict(self.rate_limiter.active_chunks_per_ip)
                }
                if hasattr(self.rate_limiter, "active_chunks_per_ip")
                else {},
            }

    def shutdown(self, timeout: float = 15.0) -> None:
        """Shutdown manager with proper cleanup of all resources.

        Args:
            timeout: Max seconds to wait for the download executor's
                in-flight worker threads to finish. Bounded rather than an
                unconditional block, since MirrorURL.cleanup() (which calls
                this from _signal_handler) has its own 30s overall deadline
                -- a single stuck worker must not be able to consume all of
                it silently. See utils.bounded_executor_shutdown.
        """
        # Set shutdown flag first to stop background threads
        self._shutdown = True
        # Wake the cleanup loop immediately instead of leaving it to finish
        # its current 30s wait -- otherwise the 5s join() below times out
        # (and warns) whenever shutdown happens more than 5s before the
        # loop's next scheduled wake-up.
        if getattr(self, "_shutdown_event", None) is not None:
            self._shutdown_event.set()

        # Stop the periodic cleanup thread
        if hasattr(self, "_cleanup_thread") and self._cleanup_thread is not None:
            if self._cleanup_thread.is_alive():
                logging.debug("Stopping periodic cleanup thread...")
                self._cleanup_thread.join(timeout=5.0)
                if self._cleanup_thread.is_alive():
                    logging.warning("Cleanup thread did not stop within timeout")

        # Mark active downloads as cancelled so threads can exit early
        with self.lock:
            active_count = len(self.active_downloads)
            for download in list(self.active_downloads.values()):
                if download.status in ("downloading", "assembling"):
                    download.status = "cancelled"
                    logging.debug(f"Cancelled parallel download for {download.final_path.name}")

            if active_count > 0:
                logging.info(f"Cancelled {active_count} active parallel downloads")

        # Shutdown the executor with a bounded wait -- executor.shutdown()
        # has no timeout of its own and would otherwise block indefinitely
        # if a worker thread is stuck (e.g. a hung network read).
        if hasattr(self, "own_executor") and self.own_executor and self.executor:
            try:
                logging.debug("Shutting down download executor...")
                bounded_executor_shutdown(self.executor, timeout, "Download executor")
            except Exception as e:
                logging.error(f"Error shutting down executor: {e}")

        # Clean up temporary chunk files
        try:
            cleaned = self.cleanup_stale_chunks()
            if cleaned > 0:
                logging.info(f"Cleaned up {cleaned} stale chunk directories")
        except Exception as e:
            logging.debug(f"Error cleaning stale chunks: {e}")

        # Clear internal data structures to free memory
        with self.lock:
            self.active_downloads.clear()

        with self._ip_semaphores_lock:
            self._ip_semaphores.clear()
            self._ip_semaphores_last_used.clear()

        logging.debug("Parallel download manager shutdown complete")

    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            # Only shutdown if not already done
            if hasattr(self, "_shutdown") and not self._shutdown:
                logging.debug("ParallelDownloadManager __del__ initiating shutdown")
                self.shutdown()
        except (FileNotFoundError, OSError) as e:
            # Expected during interpreter shutdown when temp dirs are already gone
            logging.debug(f"ParallelDownloadManager __del__ cleanup (expected): {e}")
        except Exception as e:
            # Don't raise exceptions in __del__
            logging.debug(f"ParallelDownloadManager __del__ shutdown error: {e}")

    def auto_select_method(
        self, file_sizes: List[int], total_files: int, remote_urls: List[str]
    ) -> DownloadMethod:
        """
        Automatically select optimal download method based on runtime conditions.
        ONLY called when no download method arguments were provided.
        """
        # If user specified a method, respect it
        if self.config.parallel_downloads:
            return DownloadMethod.TRADITIONAL_PARALLEL
        if getattr(self.config, "streaming_parallel", False):
            return DownloadMethod.STREAMING_PARALLEL
        if getattr(self.config, "sequential_downloads", False):
            return DownloadMethod.SEQUENTIAL

        # Auto-detection logic (only when no arguments)
        logging.info("📊 Auto-selecting download method...")

        # Single file - always sequential (proven faster in tests)
        if total_files == 1:
            logging.info("📊 Auto-selected: SEQUENTIAL (single file detected)")
            return DownloadMethod.SEQUENTIAL

        # Calculate average file size
        avg_file_size = sum(file_sizes) / total_files if file_sizes else 0
        avg_file_size_mb = avg_file_size / (1024 * 1024)

        # Detect disk type
        disk_is_ssd = self._detect_ssd()

        # Estimate network speed
        network_speed_mbps = self._estimate_network_speed(remote_urls[:5])

        # Check server capabilities
        supports_range = self._check_range_support(remote_urls[0] if remote_urls else None)

        # Decision matrix
        logging.debug(
            f"Auto-select stats: files={total_files}, avg_size={avg_file_size_mb:.1f}MB, "
            f"disk={'SSD' if disk_is_ssd else 'HDD'}, network={network_speed_mbps:.0f}Mbps, "
            f"range={supports_range}"
        )

        # Many small files - parallel files without chunking
        small_files_count = sum(1 for s in file_sizes if s < 10 * 1024 * 1024)
        if small_files_count > total_files * 0.7 and total_files >= 3:
            logging.info(
                f"📊 Auto-selected: TRADITIONAL_PARALLEL ({small_files_count} small files detected)"
            )
            return DownloadMethod.TRADITIONAL_PARALLEL

        # Large files with SSD and good network - streaming parallel
        if (
            avg_file_size_mb >= 100
            and total_files >= 4
            and disk_is_ssd
            and network_speed_mbps > 100
            and supports_range
        ):
            logging.info(
                f"📊 Auto-selected: STREAMING_PARALLEL "
                f"(avg:{avg_file_size_mb:.0f}MB, SSD, {network_speed_mbps:.0f}Mbps, {total_files} files)"
            )
            return DownloadMethod.STREAMING_PARALLEL

        # Large files with HDD - traditional parallel (temp files safer)
        if avg_file_size_mb >= 50 and total_files >= 3 and not disk_is_ssd and supports_range:
            logging.info(
                f"📊 Auto-selected: TRADITIONAL_PARALLEL (HDD detected, {total_files} files)"
            )
            return DownloadMethod.TRADITIONAL_PARALLEL

        # Default to sequential for safety
        logging.info(f"📊 Auto-selected: SEQUENTIAL (balanced for {total_files} files)")
        return DownloadMethod.SEQUENTIAL

    def _detect_ssd(self) -> bool:
        """Detect if target disk is SSD (non-rotational)."""
        # Use config override if provided
        if self.config.force_disk_type:
            return self.config.force_disk_type.lower() == "ssd"

        try:
            import psutil

            if not self.mirror or not self.mirror.target_dir:
                return True  # Assume SSD if we can't detect

            target_path = str(self.mirror.target_dir)

            # Get disk partition
            for partition in psutil.disk_partitions():
                if target_path.startswith(partition.mountpoint):
                    # Check if it's SSD (non-rotational)
                    if hasattr(partition, "opts"):
                        # Linux: 'rota' flag (1=HDD, 0=SSD)
                        if "rota=0" in partition.opts or "nonrot" in partition.opts:
                            return True
                    break

            # Fallback: Test random write speed
            try:
                # Use an exclusively created file: fixed probe names can
                # overwrite mirror data or follow a pre-existing symlink.
                with tempfile.TemporaryFile(dir=self.mirror.target_dir) as f:
                    f.truncate(10 * 1024 * 1024)
                    start = time.time()
                    for _ in range(100):  # 100 random writes
                        f.seek(random.randint(0, 10 * 1024 * 1024))
                        f.write(b"x" * 1024)
                    f.flush()
                    duration = time.time() - start

                # SSDs handle random writes much faster (<0.5s)
                return duration < 0.5
            except Exception:
                pass
        except Exception:
            pass

        return True  # Assume SSD for safety

    def _estimate_network_speed(self, sample_urls: List[str]) -> float:
        """Estimate network speed in Mbps."""
        if not sample_urls:
            if self.config.manual_network_speed_mbps:
                return self.config.manual_network_speed_mbps
            return 100  # Default assumption

        # Use config override if provided
        if self.config.manual_network_speed_mbps:
            return self.config.manual_network_speed_mbps

        response = None
        try:
            test_url = sample_urls[0]
            headers = {"Range": "bytes=0-1048575", "Accept-Encoding": "identity"}
            start = time.time()
            response = self.connection_manager.request(
                test_url, method="GET", headers=headers, timeout=10, stream=True
            )
            if response.status_code == 206:
                sampled = 0
                for chunk in response.iter_raw(64 * 1024):
                    sampled += len(chunk)
                    if sampled >= 1024 * 1024:
                        break
                duration = time.time() - start
                if duration > 0:
                    speed_mbps = (sampled * 8) / duration / 1_000_000
                    logging.debug(f"Network speed estimate: {speed_mbps:.0f} Mbps")
                    return speed_mbps
        except Exception as e:
            logging.debug(f"Network speed test failed: {e}")
        finally:
            if response is not None:
                response.close()

        return 100  # Default assumption

    def _check_range_support(self, test_url: Optional[str]) -> bool:
        """Check if server supports Range requests."""
        if not test_url:
            return False
        try:
            response = self.connection_manager.request(test_url, method="HEAD", timeout=10)
            accept_ranges = response.headers.get("Accept-Ranges", "").lower()
            return accept_ranges == "bytes"
        except Exception:
            return False


class PartialDownloadManager:
    """Manage partial downloads with resume support"""

    STATE_DIRECTORY = ".mirror-url-state"

    def owns_state_directory(self) -> bool:
        if self.download_dir is None:
            return False
        state = self.download_dir / self.STATE_DIRECTORY
        try:
            if state.is_symlink() or not state.is_dir():
                return False
            marker = state / "owner.json"
            if marker.is_symlink():
                return False
            return json.loads(marker.read_text(encoding="utf-8")) == {
                "format": 1,
                "target": str(self.download_dir.resolve()),
            }
        except (OSError, ValueError):
            return False

    def _state_directory(self) -> Path:
        if self.download_dir is None:
            raise ValueError("Partial downloads require a target directory")
        state = self.download_dir / self.STATE_DIRECTORY
        with self.lock:
            if not state.exists() and not state.is_symlink():
                state.mkdir(mode=0o700)
                (state / "owner.json").write_text(
                    json.dumps({"format": 1, "target": str(self.download_dir.resolve())}),
                    encoding="utf-8",
                )
            if not self.owns_state_directory():
                raise ValueError("Refusing to use an unowned partial-download directory")
        return state

    def __init__(self, download_dir: Optional[Path], partial_suffix: str = PARTIAL_SUFFIX):
        """
        Initialize partial download manager.

        Args:
            download_dir: Download directory
            partial_suffix: Suffix for partial files
        """
        self.download_dir = download_dir
        self.partial_suffix = partial_suffix
        self.active_partials: Dict[Path, Dict[str, Any]] = {}
        self.lock = RLock()
        self.total_partials = 0
        self.total_resumes = 0
        # FIX: Only create directory if download_dir is provided
        if self.download_dir is not None:
            try:
                self.download_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logging.debug(f"Could not create partial download directory: {e}")

    def get_partial_path(self, final_path: Path) -> Path:
        """
        Get path for partial download.

        Args:
            final_path: Final file path

        Returns:
            Partial file path
        """
        if self.download_dir is None:
            raise ValueError("Partial download directory is unavailable")
        relative = final_path.resolve().relative_to(self.download_dir.resolve())
        if relative.parts[0].casefold() == self.STATE_DIRECTORY:
            raise ValueError("Remote path conflicts with the partial-download directory")
        digest = hashlib.sha256(str(relative).encode("utf-8")).hexdigest()
        return self._state_directory() / (digest + self.partial_suffix)

    def register_partial(
        self, final_path: Path, url: str, expected_size: Optional[int] = None
    ) -> Path:
        """
        Register a new partial download.

        Args:
            final_path: Final file path
            url: Source URL
            expected_size: Expected file size

        Returns:
            Partial file path
        """
        partial_path = self.get_partial_path(final_path)
        with self.lock:
            self.active_partials[partial_path] = {
                "url": url,
                "final_path": final_path,
                "expected_size": expected_size,
                "start_time": time.time(),
                "last_activity": time.time(),
                "bytes_downloaded": 0,
            }
            self.total_partials += 1
        return partial_path

    def update_activity(self, partial_path: Path, bytes_downloaded: int = 0) -> None:
        """
        Update last activity time for partial download.

        Args:
            partial_path: Partial file path
            bytes_downloaded: Bytes downloaded since last update
        """
        with self.lock:
            if partial_path in self.active_partials:
                self.active_partials[partial_path]["last_activity"] = time.time()
                self.active_partials[partial_path]["bytes_downloaded"] += bytes_downloaded

    def complete_partial(self, partial_path: Path) -> Optional[Path]:
        """
        Complete a partial download and return final path.

        Args:
            partial_path: Partial file path

        Returns:
            Final file path or None
        """
        with self.lock:
            if partial_path in self.active_partials:
                final_path = self.active_partials[partial_path]["final_path"]
                bytes_downloaded = self.active_partials[partial_path]["bytes_downloaded"]

                if bytes_downloaded > 0:
                    self.total_resumes += 1

                del self.active_partials[partial_path]
                return final_path

        return None

    def get_resume_offset(self, partial_path: Path) -> int:
        """
        Get the current size of partial file for resume.

        Args:
            partial_path: Partial file path

        Returns:
            Current file size in bytes
        """
        try:
            if partial_path.exists():
                return partial_path.stat().st_size
        except Exception:
            pass
        return 0

    def cleanup_stale_partials(self, max_age_hours: int = PARTIAL_MAX_AGE_HOURS) -> int:
        """
        Clean up partial downloads older than max_age_hours.
        Args:
            max_age_hours: Maximum age in hours
        Returns:
            Number of partials cleaned
        """
        # FIX: Return early if download_dir is None (dry-run or connection failed)
        if self.download_dir is None:
            return 0

        now = time.time()
        max_age_seconds = max_age_hours * 3600
        cleaned = 0
        with self.lock:
            stale = [
                path
                for path, info in self.active_partials.items()
                if now - info["last_activity"] > max_age_seconds
            ]
            for path in stale:
                del self.active_partials[path]
                cleaned += 1
            # Never classify ordinary mirror files by their suffix. Older
            # versions stored partials alongside data; those cannot be safely
            # distinguished from legitimate remote files and are left alone.
            if not self.owns_state_directory():
                return cleaned
            state = self.download_dir / self.STATE_DIRECTORY
            for partial_file in state.glob(f"*{self.partial_suffix}"):
                try:
                    if partial_file in self.active_partials or partial_file.is_symlink():
                        continue
                    name = partial_file.name.removesuffix(self.partial_suffix)
                    if len(name) != 64 or any(c not in "0123456789abcdef" for c in name):
                        continue
                    if now - partial_file.stat().st_mtime > max_age_seconds:
                        partial_file.unlink()
                        clear_resume_metadata(partial_file)
                        cleaned += 1
                        logging.debug(f"Cleaned stale partial: {partial_file}")
                except Exception as e:
                    logging.debug(f"Failed to clean partial {partial_file}: {e}")
        if cleaned > 0:
            logging.info(f"Cleaned {cleaned} stale partial downloads")
        return cleaned

    def get_stats(self) -> Dict[str, Any]:
        """
        Get partial download statistics.

        Returns:
            Dictionary with partial download stats
        """
        with self.lock:
            return {
                "active_partials": len(self.active_partials),
                "total_partials": self.total_partials,
                "total_resumes": self.total_resumes,
            }


__all__ = ["ParallelDownloadManager", "PartialDownloadManager"]
