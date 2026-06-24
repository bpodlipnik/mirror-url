"""DownloadMixin: Single-file download with resume.

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Optional

import httpx

from ..constants import DOWNLOAD_CHUNK_SIZE
from ..utils import exponential_backoff, format_bytes, sanitize_url_for_log, trim_url


class DownloadMixin:
    def download_file_with_resume(
        self, remote_url: str, local_path: Path, file_size: Optional[int] = None
    ) -> bool:
        """
        Enhanced download method with parallel chunk support.
        If parallel downloads are enabled and file is large enough,
        uses parallel chunk downloads. Otherwise falls back to single-threaded.
        """
        # Check if we should use parallel download
        if self.parallel_manager and self.parallel_manager.enabled:
            # Use passed size or fetch it
            current_size = file_size if file_size is not None else self._get_file_size(remote_url)
            if current_size and self.parallel_manager.should_use_parallel(current_size):
                # Create parallel download
                download = self.parallel_manager.create_chunks(remote_url, local_path, current_size)
                if download:
                    # Download chunks in parallel
                    success = self.parallel_manager.download_parallel(download)
                    if success:
                        return True

                    # Fallback to single-threaded download if parallel fails.
                    # ⚠️ IMPORTANT: _download_file_single ALREADY increments failur
                    # on failure. We must NOT increment them here to avoid double-counting.
                    return self._download_file_single(remote_url, local_path)

        # Fallback for files that don't meet parallel criteria (too small, disabled, etc.)
        return self._download_file_single(remote_url, local_path)

    def _download_file_single(self, remote_url: str, local_path: Path) -> bool:
        """Original single-threaded download method with atomic counter updates."""
        remote_url = trim_url(remote_url)
        download_start = time.time()

        try:
            # Use cached filename extraction for performance
            filename = self._get_cached_filename(remote_url)
            parent_dir = local_path.parent
            local_path = parent_dir / filename
            logging.debug(f"Normalized path: {local_path}")
        except Exception as e:
            logging.debug(f"Error decoding filename: {e}")

        if (
            hasattr(self.connection_manager, "circuit_breaker")
            and self.connection_manager.circuit_breaker
            and not self.connection_manager.circuit_breaker.can_execute()
        ):
            self.metrics.increment("circuit_breaker_trips")
            logging.error("Download failed: Circuit breaker is open")
            self.files_failed.increment(1)
            self.metrics.increment("files_failed")
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False

        logging.debug("Circuit breaker check passed, proceeding to partial manager")

        partial_path = self.partial_manager.register_partial(local_path, remote_url)
        logging.debug(f"Partial path: {partial_path}")

        headers = {}
        mode = "wb"
        bytes_already = 0

        if self.config.enable_resume and partial_path.exists():
            bytes_already = self.partial_manager.get_resume_offset(partial_path)
            if bytes_already > 0:
                headers["Range"] = f"bytes={bytes_already}-"
                mode = "ab"
                logging.debug(f"Resuming download from {bytes_already} bytes")
                self.metrics.increment("partial_resumes")

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            ts = self.get_remote_timestamp(remote_url)

            for attempt in range(self.config.max_retries + 1):
                try:
                    start = time.time()
                    r = self.connection_manager.request(
                        remote_url, method="GET", timeout=30, headers=headers
                    )

                    if attempt == 0 and r.status_code == 416 and bytes_already > 0:
                        # Per RFC 7233, a 416 response SHOULD include
                        #   Content-Range: bytes */<total_size>
                        # to tell the client the actual file length.
                        # Earlier code read Content-Length, which is 0 for an
                        # empty 416 body — making the size comparison
                        # ``partial_size >= 0`` trivially true and renaming the
                        # partial regardless of whether it was complete. We now
                        # parse Content-Range first, fall back to Content-Length
                        # only if the server omits the standard header.
                        total_size = 0
                        content_range = r.headers.get("Content-Range", "")
                        if "/" in content_range:
                            try:
                                total_size = int(content_range.rsplit("/", 1)[1])
                            except (ValueError, IndexError):
                                total_size = 0
                        if total_size == 0:
                            try:
                                total_size = int(r.headers.get("Content-Length", 0))
                            except (ValueError, TypeError):
                                total_size = 0

                        if total_size > 0 and partial_path.stat().st_size >= total_size:
                            partial_path.rename(local_path)
                            logging.info(f"File already complete: {local_path}")
                            self.metrics.increment("resumed_downloads")
                            self.partial_manager.complete_partial(partial_path)

                            if hasattr(self, "fs_cache"):
                                self.fs_cache.invalidate(local_path)

                            self.performance_monitor.record(
                                "download", time.time() - download_start, True
                            )
                            return True

                        # 416 but partial isn't actually complete — restart from
                        # scratch (truncate the partial, drop Range header).
                        try:
                            partial_path.unlink()
                        except OSError:
                            pass
                        mode = "wb"
                        headers = {}
                        bytes_already = 0
                        continue

                    if r.status_code not in (200, 206):
                        logging.warning(
                            f"Non-200/206 status for {sanitize_url_for_log(remote_url)}: {r.status_code}"
                        )
                        self.partial_manager.complete_partial(partial_path)
                        self.performance_monitor.record(
                            "download", time.time() - download_start, False
                        )
                        return False

                    # Range-ignored protection.
                    #
                    # If we sent ``Range: bytes=N-`` (because a partial existed)
                    # but the server returned 200 with the FULL body instead of
                    # 206 with just the requested range, appending the full body
                    # to the existing partial bytes would silently corrupt the
                    # file (final size = partial_size + full_size, content =
                    # partial_bytes + full_bytes). Detected and fixed here:
                    # discard the partial and overwrite from scratch.
                    if r.status_code == 200 and mode == "ab":
                        logging.warning(
                            f"Server returned 200 instead of 206 for ranged request "
                            f"to {sanitize_url_for_log(remote_url)}; discarding "
                            f"{bytes_already}-byte partial and restarting from scratch."
                        )
                        mode = "wb"
                        bytes_already = 0
                        self.metrics.increment("range_ignored_restarts")

                    size = bytes_already

                    with open(partial_path, mode) as f:
                        for chunk in r.iter_bytes(DOWNLOAD_CHUNK_SIZE):
                            if chunk:
                                f.write(chunk)
                                size += len(chunk)
                                self.partial_manager.update_activity(partial_path, len(chunk))

                                if self.bandwidth_limiter:
                                    self.bandwidth_limiter.throttle(len(chunk))

                    download_time = time.time() - start
                    self.metrics.add_download_time(download_time)

                    partial_path.rename(local_path)
                    self.partial_manager.complete_partial(partial_path)

                    if ts:
                        os.utime(local_path, times=(ts, ts))

                    remote_etag = r.headers.get("ETag")
                    if remote_etag:
                        self.cache_manager.save_file_metadata(
                            local_path, remote_etag, time.time(), size
                        )

                    if hasattr(self, "fs_cache"):
                        self.fs_cache.invalidate(local_path)

                    # FIX v3.0.6: Update counters using atomic methods
                    downloaded_bytes = size - bytes_already
                    self.files_processed.increment(1)  # Atomic increment
                    self.total_downloaded_size.add(downloaded_bytes)  # Atomic add
                    self.metrics.increment("files_downloaded")
                    self.metrics.add_bytes(downloaded_bytes)
                    self.performance_monitor.record_bytes(downloaded_bytes)

                    if bytes_already > 0:
                        self.metrics.increment("resumed_downloads")
                        self.metrics.increment("partial_downloads")

                    logging.info(f"Downloaded: {local_path} ({format_bytes(size)})")

                    self.performance_monitor.record("download", time.time() - download_start, True)
                    return True

                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    if attempt < self.config.max_retries:
                        wait_time = exponential_backoff(attempt)
                        logging.warning(
                            f"Download attempt {attempt + 1} failed: {e}. Retrying in {wait_time:.1f}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        raise

                except httpx.HTTPStatusError as e:
                    status = e.response.status_code if e.response else 0

                    if status in (403, 404, 410, 451):
                        logging.warning(
                            f"HTTP {status}, skipping: {sanitize_url_for_log(remote_url)}"
                        )
                        # FIX: increment the ATOMIC files_skipped counter — the
                        # final summary reads self.files_skipped.value(), but
                        # this path previously only bumped the metrics dict, so
                        # 403/404/410/451 skips were invisible in the skip total.
                        self.files_skipped.increment(1)
                        self.metrics.increment("files_skipped")

                        if partial_path.exists():
                            try:
                                partial_path.unlink()
                            except Exception as unlink_err:
                                logging.debug(
                                    f"Failed to remove partial file {partial_path}: {unlink_err}"
                                )

                        self.partial_manager.complete_partial(partial_path)
                        # This is a SKIP (the resource is gone / forbidden), not a
                        # download and not a failure. We return True so neither
                        # caller counts it as a failure (the sequential caller
                        # increments files_failed on False; the parallel caller
                        # deliberately doesn't). It is already counted in
                        # files_skipped above.
                        self.performance_monitor.record(
                            "download", time.time() - download_start, True
                        )
                        return True

                    logging.error(
                        f"HTTP {status} error for {sanitize_url_for_log(remote_url)}: {e}"
                    )
                    self.files_failed.increment(1)  # Atomic
                    self.metrics.increment("files_failed")

                    if partial_path.exists():
                        try:
                            partial_path.unlink()
                        except Exception as unlink_err:
                            logging.debug(
                                f"Failed to remove partial file {partial_path}: {unlink_err}"
                            )

                    self.partial_manager.complete_partial(partial_path)
                    self.performance_monitor.record("download", time.time() - download_start, False)
                    return False

        except Exception as e:
            logging.error(f"Download failed: {e}")
            self.files_failed.increment(1)  # Atomic
            self.metrics.increment("files_failed")
            self.metrics.add_error(str(e), "download_failed")

            if partial_path.exists():
                try:
                    partial_path.unlink()
                except Exception as unlink_err:
                    logging.debug(f"Failed to remove partial file {partial_path}: {unlink_err}")

            self.partial_manager.complete_partial(partial_path)
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False
