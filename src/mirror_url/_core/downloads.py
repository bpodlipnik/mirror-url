"""DownloadMixin: Single-file download with resume.

Originally extracted from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import logging
import os
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Optional

import httpx

from ..constants import DOWNLOAD_CHUNK_SIZE
from ..download_integrity import (
    clear_resume_metadata,
    content_length,
    load_resume_metadata,
    save_resume_metadata,
    strong_etag,
    validate_range,
)
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

        # Connection-level circuit breaking is enforced inside
        # ConnectionManager request paths via circuit_breaker_manager
        # (per-domain). A previous pre-check against the always-None
        # ``connection_manager.circuit_breaker`` attribute was dead code and
        # has been removed.

        try:
            partial_path = self.partial_manager.register_partial(local_path, remote_url)
        except (OSError, ValueError) as error:
            logging.error(f"Cannot create partial download: {error}")
            self.files_failed.increment(1)
            return False
        logging.debug(f"Partial path: {partial_path}")

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if partial_path.is_symlink():
                raise ValueError("Partial download path is a symlink")

            for attempt in range(self.config.max_retries + 1):
                r = None
                try:
                    headers = {"Accept-Encoding": "identity"}
                    metadata = (
                        load_resume_metadata(partial_path, remote_url)
                        if self.config.enable_resume
                        else None
                    )
                    bytes_already = partial_path.stat().st_size if metadata else 0
                    mode = "ab" if bytes_already else "wb"
                    if metadata:
                        headers.update(
                            Range=f"bytes={bytes_already}-", **{"If-Range": metadata["etag"]}
                        )
                        self.metrics.increment("partial_resumes")
                    else:
                        clear_resume_metadata(partial_path)

                    start = time.time()
                    r = self.connection_manager.request(
                        remote_url, method="GET", timeout=30, headers=headers, stream=True
                    )
                    if metadata and r.status_code == 416:
                        # A 416 never proves local contents are complete, even
                        # when the advertised total happens to equal our size.
                        r.close()
                        r = self.connection_manager.request(
                            remote_url,
                            method="GET",
                            timeout=30,
                            headers={"Accept-Encoding": "identity"},
                            stream=True,
                        )
                        mode, bytes_already, metadata = "wb", 0, None
                        clear_resume_metadata(partial_path)

                    if r.status_code not in (200, 206):
                        logging.warning(
                            f"Non-200/206 status for {sanitize_url_for_log(remote_url)}: {r.status_code}"
                        )
                        self.files_failed.increment(1)
                        self.partial_manager.complete_partial(partial_path)
                        self.performance_monitor.record(
                            "download", time.time() - download_start, False
                        )
                        return False

                    if metadata and r.status_code == 200:
                        mode, bytes_already, metadata = "wb", 0, None
                        self.metrics.increment("range_ignored_restarts")
                        clear_resume_metadata(partial_path)
                    if r.status_code == 206:
                        if not metadata:
                            raise ValueError("Unsolicited partial response to a full download")
                        validate_range(
                            r,
                            bytes_already,
                            metadata["size"] - 1,
                            metadata["size"],
                            metadata["etag"],
                        )
                        expected_size = metadata["size"]
                    else:
                        if r.headers.get("Content-Encoding", "identity").lower() != "identity":
                            raise ValueError("Server ignored Accept-Encoding: identity")
                        expected_size = content_length(r.headers)

                    size = bytes_already
                    with open(partial_path, mode) as f:
                        f.flush()
                        os.fsync(f.fileno())
                        # Write the validator only after an old partial has
                        # been truncated, so a crash cannot bind old bytes to
                        # the new representation.
                        etag = strong_etag(r.headers)
                        if etag and expected_size is not None:
                            save_resume_metadata(partial_path, remote_url, etag, expected_size)
                        else:
                            clear_resume_metadata(partial_path)
                        for chunk in r.iter_bytes(DOWNLOAD_CHUNK_SIZE):
                            if chunk:
                                if expected_size is not None and size + len(chunk) > expected_size:
                                    raise ValueError("Download exceeds expected length")
                                f.write(chunk)
                                size += len(chunk)
                                self.partial_manager.update_activity(partial_path, len(chunk))
                                if self.bandwidth_limiter:
                                    self.bandwidth_limiter.throttle(len(chunk))
                        f.flush()
                        os.fsync(f.fileno())
                    if expected_size is not None and size != expected_size:
                        raise ValueError(f"Incomplete download: {size} != {expected_size}")

                    download_time = time.time() - start
                    self.metrics.add_download_time(download_time)

                    os.replace(partial_path, local_path)
                    try:
                        clear_resume_metadata(partial_path)
                    except OSError as error:
                        logging.warning(
                            f"Published file resume metadata could not be removed: {error}"
                        )
                    self.partial_manager.complete_partial(partial_path)

                    last_modified = r.headers.get("Last-Modified")
                    if last_modified:
                        try:
                            timestamp = parsedate_to_datetime(last_modified).timestamp()
                            os.utime(local_path, times=(timestamp, timestamp))
                        except (OSError, ValueError, TypeError, OverflowError):
                            pass

                    remote_etag = r.headers.get("ETag")
                    if remote_etag:
                        try:
                            self.cache_manager.save_file_metadata(
                                local_path, remote_etag, time.time(), size
                            )
                        except Exception as error:
                            logging.warning(f"Published file metadata could not be cached: {error}")

                    if hasattr(self, "fs_cache"):
                        try:
                            self.fs_cache.invalidate(local_path)
                        except Exception as error:
                            logging.warning(
                                f"Published file filesystem cache could not be invalidated: {error}"
                            )

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

                except (
                    httpx.ConnectError,
                    httpx.TimeoutException,
                    httpx.ReadError,
                    httpx.RemoteProtocolError,
                ) as e:
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
                                clear_resume_metadata(partial_path)
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
                            clear_resume_metadata(partial_path)
                        except Exception as unlink_err:
                            logging.debug(
                                f"Failed to remove partial file {partial_path}: {unlink_err}"
                            )

                    self.partial_manager.complete_partial(partial_path)
                    self.performance_monitor.record("download", time.time() - download_start, False)
                    return False
                finally:
                    if r is not None:
                        r.close()

        except Exception as e:
            logging.error(f"Download failed: {e}")
            self.files_failed.increment(1)  # Atomic
            self.metrics.increment("files_failed")
            self.metrics.add_error(str(e), "download_failed")

            if (
                not isinstance(
                    e,
                    (
                        httpx.ConnectError,
                        httpx.TimeoutException,
                        httpx.ReadError,
                        httpx.RemoteProtocolError,
                    ),
                )
                and partial_path.exists()
            ):
                try:
                    partial_path.unlink()
                    clear_resume_metadata(partial_path)
                except Exception as unlink_err:
                    logging.debug(f"Failed to remove partial file {partial_path}: {unlink_err}")

            self.partial_manager.complete_partial(partial_path)
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False
