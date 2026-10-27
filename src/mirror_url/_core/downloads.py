"""DownloadMixin: Single-file download with resume.

Originally extracted from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import nullcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import httpx

from ..constants import DOWNLOAD_CHUNK_SIZE
from ..destination_lock import destination_operation
from ..download_integrity import (
    clear_resume_metadata,
    content_length,
    file_identity,
    file_sha256,
    load_resume_metadata,
    open_owned_partial,
    replace_verified_file,
    save_resume_metadata,
    strong_etag,
    validate_range,
)
from ..exceptions import MirrorConnectionError
from ..filename_mapping import _stat_identity
from ..metrics import log_file_downloaded, log_whole_file_fallback
from ..progress import get_download_progress
from ..utils import exponential_backoff, sanitize_url_for_log, trim_url

if TYPE_CHECKING:
    from ._typing import MirrorHost
else:
    MirrorHost = object


class DownloadMixin(MirrorHost):
    @destination_operation
    def download_file_with_resume(
        self, remote_url: str, local_path: Path, file_size: Optional[int] = None
    ) -> bool:
        """
        Enhanced download method with parallel chunk support.
        If parallel downloads are enabled and file is large enough,
        uses parallel chunk downloads. Otherwise falls back to single-threaded.
        """
        progress = get_download_progress(self)
        progress.begin(local_path, "waiting for response")
        success = False
        try:
            # Check if we should use parallel download
            if self.parallel_manager and self.parallel_manager.enabled:
                # Use passed size or fetch it
                current_size = file_size
                if current_size is None:
                    current_size = self._get_file_size(remote_url)
                if current_size and self.parallel_manager.should_use_parallel(current_size):
                    # Create parallel download
                    download = self.parallel_manager.create_chunks(
                        remote_url, local_path, current_size
                    )
                    if download:
                        # Download chunks in parallel
                        success = self.parallel_manager.download_parallel(download)
                        if success:
                            return True

                        try:
                            destination_changed = (
                                download.identity_captured
                                and file_identity(local_path) != download.original_identity
                            )
                        except (OSError, ValueError):
                            destination_changed = True
                        if download.publication_failed or destination_changed:
                            logging.error("Publication failed; preserving the destination")
                            self.files_failed.increment(1)
                            self.metrics.increment("files_failed")
                            return False

                        log_whole_file_fallback(remote_url, "parallel chunk transfer failed")
                        # Fallback to single-threaded download if parallel fails.
                        # ⚠️ IMPORTANT: _download_file_single ALREADY increments failur
                        # on failure. We must NOT increment them here to avoid double-counting.
                        success = self._download_file_single(remote_url, local_path)
                        return success
                else:
                    log_whole_file_fallback(
                        remote_url,
                        self.parallel_manager.whole_file_fallback_reason(current_size or None, 1),
                    )
            elif self.config.parallel_downloads or self.config.streaming_parallel:
                log_whole_file_fallback(remote_url, "parallel chunk downloads are disabled")

            # Fallback for files that don't meet parallel criteria (too small, disabled, etc.)
            success = self._download_file_single(remote_url, local_path)
            return success
        finally:
            progress.finish(local_path, success)

    @destination_operation
    def _download_file_single(self, remote_url: str, local_path: Path) -> bool:
        """Original single-threaded download method with atomic counter updates."""
        remote_url = trim_url(remote_url)
        download_start = time.time()
        progress = get_download_progress(self)
        partial_manager = self.partial_manager
        target = self.target_dir
        if partial_manager is None or target is None:
            logging.error("Partial download manager or destination is unavailable")
            self.files_failed.increment(1)
            return False

        # Connection-level circuit breaking is enforced inside
        # ConnectionManager request paths via circuit_breaker_manager
        # (per-domain). A previous pre-check against the always-None
        # ``connection_manager.circuit_breaker`` attribute was dead code and
        # has been removed.

        partial_path = None
        try:
            original_destination = file_identity(local_path)
            partial_path = partial_manager.register_partial(local_path, remote_url)
            original_partial = file_identity(partial_path)
        except (OSError, ValueError) as error:
            if partial_path is not None:
                partial_manager.complete_partial(partial_path)
            logging.error(f"Cannot create partial download: {error}")
            self.files_failed.increment(1)
            return False
        logging.debug(f"Partial path: {partial_path}")
        owned_partial = original_partial[:2] if original_partial is not None else None

        def discard_partial() -> None:
            try:
                current = file_identity(partial_path)
                if current is not None and current[:2] == owned_partial:
                    partial_path.unlink()
                    clear_resume_metadata(partial_path)
            except (OSError, ValueError) as error:
                logging.debug(f"Preserving partial download after cleanup failure: {error}")

        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)

            for attempt in range(self.config.max_retries + 1):
                r = None
                try:
                    progress.begin(local_path, "waiting for response")
                    headers = {"Accept-Encoding": "identity"}
                    expected_partial = file_identity(partial_path)
                    if expected_partial is not None and expected_partial[:2] != owned_partial:
                        raise ValueError("Partial download changed between attempts; preserving it")
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
                    with self.metrics.run_report.transfer_stage("transfer"):
                        r = self.connection_manager.request(
                            remote_url, method="GET", timeout=30, headers=headers, stream=True
                        )
                    if metadata and r.status_code == 416:
                        # A 416 never proves local contents are complete, even
                        # when the advertised total happens to equal our size.
                        r.close()
                        with self.metrics.run_report.transfer_stage("transfer"):
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
                        partial_manager.complete_partial(partial_path)
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
                    progress.resume(local_path, bytes_already, expected_size)
                    with open_owned_partial(partial_path, expected_partial) as f:
                        opened = os.fstat(f.fileno())
                        owned_partial = (opened.st_dev, opened.st_ino)
                        if mode == "wb":
                            f.truncate(0)
                        f.seek(0, os.SEEK_END if mode == "ab" else os.SEEK_SET)
                        with self.metrics.run_report.transfer_stage("flush"):
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
                        with self.metrics.run_report.transfer_stage("transfer"):
                            for chunk in r.iter_bytes(DOWNLOAD_CHUNK_SIZE):
                                if chunk:
                                    if (
                                        expected_size is not None
                                        and size + len(chunk) > expected_size
                                    ):
                                        raise ValueError("Download exceeds expected length")
                                    f.write(chunk)
                                    size += len(chunk)
                                    progress.received(local_path, size, expected_size)
                                    partial_manager.update_activity(partial_path, len(chunk))
                                    if self.bandwidth_limiter:
                                        self.bandwidth_limiter.throttle(len(chunk))
                        progress.stage(local_path, "flushing file")
                        with self.metrics.run_report.transfer_stage("flush"):
                            f.flush()
                            os.fsync(f.fileno())
                        written_identity = _stat_identity(os.fstat(f.fileno()))
                    # Windows may finalize write timestamps only when the
                    # handle closes. Retain its inode/size proof, then snapshot
                    # the closed file's timestamps for the hashing guard.
                    staged_identity = file_identity(partial_path)
                    if staged_identity is None or staged_identity[:3] != written_identity[:3]:
                        raise ValueError("Owned partial changed before verification")
                    if expected_size is not None and size != expected_size:
                        raise ValueError(f"Incomplete download: {size} != {expected_size}")

                    download_time = time.time() - start
                    self.metrics.add_download_time(download_time)

                    progress.stage(
                        local_path,
                        "hashing (SHA-256)" if self.config.verify_content else "verifying file",
                    )
                    with (
                        self.metrics.run_report.transfer_stage("hash")
                        if self.config.verify_content
                        else nullcontext()
                    ):
                        digest = file_sha256(
                            partial_path, enabled=getattr(self.config, "verify_content", False)
                        )
                    progress.stage(local_path, "publishing")
                    replace_verified_file(
                        partial_path,
                        local_path,
                        target=target,
                        original=original_destination,
                        staged_identity=staged_identity,
                        max_depth=self.config.max_depth + 1,
                        max_filename_len=self.config.max_filename_len,
                    )
                    try:
                        clear_resume_metadata(partial_path)
                    except OSError as error:
                        logging.warning(
                            f"Published file resume metadata could not be removed: {error}"
                        )
                    partial_manager.complete_partial(partial_path)

                    last_modified = r.headers.get("Last-Modified")
                    if last_modified:
                        try:
                            timestamp = parsedate_to_datetime(last_modified).timestamp()
                            os.utime(local_path, times=(timestamp, timestamp))
                        except (OSError, ValueError, TypeError, OverflowError):
                            pass

                    remote_etag = r.headers.get("ETag")
                    if remote_etag or digest is not None:
                        try:
                            self.cache_manager.save_file_metadata(
                                local_path, remote_etag, time.time(), size, sha256=digest
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
                    self.metrics.run_report.record_publication(remote_url, sha256=digest)
                    self.files_processed.increment(1)  # Atomic increment
                    self.total_downloaded_size.add(downloaded_bytes)  # Atomic add
                    self.metrics.increment("files_downloaded")
                    self.metrics.add_bytes(downloaded_bytes)
                    self.performance_monitor.record_bytes(downloaded_bytes)

                    if bytes_already > 0:
                        self.metrics.increment("resumed_downloads")
                        self.metrics.increment("partial_downloads")

                    log_file_downloaded(local_path, size, digest)

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
                        progress.stage(local_path, "waiting to retry")
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

                        discard_partial()

                        partial_manager.complete_partial(partial_path)
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

                    discard_partial()

                    partial_manager.complete_partial(partial_path)
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
                        MirrorConnectionError,
                        httpx.ConnectError,
                        httpx.TimeoutException,
                        httpx.ReadError,
                        httpx.RemoteProtocolError,
                    ),
                )
                and partial_path.exists()
            ):
                discard_partial()

            partial_manager.complete_partial(partial_path)
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False

        return False
