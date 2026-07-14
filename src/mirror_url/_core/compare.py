"""CompareMixin: Metadata comparison: which remote files need downloading.

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import httpx

from ..async_connection import AsyncTaskManager
from ..constants import (
    ASYNC_TEST_BATCH_SIZE,
    ASYNC_TEST_MAX_SECONDS,
    ASYNC_TEST_MAX_SECONDS_THROTTLED,
    ASYNC_TEST_MIN_FILES,
    ASYNC_TEST_MIN_FILES_THROTTLED,
    ASYNC_TEST_MIN_SPEED,
    ASYNC_TEST_MIN_SPEED_THROTTLED,
    KNOWN_THROTTLED_DOMAINS,
    PROFILE_SAMPLE_SIZE,
    TIMESTAMP_TOLERANCE_SECONDS,
)
from ..decorators import log_performance
from ..utils import normalize_etag, sanitize_url_for_log, trim_url

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from ..progress import ProgressTracker


class CompareMixin:
    @log_performance("file_check")
    def file_exists_and_up_to_date(
        self, local_path: Path, remote_url: str, use_cache: bool = True
    ) -> bool:
        start_time = time.time()

        # First, check if file exists locally
        if hasattr(self, "fs_cache"):
            exists = self.fs_cache.exists(local_path)
            if not exists:
                self.performance_monitor.record("file_check", time.time() - start_time, True)
                return False
        else:
            # Fix: when fs_cache is unavailable, still verify existence here.
            # Otherwise a missing file falls through to local_path.stat()
            # below, raises FileNotFoundError, and the broad except handler
            # would have reported it as up-to-date (never downloaded).
            if not local_path.exists():
                self.performance_monitor.record("file_check", time.time() - start_time, True)
                return False

        # Try to get metadata from cache if enabled
        stored_meta = None
        stored_etag = None

        if use_cache:
            stored_meta = self.cache_manager.get_file_metadata(local_path)
            stored_etag = stored_meta.get("etag") if stored_meta else None

        # If cache is disabled but file exists, we need to check it properly
        # We should still try to get ETag from local file metadata if available
        if not use_cache and local_path.exists():
            # Try to read ETag from a sidecar file or compute file hash
            # For now, let's check size and modification time
            local_size = local_path.stat().st_size

            # Make HEAD request to get remote info
            try:
                r = self.connection_manager.request(
                    remote_url, method="HEAD", timeout=(10, 20), allow_redirects=True
                )
                if r.status_code == 200:
                    remote_size = int(r.headers.get("Content-Length", 0))
                    if remote_size == local_size:
                        # Sizes match, consider it up-to-date
                        self.performance_monitor.record(
                            "file_check", time.time() - start_time, True
                        )
                        return True
            except Exception as e:
                logging.debug(f"Error checking file without cache: {e}")

        # Continue with normal cache-enabled logic...
        if use_cache and hasattr(self.scanner, "cached_signatures"):
            dir_url = trim_url(remote_url.rsplit("/", 1)[0] + "/")
            if dir_url in self.scanner.cached_signatures:
                old_sig = self.scanner.cached_signatures[dir_url]
                new_sig = getattr(self.scanner, "fresh_dir_signatures", {}).get(dir_url)
                # Only trust the shortcut when we have this run's actual
                # signature for the directory AND it matches what was
                # cached. The url:...:timestamp fallback form (used when a
                # server gives no ETag/Last-Modified) changes every run by
                # construction and carries no real change signal, so it
                # can never legitimately match here — which is correct:
                # a directory we can't fingerprint should always be
                # re-verified, not trusted indefinitely.
                if new_sig and new_sig == old_sig and not new_sig.startswith("url:"):
                    self.metrics.increment("cache_hits")
                    self.metrics.increment("cache_head_requests_saved")
                    self.performance_monitor.record("file_check", time.time() - start_time, True)
                    return True
                self.metrics.increment("dir_signature_changed_forced_recheck")

        try:
            local_ts = local_path.stat().st_mtime
            local_size = local_path.stat().st_size
            headers = {}

            if stored_etag and not self.config.no_etag:
                headers["If-None-Match"] = stored_etag

            start = time.time()
            r = self.connection_manager.request(
                remote_url, method="HEAD", timeout=(10, 20), allow_redirects=True, headers=headers
            )
            self.metrics.add_request_time(time.time() - start)

            if r.status_code == 304:
                self.metrics.increment("cache_hits")
                self.metrics.increment("etag_304_responses")
                self.performance_monitor.record("file_check", time.time() - start_time, True)
                return True
            if r.status_code != 200:
                # Non-200/non-304 means we can't verify the file is up-to-date
                # Safe behavior: treat as cache miss and trigger download
                self.metrics.increment("cache_misses")  # ✅ Correct metric
                self.performance_monitor.record("file_check", time.time() - start_time, False)
                return False  # ✅ File needs download when verification fails

            remote_etag = r.headers.get("ETag")
            if remote_etag and stored_etag and not self.config.no_etag:
                remote_etag_norm = normalize_etag(remote_etag)
                stored_etag_norm = normalize_etag(stored_etag)

                if remote_etag_norm == stored_etag_norm:
                    self.metrics.increment("cache_hits")
                    self.metrics.increment("etag_matches")
                    self.performance_monitor.record("file_check", time.time() - start_time, True)
                    return True
                else:
                    self.metrics.increment("cache_misses")
                    self.metrics.increment("etag_mismatches")
                    self.performance_monitor.record("file_check", time.time() - start_time, False)
                    return False

            # Check Last-Modified
            if "Last-Modified" in r.headers:
                try:
                    dt = parsedate_to_datetime(r.headers["Last-Modified"])
                    remote_ts = dt.timestamp()

                    if remote_ts > local_ts + TIMESTAMP_TOLERANCE_SECONDS:
                        self.metrics.increment("cache_misses")
                        self.performance_monitor.record(
                            "file_check", time.time() - start_time, False
                        )
                        return False

                    self.metrics.increment("cache_hits")
                    self.performance_monitor.record("file_check", time.time() - start_time, True)
                    return True
                except Exception:
                    pass

            # Check file size
            remote_size = int(r.headers.get("Content-Length", 0))
            if remote_size != local_size:
                self.metrics.increment("cache_misses")
                self.performance_monitor.record("file_check", time.time() - start_time, False)
                return False

            self.metrics.increment("cache_hits")
            self.performance_monitor.record("file_check", time.time() - start_time, True)
            return True

        except Exception as e:
            logging.debug(f"Error checking file {local_path}: {e}")
            # Fix: a failed verification (network error, timeout, stat error)
            # must NOT be treated as up-to-date. Returning True here silently
            # skipped re-downloads on any transient failure. Treat it as a
            # cache miss so the file is re-fetched — consistent with the
            # non-200 branch above.
            self.metrics.increment("cache_misses")
            self.performance_monitor.record("file_check", time.time() - start_time, False)
            return False

    def _check_files_sync(
        self, remote_files: List[str], progress: Optional[ProgressTracker] = None
    ) -> List[Tuple[str, Path]]:
        """
        Check files synchronously to determine which need downloading.

        Args:
            remote_files: List of remote file URLs
            progress: Optional progress tracker

        Returns:
            List of (url, local_path) tuples for files that need downloading
        """
        to_download: List[Tuple[str, Path]] = []

        if self.symlink_tracker:
            self.symlink_tracker.clear_chain()

        # Convert URLs to (url, path) tuples
        file_items: List[Tuple[str, Path]] = []
        for item in remote_files:
            # FIX: Handle both string URLs and (url, path) tuples
            if isinstance(item, tuple):
                url, local_path = item
            else:
                url = item
                local_path = self._get_local_path_from_url(url)

            if local_path is None:
                self.files_skipped.increment(1)
                self.metrics.increment("files_skipped")
                continue
            file_items.append((url, local_path))

        if not file_items:
            return []

        total = len(file_items)
        logging.debug(f"Sync check starting: total files to check = {total}")

        if total == 0:
            return to_download

        # Use ThreadPoolExecutor for parallel checking
        max_workers = min(self.config.workers, total)
        results_lock = threading.Lock()

        def check_file(url: str, path: Path) -> Tuple[str, Path, bool]:
            """Check a single file and return whether it needs download."""
            try:
                is_up_to_date = self.file_exists_and_up_to_date(path, url, use_cache=True)
                needs_download = not is_up_to_date
                return (url, path, needs_download)
            except Exception as e:
                logging.error(f"File check failed for {url}: {e}")
                self.files_failed.increment(1)
                self.metrics.increment("files_failed")
                return (url, path, False)  # False = don't download on error

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all check tasks
            future_to_item = {
                executor.submit(check_file, url, path): (url, path) for url, path in file_items
            }

            # Process results as they complete
            for i, future in enumerate(as_completed(future_to_item)):
                url, path = future_to_item[future]
                try:
                    _, _, needs_download = future.result(timeout=30)
                    if needs_download:
                        with results_lock:
                            to_download.append((url, path))
                    else:
                        with results_lock:
                            self.files_skipped.increment(1)
                            self.metrics.increment("files_skipped")
                except Exception as e:
                    logging.error(f"File check result failed for {url}: {e}")
                    with results_lock:
                        self.files_failed.increment(1)
                        self.metrics.increment("files_failed")

                if progress:
                    progress.update(1)

                # Note: no per-N-files logging.info() here by design.
                # progress.update(1) above already drives ProgressTracker's
                # own percentage-milestone logging (25/50/75/90/100% for
                # large jobs — see progress.py PROGRESS_PCT_MILESTONES) and
                # any --progress-bar tqdm display. Keeping a second, separate
                # "Checked N/total" log line here duplicated that reporting
                # with a fixed, dataset-size-independent interval (every 100
                # files), which produced 700+ near-simultaneous log lines on
                # large cron-driven cache-hit runs. The final need-download
                # count is still reported once, below, when the check completes.

        logging.info(f"Sync check complete: {len(to_download)}/{total} files need download")
        return to_download

    async def _check_files_async(
        self,
        remote_files: Union[List[str], List[Tuple[str, Path]]],
        progress: Optional[ProgressTracker] = None,
        _depth: int = 0,
    ) -> List[Tuple[str, Path]]:
        """
        Async file checking with proper task management using AsyncTaskManager.

        Args:
            remote_files: List of remote URLs or list of (url, path) tuples
            progress: Optional progress tracker

        Returns:
            List of (url, local_path) tuples that need to be downloaded
        """
        # Initialize task manager if not already initialized
        if not self.async_task_manager:
            self.async_task_manager = AsyncTaskManager()
            logging.debug("AsyncTaskManager created during _check_files_async")

        file_checks: List[Tuple[Path, str]] = []
        to_download: List[Tuple[str, Path]] = []

        if self.symlink_tracker:
            self.symlink_tracker.clear_chain()

        # FIX: Normalize to List[Tuple[str, Path]] consistently (same as sync version)
        file_items: List[Tuple[str, Path]] = []

        if remote_files:
            first_item = remote_files[0]
            if isinstance(first_item, tuple):
                # Already list of tuples
                file_items = [(url, path) for url, path in remote_files]  # type: ignore
            else:
                # List of strings - convert to tuples
                for url in remote_files:  # type: ignore
                    local_path = self._get_local_path_from_url(url)
                    if local_path is None:
                        self.files_failed.increment(1)
                        self.metrics.increment("files_failed")
                        continue

                    if self.config.handle_symlinks:
                        is_link, target_url = self.is_symlink(url, depth=0)
                        if is_link and target_url:
                            if self.config.symlink_mode == "follow":
                                target_local_path = self._get_local_path_from_url(target_url)
                                if target_local_path:
                                    file_items.append((target_url, target_local_path))
                                    self.record_symlink(url, target_url, local_path, depth=0)
                                    continue
                            elif self.config.symlink_mode == "skip":
                                self.files_skipped.increment(1)
                                self.metrics.increment("files_skipped")
                                self.record_symlink(url, target_url, local_path, depth=0)
                                continue

                    file_items.append((url, local_path))

        if not file_items:
            return []

        # Convert to (Path, url) format for internal processing
        for url, path in file_items:
            file_checks.append((path, url))

        total_files = len(file_checks)
        logging.debug(f"Async check starting: total files to check = {total_files}")

        # Determine which manager to use
        use_adaptive = self.config.adaptive_async and self.adaptive_async_manager is not None
        manager = None

        if use_adaptive:
            if not self.adaptive_async_manager.is_available():
                logging.warning("Adaptive async manager not available, falling back to sync")
                # return self._check_files_sync(file_items, progress)
                return self._check_files_sync([url for url, _ in file_items], progress)  # qwen

            manager = self.adaptive_async_manager
        else:
            if (
                self.async_connection_manager is None
                or not self.async_connection_manager.is_available()
            ):
                logging.debug("No async manager available, falling back to sync")
                return self._check_files_sync(file_items, progress)
            manager = self.async_connection_manager

        test_start_time = time.time()
        test_checked = 0
        fallback_triggered = False
        files_processed_in_test = 0

        is_throttled = any(
            domain in str(self.config.base_url).lower() for domain in KNOWN_THROTTLED_DOMAINS
        )
        test_max_seconds = (
            ASYNC_TEST_MAX_SECONDS_THROTTLED if is_throttled else ASYNC_TEST_MAX_SECONDS
        )
        test_min_files = ASYNC_TEST_MIN_FILES_THROTTLED if is_throttled else ASYNC_TEST_MIN_FILES
        min_speed_threshold = (
            ASYNC_TEST_MIN_SPEED_THROTTLED * 2 if is_throttled else ASYNC_TEST_MIN_SPEED
        )

        # Define check_one with timeout wrapper
        async def check_one_with_timeout(local_path: Path, remote_url: str, mgr) -> bool:
            """Check with timeout wrapper."""
            try:
                return await asyncio.wait_for(check_one(local_path, remote_url, mgr), timeout=30.0)
            except asyncio.TimeoutError:
                logging.warning(f"Check timeout for {remote_url}")
                nonlocal test_checked, files_processed_in_test
                test_checked += 1
                files_processed_in_test += 1
                loop = (
                    asyncio.get_running_loop()
                )  # FIX: Run blocking I/O in executor to unblock event loop
                return await loop.run_in_executor(
                    self._meta_check_executor,
                    self.file_exists_and_up_to_date,
                    local_path,
                    remote_url,
                    True,
                )

        async def check_one(local_path: Path, remote_url: str, mgr) -> bool:
            """Check if a single file needs download."""
            nonlocal test_checked, fallback_triggered, files_processed_in_test

            if fallback_triggered:
                return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)

            # Fast path: check directory signature cache
            if hasattr(self.scanner, "cached_signatures"):
                dir_url = trim_url(remote_url.rsplit("/", 1)[0] + "/")
                if dir_url in self.scanner.cached_signatures:
                    old_sig = self.scanner.cached_signatures[dir_url]
                    new_sig = getattr(self.scanner, "fresh_dir_signatures", {}).get(dir_url)
                    # See file_exists_and_up_to_date for why this compares
                    # fresh vs. cached rather than trusting mere presence.
                    if new_sig and new_sig == old_sig and not new_sig.startswith("url:"):
                        self.metrics.increment("cache_hits")
                        self.metrics.increment("cache_head_requests_saved")
                        test_checked += 1
                        files_processed_in_test += 1
                        return True
                    self.metrics.increment("dir_signature_changed_forced_recheck")

            # If file doesn't exist locally, needs download
            if not local_path.exists():
                test_checked += 1
                files_processed_in_test += 1
                return False

            # Get cached metadata
            stored = self.cache_manager.get_file_metadata(local_path)
            headers = {}

            if stored and stored.get("etag") and not self.config.no_etag:
                headers["If-None-Match"] = stored["etag"]

            try:
                # Use asyncio.wait_for with timeout
                try:
                    resp = await asyncio.wait_for(mgr.head(remote_url, headers), timeout=15.0)
                except asyncio.TimeoutError:
                    logging.debug(f"Async HEAD timeout for {remote_url}")
                    test_checked += 1
                    files_processed_in_test += 1
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)

                if resp is None:
                    test_checked += 1
                    files_processed_in_test += 1
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)

                # Handle 304 Not Modified (already correct - keep this)
                if resp.status_code == 304:
                    self.metrics.increment("etag_304_responses")
                    self.metrics.increment("cache_hits")
                    test_checked += 1
                    files_processed_in_test += 1
                    return True

                # Handle client errors: file doesn't exist or is forbidden → skip safely
                if resp.status_code in (403, 404, 410, 451):
                    self.metrics.increment("files_skipped")  # ✅ Correct metric
                    test_checked += 1
                    files_processed_in_test += 1
                    logging.debug(
                        f"Async HEAD {resp.status_code}, skipping: {sanitize_url_for_log(remote_url)}"
                    )
                    return True  # True = "don't download this file"

                # Handle server errors or other issues: fall back to sync check for safety
                if resp.status_code != 200:
                    test_checked += 1
                    files_processed_in_test += 1
                    logging.debug(
                        f"Async HEAD {resp.status_code}, falling back to sync check: {sanitize_url_for_log(remote_url)}"
                    )
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)

                # Check ETag
                remote_etag = resp.headers.get("ETag")
                if remote_etag and stored and stored.get("etag"):
                    if normalize_etag(remote_etag) == normalize_etag(stored["etag"]):
                        self.metrics.increment("etag_matches")
                        self.metrics.increment("cache_hits")
                        test_checked += 1
                        files_processed_in_test += 1
                        return True
                    else:
                        self.metrics.increment("etag_mismatches")
                        self.metrics.increment("cache_misses")
                        test_checked += 1
                        files_processed_in_test += 1
                        return False

                # Check Last-Modified
                if "Last-Modified" in resp.headers:
                    try:
                        local_ts = local_path.stat().st_mtime
                        dt = parsedate_to_datetime(resp.headers["Last-Modified"])
                        remote_ts = dt.timestamp()

                        if remote_ts > local_ts + TIMESTAMP_TOLERANCE_SECONDS:
                            self.metrics.increment("cache_misses")
                            test_checked += 1
                            files_processed_in_test += 1
                            return False

                        self.metrics.increment("cache_hits")
                        test_checked += 1
                        files_processed_in_test += 1
                        return True
                    except Exception as e:
                        logging.debug(f"Last-Modified parsing error: {e}")

                # Default: assume up to date
                self.metrics.increment("cache_hits")
                test_checked += 1
                files_processed_in_test += 1
                return True

            except Exception as e:
                logging.debug(f"Async check error for {remote_url}: {e}")
                test_checked += 1
                files_processed_in_test += 1
                return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)

        # Use the async task manager for all async operations
        async with manager:
            if not manager.is_available():
                raise RuntimeError("Async manager became unavailable")

            # Profile server if using adaptive async
            if use_adaptive:
                sample_urls = [url for _, url in file_checks[:PROFILE_SAMPLE_SIZE]]
                if sample_urls:
                    try:
                        profile_task = await self.async_task_manager.create_task(
                            manager.profile_server(sample_urls)
                        )
                        profile_result = await asyncio.wait_for(profile_task, timeout=30.0)
                        if not profile_result:
                            logging.warning("Server profiling failed, falling back to sync")
                            self.metrics.metrics["adaptive_fallback_to_sync"] = True
                            return self._check_files_sync(file_items, progress)
                    except asyncio.TimeoutError:
                        logging.warning("Server profiling timed out, falling back to sync")
                        self.metrics.metrics["adaptive_fallback_to_sync"] = True
                        return self._check_files_sync(file_items, progress)

            # Process batches
            for start_idx in range(0, len(file_checks), ASYNC_TEST_BATCH_SIZE):
                if fallback_triggered:
                    break

                batch_start_time = time.time()
                batch = file_checks[start_idx : start_idx + ASYNC_TEST_BATCH_SIZE]

                # Create tasks for this batch using AsyncTaskManager with timeout
                tasks = []
                for local, url in batch:
                    task = await self.async_task_manager.create_task(
                        asyncio.wait_for(check_one_with_timeout(local, url, manager), timeout=30.0)
                    )
                    tasks.append((task, local, url))

                # Wait for batch with timeout
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*[t for t, _, _ in tasks], return_exceptions=True),
                        timeout=120.0,
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"Batch {start_idx} timed out after 120s, falling back to sync")
                    remaining = [(url, path) for path, url in file_checks[start_idx:]]
                    return to_download + self._check_files_sync(remaining, progress)

                # Process results
                batch_needs_download = []
                for (task, local, url), result in zip(tasks, results):
                    if isinstance(result, Exception):
                        logging.warning(f"Async check failed for {url}: {result}")
                        batch_needs_download.append((url, local))
                    elif not result:
                        batch_needs_download.append((url, local))

                to_download.extend(batch_needs_download)

                test_checked += len(batch)
                files_processed_in_test += len(batch)

                if progress is not None:
                    try:
                        progress.update(len(batch))
                    except Exception as e:
                        logging.debug(f"Progress update failed: {e}")

                # Speed test
                elapsed = time.time() - test_start_time
                if elapsed > test_max_seconds or test_checked >= test_min_files:
                    logging.debug(f"Speed test complete: {test_checked} files in {elapsed:.1f}s")
                    break

                # Use a rolling average for more accurate speed measurement
                if test_checked >= 50:
                    # Calculate rolling average over last 5 batches or all batches so far

                    batch_duration = time.time() - batch_start_time
                    batch_speed = len(batch) / batch_duration if batch_duration > 0 else 0
                    self._speed_samples.append(batch_speed)

                    # Keep last 5 samples for rolling average
                    if len(self._speed_samples) > 5:
                        self._speed_samples.pop(0)

                    # Use rolling average for more stable decision
                    avg_speed = sum(self._speed_samples) / len(self._speed_samples)

                    if avg_speed < min_speed_threshold * 0.6:
                        logging.warning(
                            f"Async speed test too slow (avg {avg_speed:.1f} files/s over {len(self._speed_samples)} batches, "
                            f"threshold {min_speed_threshold:.1f}) → falling back to synchronous checking"
                        )
                        fallback_triggered = True
                        self.metrics.metrics["adaptive_fallback_to_sync"] = True
                        break

            # Handle fallback
            if fallback_triggered:
                logging.info("Switching to synchronous mode for remaining files")
                remaining_checks = file_checks[files_processed_in_test:]
                # Convert remaining_checks from (Path, url) to (url, Path) format
                remaining_items = [(url, path) for path, url in remaining_checks]
                remaining_to_download = self._check_files_sync(remaining_items, progress)
                return to_download + remaining_to_download

            # Process remaining files if any
            if files_processed_in_test < len(file_checks):
                logging.info(
                    f"Speed test passed, continuing async check for remaining {len(file_checks) - files_processed_in_test} files"
                )
                for start_idx in range(
                    files_processed_in_test, len(file_checks), ASYNC_TEST_BATCH_SIZE
                ):
                    batch = file_checks[start_idx : start_idx + ASYNC_TEST_BATCH_SIZE]

                    tasks = []
                    for local, url in batch:
                        task = await self.async_task_manager.create_task(
                            asyncio.wait_for(
                                check_one_with_timeout(local, url, manager), timeout=30.0
                            )
                        )
                        tasks.append((task, local, url))

                    try:
                        results = await asyncio.wait_for(
                            asyncio.gather(*[t for t, _, _ in tasks], return_exceptions=True),
                            timeout=120.0,
                        )
                    except asyncio.TimeoutError:
                        logging.warning(f"Batch {start_idx} timed out, falling back to sync")
                        remaining = [(url, path) for path, url in file_checks[start_idx:]]
                        return to_download + self._check_files_sync(remaining, progress)

                    for (task, local, url), result in zip(tasks, results):
                        if isinstance(result, Exception) or not result:
                            to_download.append((url, local))

                    if progress is not None:
                        try:
                            progress.update(len(batch))
                        except Exception as e:
                            logging.debug(f"Progress update failed: {e}")

            # Apply concurrency changes if using adaptive async
            if use_adaptive and hasattr(manager, "apply_pending_concurrency_change"):
                await manager.apply_pending_concurrency_change()

        return to_download

    def get_remote_timestamp(self, url: str) -> Optional[float]:
        """Get remote file timestamp from Last-Modified header."""
        try:
            r = self.connection_manager.request(
                url, method="HEAD", timeout=(15, 30), allow_redirects=True
            )
            if r.status_code == 200 and "Last-Modified" in r.headers:
                dt = parsedate_to_datetime(r.headers["Last-Modified"])
                return dt.timestamp()
        except httpx.RequestError as e:
            logging.debug(f"Failed to get timestamp for {sanitize_url_for_log(url)}: {e}")
        except Exception as e:
            logging.debug(f"Error parsing timestamp for {sanitize_url_for_log(url)}: {e}")
        return None

    def get_directory_size(self, path: Path) -> int:
        """Get total size of directory recursively."""
        total = 0
        for item in path.rglob("*"):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except OSError:
                    pass
        return total
