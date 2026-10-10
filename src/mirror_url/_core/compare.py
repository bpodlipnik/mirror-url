"""CompareMixin: Metadata comparison: which remote files need downloading.

Originally extracted from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.utils import parsedate_to_datetime
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Tuple, Union, cast

import httpx

from ..async_connection import AdaptiveAsyncManager, AsyncConnectionManager, AsyncTaskManager
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
from ..download_integrity import local_content_matches
from ..utils import _relative_url_path, normalize_etag, sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from ..progress import ProgressTracker


if TYPE_CHECKING:
    from ._typing import MirrorHost
else:
    MirrorHost = object


class CompareMixin(MirrorHost):
    async_task_manager: Optional[AsyncTaskManager]

    def _should_check_existing_file(self, remote_url: str) -> bool:
        if not getattr(self.config, "missing_files", False):
            return True
        if not getattr(self.config, "check_files", ()):
            return False
        return self.config.check_file_selected(remote_url)

    def _comparison_metadata(self, local_path: Path, use_cache: bool = True):
        """Only use an ETag belonging to the current local file."""
        stat = local_path.stat()
        stored = None
        if use_cache and not getattr(self.config, "no_cache", False):
            stored = self.cache_manager.get_file_metadata(local_path)
            if stored:
                stored = dict(stored)
                if getattr(self.config, "verify_content", False):
                    stored["_local_verified"] = stored.get("size") == stat.st_size and (
                        local_content_matches(local_path, stored.get("sha256"))
                    )
                else:
                    stored["_local_verified"] = (
                        stored.get("size") == stat.st_size
                        and stored.get("local_mtime_ns") == stat.st_mtime_ns
                        and stored.get("local_ctime_ns") == stat.st_ctime_ns
                    )
        return stat, stored

    def _freshness_headers(self, stored):
        headers = {"Accept-Encoding": "identity"}
        if (
            stored
            and stored.get("_local_verified")
            and stored.get("etag")
            and not self.config.no_etag
        ):
            headers["If-None-Match"] = stored["etag"]
        return headers

    def _response_is_current(self, response, stat, stored) -> bool:
        """Shared sync/async policy; listing validators never validate children."""
        return self._freshness_result(response, stat, stored)[0]

    def _freshness_result(self, response, stat, stored) -> Tuple[bool, str, str]:
        """Return the existing policy decision with its reporting classification."""
        if response.status_code not in (200, 304):
            return False, "uncertain", f"metadata returned HTTP {response.status_code}"
        if getattr(self.config, "verify_content", False) and not (
            stored and stored.get("_local_verified")
        ):
            return False, "uncertain", "local content could not be verified against its receipt"

        raw_size = response.headers.get("Content-Length")
        size_matches = False
        if raw_size is not None:
            try:
                size = int(raw_size)
            except (TypeError, ValueError):
                return False, "uncertain", "invalid remote Content-Length"
            if size < 0 or size != stat.st_size:
                if size < 0 or response.status_code == 304:
                    return False, "uncertain", "invalid or conflicting remote Content-Length"
                return False, "changed", "remote size differs from local size"
            size_matches = True

        if response.status_code == 304:
            valid = bool(
                stored
                and stored.get("_local_verified")
                and stored.get("etag")
                and not self.config.no_etag
            )
            if valid:
                self.metrics.increment("etag_304_responses")
            return (
                (True, "current", "HTTP 304 Not Modified")
                if valid
                else (False, "uncertain", "HTTP 304 without a trusted local validator")
            )

        remote_etag = response.headers.get("ETag")
        if remote_etag and stored and stored.get("etag") and not self.config.no_etag:
            matches = normalize_etag(remote_etag) == normalize_etag(stored["etag"])
            self.metrics.increment("etag_matches" if matches else "etag_mismatches")
            if not matches:
                return False, "changed", "ETag changed"
            if not stored.get("_local_verified"):
                return False, "uncertain", "local file differs from its saved receipt"
            return True, "current", "ETag unchanged"

        last_modified = response.headers.get("Last-Modified")
        if last_modified:
            try:
                remote_ts = parsedate_to_datetime(last_modified).timestamp()
                if remote_ts > stat.st_mtime + TIMESTAMP_TOLERANCE_SECONDS:
                    return False, "changed", "remote modification time is newer"
                return (
                    (True, "current", "size matches and remote modification time is not newer")
                    if size_matches
                    else (False, "uncertain", "remote size is unavailable")
                )
            except (TypeError, ValueError, OverflowError):
                return False, "uncertain", "invalid remote Last-Modified"
        return (
            (True, "current", "size matches; no usable ETag or modification time")
            if size_matches
            else (False, "uncertain", "remote freshness metadata is unavailable")
        )

    def _record_check(self, remote_url: str, outcome: str, reason: str) -> None:
        selected = bool(
            getattr(self.config, "check_files", ())
        ) and self.config.check_file_selected(remote_url)
        self.metrics.run_report.record_check(remote_url, outcome, selected)
        display = _relative_url_path(remote_url, str(getattr(self.config, "base_url", "")))
        display = sanitize_url_for_log(display or remote_url)
        level = logging.DEBUG
        if outcome == "uncertain":
            level = logging.WARNING
        elif outcome == "changed" or (selected and outcome == "current"):
            level = logging.INFO
        action = {"current": "Current", "changed": "Updating", "uncertain": "Revalidating"}.get(
            outcome, "File decision"
        )
        logging.log(level, "%s: %s — %s", action, display, reason)

    @log_performance("file_check")
    def file_exists_and_up_to_date(
        self, local_path: Path, remote_url: str, use_cache: bool = True
    ) -> bool:
        start_time = time.time()
        current = False
        try:
            if not local_path.is_file():
                self._record_check(remote_url, "missing", "local file is absent")
                return False
            if not self._should_check_existing_file(remote_url):
                self.metrics.increment("missing_files_skipped_check")
                self._record_check(
                    remote_url, "unchecked", "freshness check skipped by --missing-files"
                )
                current = True
                return True
            stat, stored = self._comparison_metadata(local_path, use_cache)
            response = self.connection_manager.request(
                remote_url,
                method="HEAD",
                timeout=(10, 20),
                allow_redirects=True,
                headers=self._freshness_headers(stored),
            )
            logging.debug(
                "Freshness HEAD %s: HTTP %s, ETag=%r, Last-Modified=%r, Content-Length=%r",
                sanitize_url_for_log(remote_url),
                response.status_code,
                response.headers.get("ETag"),
                response.headers.get("Last-Modified"),
                response.headers.get("Content-Length"),
            )
            current, outcome, reason = self._freshness_result(response, stat, stored)
            self._record_check(remote_url, outcome, reason)
            self.metrics.increment("cache_hits" if current else "cache_misses")
            return current
        except Exception as e:
            logging.debug(f"Error checking file {local_path}: {e}")
            self._record_check(
                remote_url, "uncertain", f"metadata check failed ({type(e).__name__})"
            )
            self.metrics.increment("cache_misses")
            return False
        finally:
            self.performance_monitor.record("file_check", time.time() - start_time, current)

    def _check_files_sync(
        self,
        remote_files: Union[List[str], List[Tuple[str, Path]]],
        progress: Optional[ProgressTracker] = None,
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
        local_path: Optional[Path]

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
            for future in as_completed(future_to_item):
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
        local_path: Optional[Path]

        if self.symlink_tracker:
            self.symlink_tracker.clear_chain()

        # FIX: Normalize to List[Tuple[str, Path]] consistently (same as sync version)
        file_items: List[Tuple[str, Path]] = []

        if remote_files:
            first_item = remote_files[0]
            if isinstance(first_item, tuple):
                # Already list of tuples
                file_items = list(cast(List[Tuple[str, Path]], remote_files))
            else:
                # List of strings - convert to tuples
                for url in cast(List[str], remote_files):
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
        manager: Union[AdaptiveAsyncManager, AsyncConnectionManager]

        if use_adaptive and self.adaptive_async_manager is not None:
            if not self.adaptive_async_manager.is_available():
                logging.warning("Adaptive async manager not available, falling back to sync")
                return self._check_files_sync(file_items, progress)

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
        # Full-file disk reads can legitimately exceed metadata-only deadlines.
        # Keep the HEAD timeout below, without abandoning a hash still running
        # in a worker and repeating it through the synchronous fallback.
        check_timeout = None if getattr(self.config, "verify_content", False) else 30.0
        batch_timeout = None if getattr(self.config, "verify_content", False) else 120.0

        async def sync_fallback(local_path: Path, remote_url: str) -> bool:
            return await asyncio.get_running_loop().run_in_executor(
                self._meta_check_executor,
                self.file_exists_and_up_to_date,
                local_path,
                remote_url,
                True,
            )

        async def check_one_with_timeout(local_path: Path, remote_url: str, mgr) -> bool:
            try:
                return await asyncio.wait_for(
                    check_one(local_path, remote_url, mgr), timeout=check_timeout
                )
            except asyncio.TimeoutError:
                logging.warning(f"Check timeout for {remote_url}")
                return await sync_fallback(local_path, remote_url)

        async def check_one(local_path: Path, remote_url: str, mgr) -> bool:
            if not local_path.is_file():
                self._record_check(remote_url, "missing", "local file is absent")
                return False
            if not self._should_check_existing_file(remote_url):
                self.metrics.increment("missing_files_skipped_check")
                self._record_check(
                    remote_url, "unchecked", "freshness check skipped by --missing-files"
                )
                return True
            try:
                if getattr(self.config, "verify_content", False):
                    stat, stored = await asyncio.get_running_loop().run_in_executor(
                        self._meta_check_executor, self._comparison_metadata, local_path
                    )
                else:
                    stat, stored = self._comparison_metadata(local_path)
                response = await asyncio.wait_for(
                    mgr.head(remote_url, self._freshness_headers(stored)), timeout=15.0
                )
                if response is None or response.status_code not in (200, 304):
                    return await sync_fallback(local_path, remote_url)
                logging.debug(
                    "Freshness HEAD %s: HTTP %s, ETag=%r, Last-Modified=%r, Content-Length=%r",
                    sanitize_url_for_log(remote_url),
                    response.status_code,
                    response.headers.get("ETag"),
                    response.headers.get("Last-Modified"),
                    response.headers.get("Content-Length"),
                )
                current, outcome, reason = self._freshness_result(response, stat, stored)
                self._record_check(remote_url, outcome, reason)
                self.metrics.increment("cache_hits" if current else "cache_misses")
                return current
            except Exception as e:
                logging.debug(f"Async check error for {remote_url}: {e}")
                return await sync_fallback(local_path, remote_url)

        # Use the async task manager for all async operations
        async with manager:
            if not manager.is_available():
                raise RuntimeError("Async manager became unavailable")

            # Profile server if using adaptive async
            if use_adaptive:
                sample_urls = list(
                    islice(
                        (url for _, url in file_checks if self._should_check_existing_file(url)),
                        PROFILE_SAMPLE_SIZE,
                    )
                )
                if sample_urls:
                    try:
                        profile_task = await self.async_task_manager.create_task(
                            cast(AdaptiveAsyncManager, manager).profile_server(sample_urls)
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
                        asyncio.wait_for(
                            check_one_with_timeout(local, url, manager), timeout=check_timeout
                        )
                    )
                    tasks.append((task, local, url))

                # Wait for batch with timeout
                try:
                    results = await asyncio.wait_for(
                        asyncio.gather(*[t for t, _, _ in tasks], return_exceptions=True),
                        timeout=batch_timeout,
                    )
                except asyncio.TimeoutError:
                    logging.warning(f"Batch {start_idx} timed out after 120s, falling back to sync")
                    remaining = [(url, path) for path, url in file_checks[start_idx:]]
                    return to_download + self._check_files_sync(remaining, progress)

                # Resize the existing semaphore before admitting the next batch.
                if use_adaptive and hasattr(manager, "apply_pending_concurrency_change"):
                    await manager.apply_pending_concurrency_change()

                # Process results
                batch_needs_download = []
                for (_task, local, url), result in zip(tasks, results, strict=False):
                    if isinstance(result, BaseException):
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
                        self._speed_samples.popleft()

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

                    if use_adaptive and hasattr(manager, "apply_pending_concurrency_change"):
                        await manager.apply_pending_concurrency_change()

                    for (_task, local, url), result in zip(tasks, results, strict=False):
                        if isinstance(result, BaseException) or not result:
                            to_download.append((url, local))

                    if progress is not None:
                        try:
                            progress.update(len(batch))
                        except Exception as e:
                            logging.debug(f"Progress update failed: {e}")

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
