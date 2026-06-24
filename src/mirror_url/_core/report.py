"""ReportMixin: Top-level sync orchestration, summaries, and benchmarking.

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
from typing import Any, Dict

from ..async_connection import AdaptiveAsyncManager, AsyncConnectionManager, AsyncTaskManager
from ..compat import TQDM_AVAILABLE
from ..constants import ADAPTIVE_START_CONCURRENCY, AUTO_CONCURRENCY_SAMPLES
from ..decorators import log_performance
from ..enums import CleanupPolicy, DownloadMethod
from ..progress import ProgressTracker
from ..utils import format_bytes, format_duration, sanitize_url_for_log


class ReportMixin:
    def sync(self) -> bool:
        """Main sync method - v3.0.2 with true parallel file downloads."""
        prefix = self._get_prefix()
        if not hasattr(self, "connection_manager") or not self.connection_manager:
            logging.warning(f"{prefix}Skipping sync (connection failed)")
            self._print_early_exit_summary(prefix)
            return False  # Connection failed

        # FIX v2.0.1: Early exit if connection is not OK
        if not self.connection_ok:
            prefix = self._get_prefix()
            logging.info(
                f"{prefix}Skipping sync - remote directory not available (connection_ok=False)"
            )
            self._print_early_exit_summary(prefix)
            # Sync atomic counters to metrics for accurate reporting
            if hasattr(self, "metrics"):
                self.metrics.metrics["files_downloaded"] = self.files_processed.value()
                self.metrics.metrics["files_skipped"] = self.files_skipped.value()
                self.metrics.metrics["files_failed"] = self.files_failed.value()
                self.metrics.metrics["bytes_downloaded"] = self.total_downloaded_size.value()
            return False  # Connection failed

        # Fix: Recreate async managers for each sync run to ensure clean state
        if self.config.async_metadata:
            try:
                if self.config.adaptive_async:
                    # Close old manager if it exists
                    if (
                        self.adaptive_async_manager
                        and hasattr(self.adaptive_async_manager, "_client")
                        and self.adaptive_async_manager._client
                    ):
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                loop.create_task(
                                    self.adaptive_async_manager.__aexit__(None, None, None)
                                )
                        except RuntimeError:
                            pass

                    # Create fresh manager
                    self.adaptive_async_manager = AdaptiveAsyncManager(self.config, self.metrics)
                    self.scanner.adaptive_manager = self.adaptive_async_manager
                    logging.debug(f"{prefix}Adaptive async manager recreated for sync")
                else:
                    # Close old manager if it exists
                    if (
                        self.async_connection_manager
                        and hasattr(self.async_connection_manager, "_client")
                        and self.async_connection_manager._client
                    ):
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                loop.create_task(
                                    self.async_connection_manager.__aexit__(None, None, None)
                                )
                        except RuntimeError:
                            pass

                    # Create fresh manager
                    self.async_connection_manager = AsyncConnectionManager(
                        self.config, self.metrics
                    )
                    logging.debug(f"{prefix}Async connection manager recreated for sync")

                # Recreate task manager too
                if self.async_task_manager:
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            loop.create_task(self.async_task_manager.shutdown())
                        else:
                            new_loop = asyncio.new_event_loop()
                            new_loop.run_until_complete(self.async_task_manager.shutdown())
                            new_loop.close()
                    except RuntimeError:
                        pass

                self.async_task_manager = AsyncTaskManager()
                logging.debug(f"{prefix}Async task manager recreated for sync")

            except Exception as e:
                logging.warning(f"{prefix}Failed to recreate async managers: {e}")
                # Set to None so sync fallback is used
                self.adaptive_async_manager = None
                self.async_connection_manager = None
                self.async_task_manager = None

        prefix = self._get_prefix()
        logging.info(f"{prefix}Starting sync for: '{self.config.dir_suffix or 'ROOT'}'")
        start = time.time()

        try:
            if self.config.quick:
                logging.info(f"{prefix}Quick mode - updating cache timestamp")
                if self.cache_file.exists() and not self.config.dry_run:
                    self.cache_file.touch()
                    logging.info(f"{prefix}Cache timestamp updated")

                duration = time.time() - start
                logging.info("-" * 50)
                logging.info(f"{prefix}QUICK MODE SUMMARY:")
                logging.info(f"{prefix}  Duration: {format_duration(duration)}")
                logging.info("-" * 50)
                return True

            # FIX v2.0.1: Skip disk space check in dry-run mode
            if not self.config.dry_run and not self.check_disk_space(100 * 1024 * 1024):
                logging.error(f"{prefix}Insufficient disk space to start")
                return False

            remote_files = self.get_remote_files()
            if remote_files is None:
                logging.error(f"{prefix}Failed to get remote files - aborting sync")
                return False

            if not isinstance(remote_files, list):
                logging.error(f"{prefix}Invalid remote_files type: {type(remote_files)}")
                return False

            # FIX: In dry-run mode, we still need to check which files exist locally
            if self.config.dry_run:
                logging.info(f"{prefix}DRY RUN MODE - Simulating what would happen")

                # First, get the list of files that would be downloaded
                # This requires checking local files
                if len(remote_files) > 0:
                    progress = ProgressTracker(
                        total=len(remote_files),
                        prefix=prefix,
                        name="files checked",
                        use_tqdm=TQDM_AVAILABLE,
                        config=self.config,
                    )
                    self.multi_progress.add_level(
                        "files", len(remote_files), prefix, self.config.progress_bar, self.config
                    )
                else:
                    progress = None

                # Determine which files would be downloaded
                # In dry-run mode, use sync checks for speed (avoid adaptive profiling delays)
                use_async = False  # Force sync mode in dry-run

                if use_async:
                    if self.config.adaptive_async and self.adaptive_async_manager:
                        logging.info(
                            f"{prefix}Using ADAPTIVE async metadata checks ({len(remote_files)} files)"
                        )
                    else:
                        logging.info(
                            f"{prefix}Using async metadata checks ({len(remote_files)} files)"
                        )

                    # Check if we're already in an async context
                    try:
                        loop = asyncio.get_running_loop()
                        logging.warning(f"{prefix}Already in async context, using sync mode")
                        to_download = self._check_files_sync(remote_files, progress)
                    except RuntimeError:
                        try:
                            to_download = asyncio.run(
                                self._check_files_async(remote_files, progress)
                            )
                        except Exception as e:
                            logging.warning(
                                f"{prefix}Async metadata check failed ({e}), falling back to sync mode"
                            )
                            to_download = self._check_files_sync(remote_files, progress)

                else:
                    logging.info(
                        f"{prefix}Using sync metadata checks (dry-run simulation - faster)"
                    )
                    to_download = self._check_files_sync(remote_files, progress)

                if progress:
                    progress.report_final()

                # Show what would be downloaded
                if to_download:
                    sample_size = min(10, len(to_download))
                    logging.info(
                        f"{prefix}Would download {len(to_download)} files (showing first {sample_size}):"
                    )
                    for i, (url, local_path) in enumerate(to_download[:sample_size]):
                        logging.info(
                            f"{prefix}  {i + 1}. {sanitize_url_for_log(url)} -> {local_path}"
                        )
                    if len(to_download) > sample_size:
                        logging.info(f"{prefix}  ... and {len(to_download) - sample_size} more")
                else:
                    logging.info(f"{prefix}No files would be downloaded - all up to date")

                # Show what would be cleaned up
                if self.config.cleanup_policy in (
                    CleanupPolicy.PREVIEW,
                    CleanupPolicy.DELETE,
                    CleanupPolicy.MOVE,
                ):
                    self.clean_obsolete(set(remote_files))

                duration = time.time() - start
                logging.info("-" * 50)
                logging.info(f"{prefix}DRY RUN SUMMARY:")
                logging.info(f"{prefix}  Remote files found: {len(remote_files)}")
                logging.info(f"{prefix}  Files that would be downloaded: {len(to_download)}")
                logging.info(
                    f"{prefix}  Files that are up to date: {len(remote_files) - len(to_download)}"
                )
                logging.info(f"{prefix}  Duration: {format_duration(duration)}")
                logging.info("-" * 50)
                return True

            if len(remote_files) > 0:
                progress = ProgressTracker(
                    total=len(remote_files),
                    prefix=prefix,
                    name="files checked",
                    use_tqdm=TQDM_AVAILABLE,
                    config=self.config,
                )
                self.multi_progress.add_level(
                    "files", len(remote_files), prefix, self.config.progress_bar, self.config
                )
            else:
                logging.info(f"{prefix}No files to check")
                progress = None

            to_download = []

            # FIX (test 29 / files_skipped accounting): reset the check-phase
            # counters here, BEFORE the up-to-date check runs, not after.
            # Both _check_files_sync() and _check_files_async() legitimately
            # increment files_skipped (already-up-to-date files, skipped
            # symlinks, etc.) and files_failed (per-file check errors) while
            # they run. Those increments need to survive into the final
            # tally for this sync() call. Resetting here (once, up front)
            # still gives each sync() call a clean baseline, without wiping
            # out the check phase's results the way the old post-check
            # reset did (see the "before downloads" block below, which now
            # only resets the download-phase counters).
            self.files_skipped.reset()
            self.files_failed.reset()

            use_async = (
                self.config.async_metadata
                and (self.adaptive_async_manager or self.async_connection_manager)
                and len(remote_files) > 80
            )

            if use_async:
                # ========== WARM UP ASYNC CONNECTIONS ==========
                if self.config.connection_pool_prewarm:
                    sample_urls = remote_files[:20] if remote_files else []
                    if sample_urls:
                        logging.info(f"{prefix}🔥 Pre-warming async connections")

                        if self.adaptive_async_manager:
                            # For adaptive, warm-up is built into profile_server
                            pass
                        elif self.async_connection_manager:
                            # FIX: Create event loop properly in a background thread
                            def async_warm_up_worker():
                                try:
                                    # Create new event loop for this thread
                                    loop = asyncio.new_event_loop()
                                    asyncio.set_event_loop(loop)

                                    # Run the warm-up with timeout
                                    try:
                                        loop.run_until_complete(
                                            asyncio.wait_for(
                                                self.async_connection_manager.warm_up(sample_urls),
                                                timeout=30.0,
                                            )
                                        )
                                    except asyncio.TimeoutError:
                                        logging.warning(
                                            f"{prefix}Async warm-up timed out after 30 seconds"
                                        )
                                    except Exception as e:
                                        logging.debug(f"{prefix}Async warm-up error: {e}")
                                    finally:
                                        # Clean up pending tasks
                                        pending = asyncio.all_tasks(loop)
                                        for task in pending:
                                            task.cancel()
                                        loop.close()

                                except Exception as e:
                                    logging.debug(f"{prefix}Warm-up thread error: {e}")

                            # Start warm-up in background thread
                            warmup_thread = threading.Thread(
                                target=async_warm_up_worker, daemon=True
                            )
                            warmup_thread.start()
                # ========== END ASYNC WARM-UP ==========

                if self.config.adaptive_async and self.adaptive_async_manager:
                    logging.info(
                        f"{prefix}Using ADAPTIVE async metadata checks ({len(remote_files)} files)"
                    )
                else:
                    logging.info(f"{prefix}Using async metadata checks ({len(remote_files)} files)")

                try:
                    to_download = asyncio.run(self._check_files_async(remote_files, progress))
                except Exception as e:
                    logging.warning(
                        f"{prefix}Async metadata check failed ({e}), falling back to sync mode"
                    )
                    to_download = self._check_files_sync(remote_files, progress)

                if self.config.adaptive_async and self.adaptive_async_manager:
                    self.metrics.metrics["adaptive_current_concurrency"] = getattr(
                        self.adaptive_async_manager,
                        "_current_concurrency",
                        ADAPTIVE_START_CONCURRENCY,
                    )

                    if self.adaptive_async_manager.should_fallback():
                        self.metrics.metrics["adaptive_fallback_to_sync"] = True
                        logging.info(f"{prefix}⚠️ Adaptive async fell back to sync mode")

                self.files_skipped.reset()
                self.files_skipped.increment(max(0, len(remote_files) - len(to_download)))
                self.metrics.metrics["files_skipped"] = self.files_skipped.value()
            else:
                # Explicitly log why we are using sync mode
                if len(remote_files) <= 80:
                    logging.info(
                        f"{prefix}Using sync metadata checks (batch size {len(remote_files)} ≤ 80, skipping async overhead)"
                    )
                else:
                    logging.info(f"{prefix}Using sync metadata checks (async disabled/unavailable)")
                to_download = self._check_files_sync(remote_files, progress)

            if progress:
                progress.report_final()

            # FIX v3.0.1: Reset counters before downloads.
            # NOTE: files_skipped / files_failed are deliberately NOT reset
            # here anymore — they're reset once up front (before the
            # up-to-date check phase, see above) and the check phase's
            # counts must survive into the download phase, since a file
            # that's already up to date never enters to_download and would
            # otherwise never be counted as "skipped" at all.
            self.files_processed.reset()
            self.total_downloaded_size.reset()

            if to_download:
                # ========== FIX 1: PARALLELIZE SIZE FETCHING ==========
                # Avoid sequential HEAD request bottleneck by fetching sizes concurrently
                total_size = 0
                file_sizes = []
                size_map = {}  # url -> size mapping to avoid redundant HEAD requests

                max_workers = min(20, len(to_download))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all size checks concurrently
                    future_to_url = {
                        executor.submit(self._get_file_size, url): url for url, _ in to_download
                    }

                    # Collect results with robust error handling
                    for future in as_completed(future_to_url):
                        url = future_to_url[future]
                        try:
                            size = future.result()
                            # FIX: Explicitly store 0 on None to prevent type errors downstream
                            size_map[url] = size if size else 0
                            if size:
                                total_size += size
                                file_sizes.append(size)
                            else:
                                file_sizes.append(0)
                        except Exception:
                            # FIX: Always store a value to prevent KeyError later
                            size_map[url] = 0
                            file_sizes.append(0)

                # ========== FIX 2: AUTO-SELECT DOWNLOAD METHOD (FULL IMPLEMENTATION) ==========
                # Only auto-select if user didn't explicitly enable a download mode
                if (
                    self.parallel_manager
                    and not self.config.parallel_downloads
                    and not self.config.streaming_parallel
                    and not self.config.sequential_downloads
                ):
                    sample_urls = [url for url, _ in to_download[:10]]
                    method = self.parallel_manager.auto_select_method(
                        file_sizes=file_sizes, total_files=len(to_download), remote_urls=sample_urls
                    )

                    # FIX: Actually apply the selected method by configuring config flags
                    if method == DownloadMethod.SEQUENTIAL:
                        self.config.sequential_downloads = True
                        if self.parallel_manager:
                            self.parallel_manager.enabled = False
                        logging.info(f"{prefix}📊 Auto-selected: SEQUENTIAL downloads")

                    elif method == DownloadMethod.STREAMING_PARALLEL:
                        self.config.streaming_parallel = True
                        if self.parallel_manager:
                            self.parallel_manager.enabled = True
                            self.parallel_manager.use_streaming = True
                        logging.info(f"{prefix}📊 Auto-selected: STREAMING PARALLEL downloads")

                    elif method == DownloadMethod.TRADITIONAL_PARALLEL:
                        self.config.parallel_downloads = True
                        if self.parallel_manager:
                            self.parallel_manager.enabled = True
                            self.parallel_manager.use_streaming = False
                        logging.info(f"{prefix}📊 Auto-selected: TRADITIONAL PARALLEL downloads")

                # ========== FIX 3: CHECK DISK SPACE ==========
                if not self.check_disk_space(total_size):
                    logging.error(f"{prefix}Insufficient disk space for downloads")
                    return False

                logging.info(
                    f"{prefix}Downloading {len(to_download)} files (approx {format_bytes(total_size)})"
                )
                self.multi_progress.add_level(
                    "downloads", len(to_download), prefix, self.config.progress_bar, self.config
                )

                # ========== DOWNLOAD EXECUTION ==========
                if self.config.sequential_downloads:
                    # Sequential mode: simple loop
                    for url, path in to_download:
                        # FIX: Pass pre-fetched size to avoid redundant HEAD request
                        pre_fetched_size = size_map.get(url, 0)
                        success = self.download_file_with_resume(url, path)
                        if success:
                            self.multi_progress.update("downloads")
                        else:
                            self.files_failed.increment(1)

                elif self.config.parallel_downloads or self.config.streaming_parallel:
                    # Parallel mode: ThreadPoolExecutor with size pass-through
                    max_parallel = min(self.config.max_concurrent_downloads, len(to_download))

                    # Auto-tune concurrency if enabled
                    if self.auto_tuner:
                        max_parallel = self.auto_tuner.get_concurrency()
                        logging.info(
                            f"{prefix}🤖 Auto-tuning: using {max_parallel} parallel downloads"
                        )

                    logging.info(f"{prefix}🚀 Starting {max_parallel} parallel file downloads")
                    start_time = time.time()
                    downloaded_count = 0
                    last_throughput_log = start_time

                    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                        # Submit all download tasks with pre-fetched sizes
                        future_to_file = {}
                        for url, path in to_download:
                            pre_fetched_size = size_map.get(
                                url, 0
                            )  # FIX: Pass size to avoid duplicate HEAD
                            future = executor.submit(
                                self.download_file_with_resume, url, path, pre_fetched_size
                            )
                            future_to_file[future] = (url, path)

                        # Process results as they complete
                        for future in as_completed(future_to_file):
                            url, path = future_to_file[future]
                            try:
                                success = future.result(timeout=300)
                                downloaded_count += 1
                                if success:
                                    self.multi_progress.update("downloads")
                                # else:
                                #    ⛔ DO NOT increment here. _download_file_single() already
                                #    increments files_failed before returning False. Doing so
                                #    would double-count every handled failure.

                                # Auto-tune after every N downloads
                                if (
                                    self.auto_tuner
                                    and downloaded_count % AUTO_CONCURRENCY_SAMPLES == 0
                                ):
                                    elapsed = time.time() - start_time
                                    if elapsed > 0:
                                        downloaded_bytes = self.total_downloaded_size.value()
                                        throughput = (downloaded_bytes / (1024 * 1024)) / elapsed
                                        new_concurrency = self.auto_tuner.record_throughput(
                                            max_parallel, throughput
                                        )
                                        if new_concurrency and new_concurrency != max_parallel:
                                            remaining = len(to_download) - downloaded_count
                                            if new_concurrency <= remaining:
                                                logging.info(
                                                    f"{prefix}🤖 Adjusting concurrency: {max_parallel} → {new_concurrency} "
                                                    f"(throughput: {throughput:.2f} MB/s after {downloaded_count} files)"
                                                )
                                                max_parallel = new_concurrency

                                # Log throughput periodically
                                if time.time() - last_throughput_log > 10:
                                    downloaded_bytes = self.total_downloaded_size.value()
                                    elapsed = time.time() - start_time
                                    throughput = (
                                        (downloaded_bytes / (1024 * 1024)) / elapsed
                                        if elapsed > 0
                                        else 0
                                    )
                                    logging.info(
                                        f"{prefix}📊 Throughput: {throughput:.2f} MB/s after {downloaded_count} files, "
                                        f"concurrency={max_parallel}"
                                    )
                                    last_throughput_log = time.time()

                            except Exception as e:
                                # ⚠️ Only triggers on uncaught exceptions (e.g., exhausted
                                # connection retries that _download_file_single re-raises).
                                # Safe to increment here exactly once.
                                logging.error(f"Download failed for {url}: {e}")
                                self.files_failed.increment(1)

                    # Log final auto-tuning stats
                    if self.auto_tuner:
                        tuner_stats = self.auto_tuner.get_stats()
                        self.metrics.metrics["auto_concurrency_enabled"] = True
                        self.metrics.metrics["auto_concurrency_adjustments"] = tuner_stats[
                            "adjustments"
                        ]
                        self.metrics.metrics["auto_concurrency_final"] = tuner_stats[
                            "current_concurrency"
                        ]
                        self.metrics.metrics["auto_concurrency_start"] = tuner_stats[
                            "start_concurrency"
                        ]
                        logging.info(
                            f"{prefix}🤖 Auto-concurrency stats: {tuner_stats['adjustments']} adjustments, "
                            f"final concurrency={tuner_stats['current_concurrency']}, "
                            f"final throughput={tuner_stats['last_throughput']:.2f} MB/s"
                        )

                    logging.info(
                        f"{prefix}✅ Completed {self.files_processed.value()} file downloads in parallel"
                    )
                    self.metrics.update_queue_metrics(
                        len(self.download_queue), self.config.download_queue_size
                    )

                # ========== END PARALLEL DOWNLOADS SECTION ==========

            # FIX v2.0.1: Skip directory size check in dry-run if directory doesn't exist
            if self.target_dir and self.target_dir.exists():
                disk_size = self.get_directory_size(self.target_dir) / (1024 * 1024)
                logging.info(f"{prefix}On-disk size: {disk_size:.2f} MB")
            else:
                logging.info(f"{prefix}On-disk size: 0.00 MB (directory not created)")

            # FIX v2.0.1: Skip cleanup in dry-run mode (already handled above)
            if remote_files and not self.config.dry_run:
                self.clean_obsolete(set(remote_files))

            duration = time.time() - start
            # total_mb = self.total_downloaded_size / (1024 * 1024)
            # speed = total_mb / duration if duration > 0 else 0

            # Get final values from atomic counters
            # FIX: Sync AtomicCounters to Metrics Collector before summary
            # This ensures metrics.report() shows correct values from AtomicCounters
            if hasattr(self, "metrics"):
                self.metrics.metrics["files_downloaded"] = self.files_processed.value()
                self.metrics.metrics["bytes_downloaded"] = self.total_downloaded_size.value()
                self.metrics.metrics["files_skipped"] = self.files_skipped.value()
                self.metrics.metrics["files_failed"] = self.files_failed.value()

            downloaded_files = self.files_processed.value()
            downloaded_bytes = self.total_downloaded_size.value()
            skipped_files = (
                self.files_skipped.value()
                if hasattr(self.files_skipped, "value")
                else self.files_skipped
            )
            failed_files = (
                self.files_failed.value()
                if hasattr(self.files_failed, "value")
                else self.files_failed
            )

            # Also get from metrics if counters weren't updated (fallback)
            if downloaded_files == 0 and hasattr(self, "metrics"):
                downloaded_files = self.metrics.metrics.get("files_downloaded", 0)
                downloaded_bytes = self.metrics.metrics.get("bytes_downloaded", 0)
                if skipped_files == 0:
                    skipped_files = self.metrics.metrics.get("files_skipped", 0)
                if failed_files == 0:
                    failed_files = self.metrics.metrics.get("files_failed", 0)

            logging.info("-" * 50)
            logging.info(f"{prefix}SUMMARY:")
            logging.info(f"{prefix}  Downloaded: {downloaded_files}")
            logging.info(f"{prefix}  Skipped: {skipped_files}")
            logging.info(f"{prefix}  Failed: {failed_files}")
            logging.info(f"{prefix}  Size: {format_bytes(downloaded_bytes)}")

            if self.target_dir and self.target_dir.exists():
                disk_size = self.get_directory_size(self.target_dir) / (1024 * 1024)
                logging.info(f"{prefix}  On disk: {disk_size:.2f} MB")

            downloaded_mb = downloaded_bytes / (1024 * 1024)
            speed = downloaded_mb / duration if duration > 0 else 0
            logging.info(f"{prefix}  Speed: {speed:.2f} MB/s")
            logging.info(f"{prefix}  Duration: {format_duration(duration)}")
            logging.info("-" * 50)

            logging.info(self.metrics.report(prefix, show_stats=self.config.stats))

            if hasattr(self.scanner, "get_parse_stats"):
                try:
                    parse_stats = self.scanner.get_parse_stats()
                    fast_parses = parse_stats.get("fast_parses", 0)
                    lxml_parses = parse_stats.get("lxml_parses", 0)
                    logging.info(
                        f"{prefix}  Parse stats: {fast_parses + lxml_parses} directories parsed"
                    )
                except Exception as e:
                    logging.debug(f"Error reporting parse stats: {e}")

            perf_summary = self.performance_monitor.get_summary()
            logging.info(
                f"{prefix}  Performance: {perf_summary['total_operations']} operations tracked"
            )

            if self.parallel_manager:
                parallel_stats = self.parallel_manager.get_stats()
                if (
                    parallel_stats.get("active_files", 0) > 0
                    or parallel_stats.get("active_chunks", 0) > 0
                ):
                    logging.info(f"{prefix}  📦 Parallel downloads:")
                    logging.info(f"{prefix}    Active files: {parallel_stats['active_files']}")
                    logging.info(f"{prefix}    Active chunks: {parallel_stats['active_chunks']}")
                    logging.info(
                        f"{prefix}    Chunk downloads: {self.metrics.metrics.get('chunk_downloads', 0)}"
                    )
                    logging.info(
                        f"{prefix}    Chunk assemblies: {self.metrics.metrics.get('chunk_assemblies', 0)}"
                    )
                    logging.info(
                        f"{prefix}    Chunk failures: {self.metrics.metrics.get('chunk_failures', 0)}"
                    )

            # NEW v3.0.0: Log parallel download stats
            if self.parallel_manager:
                parallel_stats = self.parallel_manager.get_stats()
                if parallel_stats["active_files"] > 0 or parallel_stats["active_chunks"] > 0:
                    logging.info(
                        f"{prefix}  Parallel downloads: {parallel_stats['active_files']} files, "
                        f"{parallel_stats['active_chunks']} chunks active"
                    )

            # Add filename cache stats to metrics
            filename_cache_stats = self._get_filename_cache_stats()
            if filename_cache_stats["size"] > 0:
                logging.info(
                    f"{prefix}  Filename cache: {filename_cache_stats['size']} entries, "
                    f"hit rate: {filename_cache_stats['hit_rate']:.1f}%"
                )

            if self.config.metrics_json and not self.config.dry_run:
                self.metrics.export_json(self.config.metrics_json, self.config)

            # Bug fix: previously this read ``return self.files_failed == 0``,
            # but ``files_failed`` is an AtomicCounter (object) — comparing the
            # object itself to 0 is always False, so ``sync()`` ALWAYS reported
            # failure even on a clean run. Use ``.value()`` to read the int.
            if self.files_failed.value() > 0:
                logging.warning(f"{prefix}Sync completed with {self.files_failed.value()} failures")
                return False

            logging.info(f"{prefix}Sync completed successfully")
            return True

        except Exception as e:
            logging.critical(f"{prefix}Fatal error: {e}", exc_info=True)
            self.metrics.add_error(str(e), "fatal")
            return False

    def _print_early_exit_summary(self, prefix: str) -> None:
        """Print summary when skipping sync due to connection failure."""
        logging.info("-" * 50)
        logging.info(f"{prefix}SUMMARY:")
        logging.info(f"{prefix}  Downloaded: 0")
        logging.info(f"{prefix}  Skipped: 0")
        logging.info(f"{prefix}  Failed: 1 directory not found")
        logging.info(f"{prefix}  Duration: 0s")
        logging.info(f"{prefix}  Status: Remote directory not found (404)")
        logging.info("-" * 50)

    @log_performance("benchmark")
    def benchmark(self) -> Dict[str, Any]:
        """Run performance benchmark."""
        results = {
            "connection_test": False,
            "parse_time": 0.0,
            "check_time": 0.0,
            "total_time": 0.0,
            "performance_stats": {},
        }

        start = time.time()

        results["connection_test"] = self.test_connection()

        parse_start = time.time()
        remote_files = self.get_remote_files()
        results["parse_time"] = time.time() - parse_start

        if remote_files:
            check_start = time.time()
            to_download = self._check_files_sync(remote_files[:100])
            results["check_time"] = time.time() - check_start
            results["files_checked"] = len(remote_files[:100])
            results["files_to_download"] = len(to_download)

        results["total_time"] = time.time() - start
        results["performance_stats"] = self.performance_monitor.get_summary()

        if hasattr(self.cache_manager, "lru_file_cache"):
            results["cache_stats"] = self.cache_manager.lru_file_cache.get_stats()

        return results
