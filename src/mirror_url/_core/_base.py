"""_MirrorBase: Construction, lifecycle, logging, and shared connection bring-up.

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import shlex
import signal
import socket
import sys
import tempfile
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union
from urllib.parse import ParseResult, unquote, urlparse

import httpx

from .._version import __version__
from ..async_connection import AdaptiveAsyncManager, AsyncConnectionManager, AsyncTaskManager
from ..cache import CacheManager
from ..compat import LXML_AVAILABLE, PSUTIL_AVAILABLE, TQDM_AVAILABLE
from ..concurrency import UnifiedConcurrencyManager
from ..connection import ConnectionManager
from ..constants import ADAPTIVE_MAX_CONCURRENCY, CONTENT_HASH_THRESHOLD, DEFAULT_RATE_LIMIT
from ..download import ParallelDownloadManager, PartialDownloadManager
from ..enums import CleanupPolicy
from ..exceptions import URLScopeError
from ..health import HealthChecker, HealthCheckServer
from ..metrics import MetricsCollector
from ..monitoring import DiskSpaceManager, MemoryMonitor, PerformanceMonitor
from ..parsing import AdaptiveBatchProcessor
from ..primitives import AtomicCounter, AtomicSize, LRUCache
from ..progress import MultiLevelProgress
from ..queue import DownloadQueue
from ..rate_limiter import BandwidthLimiter, PerIPRateLimiter
from ..scanner import DirectoryScanner
from ..security import PathSafety, SymlinkTracker
from ..storage import DiskBackedSet, FileSystemCache
from ..tuner import AutoConcurrencyTuner
from ..utils import _log_files, sanitize_url_for_log, trim_url

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from ..config import MirrorConfig


class _MirrorBase:
    def _get_prefix(self) -> str:
        """
        Get log prefix for multi-suffix operations.

        Returns:
            Prefix string like "[1/5] " or empty string
        """
        return f"[{self.suffix_index}/{self.total_suffixes}] " if self.total_suffixes > 1 else ""

    def _log_cleanup_policy(self) -> None:
        """Log cleanup policy with appropriate icons."""
        if self.config.cleanup_policy == CleanupPolicy.DELETE:
            if self.config.confirm_delete:
                logging.info("🔐 DELETE MODE with confirmation")
            else:
                logging.warning("⚠️ DELETE MODE: Deletion ENABLED")
        elif self.config.cleanup_policy == CleanupPolicy.MOVE:
            logging.info("📦 MOVE MODE: Obsolete files will be moved to _obsolete folder")
        elif self.config.cleanup_policy == CleanupPolicy.PREVIEW:
            logging.info("🔍 PREVIEW MODE")
        else:
            logging.info("✅ SAFE MODE: Deletion DISABLED")

    def _signal_handler(self, signum: int, frame) -> None:
        """
        Handle shutdown signals gracefully.

        Args:
            signum: Signal number
            frame: Current stack frame
        """
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        cleanup_complete = threading.Event()

        def do_cleanup():
            try:
                self.cleanup()
            except Exception as e:
                logging.error(f"Cleanup error during signal handler: {e}")
            finally:
                cleanup_complete.set()

        cleanup_thread = threading.Thread(target=do_cleanup, daemon=True)
        cleanup_thread.start()

        if not cleanup_complete.wait(timeout=30):
            logging.warning("Cleanup did not complete within 30s timeout, exiting anyway")
            sys.exit(0)

    def __init__(self, config: MirrorConfig, suffix_index: int = 0, total_suffixes: int = 1):
        """
        Initialize MirrorURL instance with proper attribute initialization and error handling.

        Args:
            config: MirrorConfig instance with all settings
            suffix_index: Current suffix index (for multi-suffix operations)
            total_suffixes: Total number of suffixes being processed
        """

        # ============================================================================
        # 1. BASIC CONFIGURATION - Initialize all attributes with safe defaults
        # ============================================================================
        self.config = config
        self.suffix_index = suffix_index
        self.total_suffixes = total_suffixes
        self.is_dry_run = config.dry_run

        # ============================================================================
        # 2. COUNTERS AND STATE - Initialize all counters to safe defaults
        # ============================================================================
        # Use atomic counters for thread safety
        self.files_processed = AtomicCounter(0)  # Changed to AtomicCounter
        self.files_skipped = AtomicCounter(0)  # Changed to AtomicCounter
        self.files_failed = AtomicCounter(0)  # Changed to AtomicCounter
        self.total_downloaded_size = AtomicSize()  # Changed to AtomicSize
        self.dir_timestamps: Dict[str, float] = {}
        self.start_time = time.time()
        self.job_start_time = datetime.now()
        self.connection_ok = True
        # FIX (partial-scan guard): set to True if any directory failed to
        # list during remote discovery (see ScanMixin._discover_directories_bfs
        # and ScanMixin.get_remote_files). clean_obsolete() refuses to run
        # while this is True, since an incomplete listing would misreport
        # still-remote files as obsolete. Reset at the start of each
        # get_remote_files() call.
        self.scan_incomplete = False
        self._speed_samples: deque = deque(maxlen=20)  # Keep last 20 samples

        # ============================================================================
        # 3. METRICS COLLECTOR
        # ============================================================================
        self.metrics = MetricsCollector()

        # ============================================================================
        # 4. PATHS - Initialize all path attributes to None (safe default)
        # ============================================================================
        self.dest_path = config.dest_path
        self.log_path = config.log_path
        self.target_dir: Optional[Path] = None
        self._target_dir_path: Optional[Path] = None
        self.cache_file: Optional[Path] = None

        # ============================================================================
        # 5. LOGGING STATE
        # ============================================================================
        self.log_handlers: List[logging.Handler] = []
        self._logging_configured = False

        # ============================================================================
        # 6. URL SETUP - Parse and normalize base URL
        # ============================================================================
        parsed_url = urlparse(str(config.base_url))
        normalized_path = PathSafety._normalize_url_path(parsed_url.path)
        normalized_url = parsed_url._replace(path=normalized_path).geturl()
        self.base_url = trim_url(normalized_url + "/")
        self.base_parsed = urlparse(self.base_url)

        # Validate URL scheme using fast method (now the default)
        if not self._validate_url_scheme(self.base_url):
            raise URLScopeError(
                f"Invalid URL scheme in base_url: {sanitize_url_for_log(self.base_url)}"
            )

        # ============================================================================
        # 7. DOWNLOAD COMPONENTS
        # ============================================================================
        self.download_queue = DownloadQueue(max_size=config.download_queue_size)
        # Dedicated executor for blocking metadata checks inside async tasks
        self._meta_check_executor = ThreadPoolExecutor(
            max_workers=min(50, self.config.workers * 2), thread_name_prefix="meta_check"
        )
        self.bandwidth_limiter = BandwidthLimiter(
            config.bandwidth_limit * 1024 * 1024 if config.bandwidth_limit else None
        )

        # ============================================================================
        # 8. SYMLINK TRACKER
        # ============================================================================
        self.symlink_tracker = None
        if config.handle_symlinks:
            self.symlink_tracker = SymlinkTracker(
                max_depth=config.max_symlink_depth,
                max_per_dir=config.max_symlinks_per_dir,
                bomb_threshold=config.symlink_bomb_threshold,
            )

        # ============================================================================
        # 9. CONCURRENCY MANAGER - Initialize before connection manager
        # ============================================================================
        self.concurrency_manager = UnifiedConcurrencyManager()
        self.concurrency_manager.start()

        # ============================================================================
        # 10. CONNECTION MANAGER
        # ============================================================================
        self.connection_manager = ConnectionManager(
            config=config, metrics=self.metrics, concurrency_manager=self.concurrency_manager
        )

        # ============================================================================
        # 11. TARGET URL SETUP - Compute but don't store or create yet
        # ============================================================================
        self.target_base_url: Optional[str] = None
        self.target_parsed: Optional[ParseResult] = None
        self._computed_target_base_url: Optional[str] = None

        # Compute target base URL without storing it yet
        try:
            self._computed_target_base_url = trim_url(self._get_target_base_url())
        except Exception as e:
            logging.warning(f"Failed to compute target base URL: {e}")
            self._computed_target_base_url = None

        # ============================================================================
        # 12. TARGET DIRECTORY PATH - Compute but don't create yet
        # ============================================================================
        self._computed_target_path: Optional[Path] = None
        suffix = config.dir_suffix
        try:
            if suffix:
                suffix_parts = [p for p in suffix.split("/") if p]
                target_dir_path = self.dest_path
                for part in suffix_parts:
                    safe_part = PathSafety._safe_filename(part, max_len=config.max_filename_len)
                    target_dir_path = target_dir_path / safe_part
            else:
                target_dir_path = self.dest_path

            self._computed_target_path = target_dir_path.resolve()
        except Exception as e:
            logging.warning(f"Failed to compute target directory path: {e}")
            self._computed_target_path = None

        # ============================================================================
        # 13. CACHE FILE - Safe initialization
        # ============================================================================
        url_hash = hashlib.sha256(str(config.base_url).encode()).hexdigest()[:16]
        try:
            if suffix:
                safe_suffix = suffix.replace("/", "_")
                cache_name = f"mirror_url_{safe_suffix}_{url_hash}.json"
            else:
                folder_name = self._get_last_path_component(self.base_url)
                cache_name = f"mirror_url_{folder_name}_{url_hash}.json"

            self.cache_file = self.log_path / cache_name
        except Exception as e:
            logging.warning(f"Failed to create cache file path: {e}")
            self.cache_file = None

        # Create log filename BEFORE using it
        if self.config.dir_suffix:
            safe_suffix = self.config.dir_suffix.replace("/", "_")
            log_filename = f"mirror_url_{safe_suffix}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        else:
            folder = self._get_last_path_component(str(self.config.base_url))
            log_filename = f"mirror_url_{folder}_{time.strftime('%Y%m%d_%H%M%S')}.log"

        # Create log directory only if needed and not in dry-run
        if not config.dry_run:
            try:
                # Ensure log directory exists BEFORE using FileHandler
                log_filepath = self.log_path / log_filename
                log_filepath.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                # Fallback to system temp if we can't create the log path
                temp_log_dir = Path(tempfile.gettempdir()) / f"mirrorurl_logs_{os.getpid()}"
                temp_log_dir.mkdir(parents=True, exist_ok=True)
                log_filepath = temp_log_dir / log_filename
                logging.warning(
                    f"Failed to create log directory {self.log_path}, using fallback: {temp_log_dir}"
                )
                self.log_path = temp_log_dir

        # ============================================================================
        # 14. SETUP LOGGING (if not using shared log)
        # ============================================================================
        if not config.use_shared_log:
            # Set log level based on config
            if config.debug or config.verbose:
                log_level = logging.DEBUG
            elif config.quiet:
                log_level = logging.WARNING
            else:
                log_level = logging.INFO

            # Configure root logger level
            logging.root.setLevel(log_level)

            self.setup_logging()

        # ============================================================================
        # 15. CACHE MANAGER
        # ============================================================================
        if self.cache_file:
            self.cache_manager = CacheManager(self.cache_file, config, self.metrics)
        else:
            # Create a dummy cache manager if cache file is None
            class DummyCacheManager:
                def __init__(self):
                    self.lru_file_cache = LRUCache(maxsize=100, ttl_seconds=3600, name="dummy")

                def get_html_cache(self, url):
                    return None

                def set_html_cache(self, url, files, subdirs, content_hash=None):
                    pass

                def get_file_metadata(self, local_path):
                    return None

                def save_file_metadata(self, local_path, etag, mtime, size=0):
                    pass

                def cleanup_file_metadata(self, local_path):
                    pass

                def handle_memory_pressure(self, pressure):
                    return 0

            self.cache_manager = DummyCacheManager()

        # ============================================================================
        # 15a. FILENAME CACHE (Performance optimization)
        # ============================================================================
        self._filename_cache: Dict[str, str] = {}
        self._filename_cache_lock = RLock()
        self._filename_cache_maxsize = 10000
        self._filename_cache_hits = 0
        self._filename_cache_misses = 0

        # ============================================================================
        # 16. FILESYSTEM CACHE
        # ============================================================================
        self.fs_cache = FileSystemCache(ttl_seconds=config.fs_cache_ttl)

        # ============================================================================
        # 17. BATCH PROCESSOR
        # ============================================================================
        self.batch_processor = AdaptiveBatchProcessor(
            initial_batch=config.initial_batch_size,
            max_batch=config.max_batch_size,
            target_time=config.target_batch_time,
        )

        # ============================================================================
        # 18. DISK-BACKED SET (optional)
        # ============================================================================
        self.remote_files_set = None
        if config.use_disk_backed_sets and config.disk_cache_dir:
            try:
                self.remote_files_set = DiskBackedSet(
                    config.disk_cache_dir, config.memory_cache_size
                )
            except Exception as e:
                logging.warning(f"Failed to initialize disk-backed set: {e}")

        # ============================================================================
        # 19. V2.0.0 COMPONENTS - Initialize with safe defaults
        # ============================================================================
        self.memory_monitor = MemoryMonitor()
        self.disk_manager: Optional[DiskSpaceManager] = None
        self.performance_monitor = PerformanceMonitor()
        self.partial_manager: Optional[PartialDownloadManager] = None
        self.health_checker = HealthChecker(self)
        self.multi_progress = MultiLevelProgress()
        self.per_ip_limiter = PerIPRateLimiter(requests_per_second=DEFAULT_RATE_LIMIT)
        self.health_server = None

        # ============================================================================
        # 20. ASYNC MANAGERS
        # ============================================================================
        self.async_connection_manager: Optional[AsyncConnectionManager] = None
        self.adaptive_async_manager: Optional[AdaptiveAsyncManager] = None
        self.async_task_manager: Optional[AsyncTaskManager] = None

        # Initialize Async managers based on config
        if self.config.async_metadata:
            try:
                self.async_task_manager = AsyncTaskManager()
                logging.debug(f"{self._get_prefix()}AsyncTaskManager initialized")

                if self.config.adaptive_async:
                    self.adaptive_async_manager = AdaptiveAsyncManager(self.config, self.metrics)
                    logging.debug(f"{self._get_prefix()}AdaptiveAsyncManager initialized")
                else:
                    self.async_connection_manager = AsyncConnectionManager(
                        self.config, self.metrics
                    )
                    logging.debug(f"{self._get_prefix()}AsyncConnectionManager initialized")
            except Exception as e:
                logging.warning(f"{self._get_prefix()}Failed to initialize async managers: {e}")
                self.async_connection_manager = None
                self.adaptive_async_manager = None
                self.async_task_manager = None

        # ============================================================================
        # 21. V3.0.0 COMPONENTS - Initialize with safe defaults
        # ============================================================================
        self.parallel_manager = None
        try:
            self.parallel_manager = ParallelDownloadManager(
                config=config,
                metrics=self.metrics,
                connection_manager=self.connection_manager,
                bandwidth_limiter=self.bandwidth_limiter,
                concurrency_manager=self.concurrency_manager,
                mirror=self,
            )
        except Exception as e:
            logging.warning(f"Failed to initialize parallel download manager: {e}")
            self.parallel_manager = None

        # ============================================================================
        # 22. V3.0.6 AUTO-CONCURRENCY TUNER
        # ============================================================================
        self.auto_tuner = None
        if config.auto_concurrency and config.parallel_downloads and self.parallel_manager:
            try:
                self.auto_tuner = AutoConcurrencyTuner(
                    start_concurrency=config.max_concurrent_downloads // 2,
                    max_concurrency=config.max_concurrent_downloads,
                )
                logging.info(
                    f"{self._get_prefix()}🤖 Auto-concurrency tuning enabled (starting at {self.auto_tuner.get_concurrency()})"
                )
            except Exception as e:
                logging.warning(f"Failed to initialize auto-concurrency tuner: {e}")
                self.auto_tuner = None

        # ============================================================================
        # 23. SCANNER
        # ============================================================================
        self.scanner = DirectoryScanner(self)
        if hasattr(self, "adaptive_async_manager") and self.adaptive_async_manager:
            self.scanner.adaptive_manager = self.adaptive_async_manager

        # ============================================================================
        # 24. LOG INITIAL CONFIGURATION (partial)
        # ============================================================================
        self._log_cleanup_policy()
        prefix = self._get_prefix()

        # Log cache settings
        if config.no_cache:
            logging.info(f"{prefix}Cache disabled by --no-cache")
        elif config.refresh_cache:
            logging.info(f"{prefix}Cache refresh forced by --refresh-cache")
        else:
            logging.info(f"{prefix}Cache max age: {config.cache_max_age} days")

        # Log scan mode
        logging.info(f"{prefix}Scan mode: {config.scan_mode.value}")

        # Log rate limiting
        delay_ms = config.request_delay * 1000
        logging.info(
            f"{prefix}Rate limiting: {delay_ms:.1f}ms delay{' (trusted server)' if config.trusted_server else ''}"
        )

        # Log async settings
        if config.cache_html:
            logging.info(f"{prefix}📦 HTML caching enabled ({config.html_cache_max_age}h)")

        if config.adaptive_async and config.async_metadata:
            logging.info(
                f"{prefix}🔄 Adaptive async: start={config.adaptive_start_concurrency}, "
                f"max={ADAPTIVE_MAX_CONCURRENCY}, error_threshold={config.adaptive_error_threshold:.1%}"
            )

        # Log bandwidth limit
        if config.bandwidth_limit:
            logging.info(f"{prefix}⏱️ Bandwidth limit: {config.bandwidth_limit} MB/s")

        # Log resume capability
        if config.enable_resume:
            logging.info(f"{prefix}↩️ Resume capability enabled")

        # Log async scanning
        if config.async_metadata:
            logging.info(f"{prefix}⚡ Async directory scanning: ENABLED")
        else:
            logging.info(f"{prefix}⚡ Async directory scanning: DISABLED (sync mode)")

        # Log symlink handling
        if config.handle_symlinks:
            logging.info(f"{prefix}🔗 Symlink handling: ENABLED (mode: {config.symlink_mode})")

        # Log monitoring
        if PSUTIL_AVAILABLE:
            logging.info(f"{prefix}📊 Memory monitoring: ENABLED")
        if config.security_validation:
            logging.info(f"{prefix}🔒 Per-IP rate limiting: ENABLED")

        # Log parallel download settings
        if config.parallel_downloads and self.parallel_manager:
            logging.info(
                f"{prefix}🚀 Parallel chunk downloads: ENABLED (max {config.max_chunks_per_file} chunks, "
                f"min {config.min_chunk_size_mb}MB)"
            )

        # Log cache file
        logging.info(f"{prefix}Cache file: {self.cache_file}")

        # Log content hash setting
        if config.content_hash_small_files:
            logging.info(f"{prefix}🔐 Content hash: files <{CONTENT_HASH_THRESHOLD / 1024:.0f}KB")

        # Log parser availability
        if LXML_AVAILABLE:
            logging.info(f"{prefix}Parser: lxml.html + fast fallback")
        else:
            logging.info(f"{prefix}Parser: fast regex only (lxml not available)")

        # Log HTTP/2 setting
        logging.info(f"{prefix}HTTP/2: {'ENABLED' if config.http2 else 'DISABLED'}")

        # Log ETag support
        logging.info(f"{prefix}ETag support: {'ENABLED' if not config.no_etag else 'DISABLED'}")

        # Log security settings
        if config.safe_urls:
            logging.info(f"{prefix}🔒 URL sanitization enabled")
        logging.info(
            f"{prefix}🛡️ Path safety: max_depth={config.max_depth}, max_filename_len={config.max_filename_len}"
        )

        # Log progress bar
        if config.progress_bar and TQDM_AVAILABLE:
            logging.info(f"{prefix}📈 Progress bar enabled")

        # Log adaptive batch processing
        if config.adaptive_batch_processing:
            logging.info(
                f"{prefix}📈 Adaptive batch processing: initial={config.initial_batch_size}"
            )

        # Log fast parsing fallback
        if config.fast_parsing_fallback:
            logging.info(f"{prefix}⚡ Fast parsing fallback enabled")

        # Log connection pool pre-warming
        if config.connection_pool_prewarm:
            logging.info(f"{prefix}🔥 Connection pool pre-warming enabled")

        # ============================================================================
        # 25. HEALTH CHECK SERVER
        # ============================================================================
        if config.metrics_json and not config.dry_run:
            try:
                self.health_server = HealthCheckServer(self, port=config.health_check_port)
                self.health_server.start()
                logging.info(
                    f"{prefix}🏥 Health check API available at http://localhost:{config.health_check_port}/health"
                )
            except Exception as e:
                logging.warning(f"{prefix}Failed to start health check server: {e}")

        # ============================================================================
        # 26. CONNECTION TEST - Critical: This determines if we can proceed
        # ============================================================================
        connection_result = self.test_connection()
        if connection_result is False:
            logging.warning(f"{prefix}Initial connection test failed. Skipping.")
            self.connection_ok = False
        elif connection_result == 404:
            logging.warning(f"{prefix}Directory not found (404). Skipping.")
            self.connection_ok = False
        else:
            self.connection_ok = True

        # ============================================================================
        # 27. SETUP TARGET PATHS AND MANAGERS (ONLY IF CONNECTION SUCCESSFUL)
        # ============================================================================
        if self.connection_ok and self._computed_target_base_url:
            # Set target URL attributes
            self.target_base_url = self._computed_target_base_url
            try:
                self.target_parsed = urlparse(self.target_base_url)
            except Exception as e:
                logging.warning(f"Failed to parse target base URL: {e}")
                self.target_parsed = None

            # Validate target URL scope (second check)
            if self.target_base_url and not self._is_url_within_scope(self.target_base_url):
                logging.error(
                    f"{prefix}Target URL outside base URL scope: {sanitize_url_for_log(self.target_base_url)}"
                )
                self.connection_ok = False
                self.target_base_url = None
                self.target_parsed = None

        # Set target directory based on connection status and dry-run mode
        if self.connection_ok and not config.dry_run and self._computed_target_path:
            # Normal mode: create directory
            self.target_dir = self._computed_target_path
            self._target_dir_path = self.target_dir.resolve()
            try:
                self.target_dir.mkdir(parents=True, exist_ok=True)
                logging.info(f"{prefix}Target directory: {self.target_dir}")
                logging.debug(f"Created target directory: {self.target_dir}")
            except Exception as e:
                logging.warning(f"Failed to create target directory {self.target_dir}: {e}")
                self.connection_ok = False

            # Initialize managers with actual target_dir
            try:
                self.disk_manager = DiskSpaceManager(self.target_dir)
                self.partial_manager = PartialDownloadManager(self.target_dir)
            except Exception as e:
                logging.warning(f"Failed to initialize disk/partial managers: {e}")
                self.disk_manager = None
                self.partial_manager = None

        elif self.connection_ok and config.dry_run and self._computed_target_path:
            # Dry-run mode: store path but DON'T create directory
            self.target_dir = self._computed_target_path
            self._target_dir_path = self.target_dir.resolve()
            logging.info(f"{prefix}Target directory (dry-run, not created): {self.target_dir}")
            # Initialize managers with None to prevent filesystem operations
            self.disk_manager = DiskSpaceManager(None)
            self.partial_manager = PartialDownloadManager(None)
        else:
            # Connection failed or no target path - set everything to None
            self.target_dir = None
            self._target_dir_path = None
            logging.info(
                f"{prefix}Target directory: Not created (connection failed or invalid path)"
            )
            # Initialize with None to prevent directory creation
            self.disk_manager = DiskSpaceManager(None)
            self.partial_manager = PartialDownloadManager(None)

        # ============================================================================
        # 28. CONNECTION POOL WARM-UP (only if connection is OK and managers exist)
        # ============================================================================
        if config.connection_pool_prewarm and not config.dry_run and self.connection_ok:
            self._warm_up_connections()

        # ============================================================================
        # 29. SIGNAL HANDLERS (always set, regardless of connection status)
        # ============================================================================
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # ============================================================================
        # 30. FINAL STATUS LOGGING
        # ============================================================================
        if self.connection_ok:
            logging.info(f"{prefix}✅ MirrorURL initialized successfully")
        else:
            logging.info(f"{prefix}⚠️ MirrorURL initialized with connection issues (will skip sync)")

        logging.debug(
            f"{prefix}Initialization complete: target_dir={self.target_dir}, "
            f"cache_file={self.cache_file}, connection_ok={self.connection_ok}"
        )

        # Initialization for _async_speed_samples:
        self._speed_samples: deque = deque(
            maxlen=20
        )  # Keep last 20 samples (already exists, ensure it's there)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup()
        return False

    def cleanup(self) -> None:
        """Enhanced cleanup with proper ordering and resource management."""
        logging.debug("Starting MirrorURL cleanup...")

        # 1. Stop health server FIRST (so no new requests come in)
        if hasattr(self, "health_server") and self.health_server:
            try:
                self.health_server.stop()
                logging.debug("Health server stopped")
            except Exception as e:
                logging.debug(f"Health server stop error: {e}")

        # 2. Shutdown async components (must be done before connection managers)
        # IMPORTANT: AsyncTaskManager must be shut down before AsyncConnectionManager
        # because tasks may be using the connection manager's client
        if hasattr(self, "async_task_manager") and self.async_task_manager:
            try:
                # Check if we're in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # In async context, schedule shutdown as task with timeout.
                    # Fire-and-forget: the result was never awaited or stored.
                    asyncio.ensure_future(self.async_task_manager.shutdown(timeout=10.0))
                    # We can't await here in a sync cleanup, so schedule it
                    logging.debug("Scheduled AsyncTaskManager shutdown in running loop")
                except RuntimeError:
                    # No running loop, create one for cleanup
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        # Try graceful shutdown first
                        loop.run_until_complete(
                            asyncio.wait_for(
                                self.async_task_manager.shutdown(timeout=10.0), timeout=15.0
                            )
                        )
                    except asyncio.TimeoutError:
                        logging.warning("AsyncTaskManager graceful shutdown timed out, forcing...")
                        try:
                            loop.run_until_complete(
                                self.async_task_manager.force_shutdown(timeout=5.0)
                            )
                        except Exception:
                            pass
                    except Exception as e:
                        logging.error(f"AsyncTaskManager shutdown error: {e}")
                        try:
                            loop.run_until_complete(
                                self.async_task_manager.force_shutdown(timeout=5.0)
                            )
                        except Exception:
                            pass
                    finally:
                        loop.close()
                logging.debug("AsyncTaskManager shutdown complete")
            except Exception as e:
                logging.debug(f"Async task manager shutdown error: {e}")

        # 3. Close async connection managers (after tasks are done)
        if hasattr(self, "async_connection_manager") and self.async_connection_manager:
            try:
                if (
                    hasattr(self.async_connection_manager, "_client")
                    and self.async_connection_manager._client
                ):
                    try:
                        loop = asyncio.get_running_loop()
                        asyncio.create_task(
                            self.async_connection_manager.__aexit__(None, None, None)
                        )
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(
                                asyncio.wait_for(
                                    self.async_connection_manager.__aexit__(None, None, None),
                                    timeout=10.0,
                                )
                            )
                        except asyncio.TimeoutError:
                            logging.warning("AsyncConnectionManager cleanup timed out")
                        except Exception as e:
                            logging.debug(f"Async connection manager cleanup error: {e}")
                        finally:
                            loop.close()
                logging.debug("AsyncConnectionManager cleanup complete")
            except Exception as e:
                logging.debug(f"Async connection manager cleanup error: {e}")

        if hasattr(self, "adaptive_async_manager") and self.adaptive_async_manager:
            try:
                if (
                    hasattr(self.adaptive_async_manager, "_client")
                    and self.adaptive_async_manager._client
                ):
                    try:
                        loop = asyncio.get_running_loop()
                        asyncio.create_task(self.adaptive_async_manager.__aexit__(None, None, None))
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(
                                asyncio.wait_for(
                                    self.adaptive_async_manager.__aexit__(None, None, None),
                                    timeout=10.0,
                                )
                            )
                        except asyncio.TimeoutError:
                            logging.warning("AdaptiveAsyncManager cleanup timed out")
                        except Exception as e:
                            logging.debug(f"Adaptive async manager cleanup error: {e}")
                        finally:
                            loop.close()
                logging.debug("AdaptiveAsyncManager cleanup complete")
            except Exception as e:
                logging.debug(f"Adaptive async manager cleanup error: {e}")

        # 4. Shutdown parallel download manager
        if hasattr(self, "parallel_manager") and self.parallel_manager:
            try:
                self.parallel_manager.shutdown()
                logging.debug("Parallel manager shutdown complete")
            except Exception as e:
                logging.debug(f"Parallel manager shutdown error: {e}")

        # 5. Close connection manager
        if hasattr(self, "connection_manager") and self.connection_manager:
            try:
                self.connection_manager.close()
                logging.debug("Connection manager closed")
            except Exception as e:
                logging.debug(f"Connection manager close error: {e}")

        # 6. Shutdown meta-check executor
        if hasattr(self, "_meta_check_executor") and self._meta_check_executor:
            try:
                try:
                    self._meta_check_executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    self._meta_check_executor.shutdown(wait=False)
                logging.debug("Meta-check executor shutdown complete")
            except Exception as e:
                logging.debug(f"Meta-check executor shutdown error: {e}")

        # 7. Clear remote files set
        if hasattr(self, "remote_files_set") and self.remote_files_set:
            try:
                self.remote_files_set.clear()
                logging.debug("Remote files set cleared")
            except Exception as e:
                logging.debug(f"Remote files set clear error: {e}")

        # 8. Clean up partial downloads
        if hasattr(self, "partial_manager") and self.partial_manager:
            if self.partial_manager.download_dir is not None and not self.is_dry_run:
                try:
                    cleaned = self.partial_manager.cleanup_stale_partials()
                    if cleaned > 0:
                        self.metrics.increment("stale_partials_cleaned", cleaned)
                        logging.info(f"Cleaned {cleaned} stale partial downloads")
                except Exception as e:
                    logging.debug(f"Partial manager cleanup error: {e}")

        # 9. Clear filename cache
        if hasattr(self, "_filename_cache"):
            with self._filename_cache_lock:
                self._filename_cache.clear()
                logging.debug("Filename cache cleared")

        # 10. Shutdown concurrency manager
        if hasattr(self, "concurrency_manager"):
            try:
                self.concurrency_manager.shutdown()
                logging.debug("Concurrency manager shutdown complete")
            except Exception as e:
                logging.debug(f"Concurrency manager shutdown error: {e}")

        # 11. Close log handlers
        if hasattr(self, "log_handlers"):
            for handler in self.log_handlers:
                try:
                    handler.flush()
                    handler.close()
                except Exception as e:
                    logging.debug(f"Log handler close error: {e}")

        # 12. Save final metrics if configured
        if hasattr(self, "config") and self.config.metrics_json and not self.config.dry_run:
            try:
                if hasattr(self, "metrics"):
                    self.metrics.export_json(self.config.metrics_json, self.config)
                    logging.info(f"Final metrics exported to {self.config.metrics_json}")
            except Exception as e:
                logging.debug(f"Failed to export final metrics: {e}")

        logging.debug("MirrorURL cleanup complete")

    def setup_logging(self) -> None:
        """Setup logging configuration for this mirror instance."""
        if self._logging_configured:
            return
        self._logging_configured = True

        # If using shared logging, don't add file handlers - just log the suffix
        if self.config.use_shared_log:
            suffix_display = self.config.dir_suffix or "ROOT"
            if self.total_suffixes > 1:
                logging.info(
                    f"[{self.suffix_index}/{self.total_suffixes}] Processing directory suffix: '{suffix_display}'"
                )
            else:
                logging.info(f"Processing directory suffix: '{suffix_display}'")
            return

        # Create log filename (MOVED UP before directory creation)
        if self.config.dir_suffix:
            safe_suffix = self.config.dir_suffix.replace("/", "_")
            log_filename = f"mirror_url_{safe_suffix}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        else:
            folder = self._get_last_path_component(str(self.config.base_url))
            log_filename = f"mirror_url_{folder}_{time.strftime('%Y%m%d_%H%M%S')}.log"

        log_filepath = self.log_path / log_filename

        # FIX: Ensure log directory exists BEFORE creating FileHandler
        if not self.config.dry_run:
            try:
                log_filepath.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                # Fallback to system temp if we can't create the log path
                temp_log_dir = Path(tempfile.gettempdir()) / f"mirrorurl_logs_{os.getpid()}"
                temp_log_dir.mkdir(parents=True, exist_ok=True)
                log_filepath = temp_log_dir / log_filename
                # Store the fallback path for later use
                self.log_path = temp_log_dir
                logging.warning(f"Failed to create log directory, using fallback: {temp_log_dir}")

        # Preserve existing console handlers if they exist
        existing_console = []
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stderr:
                existing_console.append(handler)

        # Clear all other handlers
        for handler in logging.root.handlers[:]:
            if handler not in existing_console:
                logging.root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass

        # FIX: Ensure log directory exists BEFORE creating FileHandler
        if not self.config.dry_run:
            try:
                log_filepath.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                # Fallback to system temp if we can't create the log path
                logging.warning(f"Failed to create log directory {log_filepath.parent}: {e}")

        # Ensure log directory exists (non-dry-run only)
        if not self.config.dry_run:
            try:
                log_filepath.parent.mkdir(parents=True, exist_ok=True)
            except (FileNotFoundError, PermissionError, OSError) as dir_err:
                logging.debug(f"Log directory creation failed: {dir_err}")

        # File handler (always add)
        try:
            file_handler = logging.FileHandler(str(log_filepath), mode="a", encoding="utf-8")
            file_handler.setLevel(logging.DEBUG if self.config.debug else logging.INFO)
            file_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                )
            )
            logging.root.addHandler(file_handler)
            self.log_handler = file_handler
            self.log_handlers = [file_handler]
            _log_files.append(file_handler)
        except (FileNotFoundError, PermissionError, OSError) as e:
            # Fallback: ensure console handler exists before logging warning
            if not logging.root.handlers:
                console = logging.StreamHandler(sys.stderr)
                console.setFormatter(
                    logging.Formatter(
                        "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                    )
                )
                console.setLevel(logging.WARNING)
                logging.root.addHandler(console)
                self.log_handlers = [console]

            # Use print as last resort since logging setup may be incomplete
            print(
                f"WARNING: Could not create log file {log_filepath}: {e}. Logging to console only.",
                file=sys.stderr,
            )
            self.log_handlers = [
                h for h in logging.root.handlers if isinstance(h, logging.StreamHandler)
            ]

        # Add console handler ONLY if requested AND no console handler exists
        if self.config.print_logs and not existing_console:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                )
            )
            if self.config.debug or self.config.verbose:
                console_handler.setLevel(logging.DEBUG)
            elif self.config.quiet:
                console_handler.setLevel(logging.WARNING)
            else:
                console_handler.setLevel(logging.INFO)
            logging.root.addHandler(console_handler)
            self.log_handlers.append(console_handler)
            _log_files.append(console_handler)

        # Set root log level
        if self.config.debug or self.config.verbose:
            logging.root.setLevel(logging.DEBUG)
        elif self.config.quiet:
            logging.root.setLevel(logging.WARNING)
        else:
            logging.root.setLevel(logging.INFO)

        # Log initial information
        prefix = self._get_prefix()
        suffix_display = self.config.dir_suffix or "ROOT"

        cmd_str = shlex.join([sys.executable] + sys.argv)
        logging.info(f"{prefix}Processing directory suffix: '{suffix_display}'")
        logging.info(f"{prefix}Command: {cmd_str}")
        logging.info(f"{prefix}" + "-" * 80)
        logging.info(f"{prefix}Starting MirrorURL v{__version__} for '{suffix_display}'")
        logging.info(f"{prefix}Job started at: {self.job_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{prefix}Mirroring from: {sanitize_url_for_log(str(self.config.base_url))}")
        logging.info(f"{prefix}Destination path: {self.dest_path}")
        logging.info(f"{prefix}Log file: {log_filepath}")
        logging.info(
            f"{prefix}Workers: {self.config.workers}, Max Retries: {self.config.max_retries}"
        )
        logging.info(f"{prefix}Cache max age: {self.config.cache_max_age} days")

        if LXML_AVAILABLE:
            logging.info(f"{prefix}Parser: lxml.html + fast fallback")
        else:
            logging.info(f"{prefix}Parser: fast regex only (lxml not available)")

        logging.info(f"{prefix}HTTP/2: {'ENABLED' if self.config.http2 else 'DISABLED'}")
        logging.info(f"{prefix}ETag support: ENABLED")
        logging.info(f"{prefix}🔒 URL sanitization enabled")
        logging.info(
            f"{prefix}🛡️ Path safety: max_depth={self.config.max_depth}, max_filename_len={self.config.max_filename_len}"
        )

        if self.config.progress_bar and TQDM_AVAILABLE:
            logging.info(f"{prefix}📈 Progress bar enabled")

        if self.config.adaptive_async and self.config.async_metadata:
            logging.info(
                f"{prefix}🔄 Adaptive async: {self.config.adaptive_start_concurrency}-{ADAPTIVE_MAX_CONCURRENCY} workers"
            )

        if self.config.content_hash_small_files:
            logging.info(f"{prefix}🔐 Content hash: files <{CONTENT_HASH_THRESHOLD / 1024:.0f}KB")

        delay_ms = self.config.request_delay * 1000
        logging.info(
            f"{prefix}Rate limiting: {delay_ms:.1f}ms delay{' (trusted server)' if self.config.trusted_server else ''}"
        )

        if self.config.cache_html:
            logging.info(f"{prefix}📦 HTML caching enabled ({self.config.html_cache_max_age}h)")

        if self.config.enable_resume:
            logging.info(f"{prefix}↩️ Resume capability enabled")

        if self.config.adaptive_batch_processing:
            logging.info(
                f"{prefix}📈 Adaptive batch processing: initial={self.config.initial_batch_size}"
            )

        if self.config.fast_parsing_fallback:
            logging.info(f"{prefix}⚡ Fast parsing fallback enabled")

        if self.config.connection_pool_prewarm:
            logging.info(f"{prefix}🔥 Connection pool pre-warming enabled")

        if PSUTIL_AVAILABLE:
            logging.info(f"{prefix}📊 Memory monitoring: ENABLED")
        if self.config.security_validation:
            logging.info(f"{prefix}🔒 Per-IP rate limiting: ENABLED")

        # NEW v3.0.0: Log parallel download settings
        if self.config.parallel_downloads:
            logging.info(
                f"{prefix}🚀 Parallel chunk downloads: ENABLED (max {self.config.max_chunks_per_file} chunks, "
                f"min {self.config.min_chunk_size_mb}MB)"
            )

        self._log_cleanup_policy()

        # Note: Target directory will be logged after connection test in __init__

        logging.info(f"{prefix}Cache file: {self.cache_file}")
        logging.info(f"{prefix}Scan mode: {self.config.scan_mode.value}")

        if self.config.async_metadata:
            logging.info(f"{prefix}⚡ Async directory scanning: ENABLED")

        if self.config.handle_symlinks:
            logging.info(f"{prefix}🔗 Symlink handling: ENABLED (mode: {self.config.symlink_mode})")

        if self.config.metrics_json:
            logging.info(
                f"{prefix}🏥 Health check API: http://localhost:{self.config.health_check_port}/health"
            )

    def test_connection(self) -> Union[bool, int]:
        """Test connection to target URL."""
        prefix = self._get_prefix()

        # Use computed target URL if target_base_url not set yet
        test_url = self.target_base_url or self._computed_target_base_url
        logging.info(f"{prefix}Testing connection to {sanitize_url_for_log(test_url)}")

        try:
            if not test_url:
                logging.error(f"{prefix}No target URL available for connection test")
                return False

            if not self._is_url_within_scope(test_url):
                logging.error(f"{prefix}Target URL outside base URL scope")
                return False

            parsed = urlparse(test_url)
            ip = socket.gethostbyname(parsed.hostname)
            self.per_ip_limiter.wait(ip)

            response = self.connection_manager.request(
                test_url, method="HEAD", allow_redirects=True
            )

            if response.status_code == 404:
                logging.warning(f"{prefix}Target directory not found (404)")
                return 404

            response.raise_for_status()
            logging.info(f"{prefix}Connection successful. Status Code: {response.status_code}")
            return True

        except httpx.RequestError as e:
            logging.error(f"{prefix}Connection test failed: {e}")
            self.metrics.add_error(str(e), "connection_test")
            return False
        except Exception as e:
            logging.error(f"{prefix}Connection test failed: {e}")
            self.metrics.add_error(str(e), "connection_test")
            return False

    def _warm_up_connections(self) -> None:
        """
        Pre-warm connection pools for faster initial downloads.

        This establishes connections to common domains before downloads start,
        eliminating connection setup overhead during critical download time.
        """
        try:
            # Collect sample URLs for warm-up
            sample_urls = []

            # Add target base URL if available
            if hasattr(self, "target_base_url") and self.target_base_url:
                sample_urls.append(self.target_base_url)

            # Add some directory URLs from cache if available
            if hasattr(self.scanner, "cached_signatures") and self.scanner.cached_signatures:
                dir_urls = list(self.scanner.cached_signatures.keys())[
                    :9
                ]  # Take up to 9 directories
                sample_urls.extend(dir_urls)

            # If we have connection manager with pool, warm it up
            if sample_urls and hasattr(self.connection_manager, "connection_pool"):
                logging.debug(f"Warming up connection pool with {len(sample_urls)} URLs")
                self.connection_manager.connection_pool.warm_up(
                    sample_urls[:10]
                )  # Limit to 10 URLs

        except Exception as e:
            # Non-critical - just log debug level
            logging.debug(f"Connection warm-up failed (non-critical): {e}")

    def _get_cached_filename(self, remote_url: str) -> str:
        """
        Get cached filename from URL with automatic cache management.

        Args:
            remote_url: Remote URL to extract filename from

        Returns:
            Extracted filename
        """
        with self._filename_cache_lock:
            if remote_url in self._filename_cache:
                self._filename_cache_hits += 1
                return self._filename_cache[remote_url]

            self._filename_cache_misses += 1
            parsed = urlparse(remote_url)
            # Handle URLs with query parameters
            path = parsed.path
            if not path or path == "/":
                # Generate a filename from the URL if path is empty
                filename = f"index_{hash(remote_url) & 0xFFFFFFFF:x}.html"
            else:
                filename = os.path.basename(unquote(path))
                if not filename:
                    filename = f"index_{hash(remote_url) & 0xFFFFFFFF:x}.html"

            # Store in cache
            self._filename_cache[remote_url] = filename

            # Prune if cache exceeds max size
            if len(self._filename_cache) > self._filename_cache_maxsize:
                # Remove oldest 20% of entries
                items_to_remove = len(self._filename_cache) // 5
                keys_to_remove = list(self._filename_cache.keys())[:items_to_remove]
                for key in keys_to_remove:
                    del self._filename_cache[key]
                logging.debug(
                    f"Pruned filename cache: removed {items_to_remove} entries, "
                    f"now {len(self._filename_cache)} entries"
                )

            return filename

    def _get_filename_cache_stats(self) -> Dict[str, Any]:
        """Get filename cache statistics."""
        with self._filename_cache_lock:
            return {
                "size": len(self._filename_cache),
                "maxsize": self._filename_cache_maxsize,
                "hits": self._filename_cache_hits,
                "misses": self._filename_cache_misses,
                "hit_rate": (
                    self._filename_cache_hits
                    / (self._filename_cache_hits + self._filename_cache_misses)
                    * 100
                )
                if (self._filename_cache_hits + self._filename_cache_misses) > 0
                else 0,
            }

    def _get_remote_timestamp(self, url: str) -> Optional[float]:
        """
        Get remote file timestamp from Last-Modified header.

        Args:
            url: Remote URL

        Returns:
            Timestamp as float or None
        """
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

    def _get_file_size(self, url: str) -> Optional[int]:
        """Get file size via HEAD request."""
        try:
            response = self.connection_manager.request(url, method="HEAD")
            content_length = response.headers.get("Content-Length")
            if content_length:
                return int(content_length)
        except Exception as e:
            logging.debug(f"Failed to get file size for {url}: {e}")
        return None

    def check_disk_space(self, required_bytes: int) -> bool:
        """Check if enough disk space is available."""
        self.metrics.increment("disk_space_checks")
        ok, error = self.disk_manager.check_available(required_bytes)

        if not ok:
            self.metrics.increment("disk_space_warnings")
            if error:
                logging.error(f"Disk space error: {error}")

        return ok
