"""Static shared contract for the composed MirrorURL implementation.

Imported only under TYPE_CHECKING. Runtime mixin bases remain object, so this
contract adds no methods, state, imports, or inheritance to live instances.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - declarations have no runtime implementation
    import logging
    from collections import deque
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime
    from pathlib import Path
    from typing import Any, Dict, Generator, List, Optional, Protocol, Set, Tuple, Union
    from urllib.parse import ParseResult

    import httpx

    from ..async_connection import AdaptiveAsyncManager, AsyncConnectionManager, AsyncTaskManager
    from ..cache import CacheManager, NullCacheManager
    from ..compat import Str
    from ..concurrency import UnifiedConcurrencyManager
    from ..config import MirrorConfig
    from ..connection import ConnectionManager
    from ..destination_lock import DestinationLock
    from ..download import ParallelDownloadManager, PartialDownloadManager
    from ..health import HealthChecker, HealthCheckServer
    from ..metrics import MetricsCollector
    from ..monitoring import DiskSpaceManager, MemoryMonitor, PerformanceMonitor
    from ..primitives import AtomicCounter, AtomicSize
    from ..progress import MultiLevelProgress, ProgressTracker
    from ..queue import DownloadQueue
    from ..rate_limiter import BandwidthLimiter, PerIPRateLimiter
    from ..scanner import DirectoryScanner
    from ..scratch import OwnedScratch
    from ..security import SymlinkTracker
    from ..storage import DiskBackedSet, FileSystemCache
    from ..tuner import AutoConcurrencyTuner

    class MirrorHost(Protocol):
        """State and cross-mixin methods provided by MirrorURL's composition."""

        config: MirrorConfig
        _destination_lock: Optional[DestinationLock]
        _closed: bool
        suffix_index: int
        total_suffixes: int
        is_dry_run: bool
        files_processed: AtomicCounter
        files_skipped: AtomicCounter
        files_failed: AtomicCounter
        total_downloaded_size: AtomicSize
        start_time: float
        job_start_time: datetime
        connection_ok: bool
        scan_incomplete: bool
        _speed_samples: deque[float]
        metrics: MetricsCollector
        dest_path: Path
        log_path: Path
        target_dir: Optional[Path]
        _target_dir_path: Optional[Path]
        cache_file: Optional[Path]
        cache_manager: Union[CacheManager, NullCacheManager]
        log_handlers: List[logging.Handler]
        _logging_configured: bool
        base_url: str
        base_parsed: ParseResult
        download_queue: DownloadQueue
        _meta_check_executor: ThreadPoolExecutor
        bandwidth_limiter: BandwidthLimiter
        symlink_tracker: Optional[SymlinkTracker]
        concurrency_manager: UnifiedConcurrencyManager
        connection_manager: ConnectionManager
        target_base_url: Optional[str]
        target_parsed: Optional[ParseResult]
        _computed_target_base_url: Optional[str]
        _computed_target_path: Optional[Path]
        fs_cache: FileSystemCache
        remote_files_set: Optional[DiskBackedSet]
        memory_monitor: MemoryMonitor
        disk_manager: Optional[DiskSpaceManager]
        performance_monitor: PerformanceMonitor
        partial_manager: Optional[PartialDownloadManager]
        scratch_manager: Optional[OwnedScratch]
        health_checker: HealthChecker
        multi_progress: MultiLevelProgress
        per_ip_limiter: PerIPRateLimiter
        health_server: Optional[HealthCheckServer]
        async_connection_manager: Optional[AsyncConnectionManager]
        adaptive_async_manager: Optional[AdaptiveAsyncManager]
        async_task_manager: Optional[AsyncTaskManager]
        parallel_manager: Optional[ParallelDownloadManager]
        auto_tuner: Optional[AutoConcurrencyTuner]
        scanner: DirectoryScanner
        _previous_signal_handlers: Dict[int, Any]

        def _get_prefix(self) -> str: ...

        def _log_cleanup_policy(self) -> None: ...

        def _signal_handler(self, signum: int, frame: Any) -> None: ...

        def _initialize_auto_tuner(self) -> None: ...

        def install_signal_handlers(self) -> None: ...

        def cleanup(self) -> None: ...

        def setup_logging(self) -> None: ...

        def test_connection(self) -> Union[bool, int]: ...

        def _warm_up_connections(self) -> None: ...

        def _get_file_size(self, url: str) -> Optional[int]: ...

        def check_disk_space(self, required_bytes: int) -> bool: ...

        def _scan_local_tree(self) -> Tuple[List[Path], List[Path]]: ...

        def _cleanup_path_selected(self, path: Path, *, directory: bool = ...) -> bool: ...

        def clean_obsolete(self, remote_files: Set[str]) -> None: ...

        def download_file_with_resume(
            self, remote_url: str, local_path: Path, file_size: Optional[int] = ...
        ) -> bool: ...

        def _download_file_single(self, remote_url: str, local_path: Path) -> bool: ...

        def matches_filter(self, url: str) -> bool: ...

        def get_directory_signature(self, url: str, html_content: str = ...) -> str: ...

        def is_symlink(
            self, url: str, existing_response: Optional[httpx.Response] = ..., depth: int = ...
        ) -> Tuple[bool, Optional[str]]: ...

        def record_symlink(
            self, symlink_url: str, target_url: str, local_path: Path, depth: int = ...
        ) -> None: ...

        def get_remote_files(self) -> Optional[List[str]]: ...

        def _validate_remote_paths(self, remote_files: Any) -> Any: ...

        def list_directories(self) -> bool: ...

        def list_files(self) -> bool: ...

        def _dir_entry_signature(self, files: List[str], subdirs: List[str]) -> Optional[str]: ...

        def _check_directory_symlink(
            self, url: str, files: List[str], subdirs: List[str], dir_signatures: Dict[str, str]
        ) -> Tuple[bool, Optional[str]]: ...

        def _symlink_confidence_note(self, url: str, target_url: Optional[str]) -> str: ...

        def _discover_directories_bfs(self) -> Generator[str, None, None]: ...

        def _get_local_path_from_url(self, url: str) -> Optional[Path]: ...

        @staticmethod
        def _validate_url_scheme(url: str) -> bool: ...

        def _get_last_path_component(self, url: str) -> str: ...

        def _get_target_base_url(self) -> str: ...
        def _should_check_existing_file(self, remote_url: str) -> bool: ...

        def _is_url_within_scope(self, url: str, check_base: bool = ...) -> bool: ...

        def _is_within_target_scope(self, url: str) -> bool: ...

        def _is_dir_excluded(self, url: str) -> bool: ...

        def _parse_url_cached(self, url: str) -> ParseResult: ...

        def _get_filename_fast(self, url: str) -> Str: ...

        def _comparison_metadata(self, local_path: Path, use_cache: bool = ...) -> Any: ...

        def _freshness_headers(self, stored: Any) -> Any: ...

        def _response_is_current(self, response: Any, stat: Any, stored: Any) -> bool: ...

        def file_exists_and_up_to_date(
            self, local_path: Path, remote_url: str, use_cache: bool = ...
        ) -> bool: ...

        def _check_files_sync(
            self,
            remote_files: Union[List[str], List[Tuple[str, Path]]],
            progress: Optional[ProgressTracker] = ...,
        ) -> List[Tuple[str, Path]]: ...

        async def _check_files_async(
            self,
            remote_files: Union[List[str], List[Tuple[str, Path]]],
            progress: Optional[ProgressTracker] = ...,
            _depth: int = ...,
        ) -> List[Tuple[str, Path]]: ...

        def get_remote_timestamp(self, url: str) -> Optional[float]: ...

        def get_directory_size(self, path: Path) -> int: ...

        def sync(self) -> bool: ...

        def _print_early_exit_summary(self, prefix: str) -> None: ...

        def benchmark(self) -> Dict[str, Any]: ...
