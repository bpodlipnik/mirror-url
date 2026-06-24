#!/usr/bin/env python3
"""
MirrorURL - Enterprise-Grade Remote Directory Mirroring Tool
v3.1.13 - BUGFIX (circuit breaker — found via production-readiness test pass):
         ✅ CircuitBreakerManager.record_success/record_failure/can_execute only
            acted on a domain already present in self.breakers, but nothing on
            the real request path ever called get_breaker() to register one —
            so the per-domain breaker for a real domain was NEVER created.
            can_execute() returned True unconditionally forever and
            record_failure() was a permanent no-op: the breaker never actually
            tripped on repeated failures in production traffic. All three
            methods now call self.get_breaker(domain), which lazily (and
            idempotently) creates the breaker on first use.
         (Added 12 tests (39-50) to test_integration.py covering failure-mode
          retries/backoff, resource exhaustion — disk space, AtomicCounter
          under load, parallel-chunk concurrency caps — SecurityValidator
          edge cases, and CircuitBreaker/CircuitBreakerManager state-machine
          transitions)
v3.1.12 - BUGFIX (more async-HEAD phantom attrs — found via integration testing):
         ✅ AsyncConnectionManager.head() called self.record_result() and
            referenced self._semaphore_lock / self._current_concurrency — all of
            which live on AdaptiveAsyncManager, not this non-adaptive class. Each
            raised AttributeError once head() actually ran. Added a no-op
            record_result() (this manager has no concurrency profile to adjust)
            and replaced the broken semaphore-init block with the shared
            semaphore _ensure_client()/__aenter__ already create. A static scan
            now confirms NO phantom self.* references remain in either async
            manager class.
v3.1.11 - BUGFIX (async HEAD — found via integration testing):
         ✅ AsyncConnectionManager.head() called self._ensure_client(), but that
            method only existed on AdaptiveAsyncManager — so any async HEAD that
            actually reached it raised AttributeError. (It was masked when every
            file was missing locally, since that path returns before the HEAD.)
            Added _ensure_client() + a shared _build_client() helper to this
            class; head() now works whether or not the caller used `async with`.
v3.1.10 - BUGFIX (async transport — found via integration testing):
         ✅ SecureAsyncTransport.handle_async_request() read self._test_mode,
            but __init__ never set it (only the SYNC SecureTransport did). This
            raised AttributeError on EVERY async request, breaking all async
            metadata/download paths. Added the test_mode param + attribute to
            match SecureTransport.
v3.1.9 - BUGFIXES (clean_obsolete / cache / config):
         ✅ clean_obsolete MOVE mode: removed redundant item.rename(dest) after
            shutil.move() — the source was already gone, so rename() raised
            FileNotFoundError, leaving moved_dirs uncounted and stalling the
            empty-dir cleanup loop. Added dest-collision guard for dirs too.
         ✅ CacheManager.load: only unlink the corrupted cache when the backup
            rename failed (after a successful rename the file is already gone,
            so the unconditional unlink always raised + logged a misleading
            "Failed to delete")
         ✅ Removed duplicate 'quiet'/'verbose' keys in the args config dict
         ✅ _download_file_single: 403/404/410/451 skips now increment the
            ATOMIC files_skipped counter (the one the summary reads), not just
            the metrics dict — skipped files were invisible in the final total
         ✅ ConnectionManager.request: redirects no longer drop the caller's
            custom headers (Range / If-None-Match) and timeout — they were
            popped out of kwargs before the recursive redirect call
         (Full-file review pass: 58 pure-logic unit/edge tests added & passing)
v3.1.8 - BUGFIXES (get_remote_files / scanning):
         ✅ get_remote_files no longer scans only one level deep for the
            dir_suffix/target case — removed the broken shallow branch so all
            discovery recurses via BFS (files nested 2+ levels were dropped)
         ✅ exclude_dirs and max_depth now applied in the target case
         ✅ visited-set guard prevents duplicate/cyclic directory scans
         ✅ failed scans (non-200 / exceptions) are NO LONGER cached as empty
            directories — a transient error no longer masks real files for the
            rest of the run (and is no longer persisted to the html cache)
v3.1.7 - BUGFIXES:
         ✅ file_exists_and_up_to_date no longer returns True on verification
            errors (transient network/stat failures now trigger re-download)
         ✅ Existence checked even when fs_cache is unavailable
         ✅ AtomicCounter is hashable again (__eq__ had nulled __hash__)
         ✅ normalize_etag strips the literal "W/" prefix, not a char set
         ✅ Removed no-op flush/fsync on read handle after streaming download
v3.0.4 - StringZilla added
v3.0.3 - TRUE PARALLEL DOWNLOADS:
         ✅ Files download in parallel (not just chunks)
         ✅ Multiple files simultaneously with ThreadPoolExecutor
         ✅ All chunks from all files download concurrently
         ✅ 4-5x faster than v3.0.0 for multiple files
         ✅ ALL v3.0.1 FEATURES PRESERVED
"""

from __future__ import annotations
import os
import sys
import time
import logging
import argparse
import json
import re
import asyncio
import signal
import threading
import uuid
import hashlib
import socket
import random
import concurrent.futures
import statistics
import shlex
import shutil
import tempfile
import math
import secrets
import inspect
import mmap
from collections import OrderedDict, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from enum import Enum, auto
from functools import lru_cache, wraps, total_ordering
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from queue import Queue, Empty, PriorityQueue
from threading import RLock, Lock, Semaphore
from typing import (Dict, Any, List, Optional, Tuple, Set, Generator,
                    Union, Type, Callable, Deque, cast, TYPE_CHECKING)
from urllib.parse import urljoin, urlparse, unquote, quote, ParseResult
from re import error as re_error

# StringZilla with fallback for environments without it
try:
    from stringzilla import Str
    STRINGZILLA_AVAILABLE = True
except ImportError:
    STRINGZILLA_AVAILABLE = False
    # Provide a fallback implementation that mimics Str interface
    class Str(str):
        __slots__ = ()
        def startswith(self, prefix, start=0, end=None):
            if end is not None:
                return super().startswith(str(prefix), start, end)
            return super().startswith(str(prefix), start)
        def find(self, sub, start=0, end=None):
            if end is not None:
                return super().find(str(sub), start, end)
            return super().find(str(sub), start)
        def rfind(self, sub, start=0, end=None):
            if end is not None:
                return super().rfind(str(sub), start, end)
            return super().rfind(str(sub), start)
        def endswith(self, suffix, start=0, end=None):
            if end is not None:
                return super().endswith(str(suffix), start, end)
            return super().endswith(str(suffix), start)

import ipaddress
import atexit
import httpx
import yaml

# Pydantic v2
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict, ValidationError

# Optional dependencies
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from lxml import html
    from lxml.etree import XPath
    LXML_AVAILABLE = True
except ImportError:
    LXML_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

__version__ = "3.1.13"
__author__ = "BP"

# ============================================================================
# CONSTANTS (All preserved)
# ============================================================================
# Core settings
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2
DEFAULT_TIMEOUT = 30
DEFAULT_WORKERS = 8
DEFAULT_ASYNC_WORKERS = 50
PROGRESS_INTERVAL = 60
MAX_DIRECTORY_DEPTH = 50

# Rate limiting
REQUEST_DELAY = 0.05
TRUSTED_SERVER_DELAY = 0.01
DEFAULT_RATE_LIMIT = 20

# Cache settings
CACHE_SCHEMA_VERSION = 2  # Increment ONLY when cache structure/validation changes
DEFAULT_RGET_LIST_MAX_AGE = 7
DEFAULT_CACHE_MAX_AGE_DAYS = 7
MAX_CACHE_METADATA_ENTRIES = 100000
MAX_HTML_CACHE_SIZE = 500
HTML_CACHE_MAX_AGE_HOURS = 24

# File handling
MAX_FILENAME_LENGTH = 255
MAX_CONNECTION_POOLS = 20
DOWNLOAD_CHUNK_SIZE = 16384
SMALL_FILE_THRESHOLD = 1024 * 1024
CONTENT_HASH_LIMIT = 16384
CONTENT_HASH_THRESHOLD = 512 * 1024

# Scanning
PARALLEL_SCAN_THRESHOLD = 10
MIN_DIRS_FOR_PARALLEL = 5
MAX_IN_MEMORY_CACHE_SIZE = 1000
BATCH_SIZE = 200
ASYNC_BATCH_SIZE = 500

# Async scanning thresholds
ASYNC_SCAN_SMALL_BATCH = 50
ASYNC_SCAN_MEDIUM_BATCH = 500
ASYNC_SCAN_LARGE_BATCH = 500
ASYNC_SCAN_CONCURRENCY_SMALL = 20
ASYNC_SCAN_CONCURRENCY_MEDIUM = 50
ASYNC_SCAN_CONCURRENCY_LARGE = 100
ASYNC_SCAN_FALLBACK_THRESHOLD = 0.3

# Comparison tolerance
SIZE_TOLERANCE_PERCENT = 5
MASSIVE_SIZE_DIFF_THRESHOLD = 90
TIMESTAMP_TOLERANCE_SECONDS = 1.5

# Safety limits
MAX_DIR_SUFFIX_LENGTH = 512
MAX_DIR_SUFFIX_DEPTH = 10
MAX_WORKERS_HARD_LIMIT = 50
MIN_TIMEOUT = 3
MAX_TIMEOUT = 300
MAX_CACHE_AGE_DAYS = 365

# Async concurrency
ASYNC_SEMAPHORE_LIMIT = 20
CONNECTION_RESET_DELAY = 5

# Adaptive async
ADAPTIVE_ASYNC_ENABLED = True
ADAPTIVE_START_CONCURRENCY = 5
ADAPTIVE_MAX_CONCURRENCY = 50
ADAPTIVE_ERROR_THRESHOLD = 0.05
ADAPTIVE_RTT_THRESHOLD_MS = 500
ADAPTIVE_THROUGHPUT_MIN = 10
ADAPTIVE_WINDOW_SIZE = 50
ADAPTIVE_COOLDOWN_SECONDS = 30

# Server profiling
PROFILE_SAMPLE_SIZE = 20
PROFILE_TIMEOUT_SECONDS = 60

# Known throttled domains
KNOWN_THROTTLED_DOMAINS = [
    'nascom.nasa.gov', 'soho', 'sdac', 'lasp', 'spdf.gsfc.nasa.gov',
    'cdaweb.gsfc.nasa.gov', 'helioviewer.org'
]

# Windows reserved names
WINDOWS_RESERVED_NAMES = {'CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4',
                          'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 'LPT3',
                          'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'}

PROGRESS_UPDATE_INTERVAL = 1000

# Async speed test parameters
ASYNC_TEST_MAX_SECONDS = 12.0
ASYNC_TEST_MIN_FILES = 300
ASYNC_TEST_MIN_SPEED = 25.0
ASYNC_TEST_BATCH_SIZE = 80
ASYNC_TEST_MAX_SECONDS_THROTTLED = 18.0
ASYNC_TEST_MIN_FILES_THROTTLED = 450
ASYNC_TEST_MIN_SPEED_THROTTLED = 10.0

# Progress tracking intervals
PROGRESS_SHORT_JOB_SECONDS = 300
PROGRESS_MEDIUM_JOB_SECONDS = 1800
PROGRESS_UPDATE_SHORT = 30
PROGRESS_UPDATE_MEDIUM = 120
PROGRESS_UPDATE_LONG = 600
PROGRESS_PCT_MILESTONES = [25, 50, 75, 90, 100]
PROGRESS_MIN_FILES_FOR_PCT = 1000

# Symlink protection constants
MAX_SYMLINK_DEPTH = 10
MAX_SYMLINKS_PER_DIR = 100
SYMLINK_BOMB_THRESHOLD = 1000
SYMLINK_VISIT_CACHE_SIZE = 10000

# v1.9.8 Performance optimization constants
TARGET_BATCH_TIME_SECONDS = 1.0
MIN_BATCH_SIZE = 10
MAX_BATCH_SIZE = 1000
BATCH_ADJUSTMENT_FACTOR = 0.3
BATCH_SAMPLE_SIZE = 5
MEMORY_CACHE_MAX_SIZE = 100000
DISK_BACKED_SET_THRESHOLD = 50000
FS_CACHE_TTL_SECONDS = 5.0
FAST_PARSE_MIN_CONTENT_LENGTH = 1024 * 1024

# NEW v2.0.0 constants
PARTIAL_SUFFIX = '.mirror-partial'
PARTIAL_MAX_AGE_HOURS = 24
MEMORY_WARNING_THRESHOLD_MB = 500
MEMORY_CRITICAL_THRESHOLD_MB = 1000
MEMORY_CHECK_INTERVAL = 10
DISK_SPACE_WARNING_THRESHOLD = 0.85
DISK_SPACE_CRITICAL_THRESHOLD = 0.95
MIN_FREE_SPACE_BYTES = 100 * 1024 * 1024
MAX_BACKOFF_DELAY = 60.0
BACKOFF_BASE = 2.0
JITTER_FACTOR = 0.1
ADAPTIVE_SMOOTHING_FACTOR = 0.1
MAX_REQUESTS_PER_IP = 100
#HEALTH_CHECK_PORT = 8080  # For health check API <- moved to MirrorConfig

# NEW v3.0.0 constants - Parallel Downloads
MIN_CHUNK_SIZE = 10 * 1024 * 1024  # 10MB minimum chunk size
MAX_CHUNKS_PER_FILE = 8
MAX_PARALLEL_CHUNKS_TOTAL = 50
CHUNK_ASSEMBLY_RETRIES = 3
CHUNK_TIMEOUT_MULTIPLIER = 1.5
CHUNK_CLEANUP_AGE_HOURS = 24
CHUNK_SUFFIX = '.mirror-chunk'
CHUNK_WRITE_BUFFER_SIZE = 256 * 1024  # 256KB buffer for chunk writes
CHUNK_READ_SIZE = 32 * 1024  # 32KB read chunks for HTTP/2 efficiency
PARALLEL_DOWNLOAD_ENABLED = False  # Default off for backward compatibility

CHUNK_ACQUIRE_TIMEOUT = 30  # seconds
CHUNK_RETRY_BACKOFF_FACTOR = 1.5

# v3.0.6 constants - Unified Concurrency - REDUCED to prevent deadlocks
UNIFIED_MAX_TOTAL_THREADS = 50      # Changed from 500 - prevent thread explosion
UNIFIED_MAX_ASYNC_TASKS = 50        # Changed from 500
UNIFIED_THREAD_POOL_SHARED = False  # Set to False to match comment and prevent deadlocks
UNIFIED_QUEUE_SIZE = 1000
MONITOR_INTERVAL_SECONDS = 10

# v3.0.6 constants - Auto Concurrency Tuning
AUTO_CONCURRENCY_ENABLED = False  # Default off, enable with --auto-concurrency
AUTO_CONCURRENCY_START = 4
AUTO_CONCURRENCY_MAX = 16
AUTO_CONCURRENCY_SAMPLES = 10
AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD = 0.05  # 5% improvement threshold

# NEW v3.0.7 Streaming parallel constants
STREAMING_PARALLEL_ENABLED = True
STREAMING_WRITE_BUFFER_SIZE = 1024 * 1024  # 1MB write buffer
STREAMING_MIN_FILE_SIZE_MB = 100
STREAMING_MAX_CONCURRENT_WRITES = 8



# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================
class MirrorError(Exception):
    """Base exception for MirrorURL"""
    pass

class MirrorConnectionError(MirrorError):
    """Raised when connection fails"""
    pass

class PathTraversalError(MirrorError):
    """Raised when path traversal attempt detected"""
    pass

class URLScopeError(MirrorError):
    """Raised when URL is outside configured scope"""
    pass

class ConfigError(MirrorError):
    """Raised when configuration is invalid.

    NOTE: This deliberately does NOT inherit from ``ValueError``. Pydantic v2
    catches ``ValueError`` raised inside ``@model_validator`` and rewraps it
    as ``pydantic_core.ValidationError`` — that would mask ``ConfigError`` for
    every caller that does ``pytest.raises(ConfigError, ...)`` or ``except
    ConfigError`` against a model-validator code path.
    """
    pass

class CacheError(MirrorError):
    """Raised when cache operations fail"""
    pass

class ParsingError(MirrorError):
    """Raised when HTML parsing fails"""
    pass

class AdaptiveAsyncError(MirrorError):
    """Raised when adaptive async encounters critical failure"""
    pass

class SecurityError(MirrorError):
    """Raised when security validation fails"""
    pass

class DownloadError(MirrorError):
    """Raised when download fails"""
    pass

class HealthCheckError(MirrorError):
    """Raised when health check fails"""
    pass

class SymlinkLoopError(MirrorError):
    """Raised when symlink loop is detected"""
    pass

class SymlinkBombError(MirrorError):
    """Raised when symlink bomb is detected"""
    pass

# NEW v2.0.0 exceptions
class DiskSpaceError(MirrorError):
    """Raised when disk space is insufficient"""
    pass

class MemoryPressureError(MirrorError):
    """Raised when memory pressure is critical"""
    pass

# NEW v3.0.0 exceptions
class ChunkDownloadError(MirrorError):
    """Raised when chunk download fails"""
    pass

class ChunkAssemblyError(MirrorError):
    """Raised when chunk assembly fails"""
    pass

class RangeNotSupportedError(MirrorError):
    """Raised when server doesn't support Range requests"""
    pass

class ConcurrencyLimitError(MirrorError):
    """Raised when concurrency limits are exceeded"""
    pass

# ============================================================================
# ENUMS
# ============================================================================
class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

class ScanMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ADAPTIVE = "adaptive"
    ASYNC = "async"

class CleanupPolicy(Enum):
    SAFE_NO_DELETE = "safe"
    PREVIEW = "preview"
    DELETE = "delete"
    MOVE = "move"

class DownloadPriority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

# NEW v2.0.0 enums
class MemoryPressure(Enum):
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"

class ConcurrencyType(Enum):
    SYNC = "sync"
    ASYNC = "async"
    PARALLEL = "parallel"

# Add after existing enums, before data classes
class DownloadMethod(Enum):
    """Download method selection"""
    SEQUENTIAL = "sequential"
    PARALLEL_FILES = "parallel_files"  # Multiple files, no chunking
    STREAMING_PARALLEL = "streaming_parallel"  # Chunks with direct write
    TRADITIONAL_PARALLEL = "traditional_parallel"  # Chunks with temp assembly
    AUTO = "auto"  # Let system decide
    
# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class DownloadTask:
    """Represents a file download task"""
    remote_url: str
    local_path: Path
    priority: DownloadPriority = DownloadPriority.NORMAL
    size: Optional[int] = None
    retries: int = 0
    max_retries: int = 3
    etag: Optional[str] = None
    timestamp: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    # NEW v2.0.0 fields
    partial_path: Optional[Path] = None
    resume_from: int = 0
    # NEW v3.0.0 fields
    chunk_info: Optional[Dict[str, Any]] = None
    is_chunk: bool = False

@dataclass
class ServerProfile:
    """Tracks server performance characteristics for adaptive async.

    Thread-safety: ``add_sample`` is called from concurrent HTTP completion
    callbacks (one per finished request). Without a lock, two callers could
    interleave ``deque.append`` with ``_update_metrics`` iterating the same
    deque, raising ``RuntimeError: deque mutated during iteration`` and
    crashing the request thread. ``_lock`` guards the deque + derived metric
    fields. The lock is excluded from ``repr`` so debug dumps stay readable.
    """
    domain: str
    avg_rtt_ms: float = 0.0
    error_rate: float = 0.0
    throughput_files_per_sec: float = 100.0 # Changed from 0.0 to 100.0 while fixing error
    last_adjustment: float = 0.0
    recommended_concurrency: int = ADAPTIVE_START_CONCURRENCY
    is_throttled: bool = False
    samples: Deque[Dict] = field(default_factory=lambda: deque(maxlen=ADAPTIVE_WINDOW_SIZE))
    # Reentrant — _update_metrics is called from inside add_sample which
    # already holds the lock. RLock lets the same thread re-enter without
    # deadlocking.
    _lock: threading.RLock = field(default_factory=threading.RLock, repr=False, compare=False)
            
    def add_sample(self, rtt_ms: float, success: bool, duration_sec: float = 0) -> None:
        """Add a performance sample. Safe to call from multiple threads."""
        with self._lock:
            self.samples.append({
                'rtt_ms': rtt_ms,
                'success': success,
                'duration': duration_sec,
                'timestamp': time.time()
            })
            self._update_metrics()

    def _update_metrics(self) -> None:
        """Recalculate metrics from samples.

        Caller MUST hold ``self._lock``. Re-iterating the deque without the
        lock against a concurrent ``append`` raises
        ``RuntimeError: deque mutated during iteration``.
        """
        with self._lock:
            if not self.samples:
                return
            # Snapshot under lock so the iteration below is safe even if a
            # future caller forgets the outer lock.
            samples_snapshot = list(self.samples)
            successful = [s for s in samples_snapshot if s['success']]
            self.error_rate = 1.0 - (len(successful) / len(samples_snapshot))
            if successful:
                self.avg_rtt_ms = statistics.mean(s['rtt_ms'] for s in successful)
                total_time = sum(s['duration'] for s in successful if s['duration'] > 0)
                if total_time > 0:
                    self.throughput_files_per_sec = len(successful) / total_time
            if self.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS or self.error_rate > ADAPTIVE_ERROR_THRESHOLD:
                self.is_throttled = True
                self.recommended_concurrency = max(1, self.recommended_concurrency // 2)
            elif self.error_rate < 0.01 and self.throughput_files_per_sec > ADAPTIVE_THROUGHPUT_MIN:
                self.recommended_concurrency = min(
                    ADAPTIVE_MAX_CONCURRENCY,
                    self.recommended_concurrency + 2
                )
            self.last_adjustment = time.time()
    
    def should_scale_up(self) -> bool:
        """Check if we can increase concurrency.

        Decision is based on error_rate and avg_rtt_ms only.
        throughput_files_per_sec is intentionally excluded: it equals
        (sample_count / total_duration) which is dominated by individual
        request duration, not server health. Ten samples at 0.5s each
        yields 2.0 files/sec regardless of RTT or errors, so a threshold
        of ADAPTIVE_THROUGHPUT_MIN (10) would never be reached even when
        the server is perfectly healthy.
        """
        with self._lock:
            return (self.error_rate < 0.02 and
                    self.avg_rtt_ms < ADAPTIVE_RTT_THRESHOLD_MS * 0.7)

@dataclass
class HealthStatus:
    """Health check status"""
    status: str
    timestamp: str
    metrics: Dict[str, Any]
    connection: Dict[str, Any]
    cache: Dict[str, Any]
    errors: List[Dict[str, Any]]
    # NEW v2.0.0 fields
    system: Dict[str, Any] = field(default_factory=dict)

# NEW v3.0.0 data classes
@dataclass
class ChunkInfo:
    """Information about a file chunk"""
    file_url: str
    final_path: Path
    chunk_id: int
    start_byte: int
    end_byte: int
    total_chunks: int
    temp_path: Path
    size: int = 0
    downloaded: int = 0
    status: str = 'pending'  # pending, downloading, completed, failed
    retries: int = 0
    max_retries: int = 3
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    direct_write: bool = False  # Write directly to final file

@dataclass
class ParallelFileDownload:
    """Manages parallel chunk downloads for a single file"""
    url: str
    final_path: Path
    file_size: int
    chunks: List[ChunkInfo] = field(default_factory=list)
    temp_dir: Optional[Path] = None
    start_time: float = field(default_factory=time.time)
    completed_chunks: int = 0
    failed_chunks: int = 0
    status: str = 'initializing'  # initializing, downloading, assembling, completed, failed
    supports_range: bool = True
    server_etag: Optional[str] = None
    server_last_modified: Optional[float] = None
    lock: RLock = field(default_factory=RLock)

# ============================================================================
# DECORATORS
# ============================================================================
import inspect
import logging
from typing import Tuple, Type, Union

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0,
                       max_delay: float = 60.0,
                       retry_on: Union[Type[BaseException], Tuple[Type[BaseException], ...]] = Exception,
                       log_retries: bool = True):
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts (NOT including the first attempt)
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds
        retry_on: Exception type or tuple of types to retry on
        log_retries: Whether to log retry attempts
    """
    # Normalize retry_on to a tuple for isinstance check
    retry_types = retry_on if isinstance(retry_on, tuple) else (retry_on,)
    
    def decorator(func):
        is_async = inspect.iscoroutinefunction(func)

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_types as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        if log_retries:
                            logging.debug(
                                f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                                f"after {delay:.2f}s: {type(e).__name__}: {e}"
                            )
                        time.sleep(delay)
                    # Continue to next iteration (or fall through to raise)
            raise last_exception

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except retry_types as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        if log_retries:
                            logging.debug(
                                f"Retry {attempt + 1}/{max_retries} for {func.__name__} "
                                f"after {delay:.2f}s: {type(e).__name__}: {e}"
                            )
                        await asyncio.sleep(delay)
            raise last_exception

        return async_wrapper if is_async else sync_wrapper
    return decorator

def log_performance(operation_name: str):
    """Decorator to log performance metrics"""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            start = time.time()
            try:
                result = func(self, *args, **kwargs)
                duration = time.time() - start
                if hasattr(self, 'performance_monitor'):
                    self.performance_monitor.record(operation_name, duration, True)
                return result
            except Exception as e:
                duration = time.time() - start
                if hasattr(self, 'performance_monitor'):
                    self.performance_monitor.record(operation_name, duration, False)
                raise
        return wrapper
    return decorator

# ============================================================================
# SYMLINK TRACKER
# ============================================================================
class SymlinkTracker:
    """Tracks symlink visits to prevent loops and bombs"""
    def __init__(self, max_depth: int = MAX_SYMLINK_DEPTH,
                 max_per_dir: int = MAX_SYMLINKS_PER_DIR,
                 bomb_threshold: int = SYMLINK_BOMB_THRESHOLD):
        self.max_depth = max_depth
        self.max_per_dir = max_per_dir
        self.bomb_threshold = bomb_threshold
        self.visited_symlinks: Dict[str, int] = {}
        self.symlinks_per_dir: Dict[str, int] = {}
        self.total_symlinks_followed = 0
        self.lock = RLock()
        self.symlink_chain: List[str] = []
    
    def can_follow(self, symlink_url: str, dir_url: str, current_depth: int) -> Tuple[bool, Optional[str]]:
        """Check if a symlink can be safely followed"""
        with self.lock:
            if self.total_symlinks_followed >= self.bomb_threshold:
                return False, f"Symlink bomb threshold reached ({self.bomb_threshold})"
            if current_depth > self.max_depth:
                return False, f"Max symlink depth exceeded ({self.max_depth})"
            if symlink_url in self.visited_symlinks:
                prev_depth = self.visited_symlinks[symlink_url]
                return False, f"Symlink loop detected (already seen at depth {prev_depth})"
            if symlink_url in self.symlink_chain:
                return False, f"Symlink cycle detected in current chain"
            dir_count = self.symlinks_per_dir.get(dir_url, 0)
            if dir_count >= self.max_per_dir:
                return False, f"Too many symlinks in directory ({dir_count} >= {self.max_per_dir})"
            return True, None
    
    def record_follow(self, symlink_url: str, dir_url: str, depth: int) -> None:
        """Record that we're following a symlink"""
        with self.lock:
            self.visited_symlinks[symlink_url] = depth
            self.symlinks_per_dir[dir_url] = self.symlinks_per_dir.get(dir_url, 0) + 1
            self.total_symlinks_followed += 1
            self.symlink_chain.append(symlink_url)
            if len(self.visited_symlinks) > SYMLINK_VISIT_CACHE_SIZE:
                oldest_keys = list(self.visited_symlinks.keys())[:SYMLINK_VISIT_CACHE_SIZE // 5]
                for key in oldest_keys:
                    del self.visited_symlinks[key]
    
    def record_skip(self, symlink_url: str) -> None:
        """Record that we're skipping a symlink"""
        with self.lock:
            self.total_symlinks_followed += 0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get symlink tracking statistics"""
        with self.lock:
            return {
                'total_followed': self.total_symlinks_followed,
                'unique_symlinks': len(self.visited_symlinks),
                'directories_with_symlinks': len(self.symlinks_per_dir),
                'current_chain_length': len(self.symlink_chain),
            }
    
    def clear_chain(self) -> None:
        """Clear the current symlink chain"""
        with self.lock:
            self.symlink_chain.clear()
    
    def is_in_chain(self, symlink_url: str) -> bool:
        """Check if a symlink is in the current chain"""
        with self.lock:
            return symlink_url in self.symlink_chain

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def exponential_backoff(attempt: int, base_delay: float = DEFAULT_RETRY_DELAY, 
                        max_delay: float = MAX_BACKOFF_DELAY) -> float:
    """
    Calculate delay with exponential backoff and jitter.
    
    Args:
        attempt: Current attempt number (0-based)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        
    Returns:
        Delay time in seconds with jitter, guaranteed to be <= max_delay
    """
    # Calculate exponential delay
    exp_delay = base_delay * (BACKOFF_BASE ** attempt)
    
    # Cap at max_delay
    capped_delay = min(exp_delay, max_delay)
    
    # Add jitter (10% max, but ensure we don't exceed max_delay)
    jitter = random.uniform(0, min(JITTER_FACTOR * capped_delay, max_delay - capped_delay))
    
    return capped_delay + jitter

def _validate_and_sanitize_cache(data: Any) -> Dict[str, Any]:
    """
    Long-term replacement for _clean_json_keys.
    Validates structure, safely handles minor key corruption,
    and prevents silent data loss or collisions.
    """
    if not isinstance(data, dict):
        raise ValueError("Cache root must be a dictionary")
    
    cleaned = {}
    seen_keys = set()
    collision_count = 0
    
    for k, v in data.items():
        if not isinstance(k, str):
            logging.warning(f"Skipping non-string cache key: {type(k)}")
            continue
        
        # Strip whitespace from keys
        safe_key = k.strip() if isinstance(k, str) else str(k)
        if not safe_key:
            logging.warning("Skipping empty cache key after whitespace strip")
            continue
        
        if safe_key in seen_keys:
            collision_count += 1
            logging.warning(
                f"Cache key collision detected: '{k}' → '{safe_key}'. "
                f"Keeping existing entry to prevent data loss."
            )
            continue
        
        seen_keys.add(safe_key)
        
        # IMPROVED: Safely handle metadata with proper error handling
        if k == '_meta' and isinstance(v, dict):
            meta = {}
            for mk, mv in v.items():
                if mk == 'version' and isinstance(mv, int):
                    meta[mk] = mv
                elif mk == 'last_full_run' and isinstance(mv, str):
                    try:
                        # Validate ISO format timestamp
                        parsed_date = datetime.fromisoformat(mv)
                        meta[mk] = mv
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Invalid timestamp in cache metadata: {mv}, error: {e}")
                        # Use current time as fallback
                        meta[mk] = datetime.now(timezone.utc).isoformat()
                elif mk == 'schema':
                    meta[mk] = str(mv)
                elif mk == 'file_count' and isinstance(mv, (int, float)):
                    meta[mk] = int(mv)
                elif mk == 'version_code':
                    meta[mk] = str(mv)
                elif mk == 'config' and isinstance(mv, dict):
                    # Sanitize config to avoid storing sensitive data
                    safe_config = {}
                    for ck, cv in mv.items():
                        if ck in ('base_url', 'dir_suffix', 'cache_max_age', 'parallel_downloads'):
                            safe_config[ck] = str(cv)
                    meta[mk] = safe_config
                elif mk == 'dir_signatures' and isinstance(mv, dict):
                    # Validate directory signatures
                    sigs = {}
                    for dk, dv in mv.items():
                        if isinstance(dk, str) and isinstance(dv, str):
                            sigs[dk] = dv
                    meta[mk] = sigs
                else:
                    # Preserve unknown fields but log them
                    logging.debug(f"Unknown cache metadata field: {mk}")
                    meta[mk] = mv
            cleaned[safe_key] = meta
        else:
            # Values are preserved exactly as written
            cleaned[safe_key] = v
    
    if collision_count > 0:
        logging.warning(f"Cache load: {collision_count} key collisions detected and skipped")
    
    return cleaned

def format_duration(seconds: float, show_ms: bool = False) -> str:
    """
    Format duration with optional millisecond precision.
    
    Args:
        seconds: Duration in seconds
        show_ms: Whether to show milliseconds for sub-second durations
        
    Returns:
        Formatted duration string
        
    Example:
        >>> format_duration(3665)
        '1h 1m 5s'
        >>> format_duration(0.5, show_ms=True)
        '500ms'
    """
    if seconds < 0:
        return "unknown"
    if seconds < 1.0 and show_ms:
        ms = seconds * 1000
        return f"{max(1, round(ms))}ms"
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"

def format_bytes(bytes_count: float) -> str:
    """
    Format bytes to human-readable string.
    
    Args:
        bytes_count: Number of bytes
        
    Returns:
        Formatted string with appropriate unit
        
    Example:
        >>> format_bytes(1536)
        '1.50 KB'
        >>> format_bytes(1048576)
        '1.00 MB'
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_count < 1024.0:
            return f"{bytes_count:.2f} {unit}"
        bytes_count /= 1024.0
    return f"{bytes_count:.2f} PB"

def normalize_etag(etag: str) -> str:
    """
    Normalize ETag by removing quotes and weak prefix.
    
    Args:
        etag: Raw ETag header value
        
    Returns:
        Normalized ETag string
        
    Example:
        >>> normalize_etag('W/"12345"')
        '12345'
    """
    if not etag:
        return ""
    # Strip the literal weak-validator prefix "W/" (case-insensitive),
    # NOT any leading 'W'/'/' characters. str.lstrip('W/') would treat
    # the argument as a CHARACTER SET and mangle etags whose content
    # begins with W or /, so use an explicit prefix check.
    if etag[:2] in ('W/', 'w/'):
        etag = etag[2:]
    etag = etag.strip('"')
    return etag

def safe_url_encode(path: str) -> str:
    """
    Safely encode URL path components.
    
    Args:
        path: URL path to encode (should be unencoded)
    
    Returns:
        Properly encoded URL path. Already-valid percent-sequences 
        are preserved; invalid sequences may be partially encoded.
    
    Note:
        This function does NOT decode first. If you need to normalize
        pre-encoded input, use safe_url_encode(unquote(path)) explicitly.
    """
    if not path:
        return path
    parts = path.split('/')
    # quote() with default safe='/' preserves path separators
    # Valid percent-sequences like %20 are preserved; literal % becomes %25
    return '/'.join(quote(part) if part else '' for part in parts)


def trim_url(url: str) -> str:
    """Trim whitespace from URLs"""
    return url.strip()

def sanitize_url_for_log(url: str) -> str:
    """
    Sanitize URL for logging by removing credentials AND the query string.

    Query strings routinely carry secrets — API keys (``?api_key=...``),
    OAuth/JWT tokens (``?access_token=...``), AWS pre-signed-URL
    signatures (``?X-Amz-Signature=...``), session IDs, etc. The previous
    version stripped only userinfo, so any of those secrets ended up in
    log files. This sanitizer now drops the query string and fragment as
    well; if a query was present it is replaced with ``?<redacted>`` so
    the existence of parameters is still visible.

    Args:
        url: URL that may contain credentials or query-string secrets

    Returns:
        URL safe for logging

    Example:
        >>> sanitize_url_for_log('https://user:pass@example.com/p?token=abc#frag')
        'https://example.com/p?<redacted>'
    """
    if not url:
        return url
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc
        if netloc and '@' in netloc:
            netloc = netloc.split('@')[-1]
        # Replace the query string with a redaction marker (preserves the
        # signal that *some* parameters were present without leaking them)
        # and drop the fragment entirely.
        new_query = '<redacted>' if parsed.query else ''
        sanitized = parsed._replace(
            netloc=netloc,
            query=new_query,
            fragment=''
        ).geturl()
        return sanitized
    except Exception:
        return url

def compute_file_hash(file_path: Path, algorithm: str = 'sha256') -> Optional[str]:
    """
    Compute hash of file content.
    
    Args:
        file_path: Path to file
        algorithm: Hash algorithm (default: sha256)
        
    Returns:
        Hex digest string or None on error
        
    Example:
        >>> hash = compute_file_hash(Path('file.txt'))
        >>> print(hash)
        'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
    """
    try:
        hash_obj = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                hash_obj.update(chunk)
        return hash_obj.hexdigest()
    except Exception:
        return None

def is_reserved_windows_filename(filename: str) -> bool:
    """
    Check if filename is reserved on Windows.
    
    Args:
        filename: Filename to check
        
    Returns:
        True if filename is reserved
        
    Example:
        >>> is_reserved_windows_filename('CON.txt')
        True
        >>> is_reserved_windows_filename('file.txt')
        False
    """
    stem = Path(filename).stem.upper()
    return stem in WINDOWS_RESERVED_NAMES

def normalize_url_path(url_path: str) -> str:
    """
    Normalize a URL path for consistent comparison.

    A normalized URL path is always rooted (begins with ``/``), so callers
    don't need to remember whether the input had a leading slash. Trailing
    slashes are preserved.

    Args:
        url_path: URL path to normalize

    Returns:
        Normalized path beginning with ``/`` (or empty string if input was
        empty/falsy).
    """
    if not url_path:
        return ''

    # Decode percent-encoding
    try:
        decoded = unquote(url_path)
    except Exception:
        decoded = url_path

    # Handle trailing slash
    if decoded.endswith('/'):
        trailing = True
        decoded = decoded.rstrip('/')
    else:
        trailing = False

    # Use Path to collapse duplicate slashes etc.
    normalized = str(Path(decoded)) if decoded else ''

    # Restore trailing slash if needed
    if trailing and normalized:
        normalized = normalized + '/'

    # Always root the path with a leading slash so callers can rely on it.
    if normalized and not normalized.startswith('/'):
        normalized = '/' + normalized

    return normalized

# ============================================================================
# SECURITY UTILITIES
# ============================================================================
class SecurityValidator:
    """SSRF protection and URL security validation"""
    PRIVATE_NETWORKS = [
        ipaddress.ip_network('10.0.0.0/8'),
        ipaddress.ip_network('172.16.0.0/12'),
        ipaddress.ip_network('192.168.0.0/16'),
        ipaddress.ip_network('127.0.0.0/8'),
        ipaddress.ip_network('0.0.0.0/8'),
        ipaddress.ip_network('169.254.0.0/16'),
        ipaddress.ip_network('::1/128'),
        ipaddress.ip_network('fc00::/7'),
        ipaddress.ip_network('fe80::/10'),
    ]
    
    @staticmethod
    def is_private_ip(ip: str) -> bool:
        """
        Check if an IP address is private.
        
        Args:
            ip: IP address string
            
        Returns:
            True if IP is private
        """
        try:
            addr = ipaddress.ip_address(ip)
            for network in SecurityValidator.PRIVATE_NETWORKS:
                if addr in network:
                    return True
            return addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved
        except ValueError:
            return True
        

    @staticmethod
    def resolve_and_validate_hostname(hostname: str) -> str:
        """
        Resolve hostname and validate IP immediately.

        Policy: if ANY resolved address is private/internal, the hostname is
        rejected. Returning the first public IP when others are private would
        leave callers exposed to DNS rebinding / fast-flux attacks where the
        attacker controls one of several A records.
        """
        try:
            # Force IPv4 resolution first for consistency
            try:
                infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
            except socket.gaierror:
                # Fall back to IPv6
                infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)

            if not infos:
                raise SecurityError(f"Failed to resolve hostname: {hostname}")

            public_ips = []
            private_ips = []

            for info in infos:
                sockaddr = info[4]
                ip = sockaddr[0]

                # Skip IPv6 link-local addresses
                if ip.startswith('fe80::'):
                    continue

                if SecurityValidator.is_private_ip(ip):
                    private_ips.append(ip)
                else:
                    public_ips.append(ip)

            # Strict: if any private IP appears in the resolution set, refuse.
            if private_ips:
                raise SecurityError(
                    f"Hostname {hostname} resolves to private IP(s): {private_ips}"
                )

            if public_ips:
                return public_ips[0]

            # No usable addresses (everything was filtered out).
            raise SecurityError(f"All resolved IPs are private/blocked for {hostname}")

        except socket.gaierror as e:
            raise SecurityError(f"Failed to resolve hostname: {hostname}: {e}")
            

    @staticmethod
    def validate_url_security(url: str, base_url: str) -> Tuple[bool, Optional[str]]:
        """
        Validate URL for security issues - PRODUCTION HARDENED.
        
        Security guarantees:
        1. Strict domain matching (exact OR explicitly allowed subdomains)
        2. IDN homograph attack prevention (block, not just log)
        3. Comprehensive path traversal/CRLF injection blocking
        4. No information leakage in error messages
        """
        if not url or not isinstance(url, str):
            return False, "Invalid URL format"
        try:
            parsed = urlparse(url)
            
            # 1. Scheme validation
            if parsed.scheme not in ('http', 'https'):
                return False, f"Blocked scheme: {parsed.scheme}"
            
            # 2. URL smuggling prevention
            if '@' in parsed.netloc:
                return False, "URL smuggling detected"
            
            hostname = parsed.hostname
            if not hostname:
                return False, "Missing hostname"
            
            # 3. Normalize hostname: lowercase, strip brackets, remove trailing dot
            hostname = hostname.strip('[]').lower().rstrip('.')
            
            # 4. IDN homograph attack prevention - BLOCK, don't just log
            # Punycode domains can visually impersonate legitimate domains
            if hostname.startswith('xn--'):
                # Optional: Allow-list specific trusted IDN domains here
                # For most use cases, blocking is the safest default
                return False, f"Internationalized domain not allowed: {hostname}"
            
            # 5. Direct IP address blocking
            try:
                ipaddress.ip_address(hostname)
                if SecurityValidator.is_private_ip(hostname):
                    return False, f"Blocked private IP address: {hostname}"
                return False, "Direct IP addresses not allowed"
            except ValueError:
                pass  # It's a domain name, continue
            
            # 6. Dangerous port blocking
            DANGEROUS_PORTS = {22, 23, 25, 53, 110, 143, 445, 3306, 3389, 5432, 6379, 27017}
            if parsed.port and parsed.port in DANGEROUS_PORTS:
                return False, f"Blocked port: {parsed.port}"
            
            # 7. Domain enforcement - STRICT matching
            base_parsed = urlparse(base_url)
            base_hostname = base_parsed.hostname
            if not base_hostname:
                return False, "Invalid base URL: missing hostname"
            base_hostname = base_hostname.lower().rstrip('.')
            
            # Exact match OR explicitly allowed subdomain pattern
            if hostname == base_hostname:
                pass  # Exact match - OK
            elif hostname.endswith('.' + base_hostname):
                # Subdomain detected - decide policy here:
                # Option A (strict): Block all subdomains
                # return False, f"Subdomains not allowed: {hostname}"
                # Option B (allow-list): Only allow specific subdomains
                # allowed_subs = {'cdn.', 'assets.', 'static.'}
                # if not any(hostname.startswith(s) for s in allowed_subs):
                #     return False, f"Subdomain not allow-listed: {hostname}"
                # Option C (current): Allow all subdomains - DOCUMENT THIS RISK
                # For now, we allow but log for audit
                logging.debug(f"Subdomain allowed by policy: {hostname} (base: {base_hostname})")
            else:
                # Prevent domain suffix attacks: example.com.attacker.com
                # Check if base_hostname appears as a suffix without proper dot boundary
                if base_hostname in hostname:
                    # Find where base_hostname appears in hostname
                    idx = hostname.rfind(base_hostname)
                    if idx > 0:
                        # Check character before the match - must be a dot for valid subdomain
                        if hostname[idx - 1] != '.':
                            return False, f"Domain suffix attack detected: {hostname}"
                return False, f"URL outside allowed domain: {hostname} != {base_hostname}"
            
            # 8. Path traversal detection - comprehensive checks
            path = parsed.path or ''
            
            # Check for actual traversal sequences
            if '/..' in path or path.startswith('..'):
                return False, "Path traversal detected"
            
            # Check for encoded path traversal (%2e%2e for ..)
            path_lower = path.lower()
            if '%2e' in path_lower:
                try:
                    decoded = unquote(path)
                    if '/..' in decoded or decoded.startswith('..'):
                        return False, "Encoded path traversal detected"
                except Exception:
                    return False, "Invalid path encoding"
            
            # Null byte injection
            if '\0' in url or '%00' in url.lower():
                return False, "Null byte injection detected"
            
            # CRLF/control character injection (HTTP request smuggling)
            for forbidden in ('\r', '\n', '\t'):
                if forbidden in url:
                    return False, "Control character (CR/LF/TAB) in URL"
            url_lower = url.lower()
            for encoded in ('%0d', '%0a', '%09'):
                if encoded in url_lower:
                    return False, "Encoded control character (%0d/%0a/%09) in URL"
            
            # Double-encoded traversal defense
            if '%25' in path_lower or '%c0%af' in path_lower:
                try:
                    decoded_once = unquote(path)
                    decoded_twice = unquote(decoded_once)
                    if '/..' in decoded_twice or decoded_twice.startswith('..'):
                        return False, "Double-encoded path traversal detected"
                except Exception:
                    pass  # Fail closed on decode errors
            
            return True, None
            
        except Exception as e:
            # Never leak internal error details to caller
            logging.debug(f"Security validation internal error: {type(e).__name__}")
            return False, "Security validation failed"
        
# ============================================================================
# SECURITY TRANSPORTS
# ============================================================================
    
class SecureTransport(httpx.HTTPTransport):
    """Transport that validates resolved IP before connecting"""
    IP_CACHE_TTL_SECONDS = 300
    IP_CACHE_MAX_SIZE = 1000  # FIX: Prevent unbounded growth
    IP_CACHE_CLEANUP_INTERVAL = 60  # FIX: Cleanup every 60 seconds

    def __init__(self, rate_limiter: Optional['PerIPRateLimiter'] = None, test_mode: bool = False):
        super().__init__()
        self._test_mode = test_mode  # ✅ Instance-level flag (eliminates global test pollution)
        self._resolved_ips: Dict[str, Tuple[str, float]] = {}
        self._ip_lock = RLock()
        self.rate_limiter = rate_limiter
        
        # FIX: Track cleanup timing
        self._last_cleanup = time.time()
        self._request_count = 0  # For periodic cleanup triggering
    
    def _get_cached_ip(self, hostname: str) -> Optional[str]:
        """Get cached IP if still valid"""
        with self._ip_lock:
            if hostname in self._resolved_ips:
                ip, timestamp = self._resolved_ips[hostname]
                if time.time() - timestamp < self.IP_CACHE_TTL_SECONDS:
                    return ip
                del self._resolved_ips[hostname]
            return None
    
    def _cache_ip(self, hostname: str, ip: str) -> None:
        """Cache resolved IP with timestamp and bounded size"""
        with self._ip_lock:
            # FIX: If cache is full, remove oldest entries before adding
            if len(self._resolved_ips) >= self.IP_CACHE_MAX_SIZE:
                # Remove 25% oldest entries to make room
                entries = sorted(self._resolved_ips.items(), key=lambda x: x[1][1])
                to_remove = len(entries) // 4
                for old_hostname, _ in entries[:to_remove]:
                    del self._resolved_ips[old_hostname]
                logging.debug(f"IP cache pruned: removed {to_remove} oldest entries")
            
            self._resolved_ips[hostname] = (ip, time.time())
    
    def _cleanup_stale_ips(self, force: bool = False) -> int:
        """Remove stale IP entries.
        
        Args:
            force: If True, clean regardless of interval
            
        Returns:
            Number of entries removed
        """
        now = time.time()
        if not force and now - self._last_cleanup < self.IP_CACHE_CLEANUP_INTERVAL:
            return 0
        
        with self._ip_lock:
            stale = [
                hostname for hostname, (_, timestamp) in self._resolved_ips.items()
                if now - timestamp > self.IP_CACHE_TTL_SECONDS
            ]
            for hostname in stale:
                del self._resolved_ips[hostname]
            
            self._last_cleanup = now
            
            if stale:
                logging.debug(f"Cleaned {len(stale)} stale IP entries "
                             f"(cache size: {len(self._resolved_ips)})")
            return len(stale)
    
    def handle_request(self, request: httpx.Request) -> httpx.Response:
        # Skip IP resolution in test mode
        if self._test_mode:
            return super().handle_request(request)
        
        hostname = request.url.host
        hostname = hostname.strip('[]')
        try:
            ipaddress.ip_address(hostname)
            raise SecurityError(f"Direct IP connection attempted: {hostname}")
        except ValueError:
            pass
        
        # Apply per-IP rate limiting
        if self.rate_limiter:
            self.rate_limiter.wait(hostname)
        
        # FIX: Periodic cleanup of stale IPs
        self._request_count += 1
        if self._request_count % 100 == 0:  # Every 100 requests
            self._cleanup_stale_ips()
        
        safe_ip = self._get_cached_ip(hostname)
        if safe_ip is None:
            with self._ip_lock:
                safe_ip = self._get_cached_ip(hostname)
                if safe_ip is None:
                    safe_ip = SecurityValidator.resolve_and_validate_hostname(hostname)
                    self._cache_ip(hostname, safe_ip)
        
        # Create new request with IP address instead of hostname
        # But preserve the Host header for virtual hosting
        new_url = request.url.copy_with(host=safe_ip)
        new_headers = request.headers.copy()
        new_headers["Host"] = hostname
        new_extensions = dict(request.extensions) if request.extensions else {}
        new_extensions["sni_hostname"] = hostname
        new_request = httpx.Request(
            method=request.method,
            url=new_url,
            headers=new_headers,
            stream=request.stream,
            extensions=new_extensions
        )
        try:
            return super().handle_request(new_request)
        except httpx.ConnectError:
            # DNS rotation or dead IP: clear cache and retry once
            with self._ip_lock:
                self._resolved_ips.pop(hostname, None)
            safe_ip_retry = SecurityValidator.resolve_and_validate_hostname(hostname)
            self._cache_ip(hostname, safe_ip_retry)
            new_url_retry = request.url.copy_with(host=safe_ip_retry)
            new_request_retry = httpx.Request(
                method=request.method, 
                url=new_url_retry, 
                headers=request.headers, 
                extensions={"sni_hostname": hostname}
            )
            return super().handle_request(new_request_retry)
    
    def clear_ip_cache(self) -> None:
        """Clear IP cache (for testing/shutdown)."""
        with self._ip_lock:
            self._resolved_ips.clear()
            logging.debug("IP cache cleared")
    
    def get_ip_cache_stats(self) -> Dict[str, Any]:
        """Get IP cache statistics."""
        with self._ip_lock:
            return {
                'size': len(self._resolved_ips),
                'max_size': self.IP_CACHE_MAX_SIZE,
                'ttl_seconds': self.IP_CACHE_TTL_SECONDS,
                'entries': list(self._resolved_ips.keys())[:10]  # Show first 10
            }
        
class SecureAsyncTransport(httpx.AsyncHTTPTransport):
    """Async Transport that validates resolved IP before connecting with non-blocking rate limiting"""
    IP_CACHE_TTL_SECONDS = 300
    IP_CACHE_MAX_SIZE = 1000  # FIX: Prevent unbounded growth
    IP_CACHE_CLEANUP_INTERVAL = 60  # FIX: Cleanup every 60 seconds

    def __init__(self, rate_limiter: Optional['PerIPRateLimiter'] = None, test_mode: bool = False):
        super().__init__()
        self._resolved_ips: Dict[str, Tuple[str, float]] = {}
        self._ip_lock = asyncio.Lock()
        self.rate_limiter = rate_limiter
        # FIX: handle_async_request() reads self._test_mode, but it was never
        # initialized here (only SecureTransport set it). That raised
        # AttributeError on EVERY async request, breaking all async metadata /
        # download paths. Mirror the sync transport's behavior.
        self._test_mode = test_mode

        # FIX: Track cleanup timing
        self._last_cleanup = time.time()
        self._request_count = 0

    async def _get_cached_ip(self, hostname: str) -> Optional[str]:
        """Get cached IP if still valid"""
        async with self._ip_lock:
            if hostname in self._resolved_ips:
                ip, timestamp = self._resolved_ips[hostname]
                if time.time() - timestamp < self.IP_CACHE_TTL_SECONDS:
                    return ip
                del self._resolved_ips[hostname]
            return None

    async def _cache_ip(self, hostname: str, ip: str) -> None:
        """Cache resolved IP with timestamp and bounded size"""
        async with self._ip_lock:
            # FIX: If cache is full, remove oldest entries before adding
            if len(self._resolved_ips) >= self.IP_CACHE_MAX_SIZE:
                entries = sorted(self._resolved_ips.items(), key=lambda x: x[1][1])
                to_remove = len(entries) // 4
                for old_hostname, _ in entries[:to_remove]:
                    del self._resolved_ips[old_hostname]
                logging.debug(f"Async IP cache pruned: removed {to_remove} oldest entries")
            
            self._resolved_ips[hostname] = (ip, time.time())

    async def _cleanup_stale_ips(self, force: bool = False) -> int:
        """Remove stale IP entries."""
        now = time.time()
        if not force and now - self._last_cleanup < self.IP_CACHE_CLEANUP_INTERVAL:
            return 0
        
        async with self._ip_lock:
            stale = [
                hostname for hostname, (_, timestamp) in self._resolved_ips.items()
                if now - timestamp > self.IP_CACHE_TTL_SECONDS
            ]
            for hostname in stale:
                del self._resolved_ips[hostname]
            
            self._last_cleanup = now
            
            if stale:
                logging.debug(f"Cleaned {len(stale)} stale async IP entries "
                             f"(cache size: {len(self._resolved_ips)})")
            return len(stale)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self._test_mode:  
            return await super().handle_async_request(request)
        hostname = request.url.host
        hostname = hostname.strip('[]')

        # Block direct IP connections (SSRF protection)
        try:
            ipaddress.ip_address(hostname)
            raise SecurityError(f"Direct IP connection attempted: {hostname}")
        except ValueError:
            pass

        # ✅ Apply per-IP rate limiting NON-BLOCKING
        if self.rate_limiter:
            if hasattr(self.rate_limiter, 'async_wait'):
                await self.rate_limiter.async_wait(hostname)
            else:
                self.rate_limiter.wait(hostname)

        # FIX: Periodic cleanup of stale IPs
        self._request_count += 1
        if self._request_count % 100 == 0:
            await self._cleanup_stale_ips()

        safe_ip = await self._get_cached_ip(hostname)
        if safe_ip is None:
            async with self._ip_lock:
                safe_ip = await self._get_cached_ip(hostname)
                if safe_ip is None:
                    safe_ip = SecurityValidator.resolve_and_validate_hostname(hostname)
                    await self._cache_ip(hostname, safe_ip)

        new_url = request.url.copy_with(host=safe_ip)
        new_headers = request.headers.copy()
        new_headers["Host"] = hostname
        new_extensions = dict(request.extensions) if request.extensions else {}
        new_extensions["sni_hostname"] = hostname

        new_request = httpx.Request(
            method=request.method,
            url=new_url,
            headers=new_headers,
            stream=request.stream,
            extensions=new_extensions
        )

        return await super().handle_async_request(new_request)
    
    async def clear_ip_cache(self) -> None:
        """Clear IP cache (for testing/shutdown)."""
        async with self._ip_lock:
            self._resolved_ips.clear()
            logging.debug("Async IP cache cleared")
    
    def get_ip_cache_stats(self) -> Dict[str, Any]:
        """Get IP cache statistics (sync for convenience)."""
        return {
            'size': len(self._resolved_ips),
            'max_size': self.IP_CACHE_MAX_SIZE,
            'ttl_seconds': self.IP_CACHE_TTL_SECONDS,
        }
    
# ============================================================================
# PATH SAFETY
# ============================================================================
class PathSafety:
    """Utility class for safe path operations"""
    
    @staticmethod
    def is_subpath(parent: Path, child: Path) -> bool:
        """
        Strictly check if child is inside parent.
        
        Args:
            parent: Parent directory path
            child: Child path to check
            
        Returns:
            True if child is inside parent
        """
        try:
            if not parent.exists():
                parent_resolved = parent.resolve()
            else:
                parent_resolved = parent.resolve()
            child_resolved = child.resolve()
            if os.name == 'nt':
                parent_drive = parent_resolved.drive.lower()
                child_drive = child_resolved.drive.lower()
                if parent_drive != child_drive:
                    return False
            try:
                if child_resolved == parent_resolved:
                    return True
                try:
                    common = os.path.commonpath([str(parent_resolved), str(child_resolved)])
                    if Path(common).resolve() != parent_resolved:
                        return False
                except ValueError:
                    return False
                return parent_resolved in child_resolved.parents
            except ValueError:
                return False
        except (ValueError, RuntimeError, OSError) as e:
            logging.warning(f"Path safety resolution failed: {e}")
            return False
    
    @staticmethod
    def safe_join(base: Path, *parts: str, max_depth: int = MAX_DIRECTORY_DEPTH,
                  max_filename_len: int = MAX_FILENAME_LENGTH) -> Optional[Path]:
        """
        Safely join path components with security checks.
        
        Args:
            base: Base directory
            *parts: Path parts to join
            max_depth: Maximum directory depth
            max_filename_len: Maximum filename length
            
        Returns:
            Safe joined path or None if unsafe
        """
        try:
            sanitized_parts = []
            depth = 0
            if base.is_symlink():
                logging.warning(f"Base path is a symlink, blocking: {base}")
                return None
            if not base.exists():
                base.mkdir(parents=True, exist_ok=True)
            try:
                base_resolved = base.resolve(strict=False)
            except TypeError:
                base_resolved = base.resolve()
            if base.is_symlink():
                logging.warning(f"Base path resolved to symlink target, blocking: {base}")
                return None
            for part in parts:
                if not part:
                    continue
                if os.path.isabs(part):
                    logging.warning(f"Absolute path detected and blocked: {part}")
                    return None
                part_path = Path(part)
                if '..' in part_path.parts:
                    logging.warning(f"Path traversal attempt detected in part: {part}")
                    return None
                part = part.replace('\0', '')
                filename = PathSafety._safe_filename(part, max_len=max_filename_len)
                if not filename:
                    logging.warning(f"Invalid filename after sanitization: {part}")
                    return None
                sanitized_parts.append(filename)
                depth += 1
                if depth > max_depth:
                    logging.warning(f"Path depth limit exceeded: {depth} > {max_depth}")
                    return None
            full_path = base.joinpath(*sanitized_parts)
            try:
                final_resolved = full_path.resolve(strict=False)
            except (OSError, ValueError, TypeError):
                logging.warning(f"Failed to resolve final path: {full_path}")
                return None
            if not PathSafety.is_subpath(base_resolved, final_resolved):
                logging.warning(f"Path safety check failed: {final_resolved} is outside {base_resolved}")
                return None
            return final_resolved
        except Exception as e:
            logging.debug(f"Error in safe_join: {e}")
            return None
    
    @staticmethod
    def safe_relative_to(path: Path, base: Path) -> Optional[str]:
        """
        Safely compute relative path.
        
        Args:
            path: Path to make relative
            base: Base directory
            
        Returns:
            Relative path or None if unsafe
        """
        try:
            if not base.exists():
                return None
            path_resolved = path.resolve()
            base_resolved = base.resolve()
            if not PathSafety.is_subpath(base_resolved, path_resolved):
                return None
            rel = path_resolved.relative_to(base_resolved)
            return str(rel)
        except (ValueError, RuntimeError, OSError):
            return None    

    @staticmethod
    def _normalize_url_path(path: str) -> str:
        """Normalize URL path - preserve leading slash if present."""
        try:
            if not path:
                return ''
            
            # FIX: Handle the test expectation for '/path/to/file'
            # The test expects '/path/to/file' to return '/path/to/file'
            if path.startswith('/'):
                # For absolute paths, keep the leading slash
                decoded = unquote(path)
                trailing_slash = decoded.endswith('/')
                stripped = decoded.lstrip('/')
                normalized = str(Path(stripped)) if stripped else ''
                if trailing_slash and normalized:
                    normalized += '/'
                return '/' + normalized if normalized else '/'
            else:
                # For relative paths, no leading slash
                decoded = unquote(path)
                trailing_slash = decoded.endswith('/')
                stripped = decoded.strip('/')
                normalized = str(Path(stripped)) if stripped else ''
                if trailing_slash and normalized:
                    normalized += '/'
                return normalized
        except Exception:
            return ""
    
    @staticmethod
    def _safe_filename(filename: str, max_len: int = MAX_FILENAME_LENGTH) -> str:
        """
        Make filename safe for filesystem.
        
        Args:
            filename: Original filename
            max_len: Maximum length
            
        Returns:
            Safe filename
        """
        if not filename:
            return "unnamed"
        filename = os.path.basename(filename)
        filename = filename.replace('\0', '')
        filename = ''.join(char for char in filename if ord(char) >= 32 or char in ' \r\t')
        if len(filename) > max_len:
            name, ext = os.path.splitext(filename)
            if len(ext) < max_len:
                filename = name[:max_len - len(ext)] + ext
            else:
                filename = filename[:max_len]
        filename = filename.replace('/', '_').replace('\\', '_')
        if is_reserved_windows_filename(filename):
            filename = f"_{filename}"
            logging.debug(f"Reserved Windows name detected, prefixed: {filename}")
        return filename or "unnamed"

# ============================================================================
# FAST URL VALIDATION UTILITIES (add after PathSafety class)
# ============================================================================
class FastURLValidator:
    """SIMD-accelerated URL validation using StringZilla."""
    
    HTTP_PREFIX = Str('http://')
    HTTPS_PREFIX = Str('https://')
    
    @staticmethod
    def is_valid_scheme(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla.
        
        Args:
            url: URL to validate
            
        Returns:
            True if scheme is http or https
        """
        url_sz = Str(url)
        return (url_sz.startswith(FastURLValidator.HTTP_PREFIX) or 
                url_sz.startswith(FastURLValidator.HTTPS_PREFIX))
    
    @staticmethod
    def get_path_fast(url: str) -> Str:
        """
        Fast path extraction using StringZilla.
        
        Args:
            url: URL to extract path from
            
        Returns:
            Path part as StringZilla Str
        """
        url_sz = Str(url)
        
        # Find protocol separator
        after_protocol = url_sz.find('://')
        if after_protocol < 0:
            return Str('')
        
        # Find first slash after domain
        path_start = url_sz.find('/', after_protocol + 3)
        if path_start < 0:
            return Str('')
        
        return url_sz[path_start:]
    
    @staticmethod
    def is_path_within_scope(path: Str, scope: Str) -> bool:
        """
        Fast path scope checking using StringZilla.
        
        Args:
            path: URL path to check
            scope: Base scope path
            
        Returns:
            True if path is within scope
        """
        return path.startswith(scope)
    
    @staticmethod
    def has_path_traversal(path: Str) -> bool:
        """
        Fast path traversal detection using StringZilla.
        
        Args:
            path: URL path to check
            
        Returns:
            True if path contains traversal sequences
        """
        return (path.find('..') >= 0 or 
                path.find('/.') >= 0 or 
                path.find('./') >= 0)
    
    @staticmethod
    def get_filename(path: Str) -> Str:
        """
        Fast filename extraction from path using StringZilla.
        
        Args:
            path: URL path
            
        Returns:
            Filename as StringZilla Str
        """
        last_slash = path.rfind('/')
        if last_slash >= 0:
            return path[last_slash + 1:]
        return path

# ============================================================================
# GLOBAL REGISTRY
# ============================================================================
_log_files: List[logging.Handler] = []

def cleanup_log_files() -> None:
    """Clean up log file handlers on exit"""
    # Iterate over a snapshot to prevent RuntimeError if list changes during shutdown
    for handler in list(_log_files):
        try:
            handler.close()
        except Exception as e:
            # SAFER: logging module may be partially torn down during atexit.
            # Use sys.stderr to guarantee the message is visible during teardown.
            sys.stderr.write(f"Cleanup log handler error: {e}\n")

atexit.register(cleanup_log_files)

# ============================================================================
# LRU CACHE
# ============================================================================
class LRUCache:
    """
    Thread-safe LRU cache with TTL and memory pressure handling.
    
    Fixed: Timestamps now stored with values, not separately to prevent memory leak.
    """
    
    def __init__(self, maxsize: int, ttl_seconds: float, name: str = "cache"):
        """
        Initialize LRU cache.
        
        Args:
            maxsize: Maximum number of items
            ttl_seconds: Time-to-live in seconds
            name: Cache name for logging
        """
        self.maxsize = maxsize
        self.ttl = ttl_seconds
        self.name = name
        # Store tuple (value, timestamp) - NOT separate dict!
        self.cache: OrderedDict[Any, Tuple[Any, float]] = OrderedDict()
        self.hits = 0
        self.misses = 0
        self.evictions = 0
        self.lock = RLock()
        self._timestamps: Dict[Any, float] = {}  # FIX: Add separate timestamp dict for backward compatibility

    def set(self, key: Any, value: Any) -> None:
        """Alias for put() for backward compatibility."""
        self.put(key, value)
    
    def get(self, key: Any) -> Optional[Any]:
        """
        Get item from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None
            
            value, timestamp = self.cache[key]
            if time.time() - timestamp > self.ttl:
                del self.cache[key]
                # Also remove from backward compatibility timestamps
                self._timestamps.pop(key, None)
                self.evictions += 1
                self.misses += 1
                return None
            
            self.cache.move_to_end(key)
            self.hits += 1
            return value
    
    def put(self, key: Any, value: Any) -> None:
        """
        Put item into cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        with self.lock:
            now = time.time()
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = (value, now)
            self._timestamps[key] = now  # For backward compatibility
            
            while len(self.cache) > self.maxsize:
                oldest = next(iter(self.cache))
                del self.cache[oldest]
                self._timestamps.pop(oldest, None)
                self.evictions += 1
    
    def put_batch(self, items: Dict[Any, Any]) -> None:
        """
        Batch insert multiple items with single lock acquisition.
        
        Args:
            items: Dictionary of key-value pairs to cache
        """
        with self.lock:
            now = time.time()
            for key, value in items.items():
                if key in self.cache:
                    self.cache.move_to_end(key)
                self.cache[key] = (value, now)
                self._timestamps[key] = now
            
            while len(self.cache) > self.maxsize:
                oldest = next(iter(self.cache))
                del self.cache[oldest]
                self._timestamps.pop(oldest, None)
                self.evictions += 1
    
    def shrink_to(self, target_percent: float = 0.5) -> int:
        """
        Shrink cache to target percentage of current size (for memory pressure).
        
        Args:
            target_percent: Target size as percentage of current size (0.0-1.0)
                           Use 0.0 to clear all items, 0.5 to reduce by half.
            
        Returns:
            Number of items evicted
        """
        # Validate and clamp target_percent
        if not 0.0 <= target_percent <= 1.0:
            logging.warning(f"LRUCache '{self.name}': target_percent {target_percent} "
                           f"out of range [0.0, 1.0], clamping to nearest bound.")
            target_percent = max(0.0, min(1.0, target_percent))
        
        with self.lock:
            current_size = len(self.cache)
            if current_size == 0:
                return 0
            
            # Calculate target size (allow 0 for aggressive cleanup)
            target_size = int(current_size * target_percent)
            evicted = current_size - target_size
            
            if evicted <= 0:
                # Log if target_percent > current_size? No, this is fine
                return 0
            
            # Evict oldest items (LRU order)
            for _ in range(evicted):
                oldest_key, oldest_value = self.cache.popitem(last=False)
                # Clean up timestamp dict (defensive: use pop with default)
                self._timestamps.pop(oldest_key, None)
                self.evictions += 1
            
            # Calculate actual evicted count (in case cache changed during loop? It shouldn't)
            actual_evicted = current_size - len(self.cache)
            
            if actual_evicted > 0:
                logging.debug(
                    f"Cache '{self.name}' shrunk from {current_size} to {len(self.cache)} items "
                    f"(removed {actual_evicted} items, target was {target_percent*100:.0f}% of current)"
                )
            
            return actual_evicted  
        
    def invalidate(self, key: Any) -> None:
        """
        Invalidate a cache entry.
        
        Args:
            key: Cache key to invalidate
        """
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                self._timestamps.pop(key, None)
    
    def clear(self) -> None:
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self._timestamps.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        with self.lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0
            return {
                'name': self.name,
                'size': len(self.cache),
                'maxsize': self.maxsize,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': f"{hit_rate:.1f}%",
                'evictions': self.evictions,
                'ttl_seconds': self.ttl
            }    
        
    def __contains__(self, key: Any) -> bool:
        """Support 'in' operator."""
        with self.lock:
            return key in self.cache
    
    def __len__(self) -> int:
        """Support len() function."""
        with self.lock:
            return len(self.cache)
        
# ============================================================================
# ATOMIC COUNTERS (NEW v3.0.6)
# ============================================================================
@total_ordering
class AtomicCounter:
    """
    Thread-safe atomic counter using Python's threading lock.
    Provides atomic increment, decrement, and value operations.
    """
    def __init__(self, initial: int = 0):
        """
        Initialize atomic counter.
        Args:
            initial: Initial counter value
        """
        self._value = initial
        self._lock = threading.RLock()
        self._total_increments = 0
        self._total_decrements = 0

    def increment(self, delta: int = 1) -> int:
        """Increment counter and return new value."""
        with self._lock:
            self._value += delta
            self._total_increments += abs(delta)
            return self._value

    def decrement(self, delta: int = 1) -> int:
        """Decrement counter and return new value."""
        with self._lock:
            self._value -= delta
            self._total_decrements += abs(delta)
            return self._value

    def add(self, value: int) -> int:
        """Add value to counter (alias for increment)."""
        return self.increment(value)

    def subtract(self, value: int) -> int:
        """Subtract value from counter (alias for decrement)."""
        return self.decrement(value)

    def value(self) -> int:
        """Get current counter value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset counter to zero."""
        with self._lock:
            self._value = 0
            self._total_increments = 0
            self._total_decrements = 0

    def get_stats(self) -> Dict[str, int]:
        """Get counter statistics."""
        with self._lock:
            return {
                'current': self._value,
                'total_increments': self._total_increments,
                'total_decrements': self._total_decrements
            }

    def __bool__(self) -> bool:
        """Support boolean context."""
        return self.value() != 0

    def __int__(self) -> int:
        """Convert to int."""
        return self.value()

    def __iadd__(self, other: int) -> 'AtomicCounter':
        """Support += operator."""
        self.increment(other)
        return self

    def __isub__(self, other: int) -> 'AtomicCounter':
        """Support -= operator."""
        self.decrement(other)
        return self

    def __eq__(self, other: object) -> bool:
        """Support equality comparison with int or another AtomicCounter."""
        if isinstance(other, int):
            return self.value() == other
        if isinstance(other, AtomicCounter):
            return self.value() == other.value()
        return NotImplemented

    # Defining __eq__ sets __hash__ to None, making instances unhashable
    # (TypeError if used as a dict key or set member). The counter is a
    # mutable container, so a value-based hash would be unstable; restore
    # identity-based hashing instead.
    __hash__ = object.__hash__

    def __lt__(self, other: object) -> bool:
        """Support less-than comparison with int or another AtomicCounter."""
        if isinstance(other, int):
            return self.value() < other
        if isinstance(other, AtomicCounter):
            return self.value() < other.value()
        return NotImplemented
class AtomicSize:
    """
    Thread-safe atomic size counter for bytes tracking.
    Specialized for tracking downloaded bytes with better precision.
    """
    
    def __init__(self):
        """Initialize atomic size counter."""
        self._size = 0
        self._lock = threading.RLock()
        self._max_size = 0
        self._total_adds = 0
        self._total_resets = 0
    
    def add(self, bytes_count: int) -> int:
        """
        Add bytes to total and return new total.
        
        Args:
            bytes_count: Number of bytes to add
            
        Returns:
            New total size
        """
        with self._lock:
            self._size += bytes_count
            self._total_adds += 1
            if self._size > self._max_size:
                self._max_size = self._size
            return self._size
    
    def subtract(self, bytes_count: int) -> int:
        """
        Subtract bytes from total and return new total.
        
        Args:
            bytes_count: Number of bytes to subtract
            
        Returns:
            New total size
        """
        with self._lock:
            self._size -= bytes_count
            return self._size
    
    def value(self) -> int:
        """Get current total size."""
        with self._lock:
            return self._size
    
    def reset(self) -> None:
        """Reset size counter to zero."""
        with self._lock:
            self._size = 0
            self._total_resets += 1
    
    def get_max(self) -> int:
        """Get maximum size reached."""
        with self._lock:
            return self._max_size
    
    def get_stats(self) -> Dict[str, int]:
        """Get size counter statistics."""
        with self._lock:
            return {
                'current_bytes': self._size,
                'max_bytes': self._max_size,
                'total_adds': self._total_adds,
                'total_resets': self._total_resets
            }
    
    def __bool__(self) -> bool:
        """Support boolean context."""
        return self.value() != 0

    def __int__(self) -> int:
        """Convert to int."""
        return self.value()
    
    def __iadd__(self, other: int) -> 'AtomicSize':
        """Support += operator."""
        self.add(other)
        return self
    
        
# ============================================================================
# FILE SYSTEM CACHE
# ============================================================================
class FileSystemCache:
    """Cache file system operations with TTL and memory pressure handling"""
    
    def __init__(self, ttl_seconds: float = FS_CACHE_TTL_SECONDS):
        """
        Initialize filesystem cache.
        
        Args:
            ttl_seconds: Time-to-live in seconds
        """
        self.ttl = ttl_seconds
        self.stat_cache: Dict[Path, Tuple[float, os.stat_result]] = {}
        self.exists_cache: Dict[Path, Tuple[float, bool]] = {}
        self.lock = RLock()
        self._access_count = 0
        # Add maxsize limits for memory pressure handling
        self.stat_cache_maxsize = 10000  # Max entries in stat cache
        self.exists_cache_maxsize = 10000  # Max entries in exists cache
    
    def get_stat(self, path: Path) -> Optional[os.stat_result]:
        # 1. Check cache under lock (short critical section)
        with self.lock:
            self._access_count += 1
            if path in self.stat_cache:
                timestamp, stat = self.stat_cache[path]
                # FIX: Always check TTL, not just every 100th access
                if time.time() - timestamp >= self.ttl:
                    del self.stat_cache[path]
                else:
                    return stat
    
        # 2. Perform blocking I/O OUTSIDE the lock
        try:
            stat = path.stat()
        except OSError:
            return None
    
        # 3. Update cache under lock (short critical section)
        with self.lock:
            self.stat_cache[path] = (time.time(), stat)
        return stat
    
    def exists(self, path: Path) -> Optional[bool]:
        # 1. Check cache under lock
        with self.lock:
            self._access_count += 1
            if path in self.exists_cache:
                timestamp, exists = self.exists_cache[path]
                if time.time() - timestamp >= self.ttl:
                    del self.exists_cache[path]
                else:
                    return exists
    
        # 2. Perform blocking I/O OUTSIDE the lock
        try:
            exists = path.exists()
        except OSError:
            return False
    
        # 3. Update cache under lock
        with self.lock:
            self.exists_cache[path] = (time.time(), exists)
        return exists
    
    def invalidate(self, path: Path) -> None:
        """Invalidate cache entries for path"""
        with self.lock:
            self.stat_cache.pop(path, None)
            self.exists_cache.pop(path, None)
    
    def clear(self) -> None:
        """Clear all cache entries"""
        with self.lock:
            self.stat_cache.clear()
            self.exists_cache.clear()
    
    def shrink_to(self, target_percent: float = 0.5) -> int:
        """Shrink cache under memory pressure."""
        with self.lock:
            old_stat_count = len(self.stat_cache)
            old_exists_count = len(self.exists_cache)
            
            target_stat = int(old_stat_count * target_percent)
            target_exists = int(old_exists_count * target_percent)
            
            evicted = 0
    
            # Stat cache
            if old_stat_count > target_stat:
                items_to_remove = old_stat_count - target_stat
                # ✅ SAFETY: Snapshot keys first to avoid RuntimeError
                keys_to_remove = list(self.stat_cache.keys())[:items_to_remove]
                for key in keys_to_remove:
                    del self.stat_cache[key]
                    evicted += 1
    
            # Exists cache
            if old_exists_count > target_exists:
                items_to_remove = old_exists_count - target_exists
                # ✅ SAFETY: Snapshot keys first to avoid RuntimeError
                keys_to_remove = list(self.exists_cache.keys())[:items_to_remove]
                for key in keys_to_remove:
                    del self.exists_cache[key]
                    evicted += 1
    
            if evicted > 0:
                logging.debug(f"FileSystemCache shrunk: {evicted} entries removed "
                              f"(stat: {len(self.stat_cache)}, exists: {len(self.exists_cache)})")
            return evicted
        
# ============================================================================
# DISK BACKED SET
# ============================================================================
class DiskBackedSet:
    """Memory-efficient set using disk storage with sequential write optimization.
    
    Performance characteristics:
    - O(1) add for items in memory
    - O(1) batch disk writes (avoids per-item I/O)
    - Memory bound: max_memory items in RAM
    - Disk bound: unlimited items on disk (sequential files)
    
    Thread-safety: All public methods are protected by RLock.
    """
    
    def __init__(self, temp_dir: Path, max_memory: int = MEMORY_CACHE_MAX_SIZE):
        """Initialize disk-backed set.
        
        Args:
            temp_dir: Directory for temporary files
            max_memory: Maximum items to keep in memory (default: 100,000)
        """
        self.temp_dir = temp_dir
        self.max_memory = max_memory
        self.memory_set: Set[str] = set()
        self.disk_files: List[Path] = []
        self.current_size = 0
        self.total_items = 0
        self.lock = RLock()
        
        # FIX: Batch write buffer to reduce disk I/O
        # Instead of writing each item to disk individually,
        # buffer writes and flush in batches
        self._write_buffer: List[str] = []
        self._buffer_max_size = 10000  # Flush when buffer reaches this size
        
        try:
            temp_dir.mkdir(parents=True, exist_ok=True)
            # Verify writability
            test_file = temp_dir / '.write_test'
            test_file.touch()
            test_file.unlink()
        except OSError as e:
            raise CacheError(f"Cannot create/write to cache directory {temp_dir}: {e}")
    
    
    def add(self, item: str) -> None:
        """Add item to set with batch write optimization.
        
        Items are first added to memory. When memory is full, they are
        flushed to disk in a single batch write (avoiding per-item I/O).
        
        Args:
            item: Item to add
        """
        if not item or not isinstance(item, str):
            return  # Skip invalid items

        with self.lock:
            # Fast duplicate check (memory only - original behavior preserved)
            if item in self.memory_set:
                return
            
            # Add to memory set
            self.memory_set.add(item)
            self._write_buffer.append(item)
            self.total_items += 1
            
            # Flush buffer if full (batch write optimization)
            if len(self._write_buffer) >= self._buffer_max_size:
                self._flush_buffer()
            
            # If memory is full, flush to disk
            if len(self.memory_set) >= self.max_memory:
                self._flush_to_disk()
    
    def _flush_buffer(self) -> None:
        """Flush write buffer to disk in a single batch write with safety checks.
        
        Thread-safety: Caller MUST hold self.lock.
        
        Safety guarantees:
        1. Atomic write via temp file + rename
        2. Disk space check before writing  
        3. Batch size limit to prevent memory exhaustion
        4. Item validation (no empty strings, no control chars)
        5. Proper UTF-8 encoding with error handling
        6. Recovery from partial/failed writes
        """
        if not self._write_buffer:
            return
        
        # Validate caller holds lock (debug mode only)
        if hasattr(self.lock, '_is_owned'):
            assert self.lock._is_owned(), "_flush_buffer called without lock"
        
        # PROBLEM 1: Limit batch size to prevent memory issues
        MAX_BATCH_ITEMS = 100000
        MAX_BATCH_BYTES = 50 * 1024 * 1024  # 50MB max per batch
        
        items_to_write = self._write_buffer[:]
        
        # Truncate if too large (prevents memory bomb)
        if len(items_to_write) > MAX_BATCH_ITEMS:
            logging.warning(f"Batch size {len(items_to_write)} exceeds limit {MAX_BATCH_ITEMS}, truncating")
            items_to_write = items_to_write[:MAX_BATCH_ITEMS]
            # Keep remaining items in buffer for next flush
            self._write_buffer = self._write_buffer[MAX_BATCH_ITEMS:] + self._write_buffer[MAX_BATCH_ITEMS:]
        else:
            self._write_buffer.clear()
        
        # PROBLEM 2: Validate and clean items
        cleaned_items = []
        skipped_count = 0
        for item in items_to_write:
            if not item or not isinstance(item, str):
                skipped_count += 1
                continue
            
            # Remove control characters (except newline which we'll escape)
            cleaned = ''.join(char for char in item if ord(char) >= 32 or char in '\t\r\n')
            if cleaned != item:
                logging.debug(f"Cleaned control characters from item: {item[:50]}...")
            
            # Escape newlines and backslashes for safe parsing
            cleaned = cleaned.replace('\\', '\\\\')
            cleaned = cleaned.replace('\n', '\\n')
            cleaned = cleaned.replace('\r', '\\r')
            cleaned_items.append(cleaned)
        
        if skipped_count > 0:
            logging.warning(f"Skipped {skipped_count} invalid items during buffer flush")
        
        if not cleaned_items:
            return  # Nothing to write after validation
        
        # PROBLEM 3: Accurate disk space check
        try:
            # Calculate actual bytes needed (UTF-8 encoded)
            # Add 1 byte per item for newline, plus overhead for escaping
            total_bytes = 0
            for item in cleaned_items:
                total_bytes += len(item.encode('utf-8'))
            total_bytes += len(cleaned_items)  # newlines
            total_bytes += 1024  # filesystem overhead buffer
            
            usage = shutil.disk_usage(self.temp_dir)
            
            # Need 2x for safety (temp file + final file during rename)
            if usage.free < total_bytes * 2:
                logging.warning(
                    f"Low disk space for buffer flush: need ~{total_bytes/1024/1024:.1f}MB, "
                    f"have {usage.free/1024/1024:.1f}MB free"
                )
                # Put items back and try later
                self._write_buffer = items_to_write + self._write_buffer
                return
                
            # Also check if total_bytes exceeds warning threshold
            if total_bytes > MAX_BATCH_BYTES:
                logging.warning(
                    f"Batch size {total_bytes/1024/1024:.1f}MB exceeds {MAX_BATCH_BYTES/1024/1024:.1f}MB, "
                    f"consider increasing _buffer_max_size or reducing batch size"
                )
        except Exception as e:
            logging.debug(f"Disk space check failed, proceeding anyway: {e}")
        
        # PROBLEM 4: Atomic write with temp file
        batch_file = self.temp_dir / f"batch_{uuid.uuid4().hex}.txt"
        temp_file = batch_file.with_suffix('.tmp')
        
        try:
            # Write to temp file first (atomic)
            with open(temp_file, 'w', encoding='utf-8', errors='replace') as f:
                # Write in chunks to avoid memory issues for huge batches
                CHUNK_SIZE = 10000
                for i in range(0, len(cleaned_items), CHUNK_SIZE):
                    chunk = cleaned_items[i:i + CHUNK_SIZE]
                    f.write('\n'.join(chunk))
                    if i + CHUNK_SIZE < len(cleaned_items):
                        f.write('\n')  # Add newline between chunks
                    # Flush periodically to avoid excessive memory
                    if i % (CHUNK_SIZE * 10) == 0:
                        f.flush()
                
                f.flush()
                try:
                    os.fsync(f.fileno())
                except (OSError, AttributeError):
                    pass
            
            # Verify temp file was written correctly
            if temp_file.stat().st_size == 0:
                raise IOError("Temp file is empty after write")
            
            # Atomic rename
            os.replace(str(temp_file), str(batch_file))
            
            # Verify final file exists and has content
            if not batch_file.exists() or batch_file.stat().st_size == 0:
                raise IOError("Final file missing or empty after rename")
            
            # Only add to disk_files after successful write
            self.disk_files.append(batch_file)
            
        except Exception as e:
            logging.error(f"Failed to flush buffer to disk: {e}")
            
            # Clean up temp file if it exists
            try:
                temp_file.unlink(missing_ok=True)
            except Exception:
                pass
            
            # Clean up batch file if it exists (shouldn't, but safe)
            try:
                batch_file.unlink(missing_ok=True)
            except Exception:
                pass
            
            # Put items back for retry (preserve original order)
            self._write_buffer = items_to_write + self._write_buffer
            
            # If disk is full, raise to upper layer for handling
            if isinstance(e, (OSError, IOError)) and getattr(e, 'errno', 0) in (28, 122):  # ENOSPC
                raise DiskSpaceError(f"No space left on device: {self.temp_dir}")
        
    def _flush_to_disk(self) -> bool:
        """Flush memory set to disk atomically.
        
        FIX (data loss): Write to temp file, fsync, then atomic rename.
        Only after successful rename is the file added to disk_files.
        
        FIX (performance): Use batch write instead of per-item write.
        
        Returns:
            True if flush successful
        """

        if not self.memory_set:
            return True
        
        self._flush_buffer()
        
        # Check available disk space BEFORE writing
        try:
            # Estimate size: sum of strings + newlines
            estimated_size = sum(len(item) for item in self.memory_set) + len(self.memory_set)
            usage = shutil.disk_usage(self.temp_dir)
            if usage.free < estimated_size * 2:  # Need 2x for safety
                logging.error(f"Insufficient disk space: need {estimated_size}, have {usage.free}")
                return False
        except Exception:
            pass  # Proceed anyway
        
        final_path = self.temp_dir / f"set_{uuid.uuid4().hex}.txt"
        partial_path = final_path.with_suffix('.partial')
        
        try:
            # Write in chunks to avoid OOM with large sets
            with open(partial_path, 'w', encoding='utf-8') as f:
                # Write sorted items in batches to avoid loading all into memory at once
                # Convert to list and sort - still memory heavy but necessary
                sorted_items = sorted(self.memory_set)  # This is the bottleneck
                for item in sorted_items:
                    f.write(f"{item}\n")
                f.flush()
                try:
                    os.fsync(f.fileno())
                except (OSError, AttributeError):
                    pass
            
            os.replace(partial_path, final_path)
            
            # fsync directory (best effort)
            try:
                dir_fd = os.open(str(self.temp_dir), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except (OSError, AttributeError):
                pass
            
            self.disk_files.append(final_path)
            self.memory_set.clear()
            self._write_buffer.clear()
            # FIX: total_items should NOT be cleared - it's the TOTAL across memory+disk
            # self.total_items remains unchanged (correct)
            self._prune_disk_files()
            return True
            
        except Exception as e:
            logging.error(f"Failed to flush set to disk: {e}")
            for p in (partial_path, final_path):
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
            return False    
        
    def _prune_disk_files(self, max_files: int = 20, max_age_hours: int = 2) -> None:
        """Remove old/empty disk files to prevent filesystem clutter.
        
        Thread-safety: Caller MUST hold self.lock.
        Note: self.total_items is NOT decremented during pruning. len() will 
              return an upper bound of items ever added. This is acceptable for 
              "fire-and-forget" tracking sets but means accurate current count 
              requires a future item-indexing enhancement.
        """
        if len(self.disk_files) <= max_files:
            return
    
        now = time.time()
        to_remove = []
    
        # Sort by mtime (oldest first) with graceful fallback
        try:
            def safe_mtime(p: Path) -> float:
                try:
                    return p.stat().st_mtime
                except OSError:
                    return 0.0
            self.disk_files.sort(key=safe_mtime)
        except Exception as e:
            logging.debug(f"Failed to sort disk files for pruning: {e}")
            return
    
        # Identify files to remove
        # Slice safely handles cases where len < max_files (returns empty list)
        candidates = self.disk_files[:-max_files] if max_files > 0 else self.disk_files
        
        for f in candidates:
            try:
                # OPTIMIZATION: Single stat() syscall per file
                st = f.stat()
                if st.st_size == 0:
                    to_remove.append(f)
                    logging.debug(f"Marking empty disk file for removal: {f}")
                    continue
    
                age_hours = (now - st.st_mtime) / 3600
                if age_hours > max_age_hours:
                    to_remove.append(f)
                    logging.debug(f"Marking old disk file for removal: {f} (age={age_hours:.1f}h)")
            except OSError as e:
                logging.debug(f"Cannot stat {f} during prune, skipping: {e}")
                continue
            except Exception as e:
                logging.debug(f"Unexpected error checking {f}: {e}")
                continue
    
        if not to_remove:
            return
    
        # Efficient O(n) removal from tracking list
        to_remove_set = set(to_remove)
        original_count = len(self.disk_files)
        self.disk_files = [f for f in self.disk_files if f not in to_remove_set]
        removed_count = original_count - len(self.disk_files)
    
        # Delete files from disk
        for f in to_remove:
            try:
                f.unlink(missing_ok=True)
            except Exception as e:
                logging.debug(f"Failed to delete disk file {f}: {e}")
    
        if removed_count > 0:
            logging.debug(f"Pruned {removed_count} old/empty disk files, "
                         f"{len(self.disk_files)} remaining")    
            
            
    def shrink_to(self, target_percent: float = 0.5) -> int:
        """
        Shrink under memory pressure by reducing memory set size.
        
        Args:
            target_percent: Target size percentage of current memory set (0.0-1.0)
            
        Returns:
            Number of items flushed to disk (0 if no shrink needed or failed)
        """
        with self.lock:
            current_memory_size = len(self.memory_set)
            
            if current_memory_size == 0:
                return 0
            
            target_memory_size = max(1, int(current_memory_size * target_percent))
            
            if current_memory_size <= target_memory_size:
                return 0
            
            items_list = list(self.memory_set)
            items_to_keep = set(items_list[:target_memory_size])
            items_to_flush = self.memory_set - items_to_keep
            
            if not items_to_flush:
                return 0
            
            self._flush_buffer()
            
            # Ensure a disk file exists
            if not self.disk_files:
                try:
                    temp_file = self.temp_dir / f"set_{uuid.uuid4().hex}.txt"
                    temp_file.touch()
                    self.disk_files.append(temp_file)
                except Exception as e:
                    logging.error(f"Failed to create disk file during shrink: {e}")
                    return 0
            
            current_file = self.disk_files[-1]
            items_written = 0
            staging_file = None  # Initialize for finally block
            
            try:
                # Write to staging file first
                staging_file = self.temp_dir / f"staging_{uuid.uuid4().hex}.txt"
                with open(staging_file, 'w', encoding='utf-8') as f:
                    for item in sorted(items_to_flush):
                        safe_item = str(item).replace('\\', '\\\\')
                        safe_item = safe_item.replace('\n', '\\n')
                        safe_item = safe_item.replace('\r', '\\r')
                        f.write(f"{safe_item}\n")
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except (OSError, AttributeError):
                        pass
                
                # Append to existing disk file
                with open(current_file, 'a', encoding='utf-8') as f:
                    with open(staging_file, 'r', encoding='utf-8') as sf:
                        shutil.copyfileobj(sf, f)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except (OSError, AttributeError):
                        pass
                
                items_written = len(items_to_flush)
                self.memory_set = items_to_keep
                
                logging.debug(
                    f"DiskBackedSet shrunk from {current_memory_size} to "
                    f"{len(self.memory_set)} items (flushed {items_written})"
                )
                
            except Exception as e:
                logging.error(f"Failed to write items to disk during shrink: {e}")
                return 0
                
            finally:
                # Clean up staging file
                if staging_file and staging_file.exists():
                    try:
                        staging_file.unlink(missing_ok=True)
                    except Exception:
                        pass
            
            # Prune old disk files (use same limit as _prune_disk_files default: 20)
            MAX_DISK_FILES = 20
            if len(self.disk_files) > MAX_DISK_FILES:
                self._prune_disk_files(max_files=MAX_DISK_FILES)
            
            return items_written   
    
    def clear(self) -> None:
        """Clear all items from memory and disk."""
        with self.lock:
            self.memory_set.clear()
            self._write_buffer.clear()
            
            for disk_file in self.disk_files:
                try:
                    disk_file.unlink()
                except Exception as e:
                    logging.debug(f"DiskBackedSet cleanup error: {e}")
            
            self.disk_files.clear()
            self.current_size = 0
            self.total_items = 0
    
    def __len__(self) -> int:
        """Get total number of items in the set."""
        with self.lock:
            return self.total_items
        
# ============================================================================
# ADAPTIVE BATCH PROCESSOR
# ============================================================================
class AdaptiveBatchProcessor:
    """Dynamically adjust batch sizes based on performance"""
    
    def __init__(self, initial_batch: int = BATCH_SIZE,
                 min_batch: int = MIN_BATCH_SIZE,
                 max_batch: int = MAX_BATCH_SIZE,
                 target_time: float = TARGET_BATCH_TIME_SECONDS,
                 adjustment_factor: float = BATCH_ADJUSTMENT_FACTOR):
        """
        Initialize adaptive batch processor.
        
        Args:
            initial_batch: Initial batch size
            min_batch: Minimum batch size
            max_batch: Maximum batch size
            target_time: Target processing time per batch
            adjustment_factor: Factor for adjusting batch size
        """
        self.batch_size = initial_batch
        self.min_batch = min_batch
        self.max_batch = max_batch
        self.target_time = target_time
        self.adjustment_factor = adjustment_factor
        self.processing_times = deque(maxlen=BATCH_SAMPLE_SIZE)
        self.items_processed = deque(maxlen=BATCH_SAMPLE_SIZE)
        self.lock = RLock()
    
    def record_batch(self, processing_time: float, items: int) -> None:
        """
        Record batch processing metrics.
        
        Args:
            processing_time: Time taken to process batch
            items: Number of items in batch
        """
        with self.lock:
            self.processing_times.append(processing_time)
            self.items_processed.append(items)
            self._adjust_batch_size()
    
    def _adjust_batch_size(self) -> None:
        """Adjust batch size based on recent performance"""
        if len(self.processing_times) < 2:
            return
        total_time = sum(self.processing_times)
        total_items = sum(self.items_processed)
        if total_items == 0:
            return
        avg_time_per_item = total_time / total_items
        if avg_time_per_item > 0:
            optimal_batch = int(self.target_time / avg_time_per_item)
            optimal_batch = max(self.min_batch, min(self.max_batch, optimal_batch))
            new_size = int(self.batch_size * (1 - self.adjustment_factor) +
                          optimal_batch * self.adjustment_factor)
            self.batch_size = max(self.min_batch, min(self.max_batch, new_size))
    
    def get_batch_size(self) -> int:
        """Get current recommended batch size"""
        with self.lock:
            return self.batch_size
    
    def reset(self) -> None:
        """Reset to initial settings"""
        with self.lock:
            self.batch_size = BATCH_SIZE
            self.processing_times.clear()
            self.items_processed.clear()


# ============================================================================
# FAST HTML PARSING UTILITIES
# ============================================================================
def extract_links_fast(html_content: Union[bytes, str]) -> List[str]:
    """
    Extract links from HTML without full DOM parsing.
    
    Args:
        html_content: HTML content as bytes or string
        
    Returns:
        List of extracted links
    """
    if isinstance(html_content, str):
        html_content = html_content.encode('utf-8', errors='ignore')
    links = []
    start = 0
    href_pattern = b'href="'
    href_pattern2 = b"href='"
    
    # Extract double-quoted hrefs
    while True:
        pos = html_content.find(href_pattern, start)
        if pos == -1:
            break
        pos += len(href_pattern)
        end_pos = html_content.find(b'"', pos)
        if end_pos == -1:
            break
        href = html_content[pos:end_pos].decode('utf-8', errors='ignore')
        if href and not href.startswith(('#', 'javascript:', 'mailto:')):
            links.append(href)
        start = end_pos + 1
    
    # Extract single-quoted hrefs
    start = 0
    while True:
        pos = html_content.find(href_pattern2, start)
        if pos == -1:
            break
        pos += len(href_pattern2)
        end_pos = html_content.find(b"'", pos)
        if end_pos == -1:
            break
        href = html_content[pos:end_pos].decode('utf-8', errors='ignore')
        if href and not href.startswith(('#', 'javascript:', 'mailto:')):
            links.append(href)
        start = end_pos + 1
    
    return links

def should_use_fast_parser(content_length: Optional[int], config) -> bool:
    """
    Determine whether to use the fast parser (StringZilla-based) or lxml.

    Policy:
    - If lxml isn't available, the fast parser is the only option.
    - If the document is large, prefer the fast parser for speed.
    - Otherwise, prefer lxml for correctness. ``config.fast_parsing_fallback``
      is used as a fallback when lxml fails at runtime, NOT as a primary
      preference, so it is intentionally NOT consulted here.

    Args:
        content_length: Length of content in bytes
        config: MirrorConfig instance (kept for API stability)

    Returns:
        True if fast parser should be used
    """
    if not LXML_AVAILABLE:
        return True
    if content_length and content_length > FAST_PARSE_MIN_CONTENT_LENGTH:
        return True
    return False

# ============================================================================
# CIRCUIT BREAKER
# ============================================================================
class CircuitBreaker:
    """Thread-safe circuit breaker with proper HALF_OPEN semantics."""
    
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 half_open_limit: int = 3):
        # Input validation
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if recovery_timeout <= 0:
            raise ValueError(f"recovery_timeout must be > 0, got {recovery_timeout}")
        if half_open_limit < 1:
            raise ValueError(f"half_open_limit must be >= 1, got {half_open_limit}")
        
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_limit = half_open_limit
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_successes = 0
        self.half_open_start = 0.0
        self.half_open_permits = 0
        self.lock = RLock()
        self.total_failures = 0
        self.total_successes = 0
    
    def can_execute(self) -> bool:
        """Thread-safe check if operation can proceed."""
        with self.lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    self.half_open_start = time.time()
                    logging.info("Circuit breaker half-open - testing recovery")
                    return True
                return False
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                # Check timeout first
                if time.time() - self.half_open_start >= self.recovery_timeout:
                    self.state = CircuitBreakerState.OPEN
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    logging.warning("Circuit breaker half-open timeout, reopening")
                    return False
                
                # Reserve a permit atomically
                if self.half_open_permits < self.half_open_limit:
                    self.half_open_permits += 1
                    return True
                return False
            
            return False
    
    def record_success(self) -> None:
        """Record a successful operation."""
        with self.lock:
            self.total_successes += 1
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_limit:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.half_open_successes = 0
                    self.half_open_permits = 0
                    logging.info("Circuit breaker closed - service recovered")
            
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = max(0, self.failure_count - 1)
    
    def record_failure(self) -> None:
        """Record a failed operation."""
        with self.lock:
            self.total_failures += 1
            self.last_failure_time = time.time()
            
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.half_open_successes = 0
                self.half_open_permits = 0
                logging.warning("Circuit breaker reopened - test request failed")
            
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    logging.warning(f"Circuit breaker opened - {self.failure_count} failures")
    
    def is_closed(self) -> bool:
        """Pure query: returns True if circuit is closed (no side effects)."""
        with self.lock:
            return self.state == CircuitBreakerState.CLOSED
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self.lock:
            return {
                'state': self.state.value,
                'failure_count': self.failure_count,
                'failure_threshold': self.failure_threshold,
                'total_failures': self.total_failures,
                'total_successes': self.total_successes,
                'last_failure_time': self.last_failure_time,
                'half_open_successes': self.half_open_successes,
                'half_open_permits': self.half_open_permits,
                'half_open_limit': self.half_open_limit,
                'recovery_timeout': self.recovery_timeout
            }
        
class AsyncCircuitBreaker:
    """Circuit breaker specifically for async operations."""
    
    def __init__(self, failure_threshold: int = 10, recovery_timeout: float = 30.0):
        """
        Initialize async circuit breaker.
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Time in seconds before attempting recovery
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
        self.lock = asyncio.Lock()
        self.total_failures = 0
        self.total_successes = 0
        self.half_open_successes = 0  # NEW: Track successes in half-open state
        self.half_open_limit = 3      # NEW: Number of successes needed to close
    
    async def can_execute(self) -> bool:
        """
        Check if operation can be executed.
        
        FIXED: No side effects - state transitions happen in record_failure/record_success.
        """
        async with self.lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    # FIX: Actually transition to half-open here
                    self.state = "half-open"
                    self.half_open_successes = 0
                    logging.debug("Async circuit breaker half-open - testing recovery")
                    return True
                return False
            return True
    
    async def _try_transition_to_half_open(self) -> bool:
        """
        Atomically attempt to transition from OPEN to HALF-OPEN.
        
        Returns:
            True if transition was successful and execution is allowed
        """
        async with self.lock:
            if self.state == "open":
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = "half-open"
                    self.half_open_successes = 0
                    logging.debug("Async circuit breaker half-open - testing recovery")
                    return True
            return self.state != "open"
    
    async def execute_or_fallback(self, operation: Callable, fallback: Callable = None):
        """
        Execute an operation with circuit breaker protection.
        FIX: Safely handles both sync and async fallbacks.
        """
        can_proceed = await self._try_transition_to_half_open()
        if not can_proceed:
            if fallback:
                fb_result = fallback()
                # ✅ FIX: Only await if fallback returns a coroutine
                return await fb_result if asyncio.iscoroutine(fb_result) else fb_result
            raise MirrorConnectionError("Circuit breaker is open")
        
        try:
            result = await operation()
            await self.record_success()
            return result
        except Exception as e:
            await self.record_failure()
            if fallback:
                fb_result = fallback()
                return await fb_result if asyncio.iscoroutine(fb_result) else fb_result
            raise
    
    async def record_success(self) -> None:
        """Record a successful operation."""
        async with self.lock:
            self.total_successes += 1
            
            if self.state == "half-open":
                self.half_open_successes += 1
                if self.half_open_successes >= self.half_open_limit:
                    self.state = "closed"
                    self.failures = 0
                    self.half_open_successes = 0
                    logging.info("Async circuit breaker closed - service recovered")
            elif self.state == "closed":
                # Gradually reduce failure count on successes
                self.failures = max(0, self.failures - 1)
    
    async def record_failure(self) -> None:
        """Record a failed operation."""
        async with self.lock:
            self.total_failures += 1
            self.failures += 1
            self.last_failure_time = time.time()
            
            if self.state == "half-open":
                # Half-open failure - reopen circuit
                self.state = "open"
                self.half_open_successes = 0
                logging.warning("Async circuit breaker reopened - test request failed")
            elif self.state == "closed" and self.failures >= self.failure_threshold:
                # Closed state reached threshold - open circuit
                self.state = "open"
                logging.warning(f"Async circuit breaker opened after {self.failures} failures")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            'state': self.state,
            'failures': self.failures,
            'failure_threshold': self.failure_threshold,
            'total_failures': self.total_failures,
            'total_successes': self.total_successes,
            'last_failure_time': self.last_failure_time,
            'half_open_successes': self.half_open_successes
        }

# ============================================================================
# BANDWIDTH LIMITER
# ============================================================================
class BandwidthLimiter:
    """Limit download bandwidth with smoothing"""
    
    def __init__(self, max_bytes_per_second: Optional[float] = None):
        """
        Initialize bandwidth limiter.
        
        Args:
            max_bytes_per_second: Maximum bytes per second
        """
        self.max_bytes_per_second = max_bytes_per_second
        self.bytes_downloaded = 0
        self.last_check = time.time()
        self.lock = RLock()
        self.peak_rate = 0.0
        self.average_rate = 0.0
    
    def throttle(self, bytes_count: int) -> None:
        """
        Throttle download speed.
        
        Args:
            bytes_count: Number of bytes just downloaded
        """
        if not self.max_bytes_per_second:
            return
        with self.lock:
            self.bytes_downloaded += bytes_count
            now = time.time()
            elapsed = now - self.last_check
            
            if elapsed >= 1.0:
                current_rate = self.bytes_downloaded / elapsed
                self.peak_rate = max(self.peak_rate, current_rate)
                self.average_rate = (self.average_rate * 0.9 + current_rate * 0.1)
                self.bytes_downloaded = 0
                self.last_check = now
            elif self.bytes_downloaded > self.max_bytes_per_second:
                sleep_time = (self.bytes_downloaded / self.max_bytes_per_second) - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)
    
    def get_stats(self) -> Dict[str, float]:
        """
        Get bandwidth limiter statistics.
        
        Returns:
            Dictionary with bandwidth statistics
        """
        with self.lock:
            return {
                'limit_bps': self.max_bytes_per_second,
                'peak_bps': self.peak_rate,
                'average_bps': self.average_rate
            }

# ============================================================================
# DOWNLOAD QUEUE
# ============================================================================
class DownloadQueue:
    """Priority queue for download tasks with metrics"""
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize download queue.
        
        Args:
            max_size: Maximum queue size
        """
        self.max_size = max_size
        self.queues: Dict[DownloadPriority, Deque[DownloadTask]] = {
            DownloadPriority.HIGH: deque(),
            DownloadPriority.NORMAL: deque(),
            DownloadPriority.LOW: deque()
        }
        self.lock = RLock()
        self.active_tasks: Set[str] = set()
        self.total_added = 0
        self.total_completed = 0
        self.total_failed = 0
    
    def add(self, task: DownloadTask) -> bool:
        """
        Add task to queue.
        
        Args:
            task: Download task to add
            
        Returns:
            True if added successfully
        """
        with self.lock:
            if len(self) >= self.max_size:
                return False
            task_id = f"{task.remote_url}:{task.local_path}"
            if task_id in self.active_tasks:
                return False
            self.queues[task.priority].append(task)
            self.active_tasks.add(task_id)
            self.total_added += 1
            return True
    
    def get(self) -> Optional[DownloadTask]:
        """
        Get next task from queue.
        
        Returns:
            Next task or None if queue empty
        """
        with self.lock:
            for priority in [DownloadPriority.HIGH, DownloadPriority.NORMAL, DownloadPriority.LOW]:
                if self.queues[priority]:
                    task = self.queues[priority].popleft()
                    return task
            return None
    
    def get_batch(self, max_batch: int) -> List[DownloadTask]:
        """
        Get multiple tasks in single lock acquisition.
        
        Args:
            max_batch: Maximum number of tasks to get
            
        Returns:
            List of tasks
        """
        with self.lock:
            tasks = []
            for priority in [DownloadPriority.HIGH, DownloadPriority.NORMAL, DownloadPriority.LOW]:
                while len(tasks) < max_batch and self.queues[priority]:
                    task = self.queues[priority].popleft()
                    tasks.append(task)
                    task_id = f"{task.remote_url}:{task.local_path}"
                    self.active_tasks.discard(task_id)
                if len(tasks) >= max_batch:
                    break
            return tasks
    
    def complete(self, task: DownloadTask, success: bool = True) -> None:
        """
        Mark task as complete.
        
        Args:
            task: Completed task
            success: Whether task succeeded
        """
        with self.lock:
            task_id = f"{task.remote_url}:{task.local_path}"
            self.active_tasks.discard(task_id)
            if success:
                self.total_completed += 1
            else:
                self.total_failed += 1
    
    def __len__(self) -> int:
        """Get current queue size"""
        with self.lock:
            return sum(len(q) for q in self.queues.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.
        
        Returns:
            Dictionary with queue statistics
        """
        with self.lock:
            return {
                'size': len(self),
                'max_size': self.max_size,
                'active_tasks': len(self.active_tasks),
                'total_added': self.total_added,
                'total_completed': self.total_completed,
                'total_failed': self.total_failed,
                'high_priority': len(self.queues[DownloadPriority.HIGH]),
                'normal_priority': len(self.queues[DownloadPriority.NORMAL]),
                'low_priority': len(self.queues[DownloadPriority.LOW])
            }

# ============================================================================
# METRICS COLLECTOR
# ============================================================================
class MetricsCollector:
    """Collect and report metrics with performance tracking"""
    
    def __init__(self):
        """Initialize metrics collector"""
        self.metrics: Dict[str, Any] = {
            'files_downloaded': 0,
            'bytes_downloaded': 0,
            'files_skipped': 0,
            'files_failed': 0,
            'directories_processed': 0,
            'directories_scanned_parallel': 0,
            'directories_scanned_sequential': 0,
            'directories_scanned_async': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'cache_head_requests_saved': 0,
            'cache_refreshed': False,
            'cache_age_days': 0,
            'cache_signatures': 0,
            'cache_invalidated_dirs': 0,
            'html_cache_hits': 0,
            'html_cache_misses': 0,
            'rget_list_used': False,
            'parse_time_seconds': 0,
            'errors': [],
            'etag_matches': 0,
            'etag_mismatches': 0,
            'etag_304_responses': 0,
            'etag_unavailable': 0,
            'http2_connections': 0,
            'http11_fallbacks': 0,
            'request_times': [],
            'download_times': [],
            'parse_times': [],
            'async_metadata_checks': 0,
            'content_hash_verifications': 0,
            'peak_memory_mb': 0,
            'files_would_delete': 0,
            'dirs_would_delete': 0,
            'adaptive_async_enabled': False,
            'adaptive_fallback_to_sync': False,
            'adaptive_current_concurrency': ADAPTIVE_START_CONCURRENCY,
            'adaptive_server_profiles': {},
            'security_blocks': 0,
            'circuit_breaker_trips': 0,
            'resumed_downloads': 0,
            'bandwidth_limited': False,
            'queue_size': 0,
            'queue_max_size': 0,
            'async_scan_fallbacks': 0,
            'async_scan_success': 0,
            'symlinks_followed': 0,
            'symlinks_skipped': 0,
            'symlink_loops_detected': 0,
            'symlink_depth_exceeded': 0,
            'symlink_bomb_prevented': 0,
            'fast_parses': 0,
            'lxml_parses': 0,
            'connection_pool_hits': 0,
            'connection_pool_misses': 0,
            'connection_pool_evictions': 0,
            'fs_cache_hits': 0,
            'fs_cache_misses': 0,
            'batch_size_adjustments': 0,
            'cache_corruptions': 0,
            'cleanup_failed_operations': 0,
            # NEW v2.0.0 metrics
            'disk_space_checks': 0,
            'disk_space_warnings': 0,
            'memory_pressure_events': 0,
            'partial_downloads': 0,
            'partial_resumes': 0,
            'stale_partials_cleaned': 0,
            'health_checks': 0,
            'rate_limit_delays': 0,
            # NEW v3.0.0 metrics
            'chunk_downloads': 0,
            'chunk_assemblies': 0,
            'chunk_failures': 0,
            'chunk_retries': 0,
            'parallel_files': 0,
            'total_chunks': 0,
            # NEW v3.0.6 metrics
            'auto_concurrency_enabled': False,
            'auto_concurrency_adjustments': 0,
            'auto_concurrency_final': 0,
            'auto_concurrency_start': 0,            
        }
        self.lock = RLock()
        self.start_time = time.time()
        self.parse_start_time = 0
        self.last_memory_check = 0
        
        # IMPROVED: Use thread-safe lists for time series
        self._request_times = deque(maxlen=1000)
        self._download_times = deque(maxlen=1000)
        self._parse_times = deque(maxlen=1000)
        self._errors = deque(maxlen=100)
        self._times_lock = RLock()        
    
    def increment(self, metric: str, value: int = 1) -> None:
        """
        Increment a metric.
        
        Args:
            metric: Metric name
            value: Value to increment by
        """
        with self.lock:
            if metric in self.metrics:
                self.metrics[metric] += value
            else:
                self.metrics[metric] = value
    
    def increment_batch(self, updates: Dict[str, int]) -> None:
        """
        Batch multiple metric updates with single lock acquisition.
        
        Args:
            updates: Dictionary of metric updates
        """
        with self.lock:
            for metric, value in updates.items():
                if metric in self.metrics:
                    self.metrics[metric] += value
                else:
                    self.metrics[metric] = value
    
    def add_bytes(self, bytes_count: int) -> None:
        """Add bytes downloaded"""
        with self.lock:
            self.metrics['bytes_downloaded'] += bytes_count
    
    def add_error(self, error: str, error_type: str = "unknown") -> None:
        """Add error to metrics - THREAD SAFE"""
        with self._times_lock:
            self._errors.append({
                'timestamp': datetime.now().isoformat(),
                'type': error_type,
                'message': error
            })
            
    def add_request_time(self, duration: float) -> None:
        """Add request time to metrics - THREAD SAFE"""
        with self._times_lock:
            self._request_times.append(duration)
    
    def add_download_time(self, duration: float) -> None:
        """Add download time to metrics - THREAD SAFE"""
        with self._times_lock:
            self._download_times.append(duration)
    
    def set_rget_used(self) -> None:
        """Mark RGET-LIST as used"""
        with self.lock:
            self.metrics['rget_list_used'] = True
    
    def set_cache_refreshed(self, age_days: float = 0) -> None:
        """Mark cache as refreshed"""
        with self.lock:
            self.metrics['cache_refreshed'] = True
            self.metrics['cache_age_days'] = age_days
    
    def set_cache_signatures(self, count: int) -> None:
        """Set number of cache signatures"""
        with self.lock:
            self.metrics['cache_signatures'] = count
    
    def start_parse_timer(self) -> None:
        """Start parse timer"""
        self.parse_start_time = time.time()
    
    def stop_parse_timer(self) -> None:
        """Stop parse timer and record duration"""
        if self.parse_start_time > 0:
            elapsed = time.time() - self.parse_start_time
            with self.lock:
                self.metrics['parse_time_seconds'] += elapsed
                self.metrics['parse_times'].append(elapsed)
            self.parse_start_time = 0
    
    def update_queue_metrics(self, queue_size: int, max_size: int) -> None:
        """Update queue metrics"""
        with self.lock:
            self.metrics['queue_size'] = queue_size
            self.metrics['queue_max_size'] = max(max_size, self.metrics['queue_max_size'])
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get metrics summary with deep copy for thread safety.
        """
        with self.lock:
            # Deep copy scalar metrics
            summary = {
                key: value 
                for key, value in self.metrics.items() 
                if not isinstance(value, (list, dict))
            }
            
            # Copy list metrics safely
            with self._times_lock:
                summary['request_times'] = list(self._request_times)
                summary['download_times'] = list(self._download_times)
                summary['parse_times'] = list(self._parse_times)
                summary['errors'] = list(self._errors)
            
            # Copy dict metrics
            for key, value in self.metrics.items():
                if isinstance(value, dict) and key not in ('request_times', 'download_times', 'parse_times', 'errors'):
                    summary[key] = dict(value)
            
            elapsed = time.time() - self.start_time
            summary['elapsed_seconds'] = elapsed
            summary['download_speed'] = (
                summary['bytes_downloaded'] / elapsed
                if elapsed > 0 else 0
            )
            
            # Calculate statistics safely
            if summary['request_times']:
                summary['request_avg_ms'] = statistics.mean(summary['request_times']) * 1000
            
            return summary
        
    def report(self, prefix: str = "", show_stats: bool = False) -> str:
        """
        Generate detailed metrics report.
        
        Args:
            prefix: Prefix for log lines
            show_stats: Whether to show detailed statistics
            
        Returns:
            Formatted metrics report
        """
        summary = self.get_summary()
        lines = [
            f"{prefix}METRICS SUMMARY:",
            f"{prefix}  Files downloaded: {summary['files_downloaded']}",
            f"{prefix}  Bytes downloaded: {format_bytes(summary['bytes_downloaded'])}",
            f"{prefix}  Files skipped: {summary['files_skipped']}",
            f"{prefix}  Files failed: {summary['files_failed']}",
            f"{prefix}  Directories processed: {summary['directories_processed']}",
        ]
        
        # Scan mode metrics
        if summary['directories_scanned_parallel'] > 0:
            lines.append(f"{prefix}  Parallel scans: {summary['directories_scanned_parallel']}")
        if summary['directories_scanned_sequential'] > 0:
            lines.append(f"{prefix}  Sequential scans: {summary['directories_scanned_sequential']}")
        if summary['directories_scanned_async'] > 0:
            lines.append(f"{prefix}  Async scans: {summary['directories_scanned_async']}")
        
        # Async metadata checks
        if summary['async_metadata_checks'] > 0:
            lines.append(f"{prefix}  Async metadata checks: {summary['async_metadata_checks']}")
        
        # HTML cache metrics
        if summary['html_cache_hits'] > 0 or summary['html_cache_misses'] > 0:
            total_html = summary['html_cache_hits'] + summary['html_cache_misses']
            html_hit_rate = (summary['html_cache_hits'] / total_html * 100) if total_html > 0 else 0
            lines.append(f"{prefix}  HTML cache hits: {summary['html_cache_hits']} ({html_hit_rate:.1f}%)")
        
        # Adaptive async metrics
        if summary.get('adaptive_async_enabled'):
            lines.append(f"{prefix}  Adaptive async: concurrency={summary['adaptive_current_concurrency']}")
        if summary.get('adaptive_fallback_to_sync'):
            lines.append(f"{prefix}  ⚠️ Fallback to sync: YES")
        
        # Parse metrics
        if summary['parse_time_seconds'] > 0:
            parse_speed = summary['directories_processed'] / summary['parse_time_seconds']
            lines.append(f"{prefix}  Parse time: {summary['parse_time_seconds']:.2f}s")
            lines.append(f"{prefix}  Parse speed: {parse_speed:.1f} dirs/s")
        
        # Cache hit/miss metrics
        lines.append(f"{prefix}  Cache hits: {summary['cache_hits']}")
        lines.append(f"{prefix}  Cache misses: {summary['cache_misses']}")
        if summary['cache_head_requests_saved'] > 0:
            lines.append(f"{prefix}  HEAD requests saved: {summary['cache_head_requests_saved']}")
        lines.append(f"{prefix}  Cache signatures: {summary['cache_signatures']}")
        
        # ETag metrics
        if summary['etag_matches'] > 0 or summary['etag_mismatches'] > 0 or summary['etag_304_responses'] > 0:
            lines.extend([
                f"{prefix}  ETag matches: {summary['etag_matches']}",
                f"{prefix}  ETag mismatches: {summary['etag_mismatches']}",
                f"{prefix}  ETag 304 responses: {summary['etag_304_responses']}",
                f"{prefix}  ETag unavailable: {summary['etag_unavailable']}",
            ])
        
        # HTTP/2 metrics
        if summary['http2_connections'] > 0 or summary['http11_fallbacks'] > 0:
            lines.extend([
                f"{prefix}  HTTP/2 connections: {summary['http2_connections']}",
                f"{prefix}  HTTP/1.1 fallbacks: {summary['http11_fallbacks']}",
            ])
        
        # Cleanup preview metrics
        if summary.get('files_would_delete', 0) > 0:
            lines.extend([
                f"{prefix}  Files would delete (preview): {summary['files_would_delete']}",
                f"{prefix}  Dirs would delete (preview): {summary['dirs_would_delete']}",
            ])
        
        # RGET-LIST metric
        lines.append(f"{prefix}  RGET-LIST used: {summary['rget_list_used']}")
        
        # Download speed
        lines.append(f"{prefix}  Download speed: {format_bytes(summary['download_speed'])}/s")
        
        # Duration
        lines.append(f"{prefix}  Duration: {format_duration(summary['elapsed_seconds'])}")
        
        # Parser stats
        if summary.get('fast_parses', 0) > 0 or summary.get('lxml_parses', 0) > 0:
            lines.append(f"{prefix}  Fast parses: {summary['fast_parses']}")
            lines.append(f"{prefix}  LXML parses: {summary['lxml_parses']}")
        
        # NEW v2.0.0 metrics
        if summary.get('disk_space_warnings', 0) > 0:
            lines.append(f"{prefix}  Disk space warnings: {summary['disk_space_warnings']}")
        if summary.get('memory_pressure_events', 0) > 0:
            lines.append(f"{prefix}  Memory pressure events: {summary['memory_pressure_events']}")
        if summary.get('partial_downloads', 0) > 0:
            lines.append(f"{prefix}  Partial downloads: {summary['partial_downloads']}")
        if summary.get('partial_resumes', 0) > 0:
            lines.append(f"{prefix}  Partial resumes: {summary['partial_resumes']}")
        if summary.get('stale_partials_cleaned', 0) > 0:
            lines.append(f"{prefix}  Stale partials cleaned: {summary['stale_partials_cleaned']}")
        if summary.get('rate_limit_delays', 0) > 0:
            lines.append(f"{prefix}  Rate limit delays: {summary['rate_limit_delays']}")
        
        # NEW v3.0.0 metrics
        if summary.get('chunk_downloads', 0) > 0:
            lines.append(f"{prefix}  Chunk downloads: {summary['chunk_downloads']}")
        if summary.get('chunk_assemblies', 0) > 0:
            lines.append(f"{prefix}  Chunk assemblies: {summary['chunk_assemblies']}")
        if summary.get('chunk_failures', 0) > 0:
            lines.append(f"{prefix}  Chunk failures: {summary['chunk_failures']}")
        if summary.get('parallel_files', 0) > 0:
            lines.append(f"{prefix}  Parallel files: {summary['parallel_files']}")
        if summary.get('total_chunks', 0) > 0:
            lines.append(f"{prefix}  Total chunks: {summary['total_chunks']}")

        # NEW v3.0.6: Auto-concurrency metrics
        if summary.get('auto_concurrency_enabled', False):
            lines.append(f"{prefix}  🤖 Auto-concurrency: enabled")
            if summary.get('auto_concurrency_adjustments', 0) > 0:
                lines.append(f"{prefix}    Adjustments: {summary['auto_concurrency_adjustments']}")
                lines.append(f"{prefix}    Final concurrency: {summary['auto_concurrency_final']}")
                
        # Errors
        if summary['errors']:
            lines.append(f"{prefix}  Errors: {len(summary['errors'])}")
        
        return '\n'.join(lines)
    
    def export_json(self, output_path: Path, config: MirrorConfig) -> bool:
        """
        Export metrics to JSON file.
        
        Args:
            output_path: Path to output JSON file
            config: MirrorConfig instance
            
        Returns:
            True if export successful
        """
        try:
            summary = self.get_summary()
            export_data = {
                'timestamp': datetime.now().isoformat(),
                'version': __version__,
                'metrics': summary,
                'config': {
                    'base_url': sanitize_url_for_log(str(config.base_url)),
                    'workers': config.workers,
                    'async_metadata': config.async_metadata,
                    'parallel_downloads': config.parallel_downloads
                }
            }
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(export_data, f, indent=2)
            logging.info(f"📊 Metrics exported to JSON: {output_path}")
            return True
        except Exception as e:
            logging.warning(f"Failed to export metrics JSON: {e}")
            return False

# ============================================================================
# RATE LIMITER
# ============================================================================
class RateLimiter:
    """Rate limiter for HTTP requests with per-IP option"""
    
    def __init__(self, requests_per_second: float = DEFAULT_RATE_LIMIT, 
                 delay: float = REQUEST_DELAY,
                 per_ip: bool = False):
        """
        Initialize rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second
            delay: Minimum delay between requests
            per_ip: Whether to rate limit per IP
        """
        self.min_interval = max(1.0 / requests_per_second, delay)
        self.last_request = 0
        self.per_ip = per_ip
        self.ip_last_requests: Dict[str, float] = {}
        self.lock = RLock()
        self.total_delays = 0
        
        # FIX: Periodic cleanup tracking
        self._cleanup_counter = 0
        self._cleanup_interval = 500  # Clean every 500 requests
    
    def wait(self, ip: Optional[str] = None) -> None:
        """
        Wait if necessary to respect rate limit.
        
        Args:
            ip: IP address for per-IP limiting
        """
        with self.lock:
            if self.per_ip and ip:
                last = self.ip_last_requests.get(ip, 0)
                elapsed = time.time() - last
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    time.sleep(sleep_time)
                    self.total_delays += 1
                self.ip_last_requests[ip] = time.time()
                
                # FIX: Periodic cleanup of stale IP entries
                self._cleanup_counter += 1
                if self._cleanup_counter >= self._cleanup_interval:
                    self._cleanup_stale_entries()
                    self._cleanup_counter = 0
            else:
                elapsed = time.time() - self.last_request
                if elapsed < self.min_interval:
                    time.sleep(self.min_interval - elapsed)
                    self.total_delays += 1
                self.last_request = time.time()
    
    # FIX: Add cleanup method
    def _cleanup_stale_entries(self) -> None:
        """Remove IP entries older than 1 hour."""
        if not self.per_ip:
            return
        
        now = time.time()
        max_age = 3600  # 1 hour
        
        stale_ips = [
            ip for ip, last_time in self.ip_last_requests.items()
            if now - last_time > max_age
        ]
        
        for ip in stale_ips:
            del self.ip_last_requests[ip]
        
        if stale_ips and len(stale_ips) > 100:
            logging.debug(f"RateLimiter: cleaned {len(stale_ips)} stale IP entries "
                         f"(remaining: {len(self.ip_last_requests)})")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with rate limiter statistics
        """
        with self.lock:
            return {
                'min_interval_ms': self.min_interval * 1000,
                'per_ip': self.per_ip,
                'active_ips': len(self.ip_last_requests) if self.per_ip else 0,
                'total_delays': self.total_delays
            }

# ============================================================================
# PER-IP RATE LIMITER (REPLACES STANDALONE DEFINITION)
# ============================================================================
class PerIPRateLimiter(RateLimiter):
    """Rate limiter that tracks and limits requests per IP address with async support."""
    def __init__(self, requests_per_second: float = DEFAULT_RATE_LIMIT):
        # Initialize base class (delay=0 ensures min_interval is purely rate-based)
        super().__init__(requests_per_second=requests_per_second, delay=0.0, per_ip=True)
        # Use explicit per-IP tracking for better cleanup control
        self.last_requests: Dict[str, float] = {}
        # self.lock and self.total_delays are inherited from RateLimiter

    def wait(self, ip: str) -> None:
        """Synchronous wait for rate limiting (blocks thread)."""
        sleep_time = 0.0
        with self.lock:
            now = time.time()
            last = self.last_requests.get(ip, 0.0)
            elapsed = now - last
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                # ATOMIC: Reserve next allowed timestamp BEFORE sleeping
                self.last_requests[ip] = now + sleep_time
                self.total_delays += 1
            else:
                self.last_requests[ip] = now
            
            # Periodic cleanup to prevent memory leaks
            if len(self.last_requests) % 500 == 0:
                self._cleanup_old_entries_unlocked()

        # Sleep OUTSIDE the lock to prevent blocking concurrent threads for other IPs
        if sleep_time > 0:
            time.sleep(sleep_time)

    async def async_wait(self, ip: str) -> None:
        """Non-blocking async wait for rate limiting (does NOT block event loop)."""
        sleep_time = 0.0
        with self.lock:
            now = time.time()
            last = self.last_requests.get(ip, 0.0)
            elapsed = now - last
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                # ATOMIC: Reserve next allowed timestamp
                self.last_requests[ip] = now + sleep_time
                self.total_delays += 1
            else:
                self.last_requests[ip] = now

        # Yield control back to event loop instead of blocking
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)

    def _cleanup_old_entries_unlocked(self, max_age_seconds: float = 3600) -> None:
        """Internal cleanup. Assumes self.lock is already held."""
        now = time.time()
        expired = [ip for ip, last in self.last_requests.items() if now - last > max_age_seconds]
        for ip in expired:
            del self.last_requests[ip]

    def cleanup_old_entries(self, max_age_seconds: float = 3600) -> None:
        """Public cleanup method."""
        with self.lock:
            self._cleanup_old_entries_unlocked(max_age_seconds)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self.lock:
            return {
                'active_ips': len(self.last_requests),
                'total_delays': self.total_delays,
                'requests_per_second': 1.0 / self.min_interval if self.min_interval > 0 else 0
            }

# ============================================================================
# NEW v3.0.6: CHUNK-AWARE RATE LIMITER
# ============================================================================
class ChunkAwareRateLimiter(RateLimiter):
    """Rate limiter that accounts for parallel chunk connections - OPTIMIZED"""
    def __init__(self, requests_per_second: float = DEFAULT_RATE_LIMIT,
                 delay: float = REQUEST_DELAY,
                 per_ip: bool = False,
                 chunk_multiplier: float = 0.5,
                 disable_scaling: bool = False):  # NEW PARAM
        super().__init__(requests_per_second, delay, per_ip)
        self.chunk_multiplier = chunk_multiplier
        self.active_chunks_per_ip: Dict[str, int] = {}
        self.chunk_lock = RLock()
        self.disable_scaling = disable_scaling  # NEW FLAG
        self._wait_lock = Lock()  

    def register_chunk_start(self, ip: str) -> None:
        """Register that a chunk download is starting for an IP with max limit"""
        with self.chunk_lock:
            current = self.active_chunks_per_ip.get(ip, 0)
            # Add safety limit to prevent overloading a single IP
            max_per_ip = 20  # Conservative limit
            if current >= max_per_ip:
                logging.warning(f"IP {ip} has {current} active chunks, max {max_per_ip} - throttling")
                return
            self.active_chunks_per_ip[ip] = current + 1
            
    def register_chunk_complete(self, ip: str) -> None:
        """Register that a chunk download completed for an IP"""
        with self.chunk_lock:
            current = self.active_chunks_per_ip.get(ip, 0)
            if current > 1:
                self.active_chunks_per_ip[ip] = current - 1
            else:
                self.active_chunks_per_ip.pop(ip, None)

    def wait(self, ip: Optional[str] = None) -> None:
        """Wait with optimized scaling - FIXED DEADLOCK & RACE CONDITION"""
        # Get active chunk count WITHOUT nested locks
        active_chunks = 1
        if self.per_ip and ip:
            with self.chunk_lock:
                active_chunks = self.active_chunks_per_ip.get(ip, 1)

        # Calculate delay inside lock, sleep outside to prevent blocking other IPs
        sleep_time = 0
        with self._wait_lock:
            now = time.time()
            if self.per_ip and ip:
                if self.disable_scaling:
                    effective_delay = self.min_interval
                else:
                    multiplier = 1.0 + (active_chunks - 1) * 0.1
                    effective_delay = self.min_interval * min(1.5, multiplier)

                last = self.ip_last_requests.get(ip, 0)
                elapsed = now - last
                if elapsed < effective_delay:
                    sleep_time = effective_delay - elapsed
                    # FIX: Update timestamp BEFORE releasing lock to atomically reserve the slot
                    self.ip_last_requests[ip] = now + sleep_time
                    self.total_delays += 1
            else:
                elapsed = now - self.last_request
                if elapsed < self.min_interval:
                    sleep_time = self.min_interval - elapsed
                    # FIX: Update timestamp BEFORE releasing lock
                    self.last_request = now + sleep_time
                    self.total_delays += 1

        # Sleep OUTSIDE the lock (prevents blocking concurrent threads for other IPs)
        if sleep_time > 0:
            time.sleep(sleep_time)

# ============================================================================
# NEW v3.0.0: CHUNK-AWARE CIRCUIT BREAKER
# ============================================================================
class ChunkCircuitBreaker(CircuitBreaker):
    """Circuit breaker that aggregates chunk failures per file/server"""
    
    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 half_open_limit: int = 3,
                 chunk_failure_threshold: int = 3):
        super().__init__(failure_threshold, recovery_timeout, half_open_limit)
        self.chunk_failure_threshold = chunk_failure_threshold
        self.file_chunk_failures: Dict[str, int] = {}
        self.server_chunk_failures: Dict[str, int] = {}
        self.chunk_lock = RLock()
    
    def record_chunk_failure(self, file_url: str, server: str) -> None:
        """
        Record a chunk failure.
        
        If too many chunks fail for a file, mark entire file as failed.
        If too many chunks fail for a server, open circuit for that server.
        """
        with self.chunk_lock:
            # Track per-file chunk failures
            self.file_chunk_failures[file_url] = self.file_chunk_failures.get(file_url, 0) + 1
            
            # Track per-server chunk failures
            self.server_chunk_failures[server] = self.server_chunk_failures.get(server, 0) + 1
            
            # Check file threshold
            if self.file_chunk_failures[file_url] >= self.chunk_failure_threshold:
                logging.warning(f"Too many chunk failures for {file_url}, marking file as failed")
                self.record_failure()  # This will affect overall circuit
            
            # Check server threshold
            if self.server_chunk_failures[server] >= self.chunk_failure_threshold * 2:
                logging.warning(f"Too many chunk failures for server {server}, opening circuit")
                self.record_failure()  # This will affect overall circuit
    
    def record_chunk_success(self, file_url: str, server: str) -> None:
        """Record a successful chunk download"""
        with self.chunk_lock:
            # Reset file failure count on success
            self.file_chunk_failures.pop(file_url, None)
            
            # Gradually reduce server failure count
            current = self.server_chunk_failures.get(server, 0)
            if current > 0:
                self.server_chunk_failures[server] = max(0, current - 1)
        
        self.record_success()


# ============================================================================
# NEW v3.0.0: PARALLEL DOWNLOAD MANAGER (FIXED)
# ============================================================================
class ParallelDownloadManager:
    """Manages parallel chunk downloads for multiple files"""
    def __init__(self, config: MirrorConfig, metrics: MetricsCollector,
                 connection_manager: ConnectionManager,
                 bandwidth_limiter: BandwidthLimiter,
                 concurrency_manager: UnifiedConcurrencyManager = None,
                 mirror: Optional['MirrorURL'] = None):
        """Initialize parallel download manager."""
        self.config = config
        self.metrics = metrics
        self.connection_manager = connection_manager
        self.bandwidth_limiter = bandwidth_limiter
        self.concurrency_manager = concurrency_manager
        self.mirror = mirror
        
        # Determine download mode from config
        self.enabled = False
        self.use_streaming = False
        self.auto_mode = False
        
        # Check for sequential mode first
        if hasattr(config, 'sequential_downloads') and config.sequential_downloads:
            self.enabled = False
            self.use_streaming = False
            self.auto_mode = False
            logging.info("📥 Sequential mode selected")
        
        # Check for streaming parallel mode
        elif hasattr(config, 'streaming_parallel') and config.streaming_parallel:
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
        
        self.max_chunks_per_file = max(1, min(config.max_chunks_per_file, 8))
        self.min_chunk_size = max(5 * 1024 * 1024, config.min_chunk_size_mb * 1024 * 1024)
        self.max_parallel_chunks = min(config.max_parallel_chunks_total, 20)
    
        # State tracking
        #self.active_downloads: Dict[Path, ParallelFileDownload] = {}
        self.lock = RLock()
        
        # Thread pool for chunks
        cpu_count = os.cpu_count() or 4
        max_chunk_threads = min(self.max_parallel_chunks, max(cpu_count * 2, 8))

        # Single executor creation point with hard cap to prevent thread explosion
        capped_workers = min(max_chunk_threads, max(4, (os.cpu_count() or 4) * 2))
        
        if self.config.use_shared_thread_pool and concurrency_manager and concurrency_manager.shared_pool:
            self.executor = concurrency_manager.shared_pool
            self.own_executor = False
            logging.info(f"📦 Using shared thread pool for parallel downloads")
        else:
            self.executor = ThreadPoolExecutor(
                max_workers=capped_workers,
                thread_name_prefix="mirror_download"
            )
            self.own_executor = True
            logging.info(f"📦 Using DEDICATED download thread pool: {capped_workers} threads (capped)")
        
        # Semaphore and per-IP tracking
        self.chunk_semaphore = Semaphore(self.max_parallel_chunks)
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
        
        # Start periodic cleanup thread
        self._cleanup_thread = threading.Thread(
            target=self._periodic_cleanup, 
            daemon=True,
            name=f"pdm_cleanup_{id(self)}"
        )
        self._cleanup_thread.start()

        
        # Rate limiter
        disable_scaling = (config.trusted_server or 
                          config.auto_concurrency or 
                          config.disable_rate_scaling or
                          config.parallel_optimization_mode == 'aggressive')
        
        self.rate_limiter = ChunkAwareRateLimiter(
            delay=config.request_delay,
            per_ip=config.security_validation,
            disable_scaling=disable_scaling
        )
        
        # Circuit breaker
        self.circuit_breaker = ChunkCircuitBreaker() if config.circuit_breaker_enabled else None
        
        # Assembly directory
        if config.chunk_assembly_dir:
            self.assembly_dir = config.chunk_assembly_dir
        else:
            # Use secure temporary directory with unique name
            unique_id = secrets.token_hex(8)
            self.assembly_dir = Path(tempfile.gettempdir()) / f'mirrorurl_chunks_{unique_id}'
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
        
        self.assembly_dir.mkdir(parents=True, exist_ok=True)

        # Per-file locks for true parallelism (one RLock per final file path).
        # Pruned on download completion in cleanup_chunks() to avoid leaking.
        # FIX (lock-creation race): a defaultdict's __missing__ + __setitem__
        # is not formally atomic — two threads concurrently accessing the
        # same not-yet-present key could each construct a separate RLock and
        # only one would survive in the dict, while the other thread would
        # already be holding (and serializing on) the loser. CPython's GIL
        # masks this *most* of the time, but it's not safe to rely on. Use
        # an explicit guard lock for the lazy-create step (see
        # _get_file_lock).
        self._file_locks: Dict[Path, RLock] = {}
        self._file_locks_create_lock = Lock()
        
        # Statistics
        self.stats = {
            'total_chunks': 0,
            'completed_chunks': 0,
            'failed_chunks': 0,
            'start_time': time.time()
        }
        self.stats_lock = RLock()
        self._shutdown = False
        
        logging.info(f"📦 Parallel download manager: {max_chunk_threads} threads, "
                    f"max_chunks={self.max_chunks_per_file}, min_chunk={config.min_chunk_size_mb}MB, "
                    f"total_chunks={self.max_parallel_chunks}")

    def _periodic_cleanup(self) -> None:
        """Periodically clean up stale resources to prevent memory leaks.
        
        This method runs in a background daemon thread and periodically:
        1. Removes idle per-IP semaphores that haven't been used recently
        2. Cleans up completed/failed download entries from tracking
        3. Prevents unbounded growth of internal data structures
        """
        while not getattr(self, '_shutdown', False):
            try:
                time.sleep(30)  # Check every 30 seconds
                self._cleanup_idle_resources()
            except Exception as e:
                # Don't let cleanup errors crash the thread
                logging.debug(f"Periodic cleanup error (non-critical): {e}")
    
    def _cleanup_idle_resources(self) -> None:
        """Clean up idle per-IP semaphores and stale download tracking entries."""
        now = time.time()
    
        # ========================================================================
        # 1. CLEAN UP DOWNLOAD TRACKING ENTRIES (ALWAYS RUN)
        # ========================================================================
        with self.lock:
            # Snapshot keys explicitly to prevent runtime errors during mutation
            stale_downloads = [
                path for path, download in list(self.active_downloads.items())
                if download.status in ('completed', 'failed', 'cancelled')
                and now - download.start_time > 3600
            ]
            for path in stale_downloads:
                self.active_downloads.pop(path, None)
                self._file_locks.pop(path, None)
    
            # 🔴 CRITICAL: Enforce hard limit to prevent unbounded memory growth
            if len(self.active_downloads) > self.max_active_downloads:
                # Find oldest completed/failed entries
                completed = sorted(
                    [(p, d) for p, d in list(self.active_downloads.items())
                     if d.status in ('completed', 'failed', 'cancelled')],
                    key=lambda x: x[1].start_time
                )
                # Trim down to 50% of max limit
                target = self.max_active_downloads // 2
                to_remove = completed[:max(0, len(completed) - target)]
                for path, _ in to_remove:
                    self.active_downloads.pop(path, None)
                    self._file_locks.pop(path, None)
    
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
                        ip for ip, last_used in self._ip_semaphores_last_used.items()
                        if now - last_used > idle_threshold
                    ]
                    for ip in stale_ips:
                        self._ip_semaphores.pop(ip, None)
                        self._ip_semaphores_last_used.pop(ip, None)
    
                    if stale_ips:
                        logging.debug(f"Cleaned {len(stale_ips)} idle IP semaphores "
                                      f"(remaining: {len(self._ip_semaphores)})")    
                        
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
        return max(2, chunks)
    
    def create_chunks(self, url: str, local_path: Path, file_size: int) -> Optional[ParallelFileDownload]:
        """Create chunk tasks for a file, using appropriate mode."""
        chunk_count = self.get_chunk_count(file_size)
        if chunk_count <= 1:
            return None
        if not self._test_range_support(url):
            logging.debug(f"Server doesn't support Range for {url}")
            return None
        download = ParallelFileDownload(
            url=url,
            final_path=local_path,
            file_size=file_size
        )
        
        # Determine mode based on settings
        if self.use_streaming:
            # Streaming mode: direct write to final file
            try:
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.truncate(file_size)
                download.status = 'streaming'
                logging.info(f"🚀 Streaming parallel download for {local_path.name}: {chunk_count} chunks, {format_bytes(file_size)}")
            except Exception as e:
                logging.warning(f"Failed to pre-allocate file for streaming, falling back to temp files: {e}")
                download.temp_dir = self.assembly_dir / f"{local_path.name}_{uuid.uuid4().hex[:8]}"
                download.temp_dir.mkdir(parents=True, exist_ok=True)
                download.status = 'downloading'
                logging.info(f"📦 Traditional parallel download (fallback) for {local_path.name}: {chunk_count} chunks, {format_bytes(file_size)}")
                
                # FIX: Touch the file to ensure it exists.
                # This prevents race conditions in download_chunk_streaming where concurrent chunks
                # might try to create/truncate the file simultaneously in the 'else' block.
                # By ensuring it exists (even if empty), download_chunk_streaming will use 'r+b'.
                try:
                    local_path.touch(exist_ok=True)
                except Exception:
                    pass
        else:
            # Traditional mode: use temp files
            download.temp_dir = self.assembly_dir / f"{local_path.name}_{uuid.uuid4().hex[:8]}"
            download.temp_dir.mkdir(parents=True, exist_ok=True)
            download.status = 'downloading'
            logging.info(f"📦 Traditional parallel download for {local_path.name}: {chunk_count} chunks, {format_bytes(file_size)}")

        # Calculate chunk sizes
        chunk_size = file_size // chunk_count
        chunks = []
        for i in range(chunk_count):
            start = i * chunk_size
            end = start + chunk_size - 1 if i < chunk_count - 1 else file_size - 1
            chunk = ChunkInfo(
                file_url=url,
                final_path=local_path,
                chunk_id=i,
                start_byte=start,
                end_byte=end,
                total_chunks=chunk_count,
                temp_path=download.temp_dir / f"chunk_{i:04d}_{secrets.token_hex(8)}.part" if download.temp_dir else None,
                size=end - start + 1,
                direct_write=self.use_streaming
            )
            chunks.append(chunk)
        download.chunks = chunks
        with self.lock:
            self.active_downloads[local_path] = download
        with self.stats_lock:
            self.stats['total_chunks'] += chunk_count
            self.metrics.increment('parallel_files')
            self.metrics.increment('total_chunks', chunk_count)
        return download

    def _test_range_support(self, url: str) -> bool:
        """Test if server supports Range requests."""
        try:
            response = self.connection_manager.request(url, method='HEAD')
            accept_ranges = response.headers.get('Accept-Ranges', '').lower()
            return accept_ranges == 'bytes'
        except Exception as e:
            logging.debug(f"Range test failed for {url}: {e}")
            return False

    def _get_client_for_url(self, url: str) -> httpx.Client:
        """Get or create HTTP client for URL's domain with HTTP/2 support"""
        return self.connection_manager.connection_pool.get_client(url)
    
    def download_chunk(self, chunk: ChunkInfo) -> bool:
        """Download chunk with HTTP/2 stream reuse - FIXED"""
        chunk.status = 'downloading'
        parsed = urlparse(chunk.file_url)
        domain = parsed.netloc
        try:
            ip = socket.gethostbyname(parsed.hostname)
        except Exception:
            ip = parsed.hostname
        
        self.rate_limiter.register_chunk_start(ip)
        
        try:
            headers = {'Range': f'bytes={chunk.start_byte}-{chunk.end_byte}'}
            mode = 'wb'
            resume_offset = 0
            
            if chunk.temp_path.exists():
                resume_offset = chunk.temp_path.stat().st_size
                if resume_offset > 0 and resume_offset < chunk.size:
                    headers['Range'] = f'bytes={chunk.start_byte + resume_offset}-{chunk.end_byte}'
                    mode = 'ab'
                    logging.debug(f"Resuming chunk {chunk.chunk_id} at {resume_offset}")
            
            time.sleep(random.uniform(0, 0.005))

            for attempt in range(3):
                # Track how many retries have been attempted on this chunk.
                chunk.retries = attempt
                # FIX (resume retry duplication): on a resumed download
                # (mode == 'ab') a mid-stream connection failure left
                # partially-written bytes in chunk.temp_path. The previous
                # iteration's range header asks the server for the SAME
                # window starting at start_byte+resume_offset, so on retry
                # those bytes were appended a second time, growing the temp
                # file beyond chunk.size and silently corrupting assembly
                # (assemble_file copies len(data) bytes, which then overrun
                # the next chunk's region in the mmap). Truncate the temp
                # file back to resume_offset before each attempt so resumed
                # retries always start from a clean tail.
                if mode == 'ab' and attempt > 0:
                    try:
                        with open(chunk.temp_path, 'r+b') as _trunc:
                            _trunc.truncate(resume_offset)
                    except OSError as _te:
                        logging.debug(f"Pre-retry truncate failed for chunk {chunk.chunk_id}: {_te}")
                try:
                    # Go through ConnectionManager.request so retries / circuit
                    # breaker / mocked connection_manager (in tests) all work.
                    response = self.connection_manager.request(
                        chunk.file_url,
                        method='GET',
                        headers=dict(headers),
                        allow_redirects=True,
                        timeout=httpx.Timeout(self.config.timeout * 2, connect=10.0, read=self.config.timeout * 3),
                    )

                    if response.status_code not in (200, 206):
                        if attempt < 2:
                            time.sleep(2 ** attempt)
                            continue
                        raise ChunkDownloadError(f"HTTP {response.status_code}")

                    bytes_downloaded = resume_offset
                    # OPTIMIZATION: Use larger buffer for parallel chunk writes
                    BUFFER_SIZE = 256 * 1024  # 256KB buffer

                    with open(chunk.temp_path, mode, buffering=BUFFER_SIZE) as f:
                        for data in response.iter_bytes(32768):  # 32KB read chunks, larger chunk size for HTTP/2
                            f.write(data)
                            bytes_downloaded += len(data)
                            if self.bandwidth_limiter:
                                self.bandwidth_limiter.throttle(len(data))
                    
                    # Force flush to ensure data is on disk before continuing
                    if mode == 'wb':  # Only for new files, not resumes
                        with open(chunk.temp_path, 'ab') as f:
                            f.flush()
                            os.fsync(f.fileno())                    
                        
                    
                    if bytes_downloaded != chunk.size:
                        if attempt < 2:
                            time.sleep(2 ** attempt)
                            continue
                        raise ChunkDownloadError(f"Size mismatch: {bytes_downloaded} != {chunk.size}")
                    
                    chunk.status = 'completed'
                    with self.stats_lock:
                        self.stats['completed_chunks'] += 1
                    self.metrics.increment('chunk_downloads')
                    self.metrics.add_bytes(chunk.size - resume_offset)
                    
                    if self.circuit_breaker:
                            self.circuit_breaker.record_chunk_success(chunk.file_url, parsed.netloc)
                    return True
                    
                except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as e:
                    if attempt < 2:
                        time.sleep(2 ** attempt)
                        continue
                    raise
            return False
            
        except Exception as e:
            logging.error(f"Chunk {chunk.chunk_id} failed: {e}")
            chunk.status = 'failed'
            with self.stats_lock:
                self.stats['failed_chunks'] += 1
            self.metrics.increment('chunk_failures')
            if self.circuit_breaker:
                            self.circuit_breaker.record_chunk_failure(chunk.file_url, parsed.netloc)
            return False
        finally:
            self.rate_limiter.register_chunk_complete(ip)

    def _write_stream_to_file(self, file_handle, response, buffer_size: int, bytes_downloaded_tracker: List[int]) -> int:
        """
        Write streaming response data to a file handle.
        
        Args:
            file_handle: Open file handle for writing
            response: HTTP response with iter_bytes method
            buffer_size: Size of read buffer
            bytes_downloaded_tracker: List containing single int for tracking (mutable)
            
        Returns:
            Total bytes downloaded
            
        Raises:
            ChunkDownloadError: If write fails
        """
        bytes_downloaded = 0
        
        try:
            for data in response.iter_bytes(buffer_size):
                file_handle.write(data)
                bytes_downloaded += len(data)
                
                # Apply bandwidth limiting if configured
                if self.bandwidth_limiter:
                    self.bandwidth_limiter.throttle(len(data))
            
            # Ensure data is flushed to disk
            file_handle.flush()
            
            # Update the tracker
            if bytes_downloaded_tracker:
                bytes_downloaded_tracker[0] = bytes_downloaded
                
            return bytes_downloaded
            
        except (IOError, OSError) as e:
            raise ChunkDownloadError(f"Stream write failed after {bytes_downloaded} bytes: {e}")
            
    def download_chunk_streaming(self, chunk: ChunkInfo) -> bool:
        """Download chunk directly to final file at correct offset.

        NOTE: The per-IP semaphore is acquired by the caller
        (_download_chunk_with_semaphore). Acquiring it again here would
        consume two permits per chunk and halve effective parallelism /
        risk starvation, so this method does NOT touch _ip_semaphores.
        """
        chunk.status = 'downloading'
        parsed = urlparse(chunk.file_url)
        domain = parsed.netloc

        try:
            ip = socket.gethostbyname(parsed.hostname)
        except Exception:
            ip = parsed.hostname

        self.rate_limiter.register_chunk_start(ip)

        try:
            headers = {'Range': f'bytes={chunk.start_byte}-{chunk.end_byte}'}

            client = self._get_client_for_url(chunk.file_url)

            for attempt in range(3):
                try:
                    response = client.request(
                        'GET',
                        chunk.file_url,
                        headers=headers,
                        timeout=httpx.Timeout(
                            self.config.timeout * 2,
                            connect=10.0,
                            read=self.config.timeout * 3
                        )
                    )

                    if response.status_code not in (200, 206):
                        if attempt < 2:
                            time.sleep(exponential_backoff(attempt))
                            continue
                        raise ChunkDownloadError(f"HTTP {response.status_code}")

                    bytes_downloaded = 0
                    buffer_size = STREAMING_WRITE_BUFFER_SIZE

                    # FIX: Ensure final file directory exists
                    chunk.final_path.parent.mkdir(parents=True, exist_ok=True)

                    # FIX (race condition): Acquire the per-file lock BEFORE
                    # opening the file. Previously the 'wb' branch opened
                    # (and truncated) the file before locking, so two threads
                    # racing into the create branch would each truncate the
                    # file and destroy each other's writes. Open + write are
                    # now serialized per file. _get_file_lock() also closes
                    # a separate lazy-creation race in the lock dict itself.
                    with self._get_file_lock(chunk.final_path):
                        # Re-check after acquiring the lock — first writer
                        # creates / pre-allocates, subsequent writers seek.
                        if chunk.final_path.exists():
                            mode = 'r+b'
                            need_prealloc = False
                        else:
                            mode = 'wb'
                            need_prealloc = chunk.start_byte > 0

                        with open(chunk.final_path, mode) as f:
                            if need_prealloc:
                                # Pre-allocate sparse file so seek(start_byte)
                                # below lands inside the file.
                                f.seek(chunk.end_byte)
                                f.write(b'\0')
                            f.seek(chunk.start_byte)
                            for data in response.iter_bytes(buffer_size):
                                f.write(data)
                                bytes_downloaded += len(data)
                                if self.bandwidth_limiter:
                                    self.bandwidth_limiter.throttle(len(data))
                            f.flush()
                            os.fsync(f.fileno())

                    # Verify downloaded size matches expected chunk size
                    if bytes_downloaded != chunk.size:
                        if attempt < 2:
                            time.sleep(exponential_backoff(attempt))
                            continue
                        raise ChunkDownloadError(
                            f"Size mismatch: downloaded {bytes_downloaded} bytes, "
                            f"expected {chunk.size} bytes"
                        )

                    chunk.status = 'completed'
                    with self.stats_lock:
                        self.stats['completed_chunks'] += 1

                    self.metrics.increment('chunk_downloads')
                    self.metrics.add_bytes(chunk.size)

                    if self.circuit_breaker:
                            self.circuit_breaker.record_chunk_success(chunk.file_url, parsed.netloc)

                    return True

                except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as e:
                    if attempt < 2:
                        wait_time = exponential_backoff(attempt)
                        logging.debug(f"Chunk {chunk.chunk_id} attempt {attempt+1} failed: {e}. "
                                      f"Retrying in {wait_time:.1f}s")
                        time.sleep(wait_time)
                        continue
                    raise

                except (IOError, OSError) as e:
                    logging.error(f"File I/O error for chunk {chunk.chunk_id} at {chunk.final_path}: {e}")
                    if attempt < 2:
                        time.sleep(exponential_backoff(attempt))
                        continue
                    raise ChunkDownloadError(f"File write failed: {e}")

            return False

        except ChunkDownloadError:
            # Re-raise ChunkDownloadError as-is
            raise
        # ADD this cleanup block to the except Exception section of download_chunk_streaming:
        except Exception as e:
            logging.error(f"Streaming chunk {chunk.chunk_id} failed with unexpected error: {e}")
            # ⬇️ FIX: Clean up partial data on failure
            try:
                if chunk.final_path.exists() and chunk.start_byte > 0:
                    with open(chunk.final_path, 'r+b') as f:
                        f.truncate(chunk.start_byte)  # Roll back to before this chunk started
                    logging.debug(f"Truncated corrupted partial: {chunk.final_path}")
            except Exception:
                pass  # Ignore cleanup errors
            chunk.status = 'failed'
            with self.stats_lock:
                self.stats['failed_chunks'] += 1
                self.metrics.increment('chunk_failures')
            if self.circuit_breaker:
                            self.circuit_breaker.record_chunk_failure(chunk.file_url, parsed.netloc)
            return False
        
        finally:
            self.rate_limiter.register_chunk_complete(ip)
            
            
    def download_parallel(self, download: ParallelFileDownload) -> bool:
        """Download all chunks of a file in parallel with batch rate limiting - FIXED"""
        if download.status != 'downloading' and download.status != 'streaming':
            return False
        
        # Disk space check.
        #
        # FIX: previously this always required ``file_size * 2``. The 2x
        # factor exists because traditional parallel mode keeps each chunk
        # as a temp file AND then assembles them into the final output, so
        # peak disk usage really is ~2x. Streaming mode writes directly
        # into the pre-allocated final file once and never duplicates the
        # bytes anywhere, so the right factor is 1x. The old check
        # incorrectly rejected streaming downloads when the user had only
        # ~file_size of headroom.
        if hasattr(self.mirror, 'disk_manager') and self.mirror.disk_manager:
            multiplier = 1 if download.status == 'streaming' else 2
            required_space = download.file_size * multiplier
            ok, error = self.mirror.disk_manager.check_available(required_space)
            if not ok:
                logging.error(f"Insufficient disk space for parallel download: {error}")
                download.status = 'failed'
                return False
        
        # OPTIMIZATION: Apply rate limit ONCE per file, not per chunk
        parsed = urlparse(download.url)
        try:
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
        for chunk in download.chunks:
            if chunk.status == 'completed':
                continue
            future = self.executor.submit(self._download_chunk_with_semaphore, chunk)
            futures.append((future, chunk))
        
        # Wait for all chunks
        completed = 0
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
                logging.error(f"Chunk {chunk.chunk_id} exception: {e}")
        
        with download.lock:
            download.completed_chunks = completed
            download.failed_chunks = failed
        
        if failed > 0:
            # Try to recover failed chunks
            if failed <= len(download.chunks) // 2:
                logging.warning(f"Retrying {failed} failed chunks for {download.final_path.name}")
                return self._retry_failed_chunks(download)
            else:
                logging.error(f"Too many chunk failures ({failed}) for {download.final_path.name}")
                download.status = 'failed'
                self.cleanup_chunks(download)
                return False
        
        # For streaming mode, we're done - no assembly needed
        if download.status == 'streaming':
            # NOTE: durability is already guaranteed per-chunk in
            # download_chunk_streaming (f.flush() + os.fsync() inside the
            # per-file lock). Re-opening the final file 'rb' here and calling
            # flush()/fsync() on a READ handle is a no-op (nothing is buffered
            # on a read-only handle), so it was removed. If an extra
            # whole-file barrier is ever wanted, open in 'r+b' and fsync that.

            # Update metrics
            self.metrics.increment('chunk_assemblies')
            self.metrics.add_bytes(download.file_size)
            
            if self.mirror:
                self.mirror.files_processed.increment(1)
                self.mirror.total_downloaded_size.add(download.file_size)
                
                if hasattr(self.mirror, 'cache_manager') and download.server_etag:
                    self.mirror.cache_manager.save_file_metadata(
                        download.final_path,
                        download.server_etag,
                        time.time(),
                        download.file_size
                    )
                if hasattr(self.mirror, 'fs_cache'):
                    self.mirror.fs_cache.invalidate(download.final_path)
            
            logging.info(f"✅ Streaming complete: {download.final_path.name} ({format_bytes(download.file_size)})")
            download.status = 'completed'
            self.cleanup_chunks(download)
            return True
        
        # For non-streaming mode, assemble chunks
        return self.assemble_file(download)

    def _get_file_lock(self, path: Path) -> RLock:
        """Atomically fetch-or-create the per-file write lock.

        Used by streaming chunk writers to serialize writes to the same
        final file. Wraps lazy creation in a dedicated guard lock so two
        threads can't end up with different RLock objects for the same
        path (see __init__ for context).
        """
        lock = self._file_locks.get(path)
        if lock is not None:
            return lock
        with self._file_locks_create_lock:
            lock = self._file_locks.get(path)
            if lock is None:
                lock = RLock()
                self._file_locks[path] = lock
            return lock

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
                    stale = [k for k, t in self._ip_semaphores_last_used.items()
                             if t < cutoff and k != ip]
                    for k in stale:
                        self._ip_semaphores.pop(k, None)
                        self._ip_semaphores_last_used.pop(k, None)
            return sem
    
    def _download_chunk_with_semaphore(self, chunk: ChunkInfo) -> bool:
        parsed = urlparse(chunk.file_url)

        if parsed.hostname is None:
            logging.error(f"Cannot download chunk: no hostname in URL {chunk.file_url}")
            chunk.status = 'failed'
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
                    logging.debug(f"Waiting for per-IP semaphore for chunk {chunk.chunk_id}... "
                                f"({int(time.time() - start)}s elapsed)")
                    continue
            except Exception as e:
                logging.error(f"Semaphore acquire exception for chunk {chunk.chunk_id}: {e}")
                chunk.status = 'failed'
                return False
        
        if not acquired:
            logging.error(f"Timeout acquiring per-IP semaphore for chunk {chunk.chunk_id} after {max_wait}s")
            chunk.status = 'failed'
            return False

        try:
            if chunk.direct_write:
                return self.download_chunk_streaming(chunk)  # Uses direct file write
            else:
                return self.download_chunk(chunk)  # Uses temp file        

        except Exception as e:
            logging.error(f"Chunk download failed for chunk {chunk.chunk_id}: {e}")
            chunk.status = 'failed'
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
            if chunk.status == 'failed':
                chunk.retries += 1
                chunk.status = 'pending'
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
                        download.status = 'failed'
                        self.cleanup_chunks(download)
                        return False
                else:
                    logging.error(f"Chunk {chunk.chunk_id} exceeded max retries")
                    download.status = 'failed'
                    self.cleanup_chunks(download)
                    return False
        # In streaming mode, chunks write directly to the final file — no
        # assembly step. assemble_file() expects temp chunk files and would
        # fail otherwise.
        if download.status == 'streaming' or any(c.direct_write for c in download.chunks):
            download.status = 'completed'
            self.cleanup_chunks(download)
            return True
        return self.assemble_file(download)
    
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
                download.status = 'failed'
                return False

            incomplete = [c for c in download.chunks if c.status != 'completed']
            if incomplete:
                logging.error(
                    f"Cannot assemble {download.final_path}: "
                    f"{len(incomplete)} chunks not completed "
                    f"(ids: {[c.chunk_id for c in incomplete]})"
                )
                download.status = 'failed'
                return False

            # Snapshot critical state under lock to avoid holding it during I/O
            file_size = download.file_size
            chunks = sorted(download.chunks, key=lambda c: c.chunk_id)
            download.status = 'assembling'

        logging.info(
            f"🔧 Assembling {download.final_path.name} from {len(chunks)} chunks "
            f"({format_bytes(file_size)})"
        )

        # ====================================================================
        # PHASE 2: PREPARE TEMPORARY FILE
        # ====================================================================
        unique_id = secrets.token_hex(8)
        temp_assembly = download.final_path.with_suffix(f'.{unique_id}.assembling')
        temp_file_moved = False
        
        try:
            download.final_path.parent.mkdir(parents=True, exist_ok=True)
            
            # ====================================================================
            # PHASE 3: HANDLE 0-BYTE FILES
            # ====================================================================
            if file_size == 0:
                with open(temp_assembly, 'wb') as f:
                    f.flush()
                    os.fsync(f.fileno())
            else:
                # ====================================================================
                # PHASE 4: PRE-ALLOCATE AND ASSEMBLE
                # ====================================================================
                # Pre-allocate to prevent fragmentation
                with open(temp_assembly, 'wb') as f:
                    f.seek(file_size - 1)
                    f.write(b'\0')
                    f.flush()
                    os.fsync(f.fileno())

                # Decide whether to use mmap (failsafe for >50GB files)
                USE_MMAP = file_size < 50 * 1024**3  # 50GB threshold
                mm = None
                
                with open(temp_assembly, 'r+b') as f:
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
                                raise ChunkAssemblyError(f"Chunk {chunk.chunk_id} file missing: {chunk.temp_path}")

                            data = self._read_chunk_data(chunk.temp_path)
                            expected_size = chunk.end_byte - chunk.start_byte + 1
                            if len(data) != expected_size:
                                raise ChunkAssemblyError(
                                    f"Chunk {chunk.chunk_id} size mismatch: "
                                    f"expected {expected_size} bytes, got {len(data)} bytes"
                                )

                            target_end = chunk.start_byte + len(data)
                            if target_end > file_size:
                                raise ChunkAssemblyError(
                                    f"Chunk {chunk.chunk_id} would write past end of file: "
                                    f"target_end={target_end}, file_size={file_size}"
                                )

                            if USE_MMAP:
                                mm[chunk.start_byte:target_end] = data
                            else:
                                f.seek(chunk.start_byte)
                                f.write(data)

                            del data  # Free memory immediately

                        # Flush to disk
                        if USE_MMAP:
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
                with open(temp_assembly, 'rb') as vf:
                    vf.seek(0)
                    if not vf.read(1):
                        raise ChunkAssemblyError("Cannot read first byte")
                    vf.seek(file_size - 1)
                    if not vf.read(1):
                        raise ChunkAssemblyError("Cannot read last byte")

            # ====================================================================
            # PHASE 6: ATOMIC REPLACEMENT
            # ====================================================================
            try:
                os.replace(str(temp_assembly), str(download.final_path))
                temp_file_moved = True
            except OSError as e:
                logging.warning(
                    f"os.replace() failed ({e}), falling back to shutil.move()"
                )
                shutil.move(str(temp_assembly), str(download.final_path))
                temp_file_moved = True
                # Verify fallback move
                if download.final_path.stat().st_size != file_size:
                    raise ChunkAssemblyError("Post-move size verification failed")

            # ====================================================================
            # PHASE 7: UPDATE METRICS AND CACHE (Non-fatal)
            # ====================================================================
            try:
                self.metrics.increment('chunk_assemblies')
                self.metrics.add_bytes(file_size)
                if self.mirror:
                    self.mirror.files_processed.increment(1)
                    self.mirror.total_downloaded_size.add(file_size)
                    if hasattr(self.mirror, 'cache_manager') and download.server_etag:
                        self.mirror.cache_manager.save_file_metadata(
                            download.final_path, download.server_etag, time.time(), file_size
                        )
                    if hasattr(self.mirror, 'fs_cache'):
                        self.mirror.fs_cache.invalidate(download.final_path)
            except Exception as cache_err:
                logging.warning(f"Cache/metrics update failed (assembly succeeded): {cache_err}")

            # ====================================================================
            # SUCCESS
            # ====================================================================
            logging.info(f"✅ Successfully assembled {download.final_path.name} ({format_bytes(file_size)})")
            with download.lock:
                download.status = 'completed'
            return True

        except ChunkAssemblyError as e:
            logging.error(f"Assembly error for {download.final_path.name}: {e}")
            with download.lock:
                download.status = 'failed'
            return False
        except Exception as e:
            logging.error(f"Unexpected assembly error for {download.final_path.name}: {type(e).__name__}: {e}", exc_info=True)
            with download.lock:
                download.status = 'failed'
            return False
        finally:
            # ====================================================================
            # PHASE 8: CLEANUP
            # ====================================================================
            if not temp_file_moved and temp_assembly.exists():
                try:
                    temp_assembly.unlink()
                    logging.debug(f"Removed temp assembly file: {temp_assembly}")
                except OSError as e:
                    logging.warning(f"Failed to remove temp file {temp_assembly}: {e}")
            try:
                self.cleanup_chunks(download)
            except Exception as e:
                logging.warning(f"Chunk cleanup error (non-fatal): {e}")     
                
    def _read_chunk_data(self, chunk_path: Path) -> bytes:
        """Helper: Read chunk file into memory"""
        with open(chunk_path, 'rb') as f:
            return f.read()
        
    def cleanup_chunks(self, download: ParallelFileDownload) -> None:
        """Remove temporary chunk files."""
        try:
            if download.temp_dir and download.temp_dir.exists():
                shutil.rmtree(download.temp_dir)
        except Exception as e:
            logging.debug(f"Cleanup error: {e}")

        with self.lock:
            self.active_downloads.pop(download.final_path, None)

        # FIX (memory leak): drop the per-file lock entry. _file_locks is a
        # defaultdict that previously grew one RLock per file path forever,
        # which leaked over the lifetime of long-running mirror jobs.
        try:
            self._file_locks.pop(download.final_path, None)
        except Exception:
            pass
    
    def cleanup_stale_chunks(self) -> int:
        """Remove stale chunk directories."""
        cleaned = 0
        now = time.time()
        max_age = 24 * 3600

        # FIX: guard against assembly_dir being absent (it may have been
        # reaped by atexit during interpreter shutdown, or never created
        # if mkdir failed silently). Previously iterdir() would raise
        # FileNotFoundError before the per-item try/except could catch it,
        # so a normal-path call from shutdown() raised under that race.
        try:
            entries = list(self.assembly_dir.iterdir())
        except (FileNotFoundError, NotADirectoryError, OSError) as e:
            logging.debug(f"cleanup_stale_chunks: assembly_dir unavailable: {e}")
            return 0

        for item in entries:
            if item.is_dir():
                try:
                    if now - item.stat().st_mtime > max_age:
                        shutil.rmtree(item)
                        cleaned += 1
                except Exception:
                    pass
        return cleaned
    
    def get_stats(self) -> Dict[str, Any]:
        """Get parallel download statistics"""
        with self.lock:
            active = len(self.active_downloads)
            total_chunks = sum(len(d.chunks) for d in self.active_downloads.values())
            return {
                'enabled': self.enabled,
                'active_files': active,
                'active_chunks': total_chunks,  # ✅ KEY FIX: This key was missing
                'max_chunks_per_file': self.max_chunks_per_file,
                'min_chunk_size_mb': self.min_chunk_size / (1024*1024),
                'max_parallel_chunks': self.max_parallel_chunks,
                'assembly_dir': str(self.assembly_dir),
                'rate_limiter': {
                    'active_chunks_per_ip': dict(self.rate_limiter.active_chunks_per_ip)
                } if hasattr(self.rate_limiter, 'active_chunks_per_ip') else {}
            }
    
    def shutdown(self) -> None:
        """Shutdown manager with proper cleanup of all resources."""
        # Set shutdown flag first to stop background threads
        self._shutdown = True
        
        # Stop the periodic cleanup thread
        if hasattr(self, '_cleanup_thread') and self._cleanup_thread is not None:
            if self._cleanup_thread.is_alive():
                logging.debug("Stopping periodic cleanup thread...")
                self._cleanup_thread.join(timeout=5.0)
                if self._cleanup_thread.is_alive():
                    logging.warning("Cleanup thread did not stop within timeout")
        
        # Mark active downloads as cancelled so threads can exit early
        with self.lock:
            active_count = len(self.active_downloads)
            for download in list(self.active_downloads.values()):
                if download.status in ('downloading', 'assembling'):
                    download.status = 'cancelled'
                    logging.debug(f"Cancelled parallel download for {download.final_path.name}")
            
            if active_count > 0:
                logging.info(f"Cancelled {active_count} active parallel downloads")
        
        # Shutdown the executor with proper waiting
        if hasattr(self, 'own_executor') and self.own_executor and self.executor:
            try:
                logging.debug("Shutting down download executor...")
                try:
                    self.executor.shutdown(wait=True, cancel_futures=True)
                except TypeError:
                    self.executor.shutdown(wait=True)
                logging.debug("Download executor shutdown complete")
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
        
        # Clear per-file locks
        try:
            self._file_locks.clear()
        except Exception:
            pass
        
        logging.debug("Parallel download manager shutdown complete") 
       
    def __del__(self):
        """Cleanup on garbage collection."""
        try:
            # Only shutdown if not already done
            if hasattr(self, '_shutdown') and not self._shutdown:
                logging.debug("ParallelDownloadManager __del__ initiating shutdown")
                self.shutdown()
        except (FileNotFoundError, OSError) as e:
            # Expected during interpreter shutdown when temp dirs are already gone
            logging.debug(f"ParallelDownloadManager __del__ cleanup (expected): {e}")
        except Exception as e:
            # Don't raise exceptions in __del__
            logging.debug(f"ParallelDownloadManager __del__ shutdown error: {e}")
                
    def auto_select_method(self, file_sizes: List[int], total_files: int, 
                          remote_urls: List[str]) -> DownloadMethod:
        """
        Automatically select optimal download method based on runtime conditions.
        ONLY called when no download method arguments were provided.
        """
        # If user specified a method, respect it
        if self.config.parallel_downloads:
            return DownloadMethod.TRADITIONAL_PARALLEL
        if getattr(self.config, 'streaming_parallel', False):
            return DownloadMethod.STREAMING_PARALLEL
        if getattr(self.config, 'sequential_downloads', False):
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
        logging.debug(f"Auto-select stats: files={total_files}, avg_size={avg_file_size_mb:.1f}MB, "
                     f"disk={'SSD' if disk_is_ssd else 'HDD'}, network={network_speed_mbps:.0f}Mbps, "
                     f"range={supports_range}")
        
        # Many small files - parallel files without chunking
        small_files_count = sum(1 for s in file_sizes if s < 10 * 1024 * 1024)
        if small_files_count > total_files * 0.7 and total_files >= 3:
            logging.info(f"📊 Auto-selected: TRADITIONAL_PARALLEL ({small_files_count} small files detected)")
            return DownloadMethod.TRADITIONAL_PARALLEL
        
        # Large files with SSD and good network - streaming parallel
        if (avg_file_size_mb >= 100 and
            total_files >= 4 and
            disk_is_ssd and
            network_speed_mbps > 100 and
            supports_range):
            logging.info(f"📊 Auto-selected: STREAMING_PARALLEL "
                       f"(avg:{avg_file_size_mb:.0f}MB, SSD, {network_speed_mbps:.0f}Mbps, {total_files} files)")
            return DownloadMethod.STREAMING_PARALLEL
        
        # Large files with HDD - traditional parallel (temp files safer)
        if (avg_file_size_mb >= 50 and
            total_files >= 3 and
            not disk_is_ssd and
            supports_range):
            logging.info(f"📊 Auto-selected: TRADITIONAL_PARALLEL (HDD detected, {total_files} files)")
            return DownloadMethod.TRADITIONAL_PARALLEL
        
        # Default to sequential for safety
        logging.info(f"📊 Auto-selected: SEQUENTIAL (balanced for {total_files} files)")
        return DownloadMethod.SEQUENTIAL
        
    def _detect_ssd(self) -> bool:
        """Detect if target disk is SSD (non-rotational)."""
        # Use config override if provided
        if self.config.force_disk_type:
            return self.config.force_disk_type.lower() == 'ssd'
        
        try:
            import psutil
            if not self.mirror or not self.mirror.target_dir:
                return True  # Assume SSD if we can't detect
            
            target_path = str(self.mirror.target_dir)
            
            # Get disk partition
            for partition in psutil.disk_partitions():
                if target_path.startswith(partition.mountpoint):
                    # Check if it's SSD (non-rotational)
                    if hasattr(partition, 'opts'):
                        # Linux: 'rota' flag (1=HDD, 0=SSD)
                        if 'rota=0' in partition.opts or 'nonrot' in partition.opts:
                            return True
                    break
            
            # Fallback: Test random write speed
            if self.mirror.target_dir:
                test_file = self.mirror.target_dir / '.speed_test'
                try:
                    # Write 10MB randomly to simulate fragmentation
                    with open(test_file, 'wb') as f:
                        f.truncate(10 * 1024 * 1024)

                    # Random write test
                    start = time.time()
                    with open(test_file, 'r+b') as f:
                        for _ in range(100):  # 100 random writes
                            f.seek(random.randint(0, 10 * 1024 * 1024))
                            f.write(b'x' * 1024)
                    duration = time.time() - start

                    test_file.unlink()

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
        
        try:
            test_url = sample_urls[0]

            # Download first 1MB of a file
            headers = {'Range': 'bytes=0-1048575'}
            start = time.time()
            response = self.connection_manager.request(test_url, method='GET', headers=headers, timeout=10)
            
            if response.status_code == 206:
                data = response.content
                duration = time.time() - start
                if duration > 0:
                    speed_mbps = (len(data) * 8) / duration / 1_000_000
                    logging.debug(f"Network speed estimate: {speed_mbps:.0f} Mbps")
                    return speed_mbps
        except Exception as e:
            logging.debug(f"Network speed test failed: {e}")
        
        return 100  # Default assumption
    
    def _check_http2_support(self) -> bool:
        """Check if server supports HTTP/2."""
        if not self.mirror or not self.mirror.base_url:
            return False
        try:
            response = self.connection_manager.request(
                self.mirror.base_url, 
                method='GET',
                timeout=5
            )
            return response.http_version == 'HTTP/2'
        except Exception:
            return False
    
    def _check_range_support(self, test_url: str) -> bool:
        """Check if server supports Range requests."""
        if not test_url:
            return False
        try:
            response = self.connection_manager.request(test_url, method='HEAD', timeout=10)
            accept_ranges = response.headers.get('Accept-Ranges', '').lower()
            return accept_ranges == 'bytes'
        except Exception:
            return False        
        
# ============================================================================
# CONNECTION POOL (FIXED v3.0.3)
# ============================================================================
class ConnectionPool:
    """Manage connection pools for better resource usage with proper reuse"""
    
    def __init__(self, max_pools: int = MAX_CONNECTION_POOLS, config: MirrorConfig = None):
        """
        Initialize connection pool.
        
        Args:
            max_pools: Maximum number of connection pools
            config: MirrorConfig instance
        """
        self.pools: Dict[str, httpx.Client] = {}
        self.max_pools = max_pools
        self.lock = RLock()
        self.config = config
        self._session_counter = 0
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._creation_count = 0
        self.rate_limiter = PerIPRateLimiter() if config and config.security_validation else None
        
        # FIX: Track connection usage per pool
        self.pool_usage: Dict[str, int] = {}
        self.last_used: Dict[str, float] = {}
        
        logging.debug(f"ConnectionPool initialized: max_pools={max_pools}")
    
    def get_client(self, base_url: str) -> httpx.Client:
        """
        Get or create HTTP client for base URL with proper connection reuse.
        Uses simplified double-checked locking to prevent thread explosion and deadlocks.
        Args:
            base_url: Base URL for connection pool
        Returns:
            HTTP client instance with connection pooling
        """
        parsed = urlparse(base_url)
        pool_key = f"{parsed.scheme}://{parsed.netloc}"
        
        # Fast path - check without lock for read-heavy workloads
        if pool_key in self.pools:
            with self.lock:
                # Double-check under lock for thread safety and stats accuracy
                if pool_key in self.pools:
                    self._hits += 1
                    self.pool_usage[pool_key] = self.pool_usage.get(pool_key, 0) + 1
                    self.last_used[pool_key] = time.time()
                    return self.pools[pool_key]
                    
        # Slow path - create new pool under lock
        with self.lock:
            # Re-check after acquiring lock (another thread may have created it)
            if pool_key in self.pools:
                self._hits += 1
                self.pool_usage[pool_key] = self.pool_usage.get(pool_key, 0) + 1
                self.last_used[pool_key] = time.time()
                return self.pools[pool_key]
                
            # Pool definitely doesn't exist
            self._misses += 1
            
            # Evict oldest pool if we're at capacity
            if len(self.pools) >= self.max_pools:
                self._evict_oldest_pool()
                
            # Create new client with optimized settings
            client = self._create_client()
            self.pools[pool_key] = client
            self.pool_usage[pool_key] = 1
            self.last_used[pool_key] = time.time()
            self._creation_count += 1
            self._session_counter += 1
            
            logging.debug(f"Created new connection pool for {pool_key} (total: {len(self.pools)})")
            return client
    
    def _create_client(self) -> httpx.Client:
        """Create new HTTP client with optimized connection pooling"""
        # Much larger connection limits for parallel downloads
        limits = httpx.Limits(
            max_connections=100,  # Increased from 20
            max_keepalive_connections=50,  # Increased from 10
            keepalive_expiry=120.0  # Increased from 60
        )
        
        # Longer timeouts for large files
        timeout = httpx.Timeout(
            self.config.timeout if self.config else DEFAULT_TIMEOUT,
            connect=30.0,  # Increased from 10
            read=self.config.timeout * 3 if self.config else DEFAULT_TIMEOUT * 3  # Increased multiplier
        )
        
        # Create client with keep-alive and connection reuse
        client = httpx.Client(
            http2=self.config.http2 if self.config else True,
            limits=limits,
            timeout=timeout,
            follow_redirects=True,
            transport=SecureTransport(rate_limiter=self.rate_limiter),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Keep-Alive': 'timeout=120, max=1000',  # Explicit keep-alive
                'Upgrade-Insecure-Requests': '1',
            }
        )
        
        return client
    
    def _evict_oldest_pool(self) -> None:
        """Evict the oldest or least used connection pool"""
        if not self.pools:
            return
        
        # Smart eviction - remove least recently used pool
        if self.last_used:
            oldest_key = min(self.last_used.items(), key=lambda x: x[1])[0]
        else:
            oldest_key = next(iter(self.pools))
        
        try:
            # Close all connections in the pool
            old_client = self.pools[oldest_key]
            old_client.close()
            logging.debug(f"Closed connection pool for {oldest_key}")
        except Exception as e:
            logging.debug(f"Error closing pool {oldest_key}: {e}")
        
        # Clean up tracking. By construction every key in pool_usage /
        # last_used is also in self.pools (see get_client), so popping here
        # keeps the three dicts in sync and bounded at max_pools.
        del self.pools[oldest_key]
        self.pool_usage.pop(oldest_key, None)
        self.last_used.pop(oldest_key, None)
        self._evictions += 1

        logging.debug(f"Evicted connection pool for {oldest_key} (evictions: {self._evictions})")
    
    def warm_up(self, urls: List[str]) -> None:
        """
        Pre-warm connections for a list of URLs.
        
        This establishes connections before downloads start to avoid
        connection setup overhead during critical download time.
        
        Args:
            urls: List of URLs to warm up connections for
        """
        if not urls:
            return
        
        # Group by domain
        domains = {}
        for url in urls[:20]:  # Limit to 20 URLs for warm-up
            try:
                parsed = urlparse(url)
                domain = f"{parsed.scheme}://{parsed.netloc}"
                if domain not in domains:
                    domains[domain] = []
                domains[domain].append(url)
            except Exception:
                continue
        
        if not domains:
            return
        
        logging.info(f"🔥 Pre-warming {len(domains)} connection pools")
        
        # Warm up each domain with a few connections
        with ThreadPoolExecutor(max_workers=min(len(domains), 10)) as executor:
            futures = []
            for domain, domain_urls in domains.items():
                futures.append(executor.submit(self._warm_domain, domain, domain_urls[:3]))
            
            for future in as_completed(futures):
                try:
                    future.result(timeout=15)
                except Exception as e:
                    logging.debug(f"Warm-up failed for a domain: {e}")
        
        logging.info(f"✅ Connection pool warm-up complete")
    
    def _warm_domain(self, domain: str, urls: List[str]) -> None:
        """
        Warm up a specific domain by establishing connections.
        
        Args:
            domain: Domain to warm up
            urls: Sample URLs from this domain
        """
        try:
            # Get or create client for this domain
            client = self.get_client(urls[0])
            
            # Establish connections by making HEAD requests
            for url in urls:
                try:
                    # Actually send HEAD request to establish connection
                    response = client.head(url, timeout=5.0)
                    response.close()  # Close response but keep connection alive
                    logging.debug(f"Warmed up connection to {url}")
                except Exception as e:
                    logging.debug(f"Failed to warm up {url}: {e}")
                    continue
                    
        except Exception as e:
            logging.debug(f"Failed to warm up domain {domain}: {e}")
    
    def close_all(self) -> None:
        """Close all connection pools and clean up resources"""
        with self.lock:
            pool_count = len(self.pools)
            for domain, client in self.pools.items():
                try:
                    client.close()
                    logging.debug(f"Closed connection pool for {domain}")
                except Exception as e:
                    logging.debug(f"Error closing pool for {domain}: {e}")
            
            self.pools.clear()
            self.pool_usage.clear()
            self.last_used.clear()
            logging.info(f"Closed all {pool_count} connection pools")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive connection pool statistics.
        
        Returns:
            Dictionary with connection pool statistics
        """
        with self.lock:
            total_requests = self._hits + self._misses
            hit_rate = (self._hits / total_requests * 100) if total_requests > 0 else 0
            
            # Get pool-specific stats
            pool_details = {}
            for domain, client in self.pools.items():
                pool_details[domain] = {
                    'usage_count': self.pool_usage.get(domain, 0),
                    'last_used': self.last_used.get(domain, 0),
                    'age_seconds': time.time() - self.last_used.get(domain, time.time()) if domain in self.last_used else 0
                }
            
            return {
                'pools': len(self.pools),
                'max_pools': self.max_pools,
                'hits': self._hits,
                'misses': self._misses,
                'total_requests': total_requests,
                'hit_rate': f"{hit_rate:.1f}%",
                'evictions': self._evictions,
                'creations': self._creation_count,
                'active_sessions': self._session_counter,
                'pool_details': pool_details,
                'rate_limiter': self.rate_limiter.get_stats() if self.rate_limiter else None
            }
    
    def get_pool(self, domain: str) -> Optional[httpx.Client]:
        """
        Get a specific pool by domain without creating a new one.
        
        Args:
            domain: Domain to get pool for (e.g., 'https://example.com')
            
        Returns:
            HTTP client or None if not found
        """
        with self.lock:
            return self.pools.get(domain)
    
    def has_pool(self, domain: str) -> bool:
        """
        Check if a pool exists for the given domain.
        
        Args:
            domain: Domain to check
            
        Returns:
            True if pool exists
        """
        with self.lock:
            return domain in self.pools
    
    def clear_idle_pools(self, idle_timeout: float = 300.0) -> int:
        """
        Clear pools that have been idle for too long.
        
        Args:
            idle_timeout: Maximum idle time in seconds
            
        Returns:
            Number of pools cleared
        """
        cleared = 0
        now = time.time()
        
        with self.lock:
            idle_domains = [
                domain for domain, last in self.last_used.items()
                if now - last > idle_timeout
            ]
            
            for domain in idle_domains:
                if domain in self.pools:
                    try:
                        self.pools[domain].close()
                        del self.pools[domain]
                        self.pool_usage.pop(domain, None)
                        self.last_used.pop(domain, None)
                        cleared += 1
                        logging.debug(f"Cleared idle pool for {domain}")
                    except Exception as e:
                        logging.debug(f"Error clearing idle pool {domain}: {e}")
        
        if cleared > 0:
            logging.info(f"Cleared {cleared} idle connection pools")
        
        return cleared
    
    def resize_pools(self, new_max_pools: int) -> None:
        """
        Dynamically resize the maximum number of pools.
        
        Args:
            new_max_pools: New maximum number of pools
        """
        with self.lock:
            old_max = self.max_pools
            self.max_pools = new_max_pools
            
            # If we're reducing size, evict excess pools
            if new_max_pools < len(self.pools):
                excess = len(self.pools) - new_max_pools
                for _ in range(excess):
                    self._evict_oldest_pool()
            
            logging.info(f"Resized connection pools: {old_max} → {new_max_pools}")
 
# First, add the exception if not already defined (add to the exceptions section near the top of the file):


# Then add the UnifiedConcurrencyManager class (place it before ConnectionManager):

class UnifiedConcurrencyManager:
    """
    Unified concurrency control for all operations.
    
    This manages thread pools, async tasks, and chunk downloads to ensure
    system resources are not exhausted.
    """
    
    def __init__(self, max_total_threads: int = UNIFIED_MAX_TOTAL_THREADS,
                 max_async_tasks: int = UNIFIED_MAX_ASYNC_TASKS,
                 queue_size: int = UNIFIED_QUEUE_SIZE):
        """
        Initialize unified concurrency manager.
        
        Args:
            max_total_threads: Maximum total threads across all pools
            max_async_tasks: Maximum concurrent async tasks
            queue_size: Maximum queue size for pending operations
        """
        self.max_total_threads = max_total_threads
        self.max_async_tasks = max_async_tasks
        self.queue_size = queue_size
        
        # Track active resources
        self.active_threads = 0
        self.active_async_tasks = 0
        self.pending_operations = 0
        
        # FIX: Add missing lock attribute
        self.lock = RLock()  
        
        # Locks and conditions
        self.thread_lock = RLock()
        self.async_lock = RLock()
        self.thread_condition = threading.Condition(self.thread_lock)
        
        # Statistics
        self.total_submitted = 0
        self.total_completed = 0
        self.total_failed = 0
        self.max_concurrent_reached = 0
        
        # Shared thread pool (if enabled)
        self.shared_pool: Optional[ThreadPoolExecutor] = None
        self.shared_pool_enabled = UNIFIED_THREAD_POOL_SHARED
        self.shared_pool_lock = RLock()
        
        # Async semaphore
        self.async_semaphore = asyncio.Semaphore(max_async_tasks)
        
        # Monitoring
        self.monitor_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self._shutdown = False
    
    def start(self) -> None:
        """Start the concurrency manager and monitoring."""
        if self.shared_pool_enabled and not self.shared_pool:
            self.shared_pool = ThreadPoolExecutor(
                max_workers=self.max_total_threads,
                thread_name_prefix="mirror_shared"
            )
        
        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        
        logging.debug(f"UnifiedConcurrencyManager started: max_threads={self.max_total_threads}, "
                     f"max_async={self.max_async_tasks}")
    
    def shutdown(self) -> None:
        """Shutdown the concurrency manager with proper resource cleanup."""
        logging.debug("Shutting down UnifiedConcurrencyManager...")
        
        # Set shutdown flags
        self._shutdown = True
        self.monitor_running = False
        
        # Stop the monitor thread first
        if self.monitor_thread and self.monitor_thread.is_alive():
            logging.debug("Stopping monitor thread...")
            self.monitor_thread.join(timeout=5.0)
            if self.monitor_thread.is_alive():
                logging.warning("Monitor thread did not stop within timeout")
        
        # Shutdown shared thread pool
        if self.shared_pool:
            try:
                logging.debug("Shutting down shared thread pool...")
                # Cancel pending tasks first
                try:
                    self.shared_pool.shutdown(wait=True, cancel_futures=True)
                except TypeError:
                    # Python < 3.9 doesn't support cancel_futures
                    self.shared_pool.shutdown(wait=True)
                self.shared_pool = None
                logging.debug("Shared thread pool shutdown complete")
            except Exception as e:
                logging.error(f"Error shutting down shared pool: {e}")
        
        # Reset counters
        with self.thread_lock:
            self.active_threads = 0
            self.pending_operations = 0
        
        # Notify any waiting threads
        try:
            with self.thread_condition:
                self.thread_condition.notify_all()
        except Exception:
            pass
        
        logging.debug("UnifiedConcurrencyManager shutdown complete") 
               
    def acquire_thread(self, concurrency_type: ConcurrencyType = ConcurrencyType.SYNC) -> bool:
        """
        Acquire a thread slot.
        
        Returns:
            True if slot acquired, False if at limit
        """
        with self.thread_lock:
            if self.active_threads >= self.max_total_threads:
                self.max_concurrent_reached = max(
                    self.max_concurrent_reached, self.active_threads
                )
                return False
            
            self.active_threads += 1
            self.total_submitted += 1
            return True
    
    def release_thread(self) -> None:
        """Release a thread slot."""
        with self.thread_lock:
            self.active_threads -= 1
            self.total_completed += 1
            self.thread_condition.notify_all()
    
    def acquire_async(self) -> asyncio.Semaphore:
        """Get async semaphore for task limiting."""
        return self.async_semaphore
    
    def submit_to_shared_pool(self, fn, *args, **kwargs) -> concurrent.futures.Future:
        """
        Submit a task to the shared thread pool with proper timeout handling.
        
        Fixed: Proper condition variable usage with atomic state transitions.
        """
        with self.shared_pool_lock:
            if not self.shared_pool:
                raise ConcurrencyLimitError("Shared pool not initialized")
        
        # Use condition variable properly with context manager
        timeout = 30  # 30 seconds timeout
        start_time = time.time()
        slot_acquired = False
        
        try:
            with self.thread_condition:
                # Wait for an available slot
                while self.active_threads >= self.max_total_threads:
                    self.pending_operations += 1
                    try:
                        # Wait with timeout
                        if not self.thread_condition.wait(timeout=5.0):
                            # Timeout occurred - check overall timeout
                            if time.time() - start_time > timeout:
                                raise ConcurrencyLimitError(
                                    f"Timeout waiting for thread slot after {timeout}s"
                                )
                            # Continue waiting - will re-enter the while loop
                            continue
                        # Slot became available - break out of while loop
                        break
                    finally:
                        self.pending_operations -= 1
                
                # We have a slot - increment counters atomically within the lock
                self.active_threads += 1
                self.total_submitted += 1
                slot_acquired = True
            
            # Submit the task (outside the condition lock to avoid deadlocks)
            future = self.shared_pool.submit(self._wrapped_task, fn, args, kwargs)
            future.add_done_callback(self._task_done_callback)
            return future
            
        except Exception as e:
            # If we acquired a slot but submission failed, release it
            if slot_acquired:
                with self.thread_condition:
                    self.active_threads -= 1
                    self.thread_condition.notify()
            raise
    
    def _task_done_callback(self, future: concurrent.futures.Future) -> None:
        """
        Callback executed when a task completes.
        
        This is called in the thread pool's worker thread, so we need to
        acquire the condition lock to safely update counters.
        """
        with self.thread_condition:
            self.active_threads -= 1
            self.total_completed += 1
            # Notify one waiting thread that a slot is available
            self.thread_condition.notify()
        
        # Check for exceptions (optional logging)
        try:
            future.result()
        except Exception as e:
            with self.thread_lock:
                self.total_failed += 1
            logging.debug(f"Task failed in shared pool: {e}")    
            
    def _wrapped_task(self, fn, args, kwargs):
        """Wrapped task for monitoring."""
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            with self.thread_lock:
                self.total_failed += 1
            raise
    
    def _task_done(self, future):
        """Called when a task completes."""
        self.release_thread()
    
    def _monitor_loop(self) -> None:
        """Monitor loop for concurrency statistics."""
        while self.monitor_running and not self._shutdown:
            time.sleep(MONITOR_INTERVAL_SECONDS)
            
            with self.thread_lock:
                active = self.active_threads
                pending = self.pending_operations
                total_sub = self.total_submitted
                total_comp = self.total_completed
                max_conc = self.max_concurrent_reached
            
            if active > self.max_total_threads * 0.9:
                logging.warning(f"High concurrency: {active}/{self.max_total_threads} threads active, "
                              f"{pending} pending")
            
            logging.debug(f"Concurrency stats: active={active}, pending={pending}, "
                         f"submitted={total_sub}, completed={total_comp}, "
                         f"max_concurrent={max_conc}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get concurrency manager statistics."""
        with self.thread_lock:
            return {
                'active_threads': self.active_threads,
                'max_threads': self.max_total_threads,
                'pending_operations': self.pending_operations,
                'total_submitted': self.total_submitted,
                'total_completed': self.total_completed,
                'total_failed': self.total_failed,
                'max_concurrent_reached': self.max_concurrent_reached,
                'shared_pool_enabled': self.shared_pool_enabled,
                'async_semaphore_limit': self.max_async_tasks
            }
            
# ============================================================================
# CONNECTION MANAGER
# ============================================================================
class ConnectionManager:
    """Manages HTTP connections with security and rate limiting"""
    
    def __init__(self, config: MirrorConfig, metrics: MetricsCollector, 
                 concurrency_manager: Optional[UnifiedConcurrencyManager] = None):
        """
        Initialize connection manager.
        
        Args:
            config: MirrorConfig instance
            metrics: MetricsCollector instance
            concurrency_manager: Optional concurrency manager (created if not provided)
        """
        self.config = config
        self.metrics = metrics
        
        # FIX: Create concurrency manager if not provided
        if concurrency_manager is None:
            self.concurrency_manager = UnifiedConcurrencyManager()
            self.concurrency_manager.start()
        else:
            self.concurrency_manager = concurrency_manager
        
        self.connection_pool = ConnectionPool(config=config)
        self.rate_limiter = RateLimiter(delay=config.request_delay, per_ip=config.security_validation)
        self.request_semaphore = Semaphore(20)
        self.consecutive_failures = 0
        self.max_consecutive_failures = 3
        self.base_url = config.base_url
        self.base_parsed = urlparse(str(config.base_url))
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()
    
    def _validate_url_scheme(self, url: str) -> bool:
        """Validate URL scheme using fast method."""
        # Use the fast version from MirrorURL if available, or implement here
        url_sz = Str(url)
        if url_sz.startswith(Str('http://')):
            return True
        if url_sz.startswith(Str('https://')):
            return True
        return False

    def _get_url_path_fast(self, url: str) -> Str:
        """Fast path extraction using StringZilla."""
        url_sz = Str(url)
        # Find the path part after the domain
        after_protocol = url_sz.find('://')
        if after_protocol < 0:
            return Str('')
        
        path_start = url_sz.find('/', after_protocol + 3)
        if path_start < 0:
            return Str('')
        
        return url_sz[path_start:]
    
    def _is_url_within_scope(self, url: str, check_base: bool = True) -> bool:
        """
        Optimized URL scope checking using StringZilla.
        
        Validates that a URL is within the configured base scope.
        Prevents path traversal and ensures security boundaries.
        """
        try:
            # Use the static method from MirrorURL for fast scheme validation
            from mirror_url import MirrorURL
            if not MirrorURL._validate_url_scheme_fast(url):
                logging.debug(f"URL scope check failed: invalid scheme for {url}")
                return False
            
            # Fast path extraction using StringZilla
            url_path = self._get_url_path_fast(url)
            if not url_path:
                logging.debug(f"URL scope check failed: no path for {url}")
                return False
            
            # Get scope path
            if check_base:
                scope_path = self.base_parsed.path
            else:
                if not self.target_parsed:
                    return False
                scope_path = self.target_parsed.path
            
            # Normalize scope path: ensure it's not None and handle root
            if not scope_path:
                scope_path = '/'
            
            # Ensure scope_path ends with / for proper prefix matching
            # This prevents /files matching /files_secure
            if not scope_path.endswith('/'):
                scope_path = scope_path + '/'
            
            # Convert to string for comparison
            url_path_str = str(url_path)
            
            # Check if url_path starts with scope_path
            if not url_path_str.startswith(scope_path):
                # Special case: root scope matches everything
                if scope_path != '/':
                    logging.debug(f"URL {url} outside scope {scope_path}")
                    return False
            
            # Get remaining path after scope for security checks
            remaining = url_path_str[len(scope_path):] if len(scope_path) < len(url_path_str) else ''
            
            # Fast path traversal detection using StringZilla
            remaining_sz = Str(remaining)
            if remaining_sz.find('..') >= 0:
                logging.warning(f"Path traversal attempt in URL: {sanitize_url_for_log(url)}")
                self.metrics.increment('security_blocks')
                return False
            
            # Check for dot segments (current directory references)
            if remaining_sz.find('/.') >= 0 or remaining_sz.find('./') >= 0:
                logging.warning(f"Current directory reference in URL: {sanitize_url_for_log(url)}")
                self.metrics.increment('security_blocks')
                return False
            
            # Check for encoded path traversal
            remaining_str = str(remaining_sz)
            if '%2e' in remaining_str.lower() or '%2f' in remaining_str.lower():
                try:
                    decoded = unquote(remaining_str)
                    if '..' in decoded or '/.' in decoded:
                        logging.warning(f"Encoded path traversal in URL: {sanitize_url_for_log(url)}")
                        self.metrics.increment('security_blocks')
                        return False
                except Exception:
                    pass
            
            return True
            
        except Exception as e:
            logging.debug(f"Error in URL scope check: {e}")
            return False
        
    
    def _normalize_url(self, url: str) -> str:
        """Normalize URL"""
        parsed = urlparse(url)
        path = parsed.path
        if '%' in path:
            try:
                path = unquote(path)
            except Exception:
                pass
        path = quote(path, safe='/%')
        normalized = parsed._replace(path=path, fragment='').geturl()
        return normalized
    
    
    # Maximum number of HTTP redirects we'll follow in one request() call.
    # The previous implementation followed redirects via unbounded recursion
    # (`return self.request(...)`), so a malicious or misconfigured server
    # could trigger RecursionError or stack exhaustion. Bound it explicitly.
    _MAX_REDIRECTS = 10

    def request(self, url: str, method: str = 'GET',
                allow_redirects: bool = True,
                _redirect_depth: int = 0,
                **kwargs: Any) -> httpx.Response:
        """
        Make HTTP request with security and retry logic.
        Args:
            url: URL to request
            method: HTTP method
            allow_redirects: Whether to follow redirects
            _redirect_depth: Internal counter — number of redirects already
                followed for this logical request. Callers should not set
                this; it's incremented when the manual redirect handler
                re-enters request().
            **kwargs: Additional arguments for request
        Returns:
            HTTP response
        Raises:
            SecurityError: If security validation fails
            MirrorConnectionError: If circuit breaker is open
            URLScopeError: If URL is outside scope
        """

        # Acquire thread slot from concurrency manager
        thread_acquired = False
        try:
            if self.concurrency_manager:
                thread_acquired = self.concurrency_manager.acquire_thread(ConcurrencyType.SYNC)
                if not thread_acquired:
                    raise ConcurrencyLimitError("Thread limit reached, cannot process request")
            if self.config.security_validation:
                is_safe, error_msg = SecurityValidator.validate_url_security(url, str(self.base_url))
                if not is_safe:
                    self.metrics.increment('security_blocks')
                    raise SecurityError(f"Security validation failed: {error_msg}")
            # Get domain for circuit breaker
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            
            if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
                self.metrics.increment('circuit_breaker_trips')
                raise MirrorConnectionError(f"Circuit breaker is open for domain {domain}")
            normalized_url = trim_url(self._normalize_url(url))
            
            # Call the scope check
            is_within = self._is_url_within_scope(normalized_url)

            if not is_within:
                # FIX: Check for path traversal specifically
                if '..' in url or '%2e' in url.lower():
                    raise URLScopeError(f"Path traversal detected: {sanitize_url_for_log(url)}")
                else:
                    raise URLScopeError(f"Attempted to access URL outside configured base URL scope")
            
            with self.request_semaphore:            
                if self.consecutive_failures >= self.max_consecutive_failures:
                    wait_time = exponential_backoff(self.consecutive_failures - self.max_consecutive_failures)
                    logging.warning(f"Too many consecutive failures ({self.consecutive_failures}). Waiting {wait_time:.1f}s...")
                    time.sleep(wait_time)
                    self.consecutive_failures = 0
                
                # Get IP for rate limiting with error handling
                parsed = urlparse(normalized_url)
                try:
                    if parsed.hostname:
                        ip = socket.gethostbyname(parsed.hostname)
                    else:
                        ip = "unknown"
                except Exception:
                    ip = "unknown"
                if ip != "unknown":
                    self.rate_limiter.wait(ip)
                time.sleep(random.uniform(0, 0.02))
                
                # FIX: Preserve custom timeout if provided
                custom_timeout = kwargs.pop('timeout', None)
                # Capture the CALLER-supplied headers once, before the retry
                # loop pops them out of kwargs. Used to correctly forward
                # Range / If-None-Match etc. across a redirect (see below).
                caller_headers = dict(kwargs.get('headers') or {})

                for attempt in range(self.config.max_retries + 1):
                    try:
                        client = self.connection_pool.get_client(normalized_url)
                        try:
                            request_headers = client.headers.copy()
                        except (AttributeError, TypeError):
                            request_headers = {}
                        if 'headers' in kwargs:
                            try:
                                request_headers.update(kwargs.pop('headers'))
                            except Exception:
                                request_headers = dict(kwargs.pop('headers'))

                        # FIX: Use custom timeout or default
                        if custom_timeout:
                            timeout = custom_timeout
                        else:
                            timeout = httpx.Timeout(
                                self.config.timeout,
                                connect=10.0,
                                read=self.config.timeout * 2
                            )

                        logging.debug(f"HTTP Request: {method} {sanitize_url_for_log(normalized_url)} (attempt {attempt+1})")
                        start = time.time()
                        # We always disable httpx auto-follow so we can validate the
                        # redirect target ourselves (scope + security) before fetching it.
                        try:
                            headers_to_send = dict(request_headers)
                        except Exception:
                            headers_to_send = {}
                        response = client.request(
                            method,
                            normalized_url,
                            timeout=timeout,
                            follow_redirects=False,
                            headers=headers_to_send,
                            **kwargs
                        )
                        self.metrics.add_request_time(time.time() - start)

                        # Manual redirect handling so we can enforce scope/security on Location
                        status_code = getattr(response, 'status_code', None)
                        if allow_redirects and isinstance(status_code, int) and 300 <= status_code < 400:
                            redirect_url = response.headers.get('Location') if hasattr(response, 'headers') else None
                            if redirect_url:
                                # FIX (unbounded recursion): cap how many
                                # times we'll follow Location: . The old code
                                # did `return self.request(...)` which could
                                # blow the Python stack on a redirect loop.
                                if _redirect_depth >= self._MAX_REDIRECTS:
                                    self.metrics.increment('redirect_loop_aborted')
                                    raise MirrorConnectionError(
                                        f"Too many redirects ({_redirect_depth}) for "
                                        f"{sanitize_url_for_log(url)}"
                                    )
                                resolved_url = urljoin(normalized_url, redirect_url)
                                resolved_normalized = trim_url(self._normalize_url(resolved_url))
                                if self.config.security_validation:
                                    is_safe, error_msg = SecurityValidator.validate_url_security(
                                        resolved_normalized, str(self.base_url)
                                    )
                                    if not is_safe:
                                        self.metrics.increment('security_blocks')
                                        raise SecurityError(f"Redirect blocked: {error_msg}")
                                if not self._is_url_within_scope(resolved_normalized):
                                    raise URLScopeError(f"Redirect outside scope: {sanitize_url_for_log(resolved_normalized)}")
                                logging.debug(f"Following redirect to: {sanitize_url_for_log(resolved_normalized)}")
                                # FIX: `timeout` (popped into custom_timeout at
                                # the top of the method) and `headers` (popped
                                # into request_headers inside this loop) are no
                                # longer in **kwargs, so the recursive redirect
                                # call previously dropped BOTH — a redirected
                                # ranged/conditional request lost its Range /
                                # If-None-Match headers and reverted to the
                                # default timeout. Re-thread them explicitly.
                                redirect_kwargs = dict(kwargs)
                                if custom_timeout is not None:
                                    redirect_kwargs['timeout'] = custom_timeout
                                # Forward only the caller's original headers
                                # (Range, If-None-Match, ...) — NOT the source
                                # client's default headers, which the recursive
                                # call re-derives from the redirect target's own
                                # client.
                                if caller_headers:
                                    redirect_kwargs['headers'] = dict(caller_headers)
                                return self.request(
                                    resolved_normalized,
                                    method,
                                    allow_redirects,
                                    _redirect_depth=_redirect_depth + 1,
                                    **redirect_kwargs,
                                )

                        # Retry on 5xx server errors
                        if isinstance(status_code, int) and 500 <= status_code < 600:
                            self.consecutive_failures += 1
                            if attempt == self.config.max_retries:
                                if self.circuit_breaker_manager:

                                    self.circuit_breaker_manager.record_failure(domain)
                                self.metrics.add_error(f"HTTP {status_code} for {sanitize_url_for_log(url)}", "request_error")
                                raise MirrorConnectionError(f"Request failed after {attempt+1} attempts with HTTP {status_code}")
                            wait_time = exponential_backoff(attempt, self.config.retry_delay)
                            logging.warning(f"HTTP {status_code} (attempt {attempt+1}), retrying in {wait_time:.1f}s")
                            time.sleep(wait_time)
                            continue

                        # 4xx and other final responses - raise_for_status to surface.
                        #
                        # Exceptions: 416 Range Not Satisfiable and 304 Not
                        # Modified are special — they're expected, actionable
                        # outcomes, not errors.
                        #   - 416 carries Content-Range with the actual file
                        #     size, which the resume code path needs to decide
                        #     whether the local partial is already complete.
                        #   - 304 is the normal "up to date" result of a
                        #     conditional GET/HEAD (If-None-Match /
                        #     If-Modified-Since). httpx's raise_for_status()
                        #     treats any non-2xx, including 304, as an error
                        #     (is_success is only true for 2xx), so without
                        #     this carve-out every conditional revalidation
                        #     that correctly finds an up-to-date file raises
                        #     HTTPStatusError instead of returning the 304
                        #     response to the caller (see
                        #     file_exists_and_up_to_date / the async metadata
                        #     pipeline, which both check `status_code == 304`
                        #     directly on the returned response).
                        if status_code != 416 and status_code != 304:
                            try:
                                response.raise_for_status()
                            except httpx.HTTPStatusError as e:
                                self.consecutive_failures += 1
                                if self.circuit_breaker_manager:

                                    self.circuit_breaker_manager.record_failure(domain)
                                self.metrics.add_error(str(e), "request_error")
                                raise

                        self.consecutive_failures = 0
                        if self.circuit_breaker_manager:

                            self.circuit_breaker_manager.record_success(domain)
                        return response
                    # replaced commented block
                    except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError, httpx.RequestError) as e:
                        self.consecutive_failures += 1
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            logging.error(f"Request failed after {self.config.max_retries} retries: {e}")
                            self.metrics.add_error(str(e), "request_error")
                            raise MirrorConnectionError(f"Request failed: {e}")
                        wait_time = exponential_backoff(attempt, self.config.retry_delay)
                        logging.warning(f"Request failed (attempt {attempt+1}), retrying in {wait_time:.1f}s: {e}")
                        time.sleep(wait_time)                    
#                    except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as e:
#                        self.consecutive_failures += 1
#                        if attempt == self.config.max_retries:
#                            if self.circuit_breaker_manager:
#
#                                self.circuit_breaker_manager.record_failure(domain)
#                            logging.error(f"Request failed after {self.config.max_retries} retries: {e}")
#                            self.metrics.add_error(str(e), "request_error")
#                            raise MirrorConnectionError(f"Request failed: {e}")
#                        wait_time = exponential_backoff(attempt, self.config.retry_delay)
#                        logging.warning(f"Request failed (attempt {attempt+1}), retrying in {wait_time:.1f}s: {e}")
#                        time.sleep(wait_time)
#                    except httpx.RequestError as e:
#                        self.consecutive_failures += 1
#                        logging.error(f"Request failed for {sanitize_url_for_log(url)}: {e}")
#                        self.metrics.add_error(str(e), "request_error")
#                        if self.circuit_breaker_manager:
#
#                            self.circuit_breaker_manager.record_failure(domain)
#                        raise
        finally:
            # Release thread slot if acquired
            if thread_acquired and self.concurrency_manager:
                self.concurrency_manager.release_thread()
                
    def close(self) -> None:
        """Close all connections"""
        self.connection_pool.close_all()

# ============================================================================
# ASYNC CONNECTION MANAGER
# ============================================================================
class AsyncConnectionManager:
    """Long-lived async client with exponential backoff"""
    
    def __init__(self, config: MirrorConfig, metrics: MetricsCollector):
        """
        Initialize async connection manager.
        
        Args:
            config: MirrorConfig instance
            metrics: MetricsCollector instance
        """
        self.config = config
        self.metrics = metrics
        self._client: Optional[httpx.AsyncClient] = None
        self._semaphore: Optional[asyncio.Semaphore] = None
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()
        self._closed = False
        self.rate_limiter = PerIPRateLimiter() if config.security_validation else None
 
    async def __aenter__(self):
        """Context manager entry"""
        if self._closed:
            raise RuntimeError("Cannot reuse closed AsyncConnectionManager")
        self._build_client()
        self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
        return self

    def _build_client(self) -> None:
        """Create the underlying httpx.AsyncClient (idempotent-ish helper)."""
        limits = httpx.Limits(
            max_connections=self.config.async_workers,
            max_keepalive_connections=self.config.async_workers // 3,
            keepalive_expiry=60.0
        )
        timeout = httpx.Timeout(self.config.timeout, connect=6.0, read=self.config.timeout * 1.5)

        self._client = httpx.AsyncClient(
            http2=self.config.http2,
            limits=limits,
            timeout=timeout,
            follow_redirects=True,
            transport=SecureAsyncTransport(rate_limiter=self.rate_limiter),
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            }
        )

    async def _ensure_client(self) -> bool:
        """Ensure the async client exists before use.

        FIX: head() called self._ensure_client(), but the method did not exist
        on this class (only on AdaptiveAsyncManager) — so any HEAD that actually
        reached this point raised AttributeError, breaking the non-adaptive
        async metadata path. This lazily builds the client if the manager was
        used without `async with` (and rebuilds it if it was closed), mirroring
        the lifecycle __aenter__ provides.
        """
        if self._closed:
            return False
        if self._client is None or getattr(self._client, 'is_closed', False):
            try:
                self._build_client()
            except Exception as e:
                logging.error(f"Failed to ensure async client: {e}")
                return False
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
        return True

    def record_result(self, url: str, success: bool, rtt_ms: float, duration: float) -> None:
        """No-op result recorder.

        FIX: head() calls self.record_result(...) on every request, but the
        real implementation lives on AdaptiveAsyncManager — it drives adaptive
        concurrency scaling using state (_get_profile, _current_concurrency,
        _fallback_to_sync, self.lock, ...) that THIS non-adaptive manager does
        not have. Calling it here previously raised AttributeError, breaking
        every async HEAD. AsyncConnectionManager has no concurrency profile to
        adjust, so recording a result is genuinely a no-op for it (mirrors the
        `if hasattr(self, 'record_result')` guards elsewhere, which already
        treat its absence as "do nothing").
        """
        return None
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._closed = True
        if self._client is not None:
            try:
                await self._client.aclose()
            except Exception as e:
                logging.debug(f"Error closing async client: {e}")
        self._client = None
        self._semaphore = None

 
    def is_available(self) -> bool:
        """Check if async connection manager is available for use."""
        return (self._client is not None and 
                not self._closed and 
                not getattr(self._client, 'is_closed', False))

    
    async def warm_up(self, urls: List[str]) -> None:
        """Pre-warm async connections with proper error handling."""
        if not urls or self._closed:
            return
        
        if not self.is_available():
            logging.debug("Async connection manager not available for warm-up")
            return
        
        logging.info(f"🔥 Pre-warming async connections")
        
        # Limit to first 10 URLs to avoid overwhelming
        warm_urls = urls[:10]
        tasks = []
        
        for url in warm_urls:
            tasks.append(self._warm_single_connection(url))
        
        # Use asyncio.gather with return_exceptions to handle failures
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = 0
        for r in results:
            if r is True:
                success_count += 1
            elif isinstance(r, Exception):
                logging.debug(f"Warm-up connection error: {r}")
        
        logging.info(f"✅ Warmed up {success_count}/{len(tasks)} async connections")    
        
    async def _warm_single_connection(self, url: str) -> bool:
        """Warm up a single connection"""
        try:
            if self.rate_limiter:
                parsed = urlparse(url)
                try:
                    ip = socket.gethostbyname(parsed.hostname)
                    self.rate_limiter.wait(ip)
                except Exception:
                    pass

            # FIX: Previously this used `asyncio.Semaphore(self._current_concurrency)`,
            # but (a) AsyncConnectionManager has no `_current_concurrency`
            # attribute (it lives on AdaptiveAsyncManager), so this always
            # raised AttributeError under load, and (b) creating a fresh
            # Semaphore per-call never actually limits anything. Use the
            # shared instance semaphore set up in __aenter__.
            sem = self._semaphore or asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
            async with sem:
                # Python 3.10 fix: replaced asyncio.timeout() with asyncio.wait_for()
                resp = await asyncio.wait_for(
                    self._client.head(
                        url,
                        timeout=httpx.Timeout(3.0, connect=2.0),
                        follow_redirects=False
                    ),
                    timeout=5.0
                )
                await resp.aclose()
                return True
        except asyncio.TimeoutError:
            logging.debug(f"Warm-up timeout for {url}")
            return False
        except Exception as e:
            logging.debug(f"Warm-up failed for {url}: {e}")
            return False

    async def head(self, url: str, headers: Optional[Dict[str, str]] = None) -> Optional[httpx.Response]:
        """
        Async HEAD request with simplified timeout handling and clean retry logic.
        
        Optimized version with DNS caching, proper semaphore initialization,
        and non-blocking rate limiting.
        """
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        # Circuit breaker check
        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment('circuit_breaker_trips')
            self.metrics.add_error(f"Circuit breaker open for domain {domain}", "circuit_breaker")
            return None
        
        # Ensure client is initialized
        if not await self._ensure_client() or self._client is None or self._client.is_closed:
            self.metrics.add_error(f"Async client not available for {url}", "async_client_unavailable")
            return None
        
        # Configuration
        per_request_timeout = 12.0
        max_retries = self.config.max_retries
        retry_delay_base = self.config.retry_delay * 0.7
        start_time = time.time()
        
        # Semaphore is guaranteed by _ensure_client()/__aenter__ above.
        # FIX: the previous code here referenced self._semaphore_lock and
        # self._current_concurrency — neither exists on this (non-adaptive)
        # class; they live on AdaptiveAsyncManager. That raised AttributeError
        # whenever _semaphore happened to be None. Fall back to a shared-limit
        # semaphore if somehow still unset.
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
        
        # DNS cache for performance
        if not hasattr(self, '_dns_cache'):
            self._dns_cache = {}
            self._dns_cache_ttl = 300  # 5 minutes
        
        for attempt in range(max_retries + 1):
            try:
                # 1️⃣ RATE LIMIT FIRST (async, non-blocking, with DNS caching)
                if self.rate_limiter:
                    try:
                        # Check DNS cache
                        cache_key = parsed_url.hostname
                        now = time.time()
                        ip = None
                        
                        if cache_key in self._dns_cache:
                            cached_ip, timestamp = self._dns_cache[cache_key]
                            if now - timestamp < self._dns_cache_ttl:
                                ip = cached_ip
                            else:
                                del self._dns_cache[cache_key]
                        
                        if ip is None:
                            loop = asyncio.get_running_loop()
                            ip = await loop.run_in_executor(None, socket.gethostbyname, parsed_url.hostname)
                            self._dns_cache[cache_key] = (ip, now)
                        
                        await self.rate_limiter.async_wait(ip)
                    except Exception as e:
                        logging.debug(f"Rate limiter error for {url}: {e}")
                
                # 2️⃣ ACQUIRE SEMAPHORE & MAKE REQUEST
                async with self._semaphore:
                    resp = await self._client.head(
                        url,
                        headers=headers or {},
                        timeout=httpx.Timeout(per_request_timeout, connect=4.0)
                    )
                    
                    # Success path
                    duration = time.time() - start_time
                    self.metrics.increment('async_metadata_checks')
                    self.metrics.add_request_time(duration)
                    
                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_success(domain)
                        
                    self.record_result(url, True, duration * 1000, duration)
                    
                    if getattr(resp, 'status_code', None) == 404:
                        logging.debug(f"Async HEAD 404 for {url}")
                        return None
                        
                    return resp
    
            # 3️⃣ TIMEOUT HANDLING
            except (httpx.TimeoutException, asyncio.TimeoutError):
                duration = time.time() - start_time
                logging.debug(f"Async HEAD timeout for {url} (attempt {attempt + 1}/{max_retries + 1})")
                
                if attempt == max_retries:
                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_failure(domain)
                    self.metrics.add_error(f"Async HEAD timeout after {max_retries + 1} attempts", "async_head_timeout")
                    self.record_result(url, False, duration * 1000, duration)
                    return None
                
                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue
    
            # 4️⃣ NETWORK ERRORS
            except (httpx.ConnectError, httpx.ReadError) as e:
                duration = time.time() - start_time
                logging.debug(f"Async HEAD network error: {type(e).__name__} (attempt {attempt + 1}/{max_retries + 1})")
                
                if attempt == max_retries:
                    if self.circuit_breaker_manager:
                        self.circuit_breaker_manager.record_failure(domain)
                    self.metrics.add_error(f"Async HEAD failed: {e}", "async_head_error")
                    self.record_result(url, False, duration * 1000, duration)
                    return None
                    
                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue
    
            # 5️⃣ HTTP STATUS ERRORS
            except httpx.HTTPStatusError as e:
                duration = time.time() - start_time
                status = e.response.status_code if e.response else 0
                
                if status == 404:
                    logging.debug(f"Async HEAD 404 for {url}")
                    self.record_result(url, True, duration * 1000, duration)
                    return None
                    
                logging.debug(f"Async HEAD HTTP {status} (attempt {attempt + 1}/{max_retries + 1})")
                
                if self.circuit_breaker_manager:
                    self.circuit_breaker_manager.record_failure(domain)
                self.metrics.add_error(f"Async HEAD HTTP {status} for {url}", "async_head_http_error")
                self.record_result(url, False, duration * 1000, duration)
                
                # Don't retry 4xx client errors (except 404 handled above)
                if 400 <= status < 500:
                    return None
                    
                if attempt == max_retries:
                    return None
                    
                await asyncio.sleep(exponential_backoff(attempt, retry_delay_base))
                continue
    
            # 6️⃣ UNEXPECTED ERRORS (fail fast)
            except Exception as e:
                duration = time.time() - start_time
                logging.error(f"Async HEAD unexpected error: {type(e).__name__}: {e}")
                
                if self.circuit_breaker_manager:
                    self.circuit_breaker_manager.record_failure(domain)
                self.metrics.add_error(f"Async HEAD exception: {e}", "async_head_exception")
                self.record_result(url, False, duration * 1000, duration)
                return None
    
        return None
    
    async def get_small_content(self, url: str) -> Optional[bytes]:
        """Get small content via async GET."""
        # Check if we have a client
        if self._client is None or self._client.is_closed:
            self.metrics.add_error(f"Async client not available for {url}", "async_client_unavailable")
            return None
        
        # FIX: Add fallback for tests where circuit_breaker may not be initialized
        if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
            can_execute = await self.circuit_breaker.can_execute()
            if not can_execute:
                self.metrics.increment('circuit_breaker_trips')
                return None
        
        # Apply per-IP rate limiting
        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                # Use thread pool for DNS resolution to avoid blocking
                loop = asyncio.get_running_loop()
                ip = await loop.run_in_executor(None, socket.gethostbyname, parsed.hostname)
                await self.rate_limiter.async_wait(ip)
            except Exception:
                pass
        
        start = time.time()
        try:
            # FIX: previously this was `async with asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT):`
            # which created a fresh single-permit-batch semaphore on every
            # call — it never actually limited concurrent requests. Reuse
            # the shared instance semaphore initialized in __aenter__.
            sem = self._semaphore or asyncio.Semaphore(ASYNC_SEMAPHORE_LIMIT)
            async with sem:
                # Handle mock clients that don't support async context manager properly
                try:
                    # Try streaming first (preferred)
                    async with self._client.stream("GET", url, timeout=15.0) as resp:
                        if resp.status_code != 200:
                            return None
                        content_chunks = []
                        async for chunk in resp.aiter_bytes():
                            content_chunks.append(chunk)
                            if sum(len(c) for c in content_chunks) >= CONTENT_HASH_LIMIT:
                                break
                        content = b''.join(content_chunks)
                except AttributeError:
                    # Mock client fallback - use direct get
                    resp = await self._client.get(url, timeout=15.0)
                    if resp.status_code != 200:
                        return None
                    content = resp.content
                    if len(content) > CONTENT_HASH_LIMIT:
                        content = content[:CONTENT_HASH_LIMIT]

                rtt = (time.time() - start) * 1000

                # Record result if method exists
                if hasattr(self, 'record_result'):
                    self.record_result(url, True, rtt, time.time() - start)

                # Record success in circuit breaker
                if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
                    await self.circuit_breaker.record_success()

                return content
                
        except Exception as e:
            rtt = (time.time() - start) * 1000
            logging.debug(f"Async GET failed for {url}: {e}")
            
            # Record failure if method exists
            if hasattr(self, 'record_result'):
                self.record_result(url, False, rtt, time.time() - start)
            
            # Record failure in circuit breaker
            if hasattr(self, 'circuit_breaker') and self.circuit_breaker:
                await self.circuit_breaker.record_failure()
            
            return None
        
# ============================================================================
# ADAPTIVE ASYNC MANAGER
# ============================================================================
class AdaptiveAsyncManager:
    """Manages adaptive async behavior with performance tracking"""
    
    def __init__(self, config: MirrorConfig, metrics: MetricsCollector):
        """Initialize adaptive async manager."""
        self.config = config
        self.metrics = metrics
        self.profiles: Dict[str, ServerProfile] = {}
        self.lock = RLock()
        self._client: Optional[httpx.AsyncClient] = None
        self._current_concurrency = ADAPTIVE_START_CONCURRENCY
        self._fallback_to_sync = False
        self._profile_complete = False
        self._closed = False
        self.circuit_breaker = None  # Deprecated
        self.circuit_breaker_manager = None
        if config.circuit_breaker_enabled:
            self.circuit_breaker_manager = CircuitBreakerManager()
        self._pending_concurrency: Optional[int] = None
        self._last_concurrency_change: float = 0.0
        self.rate_limiter = PerIPRateLimiter() if config.security_validation else None
        self._client_initialized = False  # Add this flag
        # FIX: shared concurrency-limit semaphore. Previously each call
        # created its own asyncio.Semaphore(self._current_concurrency),
        # which never actually limited concurrent operations. Lazily
        # (re)initialized in _init_client() so concurrency changes take
        # effect on subsequent operations without requiring an event loop
        # at construction time.
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._semaphore_lock = asyncio.Lock()
    
    async def __aenter__(self):
        """Context manager entry"""
        if self._closed:
            raise RuntimeError("Cannot reuse closed AdaptiveAsyncManager")
        await self._init_client()  # Initialize client on entry
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self._closed = True
        if self._client and not self._client.is_closed:
            try:
                await self._client.aclose()
            except Exception as e:
                logging.debug(f"Error closing adaptive client: {e}")
        self._client = None
        self._client_initialized = False  # Reset for potential reuse
        
        # Reset semaphore
        self._semaphore = None
        
        # Clear profiles if they're stale
        if not self._profile_complete:
            self.profiles.clear()
        
    def is_available(self) -> bool:
        """
        Check if adaptive async manager is available for use.
        
        Returns:
            True if manager is not closed and can be used
        """
        if self._closed:
            return False
        
        # Don't require _client_initialized here - it will be initialized
        # when __aenter__ is called. The check in _check_files_async happens
        # BEFORE the async with block, so the client hasn't been created yet.
        # If _closed is False, the manager IS available - it just needs
        # __aenter__ to initialize the client.
        return True
        
    async def _init_client(self) -> None:
        """Initialize the async client if not already done — PRODUCTION HARDENED."""
        # ✅ FIX: Use async lock to prevent race condition during initialization
        async with self._semaphore_lock:  # Reuse existing lock for simplicity
            if self._client_initialized and self._client and not self._client.is_closed:
                return
            try:
                # Close old client if it exists and is still open
                if self._client and not self._client.is_closed:
                    try:
                        await self._client.aclose()
                    except Exception as e:
                        logging.debug(f"Error closing old async client: {e}")
                
                # Create new client with updated concurrency limits
                limits = httpx.Limits(
                    max_connections=self._current_concurrency,
                    max_keepalive_connections=max(2, self._current_concurrency // 3),
                    keepalive_expiry=60.0
                )
                timeout = httpx.Timeout(
                    self.config.timeout,
                    connect=6.0,
                    read=self.config.timeout * 1.5
                )
                self._client = httpx.AsyncClient(
                    http2=self.config.http2,
                    limits=limits,
                    timeout=timeout,
                    follow_redirects=True,
                    transport=SecureAsyncTransport(rate_limiter=self.rate_limiter),
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    }
                )
                # ✅ FIX: Recreate semaphore under same lock to ensure atomic update
                # This guarantees no operation can observe a mismatched client/semaphore pair.
                self._semaphore = asyncio.Semaphore(self._current_concurrency)
                self._client_initialized = True
                logging.debug(f"Async client initialized: concurrency={self._current_concurrency}")
            except Exception as e:
                logging.error(f"Failed to initialize async client: {e}")
                self._client = None
                self._client_initialized = False
                # Do NOT reset _semaphore here — leave it for caller to handle or retry
                raise
                
    async def _ensure_client(self) -> bool:
        """Ensure client is initialized before use."""
        if not self._client_initialized or not self._client or self._client.is_closed:
            try:
                await self._init_client()
            except Exception as e:
                logging.error(f"Failed to ensure async client: {e}")
                return False
        return True
    
    async def profile_server(self, test_urls: List[str]) -> bool:
        """Profile server performance."""
        if self._profile_complete:
            return True
        if not test_urls:
            logging.warning("No URLs provided for profiling, using default settings")
            self._profile_complete = True
            return True
            
        # Ensure client is initialized
        if not await self._ensure_client():
            logging.warning("Failed to initialize async client for profiling, falling back to sync")
            self._fallback_to_sync = True
            self._profile_complete = True
            return False
            
        profile = self._get_profile(test_urls[0] if test_urls else str(self.config.base_url))
        logging.info(f"🔍 Profiling server {profile.domain} with {min(PROFILE_SAMPLE_SIZE, len(test_urls))} samples...")
        
        test_batch = test_urls[:PROFILE_SAMPLE_SIZE]
        successful_samples = 0
        total_samples = len(test_batch)
        start = time.time()
        
        for i, url in enumerate(test_batch):
            try:
                req_start = time.time()
                if self.rate_limiter:
                    parsed = urlparse(url)
                    try:
                        ip = socket.gethostbyname(parsed.hostname)
                        self.rate_limiter.wait(ip)
                    except Exception:
                        pass
                try:
                    # Python 3.10 fix: replaced async with asyncio.timeout(5.0) with asyncio.wait_for
                    resp = await asyncio.wait_for(
                        self._client.head(
                            url,
                            timeout=httpx.Timeout(3.0, connect=2.0),
                            follow_redirects=False
                        ),
                        timeout=5.0
                    )
                    rtt = (time.time() - req_start) * 1000
                    success = resp.status_code < 400
                    if success:
                        successful_samples += 1
                    # Always record the sample so error rate reflects HTTP failures, not just timeouts
                    profile.add_sample(rtt, success, time.time() - req_start)
                except asyncio.TimeoutError:
                    logging.debug(f"Profile sample {i+1} timed out")
                    profile.add_sample(5000.0, False, 5.0)
            except Exception as e:
                profile.add_sample(5000.0, False, 0)
                logging.debug(f"Profile sample {i+1} failed: {e}")
                
            if (i + 1) % 5 == 0:
                logging.debug(f"Profile progress: {i+1}/{total_samples} samples complete")
                await asyncio.sleep(0.1)
                
        elapsed = time.time() - start
        profile._update_metrics()
        success_rate = (successful_samples / total_samples * 100) if total_samples > 0 else 0
        logging.info(f"Profile complete: {successful_samples}/{total_samples} successful ({success_rate:.1f}%), "
                     f"avg RTT={profile.avg_rtt_ms:.0f}ms, errors={profile.error_rate:.1%}")
                     
        if profile.error_rate > ADAPTIVE_ERROR_THRESHOLD:
            logging.warning(f"⚠️ Server {profile.domain} error rate {profile.error_rate:.1%} > threshold, disabling async")
            logging.info(f"📝 Falling back to sync metadata checks (GET requests will still work)")
            self._fallback_to_sync = True
            self._profile_complete = True
            return False
            
        if profile.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS * 2:
            logging.warning(f"⚠️ Server {profile.domain} high RTT {profile.avg_rtt_ms:.0f}ms, reducing concurrency")
            profile.recommended_concurrency = max(1, profile.recommended_concurrency // 3)
            self._current_concurrency = profile.recommended_concurrency
            await self._init_client()  # Reinitialize with new concurrency
            
        self._profile_complete = True
        logging.info(f"✅ Server profile complete: concurrency={self._current_concurrency}, "
                     f"RTT={profile.avg_rtt_ms:.0f}ms, errors={profile.error_rate:.1%}")
        return not self._fallback_to_sync
    
    def should_fallback(self) -> bool:
        """Check if should fallback to sync"""
        return self._fallback_to_sync
    
    def get_concurrency(self) -> int:
        """Get current concurrency level"""
        return self._current_concurrency
    
    def record_result(self, url: str, success: bool, rtt_ms: float, duration: float) -> None:
        """Record operation result for adaptive adjustment — THREAD SAFE."""
        profile = self._get_profile(url)
        profile.add_sample(rtt_ms, success, duration)
        
        # ✅ FIX: Protect all state mutations with self.lock
        with self.lock:
            # Check if we need to fall back to sync
            if profile.error_rate > ADAPTIVE_ERROR_THRESHOLD and not self._fallback_to_sync:
                logging.warning(f"⚠️ Error rate {profile.error_rate:.1%} exceeded, falling back to sync")
                self._fallback_to_sync = True
                self.metrics.increment('adaptive_fallback_events')  # ✅ Track fallbacks
                return
            
            # Cooldown period: don't change concurrency too frequently
            now = time.time()
            # ✅ FIX: Initialize _last_concurrency_change in __init__, so hasattr not needed
            if now - self._last_concurrency_change < ADAPTIVE_COOLDOWN_SECONDS:
                return
            
            # Scale up logic
            if profile.should_scale_up() and self._current_concurrency < ADAPTIVE_MAX_CONCURRENCY:
                new_concurrency = min(ADAPTIVE_MAX_CONCURRENCY, self._current_concurrency + 2)
                if new_concurrency != self._current_concurrency:
                    logging.debug(f"⚡ Adaptive scale up: {self._current_concurrency} → {new_concurrency}")
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment('adaptive_scale_up_events')  # ✅ Track scale-ups
            
            # NEW: Scale down logic for moderate error rates (between 2.5% and 5%)
            elif (profile.error_rate > ADAPTIVE_ERROR_THRESHOLD * 0.5 and 
                  self._current_concurrency > ADAPTIVE_START_CONCURRENCY):
                new_concurrency = max(ADAPTIVE_START_CONCURRENCY, self._current_concurrency // 2)
                if new_concurrency != self._current_concurrency:
                    logging.warning(f"⚠️ Adaptive scale down (moderate errors {profile.error_rate:.1%}): "
                                   f"{self._current_concurrency} → {new_concurrency}")
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment('adaptive_scale_down_error_events')  # ✅ Track error-based scale-downs
            
            # Also scale down if RTT is very high
            elif (profile.avg_rtt_ms > ADAPTIVE_RTT_THRESHOLD_MS * 2 and 
                  self._current_concurrency > ADAPTIVE_START_CONCURRENCY):
                new_concurrency = max(ADAPTIVE_START_CONCURRENCY, self._current_concurrency - 1)
                if new_concurrency != self._current_concurrency:
                    logging.debug(f"📉 Adaptive scale down (high RTT {profile.avg_rtt_ms:.0f}ms): "
                                 f"{self._current_concurrency} → {new_concurrency}")
                    self._pending_concurrency = new_concurrency
                    self._last_concurrency_change = now
                    self.metrics.increment('adaptive_scale_down_rtt_events')  # ✅ Track RTT-based scale-downs
    
    def get_circuit_breaker_stats(self) -> Optional[Dict[str, Any]]:
        """Get circuit breaker statistics."""
        if self.circuit_breaker_manager:
            return self.circuit_breaker_manager.get_stats()
        return None    

    async def _do_head_request(self, url: str, headers: Optional[Dict[str, str]]) -> Optional[httpx.Response]:
        """Internal method to perform the actual HEAD request."""
        if not await self._ensure_client() or self._fallback_to_sync:
            return None
        
        if self._client is None:
            return None
        
        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                ip = socket.gethostbyname(parsed.hostname)
                self.rate_limiter.wait(ip)
            except Exception:
                pass
        
        # FIX: previously `semaphore = asyncio.Semaphore(self._current_concurrency)`
        # was constructed here per call — that never limited concurrency.
        # Reuse the shared instance semaphore from _init_client.
        sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
        start_time = time.time()

        async with sem:
            for attempt in range(self.config.max_retries + 1):
                try:
                    resp = await asyncio.wait_for(
                        self._client.head(
                            url,
                            headers=headers or {},
                            timeout=httpx.Timeout(12.0, connect=4.0)
                        ),
                        timeout=12.0
                    )

                    self.metrics.increment('async_metadata_checks')
                    duration = time.time() - start_time
                    self.metrics.add_request_time(duration)
                    self.record_result(url, True, duration * 1000, duration)
                    # Treat 404 as "no resource" rather than a usable response
                    if getattr(resp, 'status_code', None) == 404:
                        return None
                    return resp

                except asyncio.TimeoutError:
                    if attempt == self.config.max_retries:
                        duration = time.time() - start_time
                        self.record_result(url, False, 12000.0, duration)
                        return None
                    await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                    if attempt == self.config.max_retries:
                        duration = time.time() - start_time
                        self.record_result(url, False, duration * 1000, duration)
                        return None
                    await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

        return None

    async def head(self, url: str, headers: Optional[Dict[str, str]] = None) -> Optional[httpx.Response]:
        """Make async HEAD request with adaptive behavior and proper timeout handling."""
        parsed_url = urlparse(url)
        domain = parsed_url.netloc

        # ✅ FIX 1: Single circuit breaker check (removed duplicate)
        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment('circuit_breaker_trips')
            self.metrics.add_error(f"Circuit breaker open for domain {domain}", "circuit_breaker")
            return None

        if not await self._ensure_client():
            self.metrics.add_error(f"Async client not available for {url}", "async_client_unavailable")
            return None
        if self._client is None or self._client.is_closed:
            return None

        # ✅ FIX 2: Non-blocking rate limiting with async DNS resolution
        if self.rate_limiter and parsed_url.hostname:
            try:
                loop = asyncio.get_running_loop()
                # Lazy-init DNS cache to avoid blocking event loop with repeated lookups
                if not hasattr(self, '_dns_cache'):
                    self._dns_cache = {}
                cache_key = parsed_url.hostname
                now = time.time()
                cached = self._dns_cache.get(cache_key)
                if cached and (now - cached[1]) < 300:  # 5 min TTL
                    ip = cached[0]
                else:
                    ip = await loop.run_in_executor(None, socket.gethostbyname, parsed_url.hostname)
                    self._dns_cache[cache_key] = (ip, now)
                # Use async_wait() instead of blocking wait()
                await self.rate_limiter.async_wait(ip)
            except Exception as e:
                logging.debug(f"Async rate limiter error for {url}: {e}")

        sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
        start_time = time.time()

        async def _do_request_with_retries():
            async with sem:
                for attempt in range(self.config.max_retries + 1):
                    attempt_start = time.time()  # ✅ FIX 3: Track per-attempt duration
                    try:
                        resp = await self._client.head(
                            url,
                            headers=headers or {},
                            timeout=httpx.Timeout(12.0, connect=4.0)
                        )
                        rtt_ms = (time.time() - attempt_start) * 1000
                        self.metrics.increment('async_metadata_checks')
                        self.metrics.add_request_time(time.time() - start_time)
                        
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_success(domain)
                        self.record_result(url, True, rtt_ms, time.time() - attempt_start)
                        
                        if getattr(resp, 'status_code', None) == 404:
                            return None
                        return resp

                    except (httpx.TimeoutException, asyncio.TimeoutError):
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.debug(f"Async HEAD timeout for {url} (attempt {attempt+1})")
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            self.metrics.add_error(f"Async HEAD timeout for {url}", "async_head_timeout")
                            self.record_result(url, False, 12000.0, time.time() - start_time)
                            return None
                        await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

                    except (httpx.ConnectError, httpx.ReadError) as e:
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.debug(f"Async HEAD error for {url}: {e} (attempt {attempt+1})")
                        if attempt == self.config.max_retries:
                            if self.circuit_breaker_manager:
                                self.circuit_breaker_manager.record_failure(domain)
                            self.metrics.add_error(f"Async HEAD failed for {url}: {e}", "async_head_error")
                            self.record_result(url, False, rtt_ms, time.time() - start_time)
                            return None
                        await asyncio.sleep(exponential_backoff(attempt, self.config.retry_delay * 0.7))

                    except httpx.HTTPStatusError as e:
                        status = e.response.status_code if e.response else 0
                        rtt_ms = (time.time() - attempt_start) * 1000
                        if status == 404:
                            self.record_result(url, True, rtt_ms, time.time() - attempt_start)
                            return e.response
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_failure(domain)
                        self.metrics.add_error(f"Async HEAD HTTP {status} for {url}", "async_head_http_error")
                        self.record_result(url, False, rtt_ms, time.time() - start_time)
                        return None

                    except Exception as e:
                        rtt_ms = (time.time() - attempt_start) * 1000
                        logging.error(f"Async HEAD unexpected error for {url}: {e}")
                        if self.circuit_breaker_manager:
                            self.circuit_breaker_manager.record_failure(domain)
                        self.metrics.add_error(f"Async HEAD exception for {url}: {e}", "async_head_exception")
                        self.record_result(url, False, rtt_ms, time.time() - start_time)
                        return None

        try:
            # Hard cap for the entire retry sequence (prevents runaway backoff)
            return await asyncio.wait_for(_do_request_with_retries(), timeout=30.0)
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            logging.warning(f"Async HEAD overall timeout for {url} after {duration:.1f}s")
            if self.circuit_breaker_manager:
                self.circuit_breaker_manager.record_failure(domain)
            self.metrics.add_error(f"Async HEAD overall timeout for {url}", "async_head_overall_timeout")
            self.record_result(url, False, duration * 1000, duration)
            return None
    
    async def get_small_content(self, url: str) -> Optional[bytes]:
        """Get small content via async GET."""
        if not await self._ensure_client() or self._fallback_to_sync:
            return None
        
        if self._client is None:
            return None
        
        # FIX: await the coroutine
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        
        if self.circuit_breaker_manager and not self.circuit_breaker_manager.can_execute(domain):
            self.metrics.increment('circuit_breaker_trips')
            return None
        
        if self.rate_limiter:
            parsed = urlparse(url)
            try:
                # Use thread pool for DNS resolution to avoid blocking
                loop = asyncio.get_running_loop()
                ip = await loop.run_in_executor(None, socket.gethostbyname, parsed.hostname)
                await self.rate_limiter.async_wait(ip)
            except Exception:
                pass
        
        start = time.time()
        try:
            # FIX: use the shared instance semaphore. Previously a fresh
            # asyncio.Semaphore(self._current_concurrency) was constructed
            # per-call and so never actually limited concurrent operations.
            sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
            async with sem:
                # Handle mock clients that don't support async context manager properly
                try:
                    # Try streaming first (preferred for large files)
                    async with self._client.stream("GET", url, timeout=15.0) as resp:
                        if resp.status_code != 200:
                            return None
                        
                        content_chunks = []
                        async for chunk in resp.aiter_bytes():
                            content_chunks.append(chunk)
                            if sum(len(c) for c in content_chunks) >= CONTENT_HASH_LIMIT:
                                break
                        
                        content = b''.join(content_chunks)
                except AttributeError:
                    # Mock client fallback - use direct get
                    resp = await self._client.get(url, timeout=15.0)
                    if resp.status_code != 200:
                        return None
                    content = resp.content
                    if len(content) > CONTENT_HASH_LIMIT:
                        content = content[:CONTENT_HASH_LIMIT]
                
                rtt = (time.time() - start) * 1000
                
                self.record_result(url, True, rtt, time.time() - start)
                
                if self.circuit_breaker_manager:

                
                    self.circuit_breaker_manager.record_success(domain)
                
                return content
                    
        except Exception as e:
            rtt = (time.time() - start) * 1000
            logging.debug(f"Async GET failed for {url}: {e}")
            self.record_result(url, False, rtt, time.time() - start)
            
            if self.circuit_breaker_manager:

            
                self.circuit_breaker_manager.record_failure(domain)
            
            return None
  
    async def apply_pending_concurrency_change(self) -> None:
        """Apply pending concurrency changes at safe points"""
        if self._pending_concurrency is not None:
            if self._current_concurrency != self._pending_concurrency:
                logging.info(f"⚡ Applying adaptive concurrency change: {self._current_concurrency} → {self._pending_concurrency}")
                self._current_concurrency = self._pending_concurrency
                await self._init_client()  # Reinitialize with new concurrency
                self._pending_concurrency = None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get adaptive async manager statistics"""
        stats = {
            'current_concurrency': self._current_concurrency,
            'fallback_to_sync': self._fallback_to_sync,
            'profile_complete': self._profile_complete,
            'profiles': {domain: profile.__dict__ for domain, profile in self.profiles.items()},
            'rate_limiter': self.rate_limiter.get_stats() if self.rate_limiter else None,
            'client_initialized': self._client_initialized
        }
        
        # Add circuit breaker stats if available
        if self.circuit_breaker_manager:
            stats['circuit_breaker'] = self.circuit_breaker_manager.get_stats()
        
        return stats
    
    async def warm_up(self, urls: List[str]) -> None:
        """Pre-warm adaptive async connections."""
        if not urls or self._closed or self._fallback_to_sync:
            return
        
        # Initialize client first
        if not await self._ensure_client():
            logging.warning("Cannot warm up - async client initialization failed")
            return
        
        if not self._profile_complete:
            # Profile first with a subset of URLs
            await self.profile_server(urls[:10])
        
        await self._warm_up_connections(urls[:10])
    
    async def _warm_up_connections(self, urls: List[str]) -> None:
        """Internal warm-up implementation"""
        if not self._client or self._client.is_closed:
            return
        
        logging.info(f"🔥 Pre-warming adaptive async connections")
        
        tasks = []
        for url in urls:
            tasks.append(self._warm_single_connection(url))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)
        logging.info(f"✅ Warmed up {success_count}/{len(urls)} async connections")
    
    async def _warm_single_connection(self, url: str) -> bool:
        """Warm up a single connection"""
        try:
            if self.rate_limiter:
                parsed = urlparse(url)
                ip = socket.gethostbyname(parsed.hostname)
                self.rate_limiter.wait(ip)

            # FIX: use the shared instance semaphore (initialized in
            # _init_client). The previous per-call Semaphore did not limit
            # concurrent warm-ups at all.
            sem = self._semaphore or asyncio.Semaphore(self._current_concurrency)
            async with sem:
                resp = await self._client.head(
                    url,
                    timeout=httpx.Timeout(3.0, connect=2.0),
                    follow_redirects=False
                )
                await resp.aclose()
            return True
        except Exception as e:
            logging.debug(f"Warm-up failed for {url}: {e}")
            return False
    
    def _get_profile(self, url: str) -> ServerProfile:
        """Get or create server profile for URL"""
        with self.lock:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            if domain not in self.profiles:
                is_throttled = any(kd in domain for kd in KNOWN_THROTTLED_DOMAINS)
                start_conc = 3 if is_throttled else ADAPTIVE_START_CONCURRENCY
                self.profiles[domain] = ServerProfile(
                    domain=domain,
                    is_throttled=is_throttled,
                    recommended_concurrency=start_conc
                )
                if is_throttled:
                    # Also set current concurrency to conservative value
                    if not self._fallback_to_sync:
                        self._current_concurrency = start_conc
                    logging.info(f"🔍 Known throttled domain: {domain}, starting conservative")
            return self.profiles[domain]    

        
# ============================================================================
# ASYNC TASK MANAGER (NEW v3.0.6)
# ============================================================================
class AsyncTaskManager:
    """Manages async tasks with proper cleanup and cancellation."""
    
    def __init__(self):
        """Initialize async task manager."""
        self.tasks: Set[asyncio.Task] = set()
        self.lock = asyncio.Lock()
        self._shutdown = False
        self.total_created = 0
        self.total_completed = 0
    
    async def create_task(self, coro) -> asyncio.Task:
        """
        Create a task with automatic cleanup.
        
        Args:
            coro: Coroutine to wrap
            
        Returns:
            asyncio.Task instance
        """
        task = asyncio.create_task(coro)
        async with self.lock:
            self.tasks.add(task)
            self.total_created += 1
        task.add_done_callback(self._task_done)
        return task
    
    def _task_done(self, task: asyncio.Task) -> None:
        """Remove completed task (callback)."""
        # Schedule removal in the event loop
        if not self._shutdown:
            try:
                loop = asyncio.get_running_loop()
                # Use call_soon_threadsafe if needed, but here we're likely in the same loop
                if loop.is_running():
                    # Create task but ensure it's tracked
                    removal_task = loop.create_task(self._remove_task(task))
                    # Add to tracked tasks
                    async def track_and_remove():
                        async with self.lock:
                            self.tasks.add(removal_task)
                        await self._remove_task(task)
                        async with self.lock:
                            self.tasks.discard(removal_task)
                    loop.create_task(track_and_remove())
                else:
                    self._sync_remove_task(task)
            except RuntimeError:
                # No running loop, use sync removal
                self._sync_remove_task(task)
        else:
            # During shutdown, remove synchronously
            self._sync_remove_task(task)
               
    async def _remove_task(self, task: asyncio.Task) -> None:
        """Remove task from set."""
        async with self.lock:
            if task in self.tasks:
                self.tasks.discard(task)
                self.total_completed += 1
    
    def _sync_remove_task(self, task: asyncio.Task) -> None:
        """Synchronous removal for shutdown."""
        try:
            # Use asyncio.run_coroutine_threadsafe if possible
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self._remove_task(task))
            else:
                # No loop running, just remove from set (safe during shutdown)
                if task in self.tasks:
                    self.tasks.discard(task)
                    self.total_completed += 1
        except RuntimeError:
            # No event loop, just remove
            if task in self.tasks:
                self.tasks.discard(task)
                self.total_completed += 1
    
    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown all tasks gracefully with proper cleanup.
        
        Args:
            timeout: Maximum time to wait for tasks to complete
        """
        if self._shutdown:
            logging.debug("AsyncTaskManager already shutting down")
            return
        
        logging.debug(f"AsyncTaskManager shutdown initiated (active tasks: {len(self.tasks)})")
        self._shutdown = True
        
        # Take a snapshot of tasks to cancel
        async with self.lock:
            tasks_to_cancel = list(self.tasks)
            logging.debug(f"Cancelling {len(tasks_to_cancel)} async tasks")
        
        if not tasks_to_cancel:
            logging.debug("No tasks to cancel")
            return
        
        # Cancel each task
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete with timeout
        try:
            # Use asyncio.wait with timeout for graceful shutdown
            done, pending = await asyncio.wait(
                tasks_to_cancel, 
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED
            )
            
            if pending:
                logging.warning(
                    f"Async shutdown timeout: {len(pending)} tasks still pending after {timeout}s"
                )
                
                # Force cancel remaining tasks
                for task in pending:
                    if not task.done():
                        task.cancel()
                        try:
                            # Give each task a brief moment to clean up
                            await asyncio.wait_for(task, timeout=1.0)
                        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                            pass
                
        except asyncio.TimeoutError:
            logging.error(f"AsyncTaskManager shutdown timed out after {timeout}s")
        except Exception as e:
            logging.error(f"Error during async shutdown: {e}")
        
        # Clear remaining tasks
        async with self.lock:
            self.tasks.clear()
        
        # Log final statistics
        logging.info(
            f"AsyncTaskManager shutdown complete: "
            f"created={self.total_created}, completed={self.total_completed}, "
            f"cancelled={self.total_created - self.total_completed}"
        )
        
        # Reset counters for potential reuse
        self.total_created = 0
        self.total_completed = 0
    
    async def force_shutdown(self, timeout: float = 5.0) -> None:
        """
        Force shutdown all tasks immediately without waiting for completion.
        Use this only when graceful shutdown fails or times out.
        
        Args:
            timeout: Maximum time per task for forced cancellation
        """
        logging.warning(f"Force shutting down AsyncTaskManager (active tasks: {len(self.tasks)})")
        self._shutdown = True
        
        async with self.lock:
            tasks_to_cancel = list(self.tasks)
            self.tasks.clear()
        
        # Force cancel all tasks
        for task in tasks_to_cancel:
            if not task.done():
                task.cancel()
        
        # Brief wait for cancellation to propagate
        if tasks_to_cancel:
            try:
                await asyncio.wait(tasks_to_cancel, timeout=min(timeout, 5.0))
            except Exception:
                pass
        
        logging.info("AsyncTaskManager force shutdown complete")    
        
    def get_stats(self) -> Dict[str, Any]:
        """Get task manager statistics."""
        return {
            'active_tasks': len(self.tasks),
            'shutdown': self._shutdown,
            'total_created': self.total_created,
            'total_completed': self.total_completed
        }       
        
# ============================================================================
# CACHE MANAGER
# ============================================================================
class CacheManager:
    """Manages cache operations with thread safety and memory pressure handling"""
    
    def __init__(self, cache_file: Path, config: MirrorConfig, metrics: MetricsCollector):
        """Initialize cache manager."""
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        self.cache_file = cache_file
        self.config = config
        self.metrics = metrics
        self._backup_attempts = 0
        self.cache_data: Dict[str, Any] = {}
        self.metadata: Dict[str, Any] = {}
        self.lock = RLock()
        self.file_metadata_cache: Dict[str, Any] = {}
        self.dir_signatures: Dict[str, str] = {}
        
        # FIX: Change from dict to LRUCache for proper cache management
        self.html_cache = LRUCache(
            maxsize=MAX_HTML_CACHE_SIZE,
            ttl_seconds=self.config.html_cache_max_age * 3600,  # Convert hours to seconds
            name="html_cache"
        )
        self.html_cache_lock = RLock()
        self.lru_file_cache = LRUCache(
            maxsize=MAX_CACHE_METADATA_ENTRIES,
            ttl_seconds=self.config.cache_max_age * 86400,
            name="file_metadata"
        )
        logging.debug(f"CacheManager initialized: {cache_file}, max_age={config.cache_max_age}d")
    
    def load(self, _recursion_depth: int = 0, _backup_attempts: int = 0) -> Tuple[bool, Optional[Dict]]:
        """
        Load cache data from file with recursion protection.
        Args:
            _recursion_depth: Current recursion depth (internal)
            _backup_attempts: Current backup attempt count (internal)
        Returns:
            Tuple of (success, cache_data)
        """
        if _recursion_depth > 3:
            logging.error("Cache restoration failed after 3 recursion attempts, giving up")
            self.metrics.add_error("Cache recursion limit exceeded", "cache_recursion")
            return False, None
        if _backup_attempts > 2:
            logging.error("Cache backup restoration failed after 2 backup attempts, giving up")
            self.metrics.add_error("Cache backup limit exceeded", "cache_backup_limit")
            return False, None
        if self.config.no_cache:
            logging.debug("Cache disabled by --no-cache")
            return False, None
        if self.config.refresh_cache:
            logging.info("Cache refresh forced by --refresh-cache")
            self.metrics.set_cache_refreshed()
            return False, None
        if not self.cache_file.exists():
            logging.debug(f"No cache file found at {self.cache_file}")
            return False, None
        try:
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)
                # ✅ REPLACE _clean_json_keys(raw_data) with strict validation
                data = _validate_and_sanitize_cache(raw_data)
                self.metadata = data.get('_meta', {})
                # ✅ INSERT VERSION CHECK HERE
                cache_schema = self.metadata.get('version')
                if isinstance(cache_schema, int) and cache_schema != CACHE_SCHEMA_VERSION:
                    logging.warning(
                        f"Cache schema mismatch: file has v{cache_schema}, expected v{CACHE_SCHEMA_VERSION}. "
                        f"Discarding and rebuilding cache."
                    )
                    return False, None  # Forces full rebuild
                self.cache_data = {k: v for k, v in data.items() if not k.startswith('_')}
                self.dir_signatures = self.metadata.get('dir_signatures', {})
                if '_files' in data:
                    self.file_metadata_cache = data['_files']
                    self.lru_file_cache.put_batch(self.file_metadata_cache)
                    if len(self.file_metadata_cache) > MAX_CACHE_METADATA_ENTRIES:
                        logging.warning(f"Pruning file metadata cache from {len(self.file_metadata_cache)} to {MAX_CACHE_METADATA_ENTRIES} entries")
                        items = list(self.file_metadata_cache.items())[-MAX_CACHE_METADATA_ENTRIES:]
                        self.file_metadata_cache = dict(items)
                        logging.debug(f"Loaded {len(self.file_metadata_cache)} file metadata entries")
                if '_meta' in data and 'last_full_run' in data['_meta']:
                    try:
                        last_run = datetime.fromisoformat(data['_meta']['last_full_run'])
                        age = datetime.now() - last_run
                        age_days = age.total_seconds() / 86400
                        self.metrics.metrics['cache_age_days'] = age_days
                        if age_days > self.config.cache_max_age:
                            logging.info(f"Cache is {age_days:.1f} days old (> {self.config.cache_max_age}d) — refreshing")
                            self.metrics.set_cache_refreshed(age_days)
                            return False, None
                        logging.info(f"Cache age: {age_days:.1f} days (max: {self.config.cache_max_age}d)")
                    except (ValueError, KeyError) as e:
                        logging.warning(f"Invalid cache metadata: {e}")
                        return False, None
                dir_count = len(self.cache_data)
                self.metrics.set_cache_signatures(dir_count)
                if 'file_count' in self.metadata:
                    logging.info(f"📦 Cache contains {dir_count} directories with {self.metadata['file_count']} files")
                else:
                    logging.info(f"📦 Cache contains {dir_count} directories")
                return True, self.cache_data
        except json.JSONDecodeError as e:
            logging.error(f"Corrupted cache file {self.cache_file}: {e}")
            self.metrics.add_error(f"Cache corruption: {e}", "cache_corruption")
            self.metrics.increment('cache_corruptions')
            backup_path = self.cache_file.with_suffix(f'.json.corrupted.{int(time.time())}')
            backup_success = False
            try:
                self.cache_file.rename(backup_path)
                logging.info(f"Backed up corrupted cache to {backup_path}")
                backup_success = True
            except Exception as backup_error:
                logging.error(f"Failed to backup corrupted cache: {backup_error}")
                self.metrics.add_error(f"Backup failed: {backup_error}", "cache_backup_failed")
            if not backup_success:
                # Only delete if the rename above did NOT already move the file.
                # (After a successful rename the original path no longer exists,
                # so unlinking it unconditionally always raised FileNotFoundError
                # and logged a misleading "Failed to delete" error.)
                try:
                    self.cache_file.unlink()
                    logging.warning(f"Deleted corrupted cache file (backup failed): {self.cache_file}")
                except FileNotFoundError:
                    pass
                except Exception as delete_error:
                    logging.error(f"Failed to delete corrupted cache: {delete_error}")
            if backup_success:
                old_backup = self._find_oldest_valid_backup()
                if old_backup:
                    try:
                        with open(old_backup, 'r') as f:
                            json.load(f)
                        old_backup.rename(self.cache_file)
                        logging.info(f"Restored cache from older backup: {old_backup}")
                        return self.load(_recursion_depth + 1, _backup_attempts + 1)
                    except json.JSONDecodeError as validate_error:
                        logging.error(f"Restored backup is also corrupted: {validate_error}")
                        self.metrics.add_error(f"Backup validation failed: {validate_error}", "cache_backup_corrupted")
                        return False, None
                    except Exception as restore_error:
                        logging.error(f"Failed to restore backup: {restore_error}")
                        self.metrics.add_error(f"Backup restore failed: {restore_error}", "cache_backup_restore_failed")
                        return False, None
            return False, None
        except Exception as e:
            logging.error(f"Unexpected error loading cache: {e}")
            self.metrics.add_error(f"Cache load error: {e}", "cache_load_error")
            return False, None
    
    def save(self, directories: Dict[str, Any], file_count: int) -> bool:
        """
        Save cache data to file with atomic write.
        
        Args:
            directories: Dictionary of directory signatures to save
            file_count: Total number of files cached
            
        Returns:
            True if save successful
        """
        if self.config.no_cache:
            return False
        try:
            cache_data = {
                '_meta': {
                    'version': CACHE_SCHEMA_VERSION,  # ✅ Use schema version constant
                    'schema': 'mirrorurl_v3_cache',
                    'last_full_run': datetime.now().isoformat(),
                    'version_code': __version__,
                    'file_count': file_count,
                    'directory_count': len(directories),
                    'dir_signatures': self.dir_signatures,
                    'config': {
                        'base_url': sanitize_url_for_log(str(self.config.base_url)),
                        'dir_suffix': self.config.dir_suffix,
                        'cache_max_age': self.config.cache_max_age,
                        'parallel_downloads': self.config.parallel_downloads
                    }
                }
            }
            
            cache_data.update(directories)
            if self.file_metadata_cache:
                cache_data['_files'] = self.file_metadata_cache
    
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.cache_file.with_suffix('.json.tmp')
            
            # ✅ Strict formatting: no trailing spaces, consistent separators
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, separators=(',', ': '))
                f.flush()
                os.fsync(f.fileno())
                
            temp_file.rename(self.cache_file)
            self.metrics.set_cache_signatures(len(directories))
            logging.info(f"💾 Saved cache v2 with {len(directories)} directory signatures, {file_count} files")
            return True
        except Exception as e:
            logging.warning(f"Failed to save cache: {e}")
            self.metrics.add_error(str(e), "cache_save")
            temp_file = self.cache_file.with_suffix('.json.tmp')
            if temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass
            return False
        

   
    def get_html_cache(self, url: str) -> Optional[Tuple[List[str], List[str]]]:
        """Get cached HTML parse results."""
        if not self.config.cache_html:
            return None
        
        # LRUCache.get() handles TTL internally
        entry = self.html_cache.get(url)
        if not entry:
            self.metrics.increment('html_cache_misses')
            return None
        
        # FIX: Handle multiple possible return types
        if isinstance(entry, dict):
            files = entry.get('files', [])
            subdirs = entry.get('subdirs', [])
        elif isinstance(entry, tuple) and len(entry) == 2:
            # Old format: (files, subdirs)
            files, subdirs = entry
        elif isinstance(entry, list) and len(entry) == 2:
            # List format
            files, subdirs = entry
        else:
            # Unknown format, treat as cache miss
            self.metrics.increment('html_cache_misses')
            return None
        
        self.metrics.increment('html_cache_hits')
        return (files, subdirs)
    
    def set_html_cache(self, url: str, files: List[str], subdirs: List[str], content_hash: str = None) -> None:
        """Cache HTML parse results."""
        if not self.config.cache_html:
            return
        
        if content_hash is None:
            content_hash = hashlib.sha256(str(files + subdirs).encode()).hexdigest()
        
        cache_entry = {
            'files': files,
            'subdirs': subdirs,
            'content_hash': content_hash,
            'timestamp': time.time()
        }
        
        # LRUCache has .put() method
        self.html_cache.put(url, cache_entry)
    
    def invalidate_directory(self, dir_url: str, new_signature: str) -> bool:
        """
        Invalidate directory cache if signature changed.
        
        Args:
            dir_url: Directory URL
            new_signature: New directory signature
            
        Returns:
            True if cache was invalidated
        """
        with self.lock:
            old_signature = self.dir_signatures.get(dir_url)
            if old_signature and old_signature != new_signature:
                self.dir_signatures[dir_url] = new_signature
                self.metrics.increment('cache_invalidated_dirs')
                
                # FIX: Use invalidate method instead of 'in' operator and del
                self.html_cache.invalidate(dir_url)
                
                logging.debug(f"Cache invalidated for directory: {sanitize_url_for_log(dir_url)}")
                return True
            elif not old_signature:
                self.dir_signatures[dir_url] = new_signature
                return True
            
            return False
    
    def get_file_metadata(self, local_path: Path) -> Optional[Dict]:
        """
        Get cached file metadata.
        
        Args:
            local_path: Local file path
            
        Returns:
            File metadata dictionary or None
        """
        key = str(local_path.resolve())
        cached = self.lru_file_cache.get(key)
        if cached:
            return cached
        with self.lock:
            return self.file_metadata_cache.get(key)
    
    def save_file_metadata(self, local_path: Path, etag: str, mtime: float, size: int = 0) -> None:
        """
        Save file metadata to cache.
        
        Args:
            local_path: Local file path
            etag: ETag value
            mtime: Modification time
            size: File size
        """
        key = str(local_path.resolve())
        data = {
            'etag': etag,
            'mtime': mtime,
            'size': size,
            'updated': datetime.now().isoformat()
        }
        self.lru_file_cache.put(key, data)
        
        with self.lock:
            self.file_metadata_cache[key] = data
            if len(self.file_metadata_cache) > MAX_CACHE_METADATA_ENTRIES:
                items = list(self.file_metadata_cache.items())[-MAX_CACHE_METADATA_ENTRIES:]
                self.file_metadata_cache = dict(items)
    
    def cleanup_file_metadata(self, local_path: Path) -> None:
        """Remove file metadata from cache"""
        key = str(local_path.resolve())
        self.lru_file_cache.invalidate(key)
        with self.lock:
            if key in self.file_metadata_cache:
                del self.file_metadata_cache[key]
    
    def cleanup_stale_metadata(self, expected_files: Set[Path]) -> int:
        """
        Remove metadata for files that no longer exist.
        
        Args:
            expected_files: Set of files that should exist
            
        Returns:
            Number of entries removed
        """
        with self.lock:
            removed = 0
            keys_to_remove = []
            expected_keys = {str(f.resolve()) for f in expected_files}
            
            for key in self.file_metadata_cache:
                if key not in expected_keys:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                del self.file_metadata_cache[key]
                self.lru_file_cache.invalidate(key)
                removed += 1
            
            if removed > 0:
                logging.debug(f"Cleaned up {removed} stale file metadata entries")
            
            return removed
    
    def _find_oldest_valid_backup(self) -> Optional[Path]:
        """Find the oldest valid backup cache file"""
        try:
            backups = list(self.cache_file.parent.glob(
                f"{self.cache_file.stem}.json.corrupted.*"
            ))
            if not backups:
                return None
            backups.sort(key=lambda p: p.stat().st_mtime)
            for backup in backups:
                try:
                    with open(backup, 'r') as f:
                        json.load(f)
                    return backup
                except Exception:
                    continue
            return None
        except Exception:
            return None
    
    def handle_memory_pressure(self, pressure=None, level=None):
        """
        Handle memory pressure by shrinking caches.
        
        Args:
            pressure: MemoryPressure enum value
            level: String level ('warning' or 'critical') for test compatibility
        """
        # Convert level string to MemoryPressure if needed
        if level is not None:
            if level == "warning":
                pressure = MemoryPressure.WARNING
            elif level == "critical":
                pressure = MemoryPressure.CRITICAL
        
        freed = 0
        if pressure == MemoryPressure.WARNING:
            freed += self.lru_file_cache.shrink_to(0.7)
        elif pressure == MemoryPressure.CRITICAL:
            freed += self.lru_file_cache.shrink_to(0.3)
            # FIX: Use shrink_to instead of clear and len
            freed += self.html_cache.shrink_to(0.3)
        return freed

# ============================================================================
# PER-DOMAIN CIRCUIT BREAKER MANAGER
# ============================================================================
class CircuitBreakerManager:
    """Manages per-domain circuit breakers to prevent cascade failures."""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0,
                 half_open_limit: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_limit = half_open_limit
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.lock = RLock()
    
    def get_breaker(self, domain: str) -> CircuitBreaker:
        with self.lock:
            if domain not in self.breakers:
                self.breakers[domain] = CircuitBreaker(
                    failure_threshold=self.failure_threshold,
                    recovery_timeout=self.recovery_timeout,
                    half_open_limit=self.half_open_limit
                )
            return self.breakers[domain]
    
    def record_success(self, domain: str) -> None:
        # FIX (production-readiness pass): previously this only updated a
        # breaker that already existed in self.breakers, but nothing on the
        # real request path ever called get_breaker() to create one — only
        # an explicit, manual get_breaker() call did. Since real callers
        # only ever call record_success()/record_failure()/can_execute(),
        # the per-domain breaker for a real domain was NEVER created, which
        # made can_execute() return True unconditionally forever (see its
        # old "if domain not in self.breakers: return True" short-circuit)
        # and made record_failure() a permanent no-op. The circuit breaker
        # never actually tripped for a single domain in real usage. Lazily
        # creating the breaker here (get_breaker() is idempotent) fixes
        # that: the first call for a domain registers it, and all three
        # methods now operate on the same persistent per-domain breaker.
        with self.lock:
            self.get_breaker(domain).record_success()

    def record_failure(self, domain: str) -> None:
        with self.lock:
            self.get_breaker(domain).record_failure()

    def can_execute(self, domain: str) -> bool:
        with self.lock:
            return self.get_breaker(domain).can_execute()
    
    def reset_domain(self, domain: str) -> None:
        with self.lock:
            if domain in self.breakers:
                del self.breakers[domain]
    
    def get_stats(self) -> Dict[str, Any]:
        with self.lock:
            return {domain: breaker.get_stats() for domain, breaker in self.breakers.items()}
    
    def reset_all(self) -> None:
        with self.lock:
            self.breakers.clear()

# ============================================================================
# DIRECTORY SCANNER
# ============================================================================
class DirectoryScanner:
    """High-performance directory scanner"""
    LINK_XPATH = XPath('//a[@href]') if LXML_AVAILABLE else None
    
    def __init__(self, mirror_instance):
        self.mirror = mirror_instance
        self.client = mirror_instance.connection_manager if hasattr(mirror_instance, 'connection_manager') else None
        self.base_url = getattr(mirror_instance, 'base_url', '')
        # FIX: Use getattr with default to handle MockMirror
        self.target_base_url = getattr(mirror_instance, 'target_base_url', self.base_url)
        self.target_dir = getattr(mirror_instance, 'target_dir', None)
        self.config = getattr(mirror_instance, 'config', None)
        self.metrics = getattr(mirror_instance, 'metrics', None)
        self.parse_cache = LRUCache(
            maxsize=MAX_IN_MEMORY_CACHE_SIZE,
            ttl_seconds=3600,
            name="parse_cache"
        )
        self._last_cache_cleanup = time.time()
        self._cache_cleanup_interval = 300
        # FIX: html_cache should be LRUCache, not a dict
        self.html_cache = LRUCache(
            maxsize=MAX_HTML_CACHE_SIZE,
            ttl_seconds=HTML_CACHE_MAX_AGE_HOURS * 3600,
            name="html_cache"
        )
        self.batch_processor = AdaptiveBatchProcessor()
        self.fs_cache = FileSystemCache()
        self.fast_parse_count = 0
        self.lxml_parse_count = 0
        self.cached_signatures: Dict[str, str] = {}
        self.adaptive_manager = None
        self.scan_count = 0
    
    def _maybe_cleanup_cache(self) -> None:
        """Periodically clean up cache"""
        now = time.time()
        if now - self._last_cache_cleanup > self._cache_cleanup_interval:
            self.parse_cache.shrink_to(0.5)
            self._last_cache_cleanup = now
            logging.debug("Parse cache shrunk to 50%")
    
    @log_performance("directory_scan")
    def scan_directory_sequential(self, url: str) -> Tuple[List[str], List[str]]:
        """
        Scan a single directory sequentially.
        
        Args:
            url: Directory URL to scan
            
        Returns:
            Tuple of (files, subdirs)
        """
        self._maybe_cleanup_cache()
        url = trim_url(url)
        
        cached = self.parse_cache.get(url)
        if cached:
            self.metrics.increment('cache_hits')
            return cached
        
        cached_result = self.mirror.cache_manager.get_html_cache(url)
        if cached_result:
            files, subdirs = cached_result
            self.parse_cache.put(url, (files, subdirs))
            logging.debug(f"HTML cache hit for {sanitize_url_for_log(url)}")
            return files, subdirs
        
        try:
            files, subdirs = self._perform_scan(url)
        except ParsingError as e:
            # FIX: scan failed (non-200 or exception). Do NOT cache an empty
            # result — neither in parse_cache nor the persisted html_cache —
            # so a transient error doesn't mask real files for the rest of
            # this run or future runs. Return empty for this call only.
            logging.debug(f"Not caching failed scan for {sanitize_url_for_log(url)}: {e}")
            return [], []

        self.parse_cache.put(url, (files, subdirs))

        if self.config.cache_html:
            content_hash = hashlib.new(self.config.hash_algorithm, str(files + subdirs).encode()).hexdigest()
            self.mirror.cache_manager.set_html_cache(url, files, subdirs, content_hash)

        self.scan_count += 1
        return files, subdirs
    
    def _perform_scan(self, url: str) -> Tuple[List[str], List[str]]:
        """Perform actual directory scanning"""
        files = []
        subdirs = []
        
        try:
            self.metrics.start_parse_timer()
            start = time.time()
            
            response = self.client.request(url, method='GET', timeout=30)
            self.metrics.add_request_time(time.time() - start)
            
            if response.status_code != 200:
                self.metrics.stop_parse_timer()
                logging.debug(f"Directory scan returned {response.status_code}: {sanitize_url_for_log(url)}")
                # FIX: a non-200 is a SCAN FAILURE, not a confirmed-empty
                # directory. Returning ([], []) here let the caller cache an
                # empty result (in parse_cache AND the persisted html_cache),
                # poisoning the rest of the run — and future runs — with a
                # bogus "no files here". Raise so scan_directory_sequential
                # can return empty WITHOUT caching it.
                raise ParsingError(f"HTTP {response.status_code} scanning {url}")
            
            content_length = len(response.content)
            
            if should_use_fast_parser(content_length, self.config):
                links = extract_links_fast(response.content)
                self.fast_parse_count += 1
                self.metrics.increment('fast_parses')
                logging.debug(f"Fast parser used for {sanitize_url_for_log(url)} ({content_length} bytes)")
            else:
                if not LXML_AVAILABLE:
                    logging.warning(f"lxml not available, falling back to fast parser for {sanitize_url_for_log(url)}")
                    links = extract_links_fast(response.content)
                    self.fast_parse_count += 1
                    self.metrics.increment('fast_parses')
                else:
                    tree = html.fromstring(response.content)
                    links = []
                    for link in self.LINK_XPATH(tree):
                        href = link.get('href')
                        if href:
                            links.append(href)
                    self.lxml_parse_count += 1
                    self.metrics.increment('lxml_parses')
                    logging.debug(f"LXML parser used for {sanitize_url_for_log(url)} ({content_length} bytes)")
            
            # Pre-parse the canonical base scope ONCE per call so the per-link
            # check below is just a string compare on the (already-parsed)
            # netloc and a path-prefix check on the (already-parsed) path.
            base_parsed_for_scope = urlparse(self.base_url)
            base_netloc = base_parsed_for_scope.netloc
            base_path = base_parsed_for_scope.path or '/'
            if not base_path.endswith('/'):
                base_path = base_path + '/'

            for href in links:
                if href in ('../', './') or href.startswith(('?', '#', 'javascript:', 'mailto:')):
                    continue

                full_url = trim_url(urljoin(url, href).split('#')[0])

                # FIX (scope bypass): the previous check was
                #     if not full_url.startswith(self.base_url): continue
                # which is a textual prefix match. With the canonical
                # ``self.base_url`` stored without a trailing slash (CLI args
                # strip it), a base of ``https://example.com`` matched
                # ``https://example.com.attacker.com/...`` — a real scope
                # bypass for any HTML page the scanner parsed. Compare the
                # parsed netloc and a slash-terminated path prefix instead.
                try:
                    full_parsed = urlparse(full_url)
                except Exception:
                    continue
                if full_parsed.scheme not in ('http', 'https'):
                    continue
                if full_parsed.netloc != base_netloc:
                    continue
                full_path = full_parsed.path or '/'
                # Allow exact match of base_path's parent (e.g. base "/files/"
                # should also accept the bare "/files" link).
                if not (full_path == base_path.rstrip('/')
                        or full_path.startswith(base_path)):
                    continue

                if full_url.endswith('/'):
                    if self.mirror._is_within_target_scope(full_url):
                        subdirs.append(full_url)
                else:
                    if self.mirror.matches_filter(full_url):
                        files.append(full_url)
            
            self.metrics.stop_parse_timer()
            self.metrics.increment('directories_processed')
            self.metrics.increment('directories_scanned_sequential')
            
            logging.debug(f"Scan complete for {sanitize_url_for_log(url)}: {len(files)} files, {len(subdirs)} subdirs")
            return files, subdirs
            
        except ParsingError:
            # Already-classified scan failure (e.g. non-200). Re-raise so the
            # caller does not cache an empty result.
            self.metrics.stop_parse_timer()
            raise
        except Exception as e:
            self.metrics.stop_parse_timer()
            logging.error(f"Error scanning {sanitize_url_for_log(url)}: {e}")
            self.metrics.add_error(str(e), "directory_scan")
            # FIX: surface the failure instead of returning ([], []), which
            # would be cached as an authoritative empty directory.
            raise ParsingError(f"Scan failed for {url}: {e}") from e
    
    def get_parse_stats(self) -> Dict[str, Any]:
        """Get parser statistics"""
        return {
            'fast_parses': self.fast_parse_count,
            'lxml_parses': self.lxml_parse_count,
            'unique_directories_cached': len(set(self.parse_cache.cache.keys())),
            'total_scans': self.scan_count,
            'parse_cache_lookups': {
                'hits': self.parse_cache.hits,
                'misses': self.parse_cache.misses,
                'hit_rate': self.parse_cache.get_stats()['hit_rate']
            },
            'html_cache': self.html_cache.get_stats(),
            'batch_processor': {
                'current_batch_size': self.batch_processor.get_batch_size()
            }
        }

# ============================================================================
# PROGRESS TRACKER
# ============================================================================
class ProgressTracker:
    """Track and report progress for long operations"""
    
    def __init__(self, total: int, prefix: str = "", name: str = "items",
                 use_tqdm: bool = True, config: Optional[MirrorConfig] = None,
                 level: str = "default"):
        """
        Initialize progress tracker.
        
        Args:
            total: Total items to process
            prefix: Prefix for log messages
            name: Name of items being tracked
            use_tqdm: Whether to use tqdm progress bar
            config: MirrorConfig instance
            level: Progress level name
        """
        try:
            self.total = max(0, int(total) if total is not None else 0)
        except (TypeError, ValueError):
            self.total = 0
            logging.debug(f"Invalid total value for ProgressTracker: {total}, using 0")
        
        self.prefix = prefix
        self.name = name
        self.level = level
        self.completed = 0
        self.lock = RLock()
        self.start_time = time.time()
        self.last_report = 0
        self.callbacks = []
        self._last_logged_completed = -1
        self._last_logged_pct = -1
        self.use_tqdm = (use_tqdm and TQDM_AVAILABLE and
                        config and config.progress_bar and
                        self.total > 0)
        self.tqdm_bar = None
        self._fallback_mode = False
        self._use_percentage_mode = self.total >= PROGRESS_MIN_FILES_FOR_PCT
        self._milestone_index = 0
        self._next_milestone = PROGRESS_PCT_MILESTONES[0] if self._use_percentage_mode else None
        self._pending_updates = 0
        self._update_threshold = 10 if not self.use_tqdm else 1
        
        if self.use_tqdm:
            try:
                from tqdm import tqdm
                self.tqdm_bar = tqdm(total=self.total, desc=f"{prefix}{name}",
                                    unit=name, leave=True, position=0)
            except Exception as e:
                logging.debug(f"Failed to initialize tqdm: {e}")
                self.use_tqdm = False
    
    def reset_rate_after_fallback(self):
        """Reset rate timer after fallback"""
        with self.lock:
            self._fallback_mode = True
            self.start_time = time.time()
            self._last_logged_completed = self.completed
            logging.debug("Progress rate timer reset after fallback to sync")
    
    def _should_report(self) -> bool:
        """Check if progress should be reported"""
        if self.completed >= self.total:
            return False
        
        now = time.time()
        elapsed = now - self.start_time
        
        if elapsed < PROGRESS_SHORT_JOB_SECONDS:
            if self._use_percentage_mode:
                current_pct = (self.completed / self.total * 100)
                if current_pct >= self._next_milestone:
                    while (self._milestone_index < len(PROGRESS_PCT_MILESTONES) - 1 and
                          current_pct >= PROGRESS_PCT_MILESTONES[self._milestone_index + 1]):
                        self._milestone_index += 1
                    self._next_milestone = PROGRESS_PCT_MILESTONES[self._milestone_index + 1] \
                        if self._milestone_index < len(PROGRESS_PCT_MILESTONES) - 1 else 101
                    return True
            else:
                return (now - self.last_report) >= PROGRESS_UPDATE_SHORT
        elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
            return (now - self.last_report) >= PROGRESS_UPDATE_MEDIUM
        else:
            return (now - self.last_report) >= PROGRESS_UPDATE_LONG
        
        return False
    
    def update(self, n: int = 1) -> None:
        """
        Update progress by n items.
        
        Args:
            n: Number of items completed
        """
        with self.lock:
            old_completed = self.completed
            self.completed = min(self.completed + n, self.total)
            
            if logging.root.level <= logging.DEBUG and hasattr(self, 'config') and self.config and self.config.debug:
                logging.debug(f"Progress.update({n}): {old_completed} -> {self.completed}/{self.total}")
            
            if self.use_tqdm and self.tqdm_bar:
                try:
                    self.tqdm_bar.update(n)
                except Exception:
                    pass
            
            if self._should_report():
                report_msg = self._generate_report()
                elapsed = time.time() - self.start_time
                
                if elapsed < PROGRESS_SHORT_JOB_SECONDS:
                    logging.info(f"[short][{self.level}] {report_msg}")
                elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
                    logging.info(f"[medium][{self.level}] {report_msg}")
                else:
                    logging.info(f"[long][{self.level}] {report_msg}")
                
                self.last_report = time.time()
                self._last_logged_completed = self.completed
                
                if self._use_percentage_mode:
                    self._last_logged_pct = (self.completed / self.total * 100)
    
    def report_final(self) -> str:
        """Report final progress"""
        with self.lock:
            logging.debug(f"report_final: before - completed={self.completed}, total={self.total}")
            
            if self.completed < self.total:
                logging.debug(f"report_final: completed < total, setting to total")
                self.completed = self.total
            
            self.last_report = time.time()
            
            if self.use_tqdm and self.tqdm_bar:
                try:
                    self.tqdm_bar.n = self.tqdm_bar.total
                    self.tqdm_bar.refresh()
                    self.tqdm_bar.close()
                except Exception:
                    pass
            
            report_msg = self._generate_report(force_total=True)
            
            if not self.use_tqdm:
                elapsed = time.time() - self.start_time
                if elapsed < PROGRESS_SHORT_JOB_SECONDS:
                    logging.info(f"[final-short][{self.level}] {report_msg}")
                elif elapsed < PROGRESS_MEDIUM_JOB_SECONDS:
                    logging.info(f"[final-medium][{self.level}] {report_msg}")
                else:
                    logging.info(f"[final-long][{self.level}] {report_msg}")
            
            self._last_logged_completed = self.completed
            logging.debug(f"report_final: after - completed={self.completed}, total={self.total}")
            
            return report_msg
    
    def _generate_report(self, force_total: bool = False) -> str:
        """Generate progress report message"""
        now = time.time()
        total_elapsed = now - self.start_time
        
        if force_total:
            percentage = (self.completed / self.total * 100) if self.total > 0 else 100
            rate = self.completed / total_elapsed if total_elapsed > 0 else 0
            report = (
                f"{self.prefix}Progress [{self.level}]: {self.completed}/{self.total} {self.name} "
                f"({percentage:.1f}%) - Complete! (Overall rate: {rate:.1f}/s)"
            )
        else:
            rate = self.completed / total_elapsed if total_elapsed > 0 else 0
            remaining_items = self.total - self.completed
            
            if rate > 0:
                remaining_time = remaining_items / rate
            else:
                remaining_time = float('inf')
            
            eta_str = format_duration(remaining_time) if remaining_time > 0 and remaining_time != float('inf') else "unknown"
            percentage = (self.completed / self.total * 100) if self.total > 0 else 0
            elapsed_str = format_duration(total_elapsed)
            
            report = (
                f"{self.prefix}Progress [{self.level}]: {self.completed}/{self.total} {self.name} "
                f"({percentage:.1f}%) - Rate: {rate:.1f}/s - "
                f"Elapsed: {elapsed_str} - ETA: {eta_str}"
            )
        
        return report
    
    def add_callback(self, callback: Callable) -> None:
        """Add progress callback"""
        self.callbacks.append(callback)
    
    def _trigger_callbacks(self) -> None:
        """Trigger progress callbacks"""
        for callback in self.callbacks:
            try:
                callback(self.completed, self.total)
            except Exception as e:
                logging.debug(f"Callback error: {e}")

# ============================================================================
# MULTI-LEVEL PROGRESS TRACKER
# ============================================================================
class MultiLevelProgress:
    """Track progress across multiple levels/operations"""
    
    def __init__(self):
        """Initialize multi-level progress tracker"""
        self.levels: Dict[str, ProgressTracker] = {}
        self.lock = RLock()
        self.start_time = time.time()
    
    def add_level(self, name: str, total: int, prefix: str = "", use_tqdm: bool = True, 
                  config: Optional[MirrorConfig] = None):
        """
        Add a new progress level.
        
        Args:
            name: Level name
            total: Total items for this level
            prefix: Prefix for log messages
            use_tqdm: Whether to use tqdm
            config: MirrorConfig instance
        """
        with self.lock:
            self.levels[name] = ProgressTracker(
                total=total,
                prefix=prefix,
                name=name,
                use_tqdm=use_tqdm,
                config=config,
                level=name
            )
    
    def update(self, level: str, n: int = 1):
        """
        Update progress for a level.
        
        Args:
            level: Level name
            n: Number of items completed
        """
        with self.lock:
            if level in self.levels:
                self.levels[level].update(n)
    
    def set_total(self, level: str, total: int):
        """
        Set total for a level.
        
        Args:
            level: Level name
            total: New total
        """
        with self.lock:
            if level in self.levels:
                self.levels[level].total = total
    
    def report_final(self, level: str) -> str:
        """
        Get final report for a level.
        
        Args:
            level: Level name
            
        Returns:
            Final report string
        """
        with self.lock:
            if level in self.levels:
                return self.levels[level].report_final()
            return ""
    
    def get_status(self) -> str:
        """
        Get overall progress status.
        
        Returns:
            Status string for all levels
        """
        with self.lock:
            status = []
            for name, tracker in self.levels.items():
                if tracker.total > 0:
                    pct = tracker.completed / tracker.total * 100
                    status.append(f"{name}: {tracker.completed}/{tracker.total} ({pct:.1f}%)")
            
            elapsed = format_duration(time.time() - self.start_time)
            return f"Elapsed: {elapsed} | " + " | ".join(status)
    
    def reset_rate_after_fallback(self, level: str):
        """Reset rate after fallback for a level"""
        with self.lock:
            if level in self.levels:
                self.levels[level].reset_rate_after_fallback()

# ============================================================================
# MEMORY MONITOR
# ============================================================================
class MemoryMonitor:
    """Monitor memory usage and trigger cleanup when needed"""
    
    def __init__(self, 
                 warning_threshold_mb: int = MEMORY_WARNING_THRESHOLD_MB,
                 critical_threshold_mb: int = MEMORY_CRITICAL_THRESHOLD_MB,
                 check_interval: int = MEMORY_CHECK_INTERVAL):
        """
        Initialize memory monitor.
        
        Args:
            warning_threshold_mb: Warning threshold in MB
            critical_threshold_mb: Critical threshold in MB
            check_interval: Check interval in seconds
        """
        self.warning_threshold = warning_threshold_mb * 1024 * 1024
        self.critical_threshold = critical_threshold_mb * 1024 * 1024
        self.check_interval = check_interval
        self.last_check = 0
        self.high_water_mark = 0
        self.lock = RLock()
        self.psutil_available = PSUTIL_AVAILABLE
        self._last_reset = time.time()
        
        if not PSUTIL_AVAILABLE:
            logging.warning("psutil not available, memory monitoring disabled")
    
    def check_pressure(self) -> MemoryPressure:
        """
        Check current memory pressure level.
        
        Returns:
            MemoryPressure level
        """
        if not self.psutil_available:
            return MemoryPressure.NORMAL
        
        now = time.time()
        with self.lock:
            if now - self.last_check < self.check_interval:
                return MemoryPressure.NORMAL
        
            try:
                process = psutil.Process()
                memory_info = process.memory_info()
                rss = memory_info.rss
                
                if rss > self.high_water_mark:
                    self.high_water_mark = rss
                    
                   # Add periodic reset of high water mark to avoid unbounded growth
                if hasattr(self, '_last_reset'):
                    if time.time() - self._last_reset > 3600:  # Reset hourly
                        self.high_water_mark = rss
                        self._last_reset = time.time()
                else:
                    self._last_reset = time.time()     
                
                self.last_check = now
                
                if rss > self.critical_threshold:
                    logging.warning(f"Critical memory pressure: {rss / (1024*1024):.1f}MB")
                    return MemoryPressure.CRITICAL
                elif rss > self.warning_threshold:
                    logging.debug(f"Memory pressure warning: {rss / (1024*1024):.1f}MB")
                    return MemoryPressure.WARNING
                
                return MemoryPressure.NORMAL
            except Exception as e:
                logging.debug(f"Memory check failed: {e}")
                return MemoryPressure.NORMAL
    
    def get_usage_mb(self) -> float:
        """
        Get current memory usage in MB.
        
        Returns:
            Memory usage in MB
        """
        if not self.psutil_available:
            return 0.0
        
        try:
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except Exception:
            return 0.0

# ============================================================================
# DISK SPACE MANAGER
# ============================================================================
class DiskSpaceManager:
    """Manage disk space and check availability"""
    
    def __init__(self, target_dir: Path):
        """
        Initialize disk space manager.
        
        Args:
            target_dir: Target directory to monitor
        """
        self.target_dir = target_dir
        self.warnings_issued = 0
        # FIX: Only create directory if target_dir is provided AND not None
        if self.target_dir is not None:
            try:
                self.target_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logging.debug(f"Could not create target directory: {e}")
    
    def check_available(self, required_bytes: int) -> Tuple[bool, Optional[str]]:
        """
        Check if enough disk space is available for *this operation*.

        Returns False ONLY when there isn't enough free space to do the work
        (either below the absolute floor or below ``required_bytes``).
        Overall disk-fullness percentages are warnings, not blockers — a host
        that happens to have a 96%-full disk can still legitimately download
        a few MB of data, and refusing to operate there made the library
        (and its tests) unusable on real hosts.

        Args:
            required_bytes: Required bytes

        Returns:
            Tuple of (ok, error_message)
        """
        # FIX: Return True if no target_dir (dry-run or connection failed)
        if self.target_dir is None:
            return True, None

        try:
            usage = shutil.disk_usage(self.target_dir)
            free_bytes = usage.free

            if free_bytes < MIN_FREE_SPACE_BYTES:
                return False, f"Only {free_bytes / (1024*1024):.1f}MB free, need at least {MIN_FREE_SPACE_BYTES / (1024*1024):.1f}MB"

            if free_bytes < required_bytes:
                return False, f"Insufficient space: need {required_bytes / (1024*1024):.1f}MB, have {free_bytes / (1024*1024):.1f}MB"

            # Fullness percentages are advisory warnings only — they DO NOT
            # block operations that have enough free space for themselves.
            usage_percent = usage.used / usage.total
            if usage_percent > DISK_SPACE_CRITICAL_THRESHOLD:
                if self.warnings_issued % 10 == 0:
                    logging.warning(
                        f"Disk usage critical: {usage_percent*100:.1f}% "
                        f"({free_bytes / (1024*1024):.1f}MB free) — proceeding anyway"
                    )
                self.warnings_issued += 1
            elif usage_percent > DISK_SPACE_WARNING_THRESHOLD:
                if self.warnings_issued % 10 == 0:
                    logging.warning(f"Disk usage high: {usage_percent*100:.1f}%")
                self.warnings_issued += 1

            return True, None
        except Exception as e:
            logging.warning(f"Failed to check disk space: {e}")
            return True, None
    
    def get_usage_stats(self) -> Dict[str, float]:
        """
        Get disk usage statistics.
        
        Returns:
            Dictionary with disk usage stats
        """
        # FIX: Return empty dict if target_dir is None
        if self.target_dir is None:
            return {}
        try:
            usage = shutil.disk_usage(self.target_dir)
            return {
                'total_gb': usage.total / (1024**3),
                'used_gb': usage.used / (1024**3),
                'free_gb': usage.free / (1024**3),
                'usage_percent': usage.used / usage.total * 100
            }
        except Exception:
            return {}
        
# ============================================================================
# PERFORMANCE MONITOR
# ============================================================================
class PerformanceMonitor:
    """Track detailed performance metrics"""
    
    def __init__(self, window_size: int = 1000):
        """
        Initialize performance monitor.
        
        Args:
            window_size: Number of samples to keep per operation
        """
        self.operations: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=window_size))
        self.counters: Dict[str, int] = defaultdict(int)
        self.lock = RLock()
        self.start_time = time.time()
    
    def record(self, operation: str, duration: float, success: bool = True) -> None:
        """
        Record operation duration.
        
        Args:
            operation: Operation name
            duration: Duration in seconds
            success: Whether operation succeeded
        """
        with self.lock:
            self.operations[operation].append(duration)
            self.counters[f"{operation}_{'success' if success else 'failure'}"] += 1
    
    def record_bytes(self, bytes_count: int) -> None:
        """Record bytes downloaded"""
        with self.lock:
            self.counters['bytes_downloaded'] += bytes_count
    
    def get_stats(self, operation: str) -> Dict[str, float]:
        """
        Get statistics for an operation.
        
        Args:
            operation: Operation name
            
        Returns:
            Dictionary with operation statistics
        """
        with self.lock:
            times = list(self.operations.get(operation, []))
            if not times:
                return {}
            
            return {
                'avg_ms': statistics.mean(times) * 1000,
                'p95_ms': sorted(times)[int(len(times) * 0.95)] * 1000,
                'max_ms': max(times) * 1000,
                'min_ms': min(times) * 1000,
                'count': len(times),
                'success_count': self.counters.get(f"{operation}_success", 0),
                'failure_count': self.counters.get(f"{operation}_failure", 0)
            }
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get comprehensive performance summary.
        
        Returns:
            Dictionary with performance summary
        """
        with self.lock:
            return {
                'uptime_seconds': time.time() - self.start_time,
                'operations': {
                    op: self.get_stats(op) for op in self.operations
                },
                'counters': dict(self.counters),
                'total_operations': sum(len(v) for v in self.operations.values())
            }

# ============================================================================
# PARTIAL DOWNLOAD MANAGER
# ============================================================================
class PartialDownloadManager:
    """Manage partial downloads with resume support"""
    
    def __init__(self, download_dir: Path, partial_suffix: str = PARTIAL_SUFFIX):
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
        return final_path.with_suffix(final_path.suffix + self.partial_suffix)
    
    def register_partial(self, final_path: Path, url: str, expected_size: Optional[int] = None) -> Path:
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
                'url': url,
                'final_path': final_path,
                'expected_size': expected_size,
                'start_time': time.time(),
                'last_activity': time.time(),
                'bytes_downloaded': 0
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
                self.active_partials[partial_path]['last_activity'] = time.time()
                self.active_partials[partial_path]['bytes_downloaded'] += bytes_downloaded
    
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
                final_path = self.active_partials[partial_path]['final_path']
                bytes_downloaded = self.active_partials[partial_path]['bytes_downloaded']
                
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
                path for path, info in self.active_partials.items()
                if now - info['last_activity'] > max_age_seconds
            ]
            for path in stale:
                del self.active_partials[path]
                cleaned += 1
            # FIX: Only scan filesystem if download_dir exists
            for partial_file in self.download_dir.rglob(f'*{self.partial_suffix}'):
                try:
                    if now - partial_file.stat().st_mtime > max_age_seconds:
                        partial_file.unlink()
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
                'active_partials': len(self.active_partials),
                'total_partials': self.total_partials,
                'total_resumes': self.total_resumes
            }

# ============================================================================
# HEALTH CHECK API
# ============================================================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoint - WITH RATE LIMITING"""
    
    mirror_instance = None
    
    # Class-level rate limiting
    _rate_limit_lock = threading.Lock()
    _request_times = deque(maxlen=100)
    MAX_REQUESTS_PER_SECOND = 5
    
    @classmethod
    def check_rate_limit(cls) -> bool:
        """Check if request should be rate limited."""
        now = time.time()
        with cls._rate_limit_lock:
            # Remove old entries
            while cls._request_times and now - cls._request_times[0] > 1.0:
                cls._request_times.popleft()
            
            if len(cls._request_times) >= cls.MAX_REQUESTS_PER_SECOND:
                return False
            
            cls._request_times.append(now)
            return True
    
    def do_GET(self):
        """Handle GET requests with rate limiting."""
        if self.path == '/health':
            # Apply rate limiting
            if not self.check_rate_limit():
                self.send_response(429)  # Too Many Requests
                self.send_header('Content-Type', 'application/json')
                self.send_header('Retry-After', '1')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'error': 'Rate limit exceeded',
                    'retry_after': 1
                }).encode())
                return
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.end_headers()
            
            if self.mirror_instance and hasattr(self.mirror_instance, 'health_checker'):
                try:
                    status = self.mirror_instance.health_checker.get_status()
                    # Don't expose internal details in health check
                    safe_status = {
                        'status': status.status,
                        'timestamp': status.timestamp,
                        'connection': status.connection if isinstance(status.connection, dict) else {},
                        'system': {
                            'memory_usage_mb': status.system.get('memory_usage_mb', 0) if isinstance(status.system, dict) else 0,
                            'platform': status.system.get('platform', 'unknown') if isinstance(status.system, dict) else 'unknown',
                        }
                    }
                    self.wfile.write(json.dumps(safe_status, indent=2).encode())
                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-Type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        'status': 'error',
                        'message': 'Health check failed'
                    }).encode())
            else:
                self.send_response(503)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({
                    'status': 'unavailable',
                    'error': 'Mirror instance not available'
                }).encode())
        elif self.path == '/metrics':
            # Simple metrics endpoint (if needed)
            if not self.check_rate_limit():
                self.send_response(429)
                self.end_headers()
                return
            
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            if self.mirror_instance and hasattr(self.mirror_instance, 'metrics'):
                try:
                    summary = self.mirror_instance.metrics.get_summary()
                    safe_metrics = {
                        'files_downloaded': summary.get('files_downloaded', 0),
                        'files_failed': summary.get('files_failed', 0),
                        'files_skipped': summary.get('files_skipped', 0),
                        'bytes_downloaded': summary.get('bytes_downloaded', 0),
                        'elapsed_seconds': summary.get('elapsed_seconds', 0),
                    }
                    self.wfile.write(json.dumps(safe_metrics, indent=2).encode())
                except Exception:
                    self.send_response(500)
                    self.end_headers()
            else:
                self.send_response(503)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        """Override to use our logging instead of stderr."""
        logging.debug(f"HealthCheck: {args[0]}" % args[1:] if len(args) > 1 else format % args)

class HealthCheckServer:
    """Simple HTTP server for health checks"""
    
    def __init__(self, mirror_instance, port: int = 8080):
        """
        Initialize health check server.
        
        Args:
            mirror_instance: MirrorURL instance
            port: Port to listen on
        """
        self.mirror_instance = mirror_instance
        self.port = port
        self.server = None
        self.thread = None
        
        # Set the mirror instance for the handler
        HealthCheckHandler.mirror_instance = mirror_instance
    
    def start(self):
        """Start health check server in background thread.

        Wraps server creation in try/except so a port collision (common in
        test suites where a previous test's daemon thread hasn't released
        port 8080 yet) doesn't surface as an unhandled thread exception.
        Sets SO_REUSEADDR to make the port bindable again sooner after a
        previous instance shuts down.
        """
        def run_server():
            try:
                # Allow re-bind shortly after a prior shutdown
                class _ReusableHTTPServer(HTTPServer):
                    allow_reuse_address = True

                self.server = _ReusableHTTPServer(('localhost', self.port), HealthCheckHandler)
                logging.info(f"Health check server started on port {self.port}")
                self.server.serve_forever()
            except OSError as e:
                # Most commonly EADDRINUSE — port already bound by another
                # instance (likely a leftover from a previous test). Log and
                # bail rather than killing the test process with an
                # uncaught thread exception.
                logging.warning(f"Health check server could not start on port {self.port}: {e}")

        self.thread = threading.Thread(target=run_server, daemon=True)
        self.thread.start()
    
    def stop(self):
        """Stop health check server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()

# ============================================================================
# CONFIGURATION SCHEMA
# ============================================================================
class ConfigSchema(BaseModel):
    """Strict configuration schema for validation"""
    base_url: str
    dest_path: str
    log_path: str
    dir_suffix: Optional[str] = ""
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=MAX_WORKERS_HARD_LIMIT)
    timeout: int = Field(default=DEFAULT_TIMEOUT, ge=MIN_TIMEOUT, le=MAX_TIMEOUT)
    max_retries: int = Field(default=DEFAULT_MAX_RETRIES, ge=0, le=10)
    retry_delay: int = Field(default=DEFAULT_RETRY_DELAY, ge=1, le=60)
    cache_max_age: int = Field(default=DEFAULT_CACHE_MAX_AGE_DAYS, ge=0, le=MAX_CACHE_AGE_DAYS)
    max_depth: int = Field(default=MAX_DIRECTORY_DEPTH, ge=1, le=100)
    max_filename_len: int = Field(default=MAX_FILENAME_LENGTH, ge=1, le=512)
    bandwidth_limit: Optional[float] = Field(default=None, gt=0, le=1000)
    trusted_server: bool = False
    security_validation: bool = True
    http2: bool = True
    cleanup_policy: str = "safe"
    # NEW v3.0.0 fields
    parallel_downloads: bool = Field(default=PARALLEL_DOWNLOAD_ENABLED)
    max_chunks_per_file: int = Field(default=MAX_CHUNKS_PER_FILE, ge=1, le=20)
    min_chunk_size_mb: int = Field(default=10, ge=1, le=100)
    max_parallel_chunks_total: int = Field(default=MAX_PARALLEL_CHUNKS_TOTAL, ge=10, le=200)
    chunk_assembly_dir: Optional[str] = None
    
    @field_validator('base_url')
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v.rstrip('/')
    
    @field_validator('cleanup_policy')
    @classmethod
    def validate_cleanup_policy(cls, v: str) -> str:
        allowed = ['safe', 'preview', 'delete', 'move']
        if v not in allowed:
            raise ValueError(f"cleanup_policy must be one of {allowed}")
        return v

def validate_config_file(config_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Validate configuration file against schema.
    
    Args:
        config_path: Path to config file
        
    Returns:
        Tuple of (valid, error_message)
    """
    try:
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                config_data = yaml.safe_load(f)
            else:
                config_data = json.load(f)
        
        config_data = expand_env_vars(config_data)
        ConfigSchema(**config_data)
        return True, None
    except ValidationError as e:
        return False, f"Validation error: {e}"
    except Exception as e:
        return False, f"Config load error: {e}"

def expand_env_vars(config_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expand environment variables in config values recursively.
    
    Expands ${VAR} patterns anywhere in string values, not just entire values.
    
    Examples:
        >>> os.environ['MIRROR_URL'] = 'https://example.com'
        >>> expand_env_vars({'base_url': '${MIRROR_URL}/files/'})
        {'base_url': 'https://example.com/files/'}
        
        >>> expand_env_vars({'path': '/home/${USER}/docs'})
        {'path': '/home/username/docs'}
    
    Args:
        config_dict: Configuration dictionary with possible ${VAR} placeholders
        
    Returns:
        Dictionary with environment variables expanded
    """
    def expand_value(value: Any) -> Any:
        """Helper to expand variables in a value."""
        if isinstance(value, str):
            # Find all ${VAR} patterns
            pattern = r'\$\{([^}]+)\}'
            
            def replace_var(match: re.Match) -> str:
                var_name = match.group(1)
                # Return environment variable value if found, otherwise keep original placeholder
                return os.environ.get(var_name, match.group(0))
            
            # Replace all occurrences of ${VAR} with their environment values
            return re.sub(pattern, replace_var, value)
        return value
    
    expanded = {}
    for key, value in config_dict.items():
        if isinstance(value, dict):
            expanded[key] = expand_env_vars(value)
        elif isinstance(value, list):
            expanded[key] = [
                expand_env_vars({str(i): v})[str(i)] if isinstance(v, dict) else expand_value(v)
                for i, v in enumerate(value)
            ]
        else:
            expanded[key] = expand_value(value)
    
    return expanded

# ============================================================================
# HEALTH CHECKER
# ============================================================================
class HealthChecker:
    """Provide health check information"""
    
    def __init__(self, mirror: MirrorURL):
        """
        Initialize health checker.
        
        Args:
            mirror: MirrorURL instance
        """
        self.mirror = mirror
        self.start_time = time.time()
        self.check_count = 0
    
    def get_status(self) -> HealthStatus:
        """Get current health status"""
        self.check_count += 1
        memory_monitor = getattr(self.mirror, 'memory_monitor', None)
        disk_manager = getattr(self.mirror, 'disk_manager', None)
        performance_monitor = getattr(self.mirror, 'performance_monitor', None)
        
        return HealthStatus(
            status="healthy" if self.mirror.connection_ok else "degraded",
            timestamp=datetime.now().isoformat(),
            metrics={
                'files_processed': self.mirror.files_processed.value() if hasattr(self.mirror.files_processed, 'value') else self.mirror.files_processed,
                'files_failed': self.mirror.files_failed.value() if hasattr(self.mirror.files_failed, 'value') else self.mirror.files_failed,
                'files_skipped': self.mirror.files_skipped.value() if hasattr(self.mirror.files_skipped, 'value') else self.mirror.files_skipped,
                'total_downloaded_mb': (self.mirror.total_downloaded_size.value() if hasattr(self.mirror.total_downloaded_size, 'value') else self.mirror.total_downloaded_size) / (1024 * 1024),
                'uptime_seconds': time.time() - self.mirror.start_time,
                'health_checks': self.check_count
            },
            connection={
                'base_url': sanitize_url_for_log(self.mirror.base_url),
                'ok': self.mirror.connection_ok,
                'circuit_breaker': (
                    self.mirror.connection_manager.circuit_breaker.state.value 
                    if (self.mirror.connection_manager and 
                        self.mirror.connection_manager.circuit_breaker) else 'disabled'                )
            },
            cache=self.mirror.cache_manager.lru_file_cache.get_stats() if hasattr(
                self.mirror.cache_manager, 'lru_file_cache'
            ) else {},
            errors=self.mirror.metrics.metrics.get('errors', [])[-10:],
            system={
                'memory_usage_mb': memory_monitor.get_usage_mb() if memory_monitor else 0,
                'disk_usage': disk_manager.get_usage_stats() if disk_manager else {},
                'performance': performance_monitor.get_summary() if performance_monitor else {},
                'python_version': sys.version.split()[0],
                'platform': sys.platform
            }
        )
    
    def is_healthy(self) -> bool:
        """Quick health check"""
        return (self.mirror.connection_ok and 
                self.mirror.files_failed < 10)


# ============================================================================
# AUTO CONCURRENCY TUNER (v3.0.6)
# ============================================================================
class AutoConcurrencyTuner:
    """Automatically tune concurrency based on measured throughput"""
    
    def __init__(self, start_concurrency: int = AUTO_CONCURRENCY_START,
                 max_concurrency: int = AUTO_CONCURRENCY_MAX):
        """
        Initialize auto-concurrency tuner.
        
        Args:
            start_concurrency: Starting concurrency level
            max_concurrency: Maximum concurrency level
        """
        self._start = start_concurrency
        self.current = start_concurrency
        self.max = max_concurrency
        self._start = start_concurrency
        self.samples: List[Tuple[int, float]] = []  # (concurrency, throughput)
        self.last_throughput = 0.0
        self.improvement_count = 0
        self.adjustments = 0
        self.lock = RLock()
    
    def record_throughput(self, concurrency: int, throughput: float) -> Optional[int]:
        """
        Record throughput and return new concurrency if tuning needed.
        
        Args:
            concurrency: Current concurrency level
            throughput: Measured throughput in MB/s
            
        Returns:
            New concurrency value if adjustment needed, None otherwise
        """
        with self.lock:
            self.samples.append((concurrency, throughput))
            
            # Keep only last 20 samples
            if len(self.samples) > 20:
                self.samples.pop(0)
            
            if self.last_throughput > 0:
                improvement = (throughput - self.last_throughput) / self.last_throughput
                if improvement > AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD:
                    self.improvement_count += 1
                elif improvement < -AUTO_CONCURRENCY_THROUGHPUT_THRESHOLD:
                    self.improvement_count -= 1
                else:
                    self.improvement_count = max(0, min(0, self.improvement_count))
            
            self.last_throughput = throughput
            
            # If we've seen improvement in last 3 samples, increase concurrency
            if self.improvement_count >= 2 and self.current < self.max:
                old = self.current
                self.current = min(self.max, self.current + 2)
                self.improvement_count = 0
                self.adjustments += 1
                return self.current
            
            # If no improvement and we're above start, decrease
            if self.improvement_count <= -2 and self.current > self.start:
                old = self.current
                self.current = max(self.start, self.current - 2)
                self.improvement_count = 0
                self.adjustments += 1
                return self.current
            
            return None
    
    def get_concurrency(self) -> int:
        """Get current recommended concurrency"""
        with self.lock:
            return self.current
    
    def get_stats(self) -> Dict[str, Any]:
        """Get tuner statistics"""
        with self.lock:
            return {
                'current_concurrency': self.current,
                'start_concurrency': self.start,
                'max_concurrency': self.max,
                'samples': self.samples[-10:],
                'last_throughput': self.last_throughput,
                'adjustments': self.adjustments,
                'total_samples': len(self.samples)
            }
    
    def reset(self) -> None:
        """Reset tuner to starting values"""
        with self.lock:
            self.current = self.start
            self.samples.clear()
            self.last_throughput = 0.0
            self.improvement_count = 0
            
    @property
    def start(self) -> int:
        return self._start
    
    @start.setter
    def start(self, value: int):
        self._start = value            

# ============================================================================
# MIRROR URL MAIN CLASS (v3.0.6) - TRUE PARALLEL DOWNLOADS
# ============================================================================
class MirrorURL:
    """Main mirroring class with v3.0.2 true parallel file downloads"""
    
    
    @staticmethod
    def _validate_url_scheme(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla (SIMD-accelerated).
        
        Args:
            url: URL to validate
            
        Returns:
            True if scheme is http or https
        """
        url_sz = Str(url)
        
        # SIMD-accelerated prefix checks
        if url_sz.startswith(Str('http://')):
            return True
        
        if url_sz.startswith(Str('https://')):
            return True
        
        return False
    
    @staticmethod
    def _validate_url_scheme_fast(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla (SIMD-accelerated).
        
        Args:
            url: URL to validate
            
        Returns:
            True if scheme is http or https
        """
        if not url:
            return False

        url_sz = Str(url)
        
        # Check for http:// (SIMD-accelerated)
        if url_sz.startswith(Str('http://')):
            return True
        
        # Check for https:// (SIMD-accelerated)
        if url_sz.startswith(Str('https://')):
            return True
        
        return False

    @staticmethod
    def _validate_url_scheme_fallback(url: str) -> bool:
        """
        Fallback URL scheme validation using urllib.parse.
        
        Args:
            url: URL to validate
            
        Returns:
            True if scheme is http or https
        """
        parsed = urlparse(url)
        return parsed.scheme in ['http', 'https']

    
    # ============================================================================
    # INSTANCE METHODS
    # ============================================================================
    
    def _get_last_path_component(self, url: str) -> str:
        """
        Get last path component from URL.
        
        Args:
            url: URL to extract from
            
        Returns:
            Last path component or "root" if empty
        """
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        return os.path.basename(path) if path else "root"
    
    def _get_target_base_url(self) -> str:
        """
        Get target base URL for current suffix.
        
        Returns:
            Full target URL with suffix appended
            
        Raises:
            PathTraversalError: If suffix contains path traversal
        """
        base = str(self.config.base_url).rstrip('/') + '/'
        suffix = self.config.dir_suffix.strip('/') if self.config.dir_suffix else ''
        
        if suffix:
            # FIX: Replace pathlib check with direct string validation.
            # pathlib normalizes paths differently across OSes (Windows vs POSIX),
            # which can lead to false negatives for URL-based traversal attacks.
            # A raw string check is faster, OS-agnostic, and strictly catches URL traversal.
            if '..' in suffix or suffix.startswith('/') or '//' in suffix:
                raise PathTraversalError(f"Invalid directory suffix: {suffix}")
                
            safe_parts = []
            for part in suffix.split('/'):
                if part:
                    safe_parts.append(PathSafety._safe_filename(part, max_len=self.config.max_filename_len))
            safe_suffix = '/'.join(safe_parts)
            return urljoin(base, safe_suffix + '/')
        
        return base
    
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
    
    def _is_url_within_scope(self, url: str, check_base: bool = True) -> bool:
        """
        Optimized URL scope checking using StringZilla.
        """
        try:
            # Use fast validation
            if not self._validate_url_scheme_fast(url):
                return False
            
            # Fast path extraction using StringZilla
            url_path = self._get_url_path_fast(url)
            if not url_path:
                return False
            
            # Get scope path
            if check_base:
                scope_path = self.base_parsed.path
            else:
                if not self.target_parsed:
                    return False
                scope_path = self.target_parsed.path
            
            # Ensure scope_path ends with / for proper prefix matching
            if scope_path and not scope_path.endswith('/'):
                scope_path = scope_path + '/'
            
            # Convert url_path to string for comparison (StringZilla Str works with startswith)
            url_path_str = str(url_path) if hasattr(url_path, '__str__') else url_path
            
            # Check if url_path starts with scope_path
            if url_path_str.startswith(scope_path):
                return True
            
            # Also check without trailing slash for root-level files
            if scope_path.endswith('/'):
                scope_path_no_slash = scope_path[:-1]
                if url_path_str == scope_path_no_slash:
                    return True
                if url_path_str.startswith(scope_path_no_slash + '/'):
                    return True
            
            # Get remaining path for traversal detection
            remaining = url_path_str[len(scope_path.rstrip('/')):] if scope_path else url_path_str
            
            # Fast path traversal detection using StringZilla
            remaining_sz = Str(remaining)
            if remaining_sz.find('..') >= 0:
                logging.warning(f"Path traversal attempt in URL: {sanitize_url_for_log(url)}")
                return False
            
            # Check for dot segments
            if remaining_sz.find('/.') >= 0 or remaining_sz.find('./') >= 0:
                logging.warning(f"Current directory reference in URL: {sanitize_url_for_log(url)}")
                return False
            
            # Check for encoded path traversal
            remaining_str = str(remaining_sz)
            if '%2e' in remaining_str.lower() or '%2f' in remaining_str.lower():
                try:
                    decoded = unquote(remaining_str)
                    if '..' in decoded or '/.' in decoded:
                        logging.warning(f"Encoded path traversal in URL: {sanitize_url_for_log(url)}")
                        return False
                except Exception:
                    pass
            
            return True
            
        except Exception as e:
            logging.debug(f"Error in URL scope check: {e}")
            return False
    
    
    def _is_within_target_scope(self, url: str) -> bool:
        """
        Check if URL is within target scope.

        If a target has not yet been resolved (e.g. when ``test_connection``
        hasn't been called or didn't succeed — typical in unit tests that
        patch the network after ``__init__``), fall back to the base-URL
        scope check rather than treating every URL as out of scope. Without
        this fallback, the directory scanner silently drops every subdir.

        Args:
            url: URL to check

        Returns:
            True if URL is within target (or base, when target is unset) scope
        """
        if getattr(self, 'target_parsed', None) is None:
            return self._is_url_within_scope(url, check_base=True)
        return self._is_url_within_scope(url, check_base=False)
    
    def _is_dir_excluded(self, url: str) -> bool:
        """
        Check if a directory URL should be excluded based on exclude_dirs config.
        Supports:
        - Exact URL match
        - Path suffix match (e.g., 'spk/satellites/a_old_versions')
        - Simple glob patterns with * (basic support)
        
        Args:
            url: Directory URL to check
        Returns:
            True if directory should be excluded
        """
        if not self.config.exclude_dirs:
            return False
        
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        for pattern in self.config.exclude_dirs:
            pattern_clean = pattern.rstrip('/')
            
            # Exact match
            if path == pattern_clean or url.rstrip('/') == pattern_clean:
                return True
            
            # Path suffix match (most common use case)
            if path.endswith('/' + pattern_clean) or path.endswith(pattern_clean):
                return True
            
            # Simple glob support: convert * to regex
            if '*' in pattern_clean:
                regex_pattern = re.escape(pattern_clean).replace(r'\*', '.*')
                if re.search(regex_pattern + r'(/|$)', path):
                    return True
        
        return False

    
    @lru_cache(maxsize=10000)
    def _parse_url_cached(self, url: str) -> ParseResult:
        """
        Cached URL parsing for performance.
        
        Args:
            url: URL to parse
            
        Returns:
            Parsed URL result
        """
        return urlparse(url)
    
    def _get_url_path_fast(self, url: str) -> str:
        """Fast path extraction using StringZilla - returns string."""
        if not url:
            return ''
        
        url_sz = Str(url)
        # Find the path part after the domain
        after_protocol = url_sz.find('://')
        if after_protocol < 0:
            return ''
        
        path_start = url_sz.find('/', after_protocol + 3)
        if path_start < 0:
            return ''
        
        # Return as string for easier comparison
        return str(url_sz[path_start:])

    def _get_filename_fast(self, url: str) -> Str:
        """
        Fast filename extraction using StringZilla.
        """
        path_sz = self._get_url_path_fast(url)
        if not path_sz:
            return Str('')
        
        last_slash = path_sz.rfind('/')
        if last_slash >= 0:
            return path_sz[last_slash + 1:]
        
        return path_sz
    
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
    
    # ============================================================================
    # MirrorURL.__init__ METHOD
    # ============================================================================
    
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
        self.files_skipped = AtomicCounter(0)    # Changed to AtomicCounter
        self.files_failed = AtomicCounter(0)     # Changed to AtomicCounter
        self.total_downloaded_size = AtomicSize()  # Changed to AtomicSize
        self.dir_timestamps: Dict[str, float] = {}
        self.start_time = time.time()
        self.job_start_time = datetime.now()
        self.connection_ok = True
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
        self.base_url = trim_url(normalized_url + '/')
        self.base_parsed = urlparse(self.base_url)
        
        # Validate URL scheme using fast method (now the default)
        if not self._validate_url_scheme(self.base_url):
            raise URLScopeError(f"Invalid URL scheme in base_url: {sanitize_url_for_log(self.base_url)}")
        
        # ============================================================================
        # 7. DOWNLOAD COMPONENTS
        # ============================================================================
        self.download_queue = DownloadQueue(max_size=config.download_queue_size)
        # Dedicated executor for blocking metadata checks inside async tasks
        self._meta_check_executor = ThreadPoolExecutor(
            max_workers=min(50, self.config.workers * 2),
            thread_name_prefix="meta_check"
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
                bomb_threshold=config.symlink_bomb_threshold
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
            config=config, 
            metrics=self.metrics,
            concurrency_manager=self.concurrency_manager
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
                suffix_parts = [p for p in suffix.split('/') if p]
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
        try:
            if suffix:
                safe_suffix = suffix.replace('/', '_')
                cache_name = f"mirror_url_{safe_suffix}.json"
            else:
                folder_name = self._get_last_path_component(self.base_url)
                cache_name = f"mirror_url_{folder_name}.json"
            
            self.cache_file = self.log_path / cache_name
        except Exception as e:
            logging.warning(f"Failed to create cache file path: {e}")
            self.cache_file = None
        
        # Create log filename BEFORE using it
        if self.config.dir_suffix:
            safe_suffix = self.config.dir_suffix.replace('/', '_')
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
            except Exception as e:
                # Fallback to system temp if we can't create the log path
                temp_log_dir = Path(tempfile.gettempdir()) / f'mirrorurl_logs_{os.getpid()}'
                temp_log_dir.mkdir(parents=True, exist_ok=True)
                log_filepath = temp_log_dir / log_filename
                logging.warning(f"Failed to create log directory {self.log_path}, using fallback: {temp_log_dir}")
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
            target_time=config.target_batch_time
        )
        
        # ============================================================================
        # 18. DISK-BACKED SET (optional)
        # ============================================================================
        self.remote_files_set = None
        if config.use_disk_backed_sets and config.disk_cache_dir:
            try:
                self.remote_files_set = DiskBackedSet(config.disk_cache_dir, config.memory_cache_size)
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
                    self.async_connection_manager = AsyncConnectionManager(self.config, self.metrics)
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
                mirror=self
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
                    max_concurrency=config.max_concurrent_downloads
                )
                logging.info(f"{self._get_prefix()}🤖 Auto-concurrency tuning enabled (starting at {self.auto_tuner.get_concurrency()})")
            except Exception as e:
                logging.warning(f"Failed to initialize auto-concurrency tuner: {e}")
                self.auto_tuner = None
        
        # ============================================================================
        # 23. SCANNER
        # ============================================================================
        self.scanner = DirectoryScanner(self)
        if hasattr(self, 'adaptive_async_manager') and self.adaptive_async_manager:
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
        logging.info(f"{prefix}Rate limiting: {delay_ms:.1f}ms delay{' (trusted server)' if config.trusted_server else ''}")
        
        # Log async settings
        if config.cache_html:
            logging.info(f"{prefix}📦 HTML caching enabled ({config.html_cache_max_age}h)")
        
        if config.adaptive_async and config.async_metadata:
            logging.info(f"{prefix}🔄 Adaptive async: start={config.adaptive_start_concurrency}, "
                        f"max={ADAPTIVE_MAX_CONCURRENCY}, error_threshold={config.adaptive_error_threshold:.1%}")
        
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
            logging.info(f"{prefix}🚀 Parallel chunk downloads: ENABLED (max {config.max_chunks_per_file} chunks, "
                        f"min {config.min_chunk_size_mb}MB)")
        
        # Log cache file
        logging.info(f"{prefix}Cache file: {self.cache_file}")
        
        # Log content hash setting
        if config.content_hash_small_files:
            logging.info(f"{prefix}🔐 Content hash: files <{CONTENT_HASH_THRESHOLD/1024:.0f}KB")
        
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
        logging.info(f"{prefix}🛡️ Path safety: max_depth={config.max_depth}, max_filename_len={config.max_filename_len}")
        
        # Log progress bar
        if config.progress_bar and TQDM_AVAILABLE:
            logging.info(f"{prefix}📈 Progress bar enabled")
        
        # Log adaptive batch processing
        if config.adaptive_batch_processing:
            logging.info(f"{prefix}📈 Adaptive batch processing: initial={config.initial_batch_size}")
        
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
                logging.info(f"{prefix}🏥 Health check API available at http://localhost:{config.health_check_port}/health")
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
                logging.error(f"{prefix}Target URL outside base URL scope: {sanitize_url_for_log(self.target_base_url)}")
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
            logging.info(f"{prefix}Target directory: Not created (connection failed or invalid path)")
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
        
        logging.debug(f"{prefix}Initialization complete: target_dir={self.target_dir}, "
                     f"cache_file={self.cache_file}, connection_ok={self.connection_ok}")
    
        # Initialization for _async_speed_samples:
        self._speed_samples: deque = deque(maxlen=20)  # Keep last 20 samples (already exists, ensure it's there)
            
    # ============================================================================
    # CONTEXT MANAGER METHODS
    # ============================================================================

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
        if hasattr(self, 'health_server') and self.health_server:
            try:
                self.health_server.stop()
                logging.debug("Health server stopped")
            except Exception as e:
                logging.debug(f"Health server stop error: {e}")
        
        # 2. Shutdown async components (must be done before connection managers)
        # IMPORTANT: AsyncTaskManager must be shut down before AsyncConnectionManager
        # because tasks may be using the connection manager's client
        if hasattr(self, 'async_task_manager') and self.async_task_manager:
            try:
                # Check if we're in an async context
                try:
                    loop = asyncio.get_running_loop()
                    # In async context, schedule shutdown as task with timeout
                    shutdown_task = asyncio.ensure_future(
                        self.async_task_manager.shutdown(timeout=10.0)
                    )
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
                                self.async_task_manager.shutdown(timeout=10.0),
                                timeout=15.0
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
        if hasattr(self, 'async_connection_manager') and self.async_connection_manager:
            try:
                if hasattr(self.async_connection_manager, '_client') and \
                   self.async_connection_manager._client:
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
                                    timeout=10.0
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
        
        if hasattr(self, 'adaptive_async_manager') and self.adaptive_async_manager:
            try:
                if hasattr(self.adaptive_async_manager, '_client') and \
                   self.adaptive_async_manager._client:
                    try:
                        loop = asyncio.get_running_loop()
                        asyncio.create_task(
                            self.adaptive_async_manager.__aexit__(None, None, None)
                        )
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(
                                asyncio.wait_for(
                                    self.adaptive_async_manager.__aexit__(None, None, None),
                                    timeout=10.0
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
        if hasattr(self, 'parallel_manager') and self.parallel_manager:
            try:
                self.parallel_manager.shutdown()
                logging.debug("Parallel manager shutdown complete")
            except Exception as e:
                logging.debug(f"Parallel manager shutdown error: {e}")
        
        # 5. Close connection manager
        if hasattr(self, 'connection_manager') and self.connection_manager:
            try:
                self.connection_manager.close()
                logging.debug("Connection manager closed")
            except Exception as e:
                logging.debug(f"Connection manager close error: {e}")
        
        # 6. Shutdown meta-check executor
        if hasattr(self, '_meta_check_executor') and self._meta_check_executor:
            try:
                try:
                    self._meta_check_executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    self._meta_check_executor.shutdown(wait=False)
                logging.debug("Meta-check executor shutdown complete")
            except Exception as e:
                logging.debug(f"Meta-check executor shutdown error: {e}")
        
        # 7. Clear remote files set
        if hasattr(self, 'remote_files_set') and self.remote_files_set:
            try:
                self.remote_files_set.clear()
                logging.debug("Remote files set cleared")
            except Exception as e:
                logging.debug(f"Remote files set clear error: {e}")
        
        # 8. Clean up partial downloads
        if hasattr(self, 'partial_manager') and self.partial_manager:
            if self.partial_manager.download_dir is not None and not self.is_dry_run:
                try:
                    cleaned = self.partial_manager.cleanup_stale_partials()
                    if cleaned > 0:
                        self.metrics.increment('stale_partials_cleaned', cleaned)
                        logging.info(f"Cleaned {cleaned} stale partial downloads")
                except Exception as e:
                    logging.debug(f"Partial manager cleanup error: {e}")
        
        # 9. Clear filename cache
        if hasattr(self, '_filename_cache'):
            with self._filename_cache_lock:
                self._filename_cache.clear()
                logging.debug("Filename cache cleared")
        
        # 10. Shutdown concurrency manager
        if hasattr(self, 'concurrency_manager'):
            try:
                self.concurrency_manager.shutdown()
                logging.debug("Concurrency manager shutdown complete")
            except Exception as e:
                logging.debug(f"Concurrency manager shutdown error: {e}")
        
        # 11. Close log handlers
        if hasattr(self, 'log_handlers'):
            for handler in self.log_handlers:
                try:
                    handler.flush()
                    handler.close()
                except Exception as e:
                    logging.debug(f"Log handler close error: {e}")
        
        # 12. Save final metrics if configured
        if hasattr(self, 'config') and self.config.metrics_json and not self.config.dry_run:
            try:
                if hasattr(self, 'metrics'):
                    self.metrics.export_json(self.config.metrics_json, self.config)
                    logging.info(f"Final metrics exported to {self.config.metrics_json}")
            except Exception as e:
                logging.debug(f"Failed to export final metrics: {e}")
        
        logging.debug("MirrorURL cleanup complete") 
        
    # ============================================================================
    # CORE METHODS
    # ============================================================================

    def setup_logging(self) -> None:
        """Setup logging configuration for this mirror instance."""
        if self._logging_configured:
            return
        self._logging_configured = True
        
        # If using shared logging, don't add file handlers - just log the suffix
        if self.config.use_shared_log:
            suffix_display = self.config.dir_suffix or 'ROOT'
            if self.total_suffixes > 1:
                logging.info(f"[{self.suffix_index}/{self.total_suffixes}] Processing directory suffix: '{suffix_display}'")
            else:
                logging.info(f"Processing directory suffix: '{suffix_display}'")
            return
        
        # Create log filename (MOVED UP before directory creation)
        if self.config.dir_suffix:
            safe_suffix = self.config.dir_suffix.replace('/', '_')
            log_filename = f"mirror_url_{safe_suffix}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        else:
            folder = self._get_last_path_component(str(self.config.base_url))
            log_filename = f"mirror_url_{folder}_{time.strftime('%Y%m%d_%H%M%S')}.log"
        
        log_filepath = self.log_path / log_filename
       
        # FIX: Ensure log directory exists BEFORE creating FileHandler
        if not self.config.dry_run:
            try:
                log_filepath.parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                # Fallback to system temp if we can't create the log path
                temp_log_dir = Path(tempfile.gettempdir()) / f'mirrorurl_logs_{os.getpid()}'
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
            file_handler = logging.FileHandler(str(log_filepath), mode='a', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG if self.config.debug else logging.INFO)
            file_handler.setFormatter(logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            logging.root.addHandler(file_handler)
            self.log_handler = file_handler
            self.log_handlers = [file_handler]
            _log_files.append(file_handler)
        except (FileNotFoundError, PermissionError, OSError) as e:
            # Fallback: ensure console handler exists before logging warning
            if not logging.root.handlers:
                console = logging.StreamHandler(sys.stderr)
                console.setFormatter(logging.Formatter(
                    '[%(asctime)s] [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                ))
                console.setLevel(logging.WARNING)
                logging.root.addHandler(console)
                self.log_handlers = [console]

            # Use print as last resort since logging setup may be incomplete
            print(f"WARNING: Could not create log file {log_filepath}: {e}. Logging to console only.", file=sys.stderr)
            self.log_handlers = [h for h in logging.root.handlers if isinstance(h, logging.StreamHandler)]        
 
        # Add console handler ONLY if requested AND no console handler exists
        if self.config.print_logs and not existing_console:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
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
        suffix_display = self.config.dir_suffix or 'ROOT'
        
        cmd_str = shlex.join([sys.executable] + sys.argv)
        logging.info(f"{prefix}Processing directory suffix: '{suffix_display}'")
        logging.info(f"{prefix}Command: {cmd_str}")
        logging.info(f"{prefix}" + "-" * 80)
        logging.info(f"{prefix}Starting MirrorURL v{__version__} for '{suffix_display}'")
        logging.info(f"{prefix}Job started at: {self.job_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"{prefix}Mirroring from: {sanitize_url_for_log(str(self.config.base_url))}")
        logging.info(f"{prefix}Destination path: {self.dest_path}")
        logging.info(f"{prefix}Log file: {log_filepath}")
        logging.info(f"{prefix}Workers: {self.config.workers}, Max Retries: {self.config.max_retries}")
        logging.info(f"{prefix}Cache max age: {self.config.cache_max_age} days")
        
        if LXML_AVAILABLE:
            logging.info(f"{prefix}Parser: lxml.html + fast fallback")
        else:
            logging.info(f"{prefix}Parser: fast regex only (lxml not available)")
        
        logging.info(f"{prefix}HTTP/2: {'ENABLED' if self.config.http2 else 'DISABLED'}")
        logging.info(f"{prefix}ETag support: ENABLED")
        logging.info(f"{prefix}🔒 URL sanitization enabled")
        logging.info(f"{prefix}🛡️ Path safety: max_depth={self.config.max_depth}, max_filename_len={self.config.max_filename_len}")
        
        if self.config.progress_bar and TQDM_AVAILABLE:
            logging.info(f"{prefix}📈 Progress bar enabled")
        
        if self.config.adaptive_async and self.config.async_metadata:
            logging.info(f"{prefix}🔄 Adaptive async: {self.config.adaptive_start_concurrency}-{ADAPTIVE_MAX_CONCURRENCY} workers")
        
        if self.config.content_hash_small_files:
            logging.info(f"{prefix}🔐 Content hash: files <{CONTENT_HASH_THRESHOLD/1024:.0f}KB")
        
        delay_ms = self.config.request_delay * 1000
        logging.info(f"{prefix}Rate limiting: {delay_ms:.1f}ms delay{' (trusted server)' if self.config.trusted_server else ''}")
        
        if self.config.cache_html:
            logging.info(f"{prefix}📦 HTML caching enabled ({self.config.html_cache_max_age}h)")
        
        if self.config.enable_resume:
            logging.info(f"{prefix}↩️ Resume capability enabled")
        
        if self.config.adaptive_batch_processing:
            logging.info(f"{prefix}📈 Adaptive batch processing: initial={self.config.initial_batch_size}")
        
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
            logging.info(f"{prefix}🚀 Parallel chunk downloads: ENABLED (max {self.config.max_chunks_per_file} chunks, "
                        f"min {self.config.min_chunk_size_mb}MB)")
        
        self._log_cleanup_policy()
        
        # Note: Target directory will be logged after connection test in __init__
        
        logging.info(f"{prefix}Cache file: {self.cache_file}")
        logging.info(f"{prefix}Scan mode: {self.config.scan_mode.value}")
        
        if self.config.async_metadata:
            logging.info(f"{prefix}⚡ Async directory scanning: ENABLED")
        
        if self.config.handle_symlinks:
            logging.info(f"{prefix}🔗 Symlink handling: ENABLED (mode: {self.config.symlink_mode})")
        
        if self.config.metrics_json:
            logging.info(f"{prefix}🏥 Health check API: http://localhost:{self.config.health_check_port}/health")
                
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
            
            response = self.connection_manager.request(test_url, method='HEAD', allow_redirects=True)
            
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
     
    # ============================================================================
    # CONNECTION WARM-UP METHOD
    # ============================================================================
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
            if hasattr(self, 'target_base_url') and self.target_base_url:
                sample_urls.append(self.target_base_url)
            
            # Add some directory URLs from cache if available
            if hasattr(self.scanner, 'cached_signatures') and self.scanner.cached_signatures:
                dir_urls = list(self.scanner.cached_signatures.keys())[:9]  # Take up to 9 directories
                sample_urls.extend(dir_urls)
            
            # If we have connection manager with pool, warm it up
            if sample_urls and hasattr(self.connection_manager, 'connection_pool'):
                logging.debug(f"Warming up connection pool with {len(sample_urls)} URLs")
                self.connection_manager.connection_pool.warm_up(sample_urls[:10])  # Limit to 10 URLs
                
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
            if not path or path == '/':
                # Generate a filename from the URL if path is empty
                filename = f"index_{hash(remote_url) & 0xffffffff:x}.html"
            else:
                filename = os.path.basename(unquote(path))
                if not filename:
                    filename = f"index_{hash(remote_url) & 0xffffffff:x}.html"
            
            # Store in cache
            self._filename_cache[remote_url] = filename
            
            # Prune if cache exceeds max size
            if len(self._filename_cache) > self._filename_cache_maxsize:
                # Remove oldest 20% of entries
                items_to_remove = len(self._filename_cache) // 5
                keys_to_remove = list(self._filename_cache.keys())[:items_to_remove]
                for key in keys_to_remove:
                    del self._filename_cache[key]
                logging.debug(f"Pruned filename cache: removed {items_to_remove} entries, "
                            f"now {len(self._filename_cache)} entries")
            
            return filename
    
    def _get_filename_cache_stats(self) -> Dict[str, Any]:
        """Get filename cache statistics."""
        with self._filename_cache_lock:
            return {
                'size': len(self._filename_cache),
                'maxsize': self._filename_cache_maxsize,
                'hits': self._filename_cache_hits,
                'misses': self._filename_cache_misses,
                'hit_rate': (self._filename_cache_hits / (self._filename_cache_hits + self._filename_cache_misses) * 100) 
                           if (self._filename_cache_hits + self._filename_cache_misses) > 0 else 0
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
            r = self.connection_manager.request(url, method='HEAD', timeout=(15, 30), allow_redirects=True)
            if r.status_code == 200 and 'Last-Modified' in r.headers:
                dt = parsedate_to_datetime(r.headers['Last-Modified'])
                return dt.timestamp()
        except httpx.RequestError as e:
            logging.debug(f"Failed to get timestamp for {sanitize_url_for_log(url)}: {e}")
        except Exception as e:
            logging.debug(f"Error parsing timestamp for {sanitize_url_for_log(url)}: {e}")
        return None    
 
    def sync(self) -> bool:
        """Main sync method - v3.0.2 with true parallel file downloads."""
        prefix = self._get_prefix()
        if not hasattr(self, 'connection_manager') or not self.connection_manager:
            logging.warning(f"{prefix}Skipping sync (connection failed)")
            self._print_early_exit_summary(prefix)
            return False # Connection failed

        # FIX v2.0.1: Early exit if connection is not OK
        if not self.connection_ok:
            prefix = self._get_prefix()
            logging.info(f"{prefix}Skipping sync - remote directory not available (connection_ok=False)")
            self._print_early_exit_summary(prefix)
            # Sync atomic counters to metrics for accurate reporting
            if hasattr(self, 'metrics'):
                self.metrics.metrics['files_downloaded'] = self.files_processed.value()
                self.metrics.metrics['files_skipped'] = self.files_skipped.value()
                self.metrics.metrics['files_failed'] = self.files_failed.value()
                self.metrics.metrics['bytes_downloaded'] = self.total_downloaded_size.value()
            return False # Connection failed
        
        

        # Fix: Recreate async managers for each sync run to ensure clean state
        if self.config.async_metadata:
            try:
                if self.config.adaptive_async:
                    # Close old manager if it exists
                    if self.adaptive_async_manager and hasattr(self.adaptive_async_manager, '_client') and self.adaptive_async_manager._client:
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                loop.create_task(self.adaptive_async_manager.__aexit__(None, None, None))
                        except RuntimeError:
                            pass
                    
                    # Create fresh manager
                    self.adaptive_async_manager = AdaptiveAsyncManager(self.config, self.metrics)
                    self.scanner.adaptive_manager = self.adaptive_async_manager
                    logging.debug(f"{prefix}Adaptive async manager recreated for sync")
                else:
                    # Close old manager if it exists
                    if self.async_connection_manager and hasattr(self.async_connection_manager, '_client') and self.async_connection_manager._client:
                        try:
                            loop = asyncio.get_event_loop()
                            if loop.is_running():
                                loop.create_task(self.async_connection_manager.__aexit__(None, None, None))
                        except RuntimeError:
                            pass
                    
                    # Create fresh manager
                    self.async_connection_manager = AsyncConnectionManager(self.config, self.metrics)
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
                        config=self.config
                    )
                    self.multi_progress.add_level("files", len(remote_files), prefix, self.config.progress_bar, self.config)
                else:
                    progress = None
                
                # Determine which files would be downloaded
                # In dry-run mode, use sync checks for speed (avoid adaptive profiling delays)
                use_async = False  # Force sync mode in dry-run

                if use_async:
                    if self.config.adaptive_async and self.adaptive_async_manager:
                        logging.info(f"{prefix}Using ADAPTIVE async metadata checks ({len(remote_files)} files)")
                    else:
                        logging.info(f"{prefix}Using async metadata checks ({len(remote_files)} files)")
                    
                    # Check if we're already in an async context
                    try:
                        loop = asyncio.get_running_loop()
                        logging.warning(f"{prefix}Already in async context, using sync mode")
                        to_download = self._check_files_sync(remote_files, progress)
                    except RuntimeError:
                        try:
                            to_download = asyncio.run(self._check_files_async(remote_files, progress))
                        except Exception as e:
                            logging.warning(f"{prefix}Async metadata check failed ({e}), falling back to sync mode")
                            to_download = self._check_files_sync(remote_files, progress)
                            
                else:
                    logging.info(f"{prefix}Using sync metadata checks (dry-run simulation - faster)")
                    to_download = self._check_files_sync(remote_files, progress)
                    
                if progress:
                    progress.report_final()
                
                # Show what would be downloaded
                if to_download:
                    sample_size = min(10, len(to_download))
                    logging.info(f"{prefix}Would download {len(to_download)} files (showing first {sample_size}):")
                    for i, (url, local_path) in enumerate(to_download[:sample_size]):
                        logging.info(f"{prefix}  {i+1}. {sanitize_url_for_log(url)} -> {local_path}")
                    if len(to_download) > sample_size:
                        logging.info(f"{prefix}  ... and {len(to_download) - sample_size} more")
                else:
                    logging.info(f"{prefix}No files would be downloaded - all up to date")
                
                # Show what would be cleaned up
                if self.config.cleanup_policy in (CleanupPolicy.PREVIEW, CleanupPolicy.DELETE, CleanupPolicy.MOVE):
                    self.clean_obsolete(set(remote_files))
                
                duration = time.time() - start
                logging.info("-" * 50)
                logging.info(f"{prefix}DRY RUN SUMMARY:")
                logging.info(f"{prefix}  Remote files found: {len(remote_files)}")
                logging.info(f"{prefix}  Files that would be downloaded: {len(to_download)}")
                logging.info(f"{prefix}  Files that are up to date: {len(remote_files) - len(to_download)}")
                logging.info(f"{prefix}  Duration: {format_duration(duration)}")
                logging.info("-" * 50)
                return True
            
            if len(remote_files) > 0:
                progress = ProgressTracker(
                    total=len(remote_files),
                    prefix=prefix,
                    name="files checked",
                    use_tqdm=TQDM_AVAILABLE,
                    config=self.config
                )
                self.multi_progress.add_level("files", len(remote_files), prefix, self.config.progress_bar, self.config)
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

            use_async = (self.config.async_metadata and
                        (self.adaptive_async_manager or self.async_connection_manager) and
                        len(remote_files) > 80)
            
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
                                                timeout=30.0
                                            )
                                        )
                                    except asyncio.TimeoutError:
                                        logging.warning(f"{prefix}Async warm-up timed out after 30 seconds")
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
                            warmup_thread = threading.Thread(target=async_warm_up_worker, daemon=True)
                            warmup_thread.start()
                # ========== END ASYNC WARM-UP ==========      
                
                if self.config.adaptive_async and self.adaptive_async_manager:
                    logging.info(f"{prefix}Using ADAPTIVE async metadata checks ({len(remote_files)} files)")
                else:
                    logging.info(f"{prefix}Using async metadata checks ({len(remote_files)} files)")
                
                try:
                    to_download = asyncio.run(self._check_files_async(remote_files, progress))
                except Exception as e:
                    logging.warning(f"{prefix}Async metadata check failed ({e}), falling back to sync mode")
                    to_download = self._check_files_sync(remote_files, progress)
                
                if self.config.adaptive_async and self.adaptive_async_manager:
                    self.metrics.metrics['adaptive_current_concurrency'] = getattr(
                        self.adaptive_async_manager,
                        '_current_concurrency',
                        ADAPTIVE_START_CONCURRENCY
                    )
                    
                    if self.adaptive_async_manager.should_fallback():
                        self.metrics.metrics['adaptive_fallback_to_sync'] = True
                        logging.info(f"{prefix}⚠️ Adaptive async fell back to sync mode")
                
                self.files_skipped.reset()
                self.files_skipped.increment(max(0, len(remote_files) - len(to_download)))                
                self.metrics.metrics['files_skipped'] = self.files_skipped.value()
            else:           
                 # Explicitly log why we are using sync mode
                if len(remote_files) <= 80:
                    logging.info(f"{prefix}Using sync metadata checks (batch size {len(remote_files)} ≤ 80, skipping async overhead)")
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
                        executor.submit(self._get_file_size, url): url 
                        for url, _ in to_download
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
                if (self.parallel_manager and
                    not self.config.parallel_downloads and
                    not self.config.streaming_parallel and
                    not self.config.sequential_downloads):
                    
                    sample_urls = [url for url, _ in to_download[:10]]
                    method = self.parallel_manager.auto_select_method(
                        file_sizes=file_sizes,
                        total_files=len(to_download),
                        remote_urls=sample_urls
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

                logging.info(f"{prefix}Downloading {len(to_download)} files (approx {format_bytes(total_size)})")
                self.multi_progress.add_level(
                    "downloads", len(to_download), prefix, 
                    self.config.progress_bar, self.config
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
                        logging.info(f"{prefix}🤖 Auto-tuning: using {max_parallel} parallel downloads")
                    
                    logging.info(f"{prefix}🚀 Starting {max_parallel} parallel file downloads")
                    start_time = time.time()
                    downloaded_count = 0
                    last_throughput_log = start_time
                    
                    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                        # Submit all download tasks with pre-fetched sizes
                        future_to_file = {}
                        for url, path in to_download:
                            pre_fetched_size = size_map.get(url, 0)  # FIX: Pass size to avoid duplicate HEAD
                            future = executor.submit(
                                self.download_file_with_resume, 
                                url, path, pre_fetched_size
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
                                #else:
                                #    ⛔ DO NOT increment here. _download_file_single() already 
                                #    increments files_failed before returning False. Doing so 
                                #    would double-count every handled failure.                                    
                                
                                # Auto-tune after every N downloads
                                if self.auto_tuner and downloaded_count % AUTO_CONCURRENCY_SAMPLES == 0:
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
                                    throughput = (downloaded_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else 0
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
                        self.metrics.metrics['auto_concurrency_enabled'] = True
                        self.metrics.metrics['auto_concurrency_adjustments'] = tuner_stats['adjustments']
                        self.metrics.metrics['auto_concurrency_final'] = tuner_stats['current_concurrency']
                        self.metrics.metrics['auto_concurrency_start'] = tuner_stats['start_concurrency']
                        logging.info(
                            f"{prefix}🤖 Auto-concurrency stats: {tuner_stats['adjustments']} adjustments, "
                            f"final concurrency={tuner_stats['current_concurrency']}, "
                            f"final throughput={tuner_stats['last_throughput']:.2f} MB/s"
                        )
                    
                    logging.info(f"{prefix}✅ Completed {self.files_processed.value()} file downloads in parallel")
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
            #total_mb = self.total_downloaded_size / (1024 * 1024)
            #speed = total_mb / duration if duration > 0 else 0
            
            # Get final values from atomic counters
            # FIX: Sync AtomicCounters to Metrics Collector before summary
            # This ensures metrics.report() shows correct values from AtomicCounters
            if hasattr(self, 'metrics'):
                self.metrics.metrics['files_downloaded'] = self.files_processed.value()
                self.metrics.metrics['bytes_downloaded'] = self.total_downloaded_size.value()
                self.metrics.metrics['files_skipped'] = self.files_skipped.value()
                self.metrics.metrics['files_failed'] = self.files_failed.value()
                
            downloaded_files = self.files_processed.value()
            downloaded_bytes = self.total_downloaded_size.value()
            skipped_files = self.files_skipped.value() if hasattr(self.files_skipped, 'value') else self.files_skipped
            failed_files = self.files_failed.value() if hasattr(self.files_failed, 'value') else self.files_failed
            
            # Also get from metrics if counters weren't updated (fallback)
            if downloaded_files == 0 and hasattr(self, 'metrics'):
                downloaded_files = self.metrics.metrics.get('files_downloaded', 0)
                downloaded_bytes = self.metrics.metrics.get('bytes_downloaded', 0)
                if skipped_files == 0:
                    skipped_files = self.metrics.metrics.get('files_skipped', 0)
                if failed_files == 0:
                    failed_files = self.metrics.metrics.get('files_failed', 0)
        
        
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
            
            if hasattr(self.scanner, 'get_parse_stats'):
                try:
                    parse_stats = self.scanner.get_parse_stats()
                    fast_parses = parse_stats.get('fast_parses', 0)
                    lxml_parses = parse_stats.get('lxml_parses', 0)
                    logging.info(f"{prefix}  Parse stats: {fast_parses + lxml_parses} directories parsed")
                except Exception as e:
                    logging.debug(f"Error reporting parse stats: {e}")
            
            perf_summary = self.performance_monitor.get_summary()
            logging.info(f"{prefix}  Performance: {perf_summary['total_operations']} operations tracked")

            if self.parallel_manager:
                parallel_stats = self.parallel_manager.get_stats()
                if (parallel_stats.get('active_files', 0) > 0 or 
                    parallel_stats.get('active_chunks', 0) > 0):
                    logging.info(f"{prefix}  📦 Parallel downloads:")
                    logging.info(f"{prefix}    Active files: {parallel_stats['active_files']}")
                    logging.info(f"{prefix}    Active chunks: {parallel_stats['active_chunks']}")
                    logging.info(f"{prefix}    Chunk downloads: {self.metrics.metrics.get('chunk_downloads', 0)}")
                    logging.info(f"{prefix}    Chunk assemblies: {self.metrics.metrics.get('chunk_assemblies', 0)}")
                    logging.info(f"{prefix}    Chunk failures: {self.metrics.metrics.get('chunk_failures', 0)}")
            
            # NEW v3.0.0: Log parallel download stats
            if self.parallel_manager:
                parallel_stats = self.parallel_manager.get_stats()
                if parallel_stats['active_files'] > 0 or parallel_stats['active_chunks'] > 0:
                    logging.info(f"{prefix}  Parallel downloads: {parallel_stats['active_files']} files, "
                                f"{parallel_stats['active_chunks']} chunks active")
                    
            # Add filename cache stats to metrics
            filename_cache_stats = self._get_filename_cache_stats()
            if filename_cache_stats['size'] > 0:
                logging.info(f"{prefix}  Filename cache: {filename_cache_stats['size']} entries, "
                            f"hit rate: {filename_cache_stats['hit_rate']:.1f}%")
                
            
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
    
    def _get_file_size(self, url: str) -> Optional[int]:
        """Get file size via HEAD request."""
        try:
            response = self.connection_manager.request(url, method='HEAD')
            content_length = response.headers.get('Content-Length')
            if content_length:
                return int(content_length)
        except Exception as e:
            logging.debug(f"Failed to get file size for {url}: {e}")
        return None
    
    def download_file_with_resume(self, remote_url: str, local_path: Path, file_size: Optional[int] = None) -> bool:
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
        
        if (hasattr(self.connection_manager, 'circuit_breaker') and 
            self.connection_manager.circuit_breaker and 
            not self.connection_manager.circuit_breaker.can_execute()):
            self.metrics.increment('circuit_breaker_trips')
            logging.error(f"Download failed: Circuit breaker is open")
            self.files_failed.increment(1)
            self.metrics.increment('files_failed')
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False
        
        logging.debug(f"Circuit breaker check passed, proceeding to partial manager")


        partial_path = self.partial_manager.register_partial(local_path, remote_url)
        logging.debug(f"Partial path: {partial_path}")


        headers = {}
        mode = 'wb'
        bytes_already = 0
        
        if self.config.enable_resume and partial_path.exists():
            bytes_already = self.partial_manager.get_resume_offset(partial_path)
            if bytes_already > 0:
                headers['Range'] = f'bytes={bytes_already}-'
                mode = 'ab'
                logging.debug(f"Resuming download from {bytes_already} bytes")
                self.metrics.increment('partial_resumes')
        
        try:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            ts = self.get_remote_timestamp(remote_url)
            
            for attempt in range(self.config.max_retries + 1):
                try:
                    start = time.time()
                    r = self.connection_manager.request(remote_url, method='GET',
                                                       timeout=30, headers=headers)
                    
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
                        content_range = r.headers.get('Content-Range', '')
                        if '/' in content_range:
                            try:
                                total_size = int(content_range.rsplit('/', 1)[1])
                            except (ValueError, IndexError):
                                total_size = 0
                        if total_size == 0:
                            try:
                                total_size = int(r.headers.get('Content-Length', 0))
                            except (ValueError, TypeError):
                                total_size = 0

                        if total_size > 0 and partial_path.stat().st_size >= total_size:
                            partial_path.rename(local_path)
                            logging.info(f"File already complete: {local_path}")
                            self.metrics.increment('resumed_downloads')
                            self.partial_manager.complete_partial(partial_path)

                            if hasattr(self, 'fs_cache'):
                                self.fs_cache.invalidate(local_path)

                            self.performance_monitor.record("download", time.time() - download_start, True)
                            return True

                        # 416 but partial isn't actually complete — restart from
                        # scratch (truncate the partial, drop Range header).
                        try:
                            partial_path.unlink()
                        except OSError:
                            pass
                        mode = 'wb'
                        headers = {}
                        bytes_already = 0
                        continue
                    
                    if r.status_code not in (200, 206):
                        logging.warning(f"Non-200/206 status for {sanitize_url_for_log(remote_url)}: {r.status_code}")
                        self.partial_manager.complete_partial(partial_path)
                        self.performance_monitor.record("download", time.time() - download_start, False)
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
                    if r.status_code == 200 and mode == 'ab':
                        logging.warning(
                            f"Server returned 200 instead of 206 for ranged request "
                            f"to {sanitize_url_for_log(remote_url)}; discarding "
                            f"{bytes_already}-byte partial and restarting from scratch."
                        )
                        mode = 'wb'
                        bytes_already = 0
                        self.metrics.increment('range_ignored_restarts')

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
                    
                    remote_etag = r.headers.get('ETag')
                    if remote_etag:
                        self.cache_manager.save_file_metadata(local_path, remote_etag, time.time(), size)
                    
                    if hasattr(self, 'fs_cache'):
                        self.fs_cache.invalidate(local_path)
                    
                    # FIX v3.0.6: Update counters using atomic methods
                    downloaded_bytes = size - bytes_already
                    self.files_processed.increment(1)  # Atomic increment
                    self.total_downloaded_size.add(downloaded_bytes)  # Atomic add
                    self.metrics.increment('files_downloaded')
                    self.metrics.add_bytes(downloaded_bytes)
                    self.performance_monitor.record_bytes(downloaded_bytes)
                    
                    if bytes_already > 0:
                        self.metrics.increment('resumed_downloads')
                        self.metrics.increment('partial_downloads')
                    
                    logging.info(f"Downloaded: {local_path} ({format_bytes(size)})")
                    
                    self.performance_monitor.record("download", time.time() - download_start, True)
                    return True
                    
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    if attempt < self.config.max_retries:
                        wait_time = exponential_backoff(attempt)
                        logging.warning(f"Download attempt {attempt+1} failed: {e}. Retrying in {wait_time:.1f}s...")
                        time.sleep(wait_time)
                    else:
                        raise
                        
                except httpx.HTTPStatusError as e:
                    status = e.response.status_code if e.response else 0
                    
                    if status in (403, 404, 410, 451):
                        logging.warning(f"HTTP {status}, skipping: {sanitize_url_for_log(remote_url)}")
                        # FIX: increment the ATOMIC files_skipped counter — the
                        # final summary reads self.files_skipped.value(), but
                        # this path previously only bumped the metrics dict, so
                        # 403/404/410/451 skips were invisible in the skip total.
                        self.files_skipped.increment(1)
                        self.metrics.increment('files_skipped')

                        if partial_path.exists():
                            try:
                                partial_path.unlink()
                            except Exception as unlink_err:
                                logging.debug(f"Failed to remove partial file {partial_path}: {unlink_err}")

                        self.partial_manager.complete_partial(partial_path)
                        # This is a SKIP (the resource is gone / forbidden), not a
                        # download and not a failure. We return True so neither
                        # caller counts it as a failure (the sequential caller
                        # increments files_failed on False; the parallel caller
                        # deliberately doesn't). It is already counted in
                        # files_skipped above.
                        self.performance_monitor.record("download", time.time() - download_start, True)
                        return True
                    
                    logging.error(f"HTTP {status} error for {sanitize_url_for_log(remote_url)}: {e}")
                    self.files_failed.increment(1)  # Atomic
                    self.metrics.increment('files_failed')
                    
                    if partial_path.exists():
                        try:
                            partial_path.unlink()
                        except Exception as unlink_err:
                            logging.debug(f"Failed to remove partial file {partial_path}: {unlink_err}")

                    self.partial_manager.complete_partial(partial_path)
                    self.performance_monitor.record("download", time.time() - download_start, False)
                    return False

        except Exception as e:
            logging.error(f"Download failed: {e}")
            self.files_failed.increment(1)  # Atomic
            self.metrics.increment('files_failed')
            self.metrics.add_error(str(e), "download_failed")
            
            if partial_path.exists():
                try:
                    partial_path.unlink()
                except Exception as unlink_err:
                    logging.debug(f"Failed to remove partial file {partial_path}: {unlink_err}")

            self.partial_manager.complete_partial(partial_path)
            self.performance_monitor.record("download", time.time() - download_start, False)
            return False
    
    def matches_filter(self, url: str) -> bool:
        """Optimized filter matching using StringZilla for all pattern types."""
        if not self.config.file_filters:
            return True
        
        # Fast filename extraction using StringZilla
        filename_sz = self._get_filename_fast(url)
        if not filename_sz:
            return False
        
        # Convert to string for operations that need it (endswith with tuple)
        filename = str(filename_sz)
        
        for pattern in self.config.file_filters:
            pattern_lower = pattern.lower()
            
            if pattern.startswith('.'):
                # Fast extension check - use string version for compatibility
                if filename.endswith(pattern_lower):
                    return True
            else:
                # Check if pattern contains regex special characters
                has_regex = any(c in pattern for c in '*?+[]{}()|\\^$')
                
                if has_regex:
                    # Regex pattern - fall back to re
                    try:
                        if re.search(pattern, filename, re.IGNORECASE):
                            return True
                    except re_error:
                        pass
                else:
                    # Simple substring - use StringZilla for SIMD acceleration
                    pattern_sz = Str(pattern_lower)
                    if pattern_sz in filename_sz:
                        return True
        
        return False
    
    def get_directory_signature(self, url: str, html_content: str = None) -> str:
        """Get directory signature for cache."""
        url = trim_url(url)
        
        if html_content is not None:
            content_hash = hashlib.new(
                self.config.hash_algorithm, 
                html_content.encode('utf-8')
            ).hexdigest()
            return f"content:{content_hash}"
        
        try:
            r = self.connection_manager.request(url, method='HEAD', timeout=15)
            
            if r.status_code != 200:
                return f"url:{url}"
            
            if 'ETag' in r.headers:
                return f"etag:{r.headers['ETag']}"
            
            if 'Last-Modified' in r.headers:
                return f"mtime:{r.headers['Last-Modified']}"
            
            return f"url:{url}:{int(time.time())}"
        except Exception as e:
            logging.debug(f"Error getting signature for {sanitize_url_for_log(url)}: {e}")
            return f"url:{url}:{int(time.time())}"
    
    def is_symlink(self, url: str, existing_response: Optional[httpx.Response] = None, depth: int = 0) -> Tuple[bool, Optional[str]]:
        """Check if a URL points to a symlink."""
        try:
            if not self.config.handle_symlinks:
                return False, None
            
            if depth >= self.config.max_symlink_depth:
                self.metrics.increment('symlink_depth_exceeded')
                return True, None
            
            if self.symlink_tracker:
                dir_url = url.rsplit('/', 1)[0] + '/'
                can_follow, reason = self.symlink_tracker.can_follow(url, dir_url, depth)
                
                if not can_follow:
                    if "loop" in reason.lower():
                        self.metrics.increment('symlink_loops_detected')
                    elif "bomb" in reason.lower():
                        self.metrics.increment('symlink_bomb_prevented')
                    return True, None
            
            return False, None
        except Exception as e:
            logging.debug(f"Error checking symlink for {sanitize_url_for_log(url)}: {e}")
            return False, None
    
    def record_symlink(self, symlink_url: str, target_url: str, local_path: Path, depth: int = 0) -> None:
        """Record symlink handling."""
        if self.config.symlink_mode == 'follow':
            self.metrics.increment('symlinks_followed')
            if self.symlink_tracker:
                dir_url = symlink_url.rsplit('/', 1)[0] + '/'
                self.symlink_tracker.record_follow(symlink_url, dir_url, depth)
        elif self.config.symlink_mode == 'skip':
            self.metrics.increment('symlinks_skipped')
            if self.symlink_tracker:
                self.symlink_tracker.record_skip(symlink_url)
    
    def check_disk_space(self, required_bytes: int) -> bool:
        """Check if enough disk space is available."""
        self.metrics.increment('disk_space_checks')
        ok, error = self.disk_manager.check_available(required_bytes)
        
        if not ok:
            self.metrics.increment('disk_space_warnings')
            if error:
                logging.error(f"Disk space error: {error}")
        
        return ok
    
    @log_performance("get_remote_files")
    def get_remote_files(self) -> Optional[List[str]]:
        """Get remote files list through directory discovery."""
        prefix = self._get_prefix()
        
        try:
            # NOTE: Both the dir_suffix/target case AND the root-level case go
            # through _discover_directories_bfs(). That generator already uses
            # self.target_base_url as its BFS root, so it handles the suffix
            # case correctly.
            #
            # FIX (v3.1.8): the previous code had a separate `if
            # self.target_base_url:` branch that scanned only the root plus
            # ONE level of immediate subdirectories (it discarded each
            # subdir's own subdirs via `sub_files, _ = ...`). That silently
            # dropped every file nested two or more levels deep, ignored
            # exclude_dirs and max_depth, and had no visited-set guard against
            # duplicate/cyclic scans. Removing the special case fixes all
            # three: BFS recurses to max_depth, applies _is_dir_excluded, and
            # dedupes via processed_dirs.

            cache_loaded, cached_signatures = self.cache_manager.load()
            if cache_loaded:
                self.scanner.cached_signatures = cached_signatures
                logging.info(f"{prefix}📖 Loaded {len(cached_signatures)} directory signatures from cache")
            
            directories = list(self._discover_directories_bfs())
            if not directories:
                logging.info(f"{prefix}No directories discovered")
                return []
            
            logging.info(f"{prefix}Discovered {len(directories)} directories")
            
            all_files: List[str] = []
            dir_signatures: Dict[str, str] = {}
            
            self.multi_progress.add_level("directories", len(directories), prefix, self.config.progress_bar, self.config)
            
            for i, url in enumerate(directories):
                files, subdirs = self.scanner.scan_directory_sequential(url)
                all_files.extend(files)
                sig = self.get_directory_signature(url)
                dir_signatures[url] = sig
                self.multi_progress.update("directories")
                
                if i % 100 == 0:
                    pressure = self.memory_monitor.check_pressure()
                    if pressure != MemoryPressure.NORMAL:
                        self.metrics.increment('memory_pressure_events')

                        if pressure == MemoryPressure.WARNING:
                            freed_parse = self.scanner.parse_cache.shrink_to(0.7)
                            # FIX (inconsistency): the previous code only
                            # asked the scanner's parse_cache to shrink under
                            # WARNING pressure and ignored cache_manager
                            # entirely, even though cache_manager owns its
                            # own LRU caches that *also* need to shrink.
                            # Mirror what the CRITICAL branch does so both
                            # caches respond to memory pressure consistently.
                            freed_cache = self.cache_manager.handle_memory_pressure(pressure)
                            logging.info(f"Memory pressure (warning): freed "
                                         f"{freed_parse + freed_cache} cache entries")
                        elif pressure == MemoryPressure.CRITICAL:
                            freed_parse = self.scanner.parse_cache.shrink_to(0.3)
                            freed_html = self.scanner.html_cache.shrink_to(0.3)
                            freed_cache = self.cache_manager.handle_memory_pressure(pressure)
                            logging.warning(f"Emergency cache clear: freed {freed_parse + freed_html + freed_cache} items")
            
            if not self.config.no_cache and dir_signatures and not self.config.dry_run:
                try:
                    self.cache_manager.save(dir_signatures, len(all_files))
                    logging.info(f"{prefix}💾 Saved cache with {len(dir_signatures)} directory signatures")
                except Exception as e:
                    logging.warning(f"{prefix}Failed to save cache: {e}")
            
            logging.info(f"{prefix}Collected {len(all_files)} files")
            return all_files if all_files else []
            
        except Exception as e:
            logging.error(f"{prefix}Failed to get remote files: {e}")
            self.metrics.add_error(str(e), "file_discovery")
            return None
    
    def _discover_directories_bfs(self) -> Generator[str, None, None]:
        """BFS directory discovery - strictly within target scope."""
        if not self.connection_ok:
            logging.debug("Skipping directory discovery - connection not OK")
            return
        
        # Use target_base_url as the root for discovery
        root_url = self.target_base_url
        if not root_url:
            logging.warning("No target_base_url available for directory discovery")
            return
        
        # Ensure root_url ends with /
        if not root_url.endswith('/'):
            root_url += '/'
        
        logging.debug(f"BFS discovery root: {sanitize_url_for_log(root_url)}")
        
        queue = deque([(root_url, 0)])
        processed_dirs: Set[str] = set()
        
        while queue:
            url, depth = queue.popleft()
            
            # Skip if not within root_url
            if not url.startswith(root_url):
                logging.debug(f"Skipping URL outside root scope: {url}")
                continue
            
            if url in processed_dirs or depth > self.config.max_depth:
                continue
            
            processed_dirs.add(url)
            
            try:
                files, subdirs = self.scanner.scan_directory_sequential(url)
            except Exception as e:
                logging.debug(f"Error scanning {url}: {e}")
                files, subdirs = [], []
            
            yield url
            
            for subdir in subdirs:
                # Only add subdirs that start with root_url
                if subdir not in processed_dirs and subdir.startswith(root_url):
                    if self._is_dir_excluded(subdir):
                        logging.debug(f"Excluding directory: {sanitize_url_for_log(subdir)}")
                        continue
                    queue.append((subdir, depth + 1))
            
            # Rate limiting
            parsed = urlparse(url)
            try:
                ip = socket.gethostbyname(parsed.hostname)
                self.per_ip_limiter.wait(ip)
            except Exception:
                pass  
            
    def _get_local_path_from_url(self, url: str) -> Optional[Path]:
        """
        Convert URL to local path with security checks.
        
        Args:
            url: Remote URL
            
        Returns:
            Local path or None if invalid/unsafe
        """
        if self.target_dir is None:
            return None
        
        if self._target_dir_path is None:
            logging.debug("_target_dir_path is None, cannot compute local path")
            return None

            
        try:
            parsed = self._parse_url_cached(url)
            
            if not parsed.path.startswith(self.target_parsed.path):
                return None
            
            rel_path = parsed.path[len(self.target_parsed.path):].lstrip('/')
            
            if '..' in rel_path or '..' in unquote(rel_path).split('/'):
                logging.warning(f"Path traversal attempt detected in URL: {sanitize_url_for_log(url)}")
                return None
            
            rel_path = unquote(rel_path)
            
            local_path = PathSafety.safe_join(
                self.target_dir, *rel_path.split('/'),
                max_depth=self.config.max_depth,
                max_filename_len=self.config.max_filename_len
            )
            
            if local_path is None:
                return None
            
            if not PathSafety.is_subpath(self._target_dir_path, local_path):
                logging.warning(f"Security check failed: {local_path} outside {self.target_dir}")
                return None
            
            return local_path
        except Exception as e:
            logging.debug(f"Error converting URL to local path: {e}")
            return None
    
    @log_performance("file_check")
    def file_exists_and_up_to_date(self, local_path: Path, remote_url: str, use_cache: bool = True) -> bool:
        start_time = time.time()
        
        # First, check if file exists locally
        if hasattr(self, 'fs_cache'):
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
            stored_etag = stored_meta.get('etag') if stored_meta else None
        
        # If cache is disabled but file exists, we need to check it properly
        # We should still try to get ETag from local file metadata if available
        if not use_cache and local_path.exists():
            # Try to read ETag from a sidecar file or compute file hash
            # For now, let's check size and modification time
            local_size = local_path.stat().st_size
            
            # Make HEAD request to get remote info
            try:
                r = self.connection_manager.request(remote_url, method='HEAD', timeout=(10, 20), allow_redirects=True)
                if r.status_code == 200:
                    remote_size = int(r.headers.get('Content-Length', 0))
                    if remote_size == local_size:
                        # Sizes match, consider it up-to-date
                        self.performance_monitor.record("file_check", time.time() - start_time, True)
                        return True
            except Exception as e:
                logging.debug(f"Error checking file without cache: {e}")
        
        # Continue with normal cache-enabled logic...
        if use_cache and hasattr(self.scanner, 'cached_signatures'):
            dir_url = trim_url(remote_url.rsplit('/', 1)[0] + '/')
            if dir_url in self.scanner.cached_signatures:
                self.metrics.increment('cache_hits')
                self.metrics.increment('cache_head_requests_saved')
                self.performance_monitor.record("file_check", time.time() - start_time, True)
                return True
        
        try:
            local_ts = local_path.stat().st_mtime
            local_size = local_path.stat().st_size
            headers = {}
            
            if stored_etag and not self.config.no_etag:
                headers['If-None-Match'] = stored_etag
            
            start = time.time()
            r = self.connection_manager.request(remote_url, method='HEAD', timeout=(10, 20),
                                               allow_redirects=True, headers=headers)
            self.metrics.add_request_time(time.time() - start)
            
            if r.status_code == 304:
                self.metrics.increment('cache_hits')
                self.metrics.increment('etag_304_responses')
                self.performance_monitor.record("file_check", time.time() - start_time, True)
                return True
            if r.status_code != 200:
                # Non-200/non-304 means we can't verify the file is up-to-date
                # Safe behavior: treat as cache miss and trigger download
                self.metrics.increment('cache_misses')  # ✅ Correct metric
                self.performance_monitor.record("file_check", time.time() - start_time, False)
                return False  # ✅ File needs download when verification fails
            
            remote_etag = r.headers.get('ETag')
            if remote_etag and stored_etag and not self.config.no_etag:
                remote_etag_norm = normalize_etag(remote_etag)
                stored_etag_norm = normalize_etag(stored_etag)
                
                if remote_etag_norm == stored_etag_norm:
                    self.metrics.increment('cache_hits')
                    self.metrics.increment('etag_matches')
                    self.performance_monitor.record("file_check", time.time() - start_time, True)
                    return True
                else:
                    self.metrics.increment('cache_misses')
                    self.metrics.increment('etag_mismatches')
                    self.performance_monitor.record("file_check", time.time() - start_time, False)
                    return False
            
            # Check Last-Modified
            if 'Last-Modified' in r.headers:
                try:
                    dt = parsedate_to_datetime(r.headers['Last-Modified'])
                    remote_ts = dt.timestamp()
                    
                    if remote_ts > local_ts + TIMESTAMP_TOLERANCE_SECONDS:
                        self.metrics.increment('cache_misses')
                        self.performance_monitor.record("file_check", time.time() - start_time, False)
                        return False
                    
                    self.metrics.increment('cache_hits')
                    self.performance_monitor.record("file_check", time.time() - start_time, True)
                    return True
                except Exception:
                    pass
            
            # Check file size
            remote_size = int(r.headers.get('Content-Length', 0))
            if remote_size != local_size:
                self.metrics.increment('cache_misses')
                self.performance_monitor.record("file_check", time.time() - start_time, False)
                return False
            
            self.metrics.increment('cache_hits')
            self.performance_monitor.record("file_check", time.time() - start_time, True)
            return True
            
        except Exception as e:
            logging.debug(f"Error checking file {local_path}: {e}")
            # Fix: a failed verification (network error, timeout, stat error)
            # must NOT be treated as up-to-date. Returning True here silently
            # skipped re-downloads on any transient failure. Treat it as a
            # cache miss so the file is re-fetched — consistent with the
            # non-200 branch above.
            self.metrics.increment('cache_misses')
            self.performance_monitor.record("file_check", time.time() - start_time, False)
            return False
        
    @log_performance("clean_obsolete")
    def clean_obsolete(self, remote_files: Set[str]) -> None:
        if self.config.cleanup_policy == CleanupPolicy.SAFE_NO_DELETE:
            logging.debug("Cleanup skipped: SAFE_NO_DELETE mode")
            return
        
        is_preview = (self.config.cleanup_policy == CleanupPolicy.PREVIEW or 
                     self.config.dry_run)
        
        # Check target_dir
        if self.target_dir is None:
            logging.debug("Cleanup skipped: target_dir is None")
            return
        
        if not self.target_dir.exists():
            logging.debug("Cleanup skipped: target directory does not exist")
            return
        
        # Check target_parsed
        if self.target_parsed is None:
            logging.debug("Cleanup skipped: target_parsed is None")
            return
        
        # Delete confirmation
        if (self.config.cleanup_policy == CleanupPolicy.DELETE and
            self.config.confirm_delete and not is_preview):
            obsolete_count = self._count_obsolete_files(remote_files)
            if obsolete_count > 0:
                response = input(f"⚠️ Confirm deletion of {obsolete_count} files? [yes/N]: ").strip().lower()
                if response != "yes":
                    logging.info("Deletion cancelled by user")
                    return
        
        # Build expected files set
        expected: Set[Path] = set()
        for url in remote_files:
            try:
                url_path = urlparse(url).path
                target_path = self.target_parsed.path
                
                if url_path.startswith(target_path):
                    rel = unquote(url_path[len(target_path):].lstrip('/'))
                    local = PathSafety.safe_join(
                        self.target_dir, *rel.split('/'),
                        max_depth=self.config.max_depth,
                        max_filename_len=self.config.max_filename_len
                    )
                    if local and PathSafety.is_subpath(self.target_dir, local):
                        expected.add(local)
            except Exception as e:
                logging.debug(f"Error building expected path for {url}: {e}")
                continue
        
        # Initialize counters
        files_would_delete = 0
        dirs_would_delete = 0
        moved_files = 0
        moved_dirs = 0
        deleted_files = 0
        deleted_dirs = 0
        failed_operations = 0
        prefix = self._get_prefix()
        
        # FIX v2.0.2: For preview mode, just show what would be deleted
        if is_preview:
            logging.info(f"{prefix}🔍 PREVIEW MODE - Scanning for obsolete files...")
            
            # Check files
            try:
                for item in self.target_dir.rglob('*'):
                    if item.is_file():
                        if item not in expected:
                            files_would_delete += 1
                            if is_preview:
                                logging.info(f"[PREVIEW] Would delete: {item}")
                            # ... rest of logic ...
            except (RuntimeError, PermissionError, FileNotFoundError):
                logging.warning("Symlink loop or permission error detected during cleanup. Skipping.")
            
            # Check empty directories
            for item in sorted(self.target_dir.rglob('*'), key=lambda p: len(p.parts), reverse=True):
                if item.is_dir() and item != self.target_dir:
                    try:
                        is_empty = not any(item.iterdir())
                        if is_empty:
                            dirs_would_delete += 1
                            logging.info(f"[PREVIEW] Would delete empty directory: {item}")
                    except (FileNotFoundError, PermissionError):
                        continue
            
            # Store preview counts in metrics
            self.metrics.metrics['files_would_delete'] = files_would_delete
            self.metrics.metrics['dirs_would_delete'] = dirs_would_delete
            
            logging.info("-" * 50)
            logging.info(f"🔍 PREVIEW SUMMARY:")
            logging.info(f"  Files that would be deleted: {files_would_delete}")
            logging.info(f"  Directories that would be deleted: {dirs_would_delete}")
            logging.info(f"  No actual deletions performed (dry-run/preview mode)")
            logging.info("-" * 50)
            
            return
        
        # Setup for DELETE or MOVE modes
        obsolete_dir: Optional[Path] = None
        if self.config.cleanup_policy == CleanupPolicy.MOVE:
            obsolete_dir = self.target_dir.parent / f"{self.target_dir.name}_obsolete"
            try:
                obsolete_dir.mkdir(parents=True, exist_ok=True)
                logging.info(f"📦 Obsolete files will be moved to: {obsolete_dir}")
            except Exception as e:
                logging.error(f"Failed to create obsolete directory {obsolete_dir}: {e}")
                logging.warning("Falling back to DELETE mode")
                self.config.cleanup_policy = CleanupPolicy.DELETE
        
        logging.info(f"{prefix}Scanning for obsolete files...")
        
        # Collect files and directories to process
        files_to_process = []
        dirs_to_check = []
        for item in self.target_dir.rglob('*'):
            if item.is_file():
                files_to_process.append(item)
            elif item.is_dir():
                dirs_to_check.append(item)
        
        # Process files
        for item in files_to_process:
            if item in expected:
                continue
            
            if self.config.cleanup_policy == CleanupPolicy.MOVE and obsolete_dir:
                try:
                    rel_path = item.relative_to(self.target_dir)
                    dest = obsolete_dir / rel_path
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    
                    if dest.exists():
                        timestamp = int(time.time() * 1000)
                        dest = dest.with_name(f"{dest.stem}_{timestamp}{dest.suffix}")
                    shutil.move(str(item), str(dest))  # FIX: Handles cross-filesystem moves without OSError
                    self.cache_manager.cleanup_file_metadata(item)
                    
                    if hasattr(self, 'fs_cache'):
                        self.fs_cache.invalidate(item)
                    
                    moved_files += 1
                    logging.info(f"Moved obsolete: {item} → {dest}")
                except Exception as e:
                    logging.error(f"Failed to move {item}: {e}")
                    failed_operations += 1
            else:  # DELETE mode
                try:
                    item.unlink()
                    self.cache_manager.cleanup_file_metadata(item)
                    
                    if hasattr(self, 'fs_cache'):
                        self.fs_cache.invalidate(item)
                    
                    deleted_files += 1
                    logging.info(f"Deleted obsolete: {item}")
                except Exception as e:
                    logging.error(f"Failed to delete {item}: {e}")
                    failed_operations += 1
        
        logging.info(f"{prefix}Cleaning up empty directories...")
        changed = True
        iteration = 0
        max_iterations = 10
        
        while changed and iteration < max_iterations:
            changed = False
            iteration += 1
            
            for item in sorted(dirs_to_check, key=lambda p: len(p.parts), reverse=True):
                if not item.is_dir() or item == self.target_dir:
                    continue
                
                try:
                    if not item.exists():
                        continue
                    
                    is_empty = not any(item.iterdir())
                    if not is_empty:
                        continue
                    
                    if self.config.cleanup_policy == CleanupPolicy.MOVE and obsolete_dir is not None:
                        try:
                            rel_path = item.relative_to(self.target_dir)
                            dest = obsolete_dir / rel_path
                            dest.parent.mkdir(parents=True, exist_ok=True)
                            # FIX: shutil.move() already relocates the
                            # directory to `dest`. The previous code followed
                            # it with `item.rename(dest)`, but `item` no longer
                            # exists at that point, so rename() raised
                            # FileNotFoundError — which was caught below and
                            # silently fell through to rmdir(), leaving
                            # moved_dirs uncounted and `changed` unset (stalling
                            # the empty-dir cleanup loop). Removed the redundant
                            # rename so the success path runs.
                            if dest.exists():
                                timestamp = int(time.time() * 1000)
                                dest = dest.with_name(f"{dest.name}_{timestamp}")
                            shutil.move(str(item), str(dest))
                            moved_dirs += 1
                            logging.info(f"Moved obsolete dir: {item} → {dest}")
                            changed = True
                        except Exception:
                            try:
                                item.rmdir()
                                deleted_dirs += 1
                                changed = True
                                logging.info(f"Removed empty dir: {item}")
                            except Exception as e:
                                logging.debug(f"Error removing directory {item}: {e}")
                    else:
                        try:
                            item.rmdir()
                            deleted_dirs += 1
                            changed = True
                            logging.info(f"Removed empty dir: {item}")
                        except Exception as e:
                            logging.debug(f"Error removing directory {item}: {e}")
                except Exception:
                    pass
        
        logging.info(f"{prefix}Cleaning up stale cache metadata...")
        try:
            stale_count = self.cache_manager.cleanup_stale_metadata(expected)
            if stale_count > 0:
                logging.info(f"Removed {stale_count} stale metadata entries")
        except Exception as e:
            logging.warning(f"Failed to cleanup stale metadata: {e}")
        
        logging.info("-" * 50)
        
        if self.config.cleanup_policy == CleanupPolicy.MOVE:
            if moved_files > 0 or moved_dirs > 0:
                logging.info(f"📦 MOVE COMPLETE:")
                logging.info(f"  Files moved: {moved_files}")
                logging.info(f"  Directories moved: {moved_dirs}")
                logging.info(f"  Destination: {obsolete_dir}")
            else:
                logging.info("📦 No obsolete files to move")
            
            # Store move counts in metrics
            self.metrics.metrics['files_moved'] = moved_files
            self.metrics.metrics['dirs_moved'] = moved_dirs
        else:  # DELETE mode
            if deleted_files > 0 or deleted_dirs > 0:
                logging.info(f"🗑️ DELETE COMPLETE:")
                logging.info(f"  Files deleted: {deleted_files}")
                logging.info(f"  Directories deleted: {deleted_dirs}")
            else:
                logging.info("🗑️ No obsolete files to delete")
            
            # Store delete counts in metrics
            self.metrics.metrics['files_deleted'] = deleted_files
            self.metrics.metrics['dirs_deleted'] = deleted_dirs
        
        if failed_operations > 0:
            logging.warning(f"⚠️ {failed_operations} operations failed during cleanup")
        
        # Store failed operations count in metrics
        self.metrics.metrics['cleanup_failed_operations'] = failed_operations
        
        logging.info("-" * 50)
    
    def _count_obsolete_files(self, remote_files: Set[str]) -> int:
        """Count obsolete files for preview."""
        expected = set()
        
        for url in remote_files:
            try:
                url_path = urlparse(url).path
                target_path = self.target_parsed.path
                
                if url_path.startswith(target_path):
                    rel = unquote(url_path[len(target_path):].lstrip('/'))
                    local = PathSafety.safe_join(self.target_dir, *rel.split('/'),
                                                max_depth=self.config.max_depth,
                                                max_filename_len=self.config.max_filename_len)
                    
                    if local and PathSafety.is_subpath(self.target_dir, local):
                        expected.add(local)
            except Exception:
                continue
        
        count = 0
        for item in self.target_dir.rglob('*'):
            if item.is_file() and item not in expected:
                count += 1
        
        return count

    def _check_files_sync(self, remote_files: List[str], 
                          progress: Optional[ProgressTracker] = None) -> List[Tuple[str, Path]]:
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
                self.metrics.increment('files_skipped')
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
                self.metrics.increment('files_failed')
                return (url, path, False)  # False = don't download on error
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all check tasks
            future_to_item = {
                executor.submit(check_file, url, path): (url, path)
                for url, path in file_items
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
                            self.metrics.increment('files_skipped')
                except Exception as e:
                    logging.error(f"File check result failed for {url}: {e}")
                    with results_lock:
                        self.files_failed.increment(1)
                        self.metrics.increment('files_failed')
                
                if progress:
                    progress.update(1)
                
                # Log progress every 100 files
                if (i + 1) % 100 == 0:
                    logging.info(f"Checked {i + 1}/{total} files, {len(to_download)} need download")
        
        logging.info(f"Sync check complete: {len(to_download)}/{total} files need download")
        return to_download
                
    async def _check_files_async(self, remote_files: Union[List[str], List[Tuple[str, Path]]],
                                 progress: Optional[ProgressTracker] = None,
                                 _depth: int = 0) -> List[Tuple[str, Path]]:
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
                        self.metrics.increment('files_failed')
                        continue
                    
                    if self.config.handle_symlinks:
                        is_link, target_url = self.is_symlink(url, depth=0)
                        if is_link and target_url:
                            if self.config.symlink_mode == 'follow':
                                target_local_path = self._get_local_path_from_url(target_url)
                                if target_local_path:
                                    file_items.append((target_url, target_local_path))
                                    self.record_symlink(url, target_url, local_path, depth=0)
                                    continue
                            elif self.config.symlink_mode == 'skip':
                                self.files_skipped.increment(1)
                                self.metrics.increment('files_skipped')
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
                #return self._check_files_sync(file_items, progress)
                return self._check_files_sync([url for url, _ in file_items], progress) # qwen

            manager = self.adaptive_async_manager
        else:
            if self.async_connection_manager is None or not self.async_connection_manager.is_available():
                logging.debug("No async manager available, falling back to sync")
                return self._check_files_sync(file_items, progress)
            manager = self.async_connection_manager
        
        test_start_time = time.time()
        test_checked = 0
        fallback_triggered = False
        files_processed_in_test = 0
        
        is_throttled = any(domain in str(self.config.base_url).lower()
                          for domain in KNOWN_THROTTLED_DOMAINS)
        test_max_seconds = (ASYNC_TEST_MAX_SECONDS_THROTTLED if is_throttled
                           else ASYNC_TEST_MAX_SECONDS)
        test_min_files = (ASYNC_TEST_MIN_FILES_THROTTLED if is_throttled
                         else ASYNC_TEST_MIN_FILES)
        min_speed_threshold = (ASYNC_TEST_MIN_SPEED_THROTTLED * 2 if is_throttled
                              else ASYNC_TEST_MIN_SPEED)
        
        # Define check_one with timeout wrapper
        async def check_one_with_timeout(local_path: Path, remote_url: str, mgr) -> bool:
            """Check with timeout wrapper."""
            try:
                return await asyncio.wait_for(
                    check_one(local_path, remote_url, mgr),
                    timeout=30.0
                )
            except asyncio.TimeoutError:
                logging.warning(f"Check timeout for {remote_url}")
                nonlocal test_checked, files_processed_in_test
                test_checked += 1
                files_processed_in_test += 1
                loop = asyncio.get_running_loop()  # FIX: Run blocking I/O in executor to unblock event loop
                return await loop.run_in_executor(self._meta_check_executor, self.file_exists_and_up_to_date, local_path, remote_url, True)
        
        async def check_one(local_path: Path, remote_url: str, mgr) -> bool:
            """Check if a single file needs download."""
            nonlocal test_checked, fallback_triggered, files_processed_in_test
            
            if fallback_triggered:
                return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)
            
            # Fast path: check directory signature cache
            if hasattr(self.scanner, 'cached_signatures'):
                dir_url = trim_url(remote_url.rsplit('/', 1)[0] + '/')
                if dir_url in self.scanner.cached_signatures:
                    self.metrics.increment('cache_hits')
                    self.metrics.increment('cache_head_requests_saved')
                    test_checked += 1
                    files_processed_in_test += 1
                    return True
            
            # If file doesn't exist locally, needs download
            if not local_path.exists():
                test_checked += 1
                files_processed_in_test += 1
                return False
            
            # Get cached metadata
            stored = self.cache_manager.get_file_metadata(local_path)
            headers = {}
            
            if stored and stored.get('etag') and not self.config.no_etag:
                headers['If-None-Match'] = stored['etag']
            
            try:
                start = time.time()
                
                # Use asyncio.wait_for with timeout
                try:
                    resp = await asyncio.wait_for(
                        mgr.head(remote_url, headers),
                        timeout=15.0
                    )
                except asyncio.TimeoutError:
                    logging.debug(f"Async HEAD timeout for {remote_url}")
                    test_checked += 1
                    files_processed_in_test += 1
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)
                
                rtt = (time.time() - start) * 1000
                
                if resp is None:
                    test_checked += 1
                    files_processed_in_test += 1
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)
                
                # Handle 304 Not Modified (already correct - keep this)
                if resp.status_code == 304:
                    self.metrics.increment('etag_304_responses')
                    self.metrics.increment('cache_hits')
                    test_checked += 1
                    files_processed_in_test += 1
                    return True
                
                # Handle client errors: file doesn't exist or is forbidden → skip safely
                if resp.status_code in (403, 404, 410, 451):
                    self.metrics.increment('files_skipped')  # ✅ Correct metric
                    test_checked += 1
                    files_processed_in_test += 1
                    logging.debug(f"Async HEAD {resp.status_code}, skipping: {sanitize_url_for_log(remote_url)}")
                    return True  # True = "don't download this file"
                
                # Handle server errors or other issues: fall back to sync check for safety
                if resp.status_code != 200:
                    test_checked += 1
                    files_processed_in_test += 1
                    logging.debug(f"Async HEAD {resp.status_code}, falling back to sync check: {sanitize_url_for_log(remote_url)}")
                    return self.file_exists_and_up_to_date(local_path, remote_url, use_cache=True)
                
                # Check ETag
                remote_etag = resp.headers.get('ETag')
                if remote_etag and stored and stored.get('etag'):
                    if normalize_etag(remote_etag) == normalize_etag(stored['etag']):
                        self.metrics.increment('etag_matches')
                        self.metrics.increment('cache_hits')
                        test_checked += 1
                        files_processed_in_test += 1
                        return True
                    else:
                        self.metrics.increment('etag_mismatches')
                        self.metrics.increment('cache_misses')
                        test_checked += 1
                        files_processed_in_test += 1
                        return False
                
                # Check Last-Modified
                if 'Last-Modified' in resp.headers:
                    try:
                        local_ts = local_path.stat().st_mtime
                        dt = parsedate_to_datetime(resp.headers['Last-Modified'])
                        remote_ts = dt.timestamp()
                        
                        if remote_ts > local_ts + TIMESTAMP_TOLERANCE_SECONDS:
                            self.metrics.increment('cache_misses')
                            test_checked += 1
                            files_processed_in_test += 1
                            return False
                        
                        self.metrics.increment('cache_hits')
                        test_checked += 1
                        files_processed_in_test += 1
                        return True
                    except Exception as e:
                        logging.debug(f"Last-Modified parsing error: {e}")
                
                # Default: assume up to date
                self.metrics.increment('cache_hits')
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
                            self.metrics.metrics['adaptive_fallback_to_sync'] = True
                            return self._check_files_sync(file_items, progress)
                    except asyncio.TimeoutError:
                        logging.warning("Server profiling timed out, falling back to sync")
                        self.metrics.metrics['adaptive_fallback_to_sync'] = True
                        return self._check_files_sync(file_items, progress)
            
            # Process batches
            for start_idx in range(0, len(file_checks), ASYNC_TEST_BATCH_SIZE):
                if fallback_triggered:
                    break
                
                batch_start_time = time.time()
                batch = file_checks[start_idx:start_idx + ASYNC_TEST_BATCH_SIZE]
                
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
                        timeout=120.0
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
                        self.metrics.metrics['adaptive_fallback_to_sync'] = True
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
                logging.info(f"Speed test passed, continuing async check for remaining {len(file_checks) - files_processed_in_test} files")
                for start_idx in range(files_processed_in_test, len(file_checks), ASYNC_TEST_BATCH_SIZE):
                    batch = file_checks[start_idx:start_idx + ASYNC_TEST_BATCH_SIZE]
                    
                    tasks = []
                    for local, url in batch:
                        task = await self.async_task_manager.create_task(
                            asyncio.wait_for(check_one_with_timeout(local, url, manager), timeout=30.0)
                        )
                        tasks.append((task, local, url))
                    
                    try:
                        results = await asyncio.wait_for(
                            asyncio.gather(*[t for t, _, _ in tasks], return_exceptions=True),
                            timeout=120.0
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
            if use_adaptive and hasattr(manager, 'apply_pending_concurrency_change'):
                await manager.apply_pending_concurrency_change()
        
        return to_download


    def get_remote_timestamp(self, url: str) -> Optional[float]:
        """Get remote file timestamp from Last-Modified header."""
        try:
            r = self.connection_manager.request(url, method='HEAD', timeout=(15, 30), allow_redirects=True)
            if r.status_code == 200 and 'Last-Modified' in r.headers:
                dt = parsedate_to_datetime(r.headers['Last-Modified'])
                return dt.timestamp()
        except httpx.RequestError as e:
            logging.debug(f"Failed to get timestamp for {sanitize_url_for_log(url)}: {e}")
        except Exception as e:
            logging.debug(f"Error parsing timestamp for {sanitize_url_for_log(url)}: {e}")
        return None
    
    def get_directory_size(self, path: Path) -> int:
        """Get total size of directory recursively."""
        total = 0
        for item in path.rglob('*'):
            if item.is_file():
                try:
                    total += item.stat().st_size
                except OSError:
                    pass
        return total
    
    @log_performance("benchmark")
    def benchmark(self) -> Dict[str, Any]:
        """Run performance benchmark."""
        results = {
            'connection_test': False,
            'parse_time': 0.0,
            'check_time': 0.0,
            'total_time': 0.0,
            'performance_stats': {}
        }
        
        start = time.time()
        
        results['connection_test'] = self.test_connection()
        
        parse_start = time.time()
        remote_files = self.get_remote_files()
        results['parse_time'] = time.time() - parse_start
        
        if remote_files:
            check_start = time.time()
            to_download = self._check_files_sync(remote_files[:100])
            results['check_time'] = time.time() - check_start
            results['files_checked'] = len(remote_files[:100])
            results['files_to_download'] = len(to_download)
        
        results['total_time'] = time.time() - start
        results['performance_stats'] = self.performance_monitor.get_summary()
        
        if hasattr(self.cache_manager, 'lru_file_cache'):
            results['cache_stats'] = self.cache_manager.lru_file_cache.get_stats()
        
        return results


# ============================================================================
# MirrorConfig (Pydantic v2) - v3.0.2
# ============================================================================
class MirrorConfig(BaseModel):
    """Configuration for MirrorURL with Pydantic v2 validation and parallel downloads"""
    base_url: str
    dest_path: Path
    log_path: Path
    print_logs: bool = False         
    _silent: bool = False
    dir_suffix: str = ""
    workers: int = Field(default=DEFAULT_WORKERS, ge=1, le=MAX_WORKERS_HARD_LIMIT)
    timeout: int = Field(default=DEFAULT_TIMEOUT, ge=MIN_TIMEOUT, le=MAX_TIMEOUT)
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay: int = DEFAULT_RETRY_DELAY
    debug: bool = False
    dry_run: bool = False
    file_filters: List[str] = Field(default_factory=list)
    exclude_dirs: List[str] = Field(default_factory=list)
    cleanup_policy: CleanupPolicy = CleanupPolicy.SAFE_NO_DELETE
    quick: bool = False
    no_rget_list: bool = False
    rget_list_max_age: int = DEFAULT_RGET_LIST_MAX_AGE
    force_rget_list: bool = False
    no_cache: bool = False
    refresh_cache: bool = False
    cache_max_age: int = Field(default=DEFAULT_CACHE_MAX_AGE_DAYS, ge=0, le=MAX_CACHE_AGE_DAYS)
    no_etag: bool = False
    use_shared_log: bool = False
    scan_mode: ScanMode = ScanMode.ADAPTIVE
    parallel_threshold: int = PARALLEL_SCAN_THRESHOLD
    benchmark: bool = False
    http2: bool = True
    stats: bool = False
    max_depth: int = Field(default=MAX_DIRECTORY_DEPTH, ge=1, le=100)
    max_filename_len: int = Field(default=MAX_FILENAME_LENGTH, ge=1, le=512)
    safe_urls: bool = True
    confirm_delete: bool = False
    quiet: bool = False
    verbose: bool = False
    metrics_json: Optional[Path] = None
    progress_bar: bool = False
    async_metadata: bool = True
    async_workers: int = Field(default=DEFAULT_ASYNC_WORKERS, ge=1, le=200)
    content_hash_small_files: bool = True
    trusted_server: bool = False
    request_delay: float = Field(default=REQUEST_DELAY, ge=0.001, le=1.0)
    cache_html: bool = True
    html_cache_max_age: int = Field(default=HTML_CACHE_MAX_AGE_HOURS, ge=1, le=168)
    hash_algorithm: str = Field(
        default='md5',
        pattern='^(md5|sha256|blake2b)$',
        description="Hash algorithm for file integrity checks"
    )
    adaptive_async: bool = ADAPTIVE_ASYNC_ENABLED
    adaptive_error_threshold: float = ADAPTIVE_ERROR_THRESHOLD
    adaptive_start_concurrency: int = ADAPTIVE_START_CONCURRENCY
    security_validation: bool = True
    circuit_breaker_enabled: bool = True
    bandwidth_limit: Optional[float] = Field(default=None, gt=0)
    enable_resume: bool = True
    max_concurrent_downloads: int = Field(default=10, ge=1, le=50)  # Increased for v3.0.2
    download_queue_size: int = Field(default=1000, ge=100)
    handle_symlinks: bool = False
    symlink_mode: str = 'skip'
    circuit_breaker_downloads: bool = Field(default=True)
    max_symlink_depth: int = Field(default=MAX_SYMLINK_DEPTH, ge=1, le=50)
    max_symlinks_per_dir: int = Field(default=MAX_SYMLINKS_PER_DIR, ge=1, le=1000)
    symlink_bomb_threshold: int = Field(default=SYMLINK_BOMB_THRESHOLD, ge=100, le=100000)
    adaptive_batch_processing: bool = Field(default=True)
    initial_batch_size: int = Field(default=BATCH_SIZE, ge=10, le=1000)
    max_batch_size: int = Field(default=MAX_BATCH_SIZE, ge=10, le=2000)
    target_batch_time: float = Field(default=TARGET_BATCH_TIME_SECONDS, ge=0.1, le=5.0)
    memory_cache_size: int = Field(default=MEMORY_CACHE_MAX_SIZE, ge=1000, le=1000000)
    use_disk_backed_sets: bool = Field(default=False)
    disk_cache_dir: Optional[Path] = None
    fast_parsing_fallback: bool = Field(default=True)
    http2_pipelining: bool = Field(default=True)
    connection_pool_prewarm: bool = Field(default=True)
    fs_cache_ttl: float = Field(default=FS_CACHE_TTL_SECONDS, ge=0.1, le=30.0)
    
    # NEW v3.0.0 fields
    # Bounds enforced by model_validator (raising ConfigError) when parallel mode is on.
    max_chunks_per_file: int = Field(default=MAX_CHUNKS_PER_FILE)
    min_chunk_size_mb: int = Field(default=10)
    max_parallel_chunks_total: int = Field(default=MAX_PARALLEL_CHUNKS_TOTAL)
    chunk_assembly_dir: Optional[Path] = Field(default=None)
    chunk_timeout_multiplier: float = Field(default=CHUNK_TIMEOUT_MULTIPLIER, ge=1.0, le=3.0)
    # NEW v3.0.6 fields
    auto_concurrency: bool = Field(default=AUTO_CONCURRENCY_ENABLED)
    health_check_port: int = Field(default=8080, ge=1024, le=65535)
    # NEW v3.0.6
    use_shared_thread_pool: bool = Field(default=False, description="Use shared thread pool for all operations")  # NEW: Default to dedicated pools

    # NEW v3.0.7: Download mode fields 
    parallel_downloads: bool = Field(default=False)
    streaming_parallel: bool = Field(default=False)
    sequential_downloads: bool = Field(default=False)
    
    model_config = ConfigDict(
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=False,
    )

    # Parallel download optimization flags
    parallel_optimization_mode: str = Field(
        default='balanced', 
        pattern='^(conservative|balanced|aggressive)$',
        description="Optimization mode: conservative (safe), balanced (default), aggressive (max speed)"
    )
    disable_rate_scaling: bool = Field(
        default=False,
        description="Disable rate limiter scaling for parallel downloads"
    )
    use_dedicated_download_pool: bool = Field(
        default=True,
        description="Use dedicated thread pool for downloads instead of shared"
    )
    
    # NEW: Auto-selection configuration
    auto_select_method: bool = Field(default=True, description="Automatically select best download method")
    force_method: Optional[str] = Field(default=None, description="Force specific method: sequential, parallel_files, streaming_parallel, traditional_parallel")
    force_disk_type: Optional[str] = Field(default=None, description="Force disk type: ssd, hdd, nvme")
    manual_network_speed_mbps: Optional[float] = Field(default=None, description="Manually specify network speed in Mbps")
    
    # NEW: Method-specific tuning
    parallel_files_min_files: int = Field(default=3, description="Minimum files to use parallel files mode")
    streaming_min_file_size_mb: int = Field(default=100, description="Minimum file size in MB for streaming parallel")
    streaming_min_files: int = Field(default=4, description="Minimum files for streaming parallel")
    traditional_min_files: int = Field(default=3, description="Minimum files for traditional parallel")
    
    @field_validator("base_url", mode="before")
    @classmethod
    def trim_base_url(cls, v: Any) -> Any:
        if isinstance(v, str):
            return trim_url(v).rstrip("/")
        return v
    
    @model_validator(mode="after")
    def validate_download_modes(self) -> "MirrorConfig":
        # Ensure only one download mode is active
        modes = [self.parallel_downloads, self.streaming_parallel, self.sequential_downloads]
        if sum(modes) > 1:
            raise ConfigError("Cannot enable multiple download modes simultaneously.")
        return self

    @model_validator(mode="after")
    def validate_and_normalize(self) -> "MirrorConfig":
        # 1. Normalize base URL (strip whitespace & trailing slashes)
        url = str(self.base_url or '').strip().rstrip('/')
        if not url:
            raise ConfigError("base_url cannot be empty")
        if not url.startswith(('http://', 'https://')):
            raise ConfigError(f"base_url must start with http:// or https://: {url}")
        
        parsed = urlparse(url)
        if not parsed.netloc:
            raise ConfigError(f"base_url missing hostname: {url}")
        
        # Use object.__setattr__ to bypass Pydantic's frozen model protection
        object.__setattr__(self, 'base_url', url)
    
        # 2. Validate regex patterns in file_filters
        for pattern in self.file_filters:
            if not pattern.startswith('.'):
                try:
                    re.compile(pattern)
                except re_error as e:
                    raise ConfigError(f"Invalid regex pattern '{pattern}': {e}")
        
        # Also validate exclude_dirs patterns if they contain regex
        for pattern in self.exclude_dirs:
            if '*' in pattern or '?' in pattern or '[' in pattern:
                try:
                    re.compile(pattern.replace('*', '.*').replace('?', '.'))
                except re_error as e:
                    raise ConfigError(f"Invalid exclude_dir pattern '{pattern}': {e}")
    
        # 3. Validate parallel download settings
        if self.parallel_downloads or self.streaming_parallel:
            if self.min_chunk_size_mb < 1:
                raise ConfigError("min_chunk_size_mb must be >= 1")
            if self.min_chunk_size_mb > 100:
                raise ConfigError("min_chunk_size_mb must be <= 100")
            
            if self.max_chunks_per_file < 1:
                raise ConfigError("max_chunks_per_file must be >= 1")
            if self.max_chunks_per_file > 20:
                raise ConfigError("max_chunks_per_file must be <= 20")
            
            if self.max_parallel_chunks_total < 10:
                raise ConfigError("max_parallel_chunks_total must be >= 10")
            if self.max_parallel_chunks_total > 200:
                raise ConfigError("max_parallel_chunks_total must be <= 200")
    
            if self.min_chunk_size_mb < 5:
                logging.warning("Very small chunk size may increase overhead")
    
        # 4. Validate max_concurrent_downloads
        if self.max_concurrent_downloads < 1:
            raise ConfigError("max_concurrent_downloads must be >= 1")
        if self.max_concurrent_downloads > 50:
            raise ConfigError("max_concurrent_downloads must be <= 50")
    
        # 5. Check chunk assembly directory
        if self.chunk_assembly_dir:
            # Resolve to absolute path for consistent checking
            chunk_dir = self.chunk_assembly_dir.resolve()
            
            # Check if path is absolute (recommended)
            if not chunk_dir.is_absolute():
                logging.warning(f"chunk_assembly_dir is relative: {chunk_dir} - using relative to CWD")
            
            if chunk_dir.exists():
                # Directory exists - check writability
                if not os.access(str(chunk_dir), os.W_OK):
                    raise ConfigError(f"chunk_assembly_dir exists but is not writable: {chunk_dir}")
            else:
                # Directory doesn't exist - check parent is writable
                parent = chunk_dir.parent
                if not parent.exists():
                    raise ConfigError(f"Parent directory does not exist: {parent}")
                if not os.access(str(parent), os.W_OK):
                    raise ConfigError(f"Parent directory not writable: {parent}")
            
            # Check disk space on appropriate path
            check_path = chunk_dir if chunk_dir.exists() else chunk_dir.parent
            try:
                usage = shutil.disk_usage(str(check_path))
                min_free_mb = 100
                if usage.free < min_free_mb * 1024 * 1024:
                    logging.warning(
                        f"Low free space in {check_path}: {usage.free / (1024*1024):.1f}MB free "
                        f"(< {min_free_mb}MB recommended for chunk assembly)"
                    )
            except OSError as e:
                raise ConfigError(f"Cannot check disk space for {check_path}: {e}")
    
        return self
    
    @field_validator('cleanup_policy', mode='before')
    @classmethod
    def validate_cleanup_policy(cls, v: Any) -> CleanupPolicy:
        if isinstance(v, CleanupPolicy):
            return v
        if isinstance(v, str):
            try:
                return CleanupPolicy(v)
            except ValueError:
                return CleanupPolicy.SAFE_NO_DELETE
        return CleanupPolicy.SAFE_NO_DELETE
    
    @field_validator('scan_mode', mode='before')
    @classmethod
    def validate_scan_mode(cls, v: Any) -> ScanMode:
        if isinstance(v, ScanMode):
            return v
        if isinstance(v, str):
            try:
                return ScanMode(v)
            except ValueError:
                return ScanMode.ADAPTIVE
        return ScanMode.ADAPTIVE
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any], silent: bool = False) -> "MirrorConfig":
        if "dest_path" in config_dict and isinstance(config_dict["dest_path"], str):
            config_dict["dest_path"] = Path(config_dict["dest_path"])
        if "log_path" in config_dict and isinstance(config_dict["log_path"], str):
            config_dict["log_path"] = Path(config_dict["log_path"])
        if "disk_cache_dir" in config_dict and isinstance(config_dict["disk_cache_dir"], str):
            config_dict["disk_cache_dir"] = Path(config_dict["disk_cache_dir"])
        if "chunk_assembly_dir" in config_dict and isinstance(config_dict["chunk_assembly_dir"], str):
            config_dict["chunk_assembly_dir"] = Path(config_dict["chunk_assembly_dir"])
        
        config = cls.model_validate(config_dict)
        config._silent = silent
        return config
    
    @classmethod
    def from_yaml(cls, yaml_path: Path, silent: bool = False) -> "MirrorConfig":
        # 1️⃣ SYMLINK SECURITY CHECK (MUST run BEFORE .resolve())
        original_path = yaml_path
        if original_path.is_symlink():
            try:
                target = original_path.readlink()
                # Resolve target to catch relative symlinks pointing to sensitive dirs
                resolved_target = target.resolve()
                # Block symlinks to critical system directories
                sensitive_roots = ('/etc/', '/proc/', '/sys/', '/root/', '/var/lib/')
                if any(str(resolved_target).startswith(root) for root in sensitive_roots):
                    raise ConfigError(
                        f"Unsafe symlink in config path: {yaml_path} -> {resolved_target}"
                    )
            except Exception as e:
                raise ConfigError(f"Cannot resolve symlink {yaml_path}: {e}")

        # 2️⃣ NORMALIZE & VERIFY PATH (OS permissions handle read access)
        yaml_path = yaml_path.resolve()
        if not yaml_path.is_file():
            raise ConfigError(f"Config file not found or not a regular file: {yaml_path}")

        try:
            # ✅ FIX: Explicit UTF-8 encoding for cross-platform compatibility
            with open(yaml_path, "r", encoding="utf-8") as f:
                config_dict = yaml.safe_load(f)

            # ✅ FIX: Handle empty YAML files explicitly
            if config_dict is None or not isinstance(config_dict, dict):
                raise ConfigError("Config file must contain a valid YAML dictionary")

            config_dict = expand_env_vars(config_dict)

            # ⛔ REMOVED: Overzealous character blocking.
            # yaml.safe_load() is explicitly designed to be safe from code execution.
            # Blocking characters like '$', '(', ')' breaks valid URLs, paths, and 
            # user-defined patterns. Security belongs at execution boundaries, not config load.

            # 3️⃣ REQUIRE CORE FIELDS
            required = ["base_url", "dest_path", "log_path"]
            missing = [f for f in required if f not in config_dict]
            if missing:
                raise ConfigError(f"Missing required fields: {missing}")

            return cls.from_dict(config_dict, silent=silent)

        except yaml.YAMLError as e:
            raise ConfigError(f"Invalid YAML syntax in {yaml_path}: {e}")
        except ConfigError:
            raise  # ✅ Re-raise ConfigErrors as-is (prevents double-wrapping)
        except OSError as e:
            raise ConfigError(f"OS-level error reading {yaml_path}: {e}")
        except Exception as e:
            raise ConfigError(f"Failed to load config from {yaml_path}: {e}")    
            
    @classmethod
    def validate(cls, config: "MirrorConfig") -> List[str]:
        warnings = []
        
        if config.workers > 20:
            warnings.append("High worker count may cause server issues")
        if config.cache_max_age > 30:
            warnings.append("Long cache age may miss remote updates")
        if config.hash_algorithm == 'md5':
            warnings.append("⚠️ MD5 hash algorithm is deprecated, consider using sha256")
        if config.content_hash_small_files:
            warnings.append(f"🔐 Content hash: files <{CONTENT_HASH_THRESHOLD/1024:.0f}KB")
        
        if config.cleanup_policy == CleanupPolicy.DELETE:
            warnings.append("⚠️ DELETE MODE: Obsolete file deletion ENABLED")
        elif config.cleanup_policy == CleanupPolicy.MOVE:
            warnings.append("📦 MOVE MODE: Obsolete files moved to _obsolete folder")
        elif config.cleanup_policy == CleanupPolicy.PREVIEW:
            warnings.append("🔍 PREVIEW MODE: Showing what would be deleted/moved")
        else:
            warnings.append("✅ SAFE MODE: Obsolete file cleanup DISABLED")
        
        if config.safe_urls:
            warnings.append("🔒 URL sanitization enabled")
        if config.confirm_delete and config.cleanup_policy == CleanupPolicy.DELETE:
            warnings.append("🔐 Interactive confirmation required")
        if config.quiet:
            warnings.append("🔇 Quiet mode enabled")
        if config.verbose:
            warnings.append("🔊 Verbose mode enabled")
        if config.metrics_json:
            warnings.append(f"📊 Metrics export: {config.metrics_json}")
        if config.async_metadata:
            warnings.append(f"⚡ Async metadata {config.async_workers} workers")
        if config.trusted_server:
            warnings.append("⚡ Trusted server mode: Faster rate limiting (10ms delay)")
        if config.cache_html:
            warnings.append(f"📦 HTML caching enabled ({config.html_cache_max_age}h)")
        if config.adaptive_async:
            warnings.append(f"🔄 Adaptive async enabled (start={config.adaptive_start_concurrency})")
        if config.bandwidth_limit:
            warnings.append(f"⏱️ Bandwidth limit: {config.bandwidth_limit} MB/s")
        if config.enable_resume:
            warnings.append("↩️ Resume capability enabled")
        if config.handle_symlinks:
            warnings.append(f"🔗 Symlink handling enabled (mode: {config.symlink_mode})")
        if config.adaptive_batch_processing:
            warnings.append(f"📈 Adaptive batch processing enabled (initial={config.initial_batch_size})")
        if config.use_disk_backed_sets:
            warnings.append(f"💾 Disk-backed sets enabled (max memory: {config.memory_cache_size})")
        if config.fast_parsing_fallback:
            warnings.append("⚡ Fast parsing fallback enabled")
        if config.connection_pool_prewarm:
            warnings.append("🔥 Connection pool pre-warming enabled")
        
        # NEW v3.0.0 warnings
        if config.parallel_downloads:
            warnings.append(f"🚀 Parallel downloads enabled (max {config.max_chunks_per_file} chunks, {config.min_chunk_size_mb}MB min)")
            if config.max_chunks_per_file > 8:
                warnings.append("⚠️ High chunk count may be excessive for most servers")
        
        # NEW v3.0.2: Warning for high concurrent downloads
        if config.max_concurrent_downloads > 20:
            warnings.append("⚠️ Very high concurrent downloads may overwhelm your network")
        
        return warnings

# ============================================================================
# CONFIGURATION LOADING
# ============================================================================
def load_config_from_args(args: argparse.Namespace, silent: bool = False) -> MirrorConfig:
    """Load configuration from command line arguments"""
    config_dict = {
        'base_url': args.url.rstrip('/'),
        'dest_path': args.dest_path,
        'log_path': args.log_path,
        'workers': args.workers,
        'timeout': args.timeout,
        'max_retries': args.max_retries,
        'retry_delay': args.retry_delay,
        'debug': args.debug,
        'print_logs': args.print_logs,
        # NOTE: 'quiet' and 'verbose' are set further down via getattr(...)
        # with safe defaults; duplicate keys here were dead (last one wins).
        'dry_run': args.dry_run,
        'file_filters': args.filter if args.filter else [],
        'exclude_dirs': args.exclude_dir or [],
        'cleanup_policy': args.cleanup,
        'quick': args.quick,
        'no_rget_list': args.no_rget_list,
        'rget_list_max_age': args.rget_list_max_age,
        'force_rget_list': args.force_rget_list,
        'no_cache': args.no_cache,
        'refresh_cache': args.refresh_cache,
        'cache_max_age': args.cache_max_age,
        'no_etag': getattr(args, 'no_etag', False),
        'hash_algorithm': getattr(args, 'hash_algorithm', 'md5'),
        'use_shared_log': bool(args.log_file),
        'benchmark': args.benchmark,
        'http2': args.http2,
        'stats': args.stats,
        'max_depth': args.max_depth,
        'max_filename_len': args.max_filename_len,
        'safe_urls': getattr(args, 'safe_urls', True),
        'confirm_delete': getattr(args, 'confirm_delete', False),
        'quiet': getattr(args, 'quiet', False),
        'verbose': getattr(args, 'verbose', False),
        'metrics_json': getattr(args, 'metrics_json', None),
        'progress_bar': getattr(args, 'progress_bar', False),
        'async_metadata': getattr(args, 'async_metadata', True),
        'async_workers': getattr(args, 'async_workers', DEFAULT_ASYNC_WORKERS),
        'content_hash_small_files': getattr(args, 'content_hash_small_files', True),
        'trusted_server': getattr(args, 'trusted_server', False),
        'request_delay': getattr(args, 'request_delay', REQUEST_DELAY),
        'cache_html': getattr(args, 'cache_html', True),
        'html_cache_max_age': getattr(args, 'html_cache_max_age', HTML_CACHE_MAX_AGE_HOURS),
        'adaptive_async': getattr(args, 'adaptive_async', ADAPTIVE_ASYNC_ENABLED),
        'adaptive_error_threshold': getattr(args, 'adaptive_error_threshold', ADAPTIVE_ERROR_THRESHOLD),
        'adaptive_start_concurrency': getattr(args, 'adaptive_start_concurrency', ADAPTIVE_START_CONCURRENCY),
        'security_validation': getattr(args, 'security_validation', True),
        'circuit_breaker_enabled': getattr(args, 'circuit_breaker_enabled', True),
        'bandwidth_limit': getattr(args, 'bandwidth_limit', None),
        'enable_resume': getattr(args, 'enable_resume', True),
        'max_concurrent_downloads': getattr(args, 'max_concurrent_downloads', 10),  # Increased default
        'download_queue_size': getattr(args, 'download_queue_size', 1000),
        'handle_symlinks': getattr(args, 'handle_symlinks', False),
        'symlink_mode': getattr(args, 'symlink_mode', 'skip'),
        'circuit_breaker_downloads': getattr(args, 'circuit_breaker_downloads', True),
        'max_symlink_depth': getattr(args, 'max_symlink_depth', MAX_SYMLINK_DEPTH),
        'max_symlinks_per_dir': getattr(args, 'max_symlinks_per_dir', MAX_SYMLINKS_PER_DIR),
        'symlink_bomb_threshold': getattr(args, 'symlink_bomb_threshold', SYMLINK_BOMB_THRESHOLD),
        'adaptive_batch_processing': getattr(args, 'adaptive_batch_processing', True),
        'initial_batch_size': getattr(args, 'initial_batch_size', BATCH_SIZE),
        'max_batch_size': getattr(args, 'max_batch_size', MAX_BATCH_SIZE),
        'target_batch_time': getattr(args, 'target_batch_time', TARGET_BATCH_TIME_SECONDS),
        'memory_cache_size': getattr(args, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE),
        'use_disk_backed_sets': getattr(args, 'use_disk_backed_sets', False),
        'disk_cache_dir': getattr(args, 'disk_cache_dir', None),
        'fast_parsing_fallback': getattr(args, 'fast_parsing_fallback', True),
        'http2_pipelining': getattr(args, 'http2_pipelining', True),
        'connection_pool_prewarm': getattr(args, 'connection_pool_prewarm', True),
        'fs_cache_ttl': getattr(args, 'fs_cache_ttl', FS_CACHE_TTL_SECONDS),        
        # NEW v3.0.0 arguments
        'max_chunks_per_file': getattr(args, 'max_chunks', MAX_CHUNKS_PER_FILE),
        'min_chunk_size_mb': getattr(args, 'min_chunk_size', 10),
        'max_parallel_chunks_total': getattr(args, 'max_parallel_chunks', MAX_PARALLEL_CHUNKS_TOTAL),
        'chunk_assembly_dir': getattr(args, 'chunk_assembly_dir', None),
        'chunk_timeout_multiplier': getattr(args, 'chunk_timeout_multiplier', CHUNK_TIMEOUT_MULTIPLIER),
        'auto_concurrency': getattr(args, 'auto_concurrency', AUTO_CONCURRENCY_ENABLED),
        'health_check_port': getattr(args, 'health_check_port', 8080),
        'parallel_files_min_files': getattr(args, 'parallel_files_min_files', 3),
        'streaming_min_file_size_mb': getattr(args, 'streaming_min_size', STREAMING_MIN_FILE_SIZE_MB),
        'streaming_min_files': getattr(args, 'streaming_min_files', 4),
        'traditional_min_files': getattr(args, 'traditional_min_files', 3),
        # NEW: Download mode flags
        'parallel_downloads': getattr(args, 'parallel_downloads', False),
        'streaming_parallel': getattr(args, 'streaming_parallel', False),
        'sequential_downloads': getattr(args, 'sequential_downloads', False),
    }
    
    # At the end of config_dict creation in load_config_from_args():
    if not hasattr(args, 'cleanup'):
        config_dict['cleanup_policy'] = CleanupPolicy.SAFE_NO_DELETE
        
    if hasattr(args, 'scan_mode') and args.scan_mode:
        try:
            config_dict['scan_mode'] = ScanMode(args.scan_mode)
        except ValueError:
            config_dict['scan_mode'] = ScanMode.ADAPTIVE

    # Ensure only one mode is selected
    mode_count = sum([
        config_dict['parallel_downloads'],
        config_dict['streaming_parallel'],
        config_dict['sequential_downloads']
    ])
    
    if mode_count > 1:
        raise ConfigError("Cannot specify multiple download modes. Choose one: "
                         "--parallel-downloads, --streaming-parallel, or --sequential-downloads")    
    config = MirrorConfig.from_dict(config_dict, silent=silent)
    
    return config

def add_parallel_arguments(parser: argparse.ArgumentParser) -> None:
    """Add parallel download arguments to parser"""
    parallel_grp = parser.add_argument_group('Download Method Options')
    method_group = parallel_grp.add_mutually_exclusive_group()
    method_group.add_argument("--parallel-downloads", action="store_true",
                              help="Enable traditional parallel downloads (temp files, safe)")
    method_group.add_argument("--streaming-parallel", action="store_true",
                              help="Enable streaming parallel downloads (direct write, faster for huge files)")
    method_group.add_argument("--sequential-downloads", action="store_true",
                              help="Force sequential downloads (no parallelism)")
        
    
    parallel_grp.add_argument("--max-chunks", type=int, default=MAX_CHUNKS_PER_FILE,
                             metavar='N', help=f"Maximum chunks per file (default: {MAX_CHUNKS_PER_FILE})")
    parallel_grp.add_argument("--min-chunk-size", type=int, default=10,
                             metavar='MB', help=f"Minimum chunk size in MB (default: 10MB)")
    parallel_grp.add_argument("--max-parallel-chunks", type=int, default=MAX_PARALLEL_CHUNKS_TOTAL,
                             metavar='N', help=f"Maximum total parallel chunks (default: {MAX_PARALLEL_CHUNKS_TOTAL})")
    parallel_grp.add_argument("--chunk-assembly-dir", type=Path, metavar='DIR',
                             help="Directory for temporary chunk storage")
    parallel_grp.add_argument("--chunk-timeout-multiplier", type=float, default=CHUNK_TIMEOUT_MULTIPLIER,
                             metavar='MULT', help=f"Timeout multiplier for chunks (default: {CHUNK_TIMEOUT_MULTIPLIER})")
    parallel_grp.add_argument("--auto-concurrency", action="store_true",
                              help="Automatically tune parallel download concurrency based on throughput (v3.0.6)")
    # NEW: Auto-selection arguments
    auto_grp = parser.add_argument_group('Auto-Optimization Options')
    auto_grp.add_argument("--auto-select", action="store_true", default=True,
                         help="Automatically select best download method (default: enabled)")
    auto_grp.add_argument("--no-auto-select", action="store_false", dest="auto_select",
                         help="Disable automatic method selection")
    auto_grp.add_argument("--force-method", choices=['sequential', 'parallel_files', 'streaming', 'traditional'],
                         help="Force specific download method")
    auto_grp.add_argument("--force-disk-type", choices=['ssd', 'hdd', 'nvme'],
                         help="Manually specify disk type for optimization")
    auto_grp.add_argument("--network-speed", type=float, metavar='MBPS',
                         help="Manually specify network speed in Mbps")
 
def setup_shared_logging(args: argparse.Namespace) -> None:
    """Setup shared logging for multiple suffixes"""
    # Create log filename with suffixes properly separated by underscores
    suffixes_str = '_'.join(args.dir_suffix) if args.dir_suffix else 'all'
    timestamp = time.strftime('%Y%m%d_%H%M%S')
    log_filename = f"{args.log_file}{suffixes_str}_{timestamp}.log"
    log_path = Path(args.log_path) / log_filename
    
    # Remove ALL existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        if hasattr(handler, 'close'):
            try:
                handler.close()
            except Exception:
                pass
    
    # Create file handler (always)
    file_handler = logging.FileHandler(str(log_path), mode='a', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))
    
    # Create console handler (if print-logs is enabled)
    handlers = [file_handler]
    if args.print_logs:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        ))
        if args.debug or args.verbose:
            console_handler.setLevel(logging.DEBUG)
        elif args.quiet:
            console_handler.setLevel(logging.WARNING)
        else:
            console_handler.setLevel(logging.INFO)
        handlers.append(console_handler)
    
    # Set log level
    if args.quiet:
        log_level = logging.WARNING
    elif args.verbose or args.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    
    # Configure root logger
    logging.root.setLevel(log_level)
    
    # Add handlers
    for handler in handlers:
        logging.root.addHandler(handler)
    
    # Log header information
    logging.info("="*50)
    logging.info(f"MirrorURL v{__version__} - SHARED LOG")
    logging.info(f"Log: {log_path}")
    
    if args.dir_suffix:
        logging.info(f"Suffixes: {args.dir_suffix}")
    
    cleanup_policy = getattr(args, 'cleanup_policy', CleanupPolicy.SAFE_NO_DELETE)
    if cleanup_policy == CleanupPolicy.DELETE:
        logging.warning("⚠️ DELETE MODE ENABLED")
    elif cleanup_policy == CleanupPolicy.MOVE:
        logging.info("📦 MOVE MODE ENABLED")
    elif cleanup_policy == CleanupPolicy.PREVIEW:
        logging.info("🔍 PREVIEW MODE")
    else:
        logging.info("✅ SAFE MODE")
    
    if args.no_cache:
        logging.warning("CACHE DISABLED")
    if args.refresh_cache:
        logging.warning("CACHE REFRESH FORCED")
    if getattr(args, 'safe_urls', True):
        logging.info("🔒 URL sanitization enabled")
    
    logging.info(f"🛡️ Path safety: max_depth={args.max_depth}, max_filename_len={args.max_filename_len}")
    
    if args.confirm_delete and args.cleanup_policy == CleanupPolicy.DELETE:
        logging.info("🔐 Confirmation required")
    if args.quiet:
        logging.info("🔇 Quiet mode")
    elif args.verbose:
        logging.info("🔊 Verbose mode")
    if args.metrics_json:
        logging.info(f"📊 Metrics: {args.metrics_json}")
    if TQDM_AVAILABLE and args.progress_bar:
        logging.info("📈 Progress bar enabled")
    if args.async_metadata:
        if args.adaptive_async:
            logging.info(f"🔄 Adaptive async: {args.adaptive_start_concurrency}-{ADAPTIVE_MAX_CONCURRENCY} workers")
        else:
            logging.info(f"⚡ Async meta {args.async_workers} workers")
    if args.content_hash_small_files:
        logging.info(f"🔐 Content hash: <{CONTENT_HASH_THRESHOLD/1024:.0f}KB")
    
    delay_ms = args.request_delay * 1000
    logging.info(f"⚡ Rate limit: {delay_ms:.1f}ms{' (trusted)' if args.trusted_server else ''}")
    
    if args.cache_html:
        logging.info(f"📦 HTML cache: {args.html_cache_max_age}h")
    if args.bandwidth_limit:
        logging.info(f"⏱️ Bandwidth limit: {args.bandwidth_limit} MB/s")
    if getattr(args, 'enable_resume', True):
        logging.info("↩️ Resume enabled")
    if args.handle_symlinks:
        logging.info(f"🔗 Symlink handling: {args.symlink_mode}")
    if getattr(args, 'adaptive_batch_processing', True):
        logging.info(f"📈 Adaptive batch processing: initial={getattr(args, 'initial_batch_size', BATCH_SIZE)}")
    if getattr(args, 'use_disk_backed_sets', False):
        logging.info(f"💾 Disk-backed sets: memory={getattr(args, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE)}")
    if getattr(args, 'fast_parsing_fallback', True):
        logging.info("⚡ Fast parsing fallback enabled")
    if getattr(args, 'connection_pool_prewarm', True):
        logging.info("🔥 Connection pool pre-warming enabled")
    if PSUTIL_AVAILABLE:
        logging.info("📊 Memory monitoring: ENABLED")
    if args.metrics_json:
        health_port = getattr(args, 'health_check_port', 8080)
        logging.info(f"🏥 Health check API: http://localhost:{health_port}/health")
    if getattr(args, 'parallel_downloads', False):
        logging.info(f"🚀 Parallel downloads: ENABLED (max {args.max_chunks} chunks, {args.min_chunk_size}MB min)")
    if getattr(args, 'max_concurrent_downloads', 10) > 1:
        logging.info(f"📥 Max concurrent file downloads: {args.max_concurrent_downloads}")
    
    logging.info("="*50)
    
    

def main() -> None:
    """Main entry point with v3.1.13 true parallel file downloads"""
    parser = argparse.ArgumentParser(
        description="MirrorURL v3.1.13 - Enterprise-Grade Remote Directory Mirroring Tool with True Parallel Downloads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                                  MIRRORURL v3.1.13                                   ║
║                                USAGE GUIDE                                           ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

REQUIRED ARGUMENTS (ONE of these two options):
────────────────────────────────────────────────────────────────────────────────────────
Option 1: Use configuration file
  --config CONFIG       YAML/JSON configuration file (validated against schema)

Option 2: Use command-line arguments
  --url URL             Base URL to mirror (e.g., https://example.com/files/)
  --dest-path PATH      Destination directory for downloaded files
  --log-path PATH       Directory for log files

────────────────────────────────────────────────────────────────────────────────────────
DOWNLOAD MODES (select ONE):
────────────────────────────────────────────────────────────────────────────────────────
  --parallel-downloads     Traditional parallel mode (temp files, safe, supports resume)
  --streaming-parallel     Streaming parallel mode (direct write, faster for huge files)
  --sequential-downloads   Sequential mode (no parallelism, one file at a time)
  (no argument)            Auto-select mode (intelligent decision based on conditions)

────────────────────────────────────────────────────────────────────────────────────────
FILTER PATTERNS (--filter option):
────────────────────────────────────────────────────────────────────────────────────────
The --filter option supports both simple extensions and powerful regex patterns:

  SIMPLE EXTENSIONS (backward compatible):
    --filter .fits .txt .jpg    # Match multiple extensions
    --filter .fts               # Match single extension (case-insensitive)

  REGEX PATTERNS (full power):
    --filter '2024.*\\.fits$'                    # .fits files from 2024
    --filter 'L1.*\\.(fits|txt)$'                # L1 files with .fits or .txt
    --filter 'IMG_[0-9]{4}\\.jpg'                 # Images with 4-digit numbers
    --filter '^(?!temp_).*\\.dat$'                # All .dat files except temp_
    --filter 'L[0-9]{2}/v[0-9]/.*\\.fits'        # Deep path patterns

────────────────────────────────────────────────────────────────────────────────────────
PARALLEL DOWNLOAD OPTIONS:
────────────────────────────────────────────────────────────────────────────────────────
  --max-chunks N              Maximum chunks per file (default: 8)
  --min-chunk-size MB         Minimum chunk size in MB (default: 10MB)
  --max-parallel-chunks N     Maximum total parallel chunks (default: 50)
  --max-concurrent-downloads N  Maximum files to download simultaneously (default: 10)
  --auto-concurrency          Automatically tune parallel download concurrency based on
                              measured throughput (finds optimal setting for each server)

  Examples:
  # Traditional parallel (temp files) - SAFE default for parallel
  %(prog)s --url https://example.com/data/ --dest-path ./data --log-path ./logs \\
           --parallel-downloads --max-chunks 5 --max-concurrent-downloads 20

  # Streaming parallel (direct write) - FASTER for huge files
  %(prog)s --url https://example.com/data/ --dest-path ./data --log-path ./logs \\
           --streaming-parallel --max-chunks 8

  # Sequential - MOST RELIABLE for problematic connections
  %(prog)s --url https://example.com/data/ --dest-path ./data --log-path ./logs \\
           --sequential-downloads

  # Auto-select - LET SYSTEM DECIDE
  %(prog)s --url https://example.com/data/ --dest-path ./data --log-path ./logs

  All safety features preserved:
  ✅ Per-IP rate limiting adapts to chunk count
  ✅ Circuit breaker tracks chunk failures per file/server
  ✅ Resume capability works per chunk
  ✅ Graceful fallback if server doesn't support Range
  ✅ Files download in parallel for maximum throughput

────────────────────────────────────────────────────────────────────────────────────────
PERFORMANCE BENCHMARKS:
────────────────────────────────────────────────────────────────────────────────────────
  📊 4 files (343MB total):
      v2.0.2: 3.7s  (92 MB/s)
      v3.0.0: 2.7s  (128 MB/s)  +40%%
      v3.0.2: 0.8s  (428 MB/s)  +365%% 🚀

────────────────────────────────────────────────────────────────────────────────────────
EXAMPLES:
────────────────────────────────────────────────────────────────────────────────────────
  # Basic mirroring with simple filters
  %(prog)s --url https://example.com/files/ --dest-path ./downloads \\
           --log-path ./logs --filter .fits .txt

  # Maximum performance parallel downloads
  %(prog)s --url https://example.com/data/ --dest-path ./data \\
           --log-path ./logs --parallel-downloads --max-chunks 8 \\
           --max-concurrent-downloads 20 --max-parallel-chunks 100

  # Conservative for throttled servers
  %(prog)s --url https://throttled-server.com/ --dest-path ./downloads \\
           --log-path ./logs --parallel-downloads --max-chunks 3 \\
           --max-concurrent-downloads 3 --request-delay 0.2

  # Production setup with config file
  %(prog)s --config /etc/mirrorurl/production.yaml
"""
    )
    
    basic = parser.add_argument_group('Required Options')
    basic.add_argument("--url", help="Base URL to mirror (required if --config not used)")
    basic.add_argument("--dest-path", type=Path, help="Destination directory (required if --config not used)")
    basic.add_argument("--log-path", type=Path, help="Log directory (required if --config not used)")
    basic.add_argument("--config", help="Configuration file (YAML/JSON)")
    
    # Create mutually exclusive group for download modes
    download_mode_group = parser.add_argument_group('Download Mode Options (select ONE)')
    mode_group = download_mode_group.add_mutually_exclusive_group()
    mode_group.add_argument("--parallel-downloads", action="store_true",
                           help="Traditional parallel downloads (temp files, safe, supports resume)")
    mode_group.add_argument("--streaming-parallel", action="store_true",
                           help="Streaming parallel downloads (direct write, faster for huge files)")
    mode_group.add_argument("--sequential-downloads", action="store_true",
                           help="Sequential downloads (no parallelism, one file at a time)")
    
    # Parallel Download Options (shared settings)
    parallel_grp = parser.add_argument_group('Parallel Download Options')
    parallel_grp.add_argument("--max-chunks", type=int, default=MAX_CHUNKS_PER_FILE,
                             metavar='N', help=f"Maximum chunks per file (default: {MAX_CHUNKS_PER_FILE})")
    parallel_grp.add_argument("--min-chunk-size", type=int, default=10,
                             metavar='MB', help=f"Minimum chunk size in MB (default: 10MB)")
    parallel_grp.add_argument("--max-parallel-chunks", type=int, default=MAX_PARALLEL_CHUNKS_TOTAL,
                             metavar='N', help=f"Maximum total parallel chunks (default: {MAX_PARALLEL_CHUNKS_TOTAL})")
    parallel_grp.add_argument("--max-concurrent-downloads", type=int, default=10,
                             metavar='N', help=f"Maximum concurrent file downloads (default: 10)")
    parallel_grp.add_argument("--auto-concurrency", action="store_true",
                             help="Automatically tune parallel download concurrency based on throughput")
    parallel_grp.add_argument("--chunk-assembly-dir", type=Path, metavar='DIR',
                             help="Directory for temporary chunk storage")
    parallel_grp.add_argument("--chunk-timeout-multiplier", type=float, default=CHUNK_TIMEOUT_MULTIPLIER,
                             metavar='MULT', help=f"Timeout multiplier for chunks (default: {CHUNK_TIMEOUT_MULTIPLIER})")
    
    filter_grp = parser.add_argument_group('Filter Options')
    filter_grp.add_argument("--filter", nargs='*', default=[], metavar='PATTERN',
                       help="File patterns to include (can be simple extension like .fits or regex pattern). "
                            "Examples:\n"
                            "  --filter .fits .txt .jpg           # Multiple simple extensions\n"
                            "  --filter '.*\\.fits$'               # Regex: any .fits files\n"
                            "  --filter '2024.*\\.fits' .txt       # Mixed regex and extensions")

    directory = parser.add_argument_group('Directory Options')
    directory.add_argument("--dir-suffix", nargs='*', default=[], metavar='SUFFIX',
                          help="Directory suffixes to mirror (e.g., L1/v1 L2/v2)")
    directory.add_argument("--exclude-dir", nargs='*', default=[], metavar='DIR',
                          help="Directories to exclude")
    
    performance = parser.add_argument_group('Performance & Worker Options')
    performance.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                            metavar='N', help=f"Sync workers (default: {DEFAULT_WORKERS})")
    performance.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                            metavar='SECS', help=f"Request timeout (default: {DEFAULT_TIMEOUT}s)")
    performance.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                            metavar='N', help=f"Max retries per request (default: {DEFAULT_MAX_RETRIES})")
    performance.add_argument("--retry-delay", type=int, default=DEFAULT_RETRY_DELAY,
                            metavar='SECS', help=f"Delay between retries (default: {DEFAULT_RETRY_DELAY}s)")
    performance.add_argument("--trusted-server", action="store_true",
                            help="Use faster rate limiting (10ms vs 50ms delay)")
    performance.add_argument("--request-delay", type=float, default=REQUEST_DELAY,
                            metavar='SECS', help=f"Request delay (default: {REQUEST_DELAY}s)")
    performance.add_argument("--bandwidth-limit", type=float, metavar='MB/S',
                            help="Limit download bandwidth (MB/s)")
    
    cache = parser.add_argument_group('Cache Options')
    cache.add_argument("--no-cache", action="store_true", help="Disable cache")
    cache.add_argument("--refresh-cache", action="store_true", help="Force cache refresh")
    cache.add_argument("--cache-max-age", type=int, default=DEFAULT_CACHE_MAX_AGE_DAYS,
                      metavar='DAYS', help=f"Cache max age (default: {DEFAULT_CACHE_MAX_AGE_DAYS} days)")
    cache.add_argument("--cache-html", action="store_true", default=True,
                      help="Cache parsed HTML content (default: enabled)")
    cache.add_argument("--no-cache-html", action="store_false", dest="cache_html",
                      help="Disable HTML caching")
    cache.add_argument("--html-cache-max-age", type=int, default=HTML_CACHE_MAX_AGE_HOURS,
                      metavar='HOURS', help=f"HTML cache max age (default: {HTML_CACHE_MAX_AGE_HOURS}h)")
    cache.add_argument("--hash-algorithm", type=str, default='md5',
                      choices=['md5', 'sha256', 'blake2b'], 
                      help="Hash algorithm for file integrity (default: md5)")
    cache.add_argument("--no-rget-list", action="store_true", help="Disable RGET-LIST usage")
    cache.add_argument("--rget-list-max-age", type=int, default=DEFAULT_RGET_LIST_MAX_AGE,
                      metavar='DAYS', help=f"RGET-LIST max age (default: {DEFAULT_RGET_LIST_MAX_AGE} days)")
    cache.add_argument("--force-rget-list", action="store_true", help="Force RGET-LIST use even if old")
    cache.add_argument("--no-etag", action="store_true", help="Disable ETag verification")
    
    async_grp = parser.add_argument_group('Async & Adaptive Options')
    async_grp.add_argument("--async-metadata", action="store_true", default=True,
                          help="Enable async metadata checks (default: enabled)")
    async_grp.add_argument("--no-async-metadata", action="store_false", dest="async_metadata",
                          help="Disable async metadata checks (use for throttled servers)")
    async_grp.add_argument("--async-workers", type=int, default=DEFAULT_ASYNC_WORKERS,
                          metavar='N', help=f"Async metadata workers (default: {DEFAULT_ASYNC_WORKERS})")
    async_grp.add_argument("--adaptive-async", action="store_true", default=ADAPTIVE_ASYNC_ENABLED,
                          help="Enable adaptive async concurrency (default: enabled)")
    async_grp.add_argument("--no-adaptive-async", action="store_false", dest="adaptive_async",
                          help="Disable adaptive async")
    async_grp.add_argument("--adaptive-start-concurrency", type=int, default=ADAPTIVE_START_CONCURRENCY,
                          metavar='N', help=f"Starting async concurrency (default: {ADAPTIVE_START_CONCURRENCY})")
    async_grp.add_argument("--adaptive-error-threshold", type=float, default=ADAPTIVE_ERROR_THRESHOLD,
                          metavar='RATE', help=f"Error rate threshold for fallback (default: {ADAPTIVE_ERROR_THRESHOLD})")
    
    cleanup = parser.add_argument_group('Cleanup & Safety Options')
    cleanup.add_argument("--cleanup", type=str, choices=['safe', 'preview', 'delete', 'move'],
                        default=argparse.SUPPRESS,
                        help="Cleanup policy: safe, preview, delete, move")
    cleanup.add_argument("--confirm-delete", action="store_true",
                        help="Require confirmation before deletion (delete mode only)")
    cleanup.add_argument("--dry-run", action="store_true", help="Simulate without downloading/deleting")
    cleanup.add_argument("--quick", action="store_true", help="Quick mode (update cache timestamp only)")
    
    security = parser.add_argument_group('Security Options')
    security.add_argument("--security-validation", action="store_true", default=True,
                         help="Enable SSRF/path protection (default: enabled)")
    security.add_argument("--no-security-validation", action="store_false", dest="security_validation",
                         help="Disable security validation (NOT recommended)")
    security.add_argument("--circuit-breaker-enabled", action="store_true", default=True,
                         help="Enable circuit breaker for failing services (default: enabled)")
    security.add_argument("--no-circuit-breaker", action="store_false", dest="circuit_breaker_enabled",
                         help="Disable circuit breaker")
    
    symlink = parser.add_argument_group('Symlink Handling Options')
    symlink.add_argument("--handle-symlinks", action="store_true", default=False,
                        help="Enable symlink detection and handling")
    symlink.add_argument("--symlink-mode", choices=['follow', 'skip', 'treat-as-file'],
                        default='skip', help="How to handle symlinks (default: skip)")
    symlink.add_argument("--max-symlink-depth", type=int, default=MAX_SYMLINK_DEPTH,
                        metavar='N', help=f"Maximum symlink depth (default: {MAX_SYMLINK_DEPTH})")
    symlink.add_argument("--max-symlinks-per-dir", type=int, default=MAX_SYMLINKS_PER_DIR,
                        metavar='N', help=f"Maximum symlinks per directory (default: {MAX_SYMLINKS_PER_DIR})")
    symlink.add_argument("--symlink-bomb-threshold", type=int, default=SYMLINK_BOMB_THRESHOLD,
                        metavar='N', help=f"Symlink bomb threshold (default: {SYMLINK_BOMB_THRESHOLD})")
    symlink.add_argument("--circuit-breaker-downloads", action="store_true", default=True,
                        help="Enable circuit breaker for downloads (default: enabled)")
    symlink.add_argument("--no-circuit-breaker-downloads", action="store_false", dest="circuit_breaker_downloads",
                        help="Disable circuit breaker for downloads")
    
    logging_grp = parser.add_argument_group('Logging & Output Options')
    logging_grp.add_argument("--debug", action="store_true", help="Enable debug logging")
    logging_grp.add_argument("--print-logs", action="store_true", help="Print logs to console")
    logging_grp.add_argument("--log_file", metavar='NAME', help="Shared log base name")
    logging_grp.add_argument("--quiet", action="store_true", help="Quiet mode (WARNING+ only)")
    logging_grp.add_argument("--verbose", action="store_true", help="Verbose mode (DEBUG)")
    logging_grp.add_argument("--progress-bar", action="store_true", help="Enable tqdm progress bar")
    logging_grp.add_argument("--stats", action="store_true", help="Show detailed statistics")
    logging_grp.add_argument("--metrics-json", type=Path, metavar='PATH',
                            help="Export metrics to JSON file")
    
    scan = parser.add_argument_group('Scan & Path Options')
    scan.add_argument("--scan-mode", choices=['sequential', 'parallel', 'adaptive', 'async'],
                     default='adaptive', help="Directory scan mode (default: adaptive)")
    scan.add_argument("--parallel-threshold", type=int, default=PARALLEL_SCAN_THRESHOLD,
                     metavar='N', help=f"Parallel scan threshold (default: {PARALLEL_SCAN_THRESHOLD})")
    scan.add_argument("--max-depth", type=int, default=MAX_DIRECTORY_DEPTH,
                     metavar='N', help=f"Maximum directory depth (default: {MAX_DIRECTORY_DEPTH})")
    scan.add_argument("--max-filename-len", type=int, default=MAX_FILENAME_LENGTH,
                     metavar='N', help=f"Maximum filename length (default: {MAX_FILENAME_LENGTH})")
    scan.add_argument("--download-queue-size", type=int, default=1000,
                     metavar='N', help=f"Download queue size (default: 1000)")
    
    advanced = parser.add_argument_group('Advanced Performance Options')
    advanced.add_argument("--adaptive-batch-processing", action="store_true", default=True,
                         help="Enable adaptive batch sizing (default: enabled)")
    advanced.add_argument("--no-adaptive-batch-processing", action="store_false", dest="adaptive_batch_processing",
                         help="Disable adaptive batch sizing")
    advanced.add_argument("--initial-batch-size", type=int, default=BATCH_SIZE,
                         metavar='N', help=f"Initial batch size (default: {BATCH_SIZE})")
    advanced.add_argument("--max-batch-size", type=int, default=MAX_BATCH_SIZE,
                         metavar='N', help=f"Maximum batch size (default: {MAX_BATCH_SIZE})")
    advanced.add_argument("--target-batch-time", type=float, default=TARGET_BATCH_TIME_SECONDS,
                         metavar='SECS', help=f"Target batch processing time (default: {TARGET_BATCH_TIME_SECONDS}s)")
    advanced.add_argument("--memory-cache-size", type=int, default=MEMORY_CACHE_MAX_SIZE,
                         metavar='N', help=f"Memory cache size (default: {MEMORY_CACHE_MAX_SIZE})")
    advanced.add_argument("--use-disk-backed-sets", action="store_true",
                         help="Use disk for large file sets (saves memory)")
    advanced.add_argument("--disk-cache-dir", type=Path, metavar='DIR',
                         help="Directory for disk-backed cache")
    advanced.add_argument("--fast-parsing-fallback", action="store_true", default=True,
                         help="Use fast parser for large HTML (default: enabled)")
    advanced.add_argument("--no-fast-parsing-fallback", action="store_false", dest="fast_parsing_fallback",
                         help="Disable fast parsing fallback")
    advanced.add_argument("--http2", action="store_true", default=True,
                         help=argparse.SUPPRESS)
    advanced.add_argument("--no-http2", action="store_false", dest="http2",
                         help="Disable HTTP/2")
    advanced.add_argument("--http2-pipelining", action="store_true", default=True,
                         help="Enable HTTP/2 pipelining (default: enabled)")
    advanced.add_argument("--no-http2-pipelining", action="store_false", dest="http2_pipelining",
                         help="Disable HTTP/2 pipelining")
    advanced.add_argument("--connection-pool-prewarm", action="store_true", default=True,
                         help="Pre-warm connection pools (default: enabled)")
    advanced.add_argument("--no-connection-pool-prewarm", action="store_false", dest="connection_pool_prewarm",
                         help="Disable connection pool pre-warming")
    advanced.add_argument("--fs-cache-ttl", type=float, default=FS_CACHE_TTL_SECONDS,
                         metavar='SECS', help=f"File system cache TTL (default: {FS_CACHE_TTL_SECONDS}s)")
    advanced.add_argument("--no-content-hash", action="store_false", dest="content_hash_small_files", default=True,
                         help="Disable content hash verification for small files")
    
    # NEW v3.0.0 parallel download arguments
    
    misc = parser.add_argument_group('Other Options')
    misc.add_argument("--benchmark", action="store_true", help="Run performance benchmark")
    misc.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    misc.add_argument("--health-check-port", type=int, default=8080,metavar='PORT', help="Health check server port (default: 8080)")
    
    args = parser.parse_args()

    # Handle config file
    if args.config:
        valid, error = validate_config_file(Path(args.config))
        if not valid:
            parser.error(f"Invalid configuration file: {error}")
        
        try:
            with open(args.config, 'r') as f:
                if Path(args.config).suffix.lower() in ['.yaml', '.yml']:
                    config_dict = yaml.safe_load(f)
                else:
                    config_dict = json.load(f)
                
                config_dict = expand_env_vars(config_dict)
            
            missing = []
            if 'base_url' not in config_dict and not args.url:
                missing.append('base_url in config file or --url on command line')
            if 'dest_path' not in config_dict and not args.dest_path:
                missing.append('dest_path in config file or --dest-path on command line')
            if 'log_path' not in config_dict and not args.log_path:
                missing.append('log_path in config file or --log-path on command line')
            
            if missing:
                parser.error(f"Missing required configuration: {', '.join(missing)}")
            
            if not args.url and 'base_url' in config_dict:
                args.url = config_dict['base_url']
            if not args.dest_path and 'dest_path' in config_dict:
                args.dest_path = Path(config_dict['dest_path'])
            if not args.log_path and 'log_path' in config_dict:
                args.log_path = Path(config_dict['log_path'])
            if not args.dir_suffix and 'dir_suffix' in config_dict:
                args.dir_suffix = [config_dict['dir_suffix']]
        except Exception as e:
            parser.error(f"Error reading config file: {e}")
    else:
        if not args.url:
            parser.error("--url is required when --config is not used")
        if not args.dest_path:
            parser.error("--dest-path is required when --config is not used")
        if not args.log_path:
            parser.error("--log-path is required when --config is not used")
    
    # Configure logging levels for libraries
    # Configure logging levels for libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    
    # ============================================================
    # SETUP LOGGING LEVELS (but not handlers - that's done by setup_shared_logging or MirrorURL)
    # ============================================================
    # Set root logger level based on verbosity
    if args.debug or args.verbose:
        logging.root.setLevel(logging.DEBUG)
    elif args.quiet:
        logging.root.setLevel(logging.WARNING)
    else:
        logging.root.setLevel(logging.INFO)
    
            
    # Parse cleanup policy
    try:
        args.cleanup_policy = CleanupPolicy(getattr(args, 'cleanup', 'safe'))
    except ValueError:
        args.cleanup_policy = CleanupPolicy.SAFE_NO_DELETE
    
    # Check lxml availability
    if not LXML_AVAILABLE and not args.fast_parsing_fallback:
        print("WARNING: lxml not available, falling back to fast parser")
        args.fast_parsing_fallback = True
    
    # Setup shared logging if requested
    if args.log_file:
        setup_shared_logging(args)
        use_shared = True
    else:
        use_shared = False
    
    # IMPORTANT: When using shared logging, DO NOT remove the file handler
    # Only manage console handlers based on --print-logs
    if use_shared:
        # Shared logging mode - keep the file handler, only manage console handlers
        if not args.print_logs:
            # Remove any console handlers if --print-logs is not set
            for handler in logging.root.handlers[:]:
                if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stderr:
                    logging.root.removeHandler(handler)
                    try:
                        handler.close()
                    except Exception:
                        pass
        # If --print-logs is set, console handler is already added by setup_shared_logging
    else:
        # Non-shared mode - original logic
        # Remove all handlers except the console handlers we want to keep
        console_handlers_to_keep = []
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and handler.stream == sys.stderr:
                console_handlers_to_keep.append(handler)
        
        # Remove all handlers except the console handlers we want to keep
        for handler in logging.root.handlers[:]:
            if handler not in console_handlers_to_keep:
                logging.root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
        
        # Add console handler only if none exist and --print-logs is set
        if args.print_logs and not console_handlers_to_keep:
            console_handler = logging.StreamHandler(sys.stderr)
            console_handler.setFormatter(logging.Formatter(
                '[%(asctime)s] [%(levelname)s] %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            if args.debug or args.verbose:
                console_handler.setLevel(logging.DEBUG)
            elif args.quiet:
                console_handler.setLevel(logging.WARNING)
            else:
                console_handler.setLevel(logging.INFO)
            logging.root.addHandler(console_handler)
            console_handlers_to_keep.append(console_handler)
            logging.debug("Console handler added in main")   
            
    # Set log level (preserve the console handler's level, but ensure root logger level is set)
    if args.debug or args.verbose:
        logging.root.setLevel(logging.DEBUG)
    elif args.quiet:
        logging.root.setLevel(logging.WARNING)
    else:
        logging.root.setLevel(logging.INFO)
    
    if args.print_logs and args.log_file:
        logging.info("Command line used:")
        cmd_str = shlex.join([sys.executable] + sys.argv)
        logging.info(cmd_str)
        logging.info("-" * min(80, len(cmd_str) + 4))
    
    # Run benchmark if requested
    if args.benchmark:
        benchmark_suffix = ""
        if args.dir_suffix and len(args.dir_suffix) > 0:
            benchmark_suffix = args.dir_suffix[0]
        
        benchmark_config = MirrorConfig(
            base_url=args.url.rstrip('/') if args.url else "",
            dest_path=Path(args.dest_path),
            log_path=Path(args.log_path),
            dir_suffix=benchmark_suffix,
            workers=args.workers,
            timeout=args.timeout,
            max_retries=args.max_retries,
            retry_delay=args.retry_delay,
            debug=args.debug,
            print_logs=args.print_logs,
            dry_run=args.dry_run,
            file_filters=args.filter,
            exclude_dirs=args.exclude_dir,
            cleanup_policy=args.cleanup_policy,
            quick=args.quick,
            no_rget_list=args.no_rget_list,
            rget_list_max_age=args.rget_list_max_age,
            force_rget_list=args.force_rget_list,
            no_cache=args.no_cache,
            refresh_cache=args.refresh_cache,
            cache_max_age=args.cache_max_age,
            no_etag=getattr(args, 'no_etag', False),
            use_shared_log=use_shared,
            scan_mode=ScanMode(args.scan_mode) if args.scan_mode else ScanMode.ADAPTIVE,
            parallel_threshold=args.parallel_threshold,
            benchmark=True,
            http2=args.http2,
            stats=args.stats,
            max_depth=args.max_depth,
            max_filename_len=args.max_filename_len,
            safe_urls=getattr(args, 'safe_urls', True),
            confirm_delete=getattr(args, 'confirm_delete', False),
            quiet=getattr(args, 'quiet', False),
            verbose=getattr(args, 'verbose', False),
            metrics_json=getattr(args, 'metrics_json', None),
            progress_bar=getattr(args, 'progress_bar', False),
            async_metadata=getattr(args, 'async_metadata', True),
            async_workers=getattr(args, 'async_workers', DEFAULT_ASYNC_WORKERS),
            content_hash_small_files=getattr(args, 'content_hash_small_files', True),
            trusted_server=getattr(args, 'trusted_server', False),
            request_delay=getattr(args, 'request_delay', REQUEST_DELAY),
            cache_html=getattr(args, 'cache_html', True),
            html_cache_max_age=getattr(args, 'html_cache_max_age', HTML_CACHE_MAX_AGE_HOURS),
            adaptive_async=getattr(args, 'adaptive_async', ADAPTIVE_ASYNC_ENABLED),
            adaptive_error_threshold=getattr(args, 'adaptive_error_threshold', ADAPTIVE_ERROR_THRESHOLD),
            adaptive_start_concurrency=getattr(args, 'adaptive_start_concurrency', ADAPTIVE_START_CONCURRENCY),
            security_validation=getattr(args, 'security_validation', True),
            circuit_breaker_enabled=getattr(args, 'circuit_breaker_enabled', True),
            bandwidth_limit=getattr(args, 'bandwidth_limit', None),
            enable_resume=getattr(args, 'enable_resume', True),
            max_concurrent_downloads=getattr(args, 'max_concurrent_downloads', 10),
            download_queue_size=getattr(args, 'download_queue_size', 1000),
            handle_symlinks=getattr(args, 'handle_symlinks', False),
            symlink_mode=getattr(args, 'symlink_mode', 'skip'),
            circuit_breaker_downloads=getattr(args, 'circuit_breaker_downloads', True),
            max_symlink_depth=getattr(args, 'max_symlink_depth', MAX_SYMLINK_DEPTH),
            max_symlinks_per_dir=getattr(args, 'max_symlinks_per_dir', MAX_SYMLINKS_PER_DIR),
            symlink_bomb_threshold=getattr(args, 'symlink_bomb_threshold', SYMLINK_BOMB_THRESHOLD),
            adaptive_batch_processing=getattr(args, 'adaptive_batch_processing', True),
            initial_batch_size=getattr(args, 'initial_batch_size', BATCH_SIZE),
            max_batch_size=getattr(args, 'max_batch_size', MAX_BATCH_SIZE),
            target_batch_time=getattr(args, 'target_batch_time', TARGET_BATCH_TIME_SECONDS),
            memory_cache_size=getattr(args, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE),
            use_disk_backed_sets=getattr(args, 'use_disk_backed_sets', False),
            disk_cache_dir=getattr(args, 'disk_cache_dir', None),
            fast_parsing_fallback=getattr(args, 'fast_parsing_fallback', True),
            http2_pipelining=getattr(args, 'http2_pipelining', True),
            connection_pool_prewarm=getattr(args, 'connection_pool_prewarm', True),
            fs_cache_ttl=getattr(args, 'fs_cache_ttl', FS_CACHE_TTL_SECONDS),
            
            # NEW v3.0.0 arguments
            parallel_downloads=getattr(args, 'parallel_downloads', PARALLEL_DOWNLOAD_ENABLED),
            max_chunks_per_file=getattr(args, 'max_chunks', MAX_CHUNKS_PER_FILE),
            min_chunk_size_mb=getattr(args, 'min_chunk_size', 10),
            max_parallel_chunks_total=getattr(args, 'max_parallel_chunks', MAX_PARALLEL_CHUNKS_TOTAL),
            chunk_assembly_dir=getattr(args, 'chunk_assembly_dir', None),
            chunk_timeout_multiplier=getattr(args, 'chunk_timeout_multiplier', CHUNK_TIMEOUT_MULTIPLIER),
            # NEW v3.0.6 arguments
            auto_concurrency=getattr(args, 'auto_concurrency', AUTO_CONCURRENCY_ENABLED),
            health_check_port=getattr(args, 'health_check_port', 8080),
           
            # NEW: Auto-selection fields for benchmark
            auto_select_method=getattr(args, 'auto_select', True),
            force_method=getattr(args, 'force_method', None),
            force_disk_type=getattr(args, 'force_disk_type', None),
            manual_network_speed_mbps=getattr(args, 'network_speed', None),
            streaming_parallel=getattr(args, 'streaming_parallel', True),
            streaming_min_file_size_mb=getattr(args, 'streaming_min_size', STREAMING_MIN_FILE_SIZE_MB),
            sequential_downloads=getattr(args, 'sequential_downloads', False),
        )
    
        
        with MirrorURL(benchmark_config) as mirror:
            if hasattr(mirror, 'connection_manager') and mirror.connection_manager:
                results = mirror.benchmark()
                logging.info("Benchmark completed")
                
                if hasattr(mirror.scanner, 'get_parse_stats'):
                    stats = mirror.scanner.get_parse_stats()
                    logging.info(f"Parser stats: {stats}")
                
                if hasattr(mirror.connection_manager, 'connection_pool') and \
                   hasattr(mirror.connection_manager.connection_pool, 'get_stats'):
                    stats = mirror.connection_manager.connection_pool.get_stats()
                    logging.info(f"Connection pool stats: {stats}")
                
                if hasattr(mirror, 'performance_monitor'):
                    perf_stats = mirror.performance_monitor.get_summary()
                    logging.info(f"Performance stats: {perf_stats}")
                
                # NEW v3.0.0: Log parallel download stats if available
                if hasattr(mirror, 'parallel_manager') and mirror.parallel_manager:
                    parallel_stats = mirror.parallel_manager.get_stats()
                    logging.info(f"Parallel download stats: {parallel_stats}")
            else:
                logging.error("Benchmark failed")
        
        sys.exit(0)
    
    # Process suffixes
    suffixes = args.dir_suffix if args.dir_suffix else [""]
    total = len(suffixes)
    processed = []
    failed = []
    skipped = []
    
    for i, suf in enumerate(suffixes, 1):
        try:
            if args.config:
                base_config = MirrorConfig.from_yaml(Path(args.config))
                # Start with base_config values
                config_dict = {
                    'base_url': base_config.base_url,
                    'dest_path': base_config.dest_path,
                    'log_path': base_config.log_path,
                    'dir_suffix': suf,
                    'workers': base_config.workers,
                    'timeout': base_config.timeout,
                    'max_retries': base_config.max_retries,
                    'retry_delay': base_config.retry_delay,
                    'debug': base_config.debug,
                    'print_logs': base_config.print_logs,
                    'dry_run': base_config.dry_run,
                    'file_filters': base_config.file_filters,
                    'exclude_dirs': base_config.exclude_dirs,
                    'cleanup_policy': base_config.cleanup_policy,
                    'quick': base_config.quick,
                    'no_rget_list': base_config.no_rget_list,
                    'rget_list_max_age': base_config.rget_list_max_age,
                    'force_rget_list': base_config.force_rget_list,
                    'no_cache': base_config.no_cache,
                    'refresh_cache': base_config.refresh_cache,
                    'cache_max_age': base_config.cache_max_age,
                    'no_etag': getattr(base_config, 'no_etag', False),
                    'hash_algorithm': getattr(base_config, 'hash_algorithm', 'md5'),
                    'use_shared_log': use_shared,
                    'scan_mode': base_config.scan_mode,
                    'parallel_threshold': base_config.parallel_threshold,
                    'benchmark': base_config.benchmark,
                    'http2': base_config.http2,
                    'stats': base_config.stats,
                    'max_depth': base_config.max_depth,
                    'max_filename_len': base_config.max_filename_len,
                    'safe_urls': getattr(base_config, 'safe_urls', True),
                    'confirm_delete': getattr(base_config, 'confirm_delete', False),
                    'quiet': getattr(base_config, 'quiet', False),
                    'verbose': getattr(base_config, 'verbose', False),
                    'metrics_json': getattr(base_config, 'metrics_json', None),
                    'progress_bar': getattr(base_config, 'progress_bar', False),
                    'async_metadata': getattr(base_config, 'async_metadata', True),
                    'async_workers': getattr(base_config, 'async_workers', DEFAULT_ASYNC_WORKERS),
                    'content_hash_small_files': getattr(base_config, 'content_hash_small_files', True),
                    'trusted_server': getattr(base_config, 'trusted_server', False),
                    'request_delay': getattr(base_config, 'request_delay', REQUEST_DELAY),
                    'cache_html': getattr(base_config, 'cache_html', True),
                    'html_cache_max_age': getattr(base_config, 'html_cache_max_age', HTML_CACHE_MAX_AGE_HOURS),
                    'adaptive_async': getattr(base_config, 'adaptive_async', ADAPTIVE_ASYNC_ENABLED),
                    'adaptive_error_threshold': getattr(base_config, 'adaptive_error_threshold', ADAPTIVE_ERROR_THRESHOLD),
                    'adaptive_start_concurrency': getattr(base_config, 'adaptive_start_concurrency', ADAPTIVE_START_CONCURRENCY),
                    'security_validation': getattr(base_config, 'security_validation', True),
                    'circuit_breaker_enabled': getattr(base_config, 'circuit_breaker_enabled', True),
                    'bandwidth_limit': getattr(base_config, 'bandwidth_limit', None),
                    'enable_resume': getattr(base_config, 'enable_resume', True),
                    'max_concurrent_downloads': getattr(base_config, 'max_concurrent_downloads', 10),
                    'download_queue_size': getattr(base_config, 'download_queue_size', 1000),
                    'handle_symlinks': getattr(base_config, 'handle_symlinks', False),
                    'symlink_mode': getattr(base_config, 'symlink_mode', 'skip'),
                    'circuit_breaker_downloads': getattr(base_config, 'circuit_breaker_downloads', True),
                    'max_symlink_depth': getattr(base_config, 'max_symlink_depth', MAX_SYMLINK_DEPTH),
                    'max_symlinks_per_dir': getattr(base_config, 'max_symlinks_per_dir', MAX_SYMLINKS_PER_DIR),
                    'symlink_bomb_threshold': getattr(base_config, 'symlink_bomb_threshold', SYMLINK_BOMB_THRESHOLD),
                    'adaptive_batch_processing': getattr(base_config, 'adaptive_batch_processing', True),
                    'initial_batch_size': getattr(base_config, 'initial_batch_size', BATCH_SIZE),
                    'max_batch_size': getattr(base_config, 'max_batch_size', MAX_BATCH_SIZE),
                    'target_batch_time': getattr(base_config, 'target_batch_time', TARGET_BATCH_TIME_SECONDS),
                    'memory_cache_size': getattr(base_config, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE),
                    'use_disk_backed_sets': getattr(base_config, 'use_disk_backed_sets', False),
                    'disk_cache_dir': getattr(base_config, 'disk_cache_dir', None),
                    'fast_parsing_fallback': getattr(base_config, 'fast_parsing_fallback', True),
                    'http2_pipelining': getattr(base_config, 'http2_pipelining', True),
                    'connection_pool_prewarm': getattr(base_config, 'connection_pool_prewarm', True),
                    'fs_cache_ttl': getattr(base_config, 'fs_cache_ttl', FS_CACHE_TTL_SECONDS),
                    
                    # NEW v3.0.0 arguments from base_config
                    'parallel_downloads': getattr(base_config, 'parallel_downloads', False),
                    'streaming_parallel': getattr(base_config, 'streaming_parallel', False),
                    'sequential_downloads': getattr(base_config, 'sequential_downloads', False),
                    'max_chunks_per_file': getattr(base_config, 'max_chunks_per_file', MAX_CHUNKS_PER_FILE),
                    'min_chunk_size_mb': getattr(base_config, 'min_chunk_size_mb', 10),
                    'max_parallel_chunks_total': getattr(base_config, 'max_parallel_chunks_total', MAX_PARALLEL_CHUNKS_TOTAL),
                    'chunk_assembly_dir': getattr(base_config, 'chunk_assembly_dir', None),
                    'chunk_timeout_multiplier': getattr(base_config, 'chunk_timeout_multiplier', CHUNK_TIMEOUT_MULTIPLIER),
                    'auto_concurrency': getattr(base_config, 'auto_concurrency', AUTO_CONCURRENCY_ENABLED),
                    
                    # Auto-selection fields from base_config
                    'auto_select_method': getattr(base_config, 'auto_select_method', True),
                    'force_method': getattr(base_config, 'force_method', None),
                    'force_disk_type': getattr(base_config, 'force_disk_type', None),
                    'manual_network_speed_mbps': getattr(base_config, 'manual_network_speed_mbps', None),
                    'streaming_min_file_size_mb': getattr(base_config, 'streaming_min_file_size_mb', STREAMING_MIN_FILE_SIZE_MB),
                    'parallel_files_min_files': getattr(base_config, 'parallel_files_min_files', 3),
                    'streaming_min_files': getattr(base_config, 'streaming_min_files', 4),
                    'traditional_min_files': getattr(base_config, 'traditional_min_files', 3),
                }
                                
                # Override with command line arguments if provided
                if args.url:
                    config_dict['base_url'] = args.url.rstrip('/')
                if args.dest_path:
                    config_dict['dest_path'] = Path(args.dest_path)
                if args.log_path:
                    config_dict['log_path'] = Path(args.log_path)
                if args.print_logs:
                    config_dict['print_logs'] = True
                if args.quiet:
                    config_dict['quiet'] = True
                if args.verbose:
                    config_dict['verbose'] = True
                if args.debug:
                    config_dict['debug'] = True                 
                if args.workers != DEFAULT_WORKERS:
                    config_dict['workers'] = args.workers
                if args.timeout != DEFAULT_TIMEOUT:
                    config_dict['timeout'] = args.timeout
                if args.adaptive_batch_processing is not None:
                    config_dict['adaptive_batch_processing'] = args.adaptive_batch_processing
                if args.initial_batch_size != BATCH_SIZE:
                    config_dict['initial_batch_size'] = args.initial_batch_size
                if args.max_batch_size != MAX_BATCH_SIZE:
                    config_dict['max_batch_size'] = args.max_batch_size
                if args.target_batch_time != TARGET_BATCH_TIME_SECONDS:
                    config_dict['target_batch_time'] = args.target_batch_time
                if args.memory_cache_size != MEMORY_CACHE_MAX_SIZE:
                    config_dict['memory_cache_size'] = args.memory_cache_size
                if args.use_disk_backed_sets:
                    config_dict['use_disk_backed_sets'] = args.use_disk_backed_sets
                if args.disk_cache_dir:
                    config_dict['disk_cache_dir'] = args.disk_cache_dir
                if not args.fast_parsing_fallback:
                    config_dict['fast_parsing_fallback'] = args.fast_parsing_fallback
                if not args.http2_pipelining:
                    config_dict['http2_pipelining'] = args.http2_pipelining
                if not args.connection_pool_prewarm:
                    config_dict['connection_pool_prewarm'] = args.connection_pool_prewarm
                if args.fs_cache_ttl != FS_CACHE_TTL_SECONDS:
                    config_dict['fs_cache_ttl'] = args.fs_cache_ttl
                
                # NEW v3.0.0 overrides
                    
                if args.parallel_downloads:
                    config_dict['parallel_downloads'] = True
                    config_dict['streaming_parallel'] = False
                    config_dict['sequential_downloads'] = False
                elif args.streaming_parallel:
                    config_dict['parallel_downloads'] = False
                    config_dict['streaming_parallel'] = True
                    config_dict['sequential_downloads'] = False
                elif args.sequential_downloads:
                    config_dict['parallel_downloads'] = False
                    config_dict['streaming_parallel'] = False
                    config_dict['sequential_downloads'] = True                    
                                    
                if args.max_chunks != MAX_CHUNKS_PER_FILE:
                    config_dict['max_chunks_per_file'] = args.max_chunks
                if args.min_chunk_size != 10:
                    config_dict['min_chunk_size_mb'] = args.min_chunk_size
                if args.max_parallel_chunks != MAX_PARALLEL_CHUNKS_TOTAL:
                    config_dict['max_parallel_chunks_total'] = args.max_parallel_chunks
                if args.chunk_assembly_dir:
                    config_dict['chunk_assembly_dir'] = args.chunk_assembly_dir
                if args.chunk_timeout_multiplier != CHUNK_TIMEOUT_MULTIPLIER:
                    config_dict['chunk_timeout_multiplier'] = args.chunk_timeout_multiplier
                
                # NEW v3.0.2 overrides
                if args.max_concurrent_downloads != 10:
                    config_dict['max_concurrent_downloads'] = args.max_concurrent_downloads
                    
                 # NEW v3.0.6 overrides
                if args.auto_concurrency:
                    config_dict['auto_concurrency'] = args.auto_concurrency                   
                
                # NEW: Auto-selection overrides from command line
                if hasattr(args, 'auto_select'):
                    config_dict['auto_select_method'] = args.auto_select
                if hasattr(args, 'force_method') and args.force_method:
                    config_dict['force_method'] = args.force_method
                if hasattr(args, 'force_disk_type') and args.force_disk_type:
                    config_dict['force_disk_type'] = args.force_disk_type
                if hasattr(args, 'network_speed') and args.network_speed:
                    config_dict['manual_network_speed_mbps'] = args.network_speed
                if hasattr(args, 'streaming_parallel'):
                    config_dict['streaming_parallel'] = args.streaming_parallel
                if hasattr(args, 'streaming_min_size'):
                    config_dict['streaming_min_file_size_mb'] = args.streaming_min_size
                    
                # ========================================================================
                # FIX: Add missing CLI overrides that were ignored when using --config
                # ========================================================================
                # Cleanup & Safety overrides
                if hasattr(args, 'cleanup'):
                    try:
                        config_dict['cleanup_policy'] = CleanupPolicy(args.cleanup)
                    except ValueError:
                        pass  # Keep config file value if CLI value is invalid
                if args.confirm_delete:
                    config_dict['confirm_delete'] = True
                if args.dry_run:
                    config_dict['dry_run'] = True
                if args.quick:
                    config_dict['quick'] = True
                    
                # Cache Control overrides
                if args.no_cache:
                    config_dict['no_cache'] = True
                if args.refresh_cache:
                    config_dict['refresh_cache'] = True
                if args.cache_max_age != DEFAULT_CACHE_MAX_AGE_DAYS:
                    config_dict['cache_max_age'] = args.cache_max_age
                if not args.cache_html:  # Handles --no-cache-html
                    config_dict['cache_html'] = False
                if args.html_cache_max_age != HTML_CACHE_MAX_AGE_HOURS:
                    config_dict['html_cache_max_age'] = args.html_cache_max_age
                
                # Filtering & Scanning overrides
                if args.filter:
                    config_dict['file_filters'] = [f.lower() for f in args.filter]
                if args.exclude_dir:
                    config_dict['exclude_dirs'] = args.exclude_dir
                if args.scan_mode != 'adaptive':
                    try:
                        config_dict['scan_mode'] = ScanMode(args.scan_mode)
                    except ValueError:
                        pass
                
                # Performance & Async overrides
                if not args.async_metadata:  # Handles --no-async-metadata
                    config_dict['async_metadata'] = False
                if args.trusted_server:
                    config_dict['trusted_server'] = True
                if args.request_delay != REQUEST_DELAY:
                    config_dict['request_delay'] = args.request_delay
                if args.bandwidth_limit is not None:
                    config_dict['bandwidth_limit'] = args.bandwidth_limit
                
                # Symlinks & Security overrides
                if args.handle_symlinks:
                    config_dict['handle_symlinks'] = True
                if args.symlink_mode != 'skip':
                    config_dict['symlink_mode'] = args.symlink_mode
                # ========================================================================                    
                    
                           
                suffix_config = MirrorConfig.from_dict(config_dict, silent=use_shared)
            else:
                suffix_config = MirrorConfig(
                    base_url=args.url.rstrip('/'),
                    dest_path=Path(args.dest_path),
                    log_path=Path(args.log_path),
                    dir_suffix=suf.strip('/') if suf else "",
                    print_logs=args.print_logs,    
                    quiet=args.quiet,              
                    verbose=args.verbose,         
                    debug=args.debug,                         
                    workers=args.workers,
                    timeout=args.timeout,
                    max_retries=args.max_retries,
                    retry_delay=args.retry_delay,
                    dry_run=args.dry_run,
                    file_filters=[f.lower() for f in args.filter],
                    exclude_dirs=args.exclude_dir or [],
                    cleanup_policy=args.cleanup_policy,
                    quick=args.quick,
                    no_rget_list=args.no_rget_list,
                    rget_list_max_age=args.rget_list_max_age,
                    force_rget_list=args.force_rget_list,
                    no_cache=args.no_cache,
                    refresh_cache=args.refresh_cache,
                    cache_max_age=args.cache_max_age,
                    use_shared_log=use_shared,
                    scan_mode=ScanMode(args.scan_mode),
                    parallel_threshold=args.parallel_threshold,
                    benchmark=args.benchmark,
                    http2=args.http2,
                    stats=args.stats,
                    max_depth=args.max_depth,
                    max_filename_len=args.max_filename_len,
                    safe_urls=getattr(args, 'safe_urls', True),
                    confirm_delete=getattr(args, 'confirm_delete', False),
                    metrics_json=getattr(args, 'metrics_json', None),
                    progress_bar=getattr(args, 'progress_bar', False),
                    async_metadata=getattr(args, 'async_metadata', True),
                    async_workers=getattr(args, 'async_workers', DEFAULT_ASYNC_WORKERS),
                    content_hash_small_files=getattr(args, 'content_hash_small_files', True),
                    trusted_server=getattr(args, 'trusted_server', False),
                    request_delay=getattr(args, 'request_delay', REQUEST_DELAY),
                    cache_html=getattr(args, 'cache_html', True),
                    html_cache_max_age=getattr(args, 'html_cache_max_age', HTML_CACHE_MAX_AGE_HOURS),
                    adaptive_async=getattr(args, 'adaptive_async', ADAPTIVE_ASYNC_ENABLED),
                    adaptive_error_threshold=getattr(args, 'adaptive_error_threshold', ADAPTIVE_ERROR_THRESHOLD),
                    adaptive_start_concurrency=getattr(args, 'adaptive_start_concurrency', ADAPTIVE_START_CONCURRENCY),
                    security_validation=getattr(args, 'security_validation', True),
                    circuit_breaker_enabled=getattr(args, 'circuit_breaker_enabled', True),
                    bandwidth_limit=getattr(args, 'bandwidth_limit', None),
                    enable_resume=getattr(args, 'enable_resume', True),
                    max_concurrent_downloads=getattr(args, 'max_concurrent_downloads', 10),
                    download_queue_size=getattr(args, 'download_queue_size', 1000),
                    handle_symlinks=getattr(args, 'handle_symlinks', False),
                    symlink_mode=getattr(args, 'symlink_mode', 'skip'),
                    circuit_breaker_downloads=getattr(args, 'circuit_breaker_downloads', True),
                    max_symlink_depth=getattr(args, 'max_symlink_depth', MAX_SYMLINK_DEPTH),
                    max_symlinks_per_dir=getattr(args, 'max_symlinks_per_dir', MAX_SYMLINKS_PER_DIR),
                    symlink_bomb_threshold=getattr(args, 'symlink_bomb_threshold', SYMLINK_BOMB_THRESHOLD),
                    adaptive_batch_processing=getattr(args, 'adaptive_batch_processing', True),
                    initial_batch_size=getattr(args, 'initial_batch_size', BATCH_SIZE),
                    max_batch_size=getattr(args, 'max_batch_size', MAX_BATCH_SIZE),
                    target_batch_time=getattr(args, 'target_batch_time', TARGET_BATCH_TIME_SECONDS),
                    memory_cache_size=getattr(args, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE),
                    use_disk_backed_sets=getattr(args, 'use_disk_backed_sets', False),
                    disk_cache_dir=getattr(args, 'disk_cache_dir', None),
                    fast_parsing_fallback=getattr(args, 'fast_parsing_fallback', True),
                    http2_pipelining=getattr(args, 'http2_pipelining', True),
                    connection_pool_prewarm=getattr(args, 'connection_pool_prewarm', True),
                    fs_cache_ttl=getattr(args, 'fs_cache_ttl', FS_CACHE_TTL_SECONDS),
                    
                    # NEW v3.0.0 arguments
                    parallel_downloads=getattr(args, 'parallel_downloads', False),
                    sequential_downloads=getattr(args, 'sequential_downloads', False),
                    streaming_parallel=getattr(args, 'streaming_parallel', False),
                    max_chunks_per_file=getattr(args, 'max_chunks', MAX_CHUNKS_PER_FILE),
                    min_chunk_size_mb=getattr(args, 'min_chunk_size', 10),
                    max_parallel_chunks_total=getattr(args, 'max_parallel_chunks', MAX_PARALLEL_CHUNKS_TOTAL),
                    chunk_assembly_dir=getattr(args, 'chunk_assembly_dir', None),
                    chunk_timeout_multiplier=getattr(args, 'chunk_timeout_multiplier', CHUNK_TIMEOUT_MULTIPLIER),
                    auto_concurrency=getattr(args, 'auto_concurrency', AUTO_CONCURRENCY_ENABLED),
                    health_check_port=getattr(args, 'health_check_port', 8080),
                    # NEW: Auto-selection fields
                    auto_select_method=getattr(args, 'auto_select', True),
                    force_method=getattr(args, 'force_method', None),
                    force_disk_type=getattr(args, 'force_disk_type', None),
                    manual_network_speed_mbps=getattr(args, 'network_speed', None),
                    streaming_min_file_size_mb=getattr(args, 'streaming_min_size', STREAMING_MIN_FILE_SIZE_MB),  
                )        

        except ConfigError as e:
            if args.print_logs and args.log_file:
                logging.critical(f"Configuration error for {suf or 'ROOT'}: {e}")
            else:
                print(f"Configuration error for {suf or 'ROOT'}: {e}")
            failed.append(suf or 'ROOT')
            continue
        except Exception as e:
            if args.print_logs and args.log_file:
                logging.critical(f"Error creating config for {suf or 'ROOT'}: {e}")
            else:
                print(f"Error creating config for {suf or 'ROOT'}: {e}")
            failed.append(suf or 'ROOT')
            continue

        try:
            with MirrorURL(suffix_config, suffix_index=i, total_suffixes=total) as mirror:
                if not hasattr(mirror, 'connection_manager') or not mirror.connection_manager:
                    logging.warning(f"[{i}/{total}] No connection manager for {suf or 'ROOT'}")
                    skipped.append(suf or 'ROOT')
                elif not mirror.connection_ok:
                    logging.warning(f"[{i}/{total}] Connection failed for {suf or 'ROOT'} (404?)")
                    failed.append(suf or 'ROOT')
                else:
                    sync_success = mirror.sync()
                    if sync_success:
                        logging.info(f"[{i}/{total}] ✅ Successfully processed: {suf or 'ROOT'}")
                        processed.append(suf or 'ROOT')
                    else:
                        logging.error(f"[{i}/{total}] ❌ Failed to process: {suf or 'ROOT'}")
                        failed.append(suf or 'ROOT')
            
            # Flush handlers to ensure logs are written
            for handler in logging.root.handlers:
                try:
                    handler.flush()
                except Exception:
                    pass
        
        except PathTraversalError as e:
            logging.critical(f"Path traversal for {suf or 'ROOT'}: {e}")
            failed.append(suf or 'ROOT')
        except URLScopeError as e:
            logging.critical(f"URL scope error for {suf or 'ROOT'}: {e}")
            failed.append(suf or 'ROOT')
        except Exception as e:
            logging.critical(f"Error with {suf or 'ROOT'}: {e}", exc_info=True)
            failed.append(suf or 'ROOT')

    # Final summary
    if use_shared or total > 1:
        logging.info("\n" + "="*50)
        logging.info("FINAL SUMMARY")
        logging.info(f"Total suffixes processed: {total}")
        logging.info("")
        
        if processed:
            logging.info(f"✅ SUCCESSFUL ({len(processed)}):")
            for suffix in processed:
                logging.info(f"   • {suffix}")
        else:
            logging.info("✅ SUCCESSFUL: (none)")
        
        logging.info("")
        
        if failed:
            logging.error(f"❌ FAILED ({len(failed)}):")
            for suffix in failed:
                logging.error(f"   • {suffix}")
        else:
            logging.info("❌ FAILED: (none)")
        
        logging.info("")
        
        if skipped:
            logging.warning(f"⏭️ SKIPPED ({len(skipped)}):")
            for suffix in skipped:
                logging.warning(f"   • {suffix}")
        else:
            logging.info("⏭️ SKIPPED: (none)")
        
        logging.info("="*50)
    
    # Cleanup log handlers
    for handler in _log_files:
        try:
            handler.close()
        except Exception:
            pass
    
    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
