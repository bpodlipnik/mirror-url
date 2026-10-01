"""All module-level tuning constants and static tables.

Migrated verbatim from ``mirror_url.py`` (orig. lines 184-358). Pure data — no
imports, no side effects. Every other module imports its constants from here.
"""

from __future__ import annotations

# ============================================================================
# CONSTANTS
# ============================================================================
# Core settings
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 2
DEFAULT_TIMEOUT = 30
DEFAULT_WORKERS = 8
DEFAULT_ASYNC_WORKERS = 50
MAX_DIRECTORY_DEPTH = 50
LIST_DIRS_DEFAULT_MAX_DEPTH = 1  # immediate children only, unless --max-depth overrides it

# Rate limiting
REQUEST_DELAY = 0.05
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
CONTENT_HASH_THRESHOLD = 512 * 1024

# Scanning
PARALLEL_SCAN_THRESHOLD = 10
MAX_IN_MEMORY_CACHE_SIZE = 1000
BATCH_SIZE = 200

# Comparison tolerance
TIMESTAMP_TOLERANCE_SECONDS = 1.5

# Safety limits
MAX_WORKERS_HARD_LIMIT = 50
MIN_TIMEOUT = 3
MAX_TIMEOUT = 300
MAX_CACHE_AGE_DAYS = 365

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

# Known throttled domains
KNOWN_THROTTLED_DOMAINS = [
    "nascom.nasa.gov",
    "soho",
    "sdac",
    "lasp",
    "spdf.gsfc.nasa.gov",
    "cdaweb.gsfc.nasa.gov",
    "helioviewer.org",
]

# Windows reserved names
WINDOWS_RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


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
FS_CACHE_TTL_SECONDS = 5.0
FAST_PARSE_MIN_CONTENT_LENGTH = 1024 * 1024

# NEW v2.0.0 constants
PARTIAL_SUFFIX = ".mirror-partial"
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
# HEALTH_CHECK_PORT = 8080  # For health check API <- moved to MirrorConfig

# NEW v3.0.0 constants - Parallel Downloads
MAX_CHUNKS_PER_FILE = 8
MAX_PARALLEL_CHUNKS_TOTAL = 50
CHUNK_TIMEOUT_MULTIPLIER = 1.5
PARALLEL_DOWNLOAD_ENABLED = False  # Default off for backward compatibility


# v3.0.6 constants - Unified Concurrency - REDUCED to prevent deadlocks
UNIFIED_MAX_TOTAL_THREADS = 50  # Changed from 500 - prevent thread explosion
UNIFIED_MAX_ASYNC_TASKS = 50  # Changed from 500
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
STREAMING_WRITE_BUFFER_SIZE = 1024 * 1024  # 1MB write buffer
STREAMING_MIN_FILE_SIZE_MB = 100
