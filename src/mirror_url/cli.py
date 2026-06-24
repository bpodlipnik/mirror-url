"""Argument parsing, logging setup, and the ``main()`` entry point.

Migrated verbatim from ``mirror_url.py``:
``add_parallel_arguments`` (orig. 13956-13992), ``setup_shared_logging`` (orig.
13993-14126), ``main`` (orig. 14127-15142). The ``if __name__ == "__main__"``
guard lives in ``__main__.py`` instead.
"""

from __future__ import annotations

import argparse
import json
import logging
import shlex
import sys
import time
from pathlib import Path

import yaml

from ._version import __version__
from .compat import LXML_AVAILABLE, PSUTIL_AVAILABLE, TQDM_AVAILABLE
from .config import MirrorConfig, expand_env_vars, validate_config_file
from .constants import (
    ADAPTIVE_ASYNC_ENABLED,
    ADAPTIVE_ERROR_THRESHOLD,
    ADAPTIVE_MAX_CONCURRENCY,
    ADAPTIVE_START_CONCURRENCY,
    AUTO_CONCURRENCY_ENABLED,
    BATCH_SIZE,
    CHUNK_TIMEOUT_MULTIPLIER,
    CONTENT_HASH_THRESHOLD,
    DEFAULT_ASYNC_WORKERS,
    DEFAULT_CACHE_MAX_AGE_DAYS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RGET_LIST_MAX_AGE,
    DEFAULT_TIMEOUT,
    DEFAULT_WORKERS,
    FS_CACHE_TTL_SECONDS,
    HTML_CACHE_MAX_AGE_HOURS,
    MAX_BATCH_SIZE,
    MAX_CHUNKS_PER_FILE,
    MAX_DIRECTORY_DEPTH,
    MAX_FILENAME_LENGTH,
    MAX_PARALLEL_CHUNKS_TOTAL,
    MAX_SYMLINK_DEPTH,
    MAX_SYMLINKS_PER_DIR,
    MEMORY_CACHE_MAX_SIZE,
    PARALLEL_DOWNLOAD_ENABLED,
    PARALLEL_SCAN_THRESHOLD,
    REQUEST_DELAY,
    STREAMING_MIN_FILE_SIZE_MB,
    SYMLINK_BOMB_THRESHOLD,
    TARGET_BATCH_TIME_SECONDS,
)
from .core import MirrorURL
from .enums import CleanupPolicy, ScanMode
from .exceptions import ConfigError, PathTraversalError, URLScopeError
from .utils import _log_files


def add_parallel_arguments(parser: argparse.ArgumentParser) -> None:
    """Add parallel download arguments to parser"""
    parallel_grp = parser.add_argument_group("Download Method Options")
    method_group = parallel_grp.add_mutually_exclusive_group()
    method_group.add_argument(
        "--parallel-downloads",
        action="store_true",
        help="Enable traditional parallel downloads (temp files, safe)",
    )
    method_group.add_argument(
        "--streaming-parallel",
        action="store_true",
        help="Enable streaming parallel downloads (direct write, faster for huge files)",
    )
    method_group.add_argument(
        "--sequential-downloads",
        action="store_true",
        help="Force sequential downloads (no parallelism)",
    )

    parallel_grp.add_argument(
        "--max-chunks",
        type=int,
        default=MAX_CHUNKS_PER_FILE,
        metavar="N",
        help=f"Maximum chunks per file (default: {MAX_CHUNKS_PER_FILE})",
    )
    parallel_grp.add_argument(
        "--min-chunk-size",
        type=int,
        default=10,
        metavar="MB",
        help="Minimum chunk size in MB (default: 10MB)",
    )
    parallel_grp.add_argument(
        "--max-parallel-chunks",
        type=int,
        default=MAX_PARALLEL_CHUNKS_TOTAL,
        metavar="N",
        help=f"Maximum total parallel chunks (default: {MAX_PARALLEL_CHUNKS_TOTAL})",
    )
    parallel_grp.add_argument(
        "--chunk-assembly-dir",
        type=Path,
        metavar="DIR",
        help="Directory for temporary chunk storage",
    )
    parallel_grp.add_argument(
        "--chunk-timeout-multiplier",
        type=float,
        default=CHUNK_TIMEOUT_MULTIPLIER,
        metavar="MULT",
        help=f"Timeout multiplier for chunks (default: {CHUNK_TIMEOUT_MULTIPLIER})",
    )
    parallel_grp.add_argument(
        "--auto-concurrency",
        action="store_true",
        help="Automatically tune parallel download concurrency based on throughput (v3.0.6)",
    )
    # NEW: Auto-selection arguments
    auto_grp = parser.add_argument_group("Auto-Optimization Options")
    auto_grp.add_argument(
        "--auto-select",
        action="store_true",
        default=True,
        help="Automatically select best download method (default: enabled)",
    )
    auto_grp.add_argument(
        "--no-auto-select",
        action="store_false",
        dest="auto_select",
        help="Disable automatic method selection",
    )
    auto_grp.add_argument(
        "--force-method",
        choices=["sequential", "parallel_files", "streaming", "traditional"],
        help="Force specific download method",
    )
    auto_grp.add_argument(
        "--force-disk-type",
        choices=["ssd", "hdd", "nvme"],
        help="Manually specify disk type for optimization",
    )
    auto_grp.add_argument(
        "--network-speed", type=float, metavar="MBPS", help="Manually specify network speed in Mbps"
    )


def setup_shared_logging(args: argparse.Namespace) -> None:
    """Setup shared logging for multiple suffixes"""
    # Create log filename with suffixes properly separated by underscores
    suffixes_str = "_".join(args.dir_suffix) if args.dir_suffix else "all"
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    log_filename = f"{args.log_file}{suffixes_str}_{timestamp}.log"
    log_path = Path(args.log_path) / log_filename

    # Remove ALL existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        if hasattr(handler, "close"):
            try:
                handler.close()
            except Exception:
                pass

    # Create file handler (always)
    file_handler = logging.FileHandler(str(log_path), mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG if args.debug else logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )

    # Create console handler (if print-logs is enabled)
    handlers = [file_handler]
    if args.print_logs:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
            )
        )
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
    logging.info("=" * 50)
    logging.info(f"MirrorURL v{__version__} - SHARED LOG")
    logging.info(f"Log: {log_path}")

    if args.dir_suffix:
        logging.info(f"Suffixes: {args.dir_suffix}")

    cleanup_policy = getattr(args, "cleanup_policy", CleanupPolicy.SAFE_NO_DELETE)
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
    if getattr(args, "safe_urls", True):
        logging.info("🔒 URL sanitization enabled")

    logging.info(
        f"🛡️ Path safety: max_depth={args.max_depth}, max_filename_len={args.max_filename_len}"
    )

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
            logging.info(
                f"🔄 Adaptive async: {args.adaptive_start_concurrency}-{ADAPTIVE_MAX_CONCURRENCY} workers"
            )
        else:
            logging.info(f"⚡ Async meta {args.async_workers} workers")
    if args.content_hash_small_files:
        logging.info(f"🔐 Content hash: <{CONTENT_HASH_THRESHOLD / 1024:.0f}KB")

    delay_ms = args.request_delay * 1000
    logging.info(f"⚡ Rate limit: {delay_ms:.1f}ms{' (trusted)' if args.trusted_server else ''}")

    if args.cache_html:
        logging.info(f"📦 HTML cache: {args.html_cache_max_age}h")
    if args.bandwidth_limit:
        logging.info(f"⏱️ Bandwidth limit: {args.bandwidth_limit} MB/s")
    if getattr(args, "enable_resume", True):
        logging.info("↩️ Resume enabled")
    if args.handle_symlinks:
        logging.info(f"🔗 Symlink handling: {args.symlink_mode}")
    if getattr(args, "adaptive_batch_processing", True):
        logging.info(
            f"📈 Adaptive batch processing: initial={getattr(args, 'initial_batch_size', BATCH_SIZE)}"
        )
    if getattr(args, "use_disk_backed_sets", False):
        logging.info(
            f"💾 Disk-backed sets: memory={getattr(args, 'memory_cache_size', MEMORY_CACHE_MAX_SIZE)}"
        )
    if getattr(args, "fast_parsing_fallback", True):
        logging.info("⚡ Fast parsing fallback enabled")
    if getattr(args, "connection_pool_prewarm", True):
        logging.info("🔥 Connection pool pre-warming enabled")
    if PSUTIL_AVAILABLE:
        logging.info("📊 Memory monitoring: ENABLED")
    if args.metrics_json:
        health_port = getattr(args, "health_check_port", 8080)
        logging.info(f"🏥 Health check API: http://localhost:{health_port}/health")
    if getattr(args, "parallel_downloads", False):
        logging.info(
            f"🚀 Parallel downloads: ENABLED (max {args.max_chunks} chunks, {args.min_chunk_size}MB min)"
        )
    if getattr(args, "max_concurrent_downloads", 10) > 1:
        logging.info(f"📥 Max concurrent file downloads: {args.max_concurrent_downloads}")

    logging.info("=" * 50)


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
""",
    )

    basic = parser.add_argument_group("Required Options")
    basic.add_argument("--url", help="Base URL to mirror (required if --config not used)")
    basic.add_argument(
        "--dest-path", type=Path, help="Destination directory (required if --config not used)"
    )
    basic.add_argument(
        "--log-path", type=Path, help="Log directory (required if --config not used)"
    )
    basic.add_argument("--config", help="Configuration file (YAML/JSON)")

    # Create mutually exclusive group for download modes
    download_mode_group = parser.add_argument_group("Download Mode Options (select ONE)")
    mode_group = download_mode_group.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--parallel-downloads",
        action="store_true",
        help="Traditional parallel downloads (temp files, safe, supports resume)",
    )
    mode_group.add_argument(
        "--streaming-parallel",
        action="store_true",
        help="Streaming parallel downloads (direct write, faster for huge files)",
    )
    mode_group.add_argument(
        "--sequential-downloads",
        action="store_true",
        help="Sequential downloads (no parallelism, one file at a time)",
    )

    # Parallel Download Options (shared settings)
    parallel_grp = parser.add_argument_group("Parallel Download Options")
    parallel_grp.add_argument(
        "--max-chunks",
        type=int,
        default=MAX_CHUNKS_PER_FILE,
        metavar="N",
        help=f"Maximum chunks per file (default: {MAX_CHUNKS_PER_FILE})",
    )
    parallel_grp.add_argument(
        "--min-chunk-size",
        type=int,
        default=10,
        metavar="MB",
        help="Minimum chunk size in MB (default: 10MB)",
    )
    parallel_grp.add_argument(
        "--max-parallel-chunks",
        type=int,
        default=MAX_PARALLEL_CHUNKS_TOTAL,
        metavar="N",
        help=f"Maximum total parallel chunks (default: {MAX_PARALLEL_CHUNKS_TOTAL})",
    )
    parallel_grp.add_argument(
        "--max-concurrent-downloads",
        type=int,
        default=10,
        metavar="N",
        help="Maximum concurrent file downloads (default: 10)",
    )
    parallel_grp.add_argument(
        "--auto-concurrency",
        action="store_true",
        help="Automatically tune parallel download concurrency based on throughput",
    )
    parallel_grp.add_argument(
        "--chunk-assembly-dir",
        type=Path,
        metavar="DIR",
        help="Directory for temporary chunk storage",
    )
    parallel_grp.add_argument(
        "--chunk-timeout-multiplier",
        type=float,
        default=CHUNK_TIMEOUT_MULTIPLIER,
        metavar="MULT",
        help=f"Timeout multiplier for chunks (default: {CHUNK_TIMEOUT_MULTIPLIER})",
    )

    filter_grp = parser.add_argument_group("Filter Options")
    filter_grp.add_argument(
        "--filter",
        nargs="*",
        default=[],
        metavar="PATTERN",
        help="File patterns to include (can be simple extension like .fits or regex pattern). "
        "Examples:\n"
        "  --filter .fits .txt .jpg           # Multiple simple extensions\n"
        "  --filter '.*\\.fits$'               # Regex: any .fits files\n"
        "  --filter '2024.*\\.fits' .txt       # Mixed regex and extensions",
    )

    directory = parser.add_argument_group("Directory Options")
    directory.add_argument(
        "--dir-suffix",
        nargs="*",
        default=[],
        metavar="SUFFIX",
        help="Directory suffixes to mirror (e.g., L1/v1 L2/v2)",
    )
    directory.add_argument(
        "--exclude-dir", nargs="*", default=[], metavar="DIR", help="Directories to exclude"
    )

    performance = parser.add_argument_group("Performance & Worker Options")
    performance.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        metavar="N",
        help=f"Sync workers (default: {DEFAULT_WORKERS})",
    )
    performance.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        metavar="SECS",
        help=f"Request timeout (default: {DEFAULT_TIMEOUT}s)",
    )
    performance.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        metavar="N",
        help=f"Max retries per request (default: {DEFAULT_MAX_RETRIES})",
    )
    performance.add_argument(
        "--retry-delay",
        type=int,
        default=DEFAULT_RETRY_DELAY,
        metavar="SECS",
        help=f"Delay between retries (default: {DEFAULT_RETRY_DELAY}s)",
    )
    performance.add_argument(
        "--trusted-server",
        action="store_true",
        help="Use faster rate limiting (10ms vs 50ms delay)",
    )
    performance.add_argument(
        "--request-delay",
        type=float,
        default=REQUEST_DELAY,
        metavar="SECS",
        help=f"Request delay (default: {REQUEST_DELAY}s)",
    )
    performance.add_argument(
        "--bandwidth-limit", type=float, metavar="MB/S", help="Limit download bandwidth (MB/s)"
    )

    cache = parser.add_argument_group("Cache Options")
    cache.add_argument("--no-cache", action="store_true", help="Disable cache")
    cache.add_argument("--refresh-cache", action="store_true", help="Force cache refresh")
    cache.add_argument(
        "--cache-max-age",
        type=int,
        default=DEFAULT_CACHE_MAX_AGE_DAYS,
        metavar="DAYS",
        help=f"Cache max age (default: {DEFAULT_CACHE_MAX_AGE_DAYS} days)",
    )
    cache.add_argument(
        "--cache-html",
        action="store_true",
        default=True,
        help="Cache parsed HTML content (default: enabled)",
    )
    cache.add_argument(
        "--no-cache-html", action="store_false", dest="cache_html", help="Disable HTML caching"
    )
    cache.add_argument(
        "--html-cache-max-age",
        type=int,
        default=HTML_CACHE_MAX_AGE_HOURS,
        metavar="HOURS",
        help=f"HTML cache max age (default: {HTML_CACHE_MAX_AGE_HOURS}h)",
    )
    cache.add_argument(
        "--hash-algorithm",
        type=str,
        default="md5",
        choices=["md5", "sha256", "blake2b"],
        help="Hash algorithm for file integrity (default: md5)",
    )
    cache.add_argument("--no-rget-list", action="store_true", help="Disable RGET-LIST usage")
    cache.add_argument(
        "--rget-list-max-age",
        type=int,
        default=DEFAULT_RGET_LIST_MAX_AGE,
        metavar="DAYS",
        help=f"RGET-LIST max age (default: {DEFAULT_RGET_LIST_MAX_AGE} days)",
    )
    cache.add_argument(
        "--force-rget-list", action="store_true", help="Force RGET-LIST use even if old"
    )
    cache.add_argument("--no-etag", action="store_true", help="Disable ETag verification")

    async_grp = parser.add_argument_group("Async & Adaptive Options")
    async_grp.add_argument(
        "--async-metadata",
        action="store_true",
        default=True,
        help="Enable async metadata checks (default: enabled)",
    )
    async_grp.add_argument(
        "--no-async-metadata",
        action="store_false",
        dest="async_metadata",
        help="Disable async metadata checks (use for throttled servers)",
    )
    async_grp.add_argument(
        "--async-workers",
        type=int,
        default=DEFAULT_ASYNC_WORKERS,
        metavar="N",
        help=f"Async metadata workers (default: {DEFAULT_ASYNC_WORKERS})",
    )
    async_grp.add_argument(
        "--adaptive-async",
        action="store_true",
        default=ADAPTIVE_ASYNC_ENABLED,
        help="Enable adaptive async concurrency (default: enabled)",
    )
    async_grp.add_argument(
        "--no-adaptive-async",
        action="store_false",
        dest="adaptive_async",
        help="Disable adaptive async",
    )
    async_grp.add_argument(
        "--adaptive-start-concurrency",
        type=int,
        default=ADAPTIVE_START_CONCURRENCY,
        metavar="N",
        help=f"Starting async concurrency (default: {ADAPTIVE_START_CONCURRENCY})",
    )
    async_grp.add_argument(
        "--adaptive-error-threshold",
        type=float,
        default=ADAPTIVE_ERROR_THRESHOLD,
        metavar="RATE",
        help=f"Error rate threshold for fallback (default: {ADAPTIVE_ERROR_THRESHOLD})",
    )

    cleanup = parser.add_argument_group("Cleanup & Safety Options")
    cleanup.add_argument(
        "--cleanup",
        type=str,
        choices=["safe", "preview", "delete", "move"],
        default=argparse.SUPPRESS,
        help="Cleanup policy: safe, preview, delete, move",
    )
    cleanup.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Require confirmation before deletion (delete mode only)",
    )
    cleanup.add_argument(
        "--dry-run", action="store_true", help="Simulate without downloading/deleting"
    )
    cleanup.add_argument(
        "--quick", action="store_true", help="Quick mode (update cache timestamp only)"
    )

    security = parser.add_argument_group("Security Options")
    security.add_argument(
        "--security-validation",
        action="store_true",
        default=True,
        help="Enable SSRF/path protection (default: enabled)",
    )
    security.add_argument(
        "--no-security-validation",
        action="store_false",
        dest="security_validation",
        help="Disable security validation (NOT recommended)",
    )
    security.add_argument(
        "--circuit-breaker-enabled",
        action="store_true",
        default=True,
        help="Enable circuit breaker for failing services (default: enabled)",
    )
    security.add_argument(
        "--no-circuit-breaker",
        action="store_false",
        dest="circuit_breaker_enabled",
        help="Disable circuit breaker",
    )

    symlink = parser.add_argument_group("Symlink Handling Options")
    symlink.add_argument(
        "--handle-symlinks",
        action="store_true",
        default=False,
        help="Enable symlink detection and handling",
    )
    symlink.add_argument(
        "--symlink-mode",
        choices=["follow", "skip", "treat-as-file"],
        default="skip",
        help="How to handle symlinks (default: skip)",
    )
    symlink.add_argument(
        "--max-symlink-depth",
        type=int,
        default=MAX_SYMLINK_DEPTH,
        metavar="N",
        help=f"Maximum symlink depth (default: {MAX_SYMLINK_DEPTH})",
    )
    symlink.add_argument(
        "--max-symlinks-per-dir",
        type=int,
        default=MAX_SYMLINKS_PER_DIR,
        metavar="N",
        help=f"Maximum symlinks per directory (default: {MAX_SYMLINKS_PER_DIR})",
    )
    symlink.add_argument(
        "--symlink-bomb-threshold",
        type=int,
        default=SYMLINK_BOMB_THRESHOLD,
        metavar="N",
        help=f"Symlink bomb threshold (default: {SYMLINK_BOMB_THRESHOLD})",
    )
    symlink.add_argument(
        "--circuit-breaker-downloads",
        action="store_true",
        default=True,
        help="Enable circuit breaker for downloads (default: enabled)",
    )
    symlink.add_argument(
        "--no-circuit-breaker-downloads",
        action="store_false",
        dest="circuit_breaker_downloads",
        help="Disable circuit breaker for downloads",
    )

    logging_grp = parser.add_argument_group("Logging & Output Options")
    logging_grp.add_argument("--debug", action="store_true", help="Enable debug logging")
    logging_grp.add_argument("--print-logs", action="store_true", help="Print logs to console")
    logging_grp.add_argument("--log_file", metavar="NAME", help="Shared log base name")
    logging_grp.add_argument("--quiet", action="store_true", help="Quiet mode (WARNING+ only)")
    logging_grp.add_argument("--verbose", action="store_true", help="Verbose mode (DEBUG)")
    logging_grp.add_argument("--progress-bar", action="store_true", help="Enable tqdm progress bar")
    logging_grp.add_argument("--stats", action="store_true", help="Show detailed statistics")
    logging_grp.add_argument(
        "--metrics-json", type=Path, metavar="PATH", help="Export metrics to JSON file"
    )

    scan = parser.add_argument_group("Scan & Path Options")
    scan.add_argument(
        "--scan-mode",
        choices=["sequential", "parallel", "adaptive", "async"],
        default="adaptive",
        help="Directory scan mode (default: adaptive)",
    )
    scan.add_argument(
        "--parallel-threshold",
        type=int,
        default=PARALLEL_SCAN_THRESHOLD,
        metavar="N",
        help=f"Parallel scan threshold (default: {PARALLEL_SCAN_THRESHOLD})",
    )
    scan.add_argument(
        "--max-depth",
        type=int,
        default=MAX_DIRECTORY_DEPTH,
        metavar="N",
        help=f"Maximum directory depth (default: {MAX_DIRECTORY_DEPTH})",
    )
    scan.add_argument(
        "--max-filename-len",
        type=int,
        default=MAX_FILENAME_LENGTH,
        metavar="N",
        help=f"Maximum filename length (default: {MAX_FILENAME_LENGTH})",
    )
    scan.add_argument(
        "--download-queue-size",
        type=int,
        default=1000,
        metavar="N",
        help="Download queue size (default: 1000)",
    )

    advanced = parser.add_argument_group("Advanced Performance Options")
    advanced.add_argument(
        "--adaptive-batch-processing",
        action="store_true",
        default=True,
        help="Enable adaptive batch sizing (default: enabled)",
    )
    advanced.add_argument(
        "--no-adaptive-batch-processing",
        action="store_false",
        dest="adaptive_batch_processing",
        help="Disable adaptive batch sizing",
    )
    advanced.add_argument(
        "--initial-batch-size",
        type=int,
        default=BATCH_SIZE,
        metavar="N",
        help=f"Initial batch size (default: {BATCH_SIZE})",
    )
    advanced.add_argument(
        "--max-batch-size",
        type=int,
        default=MAX_BATCH_SIZE,
        metavar="N",
        help=f"Maximum batch size (default: {MAX_BATCH_SIZE})",
    )
    advanced.add_argument(
        "--target-batch-time",
        type=float,
        default=TARGET_BATCH_TIME_SECONDS,
        metavar="SECS",
        help=f"Target batch processing time (default: {TARGET_BATCH_TIME_SECONDS}s)",
    )
    advanced.add_argument(
        "--memory-cache-size",
        type=int,
        default=MEMORY_CACHE_MAX_SIZE,
        metavar="N",
        help=f"Memory cache size (default: {MEMORY_CACHE_MAX_SIZE})",
    )
    advanced.add_argument(
        "--use-disk-backed-sets",
        action="store_true",
        help="Use disk for large file sets (saves memory)",
    )
    advanced.add_argument(
        "--disk-cache-dir", type=Path, metavar="DIR", help="Directory for disk-backed cache"
    )
    advanced.add_argument(
        "--fast-parsing-fallback",
        action="store_true",
        default=True,
        help="Use fast parser for large HTML (default: enabled)",
    )
    advanced.add_argument(
        "--no-fast-parsing-fallback",
        action="store_false",
        dest="fast_parsing_fallback",
        help="Disable fast parsing fallback",
    )
    advanced.add_argument("--http2", action="store_true", default=True, help=argparse.SUPPRESS)
    advanced.add_argument("--no-http2", action="store_false", dest="http2", help="Disable HTTP/2")
    advanced.add_argument(
        "--http2-pipelining",
        action="store_true",
        default=True,
        help="Enable HTTP/2 pipelining (default: enabled)",
    )
    advanced.add_argument(
        "--no-http2-pipelining",
        action="store_false",
        dest="http2_pipelining",
        help="Disable HTTP/2 pipelining",
    )
    advanced.add_argument(
        "--connection-pool-prewarm",
        action="store_true",
        default=True,
        help="Pre-warm connection pools (default: enabled)",
    )
    advanced.add_argument(
        "--no-connection-pool-prewarm",
        action="store_false",
        dest="connection_pool_prewarm",
        help="Disable connection pool pre-warming",
    )
    advanced.add_argument(
        "--fs-cache-ttl",
        type=float,
        default=FS_CACHE_TTL_SECONDS,
        metavar="SECS",
        help=f"File system cache TTL (default: {FS_CACHE_TTL_SECONDS}s)",
    )
    advanced.add_argument(
        "--no-content-hash",
        action="store_false",
        dest="content_hash_small_files",
        default=True,
        help="Disable content hash verification for small files",
    )

    # NEW v3.0.0 parallel download arguments

    misc = parser.add_argument_group("Other Options")
    misc.add_argument("--benchmark", action="store_true", help="Run performance benchmark")
    misc.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    misc.add_argument(
        "--health-check-port",
        type=int,
        default=8080,
        metavar="PORT",
        help="Health check server port (default: 8080)",
    )

    args = parser.parse_args()

    # Handle config file
    if args.config:
        valid, error = validate_config_file(Path(args.config))
        if not valid:
            parser.error(f"Invalid configuration file: {error}")

        try:
            with open(args.config) as f:
                if Path(args.config).suffix.lower() in [".yaml", ".yml"]:
                    config_dict = yaml.safe_load(f)
                else:
                    config_dict = json.load(f)

                config_dict = expand_env_vars(config_dict)

            missing = []
            if "base_url" not in config_dict and not args.url:
                missing.append("base_url in config file or --url on command line")
            if "dest_path" not in config_dict and not args.dest_path:
                missing.append("dest_path in config file or --dest-path on command line")
            if "log_path" not in config_dict and not args.log_path:
                missing.append("log_path in config file or --log-path on command line")

            if missing:
                parser.error(f"Missing required configuration: {', '.join(missing)}")

            if not args.url and "base_url" in config_dict:
                args.url = config_dict["base_url"]
            if not args.dest_path and "dest_path" in config_dict:
                args.dest_path = Path(config_dict["dest_path"])
            if not args.log_path and "log_path" in config_dict:
                args.log_path = Path(config_dict["log_path"])
            if not args.dir_suffix and "dir_suffix" in config_dict:
                args.dir_suffix = [config_dict["dir_suffix"]]
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
        args.cleanup_policy = CleanupPolicy(getattr(args, "cleanup", "safe"))
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
            console_handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
                )
            )
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
            base_url=args.url.rstrip("/") if args.url else "",
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
            no_etag=getattr(args, "no_etag", False),
            use_shared_log=use_shared,
            scan_mode=ScanMode(args.scan_mode) if args.scan_mode else ScanMode.ADAPTIVE,
            parallel_threshold=args.parallel_threshold,
            benchmark=True,
            http2=args.http2,
            stats=args.stats,
            max_depth=args.max_depth,
            max_filename_len=args.max_filename_len,
            safe_urls=getattr(args, "safe_urls", True),
            confirm_delete=getattr(args, "confirm_delete", False),
            quiet=getattr(args, "quiet", False),
            verbose=getattr(args, "verbose", False),
            metrics_json=getattr(args, "metrics_json", None),
            progress_bar=getattr(args, "progress_bar", False),
            async_metadata=getattr(args, "async_metadata", True),
            async_workers=getattr(args, "async_workers", DEFAULT_ASYNC_WORKERS),
            content_hash_small_files=getattr(args, "content_hash_small_files", True),
            trusted_server=getattr(args, "trusted_server", False),
            request_delay=getattr(args, "request_delay", REQUEST_DELAY),
            cache_html=getattr(args, "cache_html", True),
            html_cache_max_age=getattr(args, "html_cache_max_age", HTML_CACHE_MAX_AGE_HOURS),
            adaptive_async=getattr(args, "adaptive_async", ADAPTIVE_ASYNC_ENABLED),
            adaptive_error_threshold=getattr(
                args, "adaptive_error_threshold", ADAPTIVE_ERROR_THRESHOLD
            ),
            adaptive_start_concurrency=getattr(
                args, "adaptive_start_concurrency", ADAPTIVE_START_CONCURRENCY
            ),
            security_validation=getattr(args, "security_validation", True),
            circuit_breaker_enabled=getattr(args, "circuit_breaker_enabled", True),
            bandwidth_limit=getattr(args, "bandwidth_limit", None),
            enable_resume=getattr(args, "enable_resume", True),
            max_concurrent_downloads=getattr(args, "max_concurrent_downloads", 10),
            download_queue_size=getattr(args, "download_queue_size", 1000),
            handle_symlinks=getattr(args, "handle_symlinks", False),
            symlink_mode=getattr(args, "symlink_mode", "skip"),
            circuit_breaker_downloads=getattr(args, "circuit_breaker_downloads", True),
            max_symlink_depth=getattr(args, "max_symlink_depth", MAX_SYMLINK_DEPTH),
            max_symlinks_per_dir=getattr(args, "max_symlinks_per_dir", MAX_SYMLINKS_PER_DIR),
            symlink_bomb_threshold=getattr(args, "symlink_bomb_threshold", SYMLINK_BOMB_THRESHOLD),
            adaptive_batch_processing=getattr(args, "adaptive_batch_processing", True),
            initial_batch_size=getattr(args, "initial_batch_size", BATCH_SIZE),
            max_batch_size=getattr(args, "max_batch_size", MAX_BATCH_SIZE),
            target_batch_time=getattr(args, "target_batch_time", TARGET_BATCH_TIME_SECONDS),
            memory_cache_size=getattr(args, "memory_cache_size", MEMORY_CACHE_MAX_SIZE),
            use_disk_backed_sets=getattr(args, "use_disk_backed_sets", False),
            disk_cache_dir=getattr(args, "disk_cache_dir", None),
            fast_parsing_fallback=getattr(args, "fast_parsing_fallback", True),
            http2_pipelining=getattr(args, "http2_pipelining", True),
            connection_pool_prewarm=getattr(args, "connection_pool_prewarm", True),
            fs_cache_ttl=getattr(args, "fs_cache_ttl", FS_CACHE_TTL_SECONDS),
            # NEW v3.0.0 arguments
            parallel_downloads=getattr(args, "parallel_downloads", PARALLEL_DOWNLOAD_ENABLED),
            max_chunks_per_file=getattr(args, "max_chunks", MAX_CHUNKS_PER_FILE),
            min_chunk_size_mb=getattr(args, "min_chunk_size", 10),
            max_parallel_chunks_total=getattr(
                args, "max_parallel_chunks", MAX_PARALLEL_CHUNKS_TOTAL
            ),
            chunk_assembly_dir=getattr(args, "chunk_assembly_dir", None),
            chunk_timeout_multiplier=getattr(
                args, "chunk_timeout_multiplier", CHUNK_TIMEOUT_MULTIPLIER
            ),
            # NEW v3.0.6 arguments
            auto_concurrency=getattr(args, "auto_concurrency", AUTO_CONCURRENCY_ENABLED),
            health_check_port=getattr(args, "health_check_port", 8080),
            # NEW: Auto-selection fields for benchmark
            auto_select_method=getattr(args, "auto_select", True),
            force_method=getattr(args, "force_method", None),
            force_disk_type=getattr(args, "force_disk_type", None),
            manual_network_speed_mbps=getattr(args, "network_speed", None),
            streaming_parallel=getattr(args, "streaming_parallel", True),
            streaming_min_file_size_mb=getattr(
                args, "streaming_min_size", STREAMING_MIN_FILE_SIZE_MB
            ),
            sequential_downloads=getattr(args, "sequential_downloads", False),
        )

        with MirrorURL(benchmark_config) as mirror:
            if hasattr(mirror, "connection_manager") and mirror.connection_manager:
                mirror.benchmark()
                logging.info("Benchmark completed")

                if hasattr(mirror.scanner, "get_parse_stats"):
                    stats = mirror.scanner.get_parse_stats()
                    logging.info(f"Parser stats: {stats}")

                if hasattr(mirror.connection_manager, "connection_pool") and hasattr(
                    mirror.connection_manager.connection_pool, "get_stats"
                ):
                    stats = mirror.connection_manager.connection_pool.get_stats()
                    logging.info(f"Connection pool stats: {stats}")

                if hasattr(mirror, "performance_monitor"):
                    perf_stats = mirror.performance_monitor.get_summary()
                    logging.info(f"Performance stats: {perf_stats}")

                # NEW v3.0.0: Log parallel download stats if available
                if hasattr(mirror, "parallel_manager") and mirror.parallel_manager:
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
                    "base_url": base_config.base_url,
                    "dest_path": base_config.dest_path,
                    "log_path": base_config.log_path,
                    "dir_suffix": suf,
                    "workers": base_config.workers,
                    "timeout": base_config.timeout,
                    "max_retries": base_config.max_retries,
                    "retry_delay": base_config.retry_delay,
                    "debug": base_config.debug,
                    "print_logs": base_config.print_logs,
                    "dry_run": base_config.dry_run,
                    "file_filters": base_config.file_filters,
                    "exclude_dirs": base_config.exclude_dirs,
                    "cleanup_policy": base_config.cleanup_policy,
                    "quick": base_config.quick,
                    "no_rget_list": base_config.no_rget_list,
                    "rget_list_max_age": base_config.rget_list_max_age,
                    "force_rget_list": base_config.force_rget_list,
                    "no_cache": base_config.no_cache,
                    "refresh_cache": base_config.refresh_cache,
                    "cache_max_age": base_config.cache_max_age,
                    "no_etag": getattr(base_config, "no_etag", False),
                    "hash_algorithm": getattr(base_config, "hash_algorithm", "md5"),
                    "use_shared_log": use_shared,
                    "scan_mode": base_config.scan_mode,
                    "parallel_threshold": base_config.parallel_threshold,
                    "benchmark": base_config.benchmark,
                    "http2": base_config.http2,
                    "stats": base_config.stats,
                    "max_depth": base_config.max_depth,
                    "max_filename_len": base_config.max_filename_len,
                    "safe_urls": getattr(base_config, "safe_urls", True),
                    "confirm_delete": getattr(base_config, "confirm_delete", False),
                    "quiet": getattr(base_config, "quiet", False),
                    "verbose": getattr(base_config, "verbose", False),
                    "metrics_json": getattr(base_config, "metrics_json", None),
                    "progress_bar": getattr(base_config, "progress_bar", False),
                    "async_metadata": getattr(base_config, "async_metadata", True),
                    "async_workers": getattr(base_config, "async_workers", DEFAULT_ASYNC_WORKERS),
                    "content_hash_small_files": getattr(
                        base_config, "content_hash_small_files", True
                    ),
                    "trusted_server": getattr(base_config, "trusted_server", False),
                    "request_delay": getattr(base_config, "request_delay", REQUEST_DELAY),
                    "cache_html": getattr(base_config, "cache_html", True),
                    "html_cache_max_age": getattr(
                        base_config, "html_cache_max_age", HTML_CACHE_MAX_AGE_HOURS
                    ),
                    "adaptive_async": getattr(
                        base_config, "adaptive_async", ADAPTIVE_ASYNC_ENABLED
                    ),
                    "adaptive_error_threshold": getattr(
                        base_config, "adaptive_error_threshold", ADAPTIVE_ERROR_THRESHOLD
                    ),
                    "adaptive_start_concurrency": getattr(
                        base_config, "adaptive_start_concurrency", ADAPTIVE_START_CONCURRENCY
                    ),
                    "security_validation": getattr(base_config, "security_validation", True),
                    "circuit_breaker_enabled": getattr(
                        base_config, "circuit_breaker_enabled", True
                    ),
                    "bandwidth_limit": getattr(base_config, "bandwidth_limit", None),
                    "enable_resume": getattr(base_config, "enable_resume", True),
                    "max_concurrent_downloads": getattr(
                        base_config, "max_concurrent_downloads", 10
                    ),
                    "download_queue_size": getattr(base_config, "download_queue_size", 1000),
                    "handle_symlinks": getattr(base_config, "handle_symlinks", False),
                    "symlink_mode": getattr(base_config, "symlink_mode", "skip"),
                    "circuit_breaker_downloads": getattr(
                        base_config, "circuit_breaker_downloads", True
                    ),
                    "max_symlink_depth": getattr(
                        base_config, "max_symlink_depth", MAX_SYMLINK_DEPTH
                    ),
                    "max_symlinks_per_dir": getattr(
                        base_config, "max_symlinks_per_dir", MAX_SYMLINKS_PER_DIR
                    ),
                    "symlink_bomb_threshold": getattr(
                        base_config, "symlink_bomb_threshold", SYMLINK_BOMB_THRESHOLD
                    ),
                    "adaptive_batch_processing": getattr(
                        base_config, "adaptive_batch_processing", True
                    ),
                    "initial_batch_size": getattr(base_config, "initial_batch_size", BATCH_SIZE),
                    "max_batch_size": getattr(base_config, "max_batch_size", MAX_BATCH_SIZE),
                    "target_batch_time": getattr(
                        base_config, "target_batch_time", TARGET_BATCH_TIME_SECONDS
                    ),
                    "memory_cache_size": getattr(
                        base_config, "memory_cache_size", MEMORY_CACHE_MAX_SIZE
                    ),
                    "use_disk_backed_sets": getattr(base_config, "use_disk_backed_sets", False),
                    "disk_cache_dir": getattr(base_config, "disk_cache_dir", None),
                    "fast_parsing_fallback": getattr(base_config, "fast_parsing_fallback", True),
                    "http2_pipelining": getattr(base_config, "http2_pipelining", True),
                    "connection_pool_prewarm": getattr(
                        base_config, "connection_pool_prewarm", True
                    ),
                    "fs_cache_ttl": getattr(base_config, "fs_cache_ttl", FS_CACHE_TTL_SECONDS),
                    # NEW v3.0.0 arguments from base_config
                    "parallel_downloads": getattr(base_config, "parallel_downloads", False),
                    "streaming_parallel": getattr(base_config, "streaming_parallel", False),
                    "sequential_downloads": getattr(base_config, "sequential_downloads", False),
                    "max_chunks_per_file": getattr(
                        base_config, "max_chunks_per_file", MAX_CHUNKS_PER_FILE
                    ),
                    "min_chunk_size_mb": getattr(base_config, "min_chunk_size_mb", 10),
                    "max_parallel_chunks_total": getattr(
                        base_config, "max_parallel_chunks_total", MAX_PARALLEL_CHUNKS_TOTAL
                    ),
                    "chunk_assembly_dir": getattr(base_config, "chunk_assembly_dir", None),
                    "chunk_timeout_multiplier": getattr(
                        base_config, "chunk_timeout_multiplier", CHUNK_TIMEOUT_MULTIPLIER
                    ),
                    "auto_concurrency": getattr(
                        base_config, "auto_concurrency", AUTO_CONCURRENCY_ENABLED
                    ),
                    # Auto-selection fields from base_config
                    "auto_select_method": getattr(base_config, "auto_select_method", True),
                    "force_method": getattr(base_config, "force_method", None),
                    "force_disk_type": getattr(base_config, "force_disk_type", None),
                    "manual_network_speed_mbps": getattr(
                        base_config, "manual_network_speed_mbps", None
                    ),
                    "streaming_min_file_size_mb": getattr(
                        base_config, "streaming_min_file_size_mb", STREAMING_MIN_FILE_SIZE_MB
                    ),
                    "parallel_files_min_files": getattr(base_config, "parallel_files_min_files", 3),
                    "streaming_min_files": getattr(base_config, "streaming_min_files", 4),
                    "traditional_min_files": getattr(base_config, "traditional_min_files", 3),
                }

                # Override with command line arguments if provided
                if args.url:
                    config_dict["base_url"] = args.url.rstrip("/")
                if args.dest_path:
                    config_dict["dest_path"] = Path(args.dest_path)
                if args.log_path:
                    config_dict["log_path"] = Path(args.log_path)
                if args.print_logs:
                    config_dict["print_logs"] = True
                if args.quiet:
                    config_dict["quiet"] = True
                if args.verbose:
                    config_dict["verbose"] = True
                if args.debug:
                    config_dict["debug"] = True
                if args.workers != DEFAULT_WORKERS:
                    config_dict["workers"] = args.workers
                if args.timeout != DEFAULT_TIMEOUT:
                    config_dict["timeout"] = args.timeout
                if args.adaptive_batch_processing is not None:
                    config_dict["adaptive_batch_processing"] = args.adaptive_batch_processing
                if args.initial_batch_size != BATCH_SIZE:
                    config_dict["initial_batch_size"] = args.initial_batch_size
                if args.max_batch_size != MAX_BATCH_SIZE:
                    config_dict["max_batch_size"] = args.max_batch_size
                if args.target_batch_time != TARGET_BATCH_TIME_SECONDS:
                    config_dict["target_batch_time"] = args.target_batch_time
                if args.memory_cache_size != MEMORY_CACHE_MAX_SIZE:
                    config_dict["memory_cache_size"] = args.memory_cache_size
                if args.use_disk_backed_sets:
                    config_dict["use_disk_backed_sets"] = args.use_disk_backed_sets
                if args.disk_cache_dir:
                    config_dict["disk_cache_dir"] = args.disk_cache_dir
                if not args.fast_parsing_fallback:
                    config_dict["fast_parsing_fallback"] = args.fast_parsing_fallback
                if not args.http2_pipelining:
                    config_dict["http2_pipelining"] = args.http2_pipelining
                if not args.connection_pool_prewarm:
                    config_dict["connection_pool_prewarm"] = args.connection_pool_prewarm
                if args.fs_cache_ttl != FS_CACHE_TTL_SECONDS:
                    config_dict["fs_cache_ttl"] = args.fs_cache_ttl

                # NEW v3.0.0 overrides

                if args.parallel_downloads:
                    config_dict["parallel_downloads"] = True
                    config_dict["streaming_parallel"] = False
                    config_dict["sequential_downloads"] = False
                elif args.streaming_parallel:
                    config_dict["parallel_downloads"] = False
                    config_dict["streaming_parallel"] = True
                    config_dict["sequential_downloads"] = False
                elif args.sequential_downloads:
                    config_dict["parallel_downloads"] = False
                    config_dict["streaming_parallel"] = False
                    config_dict["sequential_downloads"] = True

                if args.max_chunks != MAX_CHUNKS_PER_FILE:
                    config_dict["max_chunks_per_file"] = args.max_chunks
                if args.min_chunk_size != 10:
                    config_dict["min_chunk_size_mb"] = args.min_chunk_size
                if args.max_parallel_chunks != MAX_PARALLEL_CHUNKS_TOTAL:
                    config_dict["max_parallel_chunks_total"] = args.max_parallel_chunks
                if args.chunk_assembly_dir:
                    config_dict["chunk_assembly_dir"] = args.chunk_assembly_dir
                if args.chunk_timeout_multiplier != CHUNK_TIMEOUT_MULTIPLIER:
                    config_dict["chunk_timeout_multiplier"] = args.chunk_timeout_multiplier

                # NEW v3.0.2 overrides
                if args.max_concurrent_downloads != 10:
                    config_dict["max_concurrent_downloads"] = args.max_concurrent_downloads

                # NEW v3.0.6 overrides
                if args.auto_concurrency:
                    config_dict["auto_concurrency"] = args.auto_concurrency

                # NEW: Auto-selection overrides from command line
                if hasattr(args, "auto_select"):
                    config_dict["auto_select_method"] = args.auto_select
                if hasattr(args, "force_method") and args.force_method:
                    config_dict["force_method"] = args.force_method
                if hasattr(args, "force_disk_type") and args.force_disk_type:
                    config_dict["force_disk_type"] = args.force_disk_type
                if hasattr(args, "network_speed") and args.network_speed:
                    config_dict["manual_network_speed_mbps"] = args.network_speed
                if hasattr(args, "streaming_parallel"):
                    config_dict["streaming_parallel"] = args.streaming_parallel
                if hasattr(args, "streaming_min_size"):
                    config_dict["streaming_min_file_size_mb"] = args.streaming_min_size

                # ========================================================================
                # FIX: Add missing CLI overrides that were ignored when using --config
                # ========================================================================
                # Cleanup & Safety overrides
                if hasattr(args, "cleanup"):
                    try:
                        config_dict["cleanup_policy"] = CleanupPolicy(args.cleanup)
                    except ValueError:
                        pass  # Keep config file value if CLI value is invalid
                if args.confirm_delete:
                    config_dict["confirm_delete"] = True
                if args.dry_run:
                    config_dict["dry_run"] = True
                if args.quick:
                    config_dict["quick"] = True

                # Cache Control overrides
                if args.no_cache:
                    config_dict["no_cache"] = True
                if args.refresh_cache:
                    config_dict["refresh_cache"] = True
                if args.cache_max_age != DEFAULT_CACHE_MAX_AGE_DAYS:
                    config_dict["cache_max_age"] = args.cache_max_age
                if not args.cache_html:  # Handles --no-cache-html
                    config_dict["cache_html"] = False
                if args.html_cache_max_age != HTML_CACHE_MAX_AGE_HOURS:
                    config_dict["html_cache_max_age"] = args.html_cache_max_age

                # Filtering & Scanning overrides
                if args.filter:
                    config_dict["file_filters"] = [f.lower() for f in args.filter]
                if args.exclude_dir:
                    config_dict["exclude_dirs"] = args.exclude_dir
                if args.scan_mode != "adaptive":
                    try:
                        config_dict["scan_mode"] = ScanMode(args.scan_mode)
                    except ValueError:
                        pass

                # Performance & Async overrides
                if not args.async_metadata:  # Handles --no-async-metadata
                    config_dict["async_metadata"] = False
                if args.trusted_server:
                    config_dict["trusted_server"] = True
                if args.request_delay != REQUEST_DELAY:
                    config_dict["request_delay"] = args.request_delay
                if args.bandwidth_limit is not None:
                    config_dict["bandwidth_limit"] = args.bandwidth_limit

                # Symlinks & Security overrides
                if args.handle_symlinks:
                    config_dict["handle_symlinks"] = True
                if args.symlink_mode != "skip":
                    config_dict["symlink_mode"] = args.symlink_mode
                # ========================================================================

                suffix_config = MirrorConfig.from_dict(config_dict, silent=use_shared)
            else:
                suffix_config = MirrorConfig(
                    base_url=args.url.rstrip("/"),
                    dest_path=Path(args.dest_path),
                    log_path=Path(args.log_path),
                    dir_suffix=suf.strip("/") if suf else "",
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
                    safe_urls=getattr(args, "safe_urls", True),
                    confirm_delete=getattr(args, "confirm_delete", False),
                    metrics_json=getattr(args, "metrics_json", None),
                    progress_bar=getattr(args, "progress_bar", False),
                    async_metadata=getattr(args, "async_metadata", True),
                    async_workers=getattr(args, "async_workers", DEFAULT_ASYNC_WORKERS),
                    content_hash_small_files=getattr(args, "content_hash_small_files", True),
                    trusted_server=getattr(args, "trusted_server", False),
                    request_delay=getattr(args, "request_delay", REQUEST_DELAY),
                    cache_html=getattr(args, "cache_html", True),
                    html_cache_max_age=getattr(
                        args, "html_cache_max_age", HTML_CACHE_MAX_AGE_HOURS
                    ),
                    adaptive_async=getattr(args, "adaptive_async", ADAPTIVE_ASYNC_ENABLED),
                    adaptive_error_threshold=getattr(
                        args, "adaptive_error_threshold", ADAPTIVE_ERROR_THRESHOLD
                    ),
                    adaptive_start_concurrency=getattr(
                        args, "adaptive_start_concurrency", ADAPTIVE_START_CONCURRENCY
                    ),
                    security_validation=getattr(args, "security_validation", True),
                    circuit_breaker_enabled=getattr(args, "circuit_breaker_enabled", True),
                    bandwidth_limit=getattr(args, "bandwidth_limit", None),
                    enable_resume=getattr(args, "enable_resume", True),
                    max_concurrent_downloads=getattr(args, "max_concurrent_downloads", 10),
                    download_queue_size=getattr(args, "download_queue_size", 1000),
                    handle_symlinks=getattr(args, "handle_symlinks", False),
                    symlink_mode=getattr(args, "symlink_mode", "skip"),
                    circuit_breaker_downloads=getattr(args, "circuit_breaker_downloads", True),
                    max_symlink_depth=getattr(args, "max_symlink_depth", MAX_SYMLINK_DEPTH),
                    max_symlinks_per_dir=getattr(
                        args, "max_symlinks_per_dir", MAX_SYMLINKS_PER_DIR
                    ),
                    symlink_bomb_threshold=getattr(
                        args, "symlink_bomb_threshold", SYMLINK_BOMB_THRESHOLD
                    ),
                    adaptive_batch_processing=getattr(args, "adaptive_batch_processing", True),
                    initial_batch_size=getattr(args, "initial_batch_size", BATCH_SIZE),
                    max_batch_size=getattr(args, "max_batch_size", MAX_BATCH_SIZE),
                    target_batch_time=getattr(args, "target_batch_time", TARGET_BATCH_TIME_SECONDS),
                    memory_cache_size=getattr(args, "memory_cache_size", MEMORY_CACHE_MAX_SIZE),
                    use_disk_backed_sets=getattr(args, "use_disk_backed_sets", False),
                    disk_cache_dir=getattr(args, "disk_cache_dir", None),
                    fast_parsing_fallback=getattr(args, "fast_parsing_fallback", True),
                    http2_pipelining=getattr(args, "http2_pipelining", True),
                    connection_pool_prewarm=getattr(args, "connection_pool_prewarm", True),
                    fs_cache_ttl=getattr(args, "fs_cache_ttl", FS_CACHE_TTL_SECONDS),
                    # NEW v3.0.0 arguments
                    parallel_downloads=getattr(args, "parallel_downloads", False),
                    sequential_downloads=getattr(args, "sequential_downloads", False),
                    streaming_parallel=getattr(args, "streaming_parallel", False),
                    max_chunks_per_file=getattr(args, "max_chunks", MAX_CHUNKS_PER_FILE),
                    min_chunk_size_mb=getattr(args, "min_chunk_size", 10),
                    max_parallel_chunks_total=getattr(
                        args, "max_parallel_chunks", MAX_PARALLEL_CHUNKS_TOTAL
                    ),
                    chunk_assembly_dir=getattr(args, "chunk_assembly_dir", None),
                    chunk_timeout_multiplier=getattr(
                        args, "chunk_timeout_multiplier", CHUNK_TIMEOUT_MULTIPLIER
                    ),
                    auto_concurrency=getattr(args, "auto_concurrency", AUTO_CONCURRENCY_ENABLED),
                    health_check_port=getattr(args, "health_check_port", 8080),
                    # NEW: Auto-selection fields
                    auto_select_method=getattr(args, "auto_select", True),
                    force_method=getattr(args, "force_method", None),
                    force_disk_type=getattr(args, "force_disk_type", None),
                    manual_network_speed_mbps=getattr(args, "network_speed", None),
                    streaming_min_file_size_mb=getattr(
                        args, "streaming_min_size", STREAMING_MIN_FILE_SIZE_MB
                    ),
                )

        except ConfigError as e:
            if args.print_logs and args.log_file:
                logging.critical(f"Configuration error for {suf or 'ROOT'}: {e}")
            else:
                print(f"Configuration error for {suf or 'ROOT'}: {e}")
            failed.append(suf or "ROOT")
            continue
        except Exception as e:
            if args.print_logs and args.log_file:
                logging.critical(f"Error creating config for {suf or 'ROOT'}: {e}")
            else:
                print(f"Error creating config for {suf or 'ROOT'}: {e}")
            failed.append(suf or "ROOT")
            continue

        try:
            with MirrorURL(suffix_config, suffix_index=i, total_suffixes=total) as mirror:
                if not hasattr(mirror, "connection_manager") or not mirror.connection_manager:
                    logging.warning(f"[{i}/{total}] No connection manager for {suf or 'ROOT'}")
                    skipped.append(suf or "ROOT")
                elif not mirror.connection_ok:
                    logging.warning(f"[{i}/{total}] Connection failed for {suf or 'ROOT'} (404?)")
                    failed.append(suf or "ROOT")
                else:
                    sync_success = mirror.sync()
                    if sync_success:
                        logging.info(f"[{i}/{total}] ✅ Successfully processed: {suf or 'ROOT'}")
                        processed.append(suf or "ROOT")
                    else:
                        logging.error(f"[{i}/{total}] ❌ Failed to process: {suf or 'ROOT'}")
                        failed.append(suf or "ROOT")

            # Flush handlers to ensure logs are written
            for handler in logging.root.handlers:
                try:
                    handler.flush()
                except Exception:
                    pass

        except PathTraversalError as e:
            logging.critical(f"Path traversal for {suf or 'ROOT'}: {e}")
            failed.append(suf or "ROOT")
        except URLScopeError as e:
            logging.critical(f"URL scope error for {suf or 'ROOT'}: {e}")
            failed.append(suf or "ROOT")
        except Exception as e:
            logging.critical(f"Error with {suf or 'ROOT'}: {e}", exc_info=True)
            failed.append(suf or "ROOT")

    # Final summary
    if use_shared or total > 1:
        logging.info("\n" + "=" * 50)
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

        logging.info("=" * 50)

    # Cleanup log handlers
    for handler in _log_files:
        try:
            handler.close()
        except Exception:
            pass

    sys.exit(0 if not failed else 1)
