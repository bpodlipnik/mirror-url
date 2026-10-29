"""Argument parsing, logging setup, and the ``main()`` entry point.

Originally migrated verbatim from the legacy ``mirror_url.py`` monolith
(``setup_shared_logging`` orig. 13993-14126, ``main`` orig. 14127-15142); the
package has since diverged from that source. The ``if __name__ == "__main__"``
guard lives in ``__main__.py`` instead.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import re
import sys
import tempfile
import time
from collections import Counter
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlsplit, urlunsplit

import yaml

from ._version import __version__
from .config import MirrorConfig, _expand_check_files, expand_env_vars
from .constants import (
    ADAPTIVE_ASYNC_ENABLED,
    ADAPTIVE_ERROR_THRESHOLD,
    ADAPTIVE_MAX_CONCURRENCY,
    ADAPTIVE_START_CONCURRENCY,
    AUTO_CONCURRENCY_ENABLED,
    BATCH_SIZE,
    CHUNK_TIMEOUT_MULTIPLIER,
    DEFAULT_ASYNC_WORKERS,
    DEFAULT_CACHE_MAX_AGE_DAYS,
    DEFAULT_MAX_RETRIES,
    DEFAULT_RATE_LIMIT,
    DEFAULT_RETRY_DELAY,
    DEFAULT_RGET_LIST_MAX_AGE,
    DEFAULT_TIMEOUT,
    DEFAULT_WORKERS,
    FS_CACHE_TTL_SECONDS,
    HTML_CACHE_MAX_AGE_HOURS,
    LIST_DIRS_DEFAULT_MAX_DEPTH,
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
    SYMLINK_BOMB_THRESHOLD,
    TARGET_BATCH_TIME_SECONDS,
)
from .core import MirrorURL
from .destination_lock import MirrorFileHandler, create_run_log, run_ownership
from .directory_selection import compile_directory_patterns, resolve_directory_selection
from .enums import CleanupPolicy, ScanMode
from .exceptions import (
    ConfigError,
    DestinationLockError,
    MirrorError,
    PathTraversalError,
    URLScopeError,
)
from .filename_mapping import selected_directory_targets, validate_move_targets
from .metrics import (
    MetricsCollector,
    log_connection_pools_closed,
    log_download_report,
    log_processed_result,
    log_run_completion,
    log_run_start,
)
from .progress import ProgressConsoleHandler
from .security import PathSafety
from .utils import SafeLogFormatter, _log_files


def setup_shared_logging(
    args: argparse.Namespace, effective: Optional[argparse.Namespace] = None
) -> None:
    """Setup shared logging for multiple suffixes.

    ``args`` drives the handlers and log levels. ``effective`` (see
    :func:`_effective_args`) is only used for the informational header written
    at the top of the log; when omitted the header describes ``args`` itself.
    """
    started_epoch = time.time()
    started_at = datetime.fromtimestamp(started_epoch)
    timestamp = time.strftime("%Y%m%d_%H%M%S", time.localtime(started_epoch))
    if args.log_file:
        raw = f"{args.log_file}_{'_'.join(args.dir_suffix) if args.dir_suffix else 'all'}"
    elif len(args.dir_suffix) > 1:
        first, last = (
            suffix.strip("/").replace("/", "-")
            for suffix in (args.dir_suffix[0], args.dir_suffix[-1])
        )
        raw = f"mirror_url_{first}--{last}"
    else:
        suffix = args.dir_suffix[0].strip("/").replace("/", "_") if args.dir_suffix else "all"
        raw = f"mirror_url_{suffix}"
    safe = re.sub(r"[^\w.-]", "_", raw)
    # Bound bytes, not characters, including a digest when shortening.
    budget = MAX_FILENAME_LENGTH - len(f"_{timestamp}.log".encode())
    if len(safe.encode("utf-8")) > budget:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        preview = re.sub(r"[^\w.-]", "_", "_".join(args.dir_suffix[:3]))
        summary = f"_plus{max(0, len(args.dir_suffix) - 3)}more_{digest}"
        prefix = re.sub(r"[^\w.-]", "_", args.log_file or "mirror_url") + "_" + preview
        safe = (
            prefix.encode("utf-8")[: budget - len(summary)].decode("utf-8", errors="ignore")
            + summary
        )
    log_filename = f"{safe}_{timestamp}.log"
    log_path = Path(args.log_path) / log_filename
    # Create file handler (always)
    log_path, file_handler = create_run_log(log_path, MirrorFileHandler)
    file_handler.setLevel(logging.DEBUG if args.debug or args.verbose else logging.INFO)
    file_handler.setFormatter(
        SafeLogFormatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )

    # Create console handler (if print-logs is enabled)
    handlers: List[logging.Handler] = [file_handler]
    if args.print_logs:
        console_handler = ProgressConsoleHandler(sys.stderr)
        console_handler.setFormatter(
            SafeLogFormatter(
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

    # Replace handlers under logging's configuration lock. An empty root
    # between removal and file-handler creation lets a concurrent module-level
    # log call install basicConfig's default console handler.
    logging.basicConfig(level=log_level, handlers=handlers, force=True)

    # Log header information. Describe the settings the run will actually use
    # (CLI > --config file > default), not the raw parser defaults.
    eff = effective if effective is not None else args
    suffixes = args.dir_suffix or [""]
    destination = getattr(eff, "dest_path", None)
    if destination is not None:
        destination = Path(destination)
        if getattr(eff, "mode", "mirror") != "download" and len(suffixes) == 1:
            for part in filter(None, suffixes[0].split("/")):
                destination /= PathSafety._safe_filename(part, max_len=eff.max_filename_len)
        destination = destination.resolve()
    log_run_start(
        mode=getattr(eff, "mode", "mirror"),
        remote_root=str(getattr(eff, "url", "")),
        destination=destination,
        log_path=log_path,
        backend=getattr(eff, "backend", "httpx"),
        verify_content=getattr(eff, "verify_content", False),
        suffixes=suffixes,
        started_at=started_at,
    )

    # Mirror's lifecycle logs the common configuration once per run.
    # Repeating it here produced two configuration summaries in shared logs.
    if getattr(eff, "mode", "mirror") != "download":
        return

    cleanup_policy = getattr(eff, "cleanup_policy", CleanupPolicy.SAFE_NO_DELETE)
    if cleanup_policy == CleanupPolicy.DELETE:
        logging.warning("⚠️ DELETE MODE ENABLED")
    elif cleanup_policy == CleanupPolicy.MOVE:
        logging.info("📦 MOVE MODE ENABLED")
    elif cleanup_policy == CleanupPolicy.PREVIEW:
        logging.info("🔍 PREVIEW MODE")
    else:
        logging.info("✅ SAFE MODE")

    if eff.no_cache:
        logging.warning("CACHE DISABLED")
    if eff.refresh_cache:
        logging.warning("CACHE REFRESH FORCED")
    if getattr(eff, "safe_urls", True):
        logging.info("🔒 URL sanitization enabled")

    logging.info(
        f"🛡️ Path safety: max_depth={eff.max_depth}, max_filename_len={eff.max_filename_len}"
    )

    if eff.confirm_delete and eff.cleanup_policy == CleanupPolicy.DELETE:
        logging.info("🔐 Confirmation required")
    if eff.quiet:
        logging.info("🔇 Quiet mode")
    elif eff.verbose:
        logging.info("🔊 Verbose mode")
    if eff.metrics_json:
        logging.info(f"📊 Metrics: {eff.metrics_json}")
    if getattr(eff, "mode", None) == "download":
        chunked = eff.parallel_downloads or eff.streaming_parallel
        transfer = (
            "streaming parallel chunks"
            if eff.streaming_parallel
            else "parallel chunks with temporary files"
            if eff.parallel_downloads
            else "whole-file streaming"
        )
        logging.debug("Requested transfer method: %s", transfer)
        concurrency = 1 if eff.sequential_downloads else eff.max_concurrent_downloads
        logging.debug("Configured file-worker capacity: %s", concurrency)
        logging.debug(
            "Max connection retries: %s (restart %s)",
            eff.max_retries,
            "chunk" if chunked else "whole file",
        )
        if chunked:
            logging.debug(
                "Configured chunk limits: max %s per file, max %s worker slots, minimum size %s MiB",
                eff.max_chunks,
                eff.max_parallel_chunks,
                eff.min_chunk_size,
            )
        rate = eff.requests_per_second
        logging.debug(
            "Request spacing: %.1fms", max(1 / rate if rate else 0, eff.request_delay) * 1000
        )
        if eff.bandwidth_limit:
            logging.info("Bandwidth limit: %s MiB/s", eff.bandwidth_limit)
        return


# argparse ``dest`` -> ``MirrorConfig`` field, for the options whose dest differs
# from the field name. Every other option maps by identical name (see
# ``_cli_overrides``), so a new flag whose dest matches its field needs no entry
# here -- it is picked up automatically.
_CLI_DEST_TO_CONFIG_KEY = {
    "url": "base_url",
    "filter": "file_filters",
    "exclude_dir": "exclude_dirs",
    "cleanup": "cleanup_policy",
    "max_chunks": "max_chunks_per_file",
    "min_chunk_size": "min_chunk_size_mb",
    "max_parallel_chunks": "max_parallel_chunks_total",
}

# Options that are consumed by main() itself rather than copied into MirrorConfig.
_CLI_NON_CONFIG_DESTS = frozenset(
    {"config", "dir_suffix", "dir_regex", "log_file", "version", "help"}
)

_DOWNLOAD_MODE_DESTS = ("parallel_downloads", "streaming_parallel", "sequential_downloads")


def _explicit_cli_dests(parser: argparse.ArgumentParser, argv: list) -> set:
    """Return the ``dest`` names the user actually typed on the command line.

    ``args.foo == default`` cannot tell "not given" from "given, and equal to the
    default", and ``hasattr(args, ...)`` is always true for options with a
    default. Re-parsing with every default suppressed leaves only the options
    that were present in ``argv``.
    """
    probe = copy.deepcopy(parser)
    for action in probe._actions:
        action.default = argparse.SUPPRESS
    return set(vars(probe.parse_args(argv)))


# Options that steer how *this process* logs. They are applied from the command
# line only (main() never reads them from the ``--config`` file for the shared
# log's handlers/levels), so the log header must keep describing the CLI value
# for them rather than a config-file value that is not actually in force.
_CLI_ONLY_LOGGING_DESTS = frozenset({"quiet", "verbose", "debug", "print_logs", "log_file"})


def _effective_args(
    args: argparse.Namespace, explicit: set, base_config: MirrorConfig
) -> argparse.Namespace:
    """Return a copy of ``args`` holding the settings a ``--config`` run will use.

    Precedence is command line > config file > built-in default. ``args`` alone
    only knows the first and the last: a value the user did not type is the
    argparse *default*, even when the YAML overrides it. Used solely to make the
    ``--log-file`` header truthful (e.g. ``cleanup_policy: delete`` in the YAML
    must not be reported as "SAFE MODE").
    """
    eff = argparse.Namespace(**vars(args))
    fields = MirrorConfig.model_fields
    for dest in vars(args):
        if dest in explicit or dest in _CLI_NON_CONFIG_DESTS or dest in _CLI_ONLY_LOGGING_DESTS:
            continue
        if dest == "cleanup_policy":
            continue  # not a parser dest; main() derives it from --cleanup (handled below)
        key = _CLI_DEST_TO_CONFIG_KEY.get(dest, dest)
        if key in fields and dest not in _DOWNLOAD_MODE_DESTS:
            setattr(eff, dest, getattr(base_config, key))
    for dest in _DOWNLOAD_MODE_DESTS:
        if dest not in explicit:
            setattr(eff, dest, getattr(base_config, dest))
    # Not CLI options at all (getattr fallbacks in the header): config-file only.
    eff.safe_urls = base_config.safe_urls
    eff.enable_resume = base_config.enable_resume
    if "cleanup" not in explicit:
        eff.cleanup_policy = base_config.cleanup_policy
    return eff


def _cli_overrides(args: argparse.Namespace, explicit: set) -> dict:
    """Translate explicitly-passed CLI options into ``MirrorConfig`` fields.

    Only options present on the command line are returned, so values from a
    ``--config`` file are never clobbered by a parser default.
    """
    fields = MirrorConfig.model_fields
    out: dict = {}
    for dest in sorted(explicit - _CLI_NON_CONFIG_DESTS):
        value = getattr(args, dest)
        if dest in ("list_dirs", "list_files"):
            out.update({mode: mode == dest for mode in ("list_dirs", "list_files")})
            out[dest] = True
            out[dest + "_n"] = value or 0
        elif dest in ("download_url", "download_files", "url_list"):
            out[dest] = value
            for source in ("download_url", "download_files", "url_list"):
                if source != dest:
                    out[source] = [] if source == "download_files" else None
        elif dest in _DOWNLOAD_MODE_DESTS:
            # The three modes are mutually exclusive: choosing one on the
            # command line must also switch off the others set in the file.
            out.update({mode: mode == dest for mode in _DOWNLOAD_MODE_DESTS})
        else:
            key = _CLI_DEST_TO_CONFIG_KEY.get(dest, dest)
            if key not in fields:
                continue
            if isinstance(value, list) and not value:
                continue  # bare ``--filter`` / ``--exclude-dir`` with no values
            if dest == "url":
                value = value.rstrip("/")
            elif dest == "filter":
                value = list(value)
            elif dest == "cleanup":
                value = CleanupPolicy(value)
            elif dest == "scan_mode":
                value = ScanMode(value)
            elif dest in ("dest_path", "log_path"):
                value = Path(value)
            out[key] = value
    return out


def _direct_download_defaults(
    args: argparse.Namespace, values: dict, selected_mode: str, parser: argparse.ArgumentParser
) -> None:
    """Supply download defaults while keeping --url as the root for every input."""
    direct_url = values.get("download_url")
    files = values.get("download_files", [])
    if direct_url is None and not files:
        return
    if selected_mode != "download":
        parser.error("a direct URL requires --mode download")
    if values.get("url_list") is not None or (direct_url is not None and files):
        parser.error("download files and --url-list are mutually exclusive")
    try:
        if direct_url is not None and (not isinstance(direct_url, str) or not direct_url.strip()):
            raise ValueError("the direct URL must be a nonempty string")
        if direct_url is not None:
            direct_url = direct_url.strip()
            files = [direct_url]
        elif not isinstance(files, list) or any(
            not isinstance(value, str) or not value.strip() for value in files
        ):
            raise ValueError("download files require a list of nonempty strings")
        else:
            files = [value.strip() for value in files]
        parents = set()
        for value in files:
            parsed = urlsplit(value)
            if parsed.scheme:
                if parsed.scheme not in ("http", "https") or not parsed.hostname:
                    raise ValueError("the direct URL must be an absolute HTTP(S) file URL")
                if not parsed.path or parsed.path.endswith("/"):
                    raise ValueError("the direct URL must name a file")
                parent = parsed.path.rsplit("/", 1)[0] + "/"
                parents.add(urlunsplit((parsed.scheme, parsed.netloc, parent, "", "")))
            elif direct_url is not None:
                raise ValueError("the direct URL must be an absolute HTTP(S) file URL")
            elif not args.url:
                raise ValueError("relative download files require --url")
        if not args.url:
            if len(parents) != 1:
                raise ValueError("download URLs in different directories require --url")
            args.url = parents.pop()
        if not args.dest_path:
            args.dest_path = Path.cwd()
        if not args.log_path:
            destination_key = hashlib.sha256(str(args.dest_path.resolve()).encode()).hexdigest()[
                :16
            ]
            args.log_path = Path(tempfile.gettempdir()) / f"mirror-url-download-{destination_key}"
        if direct_url is not None:
            args.download_url = direct_url
        else:
            args.download_files = files
    except ValueError as error:
        parser.error(str(error))


def main() -> None:
    """Own the complete CLI run, including logging before mirror construction."""
    previous = set(_log_files)
    with ExitStack() as ownership:
        try:
            _main(ownership)
        except DestinationLockError as error:
            print(f"Destination ownership error: {error}", file=sys.stderr)
            sys.exit(1)
        finally:
            _close_run_handlers([handler for handler in _log_files if handler not in previous])


def _finish_mirror_job(mirror, success, error, suffix, index, total) -> dict:
    """Render after resource shutdown while CLI still owns reporting outputs."""
    report = mirror.metrics.run_report
    report.finish_job(error)
    if mirror.config.metrics_json and not mirror.config.dry_run:
        mirror.metrics.export_json(mirror.config.metrics_json, mirror.config)
    snapshot = mirror.metrics.get_summary()
    if success is not None:
        prefix = mirror._get_prefix()
        success = success and error is None
        if report.status not in ("quick", "survey"):
            mirror.metrics.log_report(
                prefix, include_metrics=mirror.config.metrics, snapshot=snapshot
            )
            log_run_completion(success, prefix=prefix, reasons=report.reasons)
        log_processed_result(success, suffix, index, total)
    log_connection_pools_closed(report.closed_connection_pools)
    return snapshot


def _close_run_handlers(handlers) -> None:
    for handler in handlers:
        logging.root.removeHandler(handler)
        if handler in _log_files:
            _log_files.remove(handler)
        handler.flush()
        handler.close()


def _main(ownership: ExitStack) -> None:
    """Main entry point with true parallel file downloads"""
    parser = argparse.ArgumentParser(
        description=f"MirrorURL v{__version__} - HTTP(S) directory-listing mirroring",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=r"""
ARGUMENT SOURCES:
  Supply --config FILE, or --url URL --dest-path DIR --log-path DIR.
  --mode download FILE [FILE ...] accepts paths relative to --url or absolute URLs.
  Absolute URLs sharing one parent infer that root, download into the current
  directory and logs to a destination-specific folder in the system temp dir.
  Explicit --url, --dest-path and --log-path override these shortcut defaults.
  CLI-only --list-dirs/--list-files need only --url. Other runs require URL,
  destination and log fields from the config, explicit flags or direct-URL defaults.
  Explicit CLI flags override file values; omitted flags preserve them.

DOWNLOAD AND INTEGRITY:
  Omit mode flags for auto-selection, or choose one download mode.
  --mode download uses whole-file GETs by default; both parallel flags enable
  eligible chunks with range-metadata HEAD probes and no discovery/freshness.
  Chunking needs a known size >= --min-chunk-size, Range support and a strong
  ETag. Exact ranges/lengths/ETags are checked before atomic publication.
  Whole-file partials can resume from an owned .mirror-url-state/ directory;
  chunks do not resume across runs. No remote cryptographic digest is checked.

FILTERS:
  Patterns match filenames only, case-insensitively; multiple patterns are OR'd.
  --filter .fits .txt                 # extensions
  --filter _fe_                      # substring
  --filter '2024.*\.fits$'            # regex
  Use --dir/--dir-regex/--exclude-dir to select directory paths.

EXAMPLES (replace the example URL with your archive's directory listing):
  # Download one known file into the current directory
  %(prog)s --mode download https://example.com/data/file.fits

  # Discover immediate child directories
  %(prog)s --url https://example.com/data/ --list-dirs

  # Mirror FITS files using automatic download selection
  %(prog)s --url https://example.com/data/ --dest-path ./data \
    --log-path ./logs --filter .fits

  # Verified streaming chunks for eligible files
  %(prog)s --url https://example.com/data/ --dest-path ./data \
    --log-path ./logs --streaming-parallel --max-chunks 8

  # Observe obsolete-file actions without changing mirrored files
  %(prog)s --config mirror.yaml --cleanup preview --dry-run

  # Conservative downloads for a throttled server
  %(prog)s --config mirror.yaml --sequential-downloads \
    --no-async-metadata --workers 2 --request-delay 0.2

Full reference: docs/USER_GUIDE.md (and docs/USER_GUIDE.html).
""",
    )

    basic = parser.add_argument_group("Target Options")
    basic.add_argument(
        "download_files",
        nargs="*",
        metavar="FILE",
        help="Files relative to --url or absolute URLs; requires --mode download; mutually exclusive with --url-list",
    )
    basic.add_argument(
        "--url",
        help="Remote root for discovery and download paths; absolute download URLs sharing one parent can infer it",
    )
    basic.add_argument(
        "--dest-path",
        type=Path,
        help="Destination directory (positional download files default to the current directory)",
    )
    basic.add_argument(
        "--log-path",
        type=Path,
        help="Logs/cache directory (positional download files default to system temp; keep outside destination)",
    )
    basic.add_argument(
        "--config",
        help="YAML/JSON config; explicit CLI flags override file values, even when equal to defaults",
    )

    basic.add_argument(
        "--mode",
        choices=["mirror", "download"],
        default="mirror",
        help="Mirror a listing, or download supplied files without discovery/freshness checks",
    )
    basic.add_argument(
        "--backend",
        choices=["httpx", "aiohttp"],
        default="httpx",
        help="Transfer backend (HTTPX default; both support whole-file and chunk transfers; aiohttp uses HTTP/1.1)",
    )
    basic.add_argument(
        "--url-list",
        type=Path,
        help="UTF-8 list of file paths relative to --url or absolute URLs, one per line; requires --mode download",
    )
    basic.add_argument(
        "--overwrite",
        action="store_true",
        help="Permit replacing existing regular files in download mode; links remain forbidden",
    )

    # Create mutually exclusive group for download modes
    download_mode_group = parser.add_argument_group("Download Modes (omit for auto-selection)")
    mode_group = download_mode_group.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--parallel-downloads",
        action="store_true",
        help="Mirror/download with either backend: parallel files with verified temporary chunks. HTTPX mirror whole-file fallbacks can resume; chunks do not resume across runs",
    )
    mode_group.add_argument(
        "--streaming-parallel",
        action="store_true",
        help="Mirror/download with either backend: parallel files; eligible chunks write to a staging file and publish atomically after verification",
    )
    mode_group.add_argument(
        "--sequential-downloads",
        action="store_true",
        help="Download one file at a time; metadata and size probes may still run concurrently",
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
        metavar="MiB",
        help="Minimum chunk size in MiB (default: 10 MiB)",
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
        "--concurrency",
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
        help="Parent for owned chunk workspaces (default: reserved destination state); staging uses the destination filesystem",
    )
    parallel_grp.add_argument(
        "--chunk-timeout-multiplier",
        type=float,
        default=CHUNK_TIMEOUT_MULTIPLIER,
        metavar="MULT",
        help=(
            f"Timeout multiplier for chunks (default: {CHUNK_TIMEOUT_MULTIPLIER}). "
            "Currently has no effect; accepted for backward compatibility."
        ),
    )

    filter_grp = parser.add_argument_group("Filter Options")
    filter_grp.add_argument(
        "--filter",
        nargs="*",
        default=[],
        metavar="PATTERN",
        help="Case-insensitive filename patterns, OR'd: extensions (.fits), substrings (_fe_), or regexes ('2024.*\\.fits$'). Does not match directory paths",
    )

    directory = parser.add_argument_group("Directory Options")
    selectors = directory.add_mutually_exclusive_group()
    selectors.add_argument(
        "--dir",
        dest="dir_suffix",
        nargs="+",
        default=[],
        metavar="PATH",
        help="Literal remote directories relative to --url (e.g., L1/v1 L2/v2)",
    )
    selectors.add_argument(
        "--dir-suffix",
        dest="dir_suffix",
        nargs="*",
        default=argparse.SUPPRESS,
        help=argparse.SUPPRESS,
    )
    selectors.add_argument(
        "--dir-regex",
        nargs="+",
        default=[],
        metavar="PATTERN",
        help=(
            "Discover directories matching complete case-sensitive paths relative to --url; "
            "quote patterns. Mirror mode only; respects --exclude-dir and root-relative --max-depth"
        ),
    )
    directory.add_argument(
        "--exclude-dir",
        nargs="*",
        default=[],
        metavar="DIR",
        help=(
            "Exclude exact paths relative to --url, including with --dir. Use quoted '*' globs for nested matches: lasco matches the root child; '*/lasco' matches nested children. Use both to cover both"
        ),
    )
    directory.add_argument(
        "--list-dirs",
        nargs="?",
        type=int,
        const=0,
        default=None,
        metavar="N",
        help=(
            "List directories and exit without file checks/downloads/cleanup. Respects exclusions/depth; ignores filters. Optional N selects the lexicographically last N paths across this target, excluding the root. CLI-only runs need no destination/log paths"
        ),
    )
    directory.add_argument(
        "--list-files",
        nargs="?",
        type=int,
        const=0,
        default=None,
        metavar="N",
        help=(
            "List matching files and exit without freshness checks/downloads/cleanup. Respects exclusions/depth/filters. Optional N selects the lexicographically last N filenames per directory, not timestamps. CLI-only runs need no destination/log paths"
        ),
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
        help=f"Base request timeout (default: {DEFAULT_TIMEOUT}s; range: 3-300); some paths use fixed limits/multiples, not a whole-run deadline",
    )
    performance.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        metavar="N",
        help=f"Connection-request retry budget (default: {DEFAULT_MAX_RETRIES}); chunk retries also have their own fixed budget",
    )
    performance.add_argument(
        "--retry-delay",
        type=int,
        default=DEFAULT_RETRY_DELAY,
        metavar="SECS",
        help=f"Base retry-backoff delay (default: {DEFAULT_RETRY_DELAY}s)",
    )
    performance.add_argument(
        "--trusted-server",
        action="store_true",
        help="Relax chunk concurrency/scaling limits; request delay stays controlled by --request-delay",
    )
    performance.add_argument(
        "--request-delay",
        type=float,
        default=REQUEST_DELAY,
        metavar="SECS",
        help=f"Minimum request spacing (default: {REQUEST_DELAY}s; range: 0-1.0). Use 0 with --requests-per-second 0 for no pacing",
    )
    performance.add_argument(
        "--requests-per-second",
        type=float,
        default=DEFAULT_RATE_LIMIT,
        metavar="N",
        help="Request rate ceiling (default: 20; 0 removes this ceiling, subject to --request-delay)",
    )
    performance.add_argument(
        "--bandwidth-limit", type=float, metavar="MiB/S", help="Limit download bandwidth (MiB/s)"
    )

    cache = parser.add_argument_group("Cache Options")
    cache.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass saved metadata and process-local parsed-listing caches; existing files are still checked for freshness",
    )
    cache.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ignore saved metadata and cached listings for this run",
    )
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
        help="Cache parsed listings in memory within this process (default: enabled); not restored on a new launch",
    )
    cache.add_argument(
        "--no-cache-html",
        action="store_false",
        dest="cache_html",
        help="Disable process-local parsed-listing caches",
    )
    cache.add_argument(
        "--html-cache-max-age",
        type=int,
        default=HTML_CACHE_MAX_AGE_HOURS,
        metavar="HOURS",
        help=f"Process-local parsed-listing cache age (default: {HTML_CACHE_MAX_AGE_HOURS}h)",
    )
    cache.add_argument(
        "--hash-algorithm",
        type=str,
        default="md5",
        choices=["md5", "sha256", "blake2b"],
        help="Hash for directory/cache signatures (default: md5); no comparison with a remote cryptographic file digest",
    )
    cache.add_argument(
        "--no-rget-list",
        action="store_true",
        help="Disable RGET-LIST usage. Currently has no effect; accepted for backward compatibility.",
    )
    cache.add_argument(
        "--rget-list-max-age",
        type=int,
        default=DEFAULT_RGET_LIST_MAX_AGE,
        metavar="DAYS",
        help=(
            f"RGET-LIST max age (default: {DEFAULT_RGET_LIST_MAX_AGE} days). "
            "Currently has no effect; accepted for backward compatibility."
        ),
    )
    cache.add_argument(
        "--force-rget-list",
        action="store_true",
        help=(
            "Force RGET-LIST use even if old. "
            "Currently has no effect; accepted for backward compatibility."
        ),
    )
    cache.add_argument(
        "--no-etag",
        action="store_true",
        help="Disable ETag-based freshness checks; range transfers still require strong ETags for integrity",
    )
    cache.add_argument(
        "--missing-files",
        action="store_true",
        help=(
            "Download absent files and skip freshness checks for existing files, "
            "except paths selected by --check-files. Unselected in-place changes "
            "will be missed; use occasional full runs when needed."
        ),
    )
    cache.add_argument(
        "--check-files",
        nargs="+",
        action="extend",
        default=[],
        metavar="PATH",
        help=(
            "With --missing-files, check selected existing files for updates. "
            "Exact paths relative to --url, including any --dir; "
            "use @FILE for a UTF-8 list (one path per line)"
        ),
    )

    async_grp = parser.add_argument_group("Async & Adaptive Options")
    async_grp.add_argument(
        "--async-metadata",
        action="store_true",
        default=True,
        help="Enable async metadata checks (default: enabled); normal sync uses them only for more than 80 remote files, outside dry runs",
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
        help=f"Async metadata admission limit (default: {DEFAULT_ASYNC_WORKERS}); adaptive admission is also capped at {ADAPTIVE_MAX_CONCURRENCY}",
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
        help="Use fixed --async-workers admission instead of adaptive metadata concurrency",
    )
    async_grp.add_argument(
        "--adaptive-start-concurrency",
        type=int,
        default=ADAPTIVE_START_CONCURRENCY,
        metavar="N",
        help=f"Starting async metadata concurrency (default: {ADAPTIVE_START_CONCURRENCY}), bounded by --async-workers and {ADAPTIVE_MAX_CONCURRENCY}",
    )
    async_grp.add_argument(
        "--adaptive-error-threshold",
        type=float,
        default=ADAPTIVE_ERROR_THRESHOLD,
        metavar="RATE",
        help=f"Adaptive metadata fallback error rate (range: 0-1; default: {ADAPTIVE_ERROR_THRESHOLD})",
    )

    cleanup = parser.add_argument_group("Cleanup & Safety Options")
    cleanup.add_argument(
        "--cleanup",
        type=str,
        choices=["safe", "preview", "delete", "move"],
        default=argparse.SUPPRESS,
        help="Obsolete-file policy (default: safe): safe preserves obsolete files; preview reports actions but downloads still run; move archives; delete removes. Changed files may still be replaced",
    )
    cleanup.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Require confirmation before deletion (delete mode only)",
    )
    cleanup.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan/check without downloading or changing mirrored files; still makes requests and may create log/cache bookkeeping directories",
    )
    cleanup.add_argument(
        "--quick",
        action="store_true",
        help="Refresh an existing JSON cache's expiry timestamp only; does not verify the mirror or create a missing cache. Connection setup may still make requests",
    )

    security = parser.add_argument_group("Security Options")
    security.add_argument(
        "--security-validation",
        action="store_true",
        default=True,
        help="Enable extra URL validation and per-IP pacing (default: enabled); transport IP restrictions are independent",
    )
    security.add_argument(
        "--no-security-validation",
        action="store_false",
        dest="security_validation",
        help="Disable extra URL validation and per-IP pacing; secure transport still rejects private/loopback targets",
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
        help=(
            "Report possible duplicate directory subtrees by matching non-empty entry names (default: disabled). This heuristic can flag unrelated directories and does not detect file symlinks. Start with --symlink-mode detect --print-logs and review the paths"
        ),
    )
    symlink.add_argument(
        "--symlink-mode",
        choices=["detect", "follow", "skip", "treat-as-file"],
        default="skip",
        help=(
            "Requires --handle-symlinks. detect reports and keeps scanning, implying --dry-run; skip omits detected duplicates (default); follow mirrors within scope/tracker limits; treat-as-file behaves like skip"
        ),
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
        help="Compatibility setting (default: enabled); has no effect on sync. --circuit-breaker-enabled controls download breakers",
    )
    symlink.add_argument(
        "--no-circuit-breaker-downloads",
        action="store_false",
        dest="circuit_breaker_downloads",
        help=(
            "Compatibility setting; has no effect on sync. Use --no-circuit-breaker to disable download breakers"
        ),
    )

    logging_grp = parser.add_argument_group("Logging & Output Options")
    logging_grp.add_argument("--debug", action="store_true", help="Enable debug logging")
    logging_grp.add_argument("--print-logs", action="store_true", help="Print logs to console")
    logging_grp.add_argument(
        "--log-file",
        metavar="NAME",
        help=(
            "Custom log prefix: NAME_SUFFIX_TIMESTAMP.log, with suffixes joined by underscores (or all) and YYYYMMDD_HHMMSS. By default, multiple directories share mirror_url_FIRST--LAST_TIMESTAMP.log. Unsafe characters are replaced, long names are shortened, and collisions get a unique identifier"
        ),
    )
    logging_grp.add_argument("--quiet", action="store_true", help="Quiet mode (WARNING+ only)")
    logging_grp.add_argument("--verbose", action="store_true", help="Verbose mode (DEBUG)")
    logging_grp.add_argument(
        "--progress-bar", "--progress", action="store_true", help="Enable tqdm progress bar"
    )
    logging_grp.add_argument(
        "--stats",
        action="store_true",
        help="Compatibility setting; no effect. Use --metrics for detailed completion metrics",
    )
    logging_grp.add_argument(
        "--metrics",
        action="store_true",
        help="Include detailed METRICS SUMMARY after the compact SUMMARY (default: off)",
    )
    logging_grp.add_argument(
        "--metrics-json",
        type=Path,
        metavar="PATH",
        help="Export metrics to JSON and enable the localhost health/metrics server; both are disabled in dry runs",
    )

    scan = parser.add_argument_group("Scan & Path Options")
    scan.add_argument(
        "--scan-mode",
        choices=["sequential", "parallel", "adaptive", "async"],
        default="adaptive",
        help="Directory discovery: parallel uses up to min(--workers, 4) listing workers; adaptive (default), sequential and async use sequential discovery. File metadata concurrency is configured separately",
    )
    scan.add_argument(
        "--parallel-threshold",
        type=int,
        default=PARALLEL_SCAN_THRESHOLD,
        metavar="N",
        help=(
            f"Parallel scan threshold (default: {PARALLEL_SCAN_THRESHOLD}). "
            "Currently has no effect; accepted for backward compatibility."
        ),
    )
    scan.add_argument(
        "--max-depth",
        type=int,
        default=None,
        metavar="N",
        help=(
            f"Target-root recursion depth (root: 0; default: {MAX_DIRECTORY_DEPTH}). CLI-only --list-dirs defaults to {LIST_DIRS_DEFAULT_MAX_DEPTH}; --config uses its file/model depth unless explicitly overridden"
        ),
    )
    scan.add_argument(
        "--max-filename-len",
        type=int,
        default=MAX_FILENAME_LENGTH,
        metavar="N",
        help=f"Local filename sanitization/truncation limit (default: {MAX_FILENAME_LENGTH}); colliding sanitized paths fail before downloads",
    )
    scan.add_argument(
        "--download-queue-size",
        type=int,
        default=1000,
        metavar="N",
        help="Compatibility queue capacity (default: 1000); sync materializes the remote file list and does not use the bounded queue",
    )

    advanced = parser.add_argument_group("Advanced Performance Options")
    advanced.add_argument(
        "--adaptive-batch-processing",
        action="store_true",
        default=True,
        help="Compatibility setting (default: enabled); does not change sync batching",
    )
    advanced.add_argument(
        "--no-adaptive-batch-processing",
        action="store_false",
        dest="adaptive_batch_processing",
        help="Compatibility setting; does not change sync batching",
    )
    advanced.add_argument(
        "--initial-batch-size",
        type=int,
        default=BATCH_SIZE,
        metavar="N",
        help=f"Compatibility initial batch size (default: {BATCH_SIZE}); does not change sync batching",
    )
    advanced.add_argument(
        "--max-batch-size",
        type=int,
        default=MAX_BATCH_SIZE,
        metavar="N",
        help=f"Compatibility maximum batch size (default: {MAX_BATCH_SIZE}); does not change sync batching",
    )
    advanced.add_argument(
        "--target-batch-time",
        type=float,
        default=TARGET_BATCH_TIME_SECONDS,
        metavar="SECS",
        help=f"Compatibility target batch time (default: {TARGET_BATCH_TIME_SECONDS}s); does not change sync batching",
    )
    advanced.add_argument(
        "--memory-cache-size",
        type=int,
        default=MEMORY_CACHE_MAX_SIZE,
        metavar="N",
        help=f"Optional tracking-component threshold (default: {MEMORY_CACHE_MAX_SIZE}); does not bound sync's remote file list or active metadata-cache capacities",
    )
    advanced.add_argument(
        "--use-disk-backed-sets",
        action="store_true",
        help="Configure optional disk-backed tracking; sync does not populate it or spill its remote file list to disk",
    )
    advanced.add_argument(
        "--disk-cache-dir",
        type=Path,
        metavar="DIR",
        help="Cache/tracking component directory; does not spill sync's remote file list",
    )
    advanced.add_argument(
        "--fast-parsing-fallback",
        action="store_true",
        default=True,
        help="Allow lightweight-parser fallback after lxml fails (default: enabled); large listings/no lxml select the lightweight parser independently",
    )
    advanced.add_argument(
        "--no-fast-parsing-fallback",
        action="store_false",
        dest="fast_parsing_fallback",
        help="Disable fallback after lxml failure; large listings/no lxml still select the lightweight parser",
    )
    advanced.add_argument(
        "--http2",
        action="store_true",
        default=True,
        help="Enable HTTP/2 (default: enabled); overrides http2: false in a config file",
    )
    advanced.add_argument("--no-http2", action="store_false", dest="http2", help="Disable HTTP/2")
    advanced.add_argument(
        "--http2-pipelining",
        action="store_true",
        default=True,
        help="Compatibility setting (default: enabled); the HTTP/2 client does not read this setting",
    )
    advanced.add_argument(
        "--no-http2-pipelining",
        action="store_false",
        dest="http2_pipelining",
        help="Disable HTTP/2 pipelining. Currently has no effect; accepted for backward compatibility.",
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
        help=f"Standalone filesystem-cache TTL (default: {FS_CACHE_TTL_SECONDS}s); sync freshness checks use direct filesystem stats",
    )
    advanced.add_argument(
        "--no-content-hash",
        action="store_false",
        dest="content_hash_small_files",
        default=True,
        help="Compatibility setting; has no effect on file freshness checks",
    )
    verification = advanced.add_mutually_exclusive_group()
    verification.add_argument(
        "--verify-content",
        action="store_true",
        default=False,
        help="Verify local files against saved SHA-256 receipts before freshness checks",
    )
    verification.add_argument(
        "--no-verify-content",
        action="store_false",
        dest="verify_content",
        default=False,
        help="Disable content verification (overrides a config file)",
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
        help="Localhost health/metrics port (default: 8080); enabled only with --metrics-json outside dry runs",
    )

    args = parser.parse_args()
    explicit_dests = _explicit_cli_dests(parser, sys.argv[1:])

    if args.max_depth is None:
        # --max-depth wasn't given explicitly. --list-dirs is almost always
        # "what's in this folder", not "walk the whole tree" -- defaulting
        # it to MAX_DIRECTORY_DEPTH (50) silently recurses into every
        # subdirectory, which is rarely what's wanted and was a reported
        # surprise in practice. Give --list-dirs its own shallow default;
        # every other mode (including --list-files and real syncs) keeps
        # the original MAX_DIRECTORY_DEPTH default. An explicit --max-depth
        # always wins, for every mode, including --list-dirs.
        args.max_depth = (
            LIST_DIRS_DEFAULT_MAX_DEPTH
            if getattr(args, "list_dirs", None) is not None
            else MAX_DIRECTORY_DEPTH
        )

    if (
        getattr(args, "list_dirs", None) is not None
        and getattr(args, "list_files", None) is not None
    ):
        parser.error("--list-dirs and --list-files are mutually exclusive")

    if "download_files" in explicit_dests and "url_list" in explicit_dests:
        parser.error("download files and --url-list are mutually exclusive")

    # Handle config file
    config_dict = {}
    if args.config:
        try:
            with open(args.config) as f:
                if Path(args.config).suffix.lower() in [".yaml", ".yml"]:
                    config_dict = yaml.safe_load(f)
                else:
                    config_dict = json.load(f)

                config_dict = expand_env_vars(config_dict)
                if not isinstance(config_dict, dict):
                    raise ValueError("Configuration must be an object")

            if not args.url and "base_url" in config_dict:
                args.url = config_dict["base_url"]
            if not args.dest_path and "dest_path" in config_dict:
                args.dest_path = Path(config_dict["dest_path"])
            if not args.log_path and "log_path" in config_dict:
                args.log_path = Path(config_dict["log_path"])
            if not args.dir_suffix and not args.dir_regex and "dir_suffix" in config_dict:
                args.dir_suffix = [config_dict["dir_suffix"]]
        except Exception as e:
            parser.error(f"Error reading config file: {e}")

    # Resolve local selection inputs before ownership/logging and only once,
    # so each suffix in a run uses the same selected remote paths.
    try:
        args.check_files = _expand_check_files(args.check_files)
        if args.config:
            config_dict["check_files"] = (
                args.check_files
                if "check_files" in explicit_dests
                else _expand_check_files(config_dict.get("check_files", []))
            )
    except ConfigError as error:
        parser.error(str(error))

    selected_mode = (
        args.mode
        if "mode" in explicit_dests or not args.config
        else config_dict.get("mode", "mirror")
    )
    try:
        compile_directory_patterns(args.dir_regex)
    except ConfigError as error:
        parser.error(str(error))
    if args.dir_regex and (selected_mode != "mirror" or args.benchmark):
        parser.error("--dir-regex requires mirror mode without --benchmark")
    input_values = {**config_dict, **_cli_overrides(args, explicit_dests)}
    _direct_download_defaults(args, input_values, selected_mode, parser)
    if args.config:
        missing = [
            f"{key} in config file or {flag} on command line"
            for key, flag, value in (
                ("base_url", "--url", args.url),
                ("dest_path", "--dest-path", args.dest_path),
                ("log_path", "--log-path", args.log_path),
            )
            if not value
        ]
        if missing:
            parser.error(f"Missing required configuration: {', '.join(missing)}")
    else:
        if not args.url:
            parser.error("--url is required when --config is not used")
        if (
            getattr(args, "list_dirs", None) is not None
            or getattr(args, "list_files", None) is not None
        ):
            # --list-dirs / --list-files only discover and print the remote
            # tree -- neither ever writes to dest_path, and log_path is only
            # used for its own run log/cache-file bookkeeping. Don't force
            # the user to name either; fall back to a scratch directory under
            # the system temp dir when they haven't supplied one.
            if not args.dest_path:
                args.dest_path = Path(tempfile.gettempdir()) / "mirror-url-list-dirs"
            if not args.log_path:
                args.log_path = Path(tempfile.gettempdir()) / "mirror-url-list-dirs"
        else:
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

    # Both modes start before job configuration, ownership and logging setup.
    first_job_started = time.perf_counter()
    effective, base_config = None, None
    if args.config:
        try:
            base_config = MirrorConfig.from_dict(config_dict, silent=True)
            effective = _effective_args(args, explicit_dests, base_config)
        except Exception:
            pass  # The existing per-suffix config path reports invalid configuration.

    if selected_mode == "download":
        from .config import load_config_from_args
        from .transfers import download_url_list, require_backend

        try:
            if len(args.dir_suffix) > 1:
                raise ConfigError("download mode supports one selected suffix per run")
            if args.config:
                values = {
                    **config_dict,
                    "base_url": args.url,
                    "dest_path": args.dest_path,
                    "log_path": args.log_path,
                    **_cli_overrides(args, explicit_dests),
                    "dir_suffix": args.dir_suffix[0] if args.dir_suffix else "",
                }
                download_config = MirrorConfig.from_dict(values, silent=True)
            else:
                download_config = load_config_from_args(args, silent=True)
            download_config.dir_suffix = args.dir_suffix[0].strip("/") if args.dir_suffix else ""
            download_config.print_logs = args.print_logs
            download_config.quiet = args.quiet
            require_backend(download_config.backend)
            target = selected_directory_targets(
                download_config.dest_path,
                [download_config.dir_suffix],
                max_filename_len=download_config.max_filename_len,
            )[0]
            download_resources = [target]
            if download_config.metrics_json is not None:
                download_resources.append(download_config.metrics_json)
            ownership.enter_context(run_ownership(download_resources))
            args.log_file = args.log_file or "download"
            display_settings = _effective_args(args, explicit_dests, download_config)
            display_settings.dest_path = target
            setup_shared_logging(args, display_settings)
            ownership.callback(_close_run_handlers, list(logging.root.handlers))
            logging.debug(
                "Known-URL download: backend=%s, discovery and freshness checks omitted",
                download_config.backend,
            )
            if download_config.parallel_downloads or download_config.streaming_parallel:
                logging.debug("Range metadata probes enabled for parallel chunk downloads")
            metrics = MetricsCollector()
            report = metrics.run_report
            report.start_job(first_job_started)
            try:
                summary = download_url_list(download_config, metrics=metrics)
            except BaseException as error:
                report.finish_job(error)
                raise
            report.finish_job()
            if download_config.metrics_json is not None and not download_config.dry_run:
                metrics.export_json(download_config.metrics_json, download_config)
            run = report.snapshot(summary.get("bytes_downloaded", 0))
            if not summary.get("dry_run"):
                summary["elapsed_seconds"] = run["elapsed_seconds"]
                summary["duration_scope"] = run["duration_scope"]
            log_download_report(download_config, summary, target, run, metrics=metrics)
            log_run_completion(summary["success"])
            log_processed_result(summary["success"], download_config.dir_suffix, 1, 1)
            log_connection_pools_closed(report.closed_connection_pools)
        except (MirrorError, ValueError, OSError) as error:
            print(f"Download error: {error}", file=sys.stderr)
            sys.exit(1)
        if not args.print_logs or args.quiet:
            print(json.dumps(summary, sort_keys=True))
        if not summary["success"]:
            sys.exit(1)
        return

    # Reserve selected roots and configured shared state before logging. Log
    # and cache files join the same ownership before access, allowing unrelated
    # jobs to share their parent directory. Suffix instances borrow this run.
    settings = effective if effective is not None else args
    resources: List[Path] = []
    try:
        targets = selected_directory_targets(
            Path(args.dest_path),
            [""] if args.dir_regex else args.dir_suffix or [""],
            max_filename_len=settings.max_filename_len,
        )
        if settings.cleanup_policy == CleanupPolicy.MOVE:
            validate_move_targets(targets)
        for target in targets:
            resources.append(target)
            if settings.cleanup_policy == CleanupPolicy.MOVE:
                resources.append(target.parent / (target.name + "_obsolete"))
        for field in ("chunk_assembly_dir", "disk_cache_dir", "metrics_json"):
            value = getattr(settings, field, getattr(base_config, field, None))
            if value is not None:
                resources.append(Path(value))
        ownership.enter_context(run_ownership(resources))
    except (DestinationLockError, PathTraversalError, ConfigError) as error:
        print(f"Destination ownership error: {error}", file=sys.stderr)
        sys.exit(1)

    # Multiple directory jobs belong to one invocation and one complete log.
    if args.log_file or (len(args.dir_suffix) > 1 and not args.benchmark):
        setup_shared_logging(args, effective)
        ownership.callback(_close_run_handlers, list(logging.root.handlers))
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
            console_handler = ProgressConsoleHandler(sys.stderr)
            console_handler.setFormatter(
                SafeLogFormatter(
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

    directory_depths = {}
    if args.dir_regex:
        from .config import load_config_from_args

        try:
            if args.config:
                selection_config = MirrorConfig.from_dict(
                    {
                        **config_dict,
                        **_cli_overrides(args, explicit_dests),
                        "base_url": args.url,
                        "dest_path": args.dest_path,
                        "log_path": args.log_path,
                        "dir_suffix": "",
                    },
                    silent=True,
                )
            else:
                selection_config = load_config_from_args(args, silent=True)
            # Initialization logs must not install basicConfig's unsolicited
            # stderr handler when neither a shared log nor --print-logs is used.
            quiet_handler = None
            if not logging.root.handlers:
                quiet_handler = logging.NullHandler()
                logging.root.addHandler(quiet_handler)
            try:
                directory_depths = resolve_directory_selection(
                    selection_config, args.dir_regex, mirror_factory=MirrorURL
                )
            finally:
                if quiet_handler is not None:
                    logging.root.removeHandler(quiet_handler)
                    quiet_handler.close()
            args.dir_suffix = list(directory_depths)
            if not use_shared and len(args.dir_suffix) > 1:
                setup_shared_logging(args, effective)
                ownership.callback(_close_run_handlers, list(logging.root.handlers))
                use_shared = True
            logging.getLogger(__name__).info(
                "Directory selection resolved %s directories", len(args.dir_suffix)
            )
        except Exception as error:
            print(f"Directory selection error: {error}", file=sys.stderr)
            sys.exit(1)

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
            missing_files=getattr(args, "missing_files", False),
            check_files=getattr(args, "check_files", []),
            verify_content=getattr(args, "verify_content", False),
            use_shared_log=use_shared,
            scan_mode=ScanMode(args.scan_mode) if args.scan_mode else ScanMode.ADAPTIVE,
            parallel_threshold=args.parallel_threshold,
            benchmark=True,
            http2=args.http2,
            stats=args.stats,
            metrics=args.metrics,
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
            requests_per_second=getattr(args, "requests_per_second", DEFAULT_RATE_LIMIT),
            mode=getattr(args, "mode", "mirror"),
            backend=getattr(args, "backend", "httpx"),
            url_list=getattr(args, "url_list", None),
            overwrite=getattr(args, "overwrite", False),
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
            streaming_parallel=getattr(args, "streaming_parallel", True),
            sequential_downloads=getattr(args, "sequential_downloads", False),
        )

        if args.config:
            benchmark_config = MirrorConfig.from_dict(
                {
                    **config_dict,
                    "base_url": args.url,
                    "dest_path": args.dest_path,
                    "log_path": args.log_path,
                    **_cli_overrides(args, explicit_dests),
                    "dir_suffix": benchmark_suffix,
                    "benchmark": True,
                },
                silent=use_shared,
            )
        benchmark_ok = False
        with MirrorURL(benchmark_config) as mirror:
            mirror.install_signal_handlers()
            if hasattr(mirror, "connection_manager") and mirror.connection_manager:
                result = mirror.benchmark()
                benchmark_ok = bool(result.get("connection_test")) and not getattr(
                    mirror, "scan_incomplete", False
                )
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

        sys.exit(0 if benchmark_ok else 1)

    # Process suffixes
    suffixes = args.dir_suffix if args.dir_suffix else [""]
    total = len(suffixes)
    processed = []
    failed = []
    skipped = []
    file_totals: Counter[str] = Counter()

    for i, suf in enumerate(suffixes, 1):
        job_started = first_job_started if i == 1 else time.perf_counter()
        try:
            if args.config:
                base_config = MirrorConfig.from_dict(
                    {
                        **config_dict,
                        "base_url": args.url,
                        "dest_path": args.dest_path,
                        "log_path": args.log_path,
                        **_cli_overrides(args, explicit_dests),
                    },
                    silent=True,
                )
                # Start from *every* field of the file's config (a hand-copied subset
                # used to drop health_check_port, parallel_optimization_mode, ...),
                # then layer on only the options that were typed on the command line.
                suffix_values = {
                    name: getattr(base_config, name) for name in MirrorConfig.model_fields
                }
                suffix_values["dir_suffix"] = suf
                suffix_values["use_shared_log"] = use_shared
                suffix_values.update(_cli_overrides(args, explicit_dests))

                suffix_config = MirrorConfig.from_dict(suffix_values, silent=use_shared)
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
                    file_filters=list(args.filter),
                    exclude_dirs=args.exclude_dir or [],
                    cleanup_policy=args.cleanup_policy,
                    quick=args.quick,
                    no_rget_list=args.no_rget_list,
                    rget_list_max_age=args.rget_list_max_age,
                    force_rget_list=args.force_rget_list,
                    hash_algorithm=args.hash_algorithm,
                    no_cache=args.no_cache,
                    refresh_cache=args.refresh_cache,
                    cache_max_age=args.cache_max_age,
                    no_etag=getattr(args, "no_etag", False),
                    missing_files=getattr(args, "missing_files", False),
                    check_files=getattr(args, "check_files", []),
                    verify_content=getattr(args, "verify_content", False),
                    list_dirs=getattr(args, "list_dirs", None) is not None,
                    list_dirs_n=getattr(args, "list_dirs", None) or 0,
                    list_files=getattr(args, "list_files", None) is not None,
                    list_files_n=getattr(args, "list_files", None) or 0,
                    use_shared_log=use_shared,
                    scan_mode=ScanMode(args.scan_mode),
                    parallel_threshold=args.parallel_threshold,
                    benchmark=args.benchmark,
                    http2=args.http2,
                    stats=args.stats,
                    metrics=args.metrics,
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
                    requests_per_second=getattr(args, "requests_per_second", DEFAULT_RATE_LIMIT),
                    mode=getattr(args, "mode", "mirror"),
                    backend=getattr(args, "backend", "httpx"),
                    url_list=getattr(args, "url_list", None),
                    overwrite=getattr(args, "overwrite", False),
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
                )

        except ConfigError as e:
            if use_shared:
                logging.critical(f"[{i}/{total}] Configuration error for {suf or 'ROOT'}: {e}")
            if not (use_shared and args.print_logs):
                print(f"Configuration error for {suf or 'ROOT'}: {e}", file=sys.stderr)
            failed.append(suf or "ROOT")
            continue
        except Exception as e:
            if use_shared:
                logging.critical(f"[{i}/{total}] Error creating config for {suf or 'ROOT'}: {e}")
            if not (use_shared and args.print_logs):
                print(f"Error creating config for {suf or 'ROOT'}: {e}", file=sys.stderr)
            failed.append(suf or "ROOT")
            continue

        try:
            mirror = None
            job_report = None
            sync_success = None
            job_error = None
            try:
                if suf in directory_depths:
                    # A selected directory at the discovery limit still includes
                    # its own files, but has zero remaining child-directory depth.
                    suffix_config = suffix_config.model_copy(
                        update={"max_depth": directory_depths[suf]}
                    )
                with MirrorURL(suffix_config, suffix_index=i, total_suffixes=total) as mirror:
                    if args.dir_regex and i == 1:
                        logging.info("Selected directories (%s):", len(args.dir_suffix))
                        for selected_path in args.dir_suffix:
                            logging.info("  %s", selected_path)
                    mirror.install_signal_handlers()
                    if not hasattr(mirror, "connection_manager") or not mirror.connection_manager:
                        logging.warning(f"[{i}/{total}] No connection manager for {suf or 'ROOT'}")
                        skipped.append(suf or "ROOT")
                    elif not mirror.connection_ok:
                        logging.warning(
                            f"[{i}/{total}] Connection failed for {suf or 'ROOT'} (404?)"
                        )
                        failed.append(suf or "ROOT")
                    elif getattr(suffix_config, "list_dirs", False):
                        if mirror.list_directories():
                            logging.info(f"[{i}/{total}] ✅ Listed directories: {suf or 'ROOT'}")
                            processed.append(suf or "ROOT")
                        else:
                            logging.error(
                                f"[{i}/{total}] ❌ Failed to list directories: {suf or 'ROOT'}"
                            )
                            failed.append(suf or "ROOT")
                    elif getattr(suffix_config, "list_files", False):
                        if mirror.list_files():
                            logging.info(f"[{i}/{total}] ✅ Listed files: {suf or 'ROOT'}")
                            processed.append(suf or "ROOT")
                        else:
                            logging.error(f"[{i}/{total}] ❌ Failed to list files: {suf or 'ROOT'}")
                            failed.append(suf or "ROOT")
                    else:
                        job_report = mirror.metrics.run_report
                        job_report.start_job(job_started)
                        sync_success = mirror.sync()

            except BaseException as error:
                job_error = error
                raise
            finally:
                if job_report is not None:
                    snapshot = _finish_mirror_job(mirror, sync_success, job_error, suf, i, total)
                    if job_report.status not in ("quick", "survey"):
                        file_totals.update(
                            {
                                "found": snapshot["run"]["remote_files_found"],
                                "downloaded": snapshot["files_downloaded"],
                                "would_download": snapshot["run"]["downloads_planned"],
                                "skipped": snapshot["files_skipped"],
                                "failed": snapshot["files_failed"],
                            }
                        )
            if sync_success is not None:
                (processed if sync_success else failed).append(suf or "ROOT")

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
        except DestinationLockError as e:
            logging.critical(f"Destination ownership error for {suf or 'ROOT'}: {e}")
            failed.append(suf or "ROOT")
        except Exception as e:
            logging.critical(f"Error with {suf or 'ROOT'}: {e}", exc_info=True)
            failed.append(suf or "ROOT")

    # Final summary
    if use_shared or total > 1:
        logging.info("=" * 50)
        logging.info("FINAL SUMMARY")
        if file_totals:
            logging.info("TOTAL FILES FOUND: %s", file_totals["found"])
            if settings.dry_run:
                logging.info("WOULD DOWNLOAD: %s", file_totals["would_download"])
            else:
                logging.info("DOWNLOADED: %s", file_totals["downloaded"])
            logging.info("SKIPPED: %s", file_totals["skipped"])
            logging.info("FILES FAILED: %s", file_totals["failed"])
            logging.info("")
        logging.info(f"Total suffixes processed: {total}")
        logging.info("")

        if processed:
            logging.info(f"✅ SUCCESSFUL DIRECTORIES ({len(processed)}):")
            for suffix in processed:
                logging.info(f"   • {suffix}")
        else:
            logging.info("✅ SUCCESSFUL DIRECTORIES: (none)")

        logging.info("")

        if failed:
            logging.error(f"❌ FAILED DIRECTORIES ({len(failed)}):")
            for suffix in failed:
                logging.error(f"   • {suffix}")
        else:
            logging.info("❌ FAILED DIRECTORIES: (none)")

        logging.info("")

        if skipped:
            logging.warning(f"⏭️ SKIPPED DIRECTORIES ({len(skipped)}):")
            for suffix in skipped:
                logging.warning(f"   • {suffix}")
        else:
            logging.info("⏭️ SKIPPED DIRECTORIES: (none)")

        logging.info("=" * 50)

    # Cleanup log handlers
    for handler in _log_files:
        try:
            handler.close()
        except Exception:
            pass

    sys.exit(0 if not failed else 1)
