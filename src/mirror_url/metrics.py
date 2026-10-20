"""Run-wide metrics aggregation.

Migrated verbatim from ``mirror_url.py`` (orig. lines 3523-3913):
``MetricsCollector``. Exact-URL download results are formatted here too.
"""

from __future__ import annotations

import json
import logging
import statistics
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional, Sequence

from ._version import __version__
from .constants import ADAPTIVE_START_CONCURRENCY
from .run_report import TRANSFER_STAGES, RunReport
from .utils import format_bytes, format_duration, sanitize_command_line, sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig

CHUNK_METRICS = (
    "chunk_downloads",
    "chunk_assemblies",
    "chunk_failures",
    "chunk_retries",
    "parallel_files",
    "total_chunks",
    "peak_parallel_chunks",
)


class MetricsCollector:
    """Collect and report metrics with performance tracking"""

    def __init__(self):
        """Initialize metrics collector"""
        self.metrics: Dict[str, Any] = {
            "files_downloaded": 0,
            "bytes_downloaded": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "directories_processed": 0,
            "directories_scanned_parallel": 0,
            "directories_scanned_sequential": 0,
            "directories_scanned_async": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "cache_head_requests_saved": 0,
            "cache_refreshed": False,
            "cache_age_days": 0,
            "cache_signatures": 0,
            "cache_invalidated_dirs": 0,
            "html_cache_hits": 0,
            "html_cache_misses": 0,
            "rget_list_used": False,
            "parse_time_seconds": 0,
            "listing_get_seconds": 0.0,
            "directory_signature_head_calls": 0,
            "directory_signature_head_seconds": 0.0,
            "directory_signatures_from_get": 0,
            "errors": [],
            "etag_matches": 0,
            "etag_mismatches": 0,
            "etag_304_responses": 0,
            "etag_unavailable": 0,
            "http2_connections": 0,
            "http11_fallbacks": 0,
            "request_times": [],
            "download_times": [],
            "parse_times": [],
            "async_metadata_checks": 0,
            "content_hash_verifications": 0,
            "peak_memory_mb": 0,
            "files_would_delete": 0,
            "dirs_would_delete": 0,
            "adaptive_async_enabled": False,
            "adaptive_fallback_to_sync": False,
            "adaptive_current_concurrency": ADAPTIVE_START_CONCURRENCY,
            "adaptive_server_profiles": {},
            "security_blocks": 0,
            "circuit_breaker_trips": 0,
            "resumed_downloads": 0,
            "bandwidth_limited": False,
            "queue_size": 0,
            "queue_max_size": 0,
            "async_scan_fallbacks": 0,
            "async_scan_success": 0,
            "symlinks_followed": 0,
            "symlinks_skipped": 0,
            "symlink_loops_detected": 0,
            "symlink_depth_exceeded": 0,
            "symlink_bomb_prevented": 0,
            "fast_parses": 0,
            "lxml_parses": 0,
            "connection_pool_hits": 0,
            "connection_pool_misses": 0,
            "connection_pool_evictions": 0,
            "fs_cache_hits": 0,
            "fs_cache_misses": 0,
            "batch_size_adjustments": 0,
            "cache_corruptions": 0,
            "cleanup_failed_operations": 0,
            # NEW v2.0.0 metrics
            "disk_space_checks": 0,
            "disk_space_warnings": 0,
            "memory_pressure_events": 0,
            "partial_downloads": 0,
            "partial_resumes": 0,
            "stale_partials_cleaned": 0,
            "health_checks": 0,
            "rate_limit_delays": 0,
            # NEW v3.0.0 metrics
            "chunk_downloads": 0,
            "chunk_assemblies": 0,
            "chunk_failures": 0,
            "chunk_retries": 0,
            "parallel_files": 0,
            "total_chunks": 0,
            "active_chunks": 0,
            "peak_parallel_chunks": 0,
            "chunk_mode_requested": False,
            # NEW v3.0.6 metrics
            "auto_concurrency_enabled": False,
            "auto_concurrency_adjustments": 0,
            "auto_concurrency_final": 0,
            "auto_concurrency_start": 0,
        }
        self.lock = RLock()
        self.start_time = time.time()
        self.parse_start_time = 0

        # IMPROVED: Use thread-safe lists for time series
        self._request_times = deque(maxlen=1000)
        self._download_times = deque(maxlen=1000)
        self._parse_times = deque(maxlen=1000)
        self._errors = deque(maxlen=100)
        self._times_lock = RLock()
        self.run_report = RunReport()

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
            self.metrics["bytes_downloaded"] += bytes_count

    def chunk_started(self) -> None:
        """Count a worker only after it acquires its origin's semaphore."""
        with self.lock:
            self.metrics["active_chunks"] += 1
            active = self.metrics["active_chunks"]
            self.metrics["peak_parallel_chunks"] = max(self.metrics["peak_parallel_chunks"], active)
            self.run_report.record_chunk_workers(active)

    def chunk_finished(self) -> None:
        with self.lock:
            self.metrics["active_chunks"] -= 1

    def chunk_summary(self) -> dict:
        with self.lock:
            result = {
                key: self.run_report.delta(self.metrics, key)
                if self.run_report.started is not None
                else self.metrics[key]
                for key in CHUNK_METRICS
            }
            result["peak_parallel_chunks"] = self.run_report.snapshot()["peak_parallel_chunks"]
            return result

    def add_error(self, error: str, error_type: str = "unknown") -> None:
        """Add error to metrics - THREAD SAFE"""
        self.increment("errors_recorded")
        with self._times_lock:
            self._errors.append(
                {"timestamp": datetime.now().isoformat(), "type": error_type, "message": error}
            )

    def add_request_time(self, duration: float) -> None:
        """Add request time to metrics - THREAD SAFE"""
        with self._times_lock:
            self._request_times.append(duration)

    def add_directory_request_time(self, method: Literal["GET", "HEAD"], duration: float) -> None:
        """Sum logical directory request durations, including failed calls."""
        key = "listing_get_seconds" if method == "GET" else "directory_signature_head_seconds"
        with self.lock:
            self.metrics[key] += duration

    def add_download_time(self, duration: float) -> None:
        """Add download time to metrics - THREAD SAFE"""
        with self._times_lock:
            self._download_times.append(duration)

    def add_parse_time(self, duration: float) -> None:
        """Retain each listing operation's duration and its accumulated work."""
        with self.lock:
            self.metrics["parse_time_seconds"] += duration
            with self._times_lock:
                self._parse_times.append(duration)

    def set_rget_used(self) -> None:
        """Mark RGET-LIST as used"""
        with self.lock:
            self.metrics["rget_list_used"] = True

    def set_cache_refreshed(self, age_days: float = 0) -> None:
        """Mark cache as refreshed"""
        with self.lock:
            self.metrics["cache_refreshed"] = True
            self.metrics["cache_age_days"] = age_days

    def set_cache_signatures(self, count: int) -> None:
        """Set number of cache signatures"""
        with self.lock:
            self.metrics["cache_signatures"] = count

    def start_parse_timer(self) -> None:
        """Start parse timer"""
        self.parse_start_time = time.time()

    def stop_parse_timer(self) -> None:
        """Stop parse timer and record duration"""
        if self.parse_start_time > 0:
            elapsed = time.time() - self.parse_start_time
            self.add_parse_time(elapsed)
            self.parse_start_time = 0

    def update_queue_metrics(self, queue_size: int, max_size: int) -> None:
        """Update queue metrics"""
        with self.lock:
            self.metrics["queue_size"] = queue_size
            self.metrics["queue_max_size"] = max(max_size, self.metrics["queue_max_size"])

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
                summary["request_times"] = list(self._request_times)
                summary["download_times"] = list(self._download_times)
                summary["parse_times"] = list(self._parse_times)
                summary["errors"] = list(self._errors)

            # Copy dict metrics
            for key, value in self.metrics.items():
                if isinstance(value, dict) and key not in (
                    "request_times",
                    "download_times",
                    "parse_times",
                    "errors",
                ):
                    summary[key] = dict(value)

            elapsed = time.time() - self.start_time
            summary["elapsed_seconds"] = elapsed
            summary["download_speed"] = summary["bytes_downloaded"] / elapsed if elapsed > 0 else 0
            summary["run"] = self.run_report.snapshot(
                summary["bytes_downloaded"], summary["files_skipped"]
            )

            # Calculate statistics safely
            if summary["request_times"]:
                summary["request_avg_ms"] = statistics.mean(summary["request_times"]) * 1000

            return summary

    def report(
        self,
        prefix: str = "",
        show_stats: bool = False,
        *,
        run: Optional[dict] = None,
        known_urls: bool = False,
        values: Optional[dict] = None,
    ) -> str:
        """
        Generate detailed metrics report.

        Args:
            prefix: Prefix for log lines
            show_stats: Unused. Accepted for backward compatibility (``--stats``
                used to be documented as switching on a detailed summary, but the
                full summary has always been produced regardless).
            run: Finished coordinator snapshot for a known-URL result.
            known_urls: Mark discovery and metadata as not performed while
                retaining the common summary layout and zero subsystem work.

        Returns:
            Formatted metrics report
        """
        summary = self.get_summary()
        if values is not None:
            summary.update(values)
        active_report = run is not None or self.run_report.started is not None
        if run is not None:
            summary["run"] = run
        elif active_report:
            # Preserve lifetime metrics in the API/JSON; display this sync's
            # cache and subsystem activity rather than earlier sync calls.
            activity = {
                "directories_processed",
                "directories_scanned_parallel",
                "directories_scanned_sequential",
                "directories_scanned_async",
                "async_metadata_checks",
                "html_cache_hits",
                "html_cache_misses",
                "parsed_listing_reuses",
                "html_cache_bypass_disabled",
                "html_cache_bypass_no_cache",
                "html_cache_bypass_refresh",
                "parse_time_seconds",
                "directory_signatures_loaded",
                "listing_fetches",
                "listing_get_seconds",
                "directory_signature_head_calls",
                "directory_signature_head_seconds",
                "directory_signatures_from_get",
                "cache_head_requests_saved",
                "etag_matches",
                "etag_mismatches",
                "etag_304_responses",
                "etag_unavailable",
                "http2_connections",
                "http11_fallbacks",
                "disk_space_warnings",
                "memory_pressure_events",
                "partial_downloads",
                "partial_resumes",
                "stale_partials_cleaned",
                "rate_limit_delays",
                "chunk_downloads",
                "chunk_assemblies",
                "chunk_failures",
                "chunk_retries",
                "parallel_files",
                "total_chunks",
                "fast_parses",
                "lxml_parses",
                "errors_recorded",
            }
            summary = {
                key: self.run_report.delta(summary, key) if key in activity else value
                for key, value in summary.items()
            }
        lines = [
            f"{prefix}METRICS SUMMARY:",
            f"{prefix}  Files downloaded: {summary['files_downloaded']}",
            f"{prefix}  Bytes downloaded: {format_bytes(summary['bytes_downloaded'])}",
            f"{prefix}  Files skipped: {summary['files_skipped']}",
            f"{prefix}  Files failed: {summary['files_failed']}",
            f"{prefix}  Directories processed: {summary['directories_processed']}",
        ]
        if active_report:
            lines = _format_run_summary(
                summary, summary["run"], prefix=prefix, known_urls=known_urls
            )
        # Scan mode metrics
        if summary["directories_scanned_parallel"] > 0:
            lines.append(f"{prefix}  Parallel scans: {summary['directories_scanned_parallel']}")
        if summary["directories_scanned_sequential"] > 0:
            lines.append(f"{prefix}  Sequential scans: {summary['directories_scanned_sequential']}")
        if summary["directories_scanned_async"] > 0:
            lines.append(f"{prefix}  Async scans: {summary['directories_scanned_async']}")

        # Async metadata checks
        if summary["async_metadata_checks"] > 0:
            lines.append(f"{prefix}  Async metadata checks: {summary['async_metadata_checks']}")

        # HTML cache metrics
        if summary["html_cache_hits"] > 0 or summary["html_cache_misses"] > 0:
            total_html = summary["html_cache_hits"] + summary["html_cache_misses"]
            html_hit_rate = (summary["html_cache_hits"] / total_html * 100) if total_html > 0 else 0
            lines.append(
                f"{prefix}  HTML listing cache: {summary['html_cache_hits']} hits, {summary['html_cache_misses']} misses ({html_hit_rate:.1f}% hits)"
            )
        lines.append(
            f"{prefix}  In-memory parsed listing reuses: {summary.get('parsed_listing_reuses', 0)}"
        )
        for key, label in (
            ("html_cache_bypass_disabled", "HTML cache disabled"),
            ("html_cache_bypass_no_cache", "--no-cache"),
            ("html_cache_bypass_refresh", "--refresh-cache"),
        ):
            if summary.get(key, 0):
                lines.append(f"{prefix}  Listing cache bypasses ({label}): {summary[key]}")

        # Adaptive async metrics
        if summary.get("adaptive_async_enabled"):
            lines.append(
                f"{prefix}  Adaptive async: concurrency={summary['adaptive_current_concurrency']}"
            )
        if summary.get("adaptive_fallback_to_sync"):
            lines.append(f"{prefix}  ⚠️ Fallback to sync: YES")

        # Parse metrics
        if summary["parse_time_seconds"] > 0:
            lines.append(
                f"{prefix}  Listing fetch/parse work (sum): {summary['parse_time_seconds']:.3f}s"
            )

        # Cache hit/miss metrics
        lines.append(
            f"{prefix}  Directory signatures loaded: {summary.get('directory_signatures_loaded', 0)}"
        )
        lines.extend(
            [
                f"{prefix}  Directory listing GET calls: {summary.get('listing_fetches', 0)} "
                f"({summary['listing_get_seconds']:.3f}s sum)",
                f"{prefix}  Directory signature HEAD calls: {summary['directory_signature_head_calls']} "
                f"({summary['directory_signature_head_seconds']:.3f}s sum)",
                f"{prefix}  Directory signatures reused from fresh GET: {summary['directory_signatures_from_get']}",
            ]
        )
        if summary["cache_head_requests_saved"] > 0:
            lines.append(f"{prefix}  HEAD requests saved: {summary['cache_head_requests_saved']}")
        lines.append(f"{prefix}  Cache signatures: {summary['cache_signatures']}")

        # ETag metrics
        if (
            summary["etag_matches"] > 0
            or summary["etag_mismatches"] > 0
            or summary["etag_304_responses"] > 0
            or summary["etag_unavailable"] > 0
        ):
            lines.extend(
                [
                    f"{prefix}  ETag matches: {summary['etag_matches']}",
                    f"{prefix}  ETag mismatches: {summary['etag_mismatches']}",
                    f"{prefix}  ETag 304 responses: {summary['etag_304_responses']}",
                    f"{prefix}  ETag unavailable: {summary['etag_unavailable']}",
                ]
            )

        # HTTP/2 metrics
        if summary["http2_connections"] > 0 or summary["http11_fallbacks"] > 0:
            lines.extend(
                [
                    f"{prefix}  HTTP/2 connections: {summary['http2_connections']}",
                    f"{prefix}  HTTP/1.1 fallbacks: {summary['http11_fallbacks']}",
                ]
            )

        # Cleanup preview metrics
        if summary.get("files_would_delete", 0) > 0:
            lines.extend(
                [
                    f"{prefix}  Files would delete (preview): {summary['files_would_delete']}",
                    f"{prefix}  Dirs would delete (preview): {summary['dirs_would_delete']}",
                ]
            )

        # RGET-LIST metric
        lines.append(f"{prefix}  RGET-LIST used: {summary['rget_list_used']}")

        # Download speed
        if not active_report:
            lines.append(
                f"{prefix}  Overall transfer average: {format_bytes(summary['download_speed'])}/s"
            )

        # Duration
        if not active_report:
            lines.append(f"{prefix}  Duration: {format_duration(summary['elapsed_seconds'])}")

        # Parser stats
        if active_report or summary.get("fast_parses", 0) > 0 or summary.get("lxml_parses", 0) > 0:
            lines.append(f"{prefix}  Fast parses: {summary['fast_parses']}")
            lines.append(f"{prefix}  LXML parses: {summary['lxml_parses']}")

        # NEW v2.0.0 metrics
        if summary.get("disk_space_warnings", 0) > 0:
            lines.append(f"{prefix}  Disk space warnings: {summary['disk_space_warnings']}")
        if summary.get("memory_pressure_events", 0) > 0:
            lines.append(f"{prefix}  Memory pressure events: {summary['memory_pressure_events']}")
        if summary.get("partial_downloads", 0) > 0:
            lines.append(f"{prefix}  Partial downloads: {summary['partial_downloads']}")
        if summary.get("partial_resumes", 0) > 0:
            lines.append(f"{prefix}  Partial resumes: {summary['partial_resumes']}")
        if summary.get("stale_partials_cleaned", 0) > 0:
            lines.append(f"{prefix}  Stale partials cleaned: {summary['stale_partials_cleaned']}")
        if summary.get("rate_limit_delays", 0) > 0:
            lines.append(f"{prefix}  Rate limit delays: {summary['rate_limit_delays']}")

        # NEW v3.0.0 metrics
        chunks_used = summary["chunk_mode_requested"] or summary.get("total_chunks", 0) > 0
        if chunks_used or summary.get("chunk_downloads", 0) > 0:
            lines.append(f"{prefix}  Chunk downloads: {summary['chunk_downloads']}")
        if chunks_used or summary.get("chunk_assemblies", 0) > 0:
            lines.append(f"{prefix}  Files finalized from chunks: {summary['chunk_assemblies']}")
        if summary.get("chunk_failures", 0) > 0:
            lines.append(f"{prefix}  Chunk failures: {summary['chunk_failures']}")
        if chunks_used or summary.get("parallel_files", 0) > 0:
            lines.append(f"{prefix}  Parallel files: {summary['parallel_files']}")
        if chunks_used:
            lines.append(f"{prefix}  Total chunks: {summary['total_chunks']}")
        peak = summary.get("run", {}).get("peak_parallel_chunks", summary["peak_parallel_chunks"])
        if chunks_used or peak:
            lines.append(f"{prefix}  Peak parallel chunks: {peak}")
        if summary.get("chunk_retries", 0) > 0:
            lines.append(f"{prefix}  Chunk retries: {summary['chunk_retries']}")

        # NEW v3.0.6: Auto-concurrency metrics
        if summary.get("auto_concurrency_enabled", False):
            lines.append(f"{prefix}  🤖 Auto-concurrency: enabled")
            if summary.get("auto_concurrency_adjustments", 0) > 0:
                lines.append(f"{prefix}    Adjustments: {summary['auto_concurrency_adjustments']}")
                lines.append(f"{prefix}    Final concurrency: {summary['auto_concurrency_final']}")

        # Errors
        error_count = summary.get("errors_recorded", 0) if active_report else len(summary["errors"])
        if error_count:
            lines.append(f"{prefix}  Errors: {error_count}")

        return "\n".join(lines)

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
                "timestamp": datetime.now().isoformat(),
                "version": __version__,
                "metrics": summary,
                "config": {
                    "base_url": sanitize_url_for_log(str(config.base_url)),
                    "workers": config.workers,
                    "async_metadata": config.async_metadata,
                    "parallel_downloads": config.parallel_downloads,
                },
            }
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w") as f:
                json.dump(export_data, f, indent=2)
            logging.info(f"📊 Metrics exported to JSON: {output_path}")
            return True
        except Exception as e:
            logging.warning(f"Failed to export metrics JSON: {e}")
            return False


def _format_run_summary(
    summary: dict, run: dict, *, prefix: str = "", known_urls: bool = False
) -> list[str]:
    """Render the same completion layout for mirror and known-URL runs."""
    lines = [f"{prefix}SUMMARY:", f"{prefix}  Result: {run['status'].upper()}"]
    found = "not performed" if known_urls else run["remote_files_found"]
    lines.append(f"{prefix}  Remote files found: {found}")
    if known_urls:
        lines.append(f"{prefix}  Files requested: {run['remote_files_found']}")
    lines.extend(
        [
            f"{prefix}  Selected paths: {run['selected_paths_found']}/{run['selected_paths_configured']} found in scope",
            f"{prefix}  Selected existing files checked: {run['selected_existing_files_checked']}",
            f"{prefix}  Existing files checked and current: {run['existing_files_current']}",
            f"{prefix}  Freshness checks skipped: {run['freshness_checks_skipped']}",
            f"{prefix}  Other files skipped: {run['other_files_skipped']}",
            f"{prefix}  Missing files downloaded: {run['missing_files_downloaded']}",
            f"{prefix}  Changed files downloaded: {run['changed_files_downloaded']}",
            f"{prefix}  Uncertain-freshness downloads: {run['uncertain_freshness_downloads']}",
            f"{prefix}  Files failed: {summary['files_failed']}",
            f"{prefix}  Obsolete files deleted: {summary.get('files_deleted', 0)}",
            f"{prefix}  Obsolete files moved: {summary.get('files_moved', 0)}",
            f"{prefix}  Downloaded bytes: {format_bytes(summary['bytes_downloaded'])}",
        ]
    )
    if run["status"] in ("dry run", "failed dry run"):
        planned = summary["files"] if known_urls else run["downloads_planned"]
        lines.append(f"{prefix}  Files that would be downloaded: {planned}")
    if known_urls or run["unclassified_downloads"]:
        label = "Existing files downloaded" if known_urls else "Other files downloaded"
        lines.append(f"{prefix}  {label}: {run['unclassified_downloads']}")
    if known_urls and run["status"] != "dry run":
        lines.append(f"{prefix}  Files downloaded: {summary['files_downloaded']}")
    hashing = run.get("verify_content")
    setting = (
        "enabled"
        if hashing
        else "disabled (use --verify-content)"
        if hashing is False
        else "not configured"
    )
    lines.append(f"{prefix}  SHA-256 receipt hashing: {setting}")
    lines.append(f"{prefix}  SHA-256 hashes recorded: {run.get('sha256_hashes_recorded', 'n/a')}")
    if run["metadata_checks_unresolved"]:
        lines.append(f"{prefix}  Unresolved freshness checks: {run['metadata_checks_unresolved']}")
    if run["reasons"]:
        lines.append(f"{prefix}  Result details: {run['reasons']}")
    mode, workers = run["discovery_mode"], run["discovery_workers"]
    strategy = f"{mode} ({workers} worker{'s' if workers != 1 else ''})" if mode else "n/a"
    discovery = f"{run['phase_seconds'].get('discovery', 0):.3f}s"
    if known_urls:
        strategy = discovery = "not performed"
    lines.extend(
        [
            f"{prefix}  Discovery mode: {strategy}",
            f"{prefix}  Directories examined: {run['directories_examined']}",
            f"{prefix}  Discovery elapsed time: {discovery}",
        ]
    )
    rate = run["discovery_throughput"]
    lines.append(
        f"{prefix}  Discovery throughput: {f'{rate:.1f} dirs/s' if rate is not None else 'n/a'}"
    )
    for phase in ("metadata", "download preparation", "downloads", "cleanup"):
        seconds = run["phase_seconds"].get(phase, None if known_urls else 0.0)
        duration = f"{seconds:.3f}s" if seconds is not None else "n/a"
        if known_urls and phase == "metadata":
            duration = "not performed"
        lines.append(f"{prefix}  {phase.capitalize()} time: {duration}")
    throughput = run["download_throughput"]
    speed = f"{format_bytes(throughput)}/s" if throughput is not None else "n/a"
    lines.append(f"{prefix}  Download throughput: {speed}")
    if "transfer_stage_seconds" in run:
        lines.append(f"{prefix}  Transfer stage times: worker sums; concurrent work overlaps")
        for stage, label in TRANSFER_STAGES:
            seconds = run["transfer_stage_seconds"].get(stage)
            value = f"{seconds:.3f}s" if seconds is not None else "not performed"
            lines.append(f"{prefix}  {label} (sum): {value}")
    elapsed = run["elapsed_seconds"]
    lines.append(f"{prefix}  Total duration: {f'{elapsed:.3f}s' if elapsed is not None else 'n/a'}")
    lines.append(f"{prefix}  Directories scanned: {summary['directories_processed']}")
    return lines


def format_download_summary(
    config: MirrorConfig,
    summary: dict,
    target: Path,
    run: Optional[dict] = None,
    *,
    metrics: Optional[MetricsCollector] = None,
) -> str:
    """Use the shared summary layout and measured known-URL coordinator phases."""
    provided_run = run is not None
    if run is None:
        # Older callers have only the JSON result, without per-phase observations.
        run = RunReport().snapshot()
        run.update(
            status="dry run"
            if summary.get("dry_run")
            else "success"
            if summary["success"]
            else "failed",
            elapsed_seconds=summary.get("elapsed_seconds"),
            duration_scope=summary.get("duration_scope", "operation"),
            remote_files_found=summary.get(
                "files", summary.get("files_downloaded", 0) + summary.get("files_failed", 0)
            ),
            missing_files_downloaded="n/a",
            unclassified_downloads="n/a",
            transfer_stage_seconds=summary.get("transfer_stage_seconds", {}),
            transfer_stage_operations=summary.get("transfer_stage_operations", {}),
        )
    run = dict(run, verify_content=config.verify_content)
    if "sha256_hashes_recorded" in summary:
        run["sha256_hashes_recorded"] = summary["sha256_hashes_recorded"]
    elif not provided_run:
        run["sha256_hashes_recorded"] = "n/a"
    metrics = metrics or MetricsCollector()
    # Mirror's renderer receives the same zero subsystem counters for work
    # this pipeline does not perform, plus the actual known-URL observations.
    values = {
        "files_downloaded": summary.get("files_downloaded", 0),
        "files_failed": summary.get("files_failed", 0),
        "bytes_downloaded": summary.get("bytes_downloaded", 0),
        "files": summary.get("files", 0),
    }
    chunks = summary.get("chunk_metrics", {})
    values.update({key: chunks[key] for key in CHUNK_METRICS if key in chunks})
    if "chunk_metrics" in summary:
        values["chunk_mode_requested"] = True
    if "peak_parallel_chunks" in chunks:
        run = dict(run, peak_parallel_chunks=chunks["peak_parallel_chunks"])
    return metrics.report(run=run, known_urls=True, values=values)


def log_run_start(
    *,
    mode: str,
    remote_root: str,
    destination: Optional[Path],
    log_path: Path,
    backend: str,
    verify_content: bool,
    suffixes: Sequence[str] = ("",),
    started_at: Optional[datetime] = None,
    prefix: str = "",
) -> None:
    """Log the same ordered startup fields for either mode and logging layout."""
    started_at = started_at or datetime.now()
    logging.info("%s%s", prefix, "-" * 80)
    logging.info("%sStarting MirrorURL v%s mode: %s", prefix, __version__, mode)
    logging.info("%sJob started at: %s", prefix, started_at.strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("%sCommand: %s", prefix, sanitize_command_line([sys.executable] + sys.argv))
    if len(suffixes) > 1:
        logging.info(
            "%sProcessing directory suffixes: %s",
            prefix,
            ", ".join(repr(s or "ROOT") for s in suffixes),
        )
    else:
        logging.info(
            "%sProcessing directory suffix: '%s'",
            prefix,
            suffixes[0] if suffixes and suffixes[0] else "ROOT",
        )
    logging.info(
        "%sRemote root: %s",
        prefix,
        sanitize_url_for_log(remote_root) if remote_root else "not configured",
    )
    logging.info(
        "%sDestination path: %s",
        prefix,
        destination if destination is not None else "not configured",
    )
    logging.info("%sLog file: %s", prefix, log_path)
    logging.info("%sBackend: %s", prefix, backend)
    logging.info(
        "%sSHA-256: %s", prefix, "enabled (local receipts)" if verify_content else "disabled"
    )
    logging.info("%s%s", prefix, "-" * 80)


def log_file_download_plan(files: int, workers: int, *, prefix: str = "") -> None:
    """Report selected files and admitted file-worker capacity with common wording."""
    logging.info("%sFile download plan: %s files; %s file workers", prefix, files, workers)


def log_whole_file_fallback(url: str, reason: str) -> None:
    """Explain an observed fallback without promising a fresh GET over a resume."""
    logging.info(
        "Whole-file fallback for %s: %s; using single-file transfer",
        sanitize_url_for_log(url),
        reason,
    )


def log_file_downloads_completed(downloaded: int, failed: int, *, prefix: str = "") -> None:
    """Report this finished batch's publication outcomes, including partial failure."""
    logging.info(
        "%sFile download batch completed: %s downloaded, %s failed", prefix, downloaded, failed
    )


def log_file_downloaded(path: Path, size: int, sha256: Optional[str] = None) -> None:
    """Report a published file once, including its computed local digest."""
    digest = f"; SHA-256: {sha256}" if sha256 is not None else ""
    logging.info("Downloaded: %s (%s)%s", path, format_bytes(size), digest)


def log_run_completion(success: bool, *, prefix: str = "", reasons: str = "") -> None:
    """Use one completion message for mirror and known-URL runs."""
    if success:
        logging.info("%sSync completed successfully", prefix)
    else:
        logging.warning("%sSync failed%s", prefix, f": {reasons}" if reasons else "")


def log_processed_result(success: bool, suffix: str, index: int, total: int) -> None:
    """Report the selected scope after its completed operation."""
    if success:
        logging.info("[%s/%s] ✅ Successfully processed: %s", index, total, suffix or "ROOT")
    else:
        logging.error("[%s/%s] ❌ Failed to process: %s", index, total, suffix or "ROOT")


def log_connection_pools_closed(count: int) -> None:
    """Report the number of pools that the caller actually closed."""
    logging.info("Closed all %s connection pools", count)


__all__ = [
    "MetricsCollector",
    "format_download_summary",
    "log_run_start",
    "log_file_download_plan",
    "log_whole_file_fallback",
    "log_file_downloads_completed",
    "log_file_downloaded",
    "log_run_completion",
    "log_processed_result",
    "log_connection_pools_closed",
]
