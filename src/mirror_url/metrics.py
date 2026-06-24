"""Run-wide metrics aggregation.

Migrated verbatim from ``mirror_url.py`` (orig. lines 3523-3913): ``MetricsCollector``.
"""

from __future__ import annotations

import json
import logging
import statistics
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict

from ._version import __version__
from .constants import ADAPTIVE_START_CONCURRENCY
from .utils import format_bytes, format_duration, sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig


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
            # NEW v3.0.6 metrics
            "auto_concurrency_enabled": False,
            "auto_concurrency_adjustments": 0,
            "auto_concurrency_final": 0,
            "auto_concurrency_start": 0,
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
            self.metrics["bytes_downloaded"] += bytes_count

    def add_error(self, error: str, error_type: str = "unknown") -> None:
        """Add error to metrics - THREAD SAFE"""
        with self._times_lock:
            self._errors.append(
                {"timestamp": datetime.now().isoformat(), "type": error_type, "message": error}
            )

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
            with self.lock:
                self.metrics["parse_time_seconds"] += elapsed
                self.metrics["parse_times"].append(elapsed)
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

            # Calculate statistics safely
            if summary["request_times"]:
                summary["request_avg_ms"] = statistics.mean(summary["request_times"]) * 1000

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
                f"{prefix}  HTML cache hits: {summary['html_cache_hits']} ({html_hit_rate:.1f}%)"
            )

        # Adaptive async metrics
        if summary.get("adaptive_async_enabled"):
            lines.append(
                f"{prefix}  Adaptive async: concurrency={summary['adaptive_current_concurrency']}"
            )
        if summary.get("adaptive_fallback_to_sync"):
            lines.append(f"{prefix}  ⚠️ Fallback to sync: YES")

        # Parse metrics
        if summary["parse_time_seconds"] > 0:
            parse_speed = summary["directories_processed"] / summary["parse_time_seconds"]
            lines.append(f"{prefix}  Parse time: {summary['parse_time_seconds']:.2f}s")
            lines.append(f"{prefix}  Parse speed: {parse_speed:.1f} dirs/s")

        # Cache hit/miss metrics
        lines.append(f"{prefix}  Cache hits: {summary['cache_hits']}")
        lines.append(f"{prefix}  Cache misses: {summary['cache_misses']}")
        if summary["cache_head_requests_saved"] > 0:
            lines.append(f"{prefix}  HEAD requests saved: {summary['cache_head_requests_saved']}")
        lines.append(f"{prefix}  Cache signatures: {summary['cache_signatures']}")

        # ETag metrics
        if (
            summary["etag_matches"] > 0
            or summary["etag_mismatches"] > 0
            or summary["etag_304_responses"] > 0
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
        lines.append(f"{prefix}  Download speed: {format_bytes(summary['download_speed'])}/s")

        # Duration
        lines.append(f"{prefix}  Duration: {format_duration(summary['elapsed_seconds'])}")

        # Parser stats
        if summary.get("fast_parses", 0) > 0 or summary.get("lxml_parses", 0) > 0:
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
        if summary.get("chunk_downloads", 0) > 0:
            lines.append(f"{prefix}  Chunk downloads: {summary['chunk_downloads']}")
        if summary.get("chunk_assemblies", 0) > 0:
            lines.append(f"{prefix}  Chunk assemblies: {summary['chunk_assemblies']}")
        if summary.get("chunk_failures", 0) > 0:
            lines.append(f"{prefix}  Chunk failures: {summary['chunk_failures']}")
        if summary.get("parallel_files", 0) > 0:
            lines.append(f"{prefix}  Parallel files: {summary['parallel_files']}")
        if summary.get("total_chunks", 0) > 0:
            lines.append(f"{prefix}  Total chunks: {summary['total_chunks']}")

        # NEW v3.0.6: Auto-concurrency metrics
        if summary.get("auto_concurrency_enabled", False):
            lines.append(f"{prefix}  🤖 Auto-concurrency: enabled")
            if summary.get("auto_concurrency_adjustments", 0) > 0:
                lines.append(f"{prefix}    Adjustments: {summary['auto_concurrency_adjustments']}")
                lines.append(f"{prefix}    Final concurrency: {summary['auto_concurrency_final']}")

        # Errors
        if summary["errors"]:
            lines.append(f"{prefix}  Errors: {len(summary['errors'])}")

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


__all__ = ["MetricsCollector"]
