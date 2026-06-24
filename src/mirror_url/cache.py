"""High-level cache manager.

Migrated verbatim from ``mirror_url.py`` (orig. lines 7942-8350): ``CacheManager``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from ._version import __version__
from .constants import (
    CACHE_SCHEMA_VERSION,
    MAX_CACHE_METADATA_ENTRIES,
    MAX_HTML_CACHE_SIZE,
)
from .enums import MemoryPressure
from .primitives import LRUCache
from .utils import _validate_and_sanitize_cache, sanitize_url_for_log

if TYPE_CHECKING:  # pragma: no cover - typing only, avoids an import cycle
    from .config import MirrorConfig
    from .metrics import MetricsCollector


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
            name="html_cache",
        )
        self.html_cache_lock = RLock()
        self.lru_file_cache = LRUCache(
            maxsize=MAX_CACHE_METADATA_ENTRIES,
            ttl_seconds=self.config.cache_max_age * 86400,
            name="file_metadata",
        )
        logging.debug(f"CacheManager initialized: {cache_file}, max_age={config.cache_max_age}d")

    def load(
        self, _recursion_depth: int = 0, _backup_attempts: int = 0
    ) -> Tuple[bool, Optional[Dict]]:
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
            with open(self.cache_file, encoding="utf-8") as f:
                raw_data = json.load(f)
                # ✅ REPLACE _clean_json_keys(raw_data) with strict validation
                data = _validate_and_sanitize_cache(raw_data)
                self.metadata = data.get("_meta", {})
                # ✅ INSERT VERSION CHECK HERE
                cache_schema = self.metadata.get("version")
                if isinstance(cache_schema, int) and cache_schema != CACHE_SCHEMA_VERSION:
                    logging.warning(
                        f"Cache schema mismatch: file has v{cache_schema}, expected v{CACHE_SCHEMA_VERSION}. "
                        f"Discarding and rebuilding cache."
                    )
                    return False, None  # Forces full rebuild
                self.cache_data = {k: v for k, v in data.items() if not k.startswith("_")}
                self.dir_signatures = self.metadata.get("dir_signatures", {})
                if "_files" in data:
                    self.file_metadata_cache = data["_files"]
                    self.lru_file_cache.put_batch(self.file_metadata_cache)
                    if len(self.file_metadata_cache) > MAX_CACHE_METADATA_ENTRIES:
                        logging.warning(
                            f"Pruning file metadata cache from {len(self.file_metadata_cache)} to {MAX_CACHE_METADATA_ENTRIES} entries"
                        )
                        items = list(self.file_metadata_cache.items())[-MAX_CACHE_METADATA_ENTRIES:]
                        self.file_metadata_cache = dict(items)
                        logging.debug(
                            f"Loaded {len(self.file_metadata_cache)} file metadata entries"
                        )
                if "_meta" in data and "last_full_run" in data["_meta"]:
                    try:
                        last_run = datetime.fromisoformat(data["_meta"]["last_full_run"])
                        age = datetime.now() - last_run
                        age_days = age.total_seconds() / 86400
                        self.metrics.metrics["cache_age_days"] = age_days
                        if age_days > self.config.cache_max_age:
                            logging.info(
                                f"Cache is {age_days:.1f} days old (> {self.config.cache_max_age}d) — refreshing"
                            )
                            self.metrics.set_cache_refreshed(age_days)
                            return False, None
                        logging.info(
                            f"Cache age: {age_days:.1f} days (max: {self.config.cache_max_age}d)"
                        )
                    except (ValueError, KeyError) as e:
                        logging.warning(f"Invalid cache metadata: {e}")
                        return False, None
                dir_count = len(self.cache_data)
                self.metrics.set_cache_signatures(dir_count)
                if "file_count" in self.metadata:
                    logging.info(
                        f"📦 Cache contains {dir_count} directories with {self.metadata['file_count']} files"
                    )
                else:
                    logging.info(f"📦 Cache contains {dir_count} directories")
                return True, self.cache_data
        except json.JSONDecodeError as e:
            logging.error(f"Corrupted cache file {self.cache_file}: {e}")
            self.metrics.add_error(f"Cache corruption: {e}", "cache_corruption")
            self.metrics.increment("cache_corruptions")
            backup_path = self.cache_file.with_suffix(f".json.corrupted.{int(time.time())}")
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
                    logging.warning(
                        f"Deleted corrupted cache file (backup failed): {self.cache_file}"
                    )
                except FileNotFoundError:
                    pass
                except Exception as delete_error:
                    logging.error(f"Failed to delete corrupted cache: {delete_error}")
            if backup_success:
                old_backup = self._find_oldest_valid_backup()
                if old_backup:
                    try:
                        with open(old_backup) as f:
                            json.load(f)
                        old_backup.rename(self.cache_file)
                        logging.info(f"Restored cache from older backup: {old_backup}")
                        return self.load(_recursion_depth + 1, _backup_attempts + 1)
                    except json.JSONDecodeError as validate_error:
                        logging.error(f"Restored backup is also corrupted: {validate_error}")
                        self.metrics.add_error(
                            f"Backup validation failed: {validate_error}", "cache_backup_corrupted"
                        )
                        return False, None
                    except Exception as restore_error:
                        logging.error(f"Failed to restore backup: {restore_error}")
                        self.metrics.add_error(
                            f"Backup restore failed: {restore_error}", "cache_backup_restore_failed"
                        )
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
                "_meta": {
                    "version": CACHE_SCHEMA_VERSION,  # ✅ Use schema version constant
                    "schema": "mirrorurl_v3_cache",
                    "last_full_run": datetime.now().isoformat(),
                    "version_code": __version__,
                    "file_count": file_count,
                    "directory_count": len(directories),
                    "dir_signatures": self.dir_signatures,
                    "config": {
                        "base_url": sanitize_url_for_log(str(self.config.base_url)),
                        "dir_suffix": self.config.dir_suffix,
                        "cache_max_age": self.config.cache_max_age,
                        "parallel_downloads": self.config.parallel_downloads,
                    },
                }
            }

            cache_data.update(directories)
            if self.file_metadata_cache:
                cache_data["_files"] = self.file_metadata_cache

            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.cache_file.with_suffix(".json.tmp")

            # ✅ Strict formatting: no trailing spaces, consistent separators
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, indent=2, ensure_ascii=False, separators=(",", ": "))
                f.flush()
                os.fsync(f.fileno())

            temp_file.rename(self.cache_file)
            self.metrics.set_cache_signatures(len(directories))
            logging.info(
                f"💾 Saved cache v2 with {len(directories)} directory signatures, {file_count} files"
            )
            return True
        except Exception as e:
            logging.warning(f"Failed to save cache: {e}")
            self.metrics.add_error(str(e), "cache_save")
            temp_file = self.cache_file.with_suffix(".json.tmp")
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
            self.metrics.increment("html_cache_misses")
            return None

        # FIX: Handle multiple possible return types
        if isinstance(entry, dict):
            files = entry.get("files", [])
            subdirs = entry.get("subdirs", [])
        elif isinstance(entry, tuple) and len(entry) == 2:
            # Old format: (files, subdirs)
            files, subdirs = entry
        elif isinstance(entry, list) and len(entry) == 2:
            # List format
            files, subdirs = entry
        else:
            # Unknown format, treat as cache miss
            self.metrics.increment("html_cache_misses")
            return None

        self.metrics.increment("html_cache_hits")
        return (files, subdirs)

    def set_html_cache(
        self, url: str, files: List[str], subdirs: List[str], content_hash: str = None
    ) -> None:
        """Cache HTML parse results."""
        if not self.config.cache_html:
            return

        if content_hash is None:
            content_hash = hashlib.sha256(str(files + subdirs).encode()).hexdigest()

        cache_entry = {
            "files": files,
            "subdirs": subdirs,
            "content_hash": content_hash,
            "timestamp": time.time(),
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
                self.metrics.increment("cache_invalidated_dirs")

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
        data = {"etag": etag, "mtime": mtime, "size": size, "updated": datetime.now().isoformat()}
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
            backups = list(self.cache_file.parent.glob(f"{self.cache_file.stem}.json.corrupted.*"))
            if not backups:
                return None
            backups.sort(key=lambda p: p.stat().st_mtime)
            for backup in backups:
                try:
                    with open(backup) as f:
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


__all__ = ["CacheManager"]
