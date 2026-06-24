"""On-disk backing stores.

Migrated verbatim from ``mirror_url.py`` (orig. lines 2348-2946):
``FileSystemCache`` and ``DiskBackedSet``.
"""

from __future__ import annotations

import logging
import os
import shutil
import time
import uuid
from pathlib import Path
from threading import RLock
from typing import Dict, List, Optional, Set, Tuple

from .constants import FS_CACHE_TTL_SECONDS, MEMORY_CACHE_MAX_SIZE
from .exceptions import CacheError, DiskSpaceError


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
                logging.debug(
                    f"FileSystemCache shrunk: {evicted} entries removed "
                    f"(stat: {len(self.stat_cache)}, exists: {len(self.exists_cache)})"
                )
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
            test_file = temp_dir / ".write_test"
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
        if hasattr(self.lock, "_is_owned"):
            assert self.lock._is_owned(), "_flush_buffer called without lock"

        # PROBLEM 1: Limit batch size to prevent memory issues
        MAX_BATCH_ITEMS = 100000
        MAX_BATCH_BYTES = 50 * 1024 * 1024  # 50MB max per batch

        items_to_write = self._write_buffer[:]

        # Truncate if too large (prevents memory bomb)
        if len(items_to_write) > MAX_BATCH_ITEMS:
            logging.warning(
                f"Batch size {len(items_to_write)} exceeds limit {MAX_BATCH_ITEMS}, truncating"
            )
            items_to_write = items_to_write[:MAX_BATCH_ITEMS]
            # Keep remaining items in buffer for next flush
            self._write_buffer = (
                self._write_buffer[MAX_BATCH_ITEMS:] + self._write_buffer[MAX_BATCH_ITEMS:]
            )
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
            cleaned = "".join(char for char in item if ord(char) >= 32 or char in "\t\r\n")
            if cleaned != item:
                logging.debug(f"Cleaned control characters from item: {item[:50]}...")

            # Escape newlines and backslashes for safe parsing
            cleaned = cleaned.replace("\\", "\\\\")
            cleaned = cleaned.replace("\n", "\\n")
            cleaned = cleaned.replace("\r", "\\r")
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
                total_bytes += len(item.encode("utf-8"))
            total_bytes += len(cleaned_items)  # newlines
            total_bytes += 1024  # filesystem overhead buffer

            usage = shutil.disk_usage(self.temp_dir)

            # Need 2x for safety (temp file + final file during rename)
            if usage.free < total_bytes * 2:
                logging.warning(
                    f"Low disk space for buffer flush: need ~{total_bytes / 1024 / 1024:.1f}MB, "
                    f"have {usage.free / 1024 / 1024:.1f}MB free"
                )
                # Put items back and try later
                self._write_buffer = items_to_write + self._write_buffer
                return

            # Also check if total_bytes exceeds warning threshold
            if total_bytes > MAX_BATCH_BYTES:
                logging.warning(
                    f"Batch size {total_bytes / 1024 / 1024:.1f}MB exceeds {MAX_BATCH_BYTES / 1024 / 1024:.1f}MB, "
                    f"consider increasing _buffer_max_size or reducing batch size"
                )
        except Exception as e:
            logging.debug(f"Disk space check failed, proceeding anyway: {e}")

        # PROBLEM 4: Atomic write with temp file
        batch_file = self.temp_dir / f"batch_{uuid.uuid4().hex}.txt"
        temp_file = batch_file.with_suffix(".tmp")

        try:
            # Write to temp file first (atomic)
            with open(temp_file, "w", encoding="utf-8", errors="replace") as f:
                # Write in chunks to avoid memory issues for huge batches
                CHUNK_SIZE = 10000
                for i in range(0, len(cleaned_items), CHUNK_SIZE):
                    chunk = cleaned_items[i : i + CHUNK_SIZE]
                    f.write("\n".join(chunk))
                    if i + CHUNK_SIZE < len(cleaned_items):
                        f.write("\n")  # Add newline between chunks
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
                raise OSError("Temp file is empty after write")

            # Atomic rename
            os.replace(str(temp_file), str(batch_file))

            # Verify final file exists and has content
            if not batch_file.exists() or batch_file.stat().st_size == 0:
                raise OSError("Final file missing or empty after rename")

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
            if isinstance(e, (OSError, IOError)) and getattr(e, "errno", 0) in (28, 122):  # ENOSPC
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
        partial_path = final_path.with_suffix(".partial")

        try:
            # Write in chunks to avoid OOM with large sets
            with open(partial_path, "w", encoding="utf-8") as f:
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
            logging.debug(
                f"Pruned {removed_count} old/empty disk files, {len(self.disk_files)} remaining"
            )

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
                with open(staging_file, "w", encoding="utf-8") as f:
                    for item in sorted(items_to_flush):
                        safe_item = str(item).replace("\\", "\\\\")
                        safe_item = safe_item.replace("\n", "\\n")
                        safe_item = safe_item.replace("\r", "\\r")
                        f.write(f"{safe_item}\n")
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except (OSError, AttributeError):
                        pass

                # Append to existing disk file
                with open(current_file, "a", encoding="utf-8") as f:
                    with open(staging_file, encoding="utf-8") as sf:
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


__all__ = ["FileSystemCache", "DiskBackedSet"]
