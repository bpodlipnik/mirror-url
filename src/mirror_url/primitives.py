"""Thread-safe concurrency primitives.

Migrated verbatim from ``mirror_url.py`` (orig. lines 1960-2342):
``LRUCache``, ``AtomicCounter``, ``AtomicSize``.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from functools import total_ordering
from threading import RLock
from typing import Any, Dict, Optional, Tuple


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
        self._timestamps: Dict[
            Any, float
        ] = {}  # FIX: Add separate timestamp dict for backward compatibility

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
            logging.warning(
                f"LRUCache '{self.name}': target_percent {target_percent} "
                f"out of range [0.0, 1.0], clamping to nearest bound."
            )
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
                    f"(removed {actual_evicted} items, target was {target_percent * 100:.0f}% of current)"
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
                "name": self.name,
                "size": len(self.cache),
                "maxsize": self.maxsize,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": f"{hit_rate:.1f}%",
                "evictions": self.evictions,
                "ttl_seconds": self.ttl,
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
                "current": self._value,
                "total_increments": self._total_increments,
                "total_decrements": self._total_decrements,
            }

    def __bool__(self) -> bool:
        """Support boolean context."""
        return self.value() != 0

    def __int__(self) -> int:
        """Convert to int."""
        return self.value()

    def __iadd__(self, other: int) -> AtomicCounter:
        """Support += operator."""
        self.increment(other)
        return self

    def __isub__(self, other: int) -> AtomicCounter:
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
                "current_bytes": self._size,
                "max_bytes": self._max_size,
                "total_adds": self._total_adds,
                "total_resets": self._total_resets,
            }

    def __bool__(self) -> bool:
        """Support boolean context."""
        return self.value() != 0

    def __int__(self) -> int:
        """Convert to int."""
        return self.value()

    def __iadd__(self, other: int) -> AtomicSize:
        """Support += operator."""
        self.add(other)
        return self


__all__ = ["LRUCache", "AtomicCounter", "AtomicSize"]
