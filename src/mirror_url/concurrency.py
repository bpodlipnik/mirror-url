"""Unified sync/async concurrency coordinator.

Migrated verbatim from ``mirror_url.py`` (orig. lines 5992-6255): ``UnifiedConcurrencyManager``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any, Dict, Optional

from .constants import (
    MONITOR_INTERVAL_SECONDS,
    UNIFIED_MAX_ASYNC_TASKS,
    UNIFIED_MAX_TOTAL_THREADS,
    UNIFIED_QUEUE_SIZE,
    UNIFIED_THREAD_POOL_SHARED,
)
from .enums import ConcurrencyType
from .exceptions import ConcurrencyLimitError


class UnifiedConcurrencyManager:
    """
    Unified concurrency control for all operations.

    This manages thread pools, async tasks, and chunk downloads to ensure
    system resources are not exhausted.
    """

    def __init__(
        self,
        max_total_threads: int = UNIFIED_MAX_TOTAL_THREADS,
        max_async_tasks: int = UNIFIED_MAX_ASYNC_TASKS,
        queue_size: int = UNIFIED_QUEUE_SIZE,
    ):
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
        # Interruptible wait for the monitor loop: lets shutdown() wake it
        # immediately instead of waiting out the rest of a sleep() interval
        # (see _monitor_loop / shutdown).
        self._shutdown_event = threading.Event()

    def start(self) -> None:
        """Start the concurrency manager and monitoring."""
        if self.shared_pool_enabled and not self.shared_pool:
            self.shared_pool = ThreadPoolExecutor(
                max_workers=self.max_total_threads, thread_name_prefix="mirror_shared"
            )

        self.monitor_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

        logging.debug(
            f"UnifiedConcurrencyManager started: max_threads={self.max_total_threads}, "
            f"max_async={self.max_async_tasks}"
        )

    def shutdown(self) -> None:
        """Shutdown the concurrency manager with proper resource cleanup."""
        logging.debug("Shutting down UnifiedConcurrencyManager...")

        # Set shutdown flags
        self._shutdown = True
        self.monitor_running = False
        # Wake the monitor loop immediately instead of leaving it to finish
        # its current sleep(MONITOR_INTERVAL_SECONDS) -- otherwise the
        # 5s join() below times out (and warns) whenever shutdown happens
        # more than 5s before the loop's next scheduled wake-up.
        self._shutdown_event.set()

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
                self.max_concurrent_reached = max(self.max_concurrent_reached, self.active_threads)
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

        except Exception:
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
        except Exception:
            with self.thread_lock:
                self.total_failed += 1
            raise

    def _task_done(self, future):
        """Called when a task completes."""
        self.release_thread()

    def _monitor_loop(self) -> None:
        """Monitor loop for concurrency statistics."""
        while self.monitor_running and not self._shutdown:
            # wait() returns True immediately if shutdown() sets the event,
            # instead of blocking for the full interval like time.sleep() did.
            if self._shutdown_event.wait(timeout=MONITOR_INTERVAL_SECONDS):
                break

            with self.thread_lock:
                active = self.active_threads
                pending = self.pending_operations
                total_sub = self.total_submitted
                total_comp = self.total_completed
                max_conc = self.max_concurrent_reached

            if active > self.max_total_threads * 0.9:
                logging.warning(
                    f"High concurrency: {active}/{self.max_total_threads} threads active, "
                    f"{pending} pending"
                )

            logging.debug(
                f"Concurrency stats: active={active}, pending={pending}, "
                f"submitted={total_sub}, completed={total_comp}, "
                f"max_concurrent={max_conc}"
            )

    def get_stats(self) -> Dict[str, Any]:
        """Get concurrency manager statistics."""
        with self.thread_lock:
            return {
                "active_threads": self.active_threads,
                "max_threads": self.max_total_threads,
                "pending_operations": self.pending_operations,
                "total_submitted": self.total_submitted,
                "total_completed": self.total_completed,
                "total_failed": self.total_failed,
                "max_concurrent_reached": self.max_concurrent_reached,
                "shared_pool_enabled": self.shared_pool_enabled,
                "async_semaphore_limit": self.max_async_tasks,
            }


__all__ = ["UnifiedConcurrencyManager"]
