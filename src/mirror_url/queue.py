"""Priority download queue.

Migrated verbatim from ``mirror_url.py`` (orig. lines 3400-3518): ``DownloadQueue``.
"""

from __future__ import annotations

from collections import deque
from threading import RLock
from typing import Any, Deque, Dict, List, Optional, Set

from .enums import DownloadPriority
from .models import DownloadTask


class DownloadQueue:
    """Priority queue with URL/path reservations and completion metrics.

    ``active_tasks`` includes both waiting and in-flight tasks. Retrieval
    removes tasks from the waiting queue but keeps their identities reserved
    until ``complete()`` is called. Complete each retrieved task exactly once,
    on either success or failure, and keep its URL/path unchanged until then.
    ``max_size`` bounds waiting tasks, not in-flight downloads.
    """

    def __init__(self, max_size: int = 1000):
        """
        Initialize download queue.

        Args:
            max_size: Maximum number of waiting tasks
        """
        self.max_size = max_size
        self.queues: Dict[DownloadPriority, Deque[DownloadTask]] = {
            DownloadPriority.HIGH: deque(),
            DownloadPriority.NORMAL: deque(),
            DownloadPriority.LOW: deque(),
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
            True if added successfully. A URL/path already waiting or in flight
            is rejected until its earlier task is completed.
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

        The task remains reserved until ``complete()`` is called.

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

        Each returned task remains reserved until ``complete()`` is called.

        Args:
            max_batch: Maximum number of tasks to get

        Returns:
            List of tasks
        """
        with self.lock:
            tasks: List[DownloadTask] = []
            for priority in [DownloadPriority.HIGH, DownloadPriority.NORMAL, DownloadPriority.LOW]:
                while len(tasks) < max_batch and self.queues[priority]:
                    task = self.queues[priority].popleft()
                    tasks.append(task)
                if len(tasks) >= max_batch:
                    break
            return tasks

    def complete(self, task: DownloadTask, success: bool = True) -> None:
        """
        Mark task as complete.

        Call exactly once for a retrieved task, on either success or failure.
        This releases its URL/path reservation so the task can be added again.

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
        """Get the number of waiting tasks, excluding in-flight downloads."""
        with self.lock:
            return sum(len(q) for q in self.queues.values())

    def get_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.

        ``size`` counts waiting tasks; ``active_tasks`` counts all reserved
        identities, including in-flight downloads awaiting completion.

        Returns:
            Dictionary with queue statistics
        """
        with self.lock:
            return {
                "size": len(self),
                "max_size": self.max_size,
                "active_tasks": len(self.active_tasks),
                "total_added": self.total_added,
                "total_completed": self.total_completed,
                "total_failed": self.total_failed,
                "high_priority": len(self.queues[DownloadPriority.HIGH]),
                "normal_priority": len(self.queues[DownloadPriority.NORMAL]),
                "low_priority": len(self.queues[DownloadPriority.LOW]),
            }


__all__ = ["DownloadQueue"]
