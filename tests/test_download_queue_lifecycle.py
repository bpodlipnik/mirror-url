"""Queue retrieval reserves each URL/path until its download completes."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from mirror_url.enums import DownloadPriority
from mirror_url.models import DownloadTask
from mirror_url.queue import DownloadQueue


def _task(name: str, priority: DownloadPriority = DownloadPriority.NORMAL) -> DownloadTask:
    return DownloadTask(f"https://example.test/{name}", Path(name), priority=priority)


def _retrieve(queue: DownloadQueue, batched: bool) -> DownloadTask:
    task = queue.get_batch(1)[0] if batched else queue.get()
    assert task is not None
    return task


@pytest.mark.parametrize("batched", [False, True], ids=["single", "batch"])
@pytest.mark.parametrize("success", [True, False], ids=["success", "failure"])
def test_identity_stays_reserved_until_completion(batched, success):
    queue = DownloadQueue()
    task = _task("file.dat")
    duplicate = _task("file.dat", DownloadPriority.HIGH)
    assert queue.add(task)
    assert not queue.add(duplicate)

    assert _retrieve(queue, batched) is task
    assert len(queue) == 0
    assert not queue.add(duplicate)
    stats = queue.get_stats()
    assert stats["active_tasks"] == 1
    assert stats["total_added"] == 1
    assert stats["total_completed"] == stats["total_failed"] == 0

    queue.complete(task, success=success)
    stats = queue.get_stats()
    assert stats["active_tasks"] == 0
    assert stats["total_completed"] == int(success)
    assert stats["total_failed"] == int(not success)
    assert queue.add(duplicate)
    assert queue.get() is duplicate
    queue.complete(duplicate)
    assert queue.get_stats()["active_tasks"] == 0


@pytest.mark.parametrize("max_batch", [2, 3, 10])
def test_batch_respects_priority_limit_and_outstanding_identities(max_batch):
    queue = DownloadQueue()
    low = _task("low", DownloadPriority.LOW)
    high1 = _task("high1", DownloadPriority.HIGH)
    normal = _task("normal")
    high2 = _task("high2", DownloadPriority.HIGH)
    tasks = [low, high1, normal, high2]
    for task in tasks:
        assert queue.add(task)

    batch = queue.get_batch(max_batch)
    assert batch == [high1, high2, normal, low][:max_batch]
    assert len(queue) == len(tasks) - len(batch)
    stats = queue.get_stats()
    assert stats["active_tasks"] == len(tasks)
    assert stats["high_priority"] == 0
    assert stats["normal_priority"] == int(max_batch == 2)
    assert stats["low_priority"] == int(max_batch < 4)
    assert all(not queue.add(task) for task in tasks)

    for index, task in enumerate(batch):
        queue.complete(task, success=index % 2 == 0)
    stats = queue.get_stats()
    assert stats["active_tasks"] == len(tasks) - len(batch)
    assert stats["total_completed"] == (len(batch) + 1) // 2
    assert stats["total_failed"] == len(batch) // 2
    assert queue.add(batch[0])


def test_mixed_retrieval_completion_releases_only_the_completed_identity():
    queue = DownloadQueue()
    tasks = [_task(str(index)) for index in range(3)]
    for task in tasks:
        assert queue.add(task)

    assert queue.get() is tasks[0]
    assert queue.get_batch(2) == tasks[1:]
    queue.complete(tasks[2], success=False)
    assert not queue.add(_task("0"))
    assert not queue.add(_task("1"))
    assert queue.add(_task("2"))
    stats = queue.get_stats()
    assert stats["size"] == 1
    assert stats["active_tasks"] == 3
    assert stats["total_failed"] == 1


@pytest.mark.parametrize("batched", [False, True], ids=["single", "batch"])
def test_inflight_identity_rejects_concurrent_readditions(batched):
    queue = DownloadQueue()
    task = _task("concurrent.dat")
    assert queue.add(task)
    assert _retrieve(queue, batched) is task

    with ThreadPoolExecutor(max_workers=8) as executor:
        accepted = list(executor.map(lambda _: queue.add(_task("concurrent.dat")), range(32)))
    assert not any(accepted)
    assert len(queue) == 0
    assert queue.get_stats()["total_added"] == 1
    queue.complete(task)
    assert queue.add(_task("concurrent.dat"))


def test_capacity_bounds_waiting_tasks_without_discarding_inflight_reservations():
    queue = DownloadQueue(max_size=1)
    first, second = _task("first"), _task("second")
    assert queue.add(first)
    assert not queue.add(second)
    assert queue.get() is first
    assert queue.add(second)
    assert queue.get_stats()["size"] == 1
    assert queue.get_stats()["active_tasks"] == 2
    queue.complete(first)
    assert queue.get() is second
    queue.complete(second)
    assert queue.get_stats()["active_tasks"] == 0


@pytest.mark.parametrize("max_batch", [0, -1])
def test_nonpositive_batch_limit_does_not_consume_or_release_tasks(max_batch):
    queue = DownloadQueue()
    task = _task("waiting")
    assert queue.add(task)
    assert queue.get_batch(max_batch) == []
    assert queue.get_stats()["size"] == queue.get_stats()["active_tasks"] == 1
    assert not queue.add(_task("waiting"))
    assert queue.get() is task
    queue.complete(task)


def test_empty_retrieval_does_not_change_metrics():
    queue = DownloadQueue(max_size=2)
    before = queue.get_stats()
    assert queue.get() is None
    assert queue.get_batch(4) == []
    assert queue.get_stats() == before
    assert before["size"] == before["active_tasks"] == before["total_added"] == 0
    assert before["total_completed"] == before["total_failed"] == 0


@pytest.mark.parametrize("different_part", ["url", "path"])
def test_task_identity_includes_both_url_and_local_path(different_part):
    queue = DownloadQueue()
    first = _task("original")
    second = DownloadTask(
        "https://example.test/other" if different_part == "url" else first.remote_url,
        Path("other") if different_part == "path" else first.local_path,
    )
    assert queue.add(first)
    assert queue.add(second)
    assert len(queue.get_batch(2)) == 2
    queue.complete(first)
    queue.complete(second)
    assert queue.get_stats()["active_tasks"] == 0
