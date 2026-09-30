"""Async objects must be constructible without a current event loop (Python 3.9)."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from mirror_url.async_connection import AdaptiveAsyncManager, AsyncTaskManager
from mirror_url.circuit_breaker import AsyncCircuitBreaker
from mirror_url.concurrency import UnifiedConcurrencyManager
from mirror_url.config import MirrorConfig
from mirror_url.metrics import MetricsCollector
from mirror_url.transport import SecureAsyncTransport

KINDS = ["tasks", "adaptive", "transport", "concurrency", "circuit"]


def make_manager(kind, tmp_path):
    if kind == "tasks":
        return AsyncTaskManager()
    if kind == "adaptive":
        config = MirrorConfig(
            base_url="https://example.com/root/",
            dest_path=tmp_path / "dest",
            log_path=tmp_path / "logs",
            security_validation=False,
            http2=False,
        )
        return AdaptiveAsyncManager(config, MetricsCollector())
    if kind == "transport":
        return SecureAsyncTransport()
    if kind == "concurrency":
        return UnifiedConcurrencyManager(max_async_tasks=2)
    return AsyncCircuitBreaker(failure_threshold=1)


async def exercise_manager(kind, manager):
    if kind == "tasks":
        task = await manager.create_task(asyncio.sleep(0, result=42))
        assert await task == 42
        await manager.shutdown()
    elif kind == "adaptive":
        await asyncio.gather(manager._init_client(), manager._init_client())
        assert manager._client_initialized
        assert manager._semaphore is not None
        await manager.__aexit__(None, None, None)
    elif kind == "transport":
        await manager._cache_ip("example.com", "8.8.8.8")
        assert await manager._get_cached_ip("example.com") == "8.8.8.8"
        await manager.clear_ip_cache()
        assert await manager._get_cached_ip("example.com") is None
        await manager.aclose()
    elif kind == "concurrency":
        async with manager.acquire_async():
            assert manager.acquire_async() is manager.async_semaphore
        manager.shutdown()
    else:
        assert await manager.can_execute()
        await manager.record_failure()
        assert not await manager.can_execute()


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("placement", ["after_run", "worker_thread"])
def test_construct_without_event_loop_then_use_in_asyncio_run(kind, placement, tmp_path):
    def construct():
        manager = make_manager(kind, tmp_path)
        # Construction must not create/install an unrelated event loop.
        with pytest.raises(RuntimeError, match="no current event loop"):
            asyncio.get_event_loop()
        return manager

    if placement == "after_run":
        asyncio.run(asyncio.sleep(0))
        manager = construct()
    else:
        with ThreadPoolExecutor(max_workers=1) as pool:
            manager = pool.submit(construct).result()
    asyncio.run(exercise_manager(kind, manager))


def test_async_capacity_is_shared_and_can_rebind_after_loop_closes():
    asyncio.run(asyncio.sleep(0))
    manager = UnifiedConcurrencyManager(max_async_tasks=2)

    async def check_capacity():
        active = 0
        peak = 0

        async def work():
            nonlocal active, peak
            async with manager.acquire_async():
                active += 1
                peak = max(peak, active)
                await asyncio.sleep(0.005)
                active -= 1

        await asyncio.gather(*(work() for _ in range(6)))
        assert peak == 2
        assert active == 0

    try:
        asyncio.run(check_capacity())
        asyncio.run(check_capacity())
    finally:
        manager.shutdown()


def test_transport_cache_can_be_used_in_successive_closed_loops():
    transport = SecureAsyncTransport()

    async def cache_address(address):
        await asyncio.gather(*(transport._cache_ip("example.com", address) for _ in range(6)))
        assert await transport._get_cached_ip("example.com") == address

    try:
        asyncio.run(cache_address("8.8.8.8"))
        asyncio.run(cache_address("8.8.4.4"))
    finally:
        asyncio.run(transport.aclose())


def test_async_capacity_cannot_be_bypassed_using_another_open_loop():
    manager = UnifiedConcurrencyManager(max_async_tasks=1)

    async def competing_loop():
        async with manager.acquire_async():
            pytest.fail("Another open loop bypassed the shared task limit")

    async def hold_capacity():
        async with manager.acquire_async():
            with pytest.raises(RuntimeError, match="across open event loops"):
                await asyncio.to_thread(asyncio.run, competing_loop())
            assert manager.async_semaphore.locked()

    try:
        asyncio.run(hold_capacity())
    finally:
        manager.shutdown()
