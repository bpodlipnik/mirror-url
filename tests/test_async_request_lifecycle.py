"""Async HTTP retries, redirect scope, cancellation, and live admission limits."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from mirror_url import MirrorConfig
from mirror_url.async_connection import (
    AdaptiveAsyncManager,
    AsyncConnectionManager,
    AsyncTaskManager,
)
from mirror_url.async_primitives import ResizableSemaphore
from mirror_url.constants import ADAPTIVE_RTT_THRESHOLD_MS
from mirror_url.metrics import MetricsCollector

BASE = "https://example.com/root/"
pytestmark = pytest.mark.asyncio


@pytest.fixture(params=[AsyncConnectionManager, AdaptiveAsyncManager], ids=["fixed", "adaptive"])
def manager(request, tmp_path, monkeypatch):
    config = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        security_validation=False,
        max_retries=2,
        async_workers=3,
        adaptive_start_concurrency=3,
        circuit_breaker_enabled=False,
    )
    monkeypatch.setattr("mirror_url.async_connection.asyncio.sleep", AsyncMock())
    monkeypatch.setattr(
        "mirror_url.async_connection.get_domain_health_tracker",
        lambda: SimpleNamespace(is_throttled=lambda _: False),
    )
    return request.param(config, MetricsCollector())


def install_client(manager, handler):
    manager._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    manager._semaphore = (
        ResizableSemaphore(3) if isinstance(manager, AdaptiveAsyncManager) else asyncio.Semaphore(3)
    )
    if isinstance(manager, AdaptiveAsyncManager):
        manager._client_initialized = True
    return manager._client


@pytest.mark.parametrize("failure", ["timeout", "connect", "read", "503", "429"])
async def test_transient_failure_retries_and_preserves_conditional_headers(manager, failure):
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) == 1:
            if failure in ("503", "429"):
                return httpx.Response(int(failure), headers={"Retry-After": "2"})
            error = {
                "timeout": httpx.ReadTimeout,
                "connect": httpx.ConnectError,
                "read": httpx.ReadError,
            }[failure]
            raise error("transient network failure", request=request)
        return httpx.Response(304, headers={"ETag": '"v1"'})

    client = install_client(manager, handle)
    try:
        result = await manager.head(BASE + "a", {"If-None-Match": '"v1"'})
        assert result.status_code == 304
        assert len(requests) == 2
        assert all(request.headers["if-none-match"] == '"v1"' for request in requests)
        assert manager.metrics.metrics["async_metadata_checks"] == 1
        if failure in ("429", "503"):
            assert asyncio.sleep.await_args.args[0] >= 2
    finally:
        await client.aclose()


@pytest.mark.parametrize("failure", [httpx.ReadTimeout, httpx.ConnectError, httpx.ReadError])
async def test_retry_exhaustion_returns_failure_and_records_error(manager, failure):
    requests = []

    def handle(request):
        requests.append(request)
        raise failure("persistent failure", request=request)

    client = install_client(manager, handle)
    try:
        assert await manager.head(BASE + "a") is None
        assert len(requests) == 3
        assert manager.metrics.metrics["async_metadata_checks"] == 0
        assert manager.metrics.get_summary()["errors"]
    finally:
        await client.aclose()


@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_client_errors_are_not_retried_and_responses_are_closed(manager, status):
    responses = []

    def handle(request):
        result = httpx.Response(status)
        responses.append(result)
        return result

    client = install_client(manager, handle)
    try:
        assert await manager.head(BASE + "a") is None
        assert len(responses) == 1
        assert responses[0].is_closed
        assert manager.metrics.metrics["async_metadata_checks"] == 0
    finally:
        await client.aclose()


async def test_unexpected_transport_error_fails_without_retry(manager):
    calls = []

    def handle(request):
        calls.append(request)
        raise ValueError("malformed transport response")

    client = install_client(manager, handle)
    try:
        assert await manager.head(BASE + "a") is None
        assert len(calls) == 1
        assert manager.metrics.get_summary()["errors"]
    finally:
        await client.aclose()


async def test_scoped_redirect_chain_keeps_headers_and_closes_intermediate_responses(manager):
    requests, responses = [], []

    def handle(request):
        requests.append(request)
        response = (
            httpx.Response(307, headers={"Location": "b"})
            if request.url.path.endswith("/a")
            else httpx.Response(200)
        )
        responses.append(response)
        return response

    client = install_client(manager, handle)
    try:
        assert (await manager.head(BASE + "a", {"X-Test": "kept"})).status_code == 200
        assert [str(request.url) for request in requests] == [BASE + "a", BASE + "b"]
        assert all(request.headers["x-test"] == "kept" for request in requests)
        assert responses[0].is_closed
    finally:
        await client.aclose()


@pytest.mark.parametrize("target", ["https://evil.example/root/a", "/outside/a", "../outside/a"])
async def test_redirect_scope_violation_never_fetches_target(manager, target):
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(302, headers={"Location": target})

    client = install_client(manager, handle)
    try:
        assert await manager.head(BASE + "a") is None
        assert len(requests) == 1
    finally:
        await client.aclose()


async def test_redirect_loop_is_bounded(manager):
    responses = []

    def handle(request):
        response = httpx.Response(302, headers={"Location": "a"})
        responses.append(response)
        return response

    client = install_client(manager, handle)
    try:
        assert await manager.head(BASE + "a") is None
        assert len(responses) == 11
        assert all(response.is_closed for response in responses)
    finally:
        await client.aclose()


async def test_dns_cache_reuses_fresh_lookup_and_refreshes_expired_entry(manager, monkeypatch):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr("mirror_url.async_connection.socket.gethostbyname", resolver)
    limiter = SimpleNamespace(async_wait=AsyncMock())
    manager.rate_limiter = limiter
    client = install_client(manager, lambda _: httpx.Response(200))
    try:
        assert await manager.head(BASE + "a")
        assert await manager.head(BASE + "b")
        assert resolver.call_count == 1
        manager._dns_cache["example.com"] = ("old", 0)
        assert await manager.head(BASE + "c")
        assert resolver.call_count == 2
        assert limiter.async_wait.await_count == 3
        assert limiter.async_wait.await_args.args == ("8.8.8.8",)
    finally:
        await client.aclose()


async def test_dns_failure_does_not_disable_metadata_checks(manager, monkeypatch):
    monkeypatch.setattr(
        "mirror_url.async_connection.socket.gethostbyname",
        Mock(side_effect=OSError("DNS unavailable")),
    )
    manager.rate_limiter = SimpleNamespace(async_wait=AsyncMock())
    client = install_client(manager, lambda _: httpx.Response(200))
    try:
        assert (await manager.head(BASE + "a")).status_code == 200
        manager.rate_limiter.async_wait.assert_not_awaited()
    finally:
        await client.aclose()


async def test_cancellation_releases_request_permit(manager):
    started = asyncio.Event()
    hold = asyncio.Event()

    async def handle(request):
        started.set()
        await hold.wait()
        return httpx.Response(200)

    client = install_client(manager, handle)
    try:
        task = asyncio.create_task(manager.head(BASE + "a"))
        await asyncio.wait_for(started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        hold.set()
        assert (await asyncio.wait_for(manager.head(BASE + "b"), 1)).status_code == 200
        assert manager._semaphore._value == 3
    finally:
        await client.aclose()


async def test_live_request_limit_bounds_simultaneous_work(manager):
    entered = asyncio.Event()
    release = asyncio.Event()
    active, peak = 0, 0

    async def handle(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        if active == 3:
            entered.set()
        try:
            await release.wait()
            return httpx.Response(200)
        finally:
            active -= 1

    client = install_client(manager, handle)
    tasks = [asyncio.create_task(manager.head(BASE + str(i))) for i in range(9)]
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert peak == 3
        release.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), 2)
        assert all(result.status_code == 200 for result in results)
        assert peak == 3
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.aclose()


async def test_client_initialization_failure_is_reported(manager, monkeypatch):
    monkeypatch.setattr(
        "mirror_url.async_connection.httpx.AsyncClient",
        Mock(side_effect=OSError("cannot initialize")),
    )
    assert await manager.head(BASE + "a") is None
    assert manager.metrics.get_summary()["errors"]


async def test_context_exit_closes_client_and_rejects_reentry(manager):
    client = install_client(manager, lambda _: httpx.Response(200))
    await manager.__aexit__(None, None, None)
    assert client.is_closed
    assert not manager.is_available()
    with pytest.raises(RuntimeError):
        await manager.__aenter__()


@pytest.mark.parametrize("failures", [False, True])
async def test_warmup_caps_requests_and_tolerates_failed_connections(manager, failures):
    requests = []

    def handle(request):
        requests.append(request)
        if failures and len(requests) % 2:
            raise asyncio.TimeoutError("connection warmup timeout")
        return httpx.Response(200)

    install_client(manager, handle)
    if isinstance(manager, AdaptiveAsyncManager):
        manager._profile_complete = True
    try:
        await manager.warm_up([BASE + str(i) for i in range(15)])
        assert len(requests) == 10
        await manager.warm_up([])
        assert len(requests) == 10
    finally:
        await manager.__aexit__(None, None, None)


async def test_task_manager_returns_results_and_cancels_pending_work():
    manager = AsyncTaskManager()
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def forever():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    pending = await manager.create_task(forever())
    await asyncio.wait_for(started.wait(), 1)
    completed = await manager.create_task(asyncio.sleep(0, result=42))
    assert await completed == 42
    await manager.shutdown(timeout=0.01)
    assert pending.cancelled()
    assert cancelled.is_set()
    assert manager.get_stats()["active_tasks"] == 0


@pytest.mark.parametrize(
    "condition,expected",
    [
        ("healthy", 5),
        ("moderate-errors", 8),
        ("slow", 15),
        ("high-errors", None),
        ("cooldown", None),
    ],
)
async def test_adaptive_samples_drive_live_limit_and_fallback(
    tmp_path, monkeypatch, condition, expected
):
    config = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        async_workers=20,
        adaptive_start_concurrency=3,
        adaptive_error_threshold=0.2,
        security_validation=False,
    )
    monkeypatch.setattr(
        "mirror_url.async_connection.get_domain_health_tracker",
        lambda: SimpleNamespace(is_throttled=lambda _: False),
    )
    monkeypatch.setattr("mirror_url.async_connection.time.time", lambda: 1000)
    manager = AdaptiveAsyncManager(config, MetricsCollector())
    manager._current_concurrency = 3 if condition == "healthy" else 16
    manager._semaphore = ResizableSemaphore(manager._current_concurrency)
    profile = manager._get_profile(BASE + "a")
    errors = 3 if condition == "moderate-errors" else 6 if condition == "high-errors" else 0
    rtt = ADAPTIVE_RTT_THRESHOLD_MS * 3 if condition == "slow" else 10
    for i in range(20):
        profile.add_sample(rtt, i >= errors, 0.1)
    if condition == "cooldown":
        manager._last_concurrency_change = 1000
    manager.record_result(BASE + "a", True, rtt, 0.1)
    assert manager._pending_concurrency == expected
    assert manager.should_fallback() is (condition == "high-errors")
    if expected is not None:
        await manager.apply_pending_concurrency_change()
        assert manager.get_concurrency() == expected
        assert manager._semaphore._value == expected
    else:
        await manager.apply_pending_concurrency_change()
        assert manager.get_concurrency() == 16
    stats = manager.get_stats()
    assert "example.com" in stats["profiles"]
    assert stats["fallback_to_sync"] is (condition == "high-errors")


@pytest.mark.parametrize("reason", ["learned", "known"])
async def test_throttled_domains_start_with_conservative_profile(tmp_path, monkeypatch, reason):
    config = MirrorConfig(
        base_url=BASE,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        async_workers=20,
        adaptive_start_concurrency=15,
    )
    monkeypatch.setattr(
        "mirror_url.async_connection.get_domain_health_tracker",
        lambda: SimpleNamespace(is_throttled=lambda _: reason == "learned"),
    )
    monkeypatch.setattr(
        "mirror_url.async_connection.KNOWN_THROTTLED_DOMAINS",
        {"example.com"} if reason == "known" else set(),
    )
    manager = AdaptiveAsyncManager(config, MetricsCollector())
    profile = manager._get_profile(BASE + "a")
    assert profile.is_throttled
    assert profile.recommended_concurrency == manager.get_concurrency() == 3
    assert manager._get_profile(BASE + "b") is profile
