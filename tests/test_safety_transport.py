"""Exercise transport security without making uncontrolled network requests.

Only the socket-facing httpx parent is replaced. DNS validation, IP pinning,
headers, streams, rate limiting, cache state and concurrency remain real.
"""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from mirror_url.exceptions import SecurityError
from mirror_url.transport import SecureAsyncTransport, SecureTransport


@pytest.fixture
def wire(monkeypatch):
    requests = []
    lock = threading.Lock()

    def send(self, request):
        with lock:
            requests.append(request)
        return httpx.Response(200, content=b"remote", request=request)

    async def async_send(self, request):
        return send(self, request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", send)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", async_send)
    return requests


@pytest.mark.parametrize("extensions", [{}, {"trace_id": "preserve"}])
def test_sync_pins_address_preserves_host_sni_stream_and_cache(monkeypatch, wire, extensions):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    limiter = SimpleNamespace(wait=Mock())
    with SecureTransport(rate_limiter=limiter, limits=httpx.Limits(max_connections=2)) as transport:
        request = httpx.Request(
            "POST", "https://example.com:8443/root/a", content=b"body", extensions=extensions
        )
        assert transport.handle_request(request).content == b"remote"
        assert transport.handle_request(request).content == b"remote"
        assert resolver.call_count == 1
        assert limiter.wait.call_count == 2
        for sent in wire:
            assert sent.url.host == "8.8.8.8"
            assert sent.headers["Host"] == "example.com:8443"
            assert sent.extensions["sni_hostname"] == "example.com"
            assert sent.stream is request.stream
            assert dict(sent.extensions, sni_hostname="example.com") == dict(
                extensions, sni_hostname="example.com"
            )
        assert request.url.host == "example.com"
        assert request.extensions == extensions


@pytest.mark.parametrize("url", ["https://8.8.8.8/a", "https://[::1]/a"])
def test_sync_direct_ip_never_reaches_socket(wire, url):
    with SecureTransport() as transport, pytest.raises(SecurityError, match="Direct IP"):
        transport.handle_request(httpx.Request("GET", url))
    assert wire == []


def test_sync_dns_rotation_retries_once_and_keeps_request_identity(monkeypatch):
    resolver = Mock(side_effect=["8.8.8.8", "1.1.1.1"])
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    requests = []

    def send(self, request):
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectError("first address failed")
        return httpx.Response(200, content=b"remote", request=request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", send)
    with SecureTransport() as transport:
        assert (
            transport.handle_request(httpx.Request("GET", "https://example.com/root/a")).content
            == b"remote"
        )
        assert [r.url.host for r in requests] == ["8.8.8.8", "1.1.1.1"]
        assert all(
            r.headers["host"] == "example.com" and r.extensions["sni_hostname"] == "example.com"
            for r in requests
        )
        assert transport._get_cached_ip("example.com") == "1.1.1.1"


def test_consumed_producer_is_not_replayed_after_connect_error(monkeypatch):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    consumed = []

    class Producer(httpx.SyncByteStream):
        def __iter__(self):
            yield b"body"

    def send(self, request):
        consumed.append(b"".join(request.stream))
        raise httpx.ConnectError("injected connection failure")

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", send)
    with SecureTransport() as transport, pytest.raises(httpx.ConnectError):
        transport.handle_request(httpx.Request("POST", "https://example.com/a", stream=Producer()))
    assert consumed == [b"body"]
    assert resolver.call_count == 1


def test_sync_cache_expiry_pruning_cleanup_and_stats(monkeypatch, wire):
    now = [1000.0]
    monkeypatch.setattr("mirror_url.transport.time.time", lambda: now[0])
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", lambda _: "8.8.8.8"
    )
    with SecureTransport() as transport:
        assert transport._get_cached_ip("missing") is None
        transport._cache_ip("expired", "8.8.8.8")
        now[0] += transport.IP_CACHE_TTL_SECONDS
        assert transport._get_cached_ip("expired") is None
        for i in range(transport.IP_CACHE_MAX_SIZE):
            now[0] += 1
            transport._cache_ip(f"host{i}", "8.8.8.8")
        transport._cache_ip("new", "1.1.1.1")
        stats = transport.get_ip_cache_stats()
        assert stats["size"] == 751 and stats["entries"] == [f"host{i}" for i in range(250, 260)]
        assert transport._cleanup_stale_ips() > 0
        assert transport._cleanup_stale_ips() == 0
        assert transport._cleanup_stale_ips(force=True) == 0
        now[0] += 301
        transport._request_count = 99
        transport.handle_request(httpx.Request("GET", "https://example.com/a"))
        assert transport.get_ip_cache_stats()["size"] == 1
        transport.clear_ip_cache()
        assert transport.get_ip_cache_stats()["size"] == 0


def test_sync_waiters_share_one_validated_resolution(monkeypatch, wire):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    barrier = threading.Barrier(2)
    counts = threading.local()
    with SecureTransport() as transport:
        original = transport._get_cached_ip

        def lookup(host):
            result = original(host)
            counts.lookups = getattr(counts, "lookups", 0) + 1
            if counts.lookups == 1:
                barrier.wait(timeout=5)
            return result

        monkeypatch.setattr(transport, "_get_cached_ip", lookup)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda _: transport.handle_request(
                        httpx.Request("GET", "https://example.com/a")
                    ),
                    range(2),
                )
            )
        assert all(r.status_code == 200 for r in results)
        assert resolver.call_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("extensions", [{}, {"trace_id": "preserve"}])
@pytest.mark.parametrize("limiter_kind", ["async", "sync", "none"])
async def test_async_pins_address_and_rate_limits(monkeypatch, wire, extensions, limiter_kind):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    threads = []
    limiter = (
        SimpleNamespace(async_wait=AsyncMock())
        if limiter_kind == "async"
        else SimpleNamespace(wait=lambda host: threads.append(threading.get_ident()))
        if limiter_kind == "sync"
        else None
    )
    async with SecureAsyncTransport(
        rate_limiter=limiter, limits=httpx.Limits(max_connections=2)
    ) as transport:
        request = httpx.Request("GET", "https://example.com:8443/a", extensions=extensions)
        assert (await transport.handle_async_request(request)).content == b"remote"
        assert (await transport.handle_async_request(request)).content == b"remote"
        assert resolver.call_count == 1
        assert all(
            r.url.host == "8.8.8.8"
            and r.headers["host"] == "example.com:8443"
            and r.extensions["sni_hostname"] == "example.com"
            and r.stream is request.stream
            for r in wire
        )
        assert request.extensions == extensions
        if limiter_kind == "async":
            assert limiter.async_wait.await_count == 2
        elif limiter_kind == "sync":
            assert len(threads) == 2 and threading.get_ident() not in threads


@pytest.mark.asyncio
@pytest.mark.parametrize("url", ["https://8.8.8.8/a", "https://[::1]/a"])
async def test_async_direct_ip_never_reaches_socket(wire, url):
    async with SecureAsyncTransport() as transport:
        with pytest.raises(SecurityError, match="Direct IP"):
            await transport.handle_async_request(httpx.Request("GET", url))
    assert wire == []


@pytest.mark.asyncio
async def test_async_cache_expiry_pruning_cleanup_and_stats(monkeypatch, wire):
    now = [1000.0]
    monkeypatch.setattr("mirror_url.transport.time.time", lambda: now[0])
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", lambda _: "8.8.8.8"
    )
    async with SecureAsyncTransport() as transport:
        assert await transport._get_cached_ip("missing") is None
        await transport._cache_ip("expired", "8.8.8.8")
        now[0] += transport.IP_CACHE_TTL_SECONDS
        assert await transport._get_cached_ip("expired") is None
        for i in range(transport.IP_CACHE_MAX_SIZE):
            now[0] += 1
            await transport._cache_ip(f"host{i}", "8.8.8.8")
        await transport._cache_ip("new", "1.1.1.1")
        assert transport.get_ip_cache_stats()["size"] == 751
        assert await transport._cleanup_stale_ips() > 0
        assert await transport._cleanup_stale_ips() == 0
        assert await transport._cleanup_stale_ips(force=True) == 0
        now[0] += 301
        transport._request_count = 99
        await transport.handle_async_request(httpx.Request("GET", "https://example.com/a"))
        assert transport.get_ip_cache_stats()["size"] == 1
        await transport.clear_ip_cache()
        assert transport.get_ip_cache_stats()["size"] == 0


@pytest.mark.asyncio
async def test_async_waiters_share_one_validated_resolution(monkeypatch, wire):
    resolver = Mock(return_value="8.8.8.8")
    monkeypatch.setattr(
        "mirror_url.transport.SecurityValidator.resolve_and_validate_hostname", resolver
    )
    first_reads = 0
    ready = asyncio.Event()
    counts = {}
    async with SecureAsyncTransport() as transport:
        original = transport._get_cached_ip

        async def lookup(host):
            nonlocal first_reads
            result = await original(host)
            task = asyncio.current_task()
            counts[task] = counts.get(task, 0) + 1
            if counts[task] == 1:
                first_reads += 1
                if first_reads == 2:
                    ready.set()
                await asyncio.wait_for(ready.wait(), 5)
            return result

        monkeypatch.setattr(transport, "_get_cached_ip", lookup)
        results = await asyncio.gather(
            *(
                transport.handle_async_request(httpx.Request("GET", "https://example.com/a"))
                for _ in range(2)
            )
        )
        assert all(r.status_code == 200 for r in results)
        assert resolver.call_count == 1


@pytest.mark.asyncio
async def test_test_mode_is_explicit_per_instance(wire):
    with SecureTransport(test_mode=True) as sync:
        assert sync.handle_request(httpx.Request("GET", "http://127.0.0.1/")).status_code == 200
    async with SecureAsyncTransport(test_mode=True) as async_transport:
        assert (
            await async_transport.handle_async_request(httpx.Request("GET", "http://127.0.0.1/"))
        ).status_code == 200
    assert [r.url.host for r in wire] == ["127.0.0.1", "127.0.0.1"]
