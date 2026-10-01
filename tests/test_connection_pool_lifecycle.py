"""Connection reuse, concurrent creation, eviction, and stream lease failures."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorConfig
from mirror_url.connection import ConnectionManager, ConnectionPool
from mirror_url.exceptions import (
    ConcurrencyLimitError,
    MirrorConnectionError,
    SecurityError,
    URLScopeError,
)
from mirror_url.metrics import MetricsCollector


@pytest.fixture
def pool(monkeypatch):
    pool = ConnectionPool(max_pools=2)
    monkeypatch.setattr(
        pool,
        "_create_client",
        lambda: httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200))),
    )
    yield pool
    pool.close_all()


def test_concurrent_requests_create_one_client_and_reuse_it(pool):
    with ThreadPoolExecutor(max_workers=12) as executor:
        clients = list(executor.map(pool.get_client, ["https://example.com/root/a"] * 40))
    assert all(client is clients[0] for client in clients)
    stats = pool.get_stats()
    assert stats["creations"] == 1
    assert stats["hits"] == 39 and stats["misses"] == 1
    assert pool.has_pool("https://example.com")
    assert pool.get_pool("https://example.com") is clients[0]
    assert pool.get_pool("https://missing.example") is None


def test_capacity_eviction_closes_oldest_client_and_bounds_tracking(pool):
    first = pool.get_client("https://one.example/a")
    second = pool.get_client("https://two.example/a")
    pool.last_used["https://one.example"] = 1
    pool.last_used["https://two.example"] = 2
    pool.get_client("https://three.example/a")
    assert first.is_closed
    assert not second.is_closed
    assert set(pool.pools) == set(pool.pool_usage) == set(pool.last_used)
    assert len(pool.pools) == 2
    assert pool.get_stats()["evictions"] == 1


def test_resize_closes_excess_pools_then_allows_expansion(pool):
    first = pool.get_client("https://one.example/a")
    pool.get_client("https://two.example/a")
    pool.last_used["https://one.example"] = 1
    pool.resize_pools(1)
    assert first.is_closed
    assert len(pool.pools) == pool.get_stats()["max_pools"] == 1
    pool.resize_pools(3)
    pool.get_client("https://three.example/a")
    assert len(pool.pools) == 2


def test_idle_pool_cleanup_preserves_active_clients(pool):
    old = pool.get_client("https://old.example/a")
    active = pool.get_client("https://active.example/a")
    pool.last_used["https://old.example"] = 0
    assert pool.clear_idle_pools(idle_timeout=60) == 1
    assert old.is_closed and not active.is_closed
    assert pool.clear_idle_pools(idle_timeout=60) == 0
    assert set(pool.pools) == set(pool.pool_usage) == set(pool.last_used)


def test_close_failure_during_eviction_does_not_block_new_pool(pool, monkeypatch):
    first = pool.get_client("https://old.example/a")
    pool.get_client("https://other.example/a")
    pool.last_used["https://old.example"] = 0
    monkeypatch.setattr(first, "close", Mock(side_effect=OSError("broken close")))
    try:
        assert pool.get_client("https://new.example/a") is not None
        assert "https://old.example" not in pool.pools
        assert len(pool.pools) == 2
    finally:
        monkeypatch.undo()
        first.close()


def test_warmup_groups_domains_and_closes_responses(pool, monkeypatch):
    requests, responses = [], []

    def handle(request):
        requests.append(request)
        if request.url.path == "/fail":
            raise httpx.ConnectError("warmup failed", request=request)
        response = httpx.Response(200)
        responses.append(response)
        return response

    monkeypatch.setattr(
        pool, "_create_client", lambda: httpx.Client(transport=httpx.MockTransport(handle))
    )
    pool.warm_up(["https://one.example/fail", "https://one.example/a", "https://two.example/b"])
    assert len(requests) == 3
    assert all(request.method == "HEAD" for request in requests)
    assert all(response.is_closed for response in responses)
    pool.warm_up([])
    assert len(requests) == 3


@pytest.fixture
def manager(tmp_path, monkeypatch):
    config = MirrorConfig(
        base_url="https://example.com/root/",
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        security_validation=False,
        max_retries=0,
    )
    leases = SimpleNamespace(acquire_thread=Mock(return_value=True), release_thread=Mock())
    manager = ConnectionManager(config, MetricsCollector(), concurrency_manager=leases)
    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    yield manager, leases
    manager.close()


@pytest.mark.parametrize("failure", ["denied", "scope", "security", "network"])
def test_request_rejections_release_only_acquired_permits(manager, monkeypatch, failure):
    manager, leases = manager
    url = "https://example.com/root/a"
    if failure == "denied":
        leases.acquire_thread.return_value = False
        expected = ConcurrencyLimitError
    elif failure == "scope":
        url = "https://example.com/outside/a"
        expected = URLScopeError
    elif failure == "security":
        manager.config.security_validation = True
        monkeypatch.setattr(
            "mirror_url.connection.SecurityValidator.validate_url_security",
            lambda *args: (False, "blocked"),
        )
        expected = SecurityError
    else:
        monkeypatch.setattr(
            manager.connection_pool, "get_client", Mock(side_effect=httpx.ConnectError("offline"))
        )
        expected = MirrorConnectionError
    with pytest.raises(expected):
        manager.request(url, stream=True)
    assert leases.release_thread.call_count == (0 if failure == "denied" else 1)


def test_stream_read_failure_closes_response_and_releases_lease_once(manager, monkeypatch):
    manager, leases = manager
    closed = []

    class BrokenBody(httpx.SyncByteStream):
        def __iter__(self):
            yield b"prefix"
            raise httpx.ReadError("connection dropped")

        def close(self):
            closed.append(True)

    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=BrokenBody()))
    )
    monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
    try:
        response = manager.request("https://example.com/root/a", stream=True)
        assert leases.release_thread.call_count == 0
        try:
            with pytest.raises(httpx.ReadError):
                list(response.iter_bytes())
        finally:
            response.close()
        response.close()
        assert closed == [True]
        assert leases.release_thread.call_count == 1
    finally:
        client.close()
