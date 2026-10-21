"""Bounded HEAD EOF recovery, including real TLS close_notify and reused sockets."""

from __future__ import annotations

import asyncio
import logging
import ssl
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from mirror_url import MirrorConfig
from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager
from mirror_url.async_primitives import ResizableSemaphore
from mirror_url.connection import ConnectionManager
from mirror_url.exceptions import MirrorConnectionError
from mirror_url.metrics import MetricsCollector

EOF = "Server disconnected without sending a response."
BASE = "https://example.com/root/"
MODES = ("sync", "fixed", "adaptive")


def configuration(tmp_path, **changes):
    values = {
        "base_url": BASE,
        "dest_path": tmp_path / "mirror",
        "log_path": tmp_path / "logs",
        "security_validation": False,
        "circuit_breaker_enabled": False,
        "max_retries": 2,
        "requests_per_second": 0,
        "request_delay": 0,
        "async_workers": 3,
        "adaptive_start_concurrency": 3,
    }
    values.update(changes)
    return MirrorConfig(**values)


def async_manager(mode, config, metrics, client):
    cls = AsyncConnectionManager if mode == "fixed" else AdaptiveAsyncManager
    manager = cls(config, metrics)
    manager._client = client
    manager._semaphore = ResizableSemaphore(3) if mode == "adaptive" else asyncio.Semaphore(3)
    if mode == "adaptive":
        manager._client_initialized = True
    return manager


def request_with_mock(mode, config, metrics, handler, monkeypatch, *, method="HEAD"):
    url = str(config.base_url).rstrip("/") + "/file?token=private-token#private-fragment"
    waits = []
    limiter = Mock(min_interval=0)
    limiter.wait = lambda ip: waits.append(ip)

    async def async_wait(ip):
        waits.append(ip)

    limiter.async_wait = async_wait
    if mode == "sync":
        manager = ConnectionManager(config, metrics)
        manager.rate_limiter = limiter
        with httpx.Client(transport=httpx.MockTransport(handler)) as client:
            monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
            try:
                try:
                    result = manager.request(url, method=method, headers={"If-None-Match": '"v1"'})
                except MirrorConnectionError:
                    result = None
            finally:
                manager.close()
        return result, waits

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            manager = async_manager(mode, config, metrics, client)
            manager.rate_limiter = limiter
            return await manager.head(url, {"If-None-Match": '"v1"'})

    return asyncio.run(run()), waits


@pytest.fixture
def policy(monkeypatch, caplog):
    backoffs = []
    sleeps = []

    def backoff(attempt, base):
        backoffs.append(attempt)
        return 9.0

    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda delay: sleeps.append(delay))
    monkeypatch.setattr("mirror_url.async_connection.asyncio.sleep", AsyncMock())
    monkeypatch.setattr("mirror_url.connection.exponential_backoff", backoff)
    monkeypatch.setattr("mirror_url.async_connection.exponential_backoff", backoff)
    health = SimpleNamespace(is_throttled=lambda _: False, record_incident=Mock())
    monkeypatch.setattr("mirror_url.connection.get_domain_health_tracker", lambda: health)
    monkeypatch.setattr("mirror_url.async_connection.get_domain_health_tracker", lambda: health)
    caplog.set_level(logging.WARNING, logger="httpx")
    caplog.set_level(logging.INFO)
    return backoffs, sleeps


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("failures,budget", [(1, 2), (3, 2), (1, 0)])
def test_prompt_retry_budget_pacing_logging_and_recovery(
    tmp_path, monkeypatch, caplog, policy, mode, failures, budget
):
    metrics = MetricsCollector()
    config = configuration(tmp_path, max_retries=budget)
    requests = []

    def handle(request):
        requests.append(request)
        if len(requests) <= failures:
            raise httpx.RemoteProtocolError(EOF, request=request)
        return httpx.Response(304)

    result, waits = request_with_mock(mode, config, metrics, handle, monkeypatch)
    recovered = failures <= budget
    assert len(requests) == min(failures + 1, budget + 1)
    assert all(r.headers["if-none-match"] == '"v1"' for r in requests)
    assert (result is not None) == recovered
    assert metrics.metrics["head_early_disconnects"] == min(failures, budget + 1)
    assert metrics.metrics["head_disconnect_recoveries"] == int(recovered)
    assert metrics.metrics["head_disconnect_exhausted"] == int(not recovered)
    assert policy[0] == ([1] if budget == 2 and not recovered else [])
    assert len(waits) >= len(requests) if recovered else len(waits) >= 1
    retries = [r for r in caplog.records if "retrying in" in r.message]
    assert len(retries) == min(failures, budget)
    if budget:
        assert retries[0].levelno == logging.INFO
        assert "retrying in 0.0s" in retries[0].message
    if len(retries) > 1:
        assert retries[1].levelno == logging.WARNING
        assert "retrying in 9.0s" in retries[1].message
    assert sum("exhausted retry budget" in r.message for r in caplog.records) == int(not recovered)
    assert sum("Request recovered:" in r.message for r in caplog.records) == int(recovered)
    assert "private-token" not in caplog.text and "private-fragment" not in caplog.text


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "error", [httpx.ReadError("read failed"), httpx.RemoteProtocolError("illegal status line")]
)
def test_other_errors_keep_backoff(tmp_path, monkeypatch, policy, mode, error):
    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            raise error
        return httpx.Response(200)

    metrics = MetricsCollector()
    response, _ = request_with_mock(mode, configuration(tmp_path), metrics, handle, monkeypatch)
    assert response.status_code == 200
    assert policy[0] == [0]
    assert metrics.metrics["head_early_disconnects"] == 0


@pytest.mark.parametrize("method", ["GET", "POST"])
def test_other_methods_keep_backoff(tmp_path, monkeypatch, policy, method):
    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.RemoteProtocolError(EOF, request=request)
        return httpx.Response(200)

    metrics = MetricsCollector()
    result, _ = request_with_mock(
        "sync", configuration(tmp_path), metrics, handle, monkeypatch, method=method
    )
    assert result.status_code == 200
    assert policy[0] == [0]
    assert metrics.metrics["head_early_disconnects"] == 0


@pytest.mark.parametrize("mode", MODES)
def test_disconnect_then_server_error_exhausts_original_budget(tmp_path, monkeypatch, policy, mode):
    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            raise httpx.RemoteProtocolError(EOF, request=request)
        return httpx.Response(503)

    metrics = MetricsCollector()
    result, _ = request_with_mock(
        mode, configuration(tmp_path, max_retries=1), metrics, handle, monkeypatch
    )
    assert result is None
    assert len(calls) == 2
    assert metrics.metrics["head_disconnect_exhausted"] == 1
    assert metrics.metrics["head_disconnect_recoveries"] == 0


@pytest.mark.parametrize("mode", MODES)
def test_later_disconnect_keeps_backoff(tmp_path, monkeypatch, policy, mode):
    calls = []

    def handle(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503)
        if len(calls) == 2:
            raise httpx.RemoteProtocolError(EOF, request=request)
        return httpx.Response(200)

    metrics = MetricsCollector()
    result, _ = request_with_mock(mode, configuration(tmp_path), metrics, handle, monkeypatch)
    assert result.status_code == 200
    assert policy[0] == [0, 1]
    assert metrics.metrics["head_disconnect_recoveries"] == 1


@pytest.mark.parametrize("mode", ["fixed", "adaptive"])
def test_prompt_retry_rate_wait_remains_cancellable(tmp_path, monkeypatch, policy, mode):
    metrics = MetricsCollector()
    attempts = []

    def handle(request):
        attempts.append(request)
        raise httpx.RemoteProtocolError(EOF, request=request)

    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            manager = async_manager(mode, configuration(tmp_path), metrics, client)
            waits = []

            async def wait(ip):
                waits.append(ip)
                if len(waits) == 2:
                    raise asyncio.CancelledError

            manager.rate_limiter = Mock(async_wait=wait)
            with pytest.raises(asyncio.CancelledError):
                await manager.head(BASE + "file")
            assert len(waits) == 2

    asyncio.run(run())
    assert len(attempts) == 1
    assert metrics.metrics["head_early_disconnects"] == 1
    assert metrics.metrics["head_disconnect_recoveries"] == 0
    assert metrics.metrics["head_disconnect_exhausted"] == 0


def test_run_counts_reset_and_export_without_losing_lifetime_totals(tmp_path):
    metrics = MetricsCollector()
    metrics.increment_batch({"head_early_disconnects": 8, "head_disconnect_recoveries": 5})
    metrics.run_report.start(metrics.metrics, 0)
    metrics.increment_batch(
        {
            "head_early_disconnects": 3,
            "head_disconnect_recoveries": 1,
            "head_disconnect_exhausted": 1,
        }
    )
    metrics.run_report.finish("success")
    summary = metrics.get_summary()
    assert summary["head_early_disconnects"] == 11
    assert summary["run"]["head_early_disconnects"] == 3
    assert "HEAD early disconnects: 3" in metrics.report()
    assert "HEAD requests recovered after disconnect: 1" in metrics.report()
    assert "HEAD requests exhausted after disconnect: 1" in metrics.report()
    output = tmp_path / "metrics.json"
    assert metrics.export_json(output, configuration(tmp_path))
    import json

    exported = json.loads(output.read_text())
    assert exported["metrics"]["run"]["head_early_disconnects"] == 3
    metrics.run_report.start(metrics.metrics, 0)
    assert metrics.get_summary()["run"]["head_early_disconnects"] == 0
    assert "HEAD early disconnects:" not in metrics.report()
    assert metrics.get_summary()["head_early_disconnects"] == 11


@contextmanager
def tls_origin(failures):
    """Advertise keep-alive, then close TLS without answering selected HEADs."""
    state = {"remaining": failures, "requests": [], "lock": threading.Lock()}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self):
            with state["lock"]:
                state["requests"].append(
                    (
                        self.path,
                        self.client_address[1],
                        {k.lower(): v for k, v in self.headers.items()},
                    )
                )
                drop = self.path == "/target" and state["remaining"] > 0
                if drop:
                    state["remaining"] -= 1
            if drop:
                self.close_connection = True
                self.connection.settimeout(2)
                try:
                    # unwrap sends close_notify before waiting for its peer.
                    self.connection.unwrap().close()
                except (OSError, ssl.SSLError):
                    pass
                return
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "keep-alive")
            self.send_header("Keep-Alive", "timeout=5, max=100")
            self.end_headers()

        def log_message(self, *args):
            pass

    fixtures = Path(__file__).parent / "fixtures/head-recovery-tls"
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(fixtures / "server.pem", fixtures / "server-key.pem")
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    verify = ssl.create_default_context(cafile=str(fixtures / "ca.pem"))
    try:
        yield state, f"https://127.0.0.1:{server.server_port}/", verify
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.integration
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("exhausted", [False, True])
def test_real_tls_close_notify_on_reused_connection_recovers_or_exhausts(
    tmp_path, monkeypatch, mode, exhausted
):
    metrics = MetricsCollector()
    with tls_origin(2 if exhausted else 1) as (state, base, verify):
        config = configuration(tmp_path, base_url=base, max_retries=1, http2=False)
        if mode == "sync":
            manager = ConnectionManager(config, metrics)
            # Only this loopback fixture uses a plain HTTPX transport. Production
            # continues through the SSRF-validating SecureTransport.
            with httpx.Client(verify=verify, trust_env=False) as client:
                monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
                try:
                    assert manager.request(base + "warm", method="HEAD").status_code == 200
                    if exhausted:
                        with pytest.raises(MirrorConnectionError):
                            manager.request(
                                base + "target", method="HEAD", headers={"If-None-Match": '"v1"'}
                            )
                    else:
                        assert (
                            manager.request(
                                base + "target", method="HEAD", headers={"If-None-Match": '"v1"'}
                            ).status_code
                            == 200
                        )
                finally:
                    manager.close()
        else:

            async def run():
                async with httpx.AsyncClient(verify=verify, trust_env=False) as client:
                    manager = async_manager(mode, config, metrics, client)
                    assert (await manager.head(base + "warm")).status_code == 200
                    response = await manager.head(base + "target", {"If-None-Match": '"v1"'})
                    assert (response is None) == exhausted
                    if response is not None:
                        assert response.status_code == 200

            asyncio.run(run())
        requests = state["requests"]
    assert [r[0] for r in requests] == ["/warm", "/target", "/target"]
    assert requests[0][1] == requests[1][1]  # actually reused before failure
    assert requests[2][1] != requests[1][1]  # failed socket discarded by HTTPX
    assert all(r[2]["if-none-match"] == '"v1"' for r in requests[1:])
    assert metrics.metrics["head_early_disconnects"] == (2 if exhausted else 1)
    assert metrics.metrics["head_disconnect_recoveries"] == int(not exhausted)
    assert metrics.metrics["head_disconnect_exhausted"] == int(exhausted)
