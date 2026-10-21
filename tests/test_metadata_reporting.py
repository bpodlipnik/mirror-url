"""Metadata totals and retry diagnostics through the shared comparison paths."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from types import SimpleNamespace

import httpx
import pytest

from mirror_url import MirrorURL
from mirror_url.async_connection import AsyncConnectionManager
from mirror_url.connection import ConnectionManager
from mirror_url.exceptions import MirrorConnectionError
from mirror_url.metrics import MetricsCollector
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote
from test_immediate_audit_fixes import BASE, AsyncManager, check_async, response
from test_immediate_audit_fixes import mirror as mirror


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("no_etag", [False, True])
@pytest.mark.parametrize(
    "status,headers,current,unavailable",
    [
        (200, {"Content-Length": "3"}, True, 1),
        (200, {"Content-Length": "4"}, False, 1),
        (200, {"Content-Length": "invalid"}, False, 1),
        (200, {"Content-Length": "3", "ETag": ""}, True, 1),
        (200, {"Content-Length": "3", "ETag": '"v1"'}, True, 0),
        (200, {"Content-Length": "3", "ETag": '"v2"'}, False, 0),
        (304, {}, True, 0),
        (503, {}, False, 0),
    ],
)
def test_missing_etag_counts_responses_without_changing_freshness(
    mirror, asynchronous, no_etag, status, headers, current, unavailable
):
    path = mirror.target_dir / "a"
    path.write_bytes(b"ABC")
    stat = path.stat()
    stored = {
        "size": stat.st_size,
        "local_mtime_ns": stat.st_mtime_ns,
        "local_ctime_ns": stat.st_ctime_ns,
        "etag": '"v1"',
    }
    mirror.cache_manager = SimpleNamespace(get_file_metadata=lambda _: stored)
    mirror.config.no_etag = no_etag
    result = response(status, headers)
    mirror.connection_manager.request.return_value = result
    if no_etag and status == 304:
        current = False  # No trusted conditional validator when ETags are disabled.
    elif no_etag and status == 200 and headers.get("ETag") == '"v2"':
        current = True  # The existing size-only fallback remains unchanged.
    if asynchronous:
        planned, manager = check_async(mirror, [(BASE + "a", path)], result)
        assert planned == ([] if current else [(BASE + "a", path)])
        assert len(manager.calls) == 1
    else:
        assert mirror.file_exists_and_up_to_date(path, BASE + "a") is current
        assert mirror.connection_manager.request.call_count == 1
    assert mirror.metrics.metrics["etag_unavailable"] == (0 if no_etag else unavailable)
    assert path.read_bytes() == b"ABC"


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("missing", [False, True])
def test_unchecked_or_missing_files_do_not_count_missing_etags(mirror, asynchronous, missing):
    path = mirror.target_dir / "a"
    if not missing:
        path.write_bytes(b"ABC")
    mirror.config.missing_files = True
    if asynchronous:
        planned, manager = check_async(mirror, [(BASE + "a", path)])
        assert planned == ([(BASE + "a", path)] if missing else [])
        assert manager.calls == []
    else:
        assert mirror.file_exists_and_up_to_date(path, BASE + "a") is (not missing)
    mirror.connection_manager.request.assert_not_called()
    assert mirror.metrics.metrics["etag_unavailable"] == 0


def test_missing_etag_section_is_visible_alone_and_uses_per_run_deltas():
    metrics = MetricsCollector()
    metrics.increment("etag_unavailable", 9)
    metrics.run_report.start(metrics.metrics, 0)
    metrics.increment("etag_unavailable", 2)
    metrics.run_report.finish("success")
    assert "ETag unavailable: 2" in metrics.report()
    assert "ETag 304 responses: 0" in metrics.report()
    assert metrics.get_summary()["etag_unavailable"] == 11
    metrics.run_report.start(metrics.metrics, 0)
    metrics.run_report.finish("success")
    assert "ETag unavailable:" not in metrics.report()


@pytest.mark.parametrize("throttled", [False, True])
@pytest.mark.parametrize("speed", [1.0, 11.1, 14.9])
def test_slow_successful_metadata_checks_remain_async(
    mirror, monkeypatch, caplog, throttled, speed
):
    import mirror_url._core.compare as module

    if throttled:
        mirror.config = mirror.config.model_copy(
            update={"base_url": "https://sohoftp.nascom.nasa.gov/solarsoft/"}
        )
    now = [0.0]
    # Only the comparison clock is simulated; asyncio and logging retain their clocks.
    monkeypatch.setattr(module, "time", SimpleNamespace(time=lambda: now[0]))

    class TimedManager(AsyncManager):
        async def head(self, url, headers=None):
            now[0] += 1 / speed
            return await super().head(url, headers)

    items = []
    for index in range(165):
        path = mirror.target_dir / str(index)
        path.write_bytes(b"ABC")
        items.append((BASE + str(index), path))
    manager = TimedManager()
    mirror.adaptive_async_manager = manager
    mirror.connection_manager.request.return_value = response(headers={"Content-Length": "3"})
    with caplog.at_level(logging.INFO):
        assert asyncio.run(mirror._check_files_async(items)) == []
    assert len(manager.calls) == 165
    assert len({url for url, _ in manager.calls}) == 165
    mirror.connection_manager.request.assert_not_called()
    assert "falling back to sync" not in caplog.text


@pytest.mark.integration
def test_mixed_async_and_sync_batches_report_one_combined_total(
    remote, config, monkeypatch, caplog
):
    import mirror_url._core.compare as module

    # Expire a live second-batch request through asyncio's cancellation path.
    batches = 0
    blocked = asyncio.Event()
    original_head = AsyncConnectionManager.head

    async def head(manager, url, headers=None):
        if url.endswith("080.dat"):
            blocked.set()
            await asyncio.Event().wait()
        return await original_head(manager, url, headers)

    monkeypatch.setattr(AsyncConnectionManager, "head", head)

    async def wait_for(awaitable, timeout):
        nonlocal batches
        if timeout == 120.0:
            batches += 1
            if batches == 2:
                await blocked.wait()
                timeout = 0
        return await asyncio.wait_for(awaitable, timeout)

    monkeypatch.setattr(
        module, "asyncio", SimpleNamespace(**{**vars(asyncio), "wait_for": wait_for})
    )
    names = [f"{index:03}.dat" for index in range(165)]
    remote.files = dict.fromkeys(names, b"current")
    remote.files.update(dict.fromkeys(names[:7], b"updated bytes"))
    missing = {names[7], *names[160:]}
    config.async_metadata = True
    config.adaptive_async = False
    config.cache_html = False
    config.use_shared_log = True
    with MirrorURL(config) as mirror:
        for name in names:
            if name not in missing:
                (mirror.target_dir / name).write_bytes(b"current")
        logging.getLogger().addHandler(caplog.handler)
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        assert "85 remaining files (80 already examined; 8 queued for download)" in caplog.text
        assert "Synchronous metadata batch complete: 5/85 files need download" in caplog.text
        assert caplog.text.count("Metadata check complete: 13/165 files need download") == 1
        assert caplog.text.index("Synchronous metadata batch complete:") < caplog.text.index(
            "Metadata check complete:"
        )
        result = mirror.metrics.get_summary()["run"]
        assert result["existing_files_current"] == 152
        assert result["changed_files_downloaded"] == 7
        assert result["missing_files_downloaded"] == 6
        assert result["status"] == "success"
        assert all((mirror.target_dir / name).read_bytes() == remote.files[name] for name in names)
    heads = Counter(path for method, path, _ in remote.requests if method == "HEAD")
    assert all(heads[name] == 1 for name in names[8:160])
    gets = [path for method, path, _ in remote.requests if method == "GET" and path in remote.files]
    assert Counter(gets) == Counter([*names[:7], *missing])


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("failure", ["disconnect", 503, 429])
@pytest.mark.parametrize("exhausted", [False, True])
def test_request_retry_context_and_recovery_are_attributed_and_sanitized(
    mirror, monkeypatch, caplog, method, failure, exhausted
):
    # Match CLI logging: httpx's own INFO request trace is not a MirrorURL diagnostic.
    caplog.set_level(logging.WARNING, logger="httpx")
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda _: None)
    monkeypatch.setattr("mirror_url.connection.socket.gethostbyname", lambda _: "8.8.8.8")
    config = mirror.config.model_copy(
        update={"security_validation": False, "max_retries": 1, "circuit_breaker_enabled": False}
    )
    manager = ConnectionManager(config, mirror.metrics)
    attempts = []

    def handle(request):
        attempts.append(request)
        if exhausted or len(attempts) == 1:
            if failure == "disconnect":
                raise httpx.RemoteProtocolError(
                    "Server disconnected without sending a response.", request=request
                )
            return httpx.Response(failure)
        return httpx.Response(200)

    url = BASE + "file?token=private-token#private-fragment"
    context = method + " " + BASE + "file?<redacted>"
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        monkeypatch.setattr(manager.connection_pool, "get_client", lambda _: client)
        try:
            with caplog.at_level(logging.INFO):
                if exhausted:
                    with pytest.raises(MirrorConnectionError):
                        manager.request(url, method=method)
                else:
                    result = manager.request(url, method=method)
                    assert result.status_code == 200
                    result.close()
        finally:
            manager.close()
    assert len(attempts) == 2
    retries = [record.message for record in caplog.records if "retrying in" in record.message]
    assert len(retries) == 1
    assert context in retries[0]
    assert (
        "RemoteProtocolError" in retries[0]
        if failure == "disconnect"
        else f"HTTP {failure}" in retries[0]
    )
    recovered = [
        record.message for record in caplog.records if "Request recovered:" in record.message
    ]
    assert recovered == (
        [] if exhausted else [f"Request recovered: {context} returned HTTP 200 after 2 attempts"]
    )
    assert "private-token" not in caplog.text
    assert "private-fragment" not in caplog.text
