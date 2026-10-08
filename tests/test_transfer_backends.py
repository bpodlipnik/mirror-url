"""Exercise both adapters through the same owned, scoped download pipeline."""

from __future__ import annotations

import asyncio
import builtins
import hashlib
import json
import logging
import os
import socket
import threading
from collections import Counter
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL, cli, download_url_list
from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager
from mirror_url.connection import ConnectionManager
from mirror_url.destination_lock import DestinationLock
from mirror_url.download import PartialDownloadManager
from mirror_url.enums import CleanupPolicy
from mirror_url.exceptions import ConfigError, DestinationLockError, SecurityError, URLScopeError
from mirror_url.metrics import MetricsCollector
from mirror_url.rate_limiter import PerIPRateLimiter, RateLimiter
from mirror_url.scratch import OwnedScratch
from mirror_url.security import SecurityValidator
from mirror_url.transfers import (
    AsyncBandwidthBudget,
    AsyncRequestPacer,
    AsyncTransfers,
    _identity,
    _save_receipts,
    plan_targets,
    require_backend,
)


@pytest.fixture(params=["httpx", "aiohttp"])
def backend(request):
    if request.param == "aiohttp":
        pytest.importorskip("aiohttp")
    return request.param


@pytest.fixture
def origin(monkeypatch):
    """Loopback is permitted only by this test's injected DNS validator."""
    requests = []
    counts = Counter()
    routes = {}
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body):
            with lock:
                counts[self.path] += 1
                count = counts[self.path]
                requests.append((self.command, self.path, dict(self.headers), self.client_address))
            route = routes.get(self.path, (200, {}, b"bytes"))
            code, headers, payload = route(count) if callable(route) else route
            self.send_response(code)
            if "Content-Length" not in headers and "Connection" not in headers:
                headers = {"Content-Length": str(len(payload)), **headers}
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            if body:
                try:
                    self.wfile.write(payload)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            if headers.get("Connection") == "close":
                self.close_connection = True

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    server.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setattr(SecurityValidator, "resolve_and_validate_hostname", lambda _: "127.0.0.1")
    # Existing sync metadata pacing resolves a name before using the pinned transport.
    monkeypatch.setattr(socket, "gethostbyname", lambda _: "127.0.0.1")
    yield SimpleNamespace(
        base=f"http://files.example:{server.server_port}/root/",
        routes=routes,
        requests=requests,
        counts=counts,
    )
    server.shutdown()
    server.server_close()
    thread.join(timeout=2)


def job(tmp_path, origin, backend="httpx", paths=("a",), **options):
    urls = tmp_path / "urls.txt"
    urls.write_text("\n".join(origin.base + path for path in paths), encoding="utf-8")
    values = {
        "base_url": origin.base,
        "dest_path": tmp_path / "dest",
        "log_path": tmp_path / "logs",
        "mode": "download",
        "backend": backend,
        "url_list": urls,
        "requests_per_second": 0,
        "request_delay": 0,
        "max_retries": 0,
        "retry_delay": 1,
        "verify_content": True,
        "timeout": 5,
        "max_concurrent_downloads": 3,
    }
    values.update(options)
    return MirrorConfig(**values)


def assert_no_scratch(target):
    assert not list(target.rglob("work_*"))
    assert not list(target.rglob("*.part"))


def test_both_backends_exact_streams_and_receipts(tmp_path, origin, backend):
    payloads = {
        "empty": b"",
        "nested/binary": bytes(range(256)) * 1700,
        "literal%2520name?q=%2f": b"encoded-name",
        "unknown": b"no length",
    }
    for name, payload in payloads.items():
        headers = {"ETag": '"proof"', "Last-Modified": "Wed, 07 Oct 2026 00:00:00 GMT"}
        if name == "unknown":
            headers["Connection"] = "close"
        origin.routes["/root/" + name] = (200, headers, payload)
    config = job(tmp_path, origin, backend, tuple(payloads))
    summary = download_url_list(config)
    assert summary["success"] and summary["files_downloaded"] == 4
    assert summary["bytes_downloaded"] == sum(map(len, payloads.values()))
    receipts = json.loads(Path(summary["receipts"]).read_text())["files"]
    for name, payload in payloads.items():
        local = config.dest_path / name.replace("%2520", "%20").split("?")[0]
        assert local.read_bytes() == payload
        assert local.stat().st_mtime == pytest.approx(1791331200, abs=0.001)
        receipt = receipts[str(local)]
        assert receipt["sha256"] == hashlib.sha256(payload).hexdigest()
        assert receipt["identity"] == list(_identity(local))
        assert receipt["etag"] == '"proof"'
    assert {r[0] for r in origin.requests} == {"GET"}
    assert {r[1] for r in origin.requests} == {"/root/" + x for x in payloads}
    assert all(r[2]["Host"] == origin.base.split("/")[2] for r in origin.requests)
    assert all(
        r[2]["Accept-Encoding"] == "identity" and "Range" not in r[2] for r in origin.requests
    )
    assert_no_scratch(config.dest_path)
    # Valid owned payloads are fetched again: this mode has no freshness checks.
    assert download_url_list(config)["success"]
    assert len(origin.requests) == 8


@pytest.mark.parametrize("code", [206, 304, 404, 416, 429, 500])
def test_failed_http_preserves_old_file_without_status_retries(tmp_path, origin, backend, code):
    config = job(tmp_path, origin, backend, overwrite=True, max_retries=2)
    config.dest_path.mkdir()
    old = config.dest_path / "a"
    old.write_bytes(b"original")
    identity = _identity(old)
    origin.routes["/root/a"] = (code, {}, b"bad")
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 1
    assert old.read_bytes() == b"original" and _identity(old) == identity
    assert origin.counts["/root/a"] == 1
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("recover", [True, False])
def test_truncated_stream_retry_is_whole_file_and_bounded(tmp_path, origin, backend, recover):
    config = job(tmp_path, origin, backend, overwrite=True, max_retries=1)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"old")
    origin.routes["/root/a"] = lambda count: (
        (200, {}, b"complete")
        if recover and count == 2
        else (200, {"Content-Length": "99", "Connection": "close"}, b"short")
    )
    result = download_url_list(config)
    assert result["success"] is recover
    assert origin.counts["/root/a"] == 2
    assert local.read_bytes() == (b"complete" if recover else b"old")
    assert all("Range" not in r[2] for r in origin.requests)
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize(
    "location", ["/outside/a", "http://other.example/root/a", "http://127.0.0.1/root/a"]
)
def test_outside_redirect_is_never_contacted(tmp_path, origin, backend, location):
    origin.routes["/root/a"] = (302, {"Location": location}, b"")
    config = job(tmp_path, origin, backend)
    assert not download_url_list(config)["success"]
    assert [r[1] for r in origin.requests] == ["/root/a"]
    assert not (config.dest_path / "a").exists()
    assert_no_scratch(config.dest_path)


def test_inside_redirect_and_connection_reuse(tmp_path, origin, backend):
    origin.routes["/root/a"] = (302, {"Location": "./b"}, b"")
    config = job(tmp_path, origin, backend, ("a", "c", "d"), sequential_downloads=True)
    assert download_url_list(config)["success"]
    assert [r[1] for r in origin.requests] == ["/root/a", "/root/b", "/root/c", "/root/d"]
    # HTTPX may retire an unread redirect response; completed file bodies reuse the pool.
    assert len({r[3] for r in origin.requests[1:]}) == 1


def test_literal_unicode_and_spaces_have_identical_uri_escaping(tmp_path, origin, backend):
    origin.routes["/root/caf%C3%A9%20data"] = (200, {}, b"escaped correctly")
    config = job(tmp_path, origin, backend, ("caf\u00e9 data",))
    assert download_url_list(config)["success"]
    assert (config.dest_path / "caf\u00e9 data").read_bytes() == b"escaped correctly"
    assert [r[1] for r in origin.requests] == ["/root/caf%C3%A9%20data"]


def test_private_dns_is_rejected_even_when_legacy_security_toggle_is_off(
    tmp_path, origin, backend, monkeypatch
):
    config = job(tmp_path, origin, backend, security_validation=False)

    def reject(_):
        raise SecurityError("Hostname resolves to private IP")

    monkeypatch.setattr(SecurityValidator, "resolve_and_validate_hostname", reject)
    result = download_url_list(config)
    assert not result["success"]
    assert "private IP" in result["failures"][0]["error"]
    assert not origin.requests
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize(
    "paths",
    [
        ("a", "a"),
        ("a?x=1", "a?x=2"),
        ("a", "a/b"),
        ("../outside",),
        ("%2e%2e/a",),
        (".mirror-url-state/a",),
        ("a%5Cb",),
        ("a" * 300,),
    ],
)
def test_invalid_full_list_rejected_before_any_get(tmp_path, origin, paths):
    config = job(tmp_path, origin, paths=paths)
    with pytest.raises((ValueError, SecurityError, URLScopeError)):
        download_url_list(config)
    assert not origin.requests


def test_case_collision_uses_existing_filesystem_policy(tmp_path, origin, monkeypatch):
    config = job(tmp_path, origin, paths=("Name", "name"))
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: False)
    with pytest.raises(ValueError, match="case-insensitive"):
        download_url_list(config)
    assert not origin.requests


@pytest.mark.parametrize(
    "kind", ["unowned", "symlink", "hardlink", "modified-receipt", "reserved-state"]
)
def test_unrelated_files_and_state_preserved(tmp_path, origin, backend, kind):
    config = job(tmp_path, origin, backend)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    saved = tmp_path / "saved"
    saved.write_bytes(b"keep")
    if kind == "symlink":
        local.symlink_to(saved)
    elif kind == "hardlink":
        os.link(saved, local)
    elif kind == "reserved-state":
        state = config.dest_path / ".mirror-url-state"
        state.mkdir()
        (state / "user-file").write_bytes(b"keep")
    elif kind == "modified-receipt":
        result = download_url_list(config)
        receipt = Path(result["receipts"])
        data = json.loads(receipt.read_text())
        data["files"][str(local)]["sha256"] = "0" * 64
        receipt.write_text(json.dumps(data))
        origin.requests.clear()
    else:
        local.write_bytes(b"keep")
    with pytest.raises(ValueError):
        download_url_list(config)
    assert not origin.requests
    assert saved.read_bytes() == b"keep"
    if kind in ("symlink", "hardlink", "unowned"):
        assert local.read_bytes() == b"keep"
    if kind == "reserved-state":
        assert (state / "user-file").read_bytes() == b"keep"


def test_dry_run_and_suffix_keep_exact_scope_without_network(tmp_path, origin):
    config = job(tmp_path, origin, paths=("sub/nested/a",), dry_run=True, dir_suffix="sub")
    assert download_url_list(config) == {
        "success": True,
        "dry_run": True,
        "files": 1,
        "backend": "httpx",
    }
    assert not origin.requests and not config.dest_path.exists()


def test_suffix_cannot_escape_base_scope(tmp_path, origin):
    config = job(tmp_path, origin, paths=("a",), dir_suffix="../outside")
    with pytest.raises(URLScopeError, match="outside"):
        download_url_list(config)
    assert not origin.requests and not config.dest_path.exists()


def test_failed_atomic_publication_preserves_original(tmp_path, origin, backend, monkeypatch):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"original")
    original = _identity(local)
    replace = os.replace

    def fail(source, destination):
        if Path(destination) == local:
            raise OSError("publication failed")
        return replace(source, destination)

    monkeypatch.setattr("mirror_url.transfers.os.replace", fail)
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 1
    assert local.read_bytes() == b"original" and _identity(local) == original
    assert_no_scratch(config.dest_path)


def test_destination_changed_during_transfer_is_preserved(tmp_path, origin, backend, monkeypatch):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"original")
    from mirror_url.transfers import file_sha256

    def concurrent_edit(staging, verify=True):
        if staging.name == "staging.streaming":
            local.write_bytes(b"external change")
        return file_sha256(staging, verify)

    monkeypatch.setattr("mirror_url.transfers.file_sha256", concurrent_edit)
    result = download_url_list(config)
    assert not result["success"] and local.read_bytes() == b"external change"
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("same_inode", [True, False])
def test_staging_replaced_after_hash_never_publishes_wrong_receipt(
    tmp_path, origin, backend, monkeypatch, same_inode
):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"original")
    from mirror_url.transfers import file_sha256

    def replace_after_hash(staging, verify=True):
        digest = file_sha256(staging, verify)
        if staging.name == "staging.streaming":
            if same_inode:
                staging.write_bytes(b"other")
            else:
                replacement = staging.with_name("replacement")
                replacement.write_bytes(b"other")
                os.replace(replacement, staging)
        return digest

    monkeypatch.setattr("mirror_url.transfers.file_sha256", replace_after_hash)
    result = download_url_list(config)
    assert not result["success"] and local.read_bytes() == b"original"
    assert_no_scratch(config.dest_path)


def test_staging_change_during_timestamp_update_is_preserved(
    tmp_path, origin, backend, monkeypatch
):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"original")
    origin.routes["/root/a"] = (200, {"Last-Modified": "Wed, 07 Oct 2026 00:00:00 GMT"}, b"bytes")

    original_open = Path.open

    class ReplaceAfterClose:
        def __init__(self, path, file):
            self.path = path
            self.file = file

        def __enter__(self):
            return self.file.__enter__()

        def __exit__(self, *error):
            result = self.file.__exit__(*error)
            replacement = self.path.with_name("replacement")
            replacement.write_bytes(b"other")
            os.replace(replacement, self.path)
            return result

    def replacing(path, mode="r", *args, **kwargs):
        file = original_open(path, mode, *args, **kwargs)
        if path.name == "staging.streaming" and mode == "r+b":
            return ReplaceAfterClose(path, file)
        return file

    monkeypatch.setattr(Path, "open", replacing)
    result = download_url_list(config)
    assert not result["success"] and local.read_bytes() == b"original"
    assert_no_scratch(config.dest_path)


def test_insufficient_disk_keeps_old_file(tmp_path, origin, backend, monkeypatch):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"old")
    # Application disk guards still run even though this mode skips HEAD probes.
    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", lambda _: SimpleNamespace(free=0))
    result = download_url_list(config)
    assert not result["success"] and local.read_bytes() == b"old"
    assert_no_scratch(config.dest_path)


def test_encoded_representation_rejected(tmp_path, origin, backend):
    origin.routes["/root/a"] = (200, {"Content-Encoding": "gzip"}, b"not raw identity")
    config = job(tmp_path, origin, backend)
    result = download_url_list(config)
    assert not result["success"] and not (config.dest_path / "a").exists()
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize(
    "rate,delay,expected",
    [(20, 0.05, 0.05), (100, 0.05, 0.05), (100, 0, 0.01), (0, 0.1, 0.1), (0, 0, 0)],
)
def test_rate_controls_across_metadata_and_transport_layers(tmp_path, rate, delay, expected):
    config = MirrorConfig(
        base_url="https://files.example/root",
        dest_path=tmp_path,
        log_path=tmp_path,
        requests_per_second=rate,
        request_delay=delay,
    )
    assert config.effective_request_interval == expected
    assert RateLimiter(rate, delay).min_interval == expected
    assert PerIPRateLimiter(rate, delay).min_interval == expected
    manager = ConnectionManager(config, MetricsCollector())
    try:
        assert manager.rate_limiter.min_interval == expected
        assert manager.connection_pool.rate_limiter.min_interval == expected
        assert (
            AsyncConnectionManager(config, MetricsCollector()).rate_limiter.min_interval == expected
        )
        assert (
            AdaptiveAsyncManager(config, MetricsCollector()).rate_limiter.min_interval == expected
        )
    finally:
        manager.close()


@pytest.mark.parametrize(
    "rate,delay", [(-1, 0), (float("inf"), 0), (float("nan"), 0), (0, float("nan"))]
)
def test_invalid_rate_controls_rejected(tmp_path, rate, delay):
    with pytest.raises(ValueError):
        RateLimiter(rate, delay)
    with pytest.raises(ValueError):
        MirrorConfig(
            base_url="https://files.example/root",
            dest_path=tmp_path,
            log_path=tmp_path,
            requests_per_second=rate,
            request_delay=delay,
        )


@pytest.mark.asyncio
async def test_pacer_reserves_distinct_slots_without_holding_lock(monkeypatch):
    sleeps = []
    monkeypatch.setattr("mirror_url.transfers.time.monotonic", lambda: 10.0)

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("mirror_url.transfers.asyncio.sleep", sleep)
    pacer = AsyncRequestPacer(0.1)
    await asyncio.gather(*(pacer.wait() for _ in range(4)))
    assert sleeps == pytest.approx([0.1, 0.2, 0.3])
    await AsyncRequestPacer(0).wait()
    assert len(sleeps) == 3
    bandwidth = AsyncBandwidthBudget(1)
    await asyncio.gather(*(bandwidth.wait(1024**2) for _ in range(3)))
    assert sleeps[-3:] == pytest.approx([1, 2, 3])


@pytest.mark.parametrize(
    "options",
    [
        {"mode": "download"},
        {"url_list": Path("urls")},
        {"overwrite": True},
        {"mode": "download", "url_list": Path("urls"), "cleanup_policy": CleanupPolicy.DELETE},
        {"mode": "download", "url_list": Path("urls"), "file_filters": [".fits"]},
        {"mode": "download", "url_list": Path("urls"), "missing_files": True},
        {"backend": "aiohttp", "streaming_parallel": True},
        {"backend": "aiohttp", "auto_concurrency": True},
    ],
)
def test_incompatible_modes_fail_explicitly(tmp_path, options):
    with pytest.raises(ConfigError):
        MirrorConfig(
            base_url="https://files.example/root", dest_path=tmp_path, log_path=tmp_path, **options
        )


def test_optional_dependency_error_is_actionable(monkeypatch):
    original = builtins.__import__

    def without_aiohttp(name, *args, **kwargs):
        if name == "aiohttp":
            raise ImportError(name)
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_aiohttp)
    assert require_backend("httpx") is None
    with pytest.raises(ConfigError, match=r"mirror-url\[aiohttp\]"):
        require_backend("aiohttp")
    with pytest.raises(ConfigError, match="Unknown"):
        require_backend("other")


def test_receipt_replacement_failure_preserves_old_bytes(tmp_path, monkeypatch):
    receipt = tmp_path / "receipt"
    receipt.write_text("original")

    def fail(*_):
        raise OSError("failed atomic replace")

    monkeypatch.setattr("mirror_url.transfers.os.replace", fail)
    with pytest.raises(OSError):
        _save_receipts(receipt, {"new": True})
    assert receipt.read_text() == "original"
    assert list(tmp_path.iterdir()) == [receipt]


@pytest.mark.asyncio
async def test_cancelled_workers_finish_before_ownership_release(tmp_path, origin, monkeypatch):
    started = asyncio.Event()
    closed = []

    class Backend:
        retryable = (httpx.TransportError,)

        def __init__(self, *_):
            pass

        async def close(self):
            closed.append(True)

        @asynccontextmanager
        async def open(self, _):
            async def blocks():
                yield b"partial"
                started.set()
                await asyncio.Event().wait()

            yield 200, {"Content-Length": "100"}, blocks()

    monkeypatch.setattr("mirror_url.transfers._HTTPXBackend", Backend)
    config = job(tmp_path, origin)
    config.dest_path.mkdir()
    guard = DestinationLock([config.dest_path])
    with guard.operation():
        state = PartialDownloadManager(config.dest_path)._state_directory()
        scratch = OwnedScratch(config.dest_path, state, None, guard)
        engine = AsyncTransfers(config, config.dest_path, origin.base, scratch)
        task = asyncio.create_task(
            engine.run(plan_targets([origin.base + "a"], origin.base, config.dest_path, config))
        )
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not scratch._active and closed == [True]
        assert_no_scratch(config.dest_path)
        assert not (config.dest_path / "a").exists()
    guard.close()


def test_destination_and_log_lock_conflicts_before_get(tmp_path, origin):
    config = job(tmp_path, origin)
    guard = DestinationLock([config.log_path])
    try:
        with pytest.raises(DestinationLockError):
            download_url_list(config)
        assert not origin.requests
    finally:
        guard.close()


def test_cli_download_json_config_and_override(tmp_path, origin, backend, monkeypatch, capsys):
    config = job(tmp_path, origin, backend)
    yaml = tmp_path / "job.json"
    values = config.model_dump(mode="json")
    values["requests_per_second"] = 10
    yaml.write_text(json.dumps(values))
    monkeypatch.setattr(
        "sys.argv",
        ["mirror-url", "--config", str(yaml), "--requests-per-second", "0", "--concurrency", "2"],
    )
    cli.main()
    output = json.loads(capsys.readouterr().out)
    assert output["success"] and output["effective_request_interval"] == 0
    logs = list(config.log_path.glob("download_*.log"))
    assert len(logs) == 1 and "discovery and freshness checks omitted" in logs[0].read_text(
        encoding="utf-8"
    )


def test_cli_invalid_file_not_silently_ignored(tmp_path, origin, monkeypatch, capsys):
    config = job(tmp_path, origin)
    path = tmp_path / "job.json"
    values = config.model_dump(mode="json")
    values["unexpected_option"] = 1
    path.write_text(json.dumps(values))
    monkeypatch.setattr("sys.argv", ["mirror-url", "--config", str(path)])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 1 and "Download error" in capsys.readouterr().err
    assert not origin.requests


def test_aiohttp_mirror_still_checks_freshness_and_records_receipts(tmp_path, origin, monkeypatch):
    pytest.importorskip("aiohttp")
    config = job(tmp_path, origin, "aiohttp")
    values = config.model_dump()
    values.update(
        mode="mirror",
        url_list=None,
        overwrite=False,
        async_metadata=False,
        connection_pool_prewarm=False,
    )
    config = MirrorConfig(**values)
    origin.routes["/root/a"] = (200, {"ETag": '"same"'}, b"mirror bytes")
    root = logging.getLogger()
    saved = list(root.handlers)
    try:
        with MirrorURL(config) as mirror:
            monkeypatch.setattr(mirror, "get_remote_files", lambda: [origin.base + "a"])
            assert mirror.sync()
            assert (config.dest_path / "a").read_bytes() == b"mirror bytes"
            assert mirror.files_processed.value() == 1
            assert mirror.total_downloaded_size.value() == len(b"mirror bytes")
            assert any(r[0] == "HEAD" for r in origin.requests)
            metadata = mirror.cache_manager.get_file_metadata(config.dest_path / "a")
            assert metadata["sha256"] == hashlib.sha256(b"mirror bytes").hexdigest()
            requests_before = len(origin.requests)
            assert mirror.sync()
            assert all(r[0] == "HEAD" for r in origin.requests[requests_before:])
            assert mirror.files_processed.value() == 0
            assert_no_scratch(config.dest_path)
    finally:
        root.handlers[:] = saved


def test_last_modified_restoration_never_requires_path_based_utime(
    tmp_path, origin, backend, monkeypatch
):
    config = job(tmp_path, origin, backend)
    origin.routes["/root/a"] = (
        200,
        {"Last-Modified": "Wed, 07 Oct 2026 00:00:00 GMT"},
        b"timestamped payload",
    )
    original_utime = os.utime

    def descriptor_only(descriptor, *args, **kwargs):
        if not isinstance(descriptor, int) or "follow_symlinks" in kwargs:
            raise NotImplementedError("path-based no-follow utime unavailable")
        return original_utime(descriptor, *args, **kwargs)

    monkeypatch.setattr("mirror_url.transfers.os.utime", descriptor_only)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 1
    local = config.dest_path / "a"
    assert local.read_bytes() == b"timestamped payload"
    assert local.stat().st_mtime == pytest.approx(1791331200, abs=0.001)
    receipt = json.loads(Path(result["receipts"]).read_text(encoding="utf-8"))["files"][str(local)]
    assert receipt["identity"] == list(_identity(local))
    assert receipt["sha256"] == hashlib.sha256(b"timestamped payload").hexdigest()
    assert_no_scratch(config.dest_path)


def test_timestamp_handle_identity_checked_before_mutation(tmp_path, origin, backend, monkeypatch):
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"original")
    outside = tmp_path / "user-data"
    outside.write_bytes(b"unrelated content")
    outside_identity = _identity(outside)
    origin.routes["/root/a"] = (200, {"Last-Modified": "Wed, 07 Oct 2026 00:00:00 GMT"}, b"bytes")
    original_open = Path.open
    timestamp_calls = []

    def substitute_handle(path, mode="r", *args, **kwargs):
        if path.name == "staging.streaming" and mode == "r+b":
            return original_open(outside, mode, *args, **kwargs)
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "open", substitute_handle)
    monkeypatch.setattr(
        "mirror_url.transfers._set_file_timestamp", lambda *args: timestamp_calls.append(args)
    )
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 1
    assert local.read_bytes() == b"original"
    assert outside.read_bytes() == b"unrelated content" and _identity(outside) == outside_identity
    assert not timestamp_calls
    assert_no_scratch(config.dest_path)
