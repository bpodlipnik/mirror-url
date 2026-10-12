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
import time
from collections import Counter
from contextlib import asynccontextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL, cli, download_url_list
from mirror_url import progress as transfer_progress
from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager
from mirror_url.connection import ConnectionManager
from mirror_url.destination_lock import DestinationLock
from mirror_url.download import PartialDownloadManager
from mirror_url.enums import CleanupPolicy
from mirror_url.exceptions import ConfigError, DestinationLockError, SecurityError, URLScopeError
from mirror_url.metrics import MetricsCollector
from mirror_url.rate_limiter import PerIPRateLimiter, RateLimiter
from mirror_url.run_report import RunReport
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
                    if callable(payload):
                        payload(self.wfile)
                    else:
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


@pytest.mark.parametrize("sequential", [False, True])
def test_single_worker_streams_large_and_unknown_lengths(tmp_path, origin, sequential):
    payload = bytes(range(256)) * 3100
    origin.routes["/root/a"] = (200, {}, payload)
    origin.routes["/root/unknown"] = (200, {"Connection": "close"}, payload + b"tail")
    paths = ("a", "unknown") if sequential else ("a",)
    config = job(tmp_path, origin, paths=paths, sequential_downloads=sequential)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == len(paths)
    receipts = json.loads(Path(result["receipts"]).read_text())["files"]
    for name in paths:
        expected = payload if name == "a" else payload + b"tail"
        local = config.dest_path / name
        assert local.read_bytes() == expected
        assert receipts[str(local)]["sha256"] == hashlib.sha256(expected).hexdigest()
        assert receipts[str(local)]["identity"] == list(_identity(local))
    assert_no_scratch(config.dest_path)


def test_httpx_multiple_workers_contact_origin_concurrently(tmp_path, origin):
    rendezvous = threading.Barrier(2, timeout=3)

    def overlap(_):
        rendezvous.wait()
        return 200, {}, b"concurrent payload"

    origin.routes.update({"/root/a": overlap, "/root/b": overlap})
    config = job(tmp_path, origin, paths=("a", "b"), max_concurrent_downloads=2)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 2
    assert all(
        (config.dest_path / name).read_bytes() == b"concurrent payload" for name in ("a", "b")
    )
    assert_no_scratch(config.dest_path)


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


@pytest.mark.parametrize("sequential", [False, True])
def test_download_batch_samples_free_space_once(tmp_path, origin, backend, monkeypatch, sequential):
    payload = b"payload!" * (512 * 1024)
    paths = ("a", "b", "c")
    for name in paths:
        origin.routes["/root/" + name] = (200, {}, payload)
    config = job(tmp_path, origin, backend, paths, sequential_downloads=sequential)
    headroom = (1 if sequential else config.max_concurrent_downloads) * 1024 * 1024
    calls = []

    def disk_usage(path):
        calls.append(path)
        assert len(calls) == 1, "Transfer loop repeated the filesystem query"
        return SimpleNamespace(free=headroom + len(paths) * len(payload))

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == len(paths)
    assert calls == [config.dest_path]
    for name in paths:
        assert (config.dest_path / name).read_bytes() == payload
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("sequential", [False, True])
def test_download_batch_does_not_reuse_published_space(
    tmp_path, origin, backend, monkeypatch, sequential
):
    payload = b"new" * (1024 * 1024)
    paths = ("a", "b", "c")
    for name in paths:
        origin.routes["/root/" + name] = (200, {}, payload)
    config = job(tmp_path, origin, backend, paths, overwrite=True, sequential_downloads=sequential)
    config.dest_path.mkdir()
    for name in paths:
        (config.dest_path / name).write_bytes(b"old")
    calls = []

    def disk_usage(path):
        calls.append(path)
        workers = 1 if sequential else config.max_concurrent_downloads
        return SimpleNamespace(free=workers * 1024 * 1024 + len(payload))

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    result = download_url_list(config)
    assert not result["success"]
    assert result["files_downloaded"] == 1 and result["files_failed"] == 2
    assert len(calls) == 1
    contents = [(config.dest_path / name).read_bytes() for name in paths]
    assert contents.count(payload) == 1 and contents.count(b"old") == 2
    receipts = json.loads(Path(result["receipts"]).read_text())["files"]
    assert len(receipts) == 1
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("retry", [False, True])
def test_removed_failed_attempt_returns_space_budget(tmp_path, origin, backend, monkeypatch, retry):
    payload = b"new" * (1024 * 1024)
    origin.routes["/root/a"] = lambda count: (
        (200, {}, payload)
        if retry and count > 1
        else (200, {"Content-Length": str(len(payload)), "Connection": "close"}, b"partial")
    )
    origin.routes["/root/b"] = (200, {}, payload)
    config = job(
        tmp_path,
        origin,
        backend,
        ("a", "b"),
        overwrite=True,
        sequential_downloads=True,
        max_retries=int(retry),
    )
    config.dest_path.mkdir()
    (config.dest_path / "a").write_bytes(b"old")
    calls = []

    def disk_usage(path):
        calls.append(path)
        return SimpleNamespace(free=1024 * 1024 + len(payload) * (2 if retry else 1))

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    result = download_url_list(config)
    assert result["success"] is retry
    assert result["files_downloaded"] == (2 if retry else 1)
    assert origin.counts["/root/a"] == (2 if retry else 1)
    assert (config.dest_path / "a").read_bytes() == (payload if retry else b"old")
    assert (config.dest_path / "b").read_bytes() == payload
    assert len(calls) == 1
    assert_no_scratch(config.dest_path)


def test_unknown_length_uses_remaining_batch_budget(tmp_path, origin, backend, monkeypatch):
    origin.routes["/root/a"] = (200, {}, b"1234")
    origin.routes["/root/b"] = (200, {"Connection": "close"}, b"5678")
    config = job(tmp_path, origin, backend, ("a", "b"), overwrite=True, sequential_downloads=True)
    config.dest_path.mkdir()
    (config.dest_path / "b").write_bytes(b"old")
    calls = []

    def disk_usage(path):
        calls.append(path)
        return SimpleNamespace(free=1024 * 1024 + 6)

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    result = download_url_list(config)
    assert not result["success"] and result["files_downloaded"] == 1
    assert result["files_failed"] == 1 and len(calls) == 1
    assert (config.dest_path / "a").read_bytes() == b"1234"
    assert (config.dest_path / "b").read_bytes() == b"old"
    assert_no_scratch(config.dest_path)


def test_space_consumed_by_another_writer_fails_safely(tmp_path, origin, backend, monkeypatch):
    payload = b"new payload" * 10000
    origin.routes["/root/a"] = (200, {}, payload)
    config = job(tmp_path, origin, backend, overwrite=True)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"old")
    original_identity = _identity(local)
    original_fsync = os.fsync
    calls = []

    def disk_usage(path):
        calls.append(path)
        return SimpleNamespace(free=100 * 1024 * 1024)

    def fsync(descriptor):
        # Space was available at admission, but another writer consumed it
        # before the downloaded staging file could be flushed to disk.
        if os.fstat(descriptor).st_size == len(payload):
            raise OSError(28, "No space left on device")
        return original_fsync(descriptor)

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    monkeypatch.setattr("mirror_url.transfers.os.fsync", fsync)
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 1
    assert "No space left on device" in result["failures"][0]["error"]
    assert local.read_bytes() == b"old" and _identity(local) == original_identity
    assert not json.loads(Path(result["receipts"]).read_text())["files"]
    assert len(calls) == 1
    assert_no_scratch(config.dest_path)


def test_failed_free_space_query_is_not_repeated(tmp_path, origin, backend, monkeypatch):
    config = job(tmp_path, origin, backend, ("a", "b", "c"), overwrite=True)
    config.dest_path.mkdir()
    for name in ("a", "b", "c"):
        (config.dest_path / name).write_bytes(b"old")
    calls = []

    def disk_usage(path):
        calls.append(path)
        raise OSError("Destination free space unavailable")

    monkeypatch.setattr("mirror_url.transfers.shutil.disk_usage", disk_usage)
    result = download_url_list(config)
    assert not result["success"] and result["files_failed"] == 3
    assert len(calls) == 1
    assert any("Destination free space unavailable" in f["error"] for f in result["failures"])
    assert all((config.dest_path / name).read_bytes() == b"old" for name in ("a", "b", "c"))
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


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["headers", "body"])
async def test_single_reader_cancellation_drains_network_before_cleanup(
    tmp_path, origin, stage, monkeypatch
):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    reading = threading.Event()
    original_reader = httpx.Response.iter_raw

    def observe_reads(response, *args, **kwargs):
        for block in original_reader(response, *args, **kwargs):
            yield block
            # Set only when the client starts its next raw read after receiving
            # the first block; a server-side pause alone can precede header receipt.
            reading.set()

    if stage == "body":
        monkeypatch.setattr(httpx.Response, "iter_raw", observe_reads)

    def pause():
        entered.set()
        try:
            assert release.wait(3)
        finally:
            finished.set()

    def body(file):
        file.write(b"x" * 16384)
        file.flush()
        pause()
        file.write(b"y" * 16384)

    def route(_):
        if stage == "headers":
            pause()
        return 200, {"Content-Length": "32768"}, body if stage == "body" else b"z" * 32768

    origin.routes["/root/a"] = route
    config = job(tmp_path, origin)
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"preserved old file")
    original = _identity(local)
    guard = DestinationLock([config.dest_path])
    try:
        with guard.operation():
            state = PartialDownloadManager(config.dest_path)._state_directory()
            scratch = OwnedScratch(config.dest_path, state, None, guard)
            engine = AsyncTransfers(config, config.dest_path, origin.base, scratch)
            task = asyncio.create_task(engine.run([(origin.base + "a", local)]))
            try:
                assert await asyncio.to_thread(entered.wait, 2)
                if stage == "body":
                    assert await asyncio.to_thread(reading.wait, 2)
                task.cancel()
                await asyncio.sleep(0.02)
                task.cancel()
                await asyncio.sleep(0.02)
                # The loop remains responsive, but cancellation must not release
                # staging or ownership while a background network call is live.
                assert not task.done() and scratch._active and not finished.is_set()
            finally:
                release.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
            assert finished.is_set() and not scratch._active
            assert engine.closed_connection_pools == 1
            assert local.read_bytes() == b"preserved old file" and _identity(local) == original
            assert_no_scratch(config.dest_path)
        with guard.operation():
            pass
    finally:
        release.set()
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
    assert len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "File download plan: 1 files; 1 file workers" in text
    assert "Discovery mode: not performed" in text and "Metadata time: not performed" in text
    assert [(method, path) for method, path, _headers, _client in origin.requests] == [
        ("GET", "/root/a")
    ]


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


def test_cli_direct_url_defaults_to_current_directory(
    tmp_path, origin, backend, monkeypatch, capsys
):
    target = tmp_path / "current"
    target.mkdir()
    unrelated = target / "keep.txt"
    unrelated.write_bytes(b"original unrelated bytes")
    monkeypatch.chdir(target)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "temp"))
    name = "aspiics_pb_l3_20260901T010909_307060570001_v03.fits"
    url = origin.base + "nested/" + name + "?token=abc%2fdef"
    payload = b"FITS payload\x00" * 1024
    origin.routes["/root/nested/" + name + "?token=abc%2fdef"] = (
        200,
        {"ETag": '"direct"'},
        payload,
    )
    argv = ["mirror-url", "--mode", "download", url]
    if backend == "aiohttp":
        argv.extend(["--backend", backend])
    monkeypatch.setattr("sys.argv", argv)
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result["success"] and result["files_downloaded"] == 1
    assert (target / name).read_bytes() == payload
    assert unrelated.read_bytes() == b"original unrelated bytes"
    assert [(request[0], request[1]) for request in origin.requests] == [
        ("GET", "/root/nested/" + name + "?token=abc%2fdef")
    ]
    receipts = json.loads(Path(result["receipts"]).read_text())
    assert receipts["scope"] == origin.base + "nested/"
    assert receipts["files"][str(target / name)]["url"] == url
    logs = list((tmp_path / "temp").glob("mirror-url-download-*/download_*.log"))
    assert len(logs) == 1
    assert not list(target.glob("download_*.log"))
    assert_no_scratch(target)


def test_direct_url_uses_explicit_scope_paths_and_receipts(tmp_path, origin, backend):
    url = origin.base + "nested/literal%2520name?query=kept"
    origin.routes["/root/nested/literal%2520name?query=kept"] = (200, {}, b"direct bytes")
    config = job(tmp_path, origin, backend, url_list=None, download_url=" \u00a0" + url + "\u00a0 ")
    assert config.download_url == url
    result = download_url_list(config)
    local = config.dest_path / "nested/literal%20name"
    assert result["success"] and local.read_bytes() == b"direct bytes"
    receipt = json.loads(Path(result["receipts"]).read_text())["files"][str(local)]
    assert receipt["sha256"] == hashlib.sha256(b"direct bytes").hexdigest()
    original_identity = _identity(local)
    origin.routes["/root/nested/literal%2520name?query=kept"] = (
        200,
        {"Content-Length": "100", "Connection": "close"},
        b"truncated",
    )
    failed = download_url_list(config)
    assert not failed["success"]
    assert local.read_bytes() == b"direct bytes" and _identity(local) == original_identity
    assert all(request[0] == "GET" for request in origin.requests)
    assert_no_scratch(config.dest_path)


def test_cli_direct_url_honors_explicit_targets(tmp_path, origin, monkeypatch, capsys):
    destination, logs = tmp_path / "destination", tmp_path / "logs"
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "nested/a",
            "--url",
            origin.base,
            "--dest-path",
            str(destination),
            "--log-path",
            str(logs),
        ],
    )
    cli.main()
    assert json.loads(capsys.readouterr().out)["success"]
    assert (destination / "nested/a").read_bytes() == b"bytes"
    assert len(list(logs.glob("download_*.log"))) == 1


def _observe_periodic_reports(monkeypatch, stage):
    """Release a blocked transfer only after the real timer reports twice."""
    monkeypatch.setattr(transfer_progress, "PROGRESS_INTERVAL_SECONDS", 0.02)
    observed = threading.Event()
    messages = []
    original = logging.info

    def capture(message, *args, **kwargs):
        if (
            isinstance(message, str)
            and message.startswith("Download progress:")
            and "files completed" in message
            and stage in message
        ):
            messages.append(message)
            if len(messages) >= 2:
                observed.set()
        original(message, *args, **kwargs)

    monkeypatch.setattr(logging, "info", capture)
    return observed, messages


def test_cli_print_logs_reports_during_network_wait_then_readable_summary(
    tmp_path, origin, backend, monkeypatch, capsys
):
    observed, messages = _observe_periodic_reports(monkeypatch, "waiting for response")
    waited = []

    def waiting_response(_):
        waited.append(observed.wait(3))
        return 200, {}, b"downloaded bytes"

    origin.routes["/root/a"] = waiting_response
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "a",
            "--backend",
            backend,
            "--print-logs",
        ],
    )
    cli.main()
    captured = capsys.readouterr()
    assert captured.out == "" and waited == [True]
    assert len(messages) >= 2 and "waiting for response" in "\n".join(messages)
    assert "SUMMARY:" in captured.err and "Files downloaded: 1" in captured.err
    assert f"Destination: {tmp_path}" in captured.err and "Download throughput:" in captured.err
    assert "SHA-256: disabled" in captured.err and "SHA-256 time (sum):" not in captured.err
    assert "Download result: {" not in captured.err and '"bytes_downloaded"' not in captured.err
    for unused in [
        "Adaptive async",
        "HTML cache",
        "Resume enabled",
        "Compatibility batch",
        "Fast parsing",
        "pre-warming",
        "Memory monitoring",
    ]:
        assert unused not in captured.err
    assert (tmp_path / "a").read_bytes() == b"downloaded bytes"
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


@pytest.mark.parametrize("stage", ["flush", "hash"])
def test_cli_progress_remains_live_during_blocking_finalization(
    tmp_path, origin, monkeypatch, capsys, stage
):
    import mirror_url.transfers as transfers

    observed, messages = _observe_periodic_reports(
        monkeypatch, "flushing file" if stage == "flush" else "hashing (SHA-256)"
    )
    waited = []
    payload = b"finalization bytes"
    origin.routes["/root/a"] = (200, {}, payload)
    if stage == "flush":
        original = transfers.os.fsync

        def blocking(fd):
            if os.fstat(fd).st_size == len(payload):
                waited.append(observed.wait(3))
            return original(fd)

        monkeypatch.setattr(transfers.os, "fsync", blocking)
    else:
        original_hash = transfers.file_sha256

        def blocking_hash(path, enabled=True):
            waited.append(observed.wait(3))
            return original_hash(path, enabled)

        monkeypatch.setattr(transfers, "file_sha256", blocking_hash)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        ["mirror-url", "--mode", "download", origin.base + "a", "--print-logs", "--verify-content"],
    )
    cli.main()
    captured = capsys.readouterr()
    assert captured.out == "" and waited == [True]
    assert len(messages) >= 2
    assert ("flushing file" if stage == "flush" else "hashing (SHA-256)") in "\n".join(messages)
    assert all("0/1 files completed" in message for message in messages)
    assert "Result: SUCCESS" in captured.err and "SHA-256: enabled" in captured.err
    assert (tmp_path / "a").read_bytes() == payload
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


@pytest.mark.parametrize("declared_length", [None, 6])
def test_cli_progress_tracks_bytes_while_stream_is_pending(
    tmp_path, origin, backend, monkeypatch, capsys, declared_length
):
    import mirror_url.transfers as transfers

    observed, messages = _observe_periodic_reports(monkeypatch, "downloading (3.00 B")
    waited = []

    @asynccontextmanager
    async def slow_body(self, url):
        async def chunks():
            yield b"abc"
            waited.append(await asyncio.to_thread(observed.wait, 3))
            yield b"def"

        headers = {} if declared_length is None else {"Content-Length": str(declared_length)}
        yield 200, headers, chunks()

    adapter = transfers._HTTPXBackend if backend == "httpx" else transfers._AIOHTTPBackend
    original_init = adapter.__init__

    def delayed_init(self, *args, **kwargs):
        # Earlier preparing/waiting reports must not release the body stall.
        time.sleep(0.08)
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(adapter, "__init__", delayed_init)
    monkeypatch.setattr(adapter, "open", slow_body)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "a",
            "--backend",
            backend,
            "--print-logs",
        ],
    )
    cli.main()
    captured = capsys.readouterr()
    assert captured.out == "" and waited == [True]
    assert "Result: SUCCESS" in captured.err
    assert len(messages) >= 2
    assert all(
        "received 3.00 B" in message and "0/1 files completed" in message for message in messages
    )
    assert all(("ETA" in message) is (declared_length is not None) for message in messages)
    assert (tmp_path / "a").read_bytes() == b"abcdef"
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


@pytest.mark.parametrize(
    "flags",
    [[], ["--print-logs", "--quiet"], ["--print-logs"], ["--print-logs", "--progress", "--quiet"]],
)
def test_cli_failed_download_preserves_json_and_console_preferences(
    tmp_path, origin, monkeypatch, capsys, flags
):
    # The client rejects this response without consuming its body. End the
    # fixture connection here instead of reading another keepalive request
    # after the client closes it (which can reset the socket on Linux).
    origin.routes["/root/a"] = (
        404,
        {"Content-Length": "9", "Connection": "close"},
        b"not found",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr("sys.argv", ["mirror-url", "--mode", "download", origin.base + "a", *flags])
    with pytest.raises(SystemExit) as error:
        cli.main()
    captured = capsys.readouterr()
    assert error.value.code == 1
    if flags == ["--print-logs"]:
        assert captured.out == ""
        assert "Files failed: 1" in captured.err
        assert "Sync failed" in captured.err and "❌ Failed to process: ROOT" in captured.err
        assert "Sync completed successfully" not in captured.err
        assert "Closed all 1 connection pools" in captured.err
    else:
        result = json.loads(captured.out)
        assert not result["success"] and result["files_failed"] == 1
    assert ("SUMMARY:" in captured.err) is (flags == ["--print-logs"])
    assert ("Result: FAILED" in captured.err) is (flags == ["--print-logs"])
    if not flags:
        assert not captured.err
    assert not (tmp_path / "a").exists()
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


@pytest.mark.parametrize("flag", ["--progress", "--progress-bar"])
@pytest.mark.parametrize("print_logs", [False, True])
@pytest.mark.parametrize("declared_length", [None, 6])
def test_cli_known_url_byte_bar_and_json_stream(
    tmp_path, origin, backend, monkeypatch, capsys, flag, print_logs, declared_length
):
    pytest.importorskip("tqdm")
    import mirror_url.transfers as transfers

    bars = []
    snapshots = []
    original = transfer_progress._create_progress_bar

    def create(**settings):
        bar = original(**settings)
        assert bar is not None
        bars.append(bar)
        return bar

    @asynccontextmanager
    async def response(self, url):
        async def chunks():
            yield b"abc"
            snapshots.append((bars[0].n, bars[0].total))
            yield b"def"

        headers = {} if declared_length is None else {"Content-Length": str(declared_length)}
        yield 200, headers, chunks()

    monkeypatch.setattr(transfer_progress, "_create_progress_bar", create)
    adapter = transfers._HTTPXBackend if backend == "httpx" else transfers._AIOHTTPBackend
    monkeypatch.setattr(adapter, "open", response)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    flags = [flag] + (["--print-logs"] if print_logs else [])
    monkeypatch.setattr(
        "sys.argv",
        ["mirror-url", "--mode", "download", origin.base + "a", "--backend", backend, *flags],
    )
    cli.main()
    captured = capsys.readouterr()
    if print_logs:
        assert captured.out == ""
        assert "Files downloaded: 1" in captured.err
    else:
        assert json.loads(captured.out)["files_downloaded"] == 1
    assert snapshots == [(3, declared_length)]
    assert len(bars) == 1 and bars[0].n == 6 and bars[0].disable
    assert bars[0].total == declared_length
    assert "a:" in captured.err and ("%" in captured.err) is (declared_length is not None)
    assert ("SUMMARY:" in captured.err) is print_logs
    assert (tmp_path / "a").read_bytes() == b"abcdef"
    assert not any(t.name == "mirror-url-download-progress" for t in threading.enumerate())


def test_cli_download_summary_uses_validated_destination(tmp_path, origin, monkeypatch, capsys):
    origin.routes["/root/CON/a"] = (200, {}, b"destination bytes")
    destination, logs = tmp_path / "destination", tmp_path / "logs"
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "CON/a",
            "--url",
            origin.base,
            "--dir-suffix",
            "CON",
            "--dest-path",
            str(destination),
            "--log-path",
            str(logs),
            "--print-logs",
        ],
    )
    cli.main()
    captured = capsys.readouterr()
    target = destination / "_CON"
    assert captured.out == "" and "Result: SUCCESS" in captured.err
    assert (target / "a").read_bytes() == b"destination bytes"
    assert f"Destination: {target}" in captured.err
    receipts = list((target / ".mirror-url-state").glob("url-list-*.json"))
    assert len(receipts) == 1 and json.loads(receipts[0].read_text())["target"] == str(target)


@pytest.mark.parametrize("source", ["positional", "list", "config-direct"])
def test_cli_direct_url_config_source_precedence(tmp_path, origin, source, monkeypatch, capsys):
    config = job(tmp_path, origin)
    path = tmp_path / "job.json"
    values = config.model_dump(mode="json")
    argv = ["mirror-url", "--config", str(path)]
    expected = "a"
    if source == "positional":
        argv.append(origin.base + "b")
        expected = "b"
    else:
        values.update(url_list=None, download_url=origin.base + "b")
        if source == "list":
            argv.extend(["--url-list", str(config.url_list)])
        else:
            expected = "b"
    path.write_text(json.dumps(values))
    monkeypatch.setattr("sys.argv", argv)
    cli.main()
    assert json.loads(capsys.readouterr().out)["success"]
    assert (config.dest_path / expected).read_bytes() == b"bytes"
    assert [(request[0], request[1]) for request in origin.requests] == [
        ("GET", "/root/" + expected)
    ]


def test_cli_config_direct_url_can_use_shortcut_defaults(tmp_path, origin, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "temp"))
    path = tmp_path / "direct.json"
    path.write_text(json.dumps({"mode": "download", "download_url": origin.base + "a"}))
    monkeypatch.setattr("sys.argv", ["mirror-url", "--config", str(path)])
    cli.main()
    assert json.loads(capsys.readouterr().out)["success"]
    assert (tmp_path / "a").read_bytes() == b"bytes"


@pytest.mark.parametrize(
    "argv, message",
    [
        (["https://files.example/a"], "requires --mode download"),
        (
            ["--mode", "download", "https://files.example/a", "--url-list", "urls.txt"],
            "mutually exclusive",
        ),
        (["--mode", "download", "ftp://files.example/a"], "absolute HTTP(S) file URL"),
        (["--mode", "download", "https://files.example/"], "must name a file"),
        (["--mode", "download", "https://files.example"], "must name a file"),
        (["--mode", "download", "https://[invalid/a"], "Invalid IPv6 URL"),
        (["--mode", "download", " "], "nonempty string"),
    ],
)
def test_cli_rejects_invalid_direct_input_before_writes(
    tmp_path, argv, message, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["mirror-url", *argv])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and message in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("direct_url", ["", 123, ["https://files.example/a"]])
def test_cli_rejects_invalid_config_direct_input(tmp_path, direct_url, monkeypatch, capsys):
    path = tmp_path / "direct.json"
    path.write_text(json.dumps({"mode": "download", "download_url": direct_url}))
    monkeypatch.setattr("sys.argv", ["mirror-url", "--config", str(path)])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and "nonempty string" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("unsafe", ["../outside", "%2e%2e/outside", ".mirror-url-state/a"])
def test_direct_url_retains_path_and_scope_guards(tmp_path, origin, unsafe):
    config = job(tmp_path, origin, url_list=None, download_url=origin.base + unsafe)
    with pytest.raises((SecurityError, URLScopeError, ValueError)):
        download_url_list(config)
    assert not origin.requests
    assert not config.dest_path.exists()


def test_direct_url_refuses_unrelated_existing_file(tmp_path, origin):
    config = job(tmp_path, origin, url_list=None, download_url=origin.base + "a")
    config.dest_path.mkdir()
    local = config.dest_path / "a"
    local.write_bytes(b"user file")
    before = _identity(local)
    with pytest.raises(ValueError, match="receipt|overwrite"):
        download_url_list(config)
    assert local.read_bytes() == b"user file" and _identity(local) == before
    assert not origin.requests


def test_cli_direct_url_keeps_explicit_scope_restriction(tmp_path, origin, monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "outside/a",
            "--url",
            origin.base + "selected/",
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 1 and "outside" in capsys.readouterr().err
    assert not origin.requests and not (tmp_path / "dest/a").exists()


@pytest.mark.parametrize(
    "options",
    [
        {"download_url": "https://files.example/root/a"},
        {"mode": "download", "download_url": ""},
        {"mode": "download", "download_url": "a", "url_list": Path("urls")},
    ],
)
def test_direct_url_config_rejects_conflicting_sources(tmp_path, options):
    with pytest.raises(ConfigError):
        MirrorConfig(
            base_url="https://files.example/root", dest_path=tmp_path, log_path=tmp_path, **options
        )


@pytest.mark.parametrize("change", [{"download_url": "a"}, {"mode": "mirror"}, {"url_list": None}])
def test_download_runtime_rejects_mutated_source_config(tmp_path, origin, change):
    config = job(tmp_path, origin)
    for key, value in change.items():
        object.__setattr__(config, key, value)
    with pytest.raises(ConfigError, match="exactly one URL source"):
        download_url_list(config)
    assert not origin.requests


def test_selected_updates_and_failure_preservation_on_both_backends(
    tmp_path, origin, backend, monkeypatch
):
    values = job(tmp_path, origin, backend).model_dump()
    values.update(
        mode="mirror",
        url_list=None,
        verify_content=False,
        missing_files=True,
        check_files=["nested/a"],
        async_metadata=False,
        connection_pool_prewarm=False,
    )
    config = MirrorConfig(**values)
    for name in ["nested/a", "nested/b", "nested/missing"]:
        origin.routes["/root/" + name] = (200, {"ETag": '"old"'}, b"original")
    with MirrorURL(config) as mirror:
        monkeypatch.setattr(
            mirror,
            "get_remote_files",
            lambda: [origin.base + name for name in ["nested/a", "nested/b", "nested/missing"]],
        )
        assert mirror.sync()
        local = config.dest_path / "nested/a"
        origin.routes["/root/nested/a"] = (200, {"ETag": '"new"'}, b"MODIFIED")
        origin.routes["/root/nested/b"] = (200, {"ETag": '"new"'}, b"MODIFIED")
        (config.dest_path / "nested/missing").unlink()
        origin.requests.clear()
        assert mirror.sync()
        assert local.read_bytes() == b"MODIFIED"
        assert (config.dest_path / "nested/b").read_bytes() == b"original"
        assert (config.dest_path / "nested/missing").read_bytes() == b"original"
        assert not any(path == "/root/nested/b" for _, path, _, _ in origin.requests)
        metadata = mirror.cache_manager.get_file_metadata(local)
        assert metadata["etag"] == '"new"'
        assert metadata["size"] == len(b"MODIFIED")
        origin.routes["/root/nested/a"] = (503, {}, b"")
        assert not mirror.sync()
        assert local.read_bytes() == b"MODIFIED"
        assert mirror.cache_manager.get_file_metadata(local) == metadata
        assert_no_scratch(config.dest_path)


def test_known_url_phase_report_counts_only_published_local_outcomes(tmp_path, origin, backend):
    config = job(tmp_path, origin, backend, paths=("a", "b"))
    for existing in (False, True):
        report = RunReport()
        result = download_url_list(config, run_report=report)
        run = report.snapshot(result["bytes_downloaded"])
        assert run["status"] == "success"
        assert run["remote_files_found"] == 2
        assert run["missing_files_downloaded"] == (0 if existing else 2)
        assert run["unclassified_downloads"] == (2 if existing else 0)
        assert run["changed_files_downloaded"] == run["uncertain_freshness_downloads"] == 0
        assert set(run["phase_seconds"]) == {"download preparation", "downloads", "cleanup"}
        assert all(seconds > 0 for seconds in run["phase_seconds"].values())
        assert sum(run["phase_seconds"].values()) <= run["elapsed_seconds"]
        assert run["elapsed_seconds"] == result["elapsed_seconds"]
        assert (
            run["download_throughput"]
            == result["bytes_downloaded"] / run["phase_seconds"]["downloads"]
        )
        assert report.finished is not None and report._active_phase is None
    origin.routes["/root/a"] = (500, {}, b"failed")
    report = RunReport()
    result = download_url_list(config, run_report=report)
    run = report.snapshot(result["bytes_downloaded"])
    assert not result["success"] and run["status"] == "failed"
    assert run["missing_files_downloaded"] == 0 and run["unclassified_downloads"] == 1
    assert result["files_failed"] == 1 and result["files_downloaded"] == 1
    assert (config.dest_path / "a").read_bytes() == b"bytes"
    assert all(method == "GET" for method, *_ in origin.requests)


def test_known_url_phase_report_finishes_on_preflight_error_and_dry_run(tmp_path, origin):
    config = job(tmp_path, origin, paths=("a",), dry_run=True)
    report = RunReport()
    result = download_url_list(config, run_report=report)
    assert result == {"success": True, "dry_run": True, "files": 1, "backend": "httpx"}
    assert report.status == "dry run" and report.finished is not None
    assert report.closed_connection_pools == 0
    assert "downloads" not in report.phases and not origin.requests
    config.dry_run = False
    config.dest_path.mkdir()
    (config.dest_path / "a").write_bytes(b"unowned")
    with pytest.raises(ValueError, match="no matching ownership receipt"):
        download_url_list(config, run_report=report)
    assert report.status == "failed" and report.finished is not None
    assert report._active_phase is None and "downloads" not in report.phases
    assert not origin.requests


def test_cli_download_uses_mirror_completion_footer_after_real_backend_close(
    tmp_path, origin, backend, monkeypatch, capsys
):
    import mirror_url.transfers as transfers

    adapter = transfers._HTTPXBackend if backend == "httpx" else transfers._AIOHTTPBackend
    original = adapter.close
    closures = []

    async def close(self):
        await original(self)
        closures.append(self)

    monkeypatch.setattr(adapter, "close", close)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "a",
            "--backend",
            backend,
            "--print-logs",
        ],
    )
    cli.main()
    captured = capsys.readouterr()
    assert captured.out == "" and len(closures) == 1
    lines = captured.err.splitlines()
    # Remove log prefixes while preserving the complete summary/trailer order.
    lines = [line.split("] [INFO] ", 1)[-1].rstrip() for line in lines]
    assert lines[-7:] == [
        "  RGET-LIST used: False",
        "  Fast parses: 0",
        "  LXML parses: 0",
        "-" * 50,
        "Sync completed successfully",
        "[1/1] ✅ Successfully processed: ROOT",
        "Closed all 1 connection pools",
    ]
    assert (tmp_path / "a").read_bytes() == b"bytes"
    receipts = list((tmp_path / ".mirror-url-state").glob("url-list-*.json"))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())["files"][str(tmp_path / "a")]
    assert receipt["size"] == 5 and receipt["url"] == origin.base + "a"


def test_backend_close_failure_cannot_emit_success_or_pool_closure(
    tmp_path, origin, backend, monkeypatch, capsys
):
    import mirror_url.transfers as transfers

    adapter = transfers._HTTPXBackend if backend == "httpx" else transfers._AIOHTTPBackend
    original = adapter.close

    async def close(self):
        await original(self)
        raise OSError("backend close failed")

    monkeypatch.setattr(adapter, "close", close)
    reports = []
    original_metrics = cli.MetricsCollector

    def metrics():
        value = original_metrics()
        reports.append(value.run_report)
        return value

    monkeypatch.setattr(cli, "MetricsCollector", metrics)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "a",
            "--backend",
            backend,
            "--print-logs",
        ],
    )
    with pytest.raises(SystemExit) as error:
        cli.main()
    captured = capsys.readouterr()
    assert error.value.code == 1 and "backend close failed" in captured.err
    assert captured.out == "" and "Sync completed successfully" not in captured.err
    assert "Closed all 1 connection pools" not in captured.err
    assert len(reports) == 1 and reports[0].status == "failed"
    assert reports[0].closed_connection_pools == 0
    assert (tmp_path / "a").read_bytes() == b"bytes"
    assert len(list((tmp_path / ".mirror-url-state").glob("url-list-*.json"))) == 1


def test_cli_dry_run_reports_no_pool_closures(tmp_path, origin, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv",
        ["mirror-url", "--mode", "download", origin.base + "a", "--print-logs", "--dry-run"],
    )
    cli.main()
    captured = capsys.readouterr()
    assert captured.out == "" and "Result: DRY RUN" in captured.err
    assert "Closed all 0 connection pools" in captured.err
    assert not origin.requests and not (tmp_path / "a").exists()
    assert not (tmp_path / ".mirror-url-state").exists()


def test_known_url_report_does_not_claim_success_when_ownership_close_fails(
    tmp_path, origin, monkeypatch
):
    original = DestinationLock.close

    def failing_close(lock):
        original(lock)
        raise OSError("ownership close failed")

    monkeypatch.setattr(DestinationLock, "close", failing_close)
    report = RunReport()
    config = job(tmp_path, origin, paths=("a",))
    with pytest.raises(OSError, match="ownership close failed"):
        download_url_list(config, run_report=report)
    assert report.status == "failed" and report.finished is not None
    assert report._active_phase is None
    assert (config.dest_path / "a").read_bytes() == b"bytes"
