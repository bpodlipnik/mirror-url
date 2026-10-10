"""End-to-end mirroring against a controllable, real HTTP directory server."""

from __future__ import annotations

import hashlib
import html
import re
import socket
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from urllib.parse import quote, unquote, urlsplit

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.enums import CleanupPolicy
from mirror_url.transport import SecureAsyncTransport, SecureTransport

pytestmark = pytest.mark.integration


@contextmanager
def local_archive(monkeypatch):
    state = SimpleNamespace(
        files={}, listings={}, requests=[], failures={}, redirects={}, drops={}, etag=True
    )
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.respond(send_body=False)

        def do_GET(self):
            self.respond(send_body=True)

        def respond(self, send_body):
            path = unquote(urlsplit(self.path).path).removeprefix("/")
            with lock:
                state.requests.append((self.command, path, httpx.Headers(self.headers.items())))
            status, body, headers = 200, b"", {}
            if path in state.failures:
                status = state.failures[path]
            elif path in state.redirects:
                status, headers = 302, {"Location": state.redirects[path]}
            elif path in state.listings:
                body = state.listings[path].encode()
                headers["Content-Type"] = "text/html"
            elif path.endswith("/") or not path:
                children = sorted(
                    {
                        name[len(path) :].split("/", 1)[0]
                        + ("/" if "/" in name[len(path) :] else "")
                        for name in state.files
                        if name.startswith(path)
                    }
                )
                body = (
                    "<html><body>"
                    + "".join(
                        f'<a href="{html.escape(quote(name), quote=True)}">{html.escape(name)}</a>'
                        for name in children
                    )
                    + "</body></html>"
                ).encode()
                headers["Content-Type"] = "text/html"
            elif path in state.files:
                body = state.files[path]
                etag = '"' + hashlib.sha256(body).hexdigest() + '"'
                headers = {
                    "Accept-Ranges": "bytes",
                    "Last-Modified": "Wed, 30 Sep 2026 12:00:00 GMT",
                }
                if state.etag:
                    headers["ETag"] = etag
                if state.etag and self.headers.get("If-None-Match") == etag:
                    status = 304
                elif self.headers.get("Range"):
                    match = re.fullmatch(r"bytes=(\d+)-(\d*)", self.headers["Range"])
                    start = int(match[1])
                    end = int(match[2]) if match[2] else len(body) - 1
                    if start >= len(body):
                        status, body = 416, b""
                    else:
                        end = min(end, len(body) - 1)
                        headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
                        status, body = 206, body[start : end + 1]
            else:
                status = 404
            self.send_response(status)
            for name, value in headers.items():
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if send_body and body and status != 304:
                with lock:
                    drop = state.drops.pop(path, None)
                try:
                    self.wfile.write(body if drop is None else body[:drop])
                    self.wfile.flush()
                    if drop is not None:
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        self.close_connection = True
                except (BrokenPipeError, ConnectionResetError):
                    pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state.url = f"http://127.0.0.1:{server.server_port}/"
    # Only these test servers bypass the production transport's private-IP rule.
    monkeypatch.setattr(SecureTransport, "handle_request", httpx.HTTPTransport.handle_request)
    monkeypatch.setattr(
        SecureAsyncTransport, "handle_async_request", httpx.AsyncHTTPTransport.handle_async_request
    )
    monkeypatch.setattr("mirror_url._core._base.HealthCheckServer.start", lambda _: None)
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def remote(monkeypatch):
    with local_archive(monkeypatch) as state:
        yield state


@pytest.fixture
def config(remote, tmp_path):
    return MirrorConfig(
        base_url=remote.url,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        security_validation=False,
        sequential_downloads=True,
        async_metadata=False,
        connection_pool_prewarm=False,
        handle_symlinks=False,
        request_delay=0.001,
        max_retries=1,
        retry_delay=1,
    )


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_real_http_chunk_and_whole_file_downloads_match_original_bytes(remote, config, mode):
    content = bytes(range(256)) * (8192 + 17)
    remote.files = {"large.bin": content, "sub/small.txt": b"small", "empty.txt": b""}
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        destination = mirror.target_dir
        assert (destination / "large.bin").read_bytes() == content
        assert (destination / "sub/small.txt").read_bytes() == b"small"
        assert (destination / "empty.txt").read_bytes() == b""
        assert mirror.files_processed.value() == 3
        assert mirror.files_failed.value() == 0
        if mode != "sequential":
            ranges = [
                headers["Range"]
                for method, path, headers in remote.requests
                if method == "GET" and path == "large.bin" and "Range" in headers
            ]
            assert len(ranges) == 2
            assert mirror.metrics.metrics["chunk_downloads"] == 2


def test_repeated_sync_uses_file_identity_and_redownloads_missing_local_file(remote, config):
    remote.files = {"a.txt": b"alpha", "b.txt": b"bravo"}
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        first_gets = sum(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )
        assert first_gets == 2
        assert mirror.sync()
        assert (
            sum(method == "GET" and path in remote.files for method, path, _ in remote.requests)
            == first_gets
        )
        assert mirror.files_skipped.value() == 2
        (mirror.target_dir / "a.txt").unlink()
        assert mirror.sync()
        assert (mirror.target_dir / "a.txt").read_bytes() == b"alpha"
        assert mirror.files_processed.value() == 1


def test_changed_remote_file_is_replaced_even_when_size_and_timestamp_match(remote, config):
    remote.files = {"a.txt": b"alpha"}
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        remote.files["a.txt"] = b"ALPHA"
        config.refresh_cache = True
        assert mirror.sync()
        assert (mirror.target_dir / "a.txt").read_bytes() == b"ALPHA"


@pytest.mark.parametrize(
    "policy",
    [CleanupPolicy.SAFE_NO_DELETE, CleanupPolicy.PREVIEW, CleanupPolicy.DELETE, CleanupPolicy.MOVE],
)
def test_empty_remote_cleanup_obeys_requested_policy(remote, config, policy):
    config.cleanup_policy = policy
    config.no_cache = True
    with MirrorURL(config) as mirror:
        old = mirror.target_dir / "obsolete.txt"
        old.write_bytes(b"keep if safe")
        assert mirror.sync()
        if policy in (CleanupPolicy.SAFE_NO_DELETE, CleanupPolicy.PREVIEW):
            assert old.read_bytes() == b"keep if safe"
        else:
            assert not old.exists()
            if policy == CleanupPolicy.MOVE:
                archives = list(mirror.target_dir.parent.glob("**/obsolete.txt"))
                assert len(archives) == 1
                assert archives[0].read_bytes() == b"keep if safe"


def test_incomplete_http_discovery_preserves_local_files_and_reports_failure(remote, config):
    remote.files = {"sub/new.txt": b"new"}
    remote.failures["sub/"] = 503
    config.cleanup_policy = CleanupPolicy.DELETE
    config.no_cache = True
    with MirrorURL(config) as mirror:
        old = mirror.target_dir / "sub/old.txt"
        old.parent.mkdir()
        old.write_bytes(b"important")
        assert mirror.sync() is False
        assert old.read_bytes() == b"important"


@pytest.mark.parametrize("name", ["a" * 70 + ".bin", "new\x01.bin", "CON.txt"])
def test_lossy_filename_cannot_overwrite_an_unrelated_local_file(remote, config, name):
    from mirror_url.security import PathSafety

    config.max_filename_len = 64
    config.cleanup_policy = CleanupPolicy.DELETE
    config.no_cache = True
    remote.files = {name: b"unrelated remote bytes"}
    with MirrorURL(config) as mirror:
        victim = mirror.target_dir / PathSafety._safe_filename(name, 64)
        victim.write_bytes(b"preserve original bytes")
        result = mirror.sync()
        assert victim.read_bytes() == b"preserve original bytes"
        assert result is False
        assert not any(
            method == "GET" and path == name for method, path, headers in remote.requests
        )


def test_malformed_real_http_listing_cannot_trigger_cleanup(remote, config):
    config.cleanup_policy = CleanupPolicy.DELETE
    config.no_cache = True
    remote.listings = {"": '<a href="http://[broken">bad</a>'}
    with MirrorURL(config) as mirror:
        victim = mirror.target_dir / "old.bin"
        victim.write_bytes(b"preserve original bytes")
        assert mirror.sync() is False
        assert mirror.scan_incomplete
        assert victim.read_bytes() == b"preserve original bytes"


def test_interrupted_http_body_is_resumed_with_range_and_strong_validator(remote, config):
    content = bytes(range(256)) * 4096
    remote.files = {"large.bin": content}
    remote.drops["large.bin"] = 256 * 1024
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert (mirror.target_dir / "large.bin").read_bytes() == content
        resumed = [
            headers
            for method, path, headers in remote.requests
            if method == "GET" and path == "large.bin" and "Range" in headers
        ]
        assert len(resumed) == 1
        assert resumed[0]["Range"].startswith("bytes=262144-")
        assert resumed[0]["If-Range"] == '"' + hashlib.sha256(content).hexdigest() + '"'


@pytest.mark.parametrize("kind", ["dry-run", "list-files", "list-dirs", "benchmark"])
def test_read_only_http_workflows_do_not_fetch_file_bodies(remote, config, kind):
    remote.files = {"a.txt": b"alpha", "sub/b.txt": b"bravo"}
    config.no_cache = True
    if kind != "benchmark":
        setattr(config, kind.replace("-", "_"), True)
    with MirrorURL(config) as mirror:
        if kind == "benchmark":
            result = mirror.benchmark()
            assert result["connection_test"]
            assert result["files_to_download"] == 2
        elif kind == "list-files":
            assert mirror.list_files()
        elif kind == "list-dirs":
            assert mirror.list_directories()
        else:
            assert mirror.sync()
        assert not (config.dest_path / "a.txt").exists()
        assert not any(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )


@pytest.mark.parametrize("adaptive", [False, True])
def test_large_listing_uses_real_async_metadata_requests(remote, config, adaptive):
    remote.files = {f"file-{i:03d}.txt": f"data-{i}".encode() for i in range(85)}
    config.async_metadata = True
    config.adaptive_async = adaptive
    config.async_workers = 4
    config.adaptive_start_concurrency = 4
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert mirror.files_processed.value() == 85
        assert mirror.sync()
        assert mirror.files_processed.value() == 0
        assert mirror.files_skipped.value() == 85
        assert mirror.metrics.metrics["async_metadata_checks"] >= 85
        assert (mirror.target_dir / "file-084.txt").read_bytes() == b"data-84"


def test_missing_files_mode_keeps_existing_content_and_only_downloads_absent_files(remote, config):
    remote.files = {"existing.txt": b"changed remote", "missing.txt": b"download me"}
    config.missing_files = True
    with MirrorURL(config) as mirror:
        existing = mirror.target_dir / "existing.txt"
        existing.write_bytes(b"keep local")
        assert mirror.sync()
        assert existing.read_bytes() == b"keep local"
        assert (mirror.target_dir / "missing.txt").read_bytes() == b"download me"
        assert not any(path == "existing.txt" for _, path, _ in remote.requests)


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize("suffixes", [[""], ["soho/gen", "soho/lasco/monthly"]])
def test_selected_existing_files_refresh_and_other_existing_files_skip(
    remote, config, mode, suffixes
):
    remote.files = {
        "sdb/soho/gen/selected.bin": b"original",
        "sdb/soho/gen/skipped.bin": b"original",
        "sdb/soho/lasco/monthly/selected.bin": b"original",
        "sdb/elsewhere/selected.bin": b"original",
    }
    config.base_url = remote.url + "sdb/"
    config.cache_html = False
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    selected = ["soho/gen/selected.bin", "soho/lasco/monthly/selected.bin"]
    # Each suffix uses the same base-relative selectors and destination root.
    for suffix in suffixes:
        cfg = type(config)(
            **{
                **config.model_dump(),
                "dir_suffix": suffix,
                "missing_files": True,
                "check_files": selected,
            }
        )
        with MirrorURL(cfg) as mirror:
            assert mirror.sync()
            expected = {
                path: body
                for path, body in remote.files.items()
                if not suffix or path.startswith("sdb/" + suffix + "/")
            }
            for path in expected:
                remote.files[path] = b"MODIFIED"
            remote.files["sdb/" + (suffix + "/" if suffix else "") + "missing.bin"] = b"new file"
            remote.requests.clear()
            assert mirror.sync()
            for path in expected:
                relative = path.removeprefix("sdb/")
                local = cfg.dest_path / relative
                assert local.read_bytes() == (b"MODIFIED" if relative in selected else b"original")
                requests = [(method, name) for method, name, _ in remote.requests if name == path]
                if relative in selected:
                    assert ("HEAD", path) in requests
                    assert ("GET", path) in requests
                else:
                    assert requests == []
            missing = cfg.dest_path / (suffix or "") / "missing.bin"
            assert missing.read_bytes() == b"new file"
            assert not list(cfg.dest_path.rglob("*.part"))
            assert not list(cfg.dest_path.rglob("work_*"))
            remote.requests.clear()
            assert mirror.sync()
            assert not any(
                method == "GET" and path in remote.files for method, path, _ in remote.requests
            )
            assert all(
                path.removeprefix("sdb/") in selected
                for method, path, _ in remote.requests
                if method == "HEAD" and path in remote.files
            )


@pytest.mark.parametrize("adaptive", [False, True])
@pytest.mark.parametrize("prewarm", [False, True])
def test_async_selected_checks_and_profiling_never_probe_unselected_existing_files(
    remote, config, adaptive, prewarm
):
    remote.files = {f"sub/file-{i:03d}.txt": b"original" for i in range(90)}
    config.async_metadata = True
    config.adaptive_async = adaptive
    config.connection_pool_prewarm = prewarm
    config.async_workers = config.adaptive_start_concurrency = 4
    selected = ["sub/file-088.txt", "sub/file-089.txt"]
    config.cache_html = False
    cfg = type(config)(**{**config.model_dump(), "missing_files": True, "check_files": selected})
    with MirrorURL(cfg) as mirror:
        assert mirror.sync()
        remote.files = dict.fromkeys(remote.files, b"MODIFIED")
        remote.files["sub/new.txt"] = b"new file"
        remote.requests.clear()
        assert mirror.sync()
        assert mirror.files_processed.value() == 3
        for path in remote.files:
            expected = (
                b"new file"
                if path == "sub/new.txt"
                else b"MODIFIED"
                if path in selected
                else b"original"
            )
            assert (cfg.dest_path / path).read_bytes() == expected
        assert not any(
            path in remote.files and path not in selected + ["sub/new.txt"]
            for _, path, _ in remote.requests
        )
        assert all(
            path in selected + ["sub/new.txt"]
            for method, path, _ in remote.requests
            if method == "HEAD" and path in remote.files
        )


def test_check_files_without_missing_files_keeps_normal_freshness_checks(remote, config):
    remote.files = {"a": b"old", "b": b"old"}
    config.check_files = ["a"]
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        remote.files = {"a": b"new", "b": b"new"}
        remote.requests.clear()
        assert mirror.sync()
        assert (config.dest_path / "a").read_bytes() == b"new"
        assert (config.dest_path / "b").read_bytes() == b"new"
        assert {
            path for method, path, _ in remote.requests if method == "HEAD" and path in remote.files
        } == {"a", "b"}


def test_failed_selected_update_preserves_local_bytes_and_receipt(remote, config):
    remote.files = {"selected.bin": b"old payload", "skipped.bin": b"unchanged"}
    config.check_files = ["selected.bin"]
    config.missing_files = True
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        local = config.dest_path / "selected.bin"
        receipt = mirror.cache_manager.get_file_metadata(local)
        remote.failures["selected.bin"] = 503
        remote.requests.clear()
        assert not mirror.sync()
        assert local.read_bytes() == b"old payload"
        assert mirror.cache_manager.get_file_metadata(local) == receipt
        assert not any(path == "skipped.bin" for _, path, _ in remote.requests)
        assert not list(config.dest_path.rglob("*.part"))
