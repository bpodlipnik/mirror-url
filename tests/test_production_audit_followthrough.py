"""Production regressions: validators, shared state, diagnostics and shutdown."""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL, download_url_list
from mirror_url.cache import CacheManager
from mirror_url.domain_health import DomainHealthTracker
from mirror_url.health import HealthCheckServer
from mirror_url.metrics import MetricsCollector
from test_head_disconnect_recovery import configuration, request_with_mock
from test_transfer_backends import origin as origin


@pytest.fixture
def restore_logging():
    handlers, level = logging.root.handlers[:], logging.root.level
    # basicConfig(force=True) would close pytest's handlers too.
    logging.root.handlers = []
    try:
        yield
    finally:
        for handler in logging.root.handlers[:]:
            handler.close()
        logging.root.handlers = handlers
        logging.root.setLevel(level)


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("weak", [False, True])
@pytest.mark.parametrize("verify_content", [False, True])
@pytest.mark.parametrize("last_modified", [False, True])
def test_validator_round_trip_updates_equal_size_file(
    tmp_path, origin, backend, weak, verify_content, last_modified
):
    origin.routes["/root/"] = (200, {"Content-Type": "text/html"}, b'<a href="a.bin">a.bin</a>')
    modified = {"Last-Modified": "Wed, 30 Sep 2026 12:00:00 GMT"} if last_modified else {}
    tag = ("W/" if weak else "") + '"v1"'
    origin.routes["/root/a.bin"] = (200, {"ETag": tag, **modified}, b"AAA")
    config = MirrorConfig(
        base_url=origin.base,
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        backend=backend,
        verify_content=verify_content,
        sequential_downloads=True,
        async_metadata=False,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
    )
    for expected in (b"AAA", b"AAA", b"BBB"):
        if expected == b"BBB":
            tag = ("W/" if weak else "") + '"v2"'
            origin.routes["/root/a.bin"] = (200, {"ETag": tag, **modified}, expected)
        before = sum(
            method == "GET" and path == "/root/a.bin" for method, path, *_ in origin.requests
        )
        with MirrorURL(config) as mirror:
            assert mirror.sync()
            assert mirror.cache_manager.get_file_metadata(config.dest_path / "a.bin")["etag"] == tag
        assert (config.dest_path / "a.bin").read_bytes() == expected
        after = sum(
            method == "GET" and path == "/root/a.bin" for method, path, *_ in origin.requests
        )
        if before and expected == b"AAA":
            assert after == before, "unchanged validators must avoid another file GET"


@pytest.mark.parametrize("kind", ["file", "symlink", "hardlink"])
def test_domain_health_preserves_preexisting_temporary_entries(tmp_path, kind):
    victim = tmp_path / "valuable"
    victim.write_bytes(b"keep this")
    state = tmp_path / "health.json"
    adjacent = state.with_suffix(".json.tmp")
    if kind == "symlink":
        try:
            adjacent.symlink_to(victim)
        except OSError:
            pytest.skip("symlinks unavailable")
    elif kind == "hardlink":
        os.link(victim, adjacent)
    else:
        adjacent.write_bytes(b"keep this")
    DomainHealthTracker(state).record_incident("example.test")
    assert victim.read_bytes() == adjacent.read_bytes() == b"keep this"
    assert json.loads(state.read_text())["domains"]["example.test"]
    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "health.json",
        "health.json.tmp",
        "valuable",
    ]


@pytest.mark.parametrize("kind", ["file", "symlink", "hardlink"])
def test_cache_recovery_preserves_existing_timestamp_backup(tmp_path, monkeypatch, kind):
    path = tmp_path / "listing.json"
    path.write_text("{broken")
    existing = tmp_path / "listing.json.corrupted.123"
    victim = tmp_path / "valuable"
    victim.write_text("KEEP EXISTING BACKUP")
    if kind == "symlink":
        try:
            existing.symlink_to(victim)
        except OSError:
            pytest.skip("symlinks unavailable")
    elif kind == "hardlink":
        os.link(victim, existing)
    else:
        existing.write_bytes(victim.read_bytes())
    monkeypatch.setattr("mirror_url.cache.time.time", lambda: 123)
    config = configuration(tmp_path)
    assert CacheManager(path, config, MetricsCollector()).load() == (False, None)
    assert existing.read_text() == victim.read_text() == "KEEP EXISTING BACKUP"
    assert any(p.read_text() == "{broken" for p in tmp_path.glob("*.corrupted.*"))


def test_cache_recovery_reservation_failure_preserves_corrupt_source(tmp_path, monkeypatch):
    path = tmp_path / "listing.json"
    path.write_text("{broken")

    def fail(*args, **kwargs):
        raise OSError("cannot reserve a backup")

    monkeypatch.setattr("mirror_url.cache.tempfile.mkstemp", fail)
    assert CacheManager(path, configuration(tmp_path), MetricsCollector()).load() == (False, None)
    assert path.read_text() == "{broken"
    assert list(tmp_path.iterdir()) == [path]


def test_domain_health_overlapping_writers_publish_complete_documents(tmp_path, monkeypatch):
    import mirror_url.domain_health as module

    path = tmp_path / "health.json"
    barrier = threading.Barrier(2)
    original_dump = json.dump

    def overlapping_dump(payload, stream, **kwargs):
        # Both streams are open before either writes. Shared staging inodes
        # corrupt the shorter publication or make one replacement fail.
        barrier.wait(timeout=5)
        original_dump(payload, stream, **kwargs)

    monkeypatch.setattr(module.json, "dump", overlapping_dump)
    trackers = [DomainHealthTracker(path), DomainHealthTracker(path)]
    trackers[0]._data = {"long.example": list(range(1000))}
    trackers[1]._data = {"short.example": [1]}
    published = []
    replace = Path.replace

    def observe_replace(source, destination):
        result = replace(source, destination)
        published.append(json.loads(path.read_text()))
        return result

    monkeypatch.setattr(Path, "replace", observe_replace)
    with ThreadPoolExecutor(2) as executor:
        list(executor.map(lambda tracker: tracker._save(), trackers))
    assert len(published) == 2
    assert all(document["domains"] in [t._data for t in trackers] for document in published)
    assert json.loads(path.read_text())["domains"] in [t._data for t in trackers]
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("failure", ["create", "fdopen", "dump", "flush", "fsync", "replace"])
def test_domain_health_write_failures_preserve_previous_state(tmp_path, monkeypatch, failure):
    import mirror_url.domain_health as module

    path = tmp_path / "health.json"
    tracker = DomainHealthTracker(path)
    tracker.record_incident("previous.example")
    previous = path.read_bytes()

    def fail(*args, **kwargs):
        raise OSError("injected storage failure")

    if failure == "create":
        monkeypatch.setattr(module.tempfile, "mkstemp", fail)
    elif failure == "fdopen":
        monkeypatch.setattr(module.os, "fdopen", fail)
    elif failure == "dump":
        monkeypatch.setattr(module.json, "dump", fail)
    elif failure == "fsync":
        monkeypatch.setattr(module.os, "fsync", fail)
    elif failure == "flush":
        from contextlib import contextmanager
        from unittest.mock import Mock

        fdopen = module.os.fdopen

        @contextmanager
        def failing_flush(*args, **kwargs):
            with fdopen(*args, **kwargs) as stream:
                wrapper = Mock(wraps=stream)
                wrapper.flush.side_effect = fail
                yield wrapper

        monkeypatch.setattr(module.os, "fdopen", failing_flush)
    else:
        monkeypatch.setattr(Path, "replace", fail)
    tracker.record_incident("new.example")
    assert path.read_bytes() == previous
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("sharing_violation", [False, True])
def test_domain_health_separate_process_writers_leave_valid_json(
    tmp_path, monkeypatch, sharing_violation
):
    path = tmp_path / "health.json"
    DomainHealthTracker(path).record_incident("initial.example")
    if sharing_violation:
        read_text = Path.read_text
        interrupted = False

        def interrupted_read(file, *args, **kwargs):
            nonlocal interrupted
            if file == path and not interrupted:
                interrupted = True
                raise PermissionError("injected replacement sharing violation")
            return read_text(file, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", interrupted_read)
    program = """
import sys
from pathlib import Path
from mirror_url.domain_health import DomainHealthTracker
tracker = DomainHealthTracker(Path(sys.argv[1]))
for iteration in range(40):
    tracker.record_incident(sys.argv[2] + '.example')
"""
    writers = [
        subprocess.Popen(
            [sys.executable, "-c", program, str(path), name],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for name in ("a", "long-domain-name" * 10, "c")
    ]
    try:
        deadline = time.monotonic() + 20
        observations = 0
        while any(writer.poll() is None for writer in writers):
            assert time.monotonic() < deadline, "domain-health writer did not finish"
            try:
                snapshot = path.read_text(encoding="utf-8")
            except PermissionError:
                # Windows can briefly deny opening a name being replaced.
                # Retry access only; malformed or mixed JSON must still fail.
                pass
            else:
                assert json.loads(snapshot)["_meta"]["version"] == 1
                observations += 1
            time.sleep(0.001)
        for writer in writers:
            stdout, stderr = writer.communicate(timeout=2)
            assert writer.returncode == 0, (stdout, stderr)
        assert observations > 0
        assert json.loads(path.read_text(encoding="utf-8"))["domains"]
        assert list(tmp_path.iterdir()) == [path]
    finally:
        for writer in writers:
            if writer.poll() is None:
                writer.kill()
            writer.communicate(timeout=5)


@pytest.mark.parametrize("mode", ["sync", "fixed", "adaptive"])
@pytest.mark.parametrize("failure", ["status", "transport"])
def test_request_errors_redact_metrics(tmp_path, monkeypatch, mode, failure):
    metrics = MetricsCollector()
    config = configuration(tmp_path, max_retries=0)
    monkeypatch.setattr("socket.gethostbyname", lambda _: "8.8.8.8")

    def handler(request):
        if failure == "transport":
            raise httpx.ConnectError(f"failed for {request.url}", request=request)
        response = httpx.Response(403, request=request)
        if mode != "sync":
            response.raise_for_status()
        return response

    try:
        request_with_mock(mode, config, metrics, handler, monkeypatch)
    except (httpx.HTTPError, ConnectionError):
        pass
    summary = metrics.get_summary()
    assert summary["errors"]
    assert "private-token" not in str(summary)
    assert "private-fragment" not in str(summary)
    output = tmp_path / "metrics.json"
    assert metrics.export_json(output, config)
    assert "private-token" not in output.read_text()


@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize("flag", ["--verbose", "--debug", None])
def test_real_log_handlers_redact_tracebacks_and_honor_levels(
    tmp_path, restore_logging, capsys, shared, flag
):
    from mirror_url.cli import setup_shared_logging

    argv = [
        "--url",
        "https://example.com/root/",
        "--dest-path",
        str(tmp_path / "dest"),
        "--log-path",
        str(tmp_path / "logs"),
        "--print-logs",
    ]
    if flag:
        argv.append(flag)
    mirror = None
    if shared:
        helper = Path(__file__).resolve().parents[1] / "scripts" / "render_site.py"
        spec = importlib.util.spec_from_file_location("audit_site_renderer", helper)
        assert spec is not None and spec.loader is not None
        renderer = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(renderer)
        setup_shared_logging(renderer.capture_parser().parse_args(argv))
    else:
        mirror = MirrorURL(
            MirrorConfig(
                base_url="https://example.com/root/",
                dest_path=tmp_path / "dest",
                log_path=tmp_path / "logs",
                verbose=flag == "--verbose",
                debug=flag == "--debug",
                print_logs=True,
                connection_pool_prewarm=False,
            )
        )
    try:
        logging.debug("VERBOSE_AUDIT_DETAIL")
        try:
            raise ValueError(
                "failed https://user:private-password@example.com/a?key=private-token#private-fragment"
            )
        except ValueError:
            logging.exception("OUTER_ERROR")
        for handler in logging.root.handlers:
            handler.flush()
        logfile = next((tmp_path / "logs").glob("*.log")).read_text(encoding="utf-8")
        console = capsys.readouterr().err
        for rendered in (logfile, console):
            assert "OUTER_ERROR" in rendered and "ValueError" in rendered
            assert "example.com/a" in rendered
            assert all(
                secret not in rendered
                for secret in ("private-password", "private-token", "private-fragment")
            )
            assert ("VERBOSE_AUDIT_DETAIL" in rendered) == bool(flag)
    finally:
        if mirror:
            mirror.cleanup()


@pytest.mark.parametrize("partial", [b"", b"GET /health HTTP/1.1\r\nHost: localhost\r\n"])
def test_health_slow_clients_do_not_block_probes_or_shutdown(partial):
    server = HealthCheckServer(object(), port=0)
    server.start()
    deadline = time.monotonic() + 5
    while server.server is None and time.monotonic() < deadline:
        time.sleep(0.01)
    client = probe = None
    try:
        assert server.server is not None
        address = server.server.server_address
        client = socket.create_connection(address, timeout=2)
        client.sendall(partial)
        probe = socket.create_connection(address, timeout=2)
        probe.sendall(b"GET /health HTTP/1.0\r\n\r\n")
        assert b"503" in probe.recv(4096)
        stopped = threading.Event()
        worker = threading.Thread(target=lambda: (server.stop(), stopped.set()), daemon=True)
        worker.start()
        assert stopped.wait(1), "shutdown blocked on a slow health client"
        assert not server.thread.is_alive()
        client.settimeout(0.2)
        assert client.recv(1024) == b"", "shutdown left an accepted health client open"
        server.stop()
    finally:
        if client:
            client.close()
        if probe:
            probe.close()
        server.stop()


@pytest.mark.parametrize(
    "text",
    [
        "GET 'https://u:password@example.com/a?token=secret#fragment': redirect https://example.net/b?q=another; https://[bad?leak=secret",
        '{"a":"https://u:password@example.com/a","b":"https://v:secret@example.net/b"}',
        '{"a":"https://example.com/a?q=another","b":"https://v:secret@example.net/b"}',
        "('https://example.com/a','https://v:secret@example.net/b?q=another')",
        "https://example.com/a,HTTPS://v:secret@example.net/b",
        "https://[bad?leak=secret,https://v:password@example.net/b?q=another",
    ],
)
def test_log_text_sanitizer_handles_multiple_urls_and_bad_authorities(text):
    from mirror_url.utils import sanitize_log_text

    clean = sanitize_log_text(text)
    assert all(secret not in clean for secret in ("password", "secret", "fragment", "another"))
    assert "example.net/b" in clean
    assert sanitize_log_text(clean) == clean


def test_progress_debug_uses_supplied_config(tmp_path, caplog):
    from mirror_url.progress import ProgressTracker

    with caplog.at_level(logging.DEBUG):
        tracker = ProgressTracker(3, use_tqdm=False, config=configuration(tmp_path, debug=True))
        tracker.update()
    assert "Progress.update(1): 0 -> 1/3" in caplog.text


def test_health_idle_clients_expire_and_release_capacity(monkeypatch):
    from mirror_url.health import HealthCheckHandler, _MirrorHTTPServer

    monkeypatch.setattr(_MirrorHTTPServer, "MAX_CLIENTS", 1)
    monkeypatch.setattr(_MirrorHTTPServer, "CLIENT_TIMEOUT", 0.2)
    accepted = threading.Event()
    setup = HealthCheckHandler.setup

    def announce(handler):
        setup(handler)
        accepted.set()

    monkeypatch.setattr(HealthCheckHandler, "setup", announce)
    with _MirrorHTTPServer(("localhost", 0), HealthCheckHandler, object()) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            with socket.create_connection(server.server_address, timeout=2) as idle:
                assert accepted.wait(2)
                with socket.create_connection(server.server_address, timeout=2) as excess:
                    assert excess.recv(1024) == b"", "excess clients must not allocate workers"
                assert idle.recv(1024) == b"", "idle clients must time out"
            # An expired client must release its slot for subsequent probes.
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                with socket.create_connection(server.server_address, timeout=2) as probe:
                    probe.sendall(b"GET /health HTTP/1.0\r\n\r\n")
                    try:
                        if b"503" in probe.recv(4096):
                            break
                    except ConnectionResetError:
                        pass
                time.sleep(0.01)
            else:
                pytest.fail("health request capacity was not released")
        finally:
            server.shutdown()
            worker.join(2)


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("failure", ["status", "transport"])
def test_failed_download_public_result_redacts_url(tmp_path, origin, monkeypatch, backend, failure):
    if failure == "transport":
        from contextlib import asynccontextmanager

        from mirror_url.transfers import _AIOHTTPBackend, _HTTPXBackend

        @asynccontextmanager
        async def fail_open(adapter, url):
            raise httpx.ConnectError(f"failed request to {url}")
            yield  # pragma: no cover - async context-manager protocol

        monkeypatch.setattr(
            _AIOHTTPBackend if backend == "aiohttp" else _HTTPXBackend, "open", fail_open
        )
    origin.routes["/root/a?token=private-token"] = (403, {}, b"denied")
    config = MirrorConfig(
        mode="download",
        base_url=origin.base,
        backend=backend,
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        download_url=origin.base + "a?token=private-token",
        max_retries=0,
        requests_per_second=0,
        request_delay=0,
    )
    result = download_url_list(config)
    assert not result["success"]
    assert "private-token" not in str(result["failures"])
