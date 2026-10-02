import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.cache import CacheManager
from mirror_url.connection import ConnectionManager
from mirror_url.enums import CleanupPolicy
from mirror_url.health import HealthCheckServer
from mirror_url.metrics import MetricsCollector
from mirror_url.primitives import AtomicCounter


@pytest.fixture
def build(tmp_path, monkeypatch):
    monkeypatch.setattr(MirrorURL, "test_connection", lambda self: True)
    monkeypatch.setattr("socket.gethostbyname", lambda _: "93.184.216.34")
    mirrors = []

    def create(**options):
        config = MirrorConfig(
            base_url=options.pop("base_url", "https://example.com/root/"),
            dest_path=tmp_path / "data",
            log_path=tmp_path / "logs",
            async_metadata=False,
            sequential_downloads=True,
            connection_pool_prewarm=False,
            security_validation=False,
            max_retries=0,
            **options,
        )
        mirror = MirrorURL(config)
        mirrors.append(mirror)
        return mirror

    yield create
    for mirror in mirrors:
        mirror.cleanup()


@pytest.mark.parametrize("root", ["my%20files", "%C3%A9", "literal%2520"])
def test_encoded_root_preserves_current_remote_file(build, monkeypatch, root):
    mirror = build(base_url=f"https://example.com/{root}/", cleanup_policy=CleanupPolicy.DELETE)
    file = mirror.target_dir / "keep.txt"
    file.write_bytes(b"current")

    def respond(url, method="GET", **kwargs):
        return httpx.Response(
            200,
            request=httpx.Request(method, url),
            headers={"Content-Type": "text/html"},
            content=b"current" if url.endswith("keep.txt") else b'<a href="keep.txt">keep.txt</a>',
        )

    monkeypatch.setattr(mirror.connection_manager, "request", respond)
    assert mirror.sync() is True
    assert mirror.scan_incomplete is False
    assert file.exists()
    assert file.read_bytes() == b"current"


def test_normalization_preserves_literal_percent_filename():
    manager = object.__new__(ConnectionManager)
    original = "https://example.com/root/a%2520b.txt"
    normalized = manager._normalize_url(original)
    assert normalized == original


def test_internal_symlink_blocks_download_to_unrelated_local_file(build, monkeypatch):
    mirror = build()
    real = mirror.target_dir / "unrelated.txt"
    real.write_bytes(b"old")
    link = mirror.target_dir / "remote.txt"
    link.symlink_to(real.name)
    url = mirror.target_base_url + "remote.txt"
    with pytest.raises(ValueError, match="unsafe"):
        mirror._validate_remote_paths([url])
    assert mirror._get_local_path_from_url(url) is None
    assert real.read_bytes() == b"old"
    assert link.is_symlink()


def test_rejected_expired_cache_discards_metadata(tmp_path):
    config = MirrorConfig(
        base_url="https://example.com/root", dest_path=tmp_path, log_path=tmp_path, cache_max_age=1
    )
    file = tmp_path / "file"
    file.write_bytes(b"x")
    manager = CacheManager(tmp_path / "cache.json", config, MetricsCollector())
    manager.save_file_metadata(file, '"old"', 0, 1)
    assert manager.save({}, 1)
    payload = json.loads(manager.cache_file.read_text())
    payload["_meta"]["last_full_run"] = (datetime.now() - timedelta(days=10)).isoformat()
    manager.cache_file.write_text(json.dumps(payload))
    fresh = CacheManager(manager.cache_file, config, MetricsCollector())
    assert fresh.load() == (False, None)
    assert fresh.get_file_metadata(file) is None


def test_atomic_counter_is_unhashable():
    a, b = AtomicCounter(1), AtomicCounter(1)
    assert a == b
    with pytest.raises(TypeError):
        hash(a)


def test_health_stop_before_bind_prevents_serving(monkeypatch):
    import threading

    entered, release, serving = threading.Event(), threading.Event(), threading.Event()
    fake = Mock()
    fake.serve_forever.side_effect = serving.set

    def constructor(*args):
        entered.set()
        assert release.wait(3)
        return fake

    monkeypatch.setattr("mirror_url.health._MirrorHTTPServer", constructor)
    server = HealthCheckServer(SimpleNamespace())
    server.start()
    assert entered.wait(3)
    server.stop()
    release.set()
    server.thread.join(3)
    assert not serving.is_set()
    fake.shutdown.assert_not_called()
    fake.server_close.assert_called_once()


@pytest.mark.parametrize("adaptive", [False, True])
def test_async_retry_records_domain_health(tmp_path, monkeypatch, adaptive):
    import asyncio

    from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager

    config = MirrorConfig(
        base_url="https://example.com/root/",
        dest_path=tmp_path,
        log_path=tmp_path,
        security_validation=False,
        max_retries=1,
    )
    tracker = Mock()
    monkeypatch.setattr("mirror_url.async_connection.get_domain_health_tracker", lambda: tracker)
    monkeypatch.setattr("mirror_url.async_connection.exponential_backoff", lambda *a: 0)
    statuses = iter([429, 200])
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(next(statuses), request=req))
    )
    manager = (AdaptiveAsyncManager if adaptive else AsyncConnectionManager)(
        config, MetricsCollector()
    )
    manager._client = client
    if adaptive:
        manager._client_initialized = True

    async def run():
        try:
            result = await manager.head(config.base_url + "/a")
            assert result.status_code == 200
        finally:
            await client.aclose()

    asyncio.run(run())
    tracker.record_incident.assert_called_once_with("example.com")


def test_typo_in_symlink_mode_is_rejected(tmp_path):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        MirrorConfig(
            base_url="https://example.com/root/",
            dest_path=tmp_path,
            log_path=tmp_path,
            handle_symlinks=True,
            symlink_mode="skpi",
        )


def test_traditional_chunk_metrics_count_bytes_once(build, monkeypatch):
    mirror = build(min_chunk_size_mb=1, max_chunks_per_file=2)
    manager = mirror.parallel_manager
    manager.enabled = True
    size = 2 * 1024 * 1024
    etag = '"same"'

    def request(url, method="GET", headers=None, **kw):
        response_headers = {"ETag": etag, "Accept-Ranges": "bytes", "Content-Length": str(size)}
        status, content = 200, b""
        if method == "GET":
            start, end = map(int, headers["Range"][len("bytes=") :].split("-"))
            status = 206
            content = b"x" * (end - start + 1)
            response_headers.update(
                {
                    "Content-Range": f"bytes {start}-{end}/{size}",
                    "Content-Length": str(len(content)),
                }
            )
        return httpx.Response(
            status, request=httpx.Request(method, url), headers=response_headers, content=content
        )

    monkeypatch.setattr(mirror.connection_manager, "request", request)
    download = manager.create_chunks(
        mirror.target_base_url + "large.bin", mirror.target_dir / "large.bin", size
    )
    assert manager.download_parallel(download)
    assert mirror.total_downloaded_size.value() == size
    assert mirror.metrics.get_summary()["bytes_downloaded"] == size


def test_shared_thread_pool_setting_enables_pool(build):
    mirror = build(use_shared_thread_pool=True)
    assert mirror.concurrency_manager.shared_pool is not None
    assert mirror.parallel_manager.own_executor is False


@pytest.mark.parametrize("cached", [False, True])
def test_expired_file_entry_cannot_be_revived_by_backing_dictionary(tmp_path, cached):
    config = MirrorConfig(
        base_url="https://example.com/root/", dest_path=tmp_path, log_path=tmp_path, cache_max_age=1
    )
    manager = CacheManager(tmp_path / "cache.json", config, MetricsCollector())
    local = tmp_path / "file"
    key = str(local.resolve())
    entry = {"etag": '"old"', "updated": (datetime.now() - timedelta(days=2)).isoformat()}
    manager.file_metadata_cache[key] = entry
    if cached:
        manager.lru_file_cache.put(key, entry)
    assert manager.get_file_metadata(local) is None
    assert key not in manager.file_metadata_cache
    assert manager.lru_file_cache.get(key) is None


def test_ancestor_symlink_cannot_redirect_remote_destination(build):
    mirror = build()
    other = mirror.target_dir / "unrelated"
    other.mkdir()
    (other / "file.txt").write_bytes(b"keep")
    (mirror.target_dir / "remote").symlink_to(other.name, target_is_directory=True)
    url = mirror.target_base_url + "remote/file.txt"
    with pytest.raises(ValueError, match="unsafe"):
        mirror._validate_remote_paths([url])
    assert (other / "file.txt").read_bytes() == b"keep"


@pytest.mark.parametrize("multiplier, expected", [(0, 1), (0.5, 1.1), (1, 1.2)])
def test_chunk_multiplier_controls_pacing(monkeypatch, multiplier, expected):
    from mirror_url.rate_limiter import ChunkAwareRateLimiter

    limiter = ChunkAwareRateLimiter(delay=1, per_ip=True, chunk_multiplier=multiplier)
    limiter.register_chunk_start("ip")
    limiter.register_chunk_start("ip")
    limiter.ip_last_requests["ip"] = 10
    monkeypatch.setattr("mirror_url.rate_limiter.time.time", lambda: 10)
    sleep = Mock()
    monkeypatch.setattr("mirror_url.rate_limiter.time.sleep", sleep)
    limiter.wait("ip")
    assert sleep.call_args.args[0] == pytest.approx(expected)
