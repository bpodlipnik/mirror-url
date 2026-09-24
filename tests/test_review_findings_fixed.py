"""Regression tests for latent bugs found during external code review passes.

Started as five fixes from the first review pass; grew with each follow-up
review. See CHANGELOG.md for the narrative behind each fix -- filenames
below roughly track when each section was added:
  - fix 1-5 (v3.1.54): ConnectionManager.target_parsed, module-level URL
    parse cache, exception chaining, signal handler exit paths, SymlinkTracker
    .record_skip
  - fix 1 follow-up (v3.1.55): check_base=False branch removed entirely
  - fix 4 follow-up (v3.1.56): bounded_executor_shutdown
  - LRUCache._timestamps / __contains__ TTL (v3.1.58, alongside the file
    rename from test_five_latent_bugs_fixed.py -- the "five" no longer fit)
  - dead circuit_breaker attr / health reporting (v3.1.59): removed
    always-None self.circuit_breaker; health reports from
    circuit_breaker_manager; downloads dead pre-check removed

Grouped in one file since the fixes aren't related in implementation, only
in how they were discovered (external review passes on the same codebase).
"""

from __future__ import annotations

import sys
import threading
import time

import pytest

# ---------------------------------------------------------------------------
# 1. ConnectionManager._is_url_within_scope no longer takes a check_base
#    parameter or depends on self.target_parsed at all -- the check_base=False
#    branch was unreachable dead code (no caller ever used it) that read a
#    self.target_parsed attribute __init__ never set. Follow-up to the earlier
#    defensive fix (which only silenced the AttributeError): removed the
#    parameter and the branch entirely instead of keeping permanently-dead
#    code around. See REFACTORING_PLAN.md §4.1.
# ---------------------------------------------------------------------------


def _build_connection_manager():
    from mirror_url.config import MirrorConfig
    from mirror_url.connection import ConnectionManager
    from mirror_url.metrics import MetricsCollector

    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path="/tmp/does-not-matter",
        log_path="/tmp/does-not-matter",
        no_cache=True,
    )
    return ConnectionManager(config, MetricsCollector())


def test_connection_manager_has_no_target_parsed_attribute():
    mgr = _build_connection_manager()
    # There is no target-scope concept in ConnectionManager at all now --
    # not even set to None -- so nothing should reference it.
    assert not hasattr(mgr, "target_parsed")


def test_is_url_within_scope_no_longer_accepts_check_base():
    mgr = _build_connection_manager()
    with pytest.raises(TypeError):
        mgr._is_url_within_scope("https://example.test/data/file.txt", check_base=False)


def test_is_url_within_scope_checks_base_scope():
    mgr = _build_connection_manager()
    assert mgr._is_url_within_scope("https://example.test/data/file.txt") is True
    assert mgr._is_url_within_scope("https://other.test/file.txt") is False


# ---------------------------------------------------------------------------
# 2. UrlsMixin._parse_url_cached delegates to a module-level, instance-
#    independent cache instead of leaking one lru_cache entry per instance.
# ---------------------------------------------------------------------------


def test_parse_url_cached_shared_across_instances_no_per_instance_leak():
    from mirror_url._core.urls import _parse_url_cached_module

    _parse_url_cached_module.cache_clear()

    from mirror_url._core.urls import UrlMixin

    class _Host(UrlMixin):
        pass

    a, b = _Host(), _Host()
    url = "https://example.test/data/file.txt?x=1"

    result_a = a._parse_url_cached(url)
    info_after_a = _parse_url_cached_module.cache_info()
    assert info_after_a.currsize == 1
    assert info_after_a.misses == 1

    # Same URL from a *different* instance must hit the shared cache, not
    # create a second entry keyed on (self, url).
    result_b = b._parse_url_cached(url)
    info_after_b = _parse_url_cached_module.cache_info()
    assert info_after_b.currsize == 1  # still one entry, not two
    assert info_after_b.hits == 1

    assert result_a == result_b
    assert result_a.path == "/data/file.txt"


# ---------------------------------------------------------------------------
# 3. Exception chaining: spot-check a couple of the 14 fixed raise sites.
# ---------------------------------------------------------------------------


def test_config_error_chains_original_regex_error():
    from mirror_url.config import MirrorConfig
    from mirror_url.exceptions import ConfigError

    with pytest.raises(ConfigError) as exc_info:
        MirrorConfig(
            base_url="https://example.test/",
            dest_path="/tmp/does-not-matter",
            log_path="/tmp/does-not-matter",
            file_filters=["[invalid("],
        )
    assert exc_info.value.__cause__ is not None
    assert isinstance(exc_info.value.__cause__, Exception)


def test_security_error_chains_gaierror(monkeypatch):
    import socket

    from mirror_url.exceptions import SecurityError
    from mirror_url.security import SecurityValidator

    def _raise_gaierror(*args, **kwargs):
        raise socket.gaierror("nope")

    monkeypatch.setattr(socket, "getaddrinfo", _raise_gaierror)

    with pytest.raises(SecurityError) as exc_info:
        SecurityValidator.resolve_and_validate_hostname("nonexistent.invalid.example")
    assert isinstance(exc_info.value.__cause__, socket.gaierror)


# ---------------------------------------------------------------------------
# 4. _signal_handler: exits on both the success path and the timeout path,
#    with distinct exit codes, instead of only exiting (code 0) on timeout.
# ---------------------------------------------------------------------------


class _FakeMirror:
    """Minimal stand-in exposing only what _signal_handler touches."""

    def __init__(self, cleanup_delay: float = 0.0, cleanup_raises: bool = False):
        self.cleanup_delay = cleanup_delay
        self.cleanup_raises = cleanup_raises
        self.cleanup_called = threading.Event()

    def cleanup(self):
        self.cleanup_called.set()
        if self.cleanup_delay:
            time.sleep(self.cleanup_delay)
        if self.cleanup_raises:
            raise RuntimeError("boom")


def _call_signal_handler(fake_mirror, monkeypatch, wait_timeout_override=None):
    from mirror_url._core._base import _MirrorBase

    exit_codes = []
    monkeypatch.setattr(
        sys,
        "exit",
        lambda code=0: exit_codes.append(code) or (_ for _ in ()).throw(SystemExit(code)),
    )

    if wait_timeout_override is not None:
        import threading as _threading

        real_event_cls = _threading.Event

        class _FastEvent(real_event_cls):
            def wait(self, timeout=None):
                return super().wait(timeout=wait_timeout_override)

        monkeypatch.setattr(_threading, "Event", _FastEvent)

    with pytest.raises(SystemExit):
        _MirrorBase._signal_handler(fake_mirror, 2, None)
    return exit_codes


def test_signal_handler_exits_zero_on_clean_shutdown(monkeypatch):
    """Previously: on a completed cleanup, the handler returned without
    calling sys.exit() at all, so SIGINT/SIGTERM just ran cleanup() and
    execution resumed as if nothing had happened."""
    fake_mirror = _FakeMirror(cleanup_delay=0.0)
    codes = _call_signal_handler(fake_mirror, monkeypatch)
    assert fake_mirror.cleanup_called.is_set()
    assert codes == [0]


def test_signal_handler_exits_nonzero_on_timeout(monkeypatch):
    """Previously: sys.exit(0) even though shutdown was forced by timeout,
    indistinguishable from a clean shutdown to callers checking exit codes."""
    fake_mirror = _FakeMirror(cleanup_delay=999)  # never finishes in time
    codes = _call_signal_handler(fake_mirror, monkeypatch, wait_timeout_override=0.01)
    assert codes == [1]


# ---------------------------------------------------------------------------
# 5. SymlinkTracker.record_skip actually records something now.
# ---------------------------------------------------------------------------


def test_record_skip_increments_total_skipped():
    from mirror_url.security import SymlinkTracker

    t = SymlinkTracker()
    assert t.get_stats()["total_skipped"] == 0

    t.record_skip("https://example.test/link1")
    t.record_skip("https://example.test/link2")

    stats = t.get_stats()
    assert stats["total_skipped"] == 2
    # Must not affect the unrelated "followed" counter.
    assert stats["total_followed"] == 0


def test_record_skip_does_not_affect_record_follow_counter():
    from mirror_url.security import SymlinkTracker

    t = SymlinkTracker()
    t.record_follow("s1", "d1", depth=1)
    t.record_skip("s2")

    stats = t.get_stats()
    assert stats["total_followed"] == 1
    assert stats["total_skipped"] == 1


# ---------------------------------------------------------------------------
# 4b. bounded_executor_shutdown: ThreadPoolExecutor.shutdown(wait=True) has
#    no timeout of its own and blocks indefinitely on a stuck worker.
#    UnifiedConcurrencyManager.shutdown() and ParallelDownloadManager.shutdown()
#    both relied on this unconditionally; a single hung worker thread could
#    silently consume the entire 30s budget the signal handler gives
#    cleanup() overall. Both now delegate to utils.bounded_executor_shutdown,
#    which joins the blocking shutdown call in its own thread with a timeout.
# ---------------------------------------------------------------------------


def test_bounded_executor_shutdown_returns_true_when_prompt():
    from concurrent.futures import ThreadPoolExecutor

    from mirror_url.utils import bounded_executor_shutdown

    executor = ThreadPoolExecutor(max_workers=2)
    executor.submit(lambda: None)

    result = bounded_executor_shutdown(executor, timeout=5.0, name="test-executor")
    assert result is True


def test_bounded_executor_shutdown_returns_false_and_does_not_block_on_stuck_worker():
    from concurrent.futures import ThreadPoolExecutor

    from mirror_url.utils import bounded_executor_shutdown

    release = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(release.wait)  # blocks until we release it below

    start = time.monotonic()
    result = bounded_executor_shutdown(executor, timeout=0.2, name="stuck-executor")
    elapsed = time.monotonic() - start

    assert result is False
    # The call itself must return promptly (bounded by `timeout`), not hang
    # until the stuck worker finishes -- this is the actual bug being fixed.
    assert elapsed < 2.0

    release.set()  # let the background worker (and shutdown thread) finish
    executor.shutdown(wait=True)


def test_parallel_download_manager_shutdown_accepts_timeout():
    from mirror_url.config import MirrorConfig
    from mirror_url.connection import ConnectionManager
    from mirror_url.download import ParallelDownloadManager
    from mirror_url.metrics import MetricsCollector
    from mirror_url.rate_limiter import BandwidthLimiter

    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path="/tmp/does-not-matter",
        log_path="/tmp/does-not-matter",
        no_cache=True,
    )
    metrics = MetricsCollector()
    conn = ConnectionManager(config, metrics)
    mgr = ParallelDownloadManager(
        config=config,
        metrics=metrics,
        connection_manager=conn,
        bandwidth_limiter=BandwidthLimiter(),
    )
    # Must not raise, and must accept the new timeout parameter (previously
    # shutdown() took no arguments at all).
    mgr.shutdown(timeout=1.0)


def test_unified_concurrency_manager_shutdown_accepts_timeout():
    from mirror_url.concurrency import UnifiedConcurrencyManager

    mgr = UnifiedConcurrencyManager(max_total_threads=4)
    mgr.shutdown(timeout=1.0)


# ---------------------------------------------------------------------------
# 5b. LRUCache._timestamps: a parallel dict duplicating what self.cache
#    already stores as (value, timestamp) tuples. Written to and popped from
#    on every put()/put_batch()/shrink_to()/invalidate()/clear(), but never
#    read by any method -- pure write-only overhead. Removed entirely.
# ---------------------------------------------------------------------------


def test_lru_cache_has_no_timestamps_attribute():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=10, ttl_seconds=60, name="test")
    cache.put("a", 1)
    cache.put_batch({"b": 2, "c": 3})
    assert not hasattr(cache, "_timestamps")


def test_lru_cache_still_works_correctly_without_timestamps_dict():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=2, ttl_seconds=60, name="test")
    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)  # evicts "a" (LRU, maxsize=2)

    assert cache.get("a") is None
    assert cache.get("b") == 2
    assert cache.get("c") == 3
    assert len(cache) == 2

    cache.invalidate("b")
    assert cache.get("b") is None
    assert len(cache) == 1

    cache.clear()
    assert len(cache) == 0


# ---------------------------------------------------------------------------
# 6. LRUCache.__contains__ previously ignored TTL entirely (`key in
#    self.cache`, no expiry check), so `key in cache` could be True for an
#    entry get() would immediately expire and return None for. Now checks
#    the same expiry condition get() uses, without mutating the cache (pure
#    query, like `in` on a plain dict -- eviction still happens lazily via
#    get()/put()).
# ---------------------------------------------------------------------------


def test_contains_true_for_fresh_entry():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=10, ttl_seconds=60, name="test")
    cache.put("a", 1)
    assert "a" in cache


def test_contains_false_for_missing_entry():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=10, ttl_seconds=60, name="test")
    assert "nonexistent" not in cache


def test_contains_false_for_expired_entry_matching_get():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=10, ttl_seconds=0.05, name="test")
    cache.put("a", 1)
    assert "a" in cache

    time.sleep(0.1)  # let it expire

    # The bug: previously `"a" in cache` was True here even though
    # cache.get("a") already returns None -- inconsistent with get().
    assert cache.get("a") is None
    assert "a" not in cache


def test_contains_does_not_mutate_or_evict():
    from mirror_url.primitives import LRUCache

    cache = LRUCache(maxsize=10, ttl_seconds=0.05, name="test")
    cache.put("a", 1)
    time.sleep(0.1)

    evictions_before = cache.evictions
    assert "a" not in cache  # pure query, must not evict as a side effect
    assert cache.evictions == evictions_before
    # Entry is still physically present (lazy eviction happens on get()/put()
    # instead) -- __contains__ just doesn't report it as valid.
    assert "a" in cache.cache


# ---------------------------------------------------------------------------
# 7. Deprecated always-None ``connection_manager.circuit_breaker`` removed.
#    Live path is ``circuit_breaker_manager``. health.py previously always
#    reported "disabled"; _core/downloads.py had a dead pre-check.
# ---------------------------------------------------------------------------


def test_connection_manager_has_no_deprecated_circuit_breaker_attr():
    from mirror_url.config import MirrorConfig
    from mirror_url.connection import ConnectionManager
    from mirror_url.metrics import MetricsCollector

    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path="/tmp/does-not-matter",
        log_path="/tmp/does-not-matter",
        no_cache=True,
        circuit_breaker_enabled=True,
    )
    mgr = ConnectionManager(config, MetricsCollector())
    assert not hasattr(mgr, "circuit_breaker")
    assert mgr.circuit_breaker_manager is not None


def test_async_managers_have_no_deprecated_circuit_breaker_attr():
    from mirror_url.async_connection import AdaptiveAsyncManager, AsyncConnectionManager
    from mirror_url.config import MirrorConfig
    from mirror_url.metrics import MetricsCollector

    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path="/tmp/does-not-matter",
        log_path="/tmp/does-not-matter",
        no_cache=True,
        circuit_breaker_enabled=True,
    )
    metrics = MetricsCollector()
    async_mgr = AsyncConnectionManager(config, metrics)
    adaptive = AdaptiveAsyncManager(config, metrics)
    assert not hasattr(async_mgr, "circuit_breaker")
    assert not hasattr(adaptive, "circuit_breaker")
    assert async_mgr.circuit_breaker_manager is not None
    assert adaptive.circuit_breaker_manager is not None


def test_circuit_breaker_summary_disabled_when_no_manager():
    from mirror_url.health import _circuit_breaker_summary

    class CM:
        circuit_breaker_manager = None

    assert _circuit_breaker_summary(CM()) == "disabled"
    assert _circuit_breaker_summary(None) == "disabled"


def test_circuit_breaker_summary_closed_open_half_open():
    from mirror_url.circuit_breaker import CircuitBreakerManager
    from mirror_url.enums import CircuitBreakerState
    from mirror_url.health import _circuit_breaker_summary

    class CM:
        def __init__(self):
            self.circuit_breaker_manager = CircuitBreakerManager(
                failure_threshold=2, recovery_timeout=60.0, half_open_limit=2
            )

    cm = CM()
    assert _circuit_breaker_summary(cm) == "closed"

    # Trip to OPEN
    cm.circuit_breaker_manager.record_failure("example.com")
    cm.circuit_breaker_manager.record_failure("example.com")
    assert _circuit_breaker_summary(cm) == "open"

    # Force HALF_OPEN on the domain breaker
    breaker = cm.circuit_breaker_manager.get_breaker("example.com")
    breaker.state = CircuitBreakerState.HALF_OPEN
    assert _circuit_breaker_summary(cm) == "half_open"


def test_health_checker_reports_manager_state_not_disabled_attr():
    """get_status().connection['circuit_breaker'] must not be stuck on
    'disabled' when circuit_breaker_manager is active."""
    from mirror_url.circuit_breaker import CircuitBreakerManager
    from mirror_url.health import HealthChecker
    from mirror_url.primitives import AtomicCounter, AtomicSize

    class FakeCM:
        circuit_breaker_manager = CircuitBreakerManager(failure_threshold=2)

    class FakeCache:
        class _LRU:
            def get_stats(self):
                return {}

        lru_file_cache = _LRU()

    class FakeMetrics:
        metrics = {"errors": []}

    class FakeMirror:
        connection_ok = True
        base_url = "https://example.com/data/"
        start_time = time.time()
        files_processed = AtomicCounter(0)
        files_failed = AtomicCounter(0)
        files_skipped = AtomicCounter(0)
        total_downloaded_size = AtomicSize()
        connection_manager = FakeCM()
        cache_manager = FakeCache()
        metrics = FakeMetrics()
        memory_monitor = None
        disk_manager = None
        performance_monitor = None

    mirror = FakeMirror()
    checker = HealthChecker(mirror)
    status = checker.get_status()
    assert status.connection["circuit_breaker"] == "closed"

    mirror.connection_manager.circuit_breaker_manager.record_failure("example.com")
    mirror.connection_manager.circuit_breaker_manager.record_failure("example.com")
    status2 = checker.get_status()
    assert status2.connection["circuit_breaker"] == "open"


def test_is_healthy_uses_counter_value_and_threshold():
    from mirror_url.health import HealthChecker
    from mirror_url.primitives import AtomicCounter, AtomicSize

    class FakeMirror:
        connection_ok = True
        base_url = "https://example.com/"
        start_time = time.time()
        files_processed = AtomicCounter(0)
        files_failed = AtomicCounter(5)
        files_skipped = AtomicCounter(0)
        total_downloaded_size = AtomicSize()
        connection_manager = None
        cache_manager = type(
            "C", (), {"lru_file_cache": type("L", (), {"get_stats": lambda self: {}})()}
        )()
        metrics = type("M", (), {"metrics": {"errors": []}})()
        memory_monitor = None
        disk_manager = None
        performance_monitor = None

    mirror = FakeMirror()
    assert HealthChecker(mirror, failure_threshold=10).is_healthy() is True
    assert HealthChecker(mirror, failure_threshold=3).is_healthy() is False
    mirror.connection_ok = False
    assert HealthChecker(mirror, failure_threshold=10).is_healthy() is False


def test_health_handler_binds_mirror_on_server_not_class():
    """Mirror must live on the HTTPServer instance, not the handler class."""
    from mirror_url.health import HealthCheckHandler, HealthCheckServer

    assert not hasattr(HealthCheckHandler, "mirror_instance") or (
        # property on the class is fine; a plain shared attribute is not
        isinstance(getattr(HealthCheckHandler, "mirror_instance", None), property)
        or callable(getattr(HealthCheckHandler, "mirror_instance", None))
    )
    # Class attribute that was previously a shared None should be gone as a
    # simple data attribute used for cross-instance stomp.
    # HealthCheckServer still stores mirror_instance on itself.
    server = HealthCheckServer(mirror_instance=object(), port=0)
    assert server.mirror_instance is not None


# ---------------------------------------------------------------------------
# 8. HealthCheckHandler._send_json's "build body before headers" fix only
#    covered json.dumps() failures, not wfile.write() failing *after*
#    end_headers() (e.g. a client disconnect mid-response) -- the except
#    handlers in _handle_health/_handle_metrics would retry _send_json,
#    calling send_response() a second time after headers were already sent:
#    the same double-response bug one layer deeper. _response_started now
#    tracks whether headers already went out, and
#    _send_error_unless_response_started refuses to send a second status
#    line once they have.
# ---------------------------------------------------------------------------


class _FakeHandler:
    """Minimal duck-typed stand-in for HealthCheckHandler.

    _send_error_unless_response_started and _send_json only touch
    self._response_started, self.close_connection, self._send_json (called
    recursively), send_response/send_header/end_headers, and self.wfile --
    none of BaseHTTPRequestHandler's socket/request machinery -- so a plain
    object with those attributes exercises the real, unbound methods.
    """

    def __init__(self):
        from types import MethodType

        from mirror_url.health import HealthCheckHandler

        self._response_started = False
        self.close_connection = False
        self.sent_responses = []  # (status_code, headers_sent, body)
        self._pending_status = None
        self._pending_headers = {}
        self.wfile = self
        # Bind the real (unbound) methods under test onto this duck-typed
        # instance, so _handle_health/_handle_metrics's internal
        # self._send_json(...) / self._send_error_unless_response_started(...)
        # calls resolve to the real implementation instead of AttributeError.
        self._send_json = MethodType(HealthCheckHandler._send_json, self)
        self._send_error_unless_response_started = MethodType(
            HealthCheckHandler._send_error_unless_response_started, self
        )

    def send_response(self, status_code):
        self._pending_status = status_code
        self._pending_headers = {}

    def send_header(self, key, value):
        self._pending_headers[key] = value

    def end_headers(self):
        pass

    def write(self, body):
        # Stands in for self.wfile.write in the real handler.
        self.sent_responses.append((self._pending_status, dict(self._pending_headers), body))


def test_send_json_sets_response_started_after_end_headers():
    from mirror_url.health import HealthCheckHandler

    fake = _FakeHandler()
    HealthCheckHandler._send_json(fake, 200, {"ok": True})
    assert fake._response_started is True
    assert len(fake.sent_responses) == 1
    assert fake.sent_responses[0][0] == 200


def test_send_error_unless_response_started_sends_when_not_started():
    from mirror_url.health import HealthCheckHandler

    fake = _FakeHandler()
    assert fake._response_started is False
    HealthCheckHandler._send_error_unless_response_started(fake, 500, {"status": "error"})
    assert len(fake.sent_responses) == 1
    assert fake.sent_responses[0][0] == 500
    assert fake.close_connection is False


def test_send_error_unless_response_started_closes_connection_instead_of_double_send():
    """The actual regression: once headers are on the wire, a second
    send_response() would corrupt the HTTP response. Must close the
    connection instead of sending anything further."""
    from mirror_url.health import HealthCheckHandler

    fake = _FakeHandler()
    fake._response_started = True  # simulate end_headers() already having run
    HealthCheckHandler._send_error_unless_response_started(fake, 500, {"status": "error"})
    assert fake.sent_responses == []  # no second send_response call
    assert fake.close_connection is True


def test_handle_health_does_not_double_send_when_wfile_write_fails():
    """End-to-end: a write failure after headers are sent during the
    success path must not trigger a second send_response() from the
    except handler."""
    from mirror_url.health import HealthCheckHandler

    class _FailingWriteHandler(_FakeHandler):
        def write(self, body):
            # First call (the 200 response) fails after headers are already
            # queued; simulates a broken pipe mid-write.
            self._response_started = True
            raise ConnectionError("simulated broken pipe")

    class _FakeHealthChecker:
        def get_status(self):
            from mirror_url.models import HealthStatus

            return HealthStatus(
                status="healthy",
                timestamp="now",
                metrics={},
                connection={},
                cache={},
                errors=[],
            )

    class _FakeMirror:
        health_checker = _FakeHealthChecker()

    fake = _FailingWriteHandler()
    fake.mirror_instance = _FakeMirror()
    HealthCheckHandler._handle_health(fake)

    # The write failure must be handled by closing the connection, not by
    # attempting a second send_response(500) after headers were already sent.
    assert fake.close_connection is True
    assert fake.sent_responses == []  # the failed write never actually landed


# ---------------------------------------------------------------------------
# 9. NullCacheManager: the fallback used when cache_file is None (only if
#    constructing the cache file *path* itself raised -- see
#    _MirrorBase.__init__) was an inline class implementing 6 of
#    CacheManager's 9 public methods. load(), save(), and
#    cleanup_stale_metadata() were missing, so scan.py's
#    self.cache_manager.load() raised AttributeError on the very first call
#    of any run that hit this fallback -- caught by get_remote_files()'s
#    broad except, which made the *entire sync silently abort* instead of
#    just running without a cache. Replaced with a real, fully-implemented
#    NullCacheManager in cache.py.
# ---------------------------------------------------------------------------


def test_null_cache_manager_implements_every_cache_manager_public_method():
    """Structural guard against this exact regression class: if
    CacheManager ever grows a new public method, this fails until
    NullCacheManager grows a matching one too -- rather than that gap
    being silently rediscovered via an AttributeError in production."""
    from mirror_url.cache import CacheManager, NullCacheManager

    real_public_methods = {
        name
        for name in dir(CacheManager)
        if not name.startswith("_") and callable(getattr(CacheManager, name))
    }
    null_public_methods = {
        name
        for name in dir(NullCacheManager)
        if not name.startswith("_") and callable(getattr(NullCacheManager, name))
    }
    missing = real_public_methods - null_public_methods
    assert not missing, f"NullCacheManager is missing: {missing}"


def test_null_cache_manager_load_matches_real_no_cache_return_value():
    from mirror_url.cache import NullCacheManager

    ncm = NullCacheManager()
    # Must not raise, and must match what CacheManager.load() itself
    # returns for "no cache file present" / "--no-cache" -- callers
    # (scan.py) already handle this value correctly for the real manager.
    cache_loaded, cached_signatures = ncm.load()
    assert cache_loaded is False
    assert cached_signatures is None


def test_null_cache_manager_save_returns_false():
    from mirror_url.cache import NullCacheManager

    ncm = NullCacheManager()
    assert ncm.save({"http://example.test/": "sig"}, file_count=1) is False


def test_null_cache_manager_cleanup_stale_metadata_returns_zero():
    from pathlib import Path

    from mirror_url.cache import NullCacheManager

    ncm = NullCacheManager()
    assert ncm.cleanup_stale_metadata({Path("/tmp/some/file.txt")}) == 0


def test_null_cache_manager_get_and_set_are_safe_no_ops():
    from pathlib import Path

    from mirror_url.cache import NullCacheManager

    ncm = NullCacheManager()
    assert ncm.get_html_cache("http://example.test/") is None
    ncm.set_html_cache("http://example.test/", ["a"], ["b"])  # must not raise
    assert ncm.get_file_metadata(Path("/tmp/x")) is None
    ncm.save_file_metadata(Path("/tmp/x"), etag="abc", mtime=0.0)  # must not raise
    ncm.cleanup_file_metadata(Path("/tmp/x"))  # must not raise
    assert ncm.invalidate_directory("http://example.test/", "sig") is False


def test_null_cache_manager_handle_memory_pressure_still_shrinks_its_own_caches():
    """Not purely a no-op: NullCacheManager owns lru_file_cache/html_cache
    itself (used by get_html_cache/set_html_cache), and those still need to
    respond to memory pressure even with no on-disk metadata cache."""
    from mirror_url.cache import NullCacheManager
    from mirror_url.enums import MemoryPressure

    ncm = NullCacheManager()
    for i in range(50):
        ncm.lru_file_cache.put(f"key{i}", f"value{i}")

    freed = ncm.handle_memory_pressure(pressure=MemoryPressure.CRITICAL)
    assert freed > 0
    assert len(ncm.lru_file_cache) < 50

    # Also accepts the string-level calling convention CacheManager
    # supports ("for test compatibility").
    ncm2 = NullCacheManager()
    for i in range(50):
        ncm2.lru_file_cache.put(f"key{i}", f"value{i}")
    freed2 = ncm2.handle_memory_pressure(level="critical")
    assert freed2 > 0


def test_null_cache_manager_wired_up_when_cache_file_is_none():
    """Confirms _MirrorBase actually uses NullCacheManager (not the old
    inline DummyCacheManager) as its fallback."""
    import inspect

    from mirror_url._core import _base

    source = inspect.getsource(_base)
    assert "NullCacheManager" in source
    assert "DummyCacheManager" not in source
