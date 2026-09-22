"""Regression tests for latent bugs found during code review.

Originally covered five fixes; see REFACTORING_PLAN.md and CHANGELOG.md for
the narrative. Point 1 (ConnectionManager._is_url_within_scope) was later
hardened further -- the check_base=False branch was removed entirely
instead of just being made safe -- with tests updated to match. Grouped in
one file since the fixes aren't related in implementation, only in how they
were discovered (a single review pass).
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
