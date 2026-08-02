"""Tests for 429 Too Many Requests / Retry-After handling.

Production gap found while reviewing async_connection.py for possible
performance improvements: HTTP 429 responses were treated identically to
any other 4xx client error (400, 403, ...) across the codebase -- given
up on immediately, no retry, no Retry-After header ever read. But 429 is
an explicit "come back later" signal from the server, not a permanent
failure; for a mirroring tool that expects to eventually succeed, giving
up on the first 429 is the wrong behavior.

Two parts:
1. parse_retry_after() (utils.py) -- a pure function, unit-tested directly
   against both the integer-seconds and HTTP-date header forms per
   RFC 9110 Sec 10.2.3, plus malformed/missing input.
2. ConnectionManager.request() (connection.py) -- the primary sync
   download/HEAD path -- now retries 429 specifically, honoring
   Retry-After when present. Driven end-to-end through the real retry
   loop via httpx.MockTransport rather than stubbing ConnectionManager
   itself (unlike test_missing_files.py / test_dir_signature_verification.py,
   which fake out the whole manager), since the goal here is to prove the
   retry *decision* inside request() actually fires correctly.

NOTE: async_connection.py's two HTTPStatusError branches were
deliberately NOT touched -- neither AsyncConnectionManager.head() nor
AdaptiveAsyncManager.head() ever calls response.raise_for_status(), so
that except block is unreachable dead code there; a 429 response flows
through the success path instead (returned as-is with status_code=429).
The caller (compare.py's async metadata check) already falls back to
this now-fixed sync path for any status that isn't 200/304/a
safe-to-skip 4xx, so no separate async-side fix was needed -- adding one
would have been dead code.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest

from mirror_url.utils import parse_retry_after

# ---------------------------------------------------------------------------
# parse_retry_after() -- pure function
# ---------------------------------------------------------------------------


def test_parse_retry_after_integer_seconds():
    assert parse_retry_after("30") == 30.0


def test_parse_retry_after_integer_zero():
    assert parse_retry_after("0") == 0.0


def test_parse_retry_after_integer_is_capped_at_max_delay():
    assert parse_retry_after("99999", max_delay=60.0) == 60.0


def test_parse_retry_after_strips_whitespace():
    assert parse_retry_after("  45  ") == 45.0


def test_parse_retry_after_http_date_near_future():
    future = datetime.now(timezone.utc) + timedelta(seconds=10)
    result = parse_retry_after(format_datetime(future, usegmt=True))
    assert result is not None
    # Allow a couple seconds of test-execution slop either side of 10s.
    assert 7.0 < result <= 10.0


def test_parse_retry_after_http_date_in_the_past_clamps_to_zero():
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    assert parse_retry_after(format_datetime(past, usegmt=True)) == 0.0


def test_parse_retry_after_http_date_far_future_is_capped():
    far_future = datetime.now(timezone.utc) + timedelta(hours=5)
    assert parse_retry_after(format_datetime(far_future, usegmt=True), max_delay=60.0) == 60.0


@pytest.mark.parametrize("value", [None, "", "not-a-date-or-number", "   "])
def test_parse_retry_after_missing_or_unparseable_returns_none(value):
    assert parse_retry_after(value) is None


# ---------------------------------------------------------------------------
# ConnectionManager.request() -- real retry loop, via httpx.MockTransport
# ---------------------------------------------------------------------------


def _build_connection_manager(monkeypatch, handler, tmp_path):
    """Build a real ConnectionManager whose connection pool is backed by
    an httpx.MockTransport, so request() drives its actual retry logic
    end-to-end against a scripted response sequence."""
    from mirror_url.config import MirrorConfig
    from mirror_url.connection import ConnectionManager
    from mirror_url.domain_health import DomainHealthTracker
    from mirror_url.metrics import MetricsCollector

    config = MirrorConfig(
        base_url="https://example.test/data/",
        dest_path="/tmp/does-not-matter",
        log_path="/tmp/does-not-matter",
        no_cache=True,
        max_retries=3,
        retry_delay=1,
    )
    mgr = ConnectionManager(config, MetricsCollector())

    mock_client = httpx.Client(transport=httpx.MockTransport(handler))
    monkeypatch.setattr(mgr.connection_pool, "get_client", lambda base_url: mock_client)

    # request() records 429/503 incidents via the process-wide domain-health
    # singleton (see test_domain_health.py) -- redirect it to a tmp-path
    # tracker for the duration of this test, never the real
    # ~/.cache/mirror-url/domain_health.json. Without this, every run of
    # this test file writes real incidents into the developer's/CI's
    # actual home directory cache.
    test_tracker = DomainHealthTracker(path=tmp_path / "domain_health.json")
    monkeypatch.setattr("mirror_url.connection.get_domain_health_tracker", lambda: test_tracker)

    # request() does an unrelated small random jitter sleep (0-0.02s) once,
    # BEFORE entering the retry loop, on every call regardless of outcome
    # (see connection.py). Pin it to exactly 0.0 so sleep_calls[0] is
    # always that fixed jitter entry, not noise -- actual retry-backoff
    # sleeps (what these tests care about) start at sleep_calls[1:].
    monkeypatch.setattr("mirror_url.connection.random.uniform", lambda a, b: 0.0)

    # Don't actually sleep in tests; just record what was requested.
    sleep_calls = []
    monkeypatch.setattr("mirror_url.connection.time.sleep", lambda s: sleep_calls.append(s))

    return mgr, sleep_calls, test_tracker


def test_429_with_retry_after_header_is_retried_and_succeeds(monkeypatch, tmp_path):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "2"})
        return httpx.Response(200, content=b"ok")

    mgr, sleep_calls, tracker = _build_connection_manager(monkeypatch, handler, tmp_path)

    resp = mgr.request("https://example.test/data/file.png")

    assert resp.status_code == 200
    assert len(calls) == 2, "expected exactly one retry after the 429"
    # index 0 is the fixed pre-request jitter (pinned to 0.0 above); the
    # actual retry-backoff sleep is index 1. The server-requested 2s must
    # be honored (possibly extended by the jittered exponential-backoff
    # floor, but never less than 2s).
    assert sleep_calls[1] >= 2.0
    # The 429 must also have been recorded as a domain-health incident
    # (see test_domain_health.py for the tracker's own unit tests).
    with tracker.lock:
        tracker._load()
        assert len(tracker._data.get("example.test", [])) == 1


def test_429_without_retry_after_falls_back_to_exponential_backoff(monkeypatch, tmp_path):
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429)  # no Retry-After header at all
        return httpx.Response(200, content=b"ok")

    mgr, sleep_calls, _tracker = _build_connection_manager(monkeypatch, handler, tmp_path)

    resp = mgr.request("https://example.test/data/file.png")

    assert resp.status_code == 200
    assert len(calls) == 2
    # Falls back to the same exponential_backoff() used elsewhere -- just
    # confirm *some* positive wait happened, not the exact jittered value.
    assert sleep_calls[1] > 0


def test_429_exhausting_all_retries_raises_mirror_connection_error(monkeypatch, tmp_path):
    from mirror_url.connection import MirrorConnectionError

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "1"})

    mgr, sleep_calls, tracker = _build_connection_manager(monkeypatch, handler, tmp_path)

    with pytest.raises(MirrorConnectionError, match="429"):
        mgr.request("https://example.test/data/file.png")

    # max_retries=3 -> 4 total attempts (initial + 3 retries), 3 backoff
    # sleeps between them, plus the fixed pre-loop jitter sleep (index 0).
    assert len(calls) == 4
    assert len(sleep_calls) == 4
    assert sleep_calls[0] == 0.0  # the pinned jitter, not a retry sleep
    assert all(s > 0 for s in sleep_calls[1:]), "all 3 retry backoffs must be positive waits"
    # Every one of the 4 observed 429s counts as an incident, even the
    # ones that were retried (not just the final give-up).
    with tracker.lock:
        tracker._load()
        assert len(tracker._data.get("example.test", [])) == 4


def test_429_retry_after_longer_than_computed_backoff_is_honored(monkeypatch, tmp_path):
    """A server explicitly asking for e.g. 5s must not be shortened just
    because the jittered exponential backoff would have picked less."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, content=b"ok")

    mgr, sleep_calls, _tracker = _build_connection_manager(monkeypatch, handler, tmp_path)
    mgr.request("https://example.test/data/file.png")

    assert sleep_calls[1] >= 5.0


def test_404_is_still_not_retried(monkeypatch, tmp_path):
    """Sanity check: the new 429 branch must not affect other 4xx codes --
    404 still raises immediately via raise_for_status(), no retry."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(404)

    mgr, sleep_calls, tracker = _build_connection_manager(monkeypatch, handler, tmp_path)

    with pytest.raises(httpx.HTTPStatusError):
        mgr.request("https://example.test/data/file.png")

    assert len(calls) == 1, "404 must not be retried"
    assert sleep_calls == [0.0], "only the fixed pre-request jitter, no retry-backoff sleep"
    # 404 is not a throttle/overload signal -- must not count as an incident.
    with tracker.lock:
        tracker._load()
        assert "example.test" not in tracker._data


def test_503_is_also_recorded_as_a_domain_health_incident(monkeypatch, tmp_path):
    """503 Service Unavailable is the other classic overload/throttle
    signal alongside 429 -- must count toward domain health too, not
    just get retried."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(503)
        return httpx.Response(200, content=b"ok")

    mgr, sleep_calls, tracker = _build_connection_manager(monkeypatch, handler, tmp_path)
    resp = mgr.request("https://example.test/data/file.png")

    assert resp.status_code == 200
    with tracker.lock:
        tracker._load()
        assert len(tracker._data.get("example.test", [])) == 1


def test_other_5xx_are_retried_but_not_recorded_as_incidents(monkeypatch, tmp_path):
    """500/502/504 usually indicate a server bug or gateway issue, not
    throttling -- still retried the same way as any 5xx, but must not
    pollute the domain-health throttle signal the way 429/503 do."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(500)
        return httpx.Response(200, content=b"ok")

    mgr, sleep_calls, tracker = _build_connection_manager(monkeypatch, handler, tmp_path)
    resp = mgr.request("https://example.test/data/file.png")

    assert resp.status_code == 200
    assert len(calls) == 2, "500 must still be retried"
    with tracker.lock:
        tracker._load()
        assert "example.test" not in tracker._data
