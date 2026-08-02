"""Tests for AdaptiveAsyncManager.profile_server()'s concurrent sampling.

Production report / suggestion: profile_server() sampled up to
PROFILE_SAMPLE_SIZE (20) URLs in a strictly sequential for-loop, each
awaited to completion before the next started. On a server with
non-trivial per-request latency, N samples cost N * (real round-trip
time) -- turning what should be a handful of quick HEAD probes into
several real seconds of pure warm-up before a single file transfer even
started, even though the underlying httpx.AsyncClient already has
HTTP/2 multiplexing enabled and can genuinely run several requests over
one connection at once.

Fixed: samples are now fired concurrently via asyncio.gather(), bounded
by a semaphore sized to self._current_concurrency -- the same value
_get_profile() already set for this domain (ADAPTIVE_START_CONCURRENCY
by default, or the conservative fallback for a KNOWN_THROTTLED_DOMAINS /
domain-health-learned-throttled domain). So a domain already flagged as
sensitive is still probed gently, concurrently within that same
conservative bound, rather than at full PROFILE_SAMPLE_SIZE burst
regardless of its throttle history.

These tests prove genuine concurrency (not just "still works"): the mock
transport records in-flight request counts and enforces a small
artificial per-request delay, so a sequential implementation would take
roughly N * delay wall-clock time, while a properly concurrent one
bounded by concurrency C takes roughly ceil(N / C) * delay.

Following the stubbing convention from test_429_retry_after.py: replace
self._client with a real httpx.AsyncClient backed by httpx.MockTransport
after _ensure_client() has otherwise initialized the manager, rather than
trying to drive the full SecureAsyncTransport/security-validation stack
(out of scope for what's being tested here -- pure concurrency behavior).
"""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from mirror_url.async_connection import AdaptiveAsyncManager
from mirror_url.config import MirrorConfig
from mirror_url.constants import ADAPTIVE_START_CONCURRENCY, PROFILE_SAMPLE_SIZE
from mirror_url.metrics import MetricsCollector


def _make_manager(**config_overrides):
    defaults = {
        "base_url": "https://example.test/data/",
        "dest_path": "/tmp/does-not-matter",
        "log_path": "/tmp/does-not-matter",
        "no_cache": True,
        "security_validation": False,  # keeps rate_limiter=None -- no real DNS/rate-limit calls
    }
    defaults.update(config_overrides)
    config = MirrorConfig(**defaults)
    return AdaptiveAsyncManager(config, MetricsCollector())


async def _install_mock_transport(mgr, handler):
    """Get the manager past its normal client-init path, then swap in a
    MockTransport-backed client -- same client TYPE and call surface
    (self._client.head(...)) profile_server() actually uses, without
    driving the real SecureAsyncTransport stack."""
    assert await mgr._ensure_client()
    await mgr._client.aclose()
    mgr._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))


class _ConcurrencyTrackingHandler:
    """Records the peak number of simultaneously in-flight requests, and
    optionally sleeps a fixed delay per request to make concurrency (or
    its absence) observable in wall-clock time."""

    def __init__(self, delay: float = 0.05, status_code: int = 200):
        self.delay = delay
        self.status_code = status_code
        self.in_flight = 0
        self.peak_in_flight = 0
        self.call_count = 0
        self._lock = asyncio.Lock()

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        async with self._lock:
            self.in_flight += 1
            self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
            self.call_count += 1
        try:
            await asyncio.sleep(self.delay)
            return httpx.Response(self.status_code)
        finally:
            async with self._lock:
                self.in_flight -= 1


@pytest.mark.asyncio
async def test_samples_run_concurrently_not_sequentially():
    """The core regression test: with concurrency > 1, more than one
    request must be in flight at the same time. A sequential
    implementation would never show peak_in_flight > 1."""
    handler = _ConcurrencyTrackingHandler(delay=0.05)
    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(10)]
    await mgr.profile_server(urls)

    assert handler.call_count == 10
    assert handler.peak_in_flight > 1, (
        "no overlap detected between requests -- samples are still being "
        "fired sequentially instead of concurrently"
    )


@pytest.mark.asyncio
async def test_concurrency_is_bounded_by_current_concurrency():
    """Peak in-flight must never exceed self._current_concurrency -- a
    domain already flagged conservative (e.g. via domain health) must
    stay conservative during profiling too, not burst to
    PROFILE_SAMPLE_SIZE regardless."""
    handler = _ConcurrencyTrackingHandler(delay=0.05)
    mgr = _make_manager()
    mgr._current_concurrency = 3
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(12)]
    await mgr.profile_server(urls)

    assert handler.peak_in_flight <= 3
    assert handler.peak_in_flight > 1  # still genuinely concurrent, just bounded


@pytest.mark.asyncio
async def test_concurrent_profiling_is_meaningfully_faster_than_sequential():
    """Direct, quantifiable proof of the speedup: N samples at a fixed
    per-request delay, bounded by concurrency C, must complete in
    roughly ceil(N / C) * delay wall-clock time -- not N * delay (what
    the old sequential loop would have cost)."""
    delay = 0.05
    n_samples = 10
    concurrency = 5
    handler = _ConcurrencyTrackingHandler(delay=delay)
    mgr = _make_manager()
    mgr._current_concurrency = concurrency
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(n_samples)]

    start = time.monotonic()
    await mgr.profile_server(urls)
    elapsed = time.monotonic() - start

    sequential_estimate = n_samples * delay
    concurrent_estimate = (n_samples / concurrency) * delay

    assert elapsed < sequential_estimate * 0.7, (
        f"took {elapsed:.3f}s -- too close to the sequential estimate of "
        f"{sequential_estimate:.3f}s to be genuinely concurrent"
    )
    # Generous upper bound -- allows for scheduling overhead/CI jitter
    # without the test becoming flaky.
    assert elapsed < concurrent_estimate * 3 + 0.2


@pytest.mark.asyncio
async def test_profile_records_correct_success_count():
    """error_rate must reflect actual outcomes even though results now
    arrive out of order via asyncio.gather(). Deliberately doesn't assert
    on profile_server()'s True/False return value here -- PROFILE_SAMPLE_SIZE
    caps the batch at 20, so with a single failure the finest achievable
    error rate is exactly 1/20 = 5%, right at ADAPTIVE_ERROR_THRESHOLD's
    edge (subject to float-precision flakiness); the threshold-crossing
    decision itself is covered separately below."""
    call_count = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(500)
        return httpx.Response(200)

    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(20)]
    await mgr.profile_server(urls)

    profile = mgr._get_profile(urls[0])
    assert profile.error_rate == pytest.approx(1 / 20, abs=0.001)


@pytest.mark.asyncio
async def test_high_error_rate_still_triggers_fallback_with_concurrent_sampling():
    """The pre-existing fallback-to-sync-on-high-error-rate behavior must
    survive the switch to concurrent sampling unchanged."""

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)  # every sample fails

    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(9)]
    result = await mgr.profile_server(urls)

    assert result is False
    assert mgr._fallback_to_sync is True


@pytest.mark.asyncio
async def test_profile_marks_complete_and_sets_concurrency():
    handler = _ConcurrencyTrackingHandler(delay=0.01)
    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(5)]
    await mgr.profile_server(urls)

    assert mgr._profile_complete is True
    assert mgr.get_concurrency() >= 1


@pytest.mark.asyncio
async def test_profile_respects_profile_sample_size_cap():
    """Only the first PROFILE_SAMPLE_SIZE URLs are ever sampled, even
    concurrently -- matches the previous (sequential) behavior's cap."""
    handler = _ConcurrencyTrackingHandler(delay=0.01)
    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(PROFILE_SAMPLE_SIZE + 15)]
    await mgr.profile_server(urls)

    assert handler.call_count == PROFILE_SAMPLE_SIZE


@pytest.mark.asyncio
async def test_timeouts_during_concurrent_profiling_are_recorded_not_raised():
    """A slow/unresponsive sample must not crash the whole batch -- other
    concurrent samples still complete and get recorded."""

    async def handler(request: httpx.Request) -> httpx.Response:
        if "slow" in str(request.url):
            await asyncio.sleep(10)  # far longer than the 5s overall timeout
        return httpx.Response(200)

    mgr = _make_manager()
    await _install_mock_transport(mgr, handler)

    urls = [
        "https://example.test/data/slow.png",
        "https://example.test/data/fast1.png",
        "https://example.test/data/fast2.png",
    ]
    # Must complete well under the 10s the slow one would take if it
    # blocked the batch, and must not raise.
    start = time.monotonic()
    await mgr.profile_server(urls)
    elapsed = time.monotonic() - start

    assert elapsed < 8.0
    profile = mgr._get_profile(urls[0])
    assert len(profile.samples) == 3  # all three recorded, including the timed-out one


@pytest.mark.asyncio
async def test_default_current_concurrency_used_when_not_overridden():
    """Sanity check that the bound really is self._current_concurrency,
    using the real default rather than a test override."""
    handler = _ConcurrencyTrackingHandler(delay=0.05)
    mgr = _make_manager()
    assert mgr._current_concurrency == ADAPTIVE_START_CONCURRENCY
    await _install_mock_transport(mgr, handler)

    urls = [f"https://example.test/data/file{i}.png" for i in range(ADAPTIVE_START_CONCURRENCY * 2)]
    await mgr.profile_server(urls)

    assert handler.peak_in_flight <= ADAPTIVE_START_CONCURRENCY


@pytest.mark.asyncio
async def test_ensure_client_succeeds_with_default_http2_setting():
    """Regression test for a real production bug found while building the
    concurrency tests above: config.http2 defaults to True, but the h2
    package it requires wasn't declared anywhere in pyproject.toml (base
    dependencies, [dev], or any other extra) until this fix. Without h2
    installed, httpx.AsyncClient(http2=True, ...) raises on construction,
    _ensure_client() catches that and returns False, and every single
    caller (profile_server(), head(), etc.) then silently and permanently
    falls back to the slower sync path for the entire run -- with no
    error surfaced to the user. This test builds a manager with the real,
    unmodified default config (http2 not overridden) and drives the real
    _ensure_client()/_init_client() path with no mock transport
    substitution, so it fails loudly if the h2 dependency ever regresses."""
    mgr = _make_manager()
    assert (
        mgr.config.http2 is True
    )  # confirms this test exercises the real default, not an override

    initialized = await mgr._ensure_client()

    assert initialized is True
    assert mgr._client is not None
    assert mgr._fallback_to_sync is False

    await mgr._client.aclose()
