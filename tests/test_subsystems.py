"""Subsystem integration tests.

These wire real components together and exercise genuine I/O — thread
concurrency, disk spill, pydantic/yaml config loading, circuit-breaker timing —
without requiring a live HTTP server (the SSRF-hardened transport deliberately
refuses loopback/private targets, so full end-to-end HTTP needs the bypass
documented in ``test_integration.py``).

They run in the normal (fast) test lane.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Thread-safe primitives under real concurrent load
# ---------------------------------------------------------------------------
def test_atomic_counter_under_concurrent_load():
    from mirror_url.primitives import AtomicCounter

    counter = AtomicCounter(0)
    workers, per_worker = 50, 1000

    def hammer():
        for _ in range(per_worker):
            counter.increment()

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda _: hammer(), range(workers)))

    assert counter.value() == workers * per_worker


def test_atomic_size_under_concurrent_load():
    from mirror_url.primitives import AtomicSize

    size = AtomicSize()
    workers, chunk = 32, 4096

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda _: size.add(chunk), range(workers)))

    assert size.value() == workers * chunk
    assert size.get_max() == workers * chunk


def test_metrics_collector_concurrent_increments():
    from mirror_url.metrics import MetricsCollector

    m = MetricsCollector()
    workers = 40

    def work():
        for _ in range(250):
            m.increment("files_downloaded")
            m.add_bytes(10)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(lambda _: work(), range(workers)))

    s = m.get_summary()
    assert s["files_downloaded"] == workers * 250
    assert s["bytes_downloaded"] == workers * 250 * 10


# ---------------------------------------------------------------------------
# Circuit-breaker state machine with real timing
# ---------------------------------------------------------------------------
def test_circuit_breaker_full_transition_cycle():
    import time

    from mirror_url.circuit_breaker import CircuitBreaker
    from mirror_url.enums import CircuitBreakerState as S

    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.1, half_open_limit=2)
    assert cb.can_execute() and cb.is_closed()

    for _ in range(3):
        cb.record_failure()
    assert cb.state == S.OPEN
    assert cb.can_execute() is False  # stays open within recovery window

    time.sleep(0.12)
    assert cb.can_execute() is True  # transitions to half-open
    assert cb.state == S.HALF_OPEN

    for _ in range(2):
        cb.record_success()
    assert cb.state == S.CLOSED  # recovered


def test_circuit_breaker_manager_is_per_domain():
    from mirror_url.circuit_breaker import CircuitBreakerManager

    mgr = CircuitBreakerManager(failure_threshold=2, recovery_timeout=5)
    assert mgr.can_execute("a.example.com")
    mgr.record_failure("a.example.com")
    mgr.record_failure("a.example.com")
    assert mgr.can_execute("a.example.com") is False  # tripped (v3.1.13 fix)
    assert mgr.can_execute("b.example.com") is True  # isolated per domain


def test_chunk_circuit_breaker_aggregates_file_failures():
    from mirror_url.circuit_breaker import ChunkCircuitBreaker
    from mirror_url.enums import CircuitBreakerState as S

    cb = ChunkCircuitBreaker(failure_threshold=5, chunk_failure_threshold=3)
    url, server = "https://x/big.bin", "x"
    for _ in range(3):  # file threshold reached -> records an overall failure
        cb.record_chunk_failure(url, server)
    assert cb.file_chunk_failures.get(url, 0) >= 3
    cb.record_chunk_success(url, server)
    assert url not in cb.file_chunk_failures  # success clears the file counter
    assert cb.state in (S.CLOSED, S.OPEN)


# ---------------------------------------------------------------------------
# DiskBackedSet: real spill-to-disk behavior
# ---------------------------------------------------------------------------
def test_disk_backed_set_spills_to_disk(tmp_path: Path):
    from mirror_url.storage import DiskBackedSet

    dbs = DiskBackedSet(tmp_path / "set", max_memory=100)
    for i in range(550):
        dbs.add(f"https://example.com/file_{i}.dat")

    assert len(dbs) == 550  # total tracked across memory + disk
    assert len(dbs.disk_files) >= 1  # memory exceeded -> flushed to disk
    for f in dbs.disk_files:
        assert f.exists()
    dbs.clear()
    assert len(dbs) == 0
    assert all(not f.exists() for f in dbs.disk_files)


def test_disk_backed_set_dedups_in_memory(tmp_path: Path):
    from mirror_url.storage import DiskBackedSet

    dbs = DiskBackedSet(tmp_path / "set2", max_memory=10_000)
    for _ in range(5):
        dbs.add("https://example.com/same.dat")
    assert len(dbs) == 1


# ---------------------------------------------------------------------------
# UnifiedConcurrencyManager: slot accounting across threads
# ---------------------------------------------------------------------------
def test_concurrency_manager_enforces_thread_cap():
    from mirror_url.concurrency import UnifiedConcurrencyManager
    from mirror_url.enums import ConcurrencyType

    mgr = UnifiedConcurrencyManager(max_total_threads=4)
    try:
        acquired = [mgr.acquire_thread(ConcurrencyType.SYNC) for _ in range(4)]
        assert all(acquired)
        assert mgr.acquire_thread(ConcurrencyType.SYNC) is False  # at cap
        mgr.release_thread()
        assert mgr.acquire_thread(ConcurrencyType.SYNC) is True  # slot freed
    finally:
        mgr.shutdown()


# ---------------------------------------------------------------------------
# Config: real pydantic + yaml round-trip and validation
# ---------------------------------------------------------------------------
def test_config_from_yaml_round_trip(tmp_path: Path):
    import yaml

    from mirror_url.config import MirrorConfig

    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "base_url": "https://example.com/files/",  # trailing slash trimmed
                "dest_path": str(tmp_path / "dest"),
                "log_path": str(tmp_path / "logs"),
                "workers": 6,
                "timeout": 45,
            }
        )
    )
    cfg = MirrorConfig.from_yaml(cfg_path)
    assert cfg.base_url == "https://example.com/files"
    assert isinstance(cfg.dest_path, Path)
    assert cfg.workers == 6 and cfg.timeout == 45


def test_config_expands_env_vars(tmp_path: Path, monkeypatch):
    import yaml

    from mirror_url.config import MirrorConfig

    monkeypatch.setenv("MIRROR_TEST_HOST", "https://host.example.com")
    cfg_path = tmp_path / "c.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "base_url": "${MIRROR_TEST_HOST}/data/",
                "dest_path": str(tmp_path / "d"),
                "log_path": str(tmp_path / "l"),
            }
        )
    )
    cfg = MirrorConfig.from_yaml(cfg_path)
    assert cfg.base_url == "https://host.example.com/data"


def test_config_rejects_non_http_scheme(tmp_path: Path):
    from mirror_url.config import MirrorConfig
    from mirror_url.exceptions import ConfigError

    with pytest.raises(ConfigError):
        MirrorConfig(
            base_url="ftp://example.com/x",
            dest_path=tmp_path / "d",
            log_path=tmp_path / "l",
        )


def test_config_rejects_multiple_download_modes(tmp_path: Path):
    from mirror_url.config import MirrorConfig
    from mirror_url.exceptions import ConfigError

    with pytest.raises(ConfigError):
        MirrorConfig(
            base_url="https://example.com/x",
            dest_path=tmp_path / "d",
            log_path=tmp_path / "l",
            parallel_downloads=True,
            streaming_parallel=True,
        )


def test_config_validate_warnings(tmp_path: Path):
    from mirror_url.config import MirrorConfig

    cfg = MirrorConfig(
        base_url="https://example.com/x",
        dest_path=tmp_path / "d",
        log_path=tmp_path / "l",
        hash_algorithm="md5",
    )
    warnings = MirrorConfig.validate(cfg)
    assert any("MD5" in w for w in warnings)


# ---------------------------------------------------------------------------
# DownloadQueue: priority ordering with a real DownloadTask
# ---------------------------------------------------------------------------
def test_download_queue_priority_ordering():
    from mirror_url.enums import DownloadPriority as P
    from mirror_url.models import DownloadTask
    from mirror_url.queue import DownloadQueue

    q = DownloadQueue(max_size=10)
    q.add(DownloadTask("http://x/low", Path("/t/low"), priority=P.LOW))
    q.add(DownloadTask("http://x/high", Path("/t/high"), priority=P.HIGH))
    q.add(DownloadTask("http://x/norm", Path("/t/norm"), priority=P.NORMAL))

    assert q.get().remote_url.endswith("/high")
    assert q.get().remote_url.endswith("/norm")
    assert q.get().remote_url.endswith("/low")
    assert q.get() is None


# ---------------------------------------------------------------------------
# ParallelDownloadManager: real thread lifecycle (start + clean shutdown)
# ---------------------------------------------------------------------------
def test_parallel_download_manager_lifecycle(tmp_path: Path):
    from mirror_url.config import MirrorConfig
    from mirror_url.download import ParallelDownloadManager
    from mirror_url.metrics import MetricsCollector
    from mirror_url.rate_limiter import BandwidthLimiter

    cfg = MirrorConfig(
        base_url="https://example.com/x",
        dest_path=tmp_path / "d",
        log_path=tmp_path / "l",
        chunk_assembly_dir=tmp_path / "chunks",
    )
    pdm = ParallelDownloadManager(
        config=cfg,
        metrics=MetricsCollector(),
        connection_manager=None,
        bandwidth_limiter=BandwidthLimiter(None),
        concurrency_manager=None,
        mirror=None,
    )
    try:
        assert pdm.assembly_dir.exists()
        assert pdm._cleanup_thread.is_alive()
        # chunk-count math is pure and safe to exercise
        assert pdm.get_chunk_count(0) == 1
    finally:
        pdm.shutdown()
    assert pdm._shutdown is True
