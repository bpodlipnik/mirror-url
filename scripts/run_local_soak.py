"""Long-running development test on an isolated, synthetic loopback archive.

Requires the dev environment and tests/ helpers. The private-IP bypass exists
only inside that test fixture and the test subprocess; no production flag is
added. Copy scripts/, tests/ and src/ before a long run to freeze its source.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from mirror_url import MirrorConfig, MirrorURL  # noqa: E402
from mirror_url.enums import CleanupPolicy  # noqa: E402
from test_http_mirror_workflows import local_archive  # noqa: E402

try:
    import psutil
except ImportError:
    psutil = None


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, data):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(data, indent=2) + "\n")
    os.replace(temporary, path)


def source_identity():
    return {
        str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for folder in (ROOT / "src", ROOT / "tests", ROOT / "scripts")
        for path in sorted(folder.rglob("*.py"))
    }


def resources():
    result = {"threads": threading.active_count()}
    if psutil is not None:
        process = psutil.Process()
        result["rss_bytes"] = process.memory_info().rss
        if hasattr(process, "num_fds"):
            result["file_descriptors"] = process.num_fds()
        else:
            result["handles"] = process.num_handles()
    return result


def kill_and_recover(config, phase, output, iteration):
    config_path = output / "worker.json"
    config_path.write_text(config.model_dump_json())
    ready = output / "checkpoint.json"
    ready.unlink(missing_ok=True)
    worker = ROOT / "tests" / "process_recovery_worker.py"
    command = [sys.executable, str(worker), str(config_path)]
    env = {**os.environ, "PYTHONPATH": str(ROOT / "src")}
    log_path = output / "crash-worker.log"
    if log_path.exists() and log_path.stat().st_size >= 5 * 1024 * 1024:
        os.replace(log_path, output / "crash-worker.log.1")
    with log_path.open("ab") as log:
        log.write(f"\n{timestamp()} cycle={iteration + 1} crash={phase}\n".encode("utf-8"))
        log.flush()
        process = subprocess.Popen(
            [*command, phase, str(ready)], stdout=log, stderr=log, env=env, cwd=output
        )
        try:
            deadline = time.monotonic() + 30
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.02)
            assert ready.exists(), f"Crash checkpoint failed in iteration {iteration}"
            process.kill()
            process.wait(timeout=10)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
        recovered = subprocess.run(
            [*command, "none", str(ready)],
            stdout=log,
            stderr=log,
            env=env,
            cwd=output,
            timeout=45,
        )
        assert recovered.returncode == 0, "Process recovery failed; see crash-worker.log"
        log.write(f"{timestamp()} cycle={iteration + 1} recovery=passed\n".encode("utf-8"))


def cycle(remote, config, output, iteration):
    mode = ("sequential", "parallel", "streaming")[iteration % 3]
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    payload = bytes([iteration % 251]) * (2 * 1024 * 1024 + 17)
    remote.files = {
        "large.bin": payload,
        "small.txt": str(iteration).encode(),
        "blocked/keep.txt": b"preserve through failed listing",
        "obsolete.txt": str(iteration).encode(),
    }
    remote.failures = {}
    with MirrorURL(config) as mirror:
        assert mirror.sync(), "Initial sync failed"
        target = mirror.target_dir
        assert (target / "large.bin").read_bytes() == payload
        request_count = sum(
            method == "GET" and path == "large.bin" for method, path, _ in remote.requests
        )
        assert mirror.sync(), "Repeat sync failed"
        assert request_count == sum(
            method == "GET" and path == "large.bin" for method, path, _ in remote.requests
        )
        corrupted = target / "large.bin"
        with corrupted.open("r+b") as stream:
            stream.write(bytes([(iteration + 1) % 251]))
        assert mirror.sync(), "Corruption repair failed"
        assert corrupted.read_bytes() == payload

        # A failed subtree must preserve both its file and unrelated obsolete
        # candidates until the scan is complete again.
        remote.files.pop("obsolete.txt")
        remote.failures["blocked/"] = 503
        assert not mirror.sync(), "Partial listing unexpectedly passed"
        assert (target / "obsolete.txt").read_bytes() == str(iteration).encode()
        assert (target / "blocked/keep.txt").read_bytes() == remote.files["blocked/keep.txt"]
        remote.failures = {}
        assert mirror.sync(), "Complete scan recovery failed"
        assert not (target / "obsolete.txt").exists()
        archive = target.with_name(target.name + "_obsolete")
        assert any(
            path.read_bytes() == str(iteration).encode() for path in archive.glob("obsolete*.txt")
        )

    if iteration % 7 == 0:
        remote.files["large.bin"] = bytes([(iteration + 2) % 251]) * len(payload)
        # Exercise publication on alternating sides of the atomic replacement.
        phase = "publish-before" if iteration % 14 == 0 else "publish-after"
        kill_and_recover(config, phase, output, iteration)
        assert (target / "large.bin").read_bytes() == remote.files["large.bin"]
    if iteration % 3 == 0:
        remote.files["large.bin"] = bytes([(iteration + 3) % 251]) * len(payload)
        remote.drops["large.bin"] = 65_536
        with MirrorURL(config) as mirror:
            assert mirror.sync(), "Dropped connection recovery failed"
            assert (target / "large.bin").read_bytes() == remote.files["large.bin"]
    # This fixture stores request records for assertions; bound its own memory.
    remote.requests.clear()
    return mode


def disk_resources(output, iterations):
    target = output / "mirror"
    state = target / ".mirror-url-state"
    roots = [state / "parallel", *(output / "chunks").glob(".mirror-url-scratch-*")]
    work = [path for root in roots for path in root.glob("work_*")]
    temporary_bytes = sum(
        p.stat().st_size for folder in work for p in folder.rglob("*") if p.is_file()
    )
    assert not work, f"Abandoned parallel workspaces remain after recovery: {work}"
    archive = target.with_name(target.name + "_obsolete")
    entries = list(archive.iterdir())
    assert len(entries) == iterations and all(
        p.name.startswith("obsolete") and p.suffix == ".txt" for p in entries
    ), "Unexpected or lost MOVE archive data"
    archived_bytes = sum(p.stat().st_size for p in entries)
    assert archived_bytes <= iterations * 8, "MOVE archive grew beyond the known fixture payload"
    return {
        "temporary_workspaces": len(work),
        "temporary_bytes": temporary_bytes,
        "archive_files": len(entries),
        "archive_payload_bytes": archived_bytes,
        "filesystem_free_bytes": shutil.disk_usage(output).free,
    }


def run(output, duration_hours, interval):
    output.mkdir(parents=True, exist_ok=False)
    handler = RotatingFileHandler(output / "soak.log", maxBytes=5 * 1024 * 1024, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.WARNING)
    started = time.monotonic()
    deadline = started + duration_hours * 3600
    identity = source_identity()
    write_json(output / "source-sha256.json", identity)
    status = {
        "state": "running",
        "pid": os.getpid(),
        "started_at": timestamp(),
        "duration_hours": duration_hours,
        "interval_seconds": interval,
        "iterations": 0,
        "crash_recoveries": 0,
        "kind": "local synthetic archive",
    }
    write_json(output / "status.json", status)
    try:
        with pytest.MonkeyPatch.context() as monkeypatch:
            with local_archive(monkeypatch) as remote:
                config = MirrorConfig(
                    base_url=remote.url,
                    dest_path=output / "mirror",
                    log_path=output / "cache",
                    chunk_assembly_dir=output / "chunks",
                    verify_content=True,
                    cleanup_policy=CleanupPolicy.MOVE,
                    refresh_cache=True,
                    cache_html=False,
                    workers=2,
                    max_retries=1,
                    retry_delay=1,
                    request_delay=0.001,
                    async_metadata=False,
                    handle_symlinks=False,
                    connection_pool_prewarm=False,
                    use_shared_log=True,
                    security_validation=False,
                    min_chunk_size_mb=1,
                    max_chunks_per_file=2,
                )
                baseline = None
                while time.monotonic() < deadline or status["iterations"] == 0:
                    iteration = status["iterations"]
                    mode = cycle(remote, config, output, iteration)
                    gc.collect()
                    sample = resources()
                    disk = disk_resources(output, iteration + 1)
                    if baseline is None:
                        baseline = sample
                    assert sample["threads"] <= baseline["threads"] + 20, (
                        "Thread growth exceeds limit"
                    )
                    for name in ("file_descriptors", "handles"):
                        if name in sample:
                            assert sample[name] <= baseline[name] + 30, (
                                f"{name} growth exceeds limit"
                            )
                    if "rss_bytes" in sample:
                        assert sample["rss_bytes"] <= baseline["rss_bytes"] + 256 * 1024 * 1024, (
                            "RSS growth exceeds limit"
                        )
                    status.update(
                        iterations=iteration + 1,
                        last_cycle_at=timestamp(),
                        last_mode=mode,
                        elapsed_seconds=time.monotonic() - started,
                        resources=sample,
                        resource_baseline=baseline,
                        disk=disk,
                        crash_recoveries=status["crash_recoveries"] + (iteration % 7 == 0),
                    )
                    with (output / "samples.jsonl").open("a") as stream:
                        stream.write(json.dumps(status) + "\n")
                    write_json(output / "status.json", status)
                    time.sleep(max(0, min(interval, deadline - time.monotonic())))
        assert source_identity() == identity, "Source changed during soak; evidence invalid"
        status.update(
            state="completed", finished_at=timestamp(), elapsed_seconds=time.monotonic() - started
        )
        write_json(output / "status.json", status)
        return 0
    except BaseException as error:
        status.update(
            state="failed",
            finished_at=timestamp(),
            error=repr(error),
            elapsed_seconds=time.monotonic() - started,
        )
        write_json(output / "status.json", status)
        logging.exception("Soak failed")
        raise
    finally:
        handler.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path, help="New, dedicated test directory")
    parser.add_argument("--duration-hours", type=float, default=24)
    parser.add_argument(
        "--interval", type=float, default=60, help="Seconds between completed cycles"
    )
    args = parser.parse_args()
    if args.duration_hours <= 0 or args.interval < 0:
        parser.error("duration-hours must be positive and interval must be nonnegative")
    return run(args.output.resolve(), args.duration_hours, args.interval)


if __name__ == "__main__":
    sys.exit(main())
