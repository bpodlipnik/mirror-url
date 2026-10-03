"""Kill real mirror processes at IO boundaries, then recover the same tree."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mirror_url import MirrorURL
from mirror_url.enums import CleanupPolicy
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

pytestmark = pytest.mark.integration
WORKER = Path(__file__).with_name("process_recovery_worker.py")
SOURCE = Path(__file__).resolve().parents[1] / "src"


def preserved_obsolete(destination, expected):
    archive = destination.parent / (destination.name + "_obsolete")
    for name, content in expected.items():
        candidates = [destination / name, archive / name]
        present = [path for path in candidates if path.exists()]
        assert len(present) == 1, (name, present)
        assert present[0].read_bytes() == content


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize(
    "phase", ["download", "publish-before", "publish-after", "cache", "cleanup"]
)
def test_process_kill_preserves_complete_files_and_recovery(remote, config, tmp_path, mode, phase):
    original = b"old" * 700_000
    replacement = bytes(range(256)) * 8193
    obsolete = {"old_a.txt": b"preserve alpha", "old_b.txt": b"preserve bravo"}
    remote.files = {"large.bin": original, **obsolete}
    config.verify_content = True
    config.workers = 1
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        destination, cache_path = mirror.target_dir, mirror.cache_file
    remote.files = {"large.bin": replacement}
    config.cleanup_policy = CleanupPolicy.MOVE
    config_path = tmp_path / "worker.json"
    config_path.write_text(config.model_dump_json())
    ready = tmp_path / "ready.json"
    log = tmp_path / "worker.log"
    env = {**os.environ, "PYTHONPATH": str(SOURCE)}
    command = [sys.executable, str(WORKER), str(config_path)]

    with log.open("wb") as output:
        process = subprocess.Popen(
            [*command, phase, str(ready)], cwd=tmp_path, env=env, stdout=output, stderr=output
        )
        try:
            deadline = time.monotonic() + 30
            checkpoint = None
            while time.monotonic() < deadline:
                if ready.exists():
                    try:
                        checkpoint = json.loads(ready.read_text())
                    except json.JSONDecodeError:
                        pass  # Ready marker can still be in flight.
                    if checkpoint:
                        break
                if process.poll() is not None:
                    break
                time.sleep(0.02)
            assert checkpoint and checkpoint["phase"] == phase, log.read_text(errors="replace")
            competing = subprocess.run(
                [*command, "none", str(tmp_path / "contender.json")],
                cwd=tmp_path,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=15,
            )
            assert competing.returncode != 0
            assert b"Destination or shared state is already in use" in competing.stdout
            process.kill()
            process.wait(timeout=10)
            assert process.returncode != 0
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)

    final = destination / "large.bin"
    assert final.read_bytes() == (
        original if phase in ("download", "publish-before") else replacement
    )
    preserved_obsolete(destination, obsolete)
    if phase == "download" and mode == "sequential":
        partial = Path(checkpoint["partial"])
        assert 0 < partial.stat().st_size < len(replacement)
        assert partial.with_name(partial.name + ".json").exists()
    if phase == "cache":
        assert (
            hashlib.sha256(cache_path.read_bytes()).hexdigest()
            == checkpoint["previous_cache_sha256"]
        )
        json.loads(cache_path.read_text())
        with pytest.raises(json.JSONDecodeError):
            json.loads(cache_path.with_suffix(".json.tmp").read_text())
    if phase == "cleanup":
        assert Path(checkpoint["moved"]).read_bytes() in obsolete.values()
        assert sum((destination / name).exists() for name in obsolete) == 1

    request_start = len(remote.requests)
    recovered = subprocess.run(
        [*command, "none", str(ready)],
        cwd=tmp_path,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=30,
    )
    assert recovered.returncode == 0, recovered.stdout.decode(errors="replace")
    assert final.read_bytes() == replacement
    preserved_obsolete(destination, obsolete)
    assert all(not (destination / name).exists() for name in obsolete)
    assert json.loads(cache_path.read_text())["_files"][str(final.resolve())]["sha256"] == (
        hashlib.sha256(replacement).hexdigest()
    )
    if phase == "download" and mode == "sequential":
        assert any(
            method == "GET"
            and path == "large.bin"
            and headers.get("Range", "").startswith("bytes=")
            and headers.get("If-Range")
            for method, path, headers in remote.requests[request_start:]
        )
    before = len(
        [1 for method, path, _ in remote.requests if method == "GET" and path == "large.bin"]
    )
    with MirrorURL(config) as mirror:
        assert mirror.sync()
    assert (
        len([1 for method, path, _ in remote.requests if method == "GET" and path == "large.bin"])
        == before
    )
