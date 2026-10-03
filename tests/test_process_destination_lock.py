"""Competing real processes and cleanup while a real HTTP writer is blocked."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from mirror_url import DestinationLockError, MirrorURL
from mirror_url.enums import CleanupPolicy
from mirror_url.utils import _log_files
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

pytestmark = pytest.mark.integration
WORKER = Path(__file__).with_name("destination_lock_worker.py")
SOURCE = Path(__file__).resolve().parents[1] / "src"


def launch(tmp_path, name, data, mode="raw", extra=()):
    config_path, ready = tmp_path / (name + ".json"), tmp_path / (name + ".ready")
    config_path.write_text(json.dumps(data))
    temporary = tmp_path / (name + "-temp")
    temporary.mkdir()
    env = {**os.environ, "PYTHONPATH": str(SOURCE), "TMPDIR": str(temporary)}
    process = subprocess.Popen(
        [sys.executable, str(WORKER), mode, str(config_path), str(ready), *extra],
        cwd=tmp_path,
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return process, ready


def wait_ready(process, ready):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and process.poll() is None:
        if ready.exists():
            return
        time.sleep(0.02)
    assert ready.exists(), process.communicate(timeout=5)[0]


def stop(process):
    if process.poll() is None:
        process.kill()
    return process.communicate(timeout=10)[0]


@pytest.mark.parametrize("overlap", ["same", "parent", "child"])
@pytest.mark.parametrize("release", ["normal", "kill"])
def test_processes_reject_overlap_then_recover_without_removing_lock_files(
    tmp_path, overlap, release
):
    root = tmp_path / "mirror"
    contender = {"same": root, "parent": tmp_path, "child": root / "child"}[overlap]
    first, ready = launch(tmp_path, "first", [str(root)])
    try:
        wait_ready(first, ready)
        second, second_ready = launch(tmp_path, "second", [str(contender)])
        try:
            output = second.communicate("release\n", timeout=15)[0]
            assert second.returncode == 17, output
            assert "already in use" in output and not second_ready.exists()
        finally:
            stop(second)
        if release == "kill":
            first.kill()
            first.communicate(timeout=10)
            assert first.returncode != 0
        else:
            output = first.communicate("release\n", timeout=10)[0]
            assert first.returncode == 0, output
        third, third_ready = launch(tmp_path, "third", [str(contender)])
        try:
            wait_ready(third, third_ready)
            output = third.communicate("release\n", timeout=10)[0]
            assert third.returncode == 0, output
        finally:
            stop(third)
    finally:
        stop(first)


def test_two_processes_on_separate_trees_share_ancestors(tmp_path):
    first, first_ready = launch(tmp_path, "a", [str(tmp_path / "a")])
    second, second_ready = launch(tmp_path, "b", [str(tmp_path / "b")])
    try:
        wait_ready(first, first_ready)
        wait_ready(second, second_ready)
        assert first.poll() is None and second.poll() is None
    finally:
        stop(first)
        stop(second)


def test_simultaneous_start_has_exactly_one_owner(tmp_path):
    root = tmp_path / "mirror"
    first, first_ready = launch(tmp_path, "a", [str(root)])
    second, second_ready = launch(tmp_path, "b", [str(root)])
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (first_ready.exists() or second_ready.exists()) and (
                first.poll() is not None or second.poll() is not None
            ):
                break
            time.sleep(0.02)
        assert first_ready.exists() != second_ready.exists()
        winner, loser = (first, second) if first_ready.exists() else (second, first)
        assert winner.poll() is None
        output = loser.communicate(timeout=5)[0]
        assert loser.returncode == 17, output
    finally:
        stop(first)
        stop(second)


@pytest.mark.parametrize("resource", ["destination", "cache", "archive", "assembly", "metrics"])
def test_rejected_mirror_does_not_contact_remote_or_change_existing_files(
    remote, config, tmp_path, resource
):
    remote.files = {"a.txt": b"original"}
    config.cleanup_policy = CleanupPolicy.MOVE
    config.chunk_assembly_dir = tmp_path / "assembly"
    config.metrics_json = tmp_path / "metrics.json"
    config.use_shared_log = True
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        before = (mirror.target_dir / "a.txt").read_bytes(), mirror.cache_file.read_bytes()
        competing = json.loads(config.model_dump_json())
        if resource != "destination":
            competing["dest_path"] = str(tmp_path / "other")
            competing["log_path"] = str(tmp_path / "other-logs")
            competing["chunk_assembly_dir"] = str(tmp_path / "other-assembly")
            competing["metrics_json"] = str(tmp_path / "other-metrics.json")
        if resource == "cache":
            competing["log_path"] = str(config.log_path)
        elif resource == "archive":
            competing["dest_path"] = str(mirror.target_dir.with_name("mirror_obsolete"))
        elif resource == "assembly":
            competing["chunk_assembly_dir"] = str(config.chunk_assembly_dir)
        elif resource == "metrics":
            competing["metrics_json"] = str(config.metrics_json)
        request_count = len(remote.requests)
        process, ready = launch(tmp_path, "competing", competing, mode="mirror")
        try:
            output = process.communicate("release\n", timeout=15)[0]
            assert process.returncode == 17, output
            assert not ready.exists() and len(remote.requests) == request_count
            assert before == (
                (mirror.target_dir / "a.txt").read_bytes(),
                mirror.cache_file.read_bytes(),
            )
        finally:
            stop(process)
    with MirrorURL(config) as mirror:
        assert mirror.sync()


def test_cleanup_retains_lock_while_an_http_writer_has_not_stopped(remote, config):
    remote.files = {"a.txt": b"old"}
    config.verify_content = True
    mirror = MirrorURL(config)
    assert mirror.sync()
    remote.files = {"a.txt": b"new" * 200_000}
    started, finish = threading.Event(), threading.Event()
    update = mirror.partial_manager.update_activity
    failures = []

    def pause(path, count=0):
        update(path, count)
        if count:
            started.set()
            assert finish.wait(10)

    mirror.partial_manager.update_activity = pause

    def write():
        try:
            mirror._download_file_single(remote.url + "a.txt", mirror.target_dir / "a.txt")
        except Exception as error:
            failures.append(error)

    worker = threading.Thread(target=write)
    worker.start()
    try:
        assert started.wait(10)
        mirror.cleanup()
        with pytest.raises(DestinationLockError):
            MirrorURL(config)
        with pytest.raises(DestinationLockError, match="closed"):
            mirror.sync()
    finally:
        finish.set()
        worker.join(timeout=10)
        mirror.cleanup()
    assert not worker.is_alive() and not failures
    with MirrorURL(config) as recovered:
        assert recovered.sync()
        assert (recovered.target_dir / "a.txt").read_bytes() == remote.files["a.txt"]


def test_cli_cannot_create_shared_logs_inside_another_active_destination(remote, config, tmp_path):
    remote.files = {"a.txt": b"preserve"}
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        log_path = mirror.target_dir / "competing-logs"
        competing = json.loads(config.model_dump_json())
        competing["dest_path"] = str(tmp_path / "other")
        competing["log_path"] = str(log_path)
        request_count = len(remote.requests)
        process, ready = launch(tmp_path, "cli", competing, mode="cli")
        try:
            output = process.communicate("release\n", timeout=15)[0]
            assert process.returncode == 1, output
            assert "Destination ownership error" in output
            assert not log_path.exists() and not ready.exists()
            assert len(remote.requests) == request_count
            assert (mirror.target_dir / "a.txt").read_bytes() == b"preserve"
        finally:
            stop(process)


def test_cli_shared_log_can_be_inside_its_own_destination(remote, config, tmp_path):
    remote.files = {"a.txt": b"expected"}
    config.log_path = config.dest_path / "logs"
    process, ready = launch(tmp_path, "cli", json.loads(config.model_dump_json()), mode="cli")
    try:
        output = process.communicate("release\n", timeout=15)[0]
        assert process.returncode == 0, output
        assert (config.dest_path / "a.txt").read_bytes() == b"expected"
        assert list(config.log_path.glob("contention*.log"))
    finally:
        stop(process)


@pytest.mark.parametrize("logging_mode", ["cli", "cli-unshared"])
def test_cli_keeps_group_ownership_after_first_suffix_cleanup(
    remote, config, tmp_path, logging_mode
):
    remote.files = {"a/one.txt": b"alpha", "b/two.txt": b"bravo"}
    process, ready = launch(
        tmp_path,
        "cli",
        json.loads(config.model_dump_json()),
        mode=logging_mode,
        extra=("--dir-suffix", "a", "b"),
    )
    try:
        output = process.communicate("release\n", timeout=15)[0]
        assert process.returncode == 0, output
        assert (config.dest_path / "a/one.txt").read_bytes() == b"alpha"
        assert (config.dest_path / "b/two.txt").read_bytes() == b"bravo"
        log = "\n".join(path.read_text() for path in config.log_path.glob("*.log"))
        assert "SUCCESSFUL (2)" in log and "FAILED: (none)" in log
    finally:
        stop(process)


def test_closed_instance_log_handler_cannot_reopen_its_file(remote, config):
    remote.files = {"a.txt": b"expected"}
    mirror = MirrorURL(config)
    assert mirror.sync()
    handler = next(
        handler for handler in mirror.log_handlers if isinstance(handler, logging.FileHandler)
    )
    path = Path(handler.baseFilename)
    mirror.cleanup()
    before = path.read_bytes()
    assert handler not in logging.root.handlers and handler not in _log_files
    assert handler.stream is None
    logging.root.handle(
        logging.LogRecord("after-close", logging.INFO, "", 0, "after close", (), None)
    )
    assert path.read_bytes() == before and handler.stream is None
