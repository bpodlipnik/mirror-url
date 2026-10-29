"""One invocation owns a complete log with one common startup description."""

from __future__ import annotations

import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import Mock

import pytest

from mirror_url import cli
from mirror_url import destination_lock as locks
from mirror_url.exceptions import ConfigError, DestinationLockError
from test_directory_selection import directory_archive as directory_archive
from test_directory_selection import parser as parser
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

PACKAGES = [
    f"packages/{name}"
    for name in ("findstuff", "forward", "gevloc", "goes", "gsfit", "gx_simulator", "helioviewer")
]


@pytest.fixture(autouse=True)
def restore_logging():
    handlers, level = list(logging.root.handlers), logging.root.level
    yield
    for handler in list(logging.root.handlers):
        if handler not in handlers:
            handler.close()
    logging.root.handlers[:] = handlers
    logging.root.setLevel(level)


@pytest.fixture
def invoke(config, directory_archive, monkeypatch, capsys):
    def run(suffixes, *, backend="httpx", console=False, extra=(), from_config=False):
        args = ["mirror-url"]
        if from_config:
            config.base_url = directory_archive.url
            path = config.log_path.parent / "config.json"
            path.write_text(config.model_dump_json(), encoding="utf-8")
            args += ["--config", str(path)]
        else:
            args += [
                "--url",
                directory_archive.url,
                "--dest-path",
                str(config.dest_path),
                "--log-path",
                str(config.log_path),
            ]
        if suffixes:
            args += ["--dir", *suffixes]
        args += [
            "--backend",
            backend,
            "--filter",
            ".txt",
            "--sequential-downloads",
            "--no-async-metadata",
            "--no-connection-pool-prewarm",
            "--request-delay",
            "0",
            "--requests-per-second",
            "0",
            "--max-retries",
            "0",
        ]
        if console:
            args += ["--print-logs"]
        monkeypatch.setattr(sys, "argv", [*args, *extra])
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        captured = capsys.readouterr()
        return stopped.value.code, sorted(config.log_path.glob("*.log")), captured

    return run


@pytest.mark.parametrize(
    "suffixes, prefix",
    [([], "mirror_url_all_"), (["packages/helioviewer"], "mirror_url_packages_helioviewer_")],
)
def test_default_shared_helper_handles_empty_and_single_selections(
    tmp_path, parser, suffixes, prefix
):
    args = ["--url", "https://example.com/", "--log-path", str(tmp_path)]
    if suffixes:
        args += ["--dir", *suffixes]
    cli.setup_shared_logging(parser.parse_args(args))
    paths = list(tmp_path.glob("*.log"))
    assert len(paths) == 1 and paths[0].name.startswith(prefix)
    assert paths[0].read_text(encoding="utf-8").count("Starting MirrorURL") == 1


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("console", [False, True])
def test_all_seven_directories_share_one_complete_log(remote, config, invoke, backend, console):
    for suffix in PACKAGES:
        remote.files[f"{suffix}/one.txt"] = suffix.encode()
    code, logs, output = invoke(PACKAGES, backend=backend, console=console)
    assert code == 0
    assert len(logs) == 1
    assert re.fullmatch(
        r"mirror_url_packages-findstuff--packages-helioviewer_\d{8}_\d{6}\.log", logs[0].name
    )
    text = logs[0].read_text(encoding="utf-8")
    assert text.count("Starting MirrorURL") == 1
    assert text.count("Command:") == 1
    assert text.count("SAFE MODE:") == 1
    assert text.count("Rate limiting:") == 1
    assert text.count("Cache max age:") == 1
    assert text.count("FINAL SUMMARY") == 1
    assert "TOTAL FILES FOUND: 7" in text
    assert "DOWNLOADED: 7" in text
    assert "FILES FAILED: 0" in text
    for index, suffix in enumerate(PACKAGES, 1):
        assert text.count(f"[{index}/7] Processing directory suffix: '{suffix}'") == 1
        assert f"[{index}/7] Destination path: {config.dest_path / suffix}" in text
        assert f"[{index}/7] ✅ Successfully processed: {suffix}" in text
        assert (config.dest_path / suffix / "one.txt").read_bytes() == suffix.encode()
    if console:
        assert output.err.count("Starting MirrorURL") == 1
        assert output.err.count("Rate limiting:") == 1
        assert output.err.count("Processing directory suffix: '") == 7
        assert output.err.count("FINAL SUMMARY") == 1
    else:
        assert output.err == ""
    assert output.out == ""


@pytest.mark.integration
@pytest.mark.parametrize("from_config", [False, True])
def test_single_directory_keeps_current_filename(remote, invoke, from_config):
    remote.files["packages/helioviewer/one.txt"] = b"one"
    code, logs, _ = invoke(["packages/helioviewer"], from_config=from_config)
    assert code == 0 and len(logs) == 1
    assert re.fullmatch(r"mirror_url_packages_helioviewer_\d{8}_\d{6}\.log", logs[0].name)


@pytest.mark.integration
def test_failed_directory_keeps_previous_and_subsequent_jobs(remote, invoke):
    for suffix in ("a", "b", "c"):
        remote.files[f"{suffix}/one.txt"] = b"one"
    remote.failures["b/"] = 503
    code, logs, output = invoke(["a", "b", "c"], console=True, from_config=True)
    assert code == 1 and len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "[1/3] ✅ Successfully processed: a" in text
    assert "[2/3] Connection failed for b" in text
    assert "[3/3] ✅ Successfully processed: c" in text
    assert "FAILED DIRECTORIES (1)" in text and "DOWNLOADED: 2" in text
    assert text.count("Rate limiting:") == 1
    assert "[2/3] Connection failed for b" in output.err


@pytest.mark.integration
@pytest.mark.parametrize("console", [False, True])
@pytest.mark.parametrize("error", [ConfigError("invalid config"), RuntimeError("invalid config")])
def test_configuration_errors_are_in_the_run_log(remote, invoke, monkeypatch, console, error):
    remote.files["a/one.txt"] = b"one"
    remote.files["b/one.txt"] = b"two"
    original = cli.MirrorConfig.from_dict

    def create(data, **kwargs):
        if data.get("dir_suffix") == "a":
            raise error
        return original(data, **kwargs)

    monkeypatch.setattr(cli.MirrorConfig, "from_dict", create)
    code, logs, output = invoke(["a", "b"], console=console, from_config=True)
    assert code == 1 and len(logs) == 1
    text = logs[0].read_text(encoding="utf-8")
    assert "[1/2]" in text and "invalid config" in text
    assert "[2/2] ✅ Successfully processed: b" in text
    assert text.count("Rate limiting:") == 1
    assert output.err.count("invalid config") == 1


@pytest.mark.integration
@pytest.mark.parametrize("custom", [False, True])
def test_shared_log_directory_fallback_preserves_the_run_and_blocking_file(
    remote, config, invoke, tmp_path, monkeypatch, custom
):
    remote.files.update({"a/one.txt": b"a", "b/one.txt": b"b"})
    blocked = tmp_path / "not-a-directory"
    blocked.write_bytes(b"preserve this file")
    config.log_path = blocked / "logs"
    fallback = tmp_path / "fallback"
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(fallback))
    code, _, output = invoke(
        ["a", "b"], console=True, extra=["--log-file", "custom"] if custom else []
    )
    assert code == 0
    paths = list((fallback / f"mirrorurl_logs_{os.getpid()}").glob("*.log"))
    assert len(paths) == 1
    text = paths[0].read_text(encoding="utf-8")
    assert "[1/2] ✅ Successfully processed: a" in text
    assert "[2/2] ✅ Successfully processed: b" in text
    assert text.count("Cannot create log at") == 1
    assert text.count("Rate limiting:") == 1
    assert "DOWNLOADED: 2" in text
    assert "using fallback:" in output.err
    assert blocked.read_bytes() == b"preserve this file"
    assert (config.dest_path / "a/one.txt").read_bytes() == b"a"
    assert (config.dest_path / "b/one.txt").read_bytes() == b"b"


@pytest.mark.integration
@pytest.mark.parametrize("custom", [False, True])
def test_regex_selection_names_resolved_endpoints_and_records_all_jobs(remote, invoke, custom):
    remote.files.update({"packages/a/one.txt": b"a", "packages/z/one.txt": b"z"})
    extra = ["--dir-regex", "packages/(a|z)"]
    if custom:
        extra += ["--log-file", "selected"]
    code, logs, output = invoke([], console=True, extra=extra)
    assert code == 0 and len(logs) == 1
    assert logs[0].name.startswith(
        "selected_all_" if custom else "mirror_url_packages-a--packages-z_"
    )
    text = logs[0].read_text(encoding="utf-8")
    if not custom:
        assert "Processing directory suffixes: 'packages/a', 'packages/z'" in text
    assert "[1/2] Processing directory suffix: 'packages/a'" in text
    assert "[2/2] Processing directory suffix: 'packages/z'" in text
    assert "DOWNLOADED: 2" in text
    assert text.count("Rate limiting:") == 1
    assert text.count("Cache max age:") == 1
    assert "Cache disabled by --no-cache" not in text
    assert output.err.count("Rate limiting:") == 1


@pytest.mark.parametrize("existing", ["file", "symlink"])
def test_run_logs_preserve_existing_entries_and_do_not_append(tmp_path, existing):
    target = tmp_path / "existing.txt"
    target.write_text("preserve", encoding="utf-8")
    path = tmp_path / "run.log"
    if existing == "file":
        path.write_text("previous run", encoding="utf-8")
    else:
        path.symlink_to(target)
    allocated, handler = locks.create_run_log(path)
    try:
        handler.handle(logging.makeLogRecord({"msg": "new run"}))
    finally:
        handler.close()
    assert allocated != path
    assert allocated.read_text(encoding="utf-8") == "new run\n"
    assert target.read_text(encoding="utf-8") == "preserve"
    assert path.is_symlink() if existing == "symlink" else path.read_text() == "previous run"


def test_simultaneous_logs_with_same_name_get_separate_owned_files(tmp_path, monkeypatch):
    directory = tmp_path / "locks"
    directory.mkdir(mode=0o700)
    monkeypatch.setattr(locks, "_lock_directory", lambda: directory)
    barrier = Barrier(2)

    def run(name):
        with locks.run_ownership([tmp_path / name]):
            barrier.wait(timeout=5)
            path, handler = locks.create_run_log(tmp_path / "logs" / "run.log")
            try:
                barrier.wait(timeout=5)
                handler.handle(logging.makeLogRecord({"msg": name}))
            finally:
                handler.close()
        return path

    with ThreadPoolExecutor(max_workers=2) as workers:
        paths = list(workers.map(run, ["first", "second"]))
    assert paths[0] != paths[1]
    assert {path.read_text().strip() for path in paths} == {"first", "second"}
    for path in paths:
        locks.DestinationLock([path]).close()


def test_log_allocator_bounds_unicode_names_and_collision_suffixes(tmp_path):
    path = tmp_path / ("é" * 200 + ".log")
    first, handler = locks.create_run_log(path)
    handler.close()
    second, handler = locks.create_run_log(path)
    handler.close()
    assert first != second
    assert all(len(p.name.encode("utf-8")) <= 255 for p in (first, second))


@pytest.mark.parametrize("failure", ["lock", "existing"])
def test_log_allocation_retries_are_bounded(tmp_path, failure):
    guard = Mock()
    factory = Mock(side_effect=FileExistsError("collision"))
    if failure == "lock":
        guard.add_paths.side_effect = DestinationLockError("busy")
    with pytest.raises(DestinationLockError if failure == "lock" else FileExistsError):
        locks.create_run_log(tmp_path / "run.log", factory, guard=guard)
    assert guard.add_paths.call_count == 10
    assert factory.call_count == (0 if failure == "lock" else 10)


def test_log_io_failure_is_not_treated_as_a_filename_collision(tmp_path):
    factory = Mock(side_effect=PermissionError("denied"))
    with pytest.raises(PermissionError, match="denied"):
        locks.create_run_log(tmp_path / "run.log", factory)
    factory.assert_called_once()
