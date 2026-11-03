"""Destination setup failures preserve their cause and stop before file transfers."""

from __future__ import annotations

import errno
import logging
import os
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from mirror_url import DestinationPathError, MirrorURL, cli
from mirror_url._core import _base
from mirror_url.destination_lock import DestinationLock
from test_directory_selection import directory_archive as directory_archive
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def isolate_runtime(tmp_path, monkeypatch, directory_archive):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def deny_mkdir(monkeypatch, path, error):
    original = Path.mkdir
    attempts = []

    def mkdir(self, *args, **kwargs):
        if self == path:
            attempts.append(self)
            raise error
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    return attempts


@pytest.fixture
def cleaned(monkeypatch):
    instances = []
    original = MirrorURL.cleanup

    def cleanup(self):
        original(self)
        instances.append(self)

    monkeypatch.setattr(MirrorURL, "cleanup", cleanup)
    yield instances
    assert instances
    for instance in instances:
        assert instance._closed
        assert instance._meta_check_executor._shutdown
        assert instance.concurrency_manager._shutdown
        assert not instance.concurrency_manager.monitor_thread.is_alive()


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EROFS, errno.ENOSPC, errno.ENOTDIR])
def test_directory_creation_failure_stops_initialization(
    config, monkeypatch, cleaned, backend, error_number
):
    config.backend = backend
    config.dir_suffix = "blocked"
    target = config.dest_path / config.dir_suffix
    error = OSError(error_number, os.strerror(error_number), str(target))
    attempts = deny_mkdir(monkeypatch, target, error)
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    partials = Mock(wraps=_base.PartialDownloadManager)
    monkeypatch.setattr(_base, "PartialDownloadManager", partials)
    with pytest.raises(
        DestinationPathError, match="Cannot create destination directory"
    ) as stopped:
        MirrorURL(config)
    assert stopped.value.__cause__ is error
    assert attempts == [target]
    partials.assert_not_called()
    assert cleaned[0].partial_manager is cleaned[0].scratch_manager is None
    assert not target.exists()
    DestinationLock([target]).close()


@pytest.mark.skipif(
    os.name == "nt" or getattr(os, "geteuid", lambda: 0)() == 0,
    reason="requires POSIX directory permissions and an unprivileged user",
)
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_native_unwritable_parent_preserves_permission_error(config, monkeypatch, cleaned, backend):
    config.backend = backend
    config.dir_suffix = "blocked"
    config.dest_path.mkdir()
    target = config.dest_path / config.dir_suffix
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    config.dest_path.chmod(0o500)
    try:
        with pytest.raises(DestinationPathError) as stopped:
            MirrorURL(config)
        assert isinstance(stopped.value.__cause__, PermissionError)
        assert stopped.value.__cause__.errno == errno.EACCES
        assert str(target) in str(stopped.value)
        assert not target.exists()
    finally:
        config.dest_path.chmod(0o700)
    DestinationLock([target]).close()


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("site", ["state", "marker", "disk", "partial", "scratch"])
def test_storage_initialization_failure_preserves_files_and_releases_resources(
    config, remote, monkeypatch, cleaned, backend, site
):
    config.backend = backend
    config.dir_suffix = "blocked"
    target = config.dest_path / config.dir_suffix
    target.mkdir(parents=True)
    keep = target / "keep.txt"
    keep.write_bytes(b"existing file must survive initialization failure")
    state = target / ".mirror-url-state"
    failing_path = state / "owner.json" if site == "marker" else state
    error = PermissionError(errno.EACCES, "Permission denied", str(failing_path))
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    if site == "state":
        deny_mkdir(monkeypatch, state, error)
    elif site == "marker":
        original = Path.write_text

        def write_text(self, *args, **kwargs):
            if self == failing_path:
                raise error
            return original(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", write_text)
    else:
        name = {
            "disk": "DiskSpaceManager",
            "partial": "PartialDownloadManager",
            "scratch": "OwnedScratch",
        }[site]
        monkeypatch.setattr(_base, name, Mock(side_effect=error))
    with pytest.raises(
        DestinationPathError, match="Cannot initialize destination storage"
    ) as stopped:
        MirrorURL(config)
    assert stopped.value.__cause__ is error
    assert stopped.value.__cause__.filename == str(failing_path)
    assert str(error) in str(stopped.value)
    assert keep.read_bytes() == b"existing file must survive initialization failure"
    assert not any(path in remote.files for _, path, _ in remote.requests)
    DestinationLock([target]).close()


def test_unexpected_manager_bug_is_not_suppressed(config, monkeypatch, cleaned):
    error = RuntimeError("unexpected manager failure")
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    monkeypatch.setattr(_base, "PartialDownloadManager", Mock(side_effect=error))
    with pytest.raises(RuntimeError) as stopped:
        MirrorURL(config)
    assert stopped.value is error
    DestinationLock([config.dest_path]).close()


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_dry_run_does_not_attempt_destination_creation(config, remote, monkeypatch, backend):
    config.backend = backend
    config.dir_suffix = "blocked"
    config.dry_run = True
    remote.files["blocked/file.txt"] = b"only plan this download"
    target = config.dest_path / config.dir_suffix
    attempts = deny_mkdir(
        monkeypatch, target, PermissionError(errno.EACCES, "Permission denied", str(target))
    )
    monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert mirror.partial_manager.download_dir is None
        assert mirror.scratch_manager is None
    assert attempts == []
    assert not target.exists()
    assert not any(path in remote.files for _, path, _ in remote.requests)


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("site", ["target", "state"])
@pytest.mark.parametrize("debug", [False, True])
@pytest.mark.parametrize("multiple", [False, True])
def test_cli_reports_permission_error_without_secondary_traceback(
    config, remote, monkeypatch, capsys, cleaned, backend, site, debug, multiple
):
    remote.files["blocked/file.txt"] = b"must not download"
    remote.files["good/file.txt"] = b"independent suffix succeeds"
    target = config.dest_path / "blocked"
    failing_path = target if site == "target" else target / ".mirror-url-state"
    error = PermissionError(errno.EACCES, "Permission denied", str(failing_path))
    deny_mkdir(monkeypatch, failing_path, error)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            str(config.base_url),
            "--dest-path",
            str(config.dest_path),
            "--log-path",
            str(config.log_path),
            "--dir",
            "blocked",
            *(["good"] if multiple else []),
            "--backend",
            backend,
            "--log-file",
            "destination-error",
            "--print-logs",
            "--sequential-downloads",
            "--no-async-metadata",
            "--no-connection-pool-prewarm",
            "--no-security-validation",
            "--request-delay",
            "0",
            "--requests-per-second",
            "0",
            "--max-retries",
            "0",
            *(["--debug"] if debug else []),
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 1
    output = capsys.readouterr().err
    logs = list(config.log_path.glob("destination-error*.log"))
    assert len(logs) == 1
    for text in (output, logs[0].read_text(encoding="utf-8")):
        assert "Destination error for blocked" in text
        assert (
            "Cannot create destination directory"
            if site == "target"
            else "Cannot initialize destination storage"
        ) in text
        assert "Permission denied" in text
        assert str(error) in text
        assert ("Traceback" in text) is debug
        assert "FileNotFoundError" not in text
        assert "Connection failed for blocked" not in text
        assert "FAILED DIRECTORIES (1)" in text
        if not debug:
            assert text.count("Destination error for blocked") == 1
    assert not any(path == "blocked/file.txt" for _, path, _ in remote.requests)
    assert target.exists() is (site == "state")
    if multiple:
        assert (config.dest_path / "good/file.txt").read_bytes() == remote.files["good/file.txt"]
        assert "SUCCESSFUL DIRECTORIES (1)" in output
    else:
        assert "SUCCESSFUL DIRECTORIES: (none)" in output
    DestinationLock([target]).close()
