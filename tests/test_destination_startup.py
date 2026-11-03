"""Basic startup errors run in the fast lane without DNS or an HTTP server."""

from __future__ import annotations

import errno
import logging
import os
import sys
from pathlib import Path, PureWindowsPath
from unittest.mock import Mock

import pytest

from mirror_url import DestinationPathError, MirrorConfig, MirrorURL, cli
from mirror_url.connection import ConnectionManager
from mirror_url.destination_lock import DestinationLock


@pytest.fixture
def startup(tmp_path, monkeypatch):
    config = MirrorConfig(
        base_url="https://example.com/archive/",
        dest_path=tmp_path / "destination",
        log_path=tmp_path / "logs",
        dir_suffix="261030",
        async_metadata=False,
        connection_pool_prewarm=False,
        no_cache=True,
    )
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(MirrorURL, "test_connection", lambda _: True)
    monkeypatch.setattr("mirror_url._core._base.HealthCheckServer.start", lambda _: None)
    network = Mock(side_effect=AssertionError("Startup failure must not send HTTP requests"))
    monkeypatch.setattr(ConnectionManager, "request", network)
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield config
    root.handlers[:] = handlers
    root.setLevel(level)
    network.assert_not_called()


@pytest.mark.parametrize("entrypoint", ["api", "cli"])
@pytest.mark.parametrize("site", ["target", "state"])
@pytest.mark.parametrize("error_number", [errno.EACCES, errno.ENOSPC])
@pytest.mark.parametrize("path_style", ["native", "windows"])
def test_unusable_destination_stops_startup(
    startup, monkeypatch, capsys, entrypoint, site, error_number, path_style
):
    config = startup
    target = config.dest_path / config.dir_suffix
    failed_path = target if site == "target" else target / ".mirror-url-state"
    original = Path.mkdir
    filename = (
        str(failed_path)
        if path_style == "native"
        else str(PureWindowsPath("C:/archive") / failed_path.relative_to(config.dest_path))
    )
    error = OSError(error_number, os.strerror(error_number), filename)

    def mkdir(self, *args, **kwargs):
        if self == failed_path:
            raise error
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    if entrypoint == "api":
        monkeypatch.setattr(MirrorURL, "setup_logging", lambda _: None)
        with pytest.raises(DestinationPathError) as stopped:
            MirrorURL(config)
        assert stopped.value.__cause__ is error
        assert stopped.value.__cause__.filename == filename
        assert str(error) in str(stopped.value)
    else:
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
                config.dir_suffix,
                "--log-file",
                "startup-error",
                "--print-logs",
                "--no-async-metadata",
                "--no-connection-pool-prewarm",
            ],
        )
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        assert stopped.value.code == 1
        output = capsys.readouterr().err
        assert "Destination error for 261030" in output
        assert os.strerror(error_number) in output
        assert str(error) in output
        assert "Traceback" not in output
        assert "FileNotFoundError" not in output
        assert "FAILED DIRECTORIES (1)" in output
    assert not failed_path.exists()
    DestinationLock([target]).close()


@pytest.mark.parametrize("debug", [False, True])
def test_startup_error_honors_debug_from_configuration(startup, monkeypatch, capsys, debug):
    config = startup
    config.debug = debug
    path = config.log_path.parent / "config.json"
    path.write_text(config.model_dump_json(), encoding="utf-8")
    target = config.dest_path / config.dir_suffix
    original = Path.mkdir

    def mkdir(self, *args, **kwargs):
        if self == target:
            raise PermissionError(errno.EACCES, "Permission denied", str(target))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--config",
            str(path),
            "--dir",
            config.dir_suffix,
            "--log-file",
            "config-startup-error",
            "--print-logs",
        ],
    )
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 1
    output = capsys.readouterr().err
    assert "Destination error for 261030" in output
    assert ("Traceback" in output) is debug
    assert "FileNotFoundError" not in output
    DestinationLock([target]).close()
