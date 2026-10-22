"""Subprocess fixture for native interprocess ownership tests."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from mirror_url import DestinationLockError, MirrorConfig, MirrorURL
from mirror_url import main as cli_main
from mirror_url._core._base import HealthCheckServer
from mirror_url.destination_lock import DestinationLock
from mirror_url.security import SecurityValidator
from mirror_url.transport import SecureAsyncTransport, SecureTransport


def main():
    mode, config_path, ready_path = sys.argv[1:4]
    extra = sys.argv[4:]
    data = json.loads(Path(config_path).read_text())
    try:
        if mode == "raw":
            owner = DestinationLock([Path(path) for path in data])
        elif mode in ("mirror", "cli", "cli-unshared", "cli-hold", "cli-once"):
            config = MirrorConfig.from_dict(data)
            host = urlsplit(config.base_url).hostname
            assert host in ("127.0.0.1", "files.example")
            if host == "127.0.0.1":
                SecureTransport.handle_request = httpx.HTTPTransport.handle_request
                SecureAsyncTransport.handle_async_request = (
                    httpx.AsyncHTTPTransport.handle_async_request
                )
            else:
                SecurityValidator.resolve_and_validate_hostname = lambda _: "127.0.0.1"
            HealthCheckServer.start = lambda _: None
            if mode in ("cli-hold", "cli-once"):
                # Exercise actual filename collisions without racing the clock.
                time.strftime = lambda *args: "20261022_020000"
            if mode == "cli-hold":
                sync = MirrorURL.sync

                def held_sync(self):
                    Path(ready_path).write_text("ready")
                    sys.stdin.readline()
                    return sync(self)

                MirrorURL.sync = held_sync
            if mode.startswith("cli"):
                sys.argv = ["mirror-url", "--config", config_path]
                if mode == "cli":
                    sys.argv.extend(["--log-file", "contention"])
                sys.argv.extend(extra)
                cli_main()
                if mode in ("cli-hold", "cli-once"):
                    return 0
            owner = MirrorURL(config)
        else:
            raise ValueError(mode)
        try:
            Path(ready_path).write_text("ready")
            sys.stdin.readline()  # Parent releases or kills us, without signal handlers.
        finally:
            if mode == "raw":
                owner.close()
            else:
                owner.cleanup()
    except DestinationLockError as error:
        print(str(error), file=sys.stderr)
        return 17
    return 0


if __name__ == "__main__":
    sys.exit(main())
