"""Subprocess-only crash checkpoints; never imported by production code."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import threading
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from mirror_url import MirrorConfig, MirrorURL
from mirror_url._core._base import HealthCheckServer
from mirror_url.transport import SecureAsyncTransport, SecureTransport


def main():
    config_path, phase, ready_path = sys.argv[1:]
    config = MirrorConfig.from_dict(json.loads(Path(config_path).read_text()))
    # Explicitly restricted to the parent test's loopback fixture. The normal
    # production transports continue to reject private addresses.
    assert urlsplit(config.base_url).hostname == "127.0.0.1"
    SecureTransport.handle_request = httpx.HTTPTransport.handle_request
    SecureAsyncTransport.handle_async_request = httpx.AsyncHTTPTransport.handle_async_request
    HealthCheckServer.start = lambda _: None

    with MirrorURL(config) as mirror:
        final = mirror.target_dir / "large.bin"
        lock = threading.Lock()

        def pause(**details):
            with lock:
                Path(ready_path).write_text(json.dumps({"phase": phase, **details}))
                # Parent kills this process; no exceptions, signal handlers,
                # or context-manager cleanup run at the checkpoint.
                threading.Event().wait()

        if phase == "download":
            if config.sequential_downloads:
                update = mirror.partial_manager.update_activity

                def partial_update(path, count=0):
                    update(path, count)
                    if count:
                        pause(partial=str(path), size=path.stat().st_size)

                mirror.partial_manager.update_activity = partial_update
            else:
                chunk_download = mirror.parallel_manager._download_chunk_with_semaphore

                def completed_chunk(chunk):
                    result = chunk_download(chunk)
                    if result:
                        pause(chunk=chunk.chunk_id)
                    return result

                mirror.parallel_manager._download_chunk_with_semaphore = completed_chunk
        elif phase in ("publish-before", "publish-after"):
            replace = os.replace

            def checkpoint_replace(source, destination):
                selected = Path(destination) == final
                if selected and phase == "publish-before":
                    pause()
                replace(source, destination)
                if selected and phase == "publish-after":
                    pause()

            os.replace = checkpoint_replace
        elif phase == "cache":
            dump = json.dump
            cache_tmp = mirror.cache_file.with_suffix(".json.tmp")
            initial_digest = hashlib.sha256(final.read_bytes()).hexdigest()

            def incomplete_cache(data, file, *args, **kwargs):
                receipt = data.get("_files", {}).get(str(final.resolve()), {})
                # Skip the earlier scan-only cache save. Interrupt the save
                # containing the receipt for the newly published file.
                if (
                    isinstance(file.name, (str, os.PathLike))
                    and Path(file.name) == cache_tmp
                    and receipt.get("sha256") != initial_digest
                    and receipt.get("sha256") == hashlib.sha256(final.read_bytes()).hexdigest()
                ):
                    previous = hashlib.sha256(mirror.cache_file.read_bytes()).hexdigest()
                    text = json.dumps(data)
                    file.write(text[: len(text) // 2])
                    file.flush()
                    os.fsync(file.fileno())
                    pause(previous_cache_sha256=previous)
                return dump(data, file, *args, **kwargs)

            json.dump = incomplete_cache
        elif phase == "cleanup":
            move = shutil.move

            def interrupted_move(source, destination, *args, **kwargs):
                result = move(source, destination, *args, **kwargs)
                if Path(source).name.startswith("old_"):
                    pause(moved=str(destination))
                return result

            shutil.move = interrupted_move
        elif phase != "none":
            raise ValueError(phase)

        assert mirror.sync(), "Recovery sync failed"


if __name__ == "__main__":
    main()
