"""Preservation across modes, HTTP backends and transfer methods."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from mirror_url import MirrorConfig, MirrorURL, download_url_list
from mirror_url.download_integrity import file_sha256
from mirror_url.security import SecurityValidator
from test_http_mirror_workflows import local_archive

pytestmark = pytest.mark.integration


@pytest.fixture
def remote(monkeypatch):
    with local_archive(monkeypatch) as state:
        state.url = state.url.replace("127.0.0.1", "localhost")
        monkeypatch.setattr(
            SecurityValidator, "resolve_and_validate_hostname", lambda _: "127.0.0.1"
        )
        yield state


def configuration(tmp_path, remote, mode, backend, method, names):
    return MirrorConfig(
        base_url=remote.url,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        mode=mode,
        backend=backend,
        download_files=list(names) if mode == "download" else [],
        overwrite=mode == "download",
        verify_content=True,
        security_validation=False,
        async_metadata=False,
        connection_pool_prewarm=False,
        sequential_downloads=method == "sequential",
        parallel_downloads=method == "parallel",
        streaming_parallel=method == "streaming",
        min_chunk_size_mb=1,
        max_chunks_per_file=2,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
    )


def execute(config):
    if config.mode == "download":
        return download_url_list(config)["success"]
    with MirrorURL(config) as mirror:
        return mirror.sync()


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("method", ["sequential", "parallel", "streaming"])
def test_unusual_names_and_repeated_runs_keep_exact_bytes(tmp_path, remote, mode, backend, method):
    remote.files = {
        "nested/caf\u00e9 [WARNING] #100%.bin": bytes(range(256)) * 8193,
        "Traceback.bin": b"small first version",
        "empty.txt": b"",
    }
    config = configuration(tmp_path, remote, mode, backend, method, remote.files)
    for repeat in range(2):
        assert execute(config)
        for name, data in remote.files.items():
            actual = config.dest_path / name
            assert actual.read_bytes() == data
            assert hashlib.sha256(actual.read_bytes()).digest() == hashlib.sha256(data).digest()
        assert not list(config.dest_path.rglob("work_*"))
        assert not list(config.dest_path.rglob("*.mirror-partial"))
    if method != "sequential":
        assert any(verb == "GET" and "Range" in headers for verb, path, headers in remote.requests)


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("method", ["sequential", "parallel", "streaming"])
def test_concurrent_destination_edit_survives_publication(
    tmp_path, remote, monkeypatch, mode, backend, method
):
    remote.files = {"a.bin": b"R" * (2 * 1024 * 1024 + 17)}
    config = configuration(tmp_path, remote, mode, backend, method, remote.files)
    config.dest_path.mkdir()
    local = config.dest_path / "a.bin"
    local.write_bytes(b"previous complete version")
    external = b"concurrent user edit that must survive"
    changes = []

    def edit_after_hash(path, enabled=True):
        digest = file_sha256(path, enabled)
        if Path(path) != local and not changes:
            local.write_bytes(external)
            changes.append(str(path))
        return digest

    for module in ("mirror_url._core.downloads", "mirror_url.download", "mirror_url.transfers"):
        monkeypatch.setattr(module + ".file_sha256", edit_after_hash)
    accepted = execute(config)
    print(
        json.dumps(
            {
                "mode": mode,
                "backend": backend,
                "method": method,
                "success": accepted,
                "external_edit_preserved": local.read_bytes() == external,
                "injection_reached": bool(changes),
            }
        )
    )
    assert changes, "Fault injection must reach the real pre-publication boundary"
    assert local.read_bytes() == external, "A concurrent replacement must survive"
    assert not accepted


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_real_http_final_assembly_eio_blocks_publication(
    tmp_path, remote, monkeypatch, mode, backend
):
    import errno
    import os

    import mirror_url.download as download

    remote.files = {"a.bin": b"R" * (2 * 1024 * 1024 + 17)}
    config = configuration(tmp_path, remote, mode, backend, "parallel", remote.files)
    config.dest_path.mkdir()
    local = config.dest_path / "a.bin"
    local.write_bytes(b"old final")
    real_fsync = os.fsync
    failures = []

    def fail_final_flush(descriptor):
        info = os.fstat(descriptor)
        for path in config.dest_path.rglob("staging.assembling"):
            if os.path.samestat(info, path.stat()) and path.read_bytes()[:1] == b"R":
                failures.append(str(path))
                raise OSError(errno.EIO, "injected final assembly flush failure")
        return real_fsync(descriptor)

    monkeypatch.setattr(
        download.mmap,
        "mmap",
        lambda *a, **k: (_ for _ in ()).throw(OSError(errno.ENODEV, "mmap unavailable")),
    )
    monkeypatch.setattr(download.os, "fsync", fail_final_flush)
    accepted = execute(config)
    print(
        json.dumps(
            {
                "mode": mode,
                "backend": backend,
                "success_after_eio": accepted,
                "old_final_preserved": local.read_bytes() == b"old final",
                "eio_injected": len(failures),
            }
        )
    )
    assert failures, "Final flush fault injection must reach the actual staging file"
    assert not accepted, "A failed final durable write must prevent publication"
    assert local.read_bytes() == b"old final"


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_real_http_saved_partial_hardlink_preserves_unrelated_bytes(tmp_path, remote, backend):
    import os

    from mirror_url.download import PartialDownloadManager

    remote.files = {"a.bin": b"new remote version"}
    config = configuration(tmp_path, remote, "mirror", backend, "sequential", remote.files)
    config.dest_path.mkdir()
    local = config.dest_path / "a.bin"
    local.write_bytes(b"old final")
    saved = tmp_path / "outside.bin"
    original = b"irreplaceable outside content"
    saved.write_bytes(original)
    partial = PartialDownloadManager(config.dest_path).get_partial_path(local)
    os.link(saved, partial)
    accepted = execute(config)
    print(
        json.dumps(
            {
                "backend": backend,
                "success": accepted,
                "outside_before": original.decode(),
                "outside_after": saved.read_text(),
            }
        )
    )
    assert saved.read_bytes() == original, "A saved partial must not modify another hardlink"


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_real_http_cache_temporary_alias_preserves_unrelated_bytes(
    tmp_path, remote, backend, alias
):
    import os

    remote.files = {"a.bin": b"remote payload"}
    config = configuration(tmp_path, remote, "mirror", backend, "sequential", remote.files)
    outside = tmp_path / "outside-cache.bin"
    original = b"irreplaceable independent data"
    outside.write_bytes(original)
    with MirrorURL(config) as mirror:
        temporary = mirror.cache_file.with_suffix(".json.tmp")
        if alias == "symlink":
            temporary.symlink_to(outside)
        else:
            os.link(outside, temporary)
        accepted = mirror.sync()
    print(
        json.dumps(
            {
                "backend": backend,
                "alias": alias,
                "sync_success": accepted,
                "unrelated_data_preserved": outside.read_bytes() == original,
                "outside_after_size": outside.stat().st_size,
            }
        )
    )
    assert outside.read_bytes() == original, "Cache staging must not overwrite unrelated aliases"
