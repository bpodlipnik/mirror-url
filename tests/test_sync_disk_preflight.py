"""Sync disk preflight gates transfers without sizing unrelated destination files."""

from __future__ import annotations

import pytest

from mirror_url import MirrorConfig, MirrorURL
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote
from test_transfer_backends import job
from test_transfer_backends import origin as origin

pytestmark = pytest.mark.integration


def configure_transfers(config, mode):
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    config.cache_html = False
    config.quiet = True


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_completion_and_unchanged_sync_do_not_require_destination_size(
    remote, config, mode, monkeypatch
):
    configure_transfers(config, mode)
    remote.files = {"large.bin": bytes(range(256)) * 8192, "sub/small": b"small", "empty": b""}
    checks = []

    with MirrorURL(config) as mirror:
        check_space = mirror.check_disk_space

        def preflight(required_bytes):
            checks.append(required_bytes)
            assert not any(
                method == "GET" and path in remote.files for method, path, _ in remote.requests
            ), "Payload requests must wait for the batch disk preflight"
            return check_space(required_bytes)

        def inaccessible_destination_size(path):
            raise PermissionError("Unrelated destination metadata cannot be enumerated")

        monkeypatch.setattr(mirror, "check_disk_space", preflight)
        monkeypatch.setattr(mirror, "get_directory_size", inaccessible_destination_size)
        assert mirror.sync(), mirror.metrics.get_summary()
        assert checks == [100 * 1024 * 1024, sum(map(len, remote.files.values()))]
        for path, content in remote.files.items():
            assert (mirror.target_dir / path).read_bytes() == content
        assert mirror.files_processed.value() == 3
        assert mirror.total_downloaded_size.value() == sum(map(len, remote.files.values()))
        if mode in ("parallel", "streaming"):
            assert mirror.metrics.get_summary()["chunk_downloads"] == 2

        checks.clear()
        remote.requests.clear()
        assert mirror.sync(), mirror.metrics.get_summary()
        assert checks == [100 * 1024 * 1024]
        assert mirror.files_processed.value() == 0
        assert mirror.total_downloaded_size.value() == 0
        assert mirror.metrics.get_summary()["run"]["status"] == "success"
        assert not any(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_failed_batch_disk_preflight_prevents_all_payload_downloads(
    remote, config, mode, monkeypatch
):
    configure_transfers(config, mode)
    remote.files = {"large.bin": bytes(range(256)) * 8192, "sub/small": b"small"}
    checks = []

    with MirrorURL(config) as mirror:
        preserved = mirror.target_dir / "unrelated-local-file"
        preserved.write_bytes(b"keep this file")
        check_space = mirror.check_disk_space

        def space_became_unavailable(required_bytes):
            checks.append(required_bytes)
            # Discovery can outlast the initial readiness check. Deny the batch
            # preflight to model another writer consuming space in that interval.
            return check_space(required_bytes) if len(checks) == 1 else False

        monkeypatch.setattr(mirror, "check_disk_space", space_became_unavailable)
        assert not mirror.sync()
        assert checks == [100 * 1024 * 1024, sum(map(len, remote.files.values()))]
        assert not any(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )
        assert not any((mirror.target_dir / path).exists() for path in remote.files)
        assert preserved.read_bytes() == b"keep this file"
        assert mirror.files_processed.value() == 0
        assert mirror.metrics.get_summary()["run"]["status"] != "success"


@pytest.mark.parametrize("space_available", [True, False])
def test_aiohttp_sync_requires_preflight_but_not_destination_size(
    tmp_path, origin, monkeypatch, space_available
):
    pytest.importorskip("aiohttp")
    values = job(tmp_path, origin, "aiohttp").model_dump()
    values.update(
        mode="mirror",
        url_list=None,
        async_metadata=False,
        connection_pool_prewarm=False,
        handle_symlinks=False,
        sequential_downloads=True,
        overwrite=False,
        verify_content=False,
        missing_files=True,
        quiet=True,
    )
    content = b"aiohttp payload"
    origin.routes["/root/a"] = (200, {"ETag": '"same"'}, content)
    checks = []

    with MirrorURL(MirrorConfig(**values)) as mirror:
        monkeypatch.setattr(mirror, "get_remote_files", lambda: [origin.base + "a"])
        check_space = mirror.check_disk_space

        def preflight(required_bytes):
            checks.append(required_bytes)
            assert not any(method == "GET" for method, *_ in origin.requests)
            if len(checks) == 2 and not space_available:
                return False
            return check_space(required_bytes)

        def inaccessible_destination_size(path):
            raise PermissionError("Unrelated destination metadata cannot be enumerated")

        monkeypatch.setattr(mirror, "check_disk_space", preflight)
        monkeypatch.setattr(mirror, "get_directory_size", inaccessible_destination_size)
        assert mirror.sync() is space_available
        assert checks == [100 * 1024 * 1024, len(content)]
        if space_available:
            assert (mirror.target_dir / "a").read_bytes() == content
            assert mirror.files_processed.value() == 1
            checks.clear()
            origin.requests.clear()
            assert mirror.sync()
            assert checks == [100 * 1024 * 1024]
            assert mirror.files_processed.value() == 0
        else:
            assert not (mirror.target_dir / "a").exists()
            assert mirror.files_processed.value() == 0
        assert not any(method == "GET" for method, *_ in origin.requests)
