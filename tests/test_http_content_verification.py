"""Content verification through real HTTP and persisted caches."""

from __future__ import annotations

import hashlib

import pytest

from mirror_url import MirrorURL
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

pytestmark = pytest.mark.integration


def file_gets(remote):
    return sum(method == "GET" and path in remote.files for method, path, _ in remote.requests)


@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_content_receipts_persist_and_repair_same_size_edits(remote, config, mode):
    remote.files = {"large.bin": bytes(range(256)) * 8193, "empty": b""}
    config.verify_content = True
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        for name, content in remote.files.items():
            assert mirror.cache_manager.get_file_metadata(mirror.target_dir / name)["sha256"] == (
                hashlib.sha256(content).hexdigest()
            )
    before = file_gets(remote)
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert file_gets(remote) == before
        (mirror.target_dir / "large.bin").write_bytes(b"x" * len(remote.files["large.bin"]))
        assert mirror.sync()
        assert (mirror.target_dir / "large.bin").read_bytes() == remote.files["large.bin"]
        assert file_gets(remote) > before


@pytest.mark.parametrize("no_etag", [False, True])
def test_no_server_etag_still_saves_and_verifies_receipts(remote, config, no_etag):
    remote.etag = False
    remote.files = {"a.txt": b"alpha"}
    config.verify_content = True
    config.no_etag = no_etag
    with MirrorURL(config) as mirror:
        assert mirror.sync()
    before = file_gets(remote)
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        assert file_gets(remote) == before
        (mirror.target_dir / "a.txt").write_bytes(b"xxxxx")
        assert mirror.sync()
        assert (mirror.target_dir / "a.txt").read_bytes() == b"alpha"
        assert file_gets(remote) == before + 1


def test_no_cache_content_verification_redownloads_existing_files(remote, config):
    remote.files = {"a.txt": b"alpha"}
    config.verify_content = True
    config.no_cache = True
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        before = file_gets(remote)
        assert mirror.sync()
        assert file_gets(remote) == before + 1
