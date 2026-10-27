"""Regressions for MOVE selection, saved partials and durable publication."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import sys
from unittest.mock import Mock

import pytest

from mirror_url import MirrorConfig, cli
from mirror_url.enums import CleanupPolicy
from mirror_url.security import SecurityValidator
from test_download_failure_contract import manager as manager
from test_http_mirror_workflows import local_archive
from test_immediate_audit_fixes import BASE, response
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_transfer_storage_lifecycle import make_download


@pytest.mark.parametrize("code", [errno.EIO, errno.ENOSPC])
def test_final_assembly_flush_error_preserves_old_final(manager, monkeypatch, code):
    download = make_download(manager)
    manager.MMAP_MAX_FILE_SIZE = 0
    fsync = Mock(side_effect=[None, OSError(code, os.strerror(code))])
    monkeypatch.setattr("mirror_url.download.os.fsync", fsync)
    accepted = manager.assemble_file(download)
    print(
        json.dumps(
            {
                "accepted": accepted,
                "errno": code,
                "final": download.final_path.read_text(),
                "status": download.status,
                "fsync_calls": fsync.call_count,
            }
        )
    )
    assert not accepted, "A real final flush error must prevent publication"
    assert download.final_path.read_bytes() == b"original"


@pytest.mark.parametrize("verify", [False, True])
def test_hardlinked_partial_cannot_truncate_unrelated_file(mirror, tmp_path, verify):
    mirror.config.verify_content = verify
    final = mirror.target_dir / "a"
    final.write_bytes(b"old final")
    partial = mirror.partial_manager.get_partial_path(final)
    outside = tmp_path / "important-outside-mirror.bin"
    original = b"irreplaceable unrelated bytes"
    outside.write_bytes(original)
    os.link(outside, partial)
    mirror.connection_manager.request.return_value = response(
        headers={"Content-Length": "3", "ETag": '"new"'}, content=b"new"
    )
    accepted = mirror._download_file_single(BASE + "a", final)
    print(
        json.dumps(
            {
                "accepted": accepted,
                "verify": verify,
                "unrelated_before": original.decode(),
                "unrelated_after": outside.read_text(),
                "final": final.read_text(),
            }
        )
    )
    assert outside.read_bytes() == original, "Saved partial hardlinks must never be truncated"
    assert not accepted
    assert final.read_bytes() == b"old final"


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
@pytest.mark.integration
def test_selected_move_archive_never_becomes_another_job_payload(
    tmp_path, monkeypatch, backend, mode
):
    with local_archive(monkeypatch) as remote:
        remote.url = remote.url.replace("127.0.0.1", "localhost")
        monkeypatch.setattr(
            SecurityValidator, "resolve_and_validate_hostname", lambda _: "127.0.0.1"
        )
        remote.files = {
            "data/new.txt": b"new science",
            "data_obsolete/old.txt": b"R" * (2 * 1024 * 1024 + 17),
        }
        dest = tmp_path / "mirror"
        original = b"only remaining copy of archived science"
        saved = dest / "data/old.txt"
        saved.parent.mkdir(parents=True)
        saved.write_bytes(original)
        config = MirrorConfig(
            base_url=remote.url,
            dest_path=dest,
            log_path=tmp_path / "logs",
            backend=backend,
            cleanup_policy=CleanupPolicy.MOVE,
            security_validation=False,
            async_metadata=False,
            connection_pool_prewarm=False,
            sequential_downloads=mode == "sequential",
            parallel_downloads=mode == "parallel",
            streaming_parallel=mode == "streaming",
            min_chunk_size_mb=1,
            max_chunks_per_file=2,
            request_delay=0,
            requests_per_second=0,
            max_retries=0,
            verify_content=True,
        )
        path = tmp_path / "config.json"
        path.write_text(config.model_dump_json())
        monkeypatch.setattr(
            sys, "argv", ["mirror-url", "--config", str(path), "--dir", "data", "data_obsolete"]
        )
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        snapshot = {
            str(p.relative_to(dest)): p.read_bytes()
            for p in dest.rglob("*")
            if p.is_file() and ".mirror-url-state" not in p.parts
        }
        print(
            json.dumps(
                {
                    "backend": backend,
                    "mode": mode,
                    "exit_code": stopped.value.code,
                    "files": {
                        name: {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                        for name, data in snapshot.items()
                    },
                    "original_archived_bytes_preserved": original in snapshot.values(),
                }
            )
        )
        assert stopped.value.code == 1
        assert not remote.requests, "Reject conflicting archives before starting any job"
        assert original in snapshot.values(), "MOVE must retain the original obsolete bytes"
