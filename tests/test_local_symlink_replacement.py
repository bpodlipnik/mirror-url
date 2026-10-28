"""Replace rsync-era file aliases without following or prematurely removing them."""

from __future__ import annotations

import hashlib
import logging
import os
import stat
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorURL, download_url_list
from mirror_url.download_integrity import file_identity
from mirror_url.security import PathSafety, SecurityValidator
from mirror_url.transfers import plan_targets
from mirror_url.utils import url_within_scope
from test_download_mode_chunks import configuration
from test_download_mode_chunks import ranges as ranges
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote
from test_immediate_audit_fixes import BASE, check_async, response
from test_immediate_audit_fixes import mirror as mirror
from test_transfer_backends import assert_no_scratch
from test_transfer_backends import backend as backend

NAME = "2m_orcl_100821.fts"
ORIGINAL = "2m_orcl_100813.fts"
OLD = b"original archive data"


def archive_link(root, kind, *, name=NAME, content=OLD):
    root.mkdir(parents=True, exist_ok=True)
    link = root / name
    target = (root.parent if kind == "outside" else root) / ORIGINAL
    if kind in ("relative", "outside"):
        target.write_bytes(content)
    if kind == "loop":
        target = link
    value = target if kind == "outside" else target.name
    try:
        link.symlink_to(value)
    except (OSError, NotImplementedError) as error:
        pytest.skip(f"File symlinks unavailable: {error}")
    return link, target


def assert_target_preserved(target, kind, content=OLD):
    if kind in ("relative", "outside"):
        assert target.read_bytes() == content
    elif kind == "dangling":
        assert not target.exists()


@pytest.mark.parametrize("kind", ["relative", "outside", "dangling", "loop"])
def test_mapping_keeps_link_path_and_resume_state_uses_remote_name(mirror, kind):
    link, target = archive_link(mirror.target_dir, kind)
    url = BASE + NAME
    assert PathSafety.safe_join(mirror.target_dir, NAME) is None
    assert PathSafety.safe_join(mirror.target_dir, NAME, allow_leaf_symlink=True) == link
    mirror._validate_remote_paths([url])
    assert mirror._get_local_path_from_url(url) == link
    assert plan_targets([url], BASE, mirror.target_dir, mirror.config) == [(url, link)]
    partial = mirror.partial_manager.get_partial_path(link)
    assert partial != mirror.partial_manager.get_partial_path(mirror.target_dir / ORIGINAL)
    link.unlink()
    assert mirror.partial_manager.get_partial_path(link) == partial
    assert_target_preserved(target, kind)


@pytest.mark.parametrize("kind", ["relative", "outside", "dangling", "loop"])
@pytest.mark.parametrize("missing_files", [False, True])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_metadata_always_schedules_link_replacement(mirror, kind, missing_files, asynchronous):
    link, target = archive_link(mirror.target_dir, kind)
    mirror.config.missing_files = missing_files
    mirror.connection_manager.request.side_effect = AssertionError("No linked-file freshness check")
    if asynchronous:
        planned, manager = check_async(mirror, [BASE + NAME])
        assert not manager.calls
    else:
        planned = mirror._check_files_sync([BASE + NAME])
    assert planned == [(BASE + NAME, link)]
    mirror.connection_manager.request.assert_not_called()
    assert link.is_symlink()
    assert_target_preserved(target, kind)


@pytest.mark.parametrize("kind", ["relative", "outside", "dangling", "loop"])
def test_single_transfer_removes_link_and_preserves_target(mirror, kind, caplog):
    caplog.set_level(logging.INFO)
    link, target = archive_link(mirror.target_dir, kind)

    def receive(*args, **kwargs):
        assert link.is_symlink(), "Do not unlink before a verified download is available"
        assert_target_preserved(target, kind)
        return response(headers={"Content-Length": "3"}, content=b"new")

    mirror.connection_manager.request.side_effect = receive
    assert mirror._download_file_single(BASE + NAME, link)
    assert not link.is_symlink()
    assert stat.S_ISREG(link.lstat().st_mode)
    assert link.read_bytes() == b"new"
    assert_target_preserved(target, kind)
    assert mirror.files_processed.value() == 1
    assert mirror.files_failed.value() == 0
    notes = [r.getMessage() for r in caplog.records if "Replaced local symlink" in r.getMessage()]
    assert notes == [f"Replaced local symlink with downloaded file: {link}"]


@pytest.mark.parametrize("fault", ["short", "network", "flush", "replace"])
def test_failed_single_transfer_keeps_original_link(mirror, monkeypatch, caplog, fault):
    caplog.set_level(logging.INFO)
    link, target = archive_link(mirror.target_dir, "relative")
    original = file_identity(link, allow_symlink=True)
    mirror.connection_manager.request.return_value = response(
        headers={"Content-Length": "4" if fault == "short" else "3"}, content=b"new"
    )
    if fault == "network":
        mirror.connection_manager.request.side_effect = httpx.ReadError("connection lost")
    elif fault == "flush":
        monkeypatch.setattr(
            "mirror_url._core.downloads.os.fsync", Mock(side_effect=OSError("disk"))
        )
    elif fault == "replace":
        replace = os.replace

        def fail_publication(source, destination):
            if destination == link:
                raise OSError("replacement denied")
            return replace(source, destination)

        monkeypatch.setattr("mirror_url.download_integrity.os.replace", fail_publication)
    assert not mirror._download_file_single(BASE + NAME, link)
    assert link.is_symlink()
    assert os.readlink(link) == ORIGINAL
    assert file_identity(link, allow_symlink=True) == original
    assert target.read_bytes() == OLD
    assert mirror.files_failed.value() == 1
    assert "Replaced local symlink" not in caplog.text


@pytest.mark.parametrize("change", ["file", "symlink", "removed"])
def test_destination_changed_during_download_is_preserved(mirror, change):
    link, target = archive_link(mirror.target_dir, "relative")
    other = mirror.target_dir / "other"
    other.write_bytes(b"external edit")

    def receive(*args, **kwargs):
        if change == "removed":
            link.unlink()
        else:
            replacement = mirror.target_dir / "replacement"
            if change == "file":
                replacement.write_bytes(b"external edit")
            else:
                replacement.symlink_to(other.name)
            os.replace(replacement, link)
        return response(headers={"Content-Length": "3"}, content=b"new")

    mirror.connection_manager.request.side_effect = receive
    assert not mirror._download_file_single(BASE + NAME, link)
    if change == "removed":
        assert not link.exists() and not link.is_symlink()
    else:
        assert link.is_symlink() == (change == "symlink")
        assert link.read_bytes() == b"external edit"
    assert target.read_bytes() == OLD
    assert other.read_bytes() == b"external edit"


def test_symlink_permission_does_not_apply_to_staging_or_parent_directories(mirror, tmp_path):
    link, target = archive_link(mirror.target_dir, "relative")
    with pytest.raises(ValueError, match="unsafe or unowned"):
        file_identity(link)
    assert file_identity(link, allow_symlink=True) is not None
    directory = tmp_path / "outside"
    directory.mkdir()
    parent = mirror.target_dir / "linked-dir"
    parent.symlink_to(directory, target_is_directory=True)
    assert (
        PathSafety.safe_join(mirror.target_dir, "linked-dir", "a", allow_leaf_symlink=True) is None
    )
    assert PathSafety.safe_join(parent, "a", allow_leaf_symlink=True) is None
    with pytest.raises(ValueError, match="unsafe"):
        mirror._validate_remote_paths([BASE + "linked-dir/a"])
    assert link.is_symlink() and target.read_bytes() == OLD


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize("remote_target", [False, True])
def test_real_http_mirror_replaces_alias_and_continues(
    remote, config, backend, mode, remote_target, monkeypatch, caplog
):
    caplog.set_level(logging.INFO)
    payload = bytes(range(256)) * 8193
    remote.files = {NAME: payload, "after.txt": b"another file"}
    if remote_target:
        remote.files[ORIGINAL] = b"separate remote target content"
    config.backend = backend
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    config.missing_files = not remote_target
    config.verify_content = True
    if backend == "aiohttp":

        def fixture_scope(url, scope):
            assert url.startswith(remote.url) and url_within_scope(url, scope)

        def fixture_dns(host):
            assert host == "127.0.0.1"
            return host

        monkeypatch.setattr("mirror_url.transfers.validate_url", fixture_scope)
        monkeypatch.setattr(SecurityValidator, "resolve_and_validate_hostname", fixture_dns)
    with MirrorURL(config) as active:
        # MirrorURL configures its real handlers during construction.
        logging.getLogger().addHandler(caplog.handler)
        link, target = archive_link(active.target_dir, "relative", content=payload)
        assert active.sync()
        assert not link.is_symlink() and stat.S_ISREG(link.lstat().st_mode)
        assert link.read_bytes() == payload
        assert target.read_bytes() == (remote.files[ORIGINAL] if remote_target else payload)
        assert link.stat().st_ino != target.stat().st_ino
        assert (active.target_dir / "after.txt").read_bytes() == b"another file"
        assert active.files_processed.value() == (3 if remote_target else 2)
        assert active.files_failed.value() == 0
        receipt = active.cache_manager.get_file_metadata(link)
        assert receipt["sha256"] == hashlib.sha256(payload).hexdigest()
        if mode != "sequential":
            ranges = [
                h["Range"]
                for method, name, h in remote.requests
                if method == "GET" and name == NAME and "Range" in h
            ]
            assert len(ranges) == 2
        assert "Replaced local symlink with downloaded file" in caplog.text
        assert_no_scratch(active.target_dir)


@pytest.mark.integration
@pytest.mark.parametrize(
    "mode", ["sequential_downloads", "parallel_downloads", "streaming_parallel"]
)
def test_real_url_list_replaces_outside_alias_with_both_backends(
    tmp_path, ranges, backend, mode, caplog
):
    caplog.set_level(logging.INFO)
    config = configuration(tmp_path, ranges, mode, backend=backend, overwrite=True)
    # Sequential transfers never enter the fixture's range concurrency barrier.
    link, target = archive_link(config.dest_path, "outside", name="a")
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 1
    assert not link.is_symlink() and link.read_bytes() == ranges.payload
    assert target.read_bytes() == OLD
    assert "Replaced local symlink with downloaded file" in caplog.text
    assert_no_scratch(config.dest_path)


@pytest.mark.integration
def test_url_list_still_requires_overwrite_for_an_unreceipted_link(tmp_path, ranges, backend):
    config = configuration(tmp_path, ranges, "sequential_downloads", backend=backend)
    link, target = archive_link(config.dest_path, "outside", name="a")
    with pytest.raises(ValueError, match="use --overwrite explicitly"):
        download_url_list(config)
    assert link.is_symlink() and target.read_bytes() == OLD
    assert not ranges.requests


@pytest.mark.integration
def test_replacement_failure_is_a_file_error_and_other_files_continue(
    remote, config, monkeypatch, caplog
):
    caplog.set_level(logging.INFO)
    remote.files = {NAME: b"new", "after.txt": b"another file"}
    with MirrorURL(config) as active:
        logging.getLogger().addHandler(caplog.handler)
        link, target = archive_link(active.target_dir, "relative")
        original = file_identity(link, allow_symlink=True)
        replace = os.replace

        def fail_publication(source, destination):
            if destination == link:
                raise OSError("replacement denied")
            return replace(source, destination)

        monkeypatch.setattr("mirror_url.download_integrity.os.replace", fail_publication)
        assert active.sync() is False
        assert link.is_symlink() and file_identity(link, allow_symlink=True) == original
        assert target.read_bytes() == OLD
        assert (active.target_dir / "after.txt").read_bytes() == b"another file"
        assert active.files_failed.value() == 1
        assert "Fatal error" not in caplog.text
        assert "Replaced local symlink with downloaded file" not in caplog.text


@pytest.mark.integration
def test_dry_run_reports_replacement_without_removing_link(remote, config, caplog):
    caplog.set_level(logging.INFO)
    remote.files = {NAME: b"new"}
    config.dry_run = True
    config.missing_files = True
    with MirrorURL(config) as active:
        logging.getLogger().addHandler(caplog.handler)
        link, target = archive_link(active.target_dir, "relative")
        assert active.sync()
        assert link.is_symlink() and target.read_bytes() == OLD
        assert not any(method == "GET" and name == NAME for method, name, _ in remote.requests)
        assert "local symlink will be replaced" in caplog.text
        assert "Replaced local symlink with downloaded file" not in caplog.text
