"""Regression coverage for the detailed 3.9.10 source and documentation audit."""

from __future__ import annotations

import json
import socket
import sys
import threading
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

from mirror_url import MirrorConfig, MirrorURL, cli, download_url_list
from mirror_url._core.cleanup import CleanupMixin
from mirror_url.enums import CleanupPolicy
from mirror_url.exceptions import ConfigError, SecurityError
from mirror_url.filename_mapping import selected_directory_targets
from mirror_url.metrics import MetricsCollector
from mirror_url.progress import DownloadProgress
from mirror_url.security import PathSafety, SecurityValidator
from mirror_url.utils import is_reserved_windows_filename
from test_http_mirror_workflows import local_archive
from test_transfer_backends import job
from test_transfer_backends import origin as origin


def test_literal_suffix_preflight_preserves_existing_files(tmp_path, monkeypatch):
    destination = tmp_path / "dest"
    saved = destination / "_CON" / "first.txt"
    saved.parent.mkdir(parents=True)
    saved.write_bytes(b"preserve")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            "https://files.example/",
            "--dest-path",
            str(destination),
            "--log-path",
            str(tmp_path / "logs"),
            "--dir",
            "CON",
            "_CON",
            "--cleanup",
            "delete",
        ],
    )
    monkeypatch.setattr(cli, "MirrorURL", lambda *a, **k: pytest.fail("Started before preflight"))
    with pytest.raises(SystemExit) as stopped:
        cli.main()
    assert stopped.value.code == 1
    assert saved.read_bytes() == b"preserve"
    assert not (tmp_path / "logs").exists()


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("suffix", ["NUL.backup.txt", ".mirror-url-state/nested", "a//b", "a/./b"])
def test_library_suffixes_reject_lossy_or_reserved_paths(tmp_path, mode, suffix):
    config = MirrorConfig(
        base_url="https://files.example/",
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        mode=mode,
        dir_suffix=suffix,
        download_files=["a"] if mode == "download" else [],
    )
    with pytest.raises(ConfigError, match="Unsafe directory selection"):
        MirrorURL(config) if mode == "mirror" else download_url_list(config)
    assert not config.dest_path.exists()


@pytest.mark.parametrize("suffixes", [["one", "ONE"], ["caf\u00e9", "cafe\u0301"]])
def test_selected_roots_cannot_alias_on_destination(tmp_path, monkeypatch, suffixes):
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: False)
    with pytest.raises(ConfigError, match="collide"):
        selected_directory_targets(tmp_path / "new", suffixes, max_filename_len=255)


def test_case_sensitive_destination_preserves_distinct_selections(tmp_path, monkeypatch):
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: True)
    root = tmp_path / "new"
    assert selected_directory_targets(root, ["one", "ONE", "One"], max_filename_len=255) == [
        root / "one",
        root / "ONE",
        root / "One",
    ]


@pytest.mark.parametrize(
    "ip", ["100.64.0.1", "100.127.255.254", "224.0.0.1", "ff0e::1", "::ffff:100.64.0.1"]
)
def test_nonpublic_dns_answers_are_rejected(monkeypatch, ip):
    assert SecurityValidator.is_private_ip(ip)
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *a, **k: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 0))],
    )
    with pytest.raises(SecurityError):
        SecurityValidator.resolve_and_validate_hostname("files.example")


@pytest.mark.parametrize("suffix", ["a#1", "a?b", "a%b", "a space"])
def test_download_suffix_is_a_literal_url_path(tmp_path, suffix):
    config = MirrorConfig(
        base_url="https://files.example/root/?listing=1",
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        mode="download",
        download_files=[suffix + "/file.txt"],
        dir_suffix=suffix,
        dry_run=True,
    )
    assert download_url_list(config)["files"] == 1
    assert not config.dest_path.exists()


@pytest.mark.parametrize(
    "suffix,excluded",
    [("selected", "selected"), ("parent/selected", "parent"), ("parent/selected", "*/selected")],
)
def test_excluded_selected_roots_preserve_local_files(tmp_path, monkeypatch, suffix, excluded):
    with local_archive(monkeypatch) as state:
        state.files[suffix + "/new.txt"] = b"remote"
        destination = tmp_path / "dest"
        saved = destination / suffix / "old.txt"
        saved.parent.mkdir(parents=True)
        saved.write_bytes(b"local")
        config = MirrorConfig(
            base_url=state.url,
            dest_path=destination,
            log_path=tmp_path / "logs",
            dir_suffix=suffix,
            exclude_dirs=[excluded],
            cleanup_policy=CleanupPolicy.DELETE,
            security_validation=False,
            async_metadata=False,
            parallel_downloads=False,
            streaming_parallel=False,
            request_delay=0,
            requests_per_second=0,
        )
        with MirrorURL(config) as mirror:
            assert mirror.sync()
        assert saved.read_bytes() == b"local"
        assert not saved.with_name("new.txt").exists()


def test_progress_updates_shared_active_files_under_lock():
    progress = DownloadProgress(True)
    failures = []

    class CheckedDict(dict):
        def __setitem__(self, key, value):
            assert progress.lock.locked(), "Active-file insertion raced with progress iteration"
            super().__setitem__(key, value)

    progress.active = CheckedDict()

    def writer():
        try:
            for number in range(100):
                path = Path(str(number))
                progress.begin(path)
                progress.finish(path, True)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=writer)
    thread.start()
    while thread.is_alive():
        progress.log_progress()
        thread.join(0.001)
    assert not failures
    assert not progress.active


@pytest.mark.parametrize(
    "filename", ["NUL.backup.txt", "con.a.b", "PRN .txt", "COM¹.log", "LPT².backup.tar"]
)
def test_windows_device_names_are_not_preserved_as_payload_names(filename):
    assert is_reserved_windows_filename(filename)
    assert PathSafety._safe_filename(filename) != filename
    assert not is_reserved_windows_filename("_" + filename)


def test_disabled_verification_does_not_hash_old_download(tmp_path, monkeypatch, origin):
    import mirror_url.transfers as transfers

    config = job(tmp_path, origin, url_list=None, download_files=["a"])
    assert download_url_list(config)["success"]
    config.verify_content = False
    original = transfers.file_sha256

    def hash_file(path, enabled=True):
        assert not enabled, "Disabled content verification read an old receipt's entire file"
        return original(path, enabled)

    monkeypatch.setattr(transfers, "file_sha256", hash_file)
    assert download_url_list(config)["success"]


@pytest.mark.parametrize("blocked_name", ["", "blocked"])
def test_incomplete_local_inspection_aborts_all_cleanup(tmp_path, monkeypatch, blocked_name):
    target = tmp_path / "dest"
    (target / "blocked").mkdir(parents=True)
    saved = target / "old.txt"
    saved.write_bytes(b"preserve")
    hidden = target / "blocked" / "hidden.txt"
    hidden.write_bytes(b"preserve too")
    mirror = CleanupMixin()
    mirror.config = MirrorConfig(
        base_url="https://files.example/",
        dest_path=target,
        log_path=tmp_path / "logs",
        cleanup_policy=CleanupPolicy.DELETE,
    )
    mirror.target_dir = target
    mirror.target_parsed = urlparse(mirror.config.base_url)
    mirror.metrics = MetricsCollector()
    mirror.cache_manager = SimpleNamespace(cleanup_stale_metadata=lambda _: 0)
    mirror._get_prefix = lambda: ""
    import os

    scandir = os.scandir

    def inspect(path):
        if Path(path) == target / blocked_name:
            raise PermissionError("Cannot inspect destination")
        return scandir(path)

    monkeypatch.setattr("mirror_url._core.cleanup.os.scandir", inspect)
    mirror.clean_obsolete(set())
    assert saved.read_bytes() == b"preserve"
    assert hidden.read_bytes() == b"preserve too"
    assert mirror.metrics.get_summary()["cleanup_failed_operations"] == 1


@pytest.mark.parametrize("dry_run", [False, True])
def test_download_metrics_and_health_lifecycle(tmp_path, monkeypatch, origin, dry_run):
    from mirror_url.health import DownloadHealthChecker, HealthCheckServer

    exported = tmp_path / "metrics.json"
    starts, stops = [], []
    monkeypatch.setattr(
        HealthCheckServer, "start", lambda server: starts.append(server.mirror_instance)
    )
    monkeypatch.setattr(
        HealthCheckServer, "stop", lambda server: stops.append(server.mirror_instance)
    )
    config = job(tmp_path, origin, metrics_json=exported, dry_run=dry_run)
    assert download_url_list(config)["success"]
    if dry_run:
        assert not exported.exists()
        assert not starts and not stops
    else:
        assert len(starts) == 1 and stops == starts
        assert isinstance(starts[0], DownloadHealthChecker)
        assert starts[0].get_status().status == "healthy"
        metrics = json.loads(exported.read_text())["metrics"]
        assert metrics["files_downloaded"] == 1
        assert metrics["bytes_downloaded"] == 5
        assert metrics["run"]["status"] == "success"


def test_download_cli_exports_completed_job_metrics(tmp_path, monkeypatch, origin):
    exported = tmp_path / "metrics.json"
    monkeypatch.setattr("mirror_url.health.HealthCheckServer.start", lambda _: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--mode",
            "download",
            origin.base + "a",
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
            "--metrics-json",
            str(exported),
            "--request-delay",
            "0",
            "--requests-per-second",
            "0",
        ],
    )
    cli.main()
    metrics = json.loads(exported.read_text())["metrics"]
    assert metrics["files_downloaded"] == 1 and metrics["bytes_downloaded"] == 5
    assert metrics["run"]["status"] == "success"
    assert metrics["run"]["duration_scope"] == "job"


@pytest.mark.parametrize("dry_run", [False, True])
def test_sync_fails_when_local_cleanup_inspection_fails(tmp_path, monkeypatch, dry_run):
    import os

    target = tmp_path / "dest"
    target.mkdir()
    saved = target / "old.txt"
    saved.write_bytes(b"preserve")
    with local_archive(monkeypatch) as state:
        config = MirrorConfig(
            base_url=state.url,
            dest_path=target,
            log_path=tmp_path / "logs",
            cleanup_policy=CleanupPolicy.DELETE,
            dry_run=dry_run,
            security_validation=False,
            async_metadata=False,
            request_delay=0,
            requests_per_second=0,
        )
        with MirrorURL(config) as mirror:
            original_scan = mirror._scan_local_tree
            original_scandir = os.scandir

            def inspect(path):
                if Path(path) == target:
                    raise PermissionError("Cannot list cleanup root")
                return original_scandir(path)

            def blocked_scan():
                with monkeypatch.context() as patch:
                    patch.setattr("mirror_url._core.cleanup.os.scandir", inspect)
                    return original_scan()

            monkeypatch.setattr(mirror, "_scan_local_tree", blocked_scan)
            assert not mirror.sync()
            assert mirror.metrics.get_summary()["cleanup_failed_operations"] == 1
    assert saved.read_bytes() == b"preserve"


@pytest.mark.parametrize("output", ["a", "a/metrics.json", "folder"])
def test_metrics_output_cannot_overwrite_a_download(tmp_path, monkeypatch, origin, output):
    monkeypatch.setattr("mirror_url.health.HealthCheckServer.start", lambda _: None)
    paths = ("folder/a",) if output == "folder" else ("a",)
    config = job(tmp_path, origin, paths=paths)
    config.metrics_json = config.dest_path / output
    with pytest.raises((ConfigError, ValueError), match="[Mm]etrics|same local filename"):
        download_url_list(config)
    assert not config.metrics_json.exists()
    assert not origin.requests


def test_health_server_stops_and_failed_metrics_export_on_preparation_error(
    tmp_path, monkeypatch, origin
):
    from mirror_url.health import HealthCheckServer

    starts, stops = [], []
    monkeypatch.setattr(HealthCheckServer, "start", lambda server: starts.append(server))
    monkeypatch.setattr(HealthCheckServer, "stop", lambda server: stops.append(server))
    exported = tmp_path / "metrics.json"
    config = job(tmp_path, origin, metrics_json=exported)
    config.dest_path.mkdir()
    saved = config.dest_path / "a"
    saved.write_bytes(b"unowned")
    with pytest.raises(ValueError, match="ownership receipt"):
        download_url_list(config)
    assert len(starts) == 1 and starts == stops
    assert saved.read_bytes() == b"unowned"
    assert json.loads(exported.read_text())["metrics"]["run"]["status"] == "failed"
