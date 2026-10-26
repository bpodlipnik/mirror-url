"""Preserve reports and mirrored data across export and repeated-run failures."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.enums import CleanupPolicy
from mirror_url.metrics import MetricsCollector
from test_http_mirror_workflows import local_archive


def configuration(tmp_path, base_url="https://files.example/"):
    return MirrorConfig(
        base_url=base_url,
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        security_validation=False,
        async_metadata=False,
        sequential_downloads=True,
        request_delay=0,
        requests_per_second=0,
    )


@pytest.mark.parametrize("existing", [False, True])
def test_mirror_metrics_cannot_replace_a_payload(tmp_path, monkeypatch, existing):
    with local_archive(monkeypatch) as state:
        state.files["a.txt"] = b"remote payload"
        config = configuration(tmp_path, state.url)
        config.metrics_json = config.dest_path / "a.txt"
        if existing:
            config.dest_path.mkdir()
            config.metrics_json.write_bytes(b"existing payload")
        with MirrorURL(config) as mirror:
            assert not mirror.sync()
        assert not any(method == "GET" and path == "a.txt" for method, path, _ in state.requests)
        if existing:
            assert config.metrics_json.read_bytes() == b"existing payload"
        else:
            assert not config.metrics_json.exists()


@pytest.mark.parametrize("fault", ["serialization", "replace"])
def test_failed_export_preserves_the_previous_complete_report(tmp_path, monkeypatch, fault):
    metrics, config = MetricsCollector(), configuration(tmp_path)
    output = tmp_path / "report.json"
    assert metrics.export_json(output, config)
    before = output.read_bytes()
    if fault == "serialization":
        metrics.metrics["unserializable"] = object()
    else:

        def refuse(*args):
            raise OSError("Cannot publish metrics")

        monkeypatch.setattr("mirror_url.metrics.os.replace", refuse)
    assert not metrics.export_json(output, config)
    assert output.read_bytes() == before
    assert not list(tmp_path.glob(".mirror-url-metrics-*"))


@pytest.mark.parametrize("kind", ["foreign", "symlink", "hardlink", "parent_symlink"])
def test_export_preserves_unrecognized_or_linked_files(tmp_path, kind):
    metrics, config = MetricsCollector(), configuration(tmp_path)
    original = tmp_path / "original"
    original.write_bytes(b"unrelated bytes")
    output = tmp_path / "report.json"
    if kind == "foreign":
        output.write_bytes(b"unrelated bytes")
    elif kind == "symlink":
        output.symlink_to(original)
    elif kind == "hardlink":
        os.link(original, output)
    else:
        parent = tmp_path / "linked"
        parent.symlink_to(tmp_path, target_is_directory=True)
        output = parent / "original"
    assert not metrics.export_json(output, config)
    assert original.read_bytes() == b"unrelated bytes"
    assert output.read_bytes() == b"unrelated bytes"


@pytest.mark.parametrize("dry_run", [False, True])
def test_previous_cleanup_failure_does_not_fail_a_later_safe_sync(tmp_path, monkeypatch, dry_run):
    with local_archive(monkeypatch) as state:
        config = configuration(tmp_path, state.url)
        config.cleanup_policy = CleanupPolicy.DELETE
        config.dry_run = dry_run
        config.dest_path.mkdir()
        old = config.dest_path / "old.txt"
        old.write_bytes(b"preserve")
        with MirrorURL(config) as mirror:
            real_scan, real_scandir = mirror._scan_local_tree, os.scandir

            def inspect(path):
                if Path(path) == config.dest_path:
                    raise PermissionError("Cannot inspect cleanup root")
                return real_scandir(path)

            def blocked_scan():
                with monkeypatch.context() as patch:
                    patch.setattr("mirror_url._core.cleanup.os.scandir", inspect)
                    return real_scan()

            with monkeypatch.context() as patch:
                patch.setattr(mirror, "_scan_local_tree", blocked_scan)
                assert not mirror.sync()
            config.cleanup_policy = CleanupPolicy.SAFE_NO_DELETE
            assert mirror.sync()
            assert mirror.metrics.get_summary()["cleanup_failed_operations"] == 0
        assert old.read_bytes() == b"preserve"


def test_repeated_export_produces_a_complete_updated_report(tmp_path):
    metrics, config = MetricsCollector(), configuration(tmp_path)
    output = tmp_path / "report.json"
    assert metrics.export_json(output, config)
    metrics.increment("files_downloaded")
    assert metrics.export_json(output, config)
    assert json.loads(output.read_text())["metrics"]["files_downloaded"] == 1


@pytest.mark.skipif(not hasattr(os, "fchmod"), reason="requires POSIX descriptor permissions")
def test_replacing_a_report_preserves_its_existing_permissions(tmp_path):
    output = tmp_path / "report.json"
    metrics, config = MetricsCollector(), configuration(tmp_path)
    assert metrics.export_json(output, config)
    output.chmod(0o640)
    assert metrics.export_json(output, config)
    assert output.stat().st_mode & 0o777 == 0o640


@pytest.mark.parametrize(
    "policy", [CleanupPolicy.DELETE, CleanupPolicy.MOVE, CleanupPolicy.PREVIEW]
)
@pytest.mark.parametrize("linked", [False, True])
def test_cleanup_preserves_configured_metrics_and_its_aliases(
    tmp_path, monkeypatch, policy, linked
):
    with local_archive(monkeypatch) as state:
        config = configuration(tmp_path, state.url)
        config.cleanup_policy = policy
        config.metrics_json = config.dest_path / "reports" / "report.json"
        config.metrics_json.parent.mkdir(parents=True)
        config.metrics_json.write_bytes(b"keep this unrecognized report")
        old = config.dest_path / "old.txt"
        old.write_bytes(b"obsolete")
        alias = config.dest_path / "report-alias"
        if linked:
            os.link(config.metrics_json, alias)
        with MirrorURL(config) as mirror:
            assert mirror.sync()
            key = {
                CleanupPolicy.DELETE: "files_deleted",
                CleanupPolicy.MOVE: "files_moved",
                CleanupPolicy.PREVIEW: "files_would_delete",
            }[policy]
            assert mirror.metrics.get_summary()[key] == 1
        assert config.metrics_json.read_bytes() == b"keep this unrecognized report"
        if linked:
            assert alias.read_bytes() == b"keep this unrecognized report"


def test_report_changed_during_export_is_preserved(tmp_path, monkeypatch):
    metrics, config = MetricsCollector(), configuration(tmp_path)
    output = tmp_path / "report.json"
    assert metrics.export_json(output, config)
    before = output.read_bytes()
    saved = tmp_path / "saved.json"
    fsync = os.fsync

    def changed(descriptor):
        fsync(descriptor)
        output.rename(saved)
        output.write_bytes(b"concurrent replacement")

    monkeypatch.setattr("mirror_url.metrics.os.fsync", changed)
    assert not metrics.export_json(output, config)
    assert output.read_bytes() == b"concurrent replacement"
    assert saved.read_bytes() == before
    assert not list(tmp_path.glob(".mirror-url-metrics-*"))


@pytest.mark.parametrize(
    "data", [{}, [], {"timestamp": "x", "version": "1", "metrics": [], "config": {}}]
)
def test_valid_json_without_report_shape_is_preserved(tmp_path, data):
    output = tmp_path / "report.json"
    output.write_text(json.dumps(data), encoding="utf-8")
    before = output.read_bytes()
    assert not MetricsCollector().export_json(output, configuration(tmp_path))
    assert output.read_bytes() == before
