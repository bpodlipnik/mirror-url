"""Final CLI totals count file outcomes across independently owned directory jobs."""

from __future__ import annotations

import logging
import sys

import pytest

from mirror_url import cli
from test_directory_selection import directory_archive as directory_archive
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote

pytestmark = pytest.mark.integration


@pytest.fixture
def run_cli(config, directory_archive, monkeypatch, capsys):
    config.base_url = directory_archive.url
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level

    def run(suffixes, *, backend="httpx", dry_run=False, shared=False, from_config=False):
        args = ["mirror-url"]
        if from_config:
            path = config.log_path.parent / "config.json"
            path.write_text(config.model_dump_json(), encoding="utf-8")
            args += ["--config", str(path)]
        else:
            args += [
                "--url",
                str(config.base_url),
                "--dest-path",
                str(config.dest_path),
                "--log-path",
                str(config.log_path),
            ]
        args += [
            "--dir",
            *suffixes,
            "--backend",
            backend,
            "--filter",
            ".fts",
            ".txt",
            "--missing-files",
            "--print-logs",
            "--sequential-downloads",
            "--no-async-metadata",
            "--no-connection-pool-prewarm",
            "--request-delay",
            "0",
            "--requests-per-second",
            "0",
            "--max-retries",
            "0",
        ]
        if dry_run:
            args += ["--dry-run"]
        if shared:
            args += ["--log-file", "aggregate"]
        monkeypatch.setattr(sys, "argv", args)
        with pytest.raises(SystemExit) as stopped:
            cli.main()
        output = capsys.readouterr().err
        assert output.count("FINAL SUMMARY") == 1
        final = output.split("FINAL SUMMARY", 1)[1]
        return stopped.value.code, final

    yield run
    root.handlers[:] = handlers
    root.setLevel(level)


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("from_config", [False, True])
def test_dry_run_aggregates_the_reported_year_boundary_example(
    remote, config, run_cli, backend, from_config
):
    expected = {
        "991230": (239, 119),
        "991231": (248, 122),
        "000101": (237, 237),
        "000102": (248, 248),
    }
    for suffix, (found, existing) in expected.items():
        for index in range(found):
            name = f"{suffix}/c4/{index:03d}.fts"
            remote.files[name] = b"remote payload"
            if index < existing:
                path = config.dest_path / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"existing payload")
        remote.files[f"{suffix}/ignored.bin"] = b"outside the file filter"
    before = {
        p.relative_to(config.dest_path): p.read_bytes()
        for p in config.dest_path.rglob("*")
        if p.is_file()
    }
    config.dry_run = from_config
    code, final = run_cli(
        list(expected),
        backend=backend,
        dry_run=not from_config,
        shared=from_config,
        from_config=from_config,
    )
    assert code == 0
    assert "TOTAL FILES FOUND: 972" in final
    assert "WOULD DOWNLOAD: 246" in final
    assert "SKIPPED: 726" in final
    assert "FILES FAILED: 0" in final
    assert "DOWNLOADED:" not in final
    assert "SUCCESSFUL DIRECTORIES (4)" in final
    assert "FAILED DIRECTORIES: (none)" in final
    assert "SKIPPED DIRECTORIES: (none)" in final
    assert "⏭️ SKIPPED: (none)" not in final
    assert all(suffix in final for suffix in expected)
    assert before == {
        p.relative_to(config.dest_path): p.read_bytes()
        for p in config.dest_path.rglob("*")
        if p.is_file()
    }
    assert not any(method == "GET" and path in remote.files for method, path, _ in remote.requests)


@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("unavailable", [None, "file-failed", "file-gone", "directory"])
def test_real_download_totals_include_partial_failures_and_http_skips(
    remote, config, run_cli, backend, unavailable
):
    for suffix in ("a", "b"):
        remote.files.update(
            {f"{suffix}/{name}.fts": name.encode() for name in ("existing", "one", "two")}
        )
        path = config.dest_path / suffix / "existing.fts"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"keep this existing file")
    if unavailable == "file-failed":
        remote.failures["b/two.fts"] = 503
    elif unavailable == "file-gone":
        remote.failures["b/two.fts"] = 404
    elif unavailable == "directory":
        remote.failures["b/"] = 404
    code, final = run_cli(["a", "b"], backend=backend)
    # Keep each backend's existing classification: httpx treats a vanished
    # resource as a skip; aiohttp reports the unsuccessful GET as a failure.
    http_skip = unavailable == "file-gone" and backend == "httpx"
    transfer_failure = unavailable == "file-failed" or (
        unavailable == "file-gone" and backend == "aiohttp"
    )
    assert code == int(transfer_failure or unavailable == "directory")
    assert "TOTAL FILES FOUND: " + ("3" if unavailable == "directory" else "6") in final
    published = 4 if unavailable is None else 2 if unavailable == "directory" else 3
    skipped = 1 if unavailable == "directory" else 3 if http_skip else 2
    failed = int(transfer_failure)
    assert f"DOWNLOADED: {published}" in final
    assert f"SKIPPED: {skipped}" in final
    assert f"FILES FAILED: {failed}" in final
    assert "WOULD DOWNLOAD:" not in final
    assert f"SUCCESSFUL DIRECTORIES ({2 - int(bool(code))})" in final
    assert ("FAILED DIRECTORIES (1)" in final) is bool(code)
    assert "SKIPPED DIRECTORIES: (none)" in final
    actual = [p for p in config.dest_path.rglob("*.fts") if p.name != "existing.fts"]
    assert len(actual) == published
    for suffix in ("a", "b"):
        assert (
            config.dest_path / suffix / "existing.fts"
        ).read_bytes() == b"keep this existing file"


@pytest.mark.parametrize("dry_run", [False, True])
def test_shared_log_reports_zero_file_totals_for_an_empty_directory(remote, run_cli, dry_run):
    remote.listings["empty/"] = "<html><body></body></html>"
    code, final = run_cli(["empty"], shared=True, dry_run=dry_run)
    assert code == 0
    assert "TOTAL FILES FOUND: 0" in final
    assert ("WOULD DOWNLOAD: 0" if dry_run else "DOWNLOADED: 0") in final
    assert "SKIPPED: 0" in final
    assert "FILES FAILED: 0" in final
    assert "SUCCESSFUL DIRECTORIES (1)" in final
