"""The benchmark must compare identical work and retain failed-run evidence."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from urllib.parse import quote

import pytest

from mirror_url import cli
from test_transfer_backends import origin as origin

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_modes.py"
spec = importlib.util.spec_from_file_location("benchmark_modes", SCRIPT)
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def test_schedule_alternates_mode_and_backend_order():
    plan = benchmark.schedule(2, ["httpx", "aiohttp"], ["fresh", "reused"])
    assert len(plan) == 16
    for backend in ("httpx", "aiohttp"):
        for cache in ("fresh", "reused"):
            orders = [
                [
                    r["mode"]
                    for r in plan
                    if r["pair"] == pair and r["backend"] == backend and r["cache"] == cache
                ]
                for pair in (1, 2)
            ]
            assert orders[0] == list(reversed(orders[1]))
    assert plan[0]["backend"] != plan[8]["backend"]


def test_dry_run_has_no_side_effects(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **k: pytest.fail("CLI invoked"))
    output = tmp_path / "untouched"
    assert benchmark.main(["--dry-run", "--output", str(output)]) == 0
    assert not output.exists()


@pytest.mark.parametrize(
    "entry",
    ["../file", "/file", "directory/file", "https://example.org/file", " leading", "", "a\x00b"],
)
def test_list_cannot_expand_scope(tmp_path, entry):
    listing = tmp_path / "list.txt"
    listing.write_text(entry + "\n", encoding="utf-8")
    with pytest.raises(SystemExit):
        benchmark.arguments(["--url-list", str(listing)])


def test_payload_validation_rejects_extra_or_missing_files(tmp_path):
    (tmp_path / "wanted").write_bytes(b"content")
    assert benchmark.payload_sizes(tmp_path, ["wanted"]) == {"wanted": 7}
    (tmp_path / "extra").write_bytes(b"x")
    with pytest.raises(ValueError, match="Payload set differs"):
        benchmark.payload_sizes(tmp_path, ["wanted"])


def test_summary_parser_reads_prefixed_fields_after_the_completed_header():
    lines = [
        "Downloaded [INFO] SUMMARY:",
        "SUMMARY:",
        "  Files downloaded: 3",
        "METRICS SUMMARY:",
        "  Result: SUCCESS",
        "  Files failed: 0",
        "  Download throughput: 1.00 MiB/s",
        "  Missing files downloaded: 3",
        "  Directory signatures loaded: 0",
        *["  " + label + ": 1.000s" for label in benchmark.TIMES.values()],
        "Sync completed successfully",
    ]
    console = "\n".join("[2026-10-26 12:00:00] [INFO] " + line for line in lines)
    values, fields = benchmark.parse_summary(console)
    assert values["files_downloaded"] == 3
    assert values["mib_per_second"] == 1.0
    assert fields["Result"] == "SUCCESS"


def test_timeout_reaps_the_owned_child(tmp_path):
    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        benchmark.execute(
            [sys.executable, "-c", "import time; time.sleep(20)"], tmp_path / "console.log", 0.1
        )
    assert time.monotonic() - started < 10


@pytest.mark.integration
def test_real_cli_comparison_preserves_scope_and_cache_states(tmp_path, monkeypatch, origin):
    # Use the real CLI and real fixture HTTP requests. Only the subprocess
    # boundary is replaced so the existing fixture's DNS guards stay scoped
    # to this test process; no private-IP bypass enters the benchmark script.
    def invoke(command, console, timeout):
        with console.open("w", encoding="utf-8") as output:
            with monkeypatch.context() as patch, redirect_stdout(output), redirect_stderr(output):
                patch.setattr(sys, "argv", command)
                started = time.perf_counter()
                try:
                    cli.main()
                except SystemExit as stopped:
                    return stopped.code, time.perf_counter() - started
                return 0, time.perf_counter() - started

    monkeypatch.setattr(benchmark, "execute", invoke)
    files = {
        "one (1).bin": b"a" * 1024**2,
        "two + 2.bin": b"b" * 1024**2,
        "three [3].bin": b"c" * 1024**2,
    }
    listing = tmp_path / "files.txt"
    listing.write_text("\n".join(files) + "\n", encoding="utf-8")
    output = tmp_path / "benchmark"
    origin.routes["/root/"] = (
        200,
        {"Content-Type": "text/html", "ETag": '"listing"'},
        (
            "<html><body>"
            + "".join(f'<a href="{quote(n)}">{n}</a>' for n in [*files, "extra.bin"])
            + "</body></html>"
        ).encode(),
    )
    for name, payload in files.items():
        origin.routes["/root/" + quote(name)] = (200, {"ETag": '"v1"'}, payload)
    origin.routes["/root/extra.bin"] = (200, {}, b"must not be downloaded")
    assert (
        benchmark.main(
            [
                "--executable",
                str(Path(sys.executable).parent / "mirror-url"),
                "--url",
                origin.base,
                "--url-list",
                str(listing),
                "--output",
                str(output),
                "--pairs",
                "1",
                "--pause",
                "0",
            ]
        )
        == 0
    )
    assert not any(
        method == "GET" and path == "/root/extra.bin" for method, path, _, _ in origin.requests
    )
    rows = json.loads((output / "results.json").read_text())
    assert len(rows) == 8
    assert json.loads((output / "status.json").read_text())["state"] == "completed"
    for row in rows:
        assert row["status"] == "success" and row["payload_bytes"] == 3 * 1024**2
        assert row["files_missing"] == row["files_downloaded"] == 3
        assert "--metrics-json" not in row["command"]
        assert "--metrics" in row["command"]
        assert "--no-verify-content" in row["command"]
        if row["mode"] == "mirror":
            assert bool(row["signatures_loaded"]) == (row["cache"] == "reused")
        for name, payload in files.items():
            assert (Path(row["directory"]) / "files" / name).read_bytes() == payload
    assert len(json.loads((output / "comparisons.json").read_text())) == 4
    assert len((output / "results.csv").read_text().splitlines()) == 9
    assert "Experiment status: **completed**" in (output / "summary.md").read_text()


def test_failed_run_stops_and_preserves_evidence(tmp_path, monkeypatch):
    monkeypatch.setattr(benchmark.shutil, "which", lambda _: "/unused/mirror-url")
    monkeypatch.setattr(
        benchmark.subprocess,
        "run",
        lambda *a, **k: subprocess.CompletedProcess(a, 0, stdout="mirror-url 3.9.10\n", stderr=""),
    )

    def fail(command, console, timeout):
        console.write_text("[ERROR] fixture failure\n", encoding="utf-8")
        return 1, 0.01

    monkeypatch.setattr(benchmark, "execute", fail)
    output = tmp_path / "failed"
    with pytest.raises(ValueError, match="exited with status 1"):
        benchmark.main(["--output", str(output), "--cache", "fresh", "--pairs", "1"])
    assert json.loads((output / "status.json").read_text())["state"] == "failed"
    assert json.loads((output / "results.json").read_text()) == []
    assert json.loads((output / "run-001/result.json").read_text())["status"] == "failed"
    assert "fixture failure" in (output / "run-001/console.log").read_text()
