#!/usr/bin/env python3
"""Compare installed MirrorURL CLI modes against the same flat HTTP directory.

Uses only Python's standard library. See scripts/benchmark_modes.md for examples.
No downloads are made with --dry-run. Each real run retains a new destination,
its command, console, cache manifests and results; existing paths are never reused.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

DEFAULT_URL = "https://star.mps.mpg.de/data/sami-2021-10-31/"
DEFAULT_FILES = [
    "Mars Science Laboratory Curiosity Rover Animation.mp4",
    "Solar Probe Touches the Sun.mp4",
    "What Did Voyager 1 See During its Journey Out Of The Solar System 1977-2019 (4k UHD).mp4",
]
TIMES = {
    "total_seconds": "Total duration",
    "download_seconds": "Downloads time",
    "discovery_seconds": "Discovery elapsed time",
    "metadata_seconds": "Metadata time",
    "preparation_seconds": "Download preparation time",
    "cleanup_seconds": "Cleanup time",
    "transfer_metadata_sum_seconds": "Transfer metadata probes (sum)",
    "selection_sum_seconds": "Automatic selection work (sum)",
    "transfer_sum_seconds": "Data transfer/write (sum)",
    "assembly_sum_seconds": "Chunk assembly (sum)",
    "flush_sum_seconds": "Disk flushing (sum)",
    "hash_sum_seconds": "SHA-256 hashing (sum)",
}


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def cache_manifest(logs):
    return {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(logs.glob("mirror_url_*.json"))
    }


def schedule(pairs, backends, caches):
    """Reverse both mode and backend order between successive pairs."""
    result = []
    for pair in range(pairs):
        ordered_backends = backends if pair % 2 == 0 else list(reversed(backends))
        ordered_caches = caches if pair % 2 == 0 else list(reversed(caches))
        for cache in ordered_caches:
            for backend in ordered_backends:
                modes = ["download", "mirror"]
                if (pair + backends.index(backend) + caches.index(cache)) % 2:
                    modes.reverse()
                for mode in modes:
                    result.append(
                        {"pair": pair + 1, "backend": backend, "cache": cache, "mode": mode}
                    )
    return result


def parse_summary(text):
    # Every emitted field has its own logger prefix. Keep the original text
    # for warning/error detection, and parse the actual message bodies.
    messages = re.sub(r"^.*?\[INFO\] ?", "", text, flags=re.M)
    if len(re.findall(r"^SUMMARY:\s*$", messages, flags=re.M)) != 1:
        raise ValueError("Expected exactly one completed shared SUMMARY")
    summary = re.split(r"^SUMMARY:\s*$", messages, maxsplit=1, flags=re.M)[1]
    fields = dict(re.findall(r"^  ([^:\n]+): (.+)$", summary, re.M))
    if fields.get("Result") != "SUCCESS" or fields.get("Files failed") != "0":
        raise ValueError("Run did not report SUCCESS with zero failed files")
    if "Sync completed successfully" not in summary:
        raise ValueError("Missing final successful completion")
    if re.search(r"\[(ERROR|CRITICAL)\]|Traceback", text):
        raise ValueError("An error was recorded even though the process exited successfully")
    values = {}
    for key, label in TIMES.items():
        value = fields.get(label)
        if value == "not performed":
            values[key] = None
        elif value is not None and re.fullmatch(r"\d+\.\d+s", value):
            values[key] = float(value[:-1])
        else:
            raise ValueError(f"Missing or invalid timing: {label}: {value!r}")
    if values["total_seconds"] <= 0 or values["download_seconds"] <= 0:
        raise ValueError("Timings rounded to zero; use larger benchmark files")
    rate = re.fullmatch(
        r"(\d+\.\d+) (B|KiB|MiB|GiB|TiB|PiB)/s", fields.get("Download throughput", "")
    )
    if not rate:
        raise ValueError("Missing IEC download throughput")
    values["mib_per_second"] = float(rate[1]) * 1024 ** (
        ["B", "KiB", "MiB", "GiB", "TiB", "PiB"].index(rate[2]) - 2
    )
    for key, label in (
        ("files_missing", "Missing files downloaded"),
        ("chunks", "Total chunks"),
        ("peak_chunks", "Peak parallel chunks"),
        ("signatures_loaded", "Directory signatures loaded"),
    ):
        # Sequential mode does not print chunk counters.
        if label not in fields and key in ("chunks", "peak_chunks"):
            values[key] = 0
        else:
            values[key] = int(fields[label])
    values["files_downloaded"] = int(
        fields.get(
            "Files downloaded",
            sum(
                int(fields.get(label, 0))
                for label in (
                    "Missing files downloaded",
                    "Changed files downloaded",
                    "Uncertain-freshness downloads",
                    "Other files downloaded",
                )
            ),
        )
    )
    values["warnings"] = [line for line in text.splitlines() if "[WARNING]" in line]
    return values, fields


def payload_sizes(destination, names):
    expected = set(names)
    actual = set()
    for path in destination.rglob("*"):
        relative = path.relative_to(destination)
        if relative.parts[0] == ".mirror-url-state":
            continue
        if path.is_symlink():
            raise ValueError(f"Unexpected payload symlink: {relative}")
        if path.is_file():
            actual.add(relative.as_posix())
    if actual != expected:
        raise ValueError(
            f"Payload set differs: missing={expected - actual}, extra={actual - expected}"
        )
    sizes = {name: (destination / name).stat().st_size for name in names}
    if not all(sizes.values()):
        raise ValueError("An expected file is empty")
    return sizes


def execute(command, console, timeout):
    """Stop and reap only the subprocess created here, preserving its evidence."""
    started = time.perf_counter()
    with console.open("wb") as output:
        child = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT)
        try:
            code = child.wait(timeout=timeout)
        except (KeyboardInterrupt, subprocess.TimeoutExpired):
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            raise
    return code, time.perf_counter() - started


def command_for(args, case, run_dir, url_list):
    command = [
        args.executable,
        "--mode",
        case["mode"],
        "--backend",
        case["backend"],
        "--url",
        args.url,
        "--dest-path",
        str(run_dir / "files"),
        "--log-path",
        str(run_dir / "logs"),
        "--log-file",
        "benchmark",
        "--print-logs",
        "--metrics",
        "--" + args.strategy,
        "--cleanup",
        "safe",
        "--max-depth",
        "1",
        "--verify-content" if args.verify_content else "--no-verify-content",
    ]
    if case["mode"] == "download":
        command += ["--url-list", str(url_list)]
    else:
        command += ["--filter", "^(?:" + "|".join(re.escape(n) for n in args.files) + r")\Z"]
    return command


def one_run(args, root, name, case, seeds, baseline):
    directory = root / name
    directory.mkdir(parents=True, exist_ok=False)
    (directory / "files").mkdir()
    logs = directory / "logs"
    logs.mkdir()
    if case["cache"] == "reused" and case["mode"] == "mirror":
        for source in seeds[case["backend"]].glob("mirror_url_*.json"):
            shutil.copyfile(source, logs / source.name)
    before = cache_manifest(logs)
    write_json(directory / "cache-before.json", before)
    command = command_for(args, case, directory, root / "files.txt")
    record = dict(case, name=name, status="running", command=command, directory=str(directory))
    write_json(directory / "result.json", record)
    print(f"{name}: {case['backend']} {case['mode']} ({case['cache']} cache)", flush=True)
    try:
        code, wall = execute(command, directory / "console.log", args.run_timeout)
        record.update(returncode=code, process_wall_seconds=wall)
        if code:
            raise ValueError(f"MirrorURL exited with status {code}")
        values, fields = parse_summary((directory / "console.log").read_text(encoding="utf-8"))
        record.update(values, summary=fields)
        sizes = payload_sizes(directory / "files", args.files)
        record["file_sizes"] = sizes
        if baseline is not None and baseline != sizes:
            raise ValueError("File sizes changed between runs; this comparison is invalid")
        if record["files_downloaded"] != len(args.files) or record["files_missing"] != len(
            args.files
        ):
            raise ValueError("Not all selected files were downloaded into the fresh destination")
        if case["mode"] == "mirror":
            if case["cache"] == "reused" and (not before or not record["signatures_loaded"]):
                raise ValueError("The reused-cache run did not load persisted directory signatures")
            if case["cache"] == "fresh" and (before or record["signatures_loaded"]):
                raise ValueError("The fresh-cache run unexpectedly loaded a cache")
        record.update(status="success", payload_bytes=sum(sizes.values()), cache_before=before)
        print(
            f"  total {record['total_seconds']:.3f}s; downloads {record['download_seconds']:.3f}s; "
            f"{record['mib_per_second']:.2f} MiB/s; {len(record['warnings'])} warnings",
            flush=True,
        )
    except (Exception, KeyboardInterrupt) as error:
        record.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        record["cache_after"] = cache_manifest(logs)
        write_json(directory / "result.json", record)
    return record


def save_results(root, rows):
    write_json(root / "results.json", rows)
    if not rows:
        return
    columns = [
        "name",
        "pair",
        "backend",
        "mode",
        "cache",
        "process_wall_seconds",
        *TIMES,
        "mib_per_second",
        "payload_bytes",
        "chunks",
        "peak_chunks",
        "signatures_loaded",
    ]
    with (root / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    groups = []
    for backend, mode, cache in sorted({(r["backend"], r["mode"], r["cache"]) for r in rows}):
        matching = [
            r for r in rows if (r["backend"], r["mode"], r["cache"]) == (backend, mode, cache)
        ]
        group = {"backend": backend, "mode": mode, "cache": cache, "runs": len(matching)}
        for key in ("total_seconds", "download_seconds", "process_wall_seconds", "mib_per_second"):
            values = [r[key] for r in matching]
            group[key] = {
                "median": statistics.median(values),
                "minimum": min(values),
                "maximum": max(values),
            }
        groups.append(group)
    write_json(root / "summary.json", groups)
    comparisons = []
    for backend, cache in sorted({(r["backend"], r["cache"]) for r in rows}):
        pairs = {}
        for row in rows:
            if (row["backend"], row["cache"]) == (backend, cache):
                pairs.setdefault(row["pair"], {})[row["mode"]] = row
        matched = [p for p in pairs.values() if set(p) == {"mirror", "download"}]
        if matched:
            comparisons.append(
                {
                    "backend": backend,
                    "cache": cache,
                    "complete_pairs": len(matched),
                    "median_download_to_mirror_total_ratio": statistics.median(
                        p["download"]["total_seconds"] / p["mirror"]["total_seconds"]
                        for p in matched
                    ),
                    "median_download_minus_mirror_seconds": statistics.median(
                        p["download"]["total_seconds"] - p["mirror"]["total_seconds"]
                        for p in matched
                    ),
                }
            )
    write_json(root / "comparisons.json", comparisons)
    state = json.loads((root / "status.json").read_text(encoding="utf-8"))["state"]
    lines = [
        f"Experiment status: **{state}**; {len(rows)} successful measured runs.\n",
        "| Backend | Mode | Cache | Runs | Median total (s) | Median downloads (s) | Median MiB/s |",
        "|---|---|---|---:|---:|---:|---:|",
    ]
    for g in groups:
        lines.append(
            f"| {g['backend']} | {g['mode']} | {g['cache']} | {g['runs']} | "
            f"{g['total_seconds']['median']:.3f} | {g['download_seconds']['median']:.3f} | "
            f"{g['mib_per_second']['median']:.2f} |"
        )
    (root / "summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def arguments(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--executable", default="mirror-url", help="Installed mirror-url executable"
    )
    parser.add_argument(
        "--url", default=DEFAULT_URL, help="Remote root containing the selected files"
    )
    parser.add_argument(
        "--url-list",
        type=Path,
        help="UTF-8 list of literal filenames in this root (no subdirectories)",
    )
    parser.add_argument("--output", type=Path, help="New output directory; must not already exist")
    parser.add_argument(
        "--pairs", type=int, default=3, help="Pairs per backend/cache state (default: 3)"
    )
    parser.add_argument(
        "--backends", nargs="+", choices=["httpx", "aiohttp"], default=["httpx", "aiohttp"]
    )
    parser.add_argument(
        "--cache", nargs="+", choices=["fresh", "reused"], default=["fresh", "reused"]
    )
    parser.add_argument(
        "--strategy",
        choices=["streaming-parallel", "parallel-downloads", "sequential-downloads"],
        default="streaming-parallel",
    )
    parser.add_argument(
        "--verify-content",
        action="store_true",
        help="Enable SHA-256 in both modes (default: disabled)",
    )
    parser.add_argument(
        "--run-timeout", type=float, default=900, help="Maximum seconds per CLI invocation"
    )
    parser.add_argument(
        "--pause", type=float, default=1, help="Seconds between invocations (outside timings)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan without invoking MirrorURL or creating files",
    )
    args = parser.parse_args(argv)
    if not 1 <= args.pairs <= 20:
        parser.error("--pairs must be between 1 and 20")
    if (
        not math.isfinite(args.pause)
        or args.pause < 0
        or not math.isfinite(args.run_timeout)
        or args.run_timeout <= 0
    ):
        parser.error(
            "--pause must be finite and nonnegative; --run-timeout must be finite and positive"
        )
    for values in (args.backends, args.cache):
        if len(values) != len(set(values)):
            parser.error("Backend and cache selections must not contain duplicates")
    parsed = urlsplit(args.url)
    if (
        parsed.scheme not in ("https", "http")
        or not parsed.hostname
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
    ):
        parser.error(
            "--url must be an HTTP(S) directory root without credentials, query or fragment"
        )
    args.url = args.url.rstrip("/") + "/"
    args.files = (
        args.url_list.read_text(encoding="utf-8-sig").splitlines()
        if args.url_list
        else DEFAULT_FILES
    )
    if not args.files or any(
        not n or n != n.strip() or n in (".", "..") or re.search(r"[/\\\x00-\x1f\x7f]", n)
        for n in args.files
    ):
        parser.error("The list must contain literal filenames only, one nonempty filename per line")
    if len({n.casefold() for n in args.files}) != len(args.files):
        parser.error("Filenames must be distinct, including when case is ignored")
    return args


def main(argv=None):
    args = arguments(argv)
    plan = schedule(args.pairs, args.backends, args.cache)
    warmups = len(args.backends) if "reused" in args.cache else 0
    print(f"{len(plan)} measured runs + {warmups} cache warmups; {len(args.files)} files per run.")
    if args.dry_run:
        print(json.dumps({"url": args.url, "files": args.files, "schedule": plan}, indent=2))
        return 0
    executable = shutil.which(str(Path(args.executable).expanduser()))
    if executable is None:
        raise ValueError("mirror-url not found; activate its environment or use --executable")
    args.executable = executable
    version = subprocess.run(
        [executable, "--version"], capture_output=True, text=True, check=True, timeout=30
    )
    match = re.fullmatch(r"mirror-url (\d+)\.(\d+)\.(\d+)\s*", version.stdout)
    if not match or tuple(map(int, match.groups())) < (3, 9, 9):
        raise ValueError("This script requires MirrorURL 3.9.9 or newer")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    root = (args.output or Path(f"mirror-url-benchmark-{stamp}")).expanduser().absolute()
    root.mkdir(parents=True, exist_ok=False)
    write_json(
        root / "plan.json",
        {
            "version": version.stdout.strip(),
            "executable": executable,
            "url": args.url,
            "files": args.files,
            "strategy": args.strategy,
            "verify_content": args.verify_content,
            "pairs": args.pairs,
            "schedule": plan,
            "output": str(root),
            "log_timing_resolution_seconds": 0.001,
            "cache_note": "Reused persisted directory signatures; HTML memory cache starts fresh in every process. OS and remote caches are not reset.",
        },
    )
    (root / "files.txt").write_text("\n".join(args.files) + "\n", encoding="utf-8")
    print(f"Evidence directory: {root}; CLI output is saved in each run's console.log", flush=True)
    rows, seeds, baseline = [], {}, None
    write_json(root / "status.json", {"state": "running"})
    try:
        if warmups:
            for backend in args.backends:
                case = {"pair": 0, "backend": backend, "mode": "mirror", "cache": "fresh"}
                row = one_run(args, root, f"warmup-{backend}", case, seeds, baseline)
                baseline = row["file_sizes"]
                seeds[backend] = Path(row["directory"]) / "logs"
                if not cache_manifest(seeds[backend]):
                    raise ValueError("Cache warmup produced no reusable cache")
                time.sleep(args.pause)
        for index, case in enumerate(plan, 1):
            row = one_run(args, root, f"run-{index:03d}", case, seeds, baseline)
            baseline = row["file_sizes"]
            rows.append(row)
            save_results(root, rows)
            if index < len(plan):
                time.sleep(args.pause)
        write_json(
            root / "status.json",
            {"state": "completed", "measured_runs": len(rows), "expected_runs": len(plan)},
        )
        save_results(root, rows)
    except (Exception, KeyboardInterrupt) as error:
        write_json(
            root / "status.json",
            {
                "state": "failed",
                "measured_runs": len(rows),
                "expected_runs": len(plan),
                "error": f"{type(error).__name__}: {error}",
            },
        )
        save_results(root, rows)
        print(f"Stopped; all evidence retained in {root}", file=sys.stderr)
        raise
    print((root / "summary.md").read_text(encoding="utf-8"))
    print(f"Completed. Downloads, logs and results retained in {root}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print(f"Benchmark error: {error}", file=sys.stderr)
        raise SystemExit(1) from None
