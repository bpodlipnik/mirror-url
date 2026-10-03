"""Check that selected preservation tests reject deliberately weakened guards.

Runs in temporary source copies; never edits the checkout. This is a small,
explicit set of safety mutations, not a claim of exhaustive mutation coverage.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

MUTATIONS = (
    (
        "mixed DNS",
        "security.py",
        "if private_ips:",
        "if False:",
        "tests/test_safety_security.py::test_every_dns_answer_must_be_public",
    ),
    (
        "archive collision",
        "_core/cleanup.py",
        "if dest.exists() or dest.is_symlink():",
        "if False:",
        "tests/test_safety_cleanup.py::test_timestamp_collision_preserves_both_archives",
    ),
    (
        "incomplete scan",
        "_core/cleanup.py",
        'if getattr(self, "scan_incomplete", False):',
        "if False:",
        "tests/test_cleanup_partial_scan.py::test_clean_obsolete_skips_everything_when_scan_incomplete",
    ),
    (
        "origin boundary",
        "utils.py",
        "or candidate.netloc.lower() != scope.netloc.lower()",
        "or False",
        "tests/test_safety_properties.py::test_origin_and_directory_boundaries_cannot_be_prefix_matches",
    ),
    (
        "address pinning",
        "transport.py",
        "request.url.copy_with(host=safe_ip)",
        "request.url",
        "tests/test_safety_transport.py::test_sync_pins_address_preserves_host_sni_stream_and_cache",
    ),
    (
        "lossy filename",
        "_core/scan.py",
        "if lossy_mapping:",
        "if False:",
        "tests/test_http_mirror_workflows.py::test_lossy_filename_cannot_overwrite_an_unrelated_local_file",
    ),
)


def run(root: Path, source: Path, test: str, scratch: Path) -> subprocess.CompletedProcess:
    scratch.mkdir(parents=True)
    env = dict(os.environ, PYTHONPATH=str(source / "src"), PYTHONDONTWRITEBYTECODE="1")
    # Ignore caller coverage flags so mutation subprocesses cannot overwrite
    # the release coverage report or import the unmodified installed package.
    env.pop("PYTEST_ADDOPTS", None)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-o",
            "addopts=",
            "-o",
            "cache_dir=" + str(scratch / "cache"),
            "--basetemp=" + str(scratch / "temp"),
            test,
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    complete = True
    with tempfile.TemporaryDirectory(prefix="mirror-safety-mutations-") as temporary:
        scratch = Path(temporary)
        source = scratch / "source"
        shutil.copytree(
            root / "src", source / "src", ignore=shutil.ignore_patterns("__pycache__", "*.egg-info")
        )
        for index, (name, module, old, new, test) in enumerate(MUTATIONS):
            baseline = run(root, source, test, scratch / f"baseline-{index}")
            if baseline.returncode != 0:
                print(f"{name}: baseline failed\n{baseline.stdout}\n{baseline.stderr}")
                return 1
            path = source / "src" / "mirror_url" / module
            original = path.read_text()
            if old not in original:
                print(f"{name}: mutation no longer matches source")
                return 1
            path.write_text(original.replace(old, new, 1))
            try:
                mutant = run(root, source, test, scratch / f"mutant-{index}")
            finally:
                path.write_text(original)
            # Pytest status 1 means test assertions failed. Collection errors,
            # missing dependencies and timeouts do not count as a killed mutant.
            killed = mutant.returncode == 1 and "FAILED" in mutant.stdout
            print(
                f"{name}: {'PASS (mutation rejected)' if killed else 'FAIL (mutation survived or test error)'}"
            )
            if not killed:
                print(mutant.stdout + mutant.stderr)
            complete = complete and killed
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
