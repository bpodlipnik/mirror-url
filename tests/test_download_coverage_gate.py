"""The per-module CI gate must not round away or average away coverage gaps."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "statement",
        "branch",
        "no-branches",
        "missing-module",
        "empty",
        "unreadable",
        "duplicate",
    ],
)
def test_coverage_gate_requires_both_modules_complete(tmp_path, fault):
    names = ("src/mirror_url/download.py", "src/mirror_url/download_integrity.py")
    report = {
        "meta": {"branch_coverage": True},
        "files": {
            name: {
                "summary": {
                    "num_statements": 1000,
                    "num_branches": 1000,
                    "missing_lines": 0,
                    "missing_branches": 0,
                    "percent_covered_display": "100",
                },
                "missing_lines": [],
                "missing_branches": [],
            }
            for name in names
        },
    }
    data = report["files"][names[1]]
    if fault == "statement":
        data["summary"]["missing_lines"] = 1
        data["missing_lines"] = [23]
    elif fault == "branch":
        data["summary"]["missing_branches"] = 1
        data["missing_branches"] = [[22, 23]]
    elif fault == "no-branches":
        report["meta"]["branch_coverage"] = False
    elif fault == "missing-module":
        del report["files"][names[1]]
    elif fault == "empty":
        data["summary"]["num_statements"] = 0
    elif fault == "duplicate":
        report["files"]["/workspace/" + names[1]] = data
    path = tmp_path / "coverage.json"
    path.write_text("invalid" if fault == "unreadable" else json.dumps(report))
    script = Path(__file__).resolve().parents[1] / "scripts" / "check_download_coverage.py"
    result = subprocess.run(
        [sys.executable, str(script), str(path)], capture_output=True, text=True
    )
    assert result.returncode == (0 if fault is None else 1), result.stdout + result.stderr
    assert (
        "PASS" in result.stdout
        if fault is None
        else "FAIL" in result.stdout
        or "requires" in result.stdout
        or "expected" in result.stdout
        or "Cannot" in result.stdout
    )
