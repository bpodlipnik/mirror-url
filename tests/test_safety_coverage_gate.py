"""The safety gate must detect individual gaps, exclusions and missing reports."""

import ast
import json
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "line",
        "branch",
        "missing_module",
        "no_branches",
        "helper_line",
        "helper_branch",
        "helper_excluded",
        "helper_absent",
        "duplicate_helpers",
        "bad_json",
    ],
)
def test_safety_gate_rejects_gaps_and_unmeasured_helpers(tmp_path, fault):
    root = Path(__file__).resolve().parents[1]
    names = [
        "scanner.py",
        "_core/scan.py",
        "_core/urls.py",
        "_core/cleanup.py",
        "security.py",
        "filename_mapping.py",
        "transport.py",
        "destination_lock.py",
        "scratch.py",
    ]
    files = {
        "src/mirror_url/" + name: {
            "summary": {
                "num_statements": 1000,
                "num_branches": 1000,
                "missing_lines": 0,
                "missing_branches": 0,
            },
            "missing_lines": [],
            "missing_branches": [],
        }
        for name in names
    }
    nodes = [
        node
        for node in ast.parse((root / "src/mirror_url/utils.py").read_text()).body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"url_within_scope", "_relative_url_path"}
    ]
    helper = {
        "executed_lines": [node.end_lineno for node in nodes],
        "missing_lines": [],
        "missing_branches": [],
        "excluded_lines": [],
    }
    files["src/mirror_url/utils.py"] = helper
    data = files["src/mirror_url/scanner.py"]
    report = {"meta": {"branch_coverage": fault != "no_branches"}, "files": files}
    if fault == "line":
        data["missing_lines"] = [10]
        data["summary"]["missing_lines"] = 1
    elif fault == "branch":
        data["missing_branches"] = [[10, 11]]
        data["summary"]["missing_branches"] = 1
    elif fault == "missing_module":
        del files["src/mirror_url/scanner.py"]
    elif fault == "helper_line":
        helper["missing_lines"] = [nodes[1].end_lineno]
    elif fault == "helper_branch":
        helper["missing_branches"] = [[nodes[1].end_lineno, -1]]
    elif fault == "helper_excluded":
        helper["excluded_lines"] = [nodes[1].end_lineno]
    elif fault == "helper_absent":
        helper["executed_lines"] = []
    elif fault == "duplicate_helpers":
        files["/other/src/mirror_url/utils.py"] = helper
    path = tmp_path / "coverage.json"
    path.write_text("invalid" if fault == "bad_json" else json.dumps(report))
    outcome = subprocess.run(
        [sys.executable, str(root / "scripts/check_safety_coverage.py"), str(path)],
        capture_output=True,
        text=True,
    )
    assert outcome.returncode == (0 if fault is None else 1), outcome.stdout + outcome.stderr
