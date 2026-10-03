"""Require every safety module and shared URL scope helper to be covered."""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path

from check_download_coverage import check_coverage

MODULES = tuple(
    "src/mirror_url/" + name
    for name in (
        "scanner.py",
        "_core/scan.py",
        "_core/urls.py",
        "_core/cleanup.py",
        "security.py",
        "transport.py",
    )
)
HELPERS = ("url_within_scope", "_relative_url_path")
UTILS = "src/mirror_url/utils.py"


def check_helpers(report: dict) -> bool:
    entries = [
        value
        for name, value in report["files"].items()
        if name.replace("\\", "/") == UTILS or name.replace("\\", "/").endswith("/" + UTILS)
    ]
    if len(entries) != 1:
        print(f"{UTILS}: expected exactly one coverage entry, found {len(entries)}")
        return False
    data = entries[0]
    tree = ast.parse((Path(__file__).resolve().parents[1] / UTILS).read_text())
    complete = True
    for name in HELPERS:
        node = next(
            node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name
        )
        start, end = node.lineno, node.end_lineno
        missing = [line for line in data["missing_lines"] if start <= line <= end]
        branches = [arc for arc in data["missing_branches"] if start <= arc[0] <= end]
        excluded = [line for line in data["excluded_lines"] if start <= line <= end]
        executed = [line for line in data["executed_lines"] if start < line <= end]
        passed = bool(executed) and not (missing or branches or excluded)
        print(
            f"{UTILS}:{name}: {'PASS' if passed else 'FAIL'} (lines {missing}, branches {branches}, exclusions {excluded})"
        )
        complete = complete and passed
    return complete


def main() -> int:
    try:
        report = json.loads(Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json").read_text())
        modules = check_coverage(report, MODULES)
        helpers = check_helpers(report)
        return 0 if modules and helpers else 1
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as error:
        print(f"Cannot validate safety coverage: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
