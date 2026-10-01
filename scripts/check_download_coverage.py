"""Require complete statement and branch coverage for each download module."""

from __future__ import annotations

import json
import sys
from pathlib import Path

MODULES = ("src/mirror_url/download.py", "src/mirror_url/download_integrity.py")


def check_coverage(report: dict) -> bool:
    if not report.get("meta", {}).get("branch_coverage"):
        print("Download coverage gate requires a report collected with --cov-branch.")
        return False
    complete = True
    files = report.get("files", {})
    for module in MODULES:
        matches = [
            data
            for name, data in files.items()
            if name.replace("\\", "/") == module or name.replace("\\", "/").endswith("/" + module)
        ]
        if len(matches) != 1:
            print(f"{module}: expected exactly one coverage entry, found {len(matches)}")
            complete = False
            continue
        data = matches[0]
        summary = data["summary"]
        passed = (
            summary["num_statements"] > 0
            and summary["num_branches"] > 0
            and summary["missing_lines"] == 0
            and summary["missing_branches"] == 0
            and not data["missing_lines"]
            and not data["missing_branches"]
        )
        print(
            f"{module}: {'PASS' if passed else 'FAIL'} "
            f"({summary['missing_lines']} missing statements, "
            f"{summary['missing_branches']} missing branches)"
        )
        if not passed:
            print(f"  Lines: {data['missing_lines']}; branches: {data['missing_branches']}")
        complete = complete and passed
    return complete


def main() -> int:
    try:
        report = json.loads(Path(sys.argv[1] if len(sys.argv) > 1 else "coverage.json").read_text())
        return 0 if check_coverage(report) else 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f"Cannot validate download coverage: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
