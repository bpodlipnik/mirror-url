"""Publication must require the complete CI workflow for the release commit."""

import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def workflow(name):
    # BaseLoader keeps the Actions event name "on" as a string, not YAML 1.1 True.
    return yaml.load((WORKFLOWS / name).read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_publication_cannot_bypass_full_ci():
    release = workflow("release.yml")
    jobs = release["jobs"]
    ci_calls = {
        name for name, job in jobs.items() if job.get("uses") == "./.github/workflows/ci.yml"
    }
    assert ci_calls, "Release must call the full CI workflow from the same commit"
    publishers = {
        name
        for name, job in jobs.items()
        if any(
            step.get("uses", "").startswith(
                ("pypa/gh-action-pypi-publish@", "softprops/action-gh-release@")
            )
            for step in job.get("steps", [])
        )
    }
    assert len(publishers) == 2
    for publisher in publishers:
        required = set()
        pending = [publisher]
        while pending:
            name = pending.pop()
            if name in required:
                continue
            required.add(name)
            job = jobs[name]
            assert job.get("continue-on-error", "false") == "false", name
            # Status functions can override Actions' implicit success() condition.
            assert not re.search(r"\b(always|failure|cancelled)\s*\(", job.get("if", "")), name
            dependencies = job.get("needs", [])
            pending.extend([dependencies] if isinstance(dependencies, str) else dependencies)
        assert ci_calls & required, f"{publisher} can publish without full CI"


def test_reusable_ci_requires_every_job_at_the_event_commit():
    ci = workflow("ci.yml")
    assert "workflow_call" in ci["on"]
    for name, job in ci["jobs"].items():
        assert "if" not in job, f"{name} may skip part of release CI"
        assert job.get("continue-on-error", "false") == "false", name


@pytest.mark.parametrize("workflow_name", ["ci.yml", "release.yml"])
def test_ci_and_release_build_check_out_the_event_commit(workflow_name):
    checkouts = []
    for job in workflow(workflow_name)["jobs"].values():
        checkouts.extend(
            step
            for step in job.get("steps", [])
            if step.get("uses", "").startswith("actions/checkout@")
        )
    assert checkouts, workflow_name
    for checkout in checkouts:
        assert checkout.get("with", {}).get("ref") == "${{ github.sha }}", workflow_name
