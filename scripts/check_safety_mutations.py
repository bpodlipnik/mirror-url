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
        "whole-file timestamp handle identity",
        "transfers.py",
        "or opened_identity != staged_identity",
        "or False",
        "tests/test_transfer_backends.py::test_timestamp_handle_identity_checked_before_mutation[httpx]",
    ),
    (
        "whole-file staging hash identity",
        "transfers.py",
        "if _identity(staging) != staged_identity:",
        "if False:",
        "tests/test_transfer_backends.py::test_staging_replaced_after_hash_never_publishes_wrong_receipt[httpx-True]",
    ),
    (
        "whole-file staging publication identity",
        "transfers.py",
        "current_staging[:3] != staged_identity[:3]",
        "False",
        "tests/test_transfer_backends.py::test_staging_change_during_timestamp_update_is_preserved[httpx]",
    ),
    (
        "whole-file redirect scope",
        "transfers.py",
        "if not url_within_scope(url, scope):",
        "if False:",
        "tests/test_transfer_backends.py::test_outside_redirect_is_never_contacted[httpx-/outside/a]",
    ),
    (
        "whole-file concurrent destination change",
        "transfers.py",
        "if checked != local or _identity(local) != original:",
        "if False:",
        "tests/test_transfer_backends.py::test_destination_changed_during_transfer_is_preserved[httpx]",
    ),
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
    (
        "case-insensitive filename collision",
        "filename_mapping.py",
        "if not self.sensitivity[parent.parts]:",
        "if False:",
        "tests/test_filename_mapping.py::test_case_insensitive_preflight_rejects_file_and_directory_aliases",
    ),
    (
        "case probe file identity",
        "filename_mapping.py",
        "or _stat_identity(current) != _stat_identity(owned)",
        "or False",
        "tests/test_filename_mapping.py::test_probe_never_deletes_replaced_or_shared_entries",
    ),
    (
        "existing local filename alias",
        "filename_mapping.py",
        "and name not in existing",
        "and False",
        "tests/test_filename_mapping.py::test_existing_alias_lookup_cannot_bypass_preflight",
    ),
    (
        "local content receipt",
        "_core/compare.py",
        'local_content_matches(local_path, stored.get("sha256"))',
        "True",
        "tests/test_content_verification.py::test_same_size_edit_with_identical_cached_stat_fields_requires_download",
    ),
    (
        "hash deadline",
        "_core/compare.py",
        'check_timeout = None if getattr(self.config, "verify_content", False) else 30.0',
        "check_timeout = 30.0",
        "tests/test_content_verification.py::test_async_hashing_keeps_event_loop_responsive",
    ),
    (
        "destination ownership",
        "destination_lock.py",
        "portalocker.lock(handle, flags | portalocker.LOCK_NB)",
        "pass",
        "tests/test_destination_lock.py::test_overlapping_trees_are_exclusive_and_released",
    ),
    (
        "abandoned writer ownership",
        "destination_lock.py",
        "if self._closing and self._active == 0:",
        "if self._closing:",
        "tests/test_destination_lock.py::test_operation_leases_keep_lock_after_cleanup_returns",
    ),
    (
        "scratch manifest ownership",
        "scratch.py",
        "if data != expected:",
        "if False:",
        "tests/test_owned_scratch.py::test_recovery_preserves_every_byte_when_ownership_is_ambiguous",
    ),
    (
        "scratch live work lease",
        "scratch.py",
        "portalocker.lock(stream, portalocker.LOCK_EX | portalocker.LOCK_NB)",
        "pass",
        "tests/test_owned_scratch.py::test_recovery_preserves_active_work_then_removes_recorded_abandoned_bytes",
    ),
    (
        "late chunk writer cleanup",
        "download.py",
        "        if pending:\n\n            def retire",
        "        if False:\n\n            def retire",
        "tests/test_scratch_transfer_lifecycle.py::test_timed_out_chunk_writers_keep_work_leased_until_last_future_exits",
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
