"""Tests for ProgressTracker._generate_report()'s Rate: field unit.

Production report: progress lines like "Rate: 14.8/s" never said *what*
14.8 per second was -- files? directories? downloads? The item type
(self.name -- "directories", "downloads", "files checked") was already
present earlier in the same log line ("1919/7675 files checked"), but
never repeated next to the Rate: figure itself, making that number
ambiguous when read out of context (e.g. grepped from a log file rather
than read inline).

Fixed by appending self.name to both the interim ("Rate: X {name}/s")
and final ("Overall rate: X {name}/s") report lines -- the same name
already used earlier in the line, no new parameter needed.
"""

from __future__ import annotations

import time

from mirror_url.progress import ProgressTracker


def test_interim_report_includes_unit_in_rate_field():
    tracker = ProgressTracker(total=100, name="files checked", use_tqdm=False)
    tracker.completed = 25
    tracker.start_time = time.time() - 10  # pretend 10s have elapsed

    report = tracker._generate_report()

    assert "Rate: " in report
    assert "files checked/s" in report
    # The bare, unitless form must not appear anywhere in the report.
    assert " Rate: 2.5/s" not in report


def test_final_report_includes_unit_in_overall_rate_field():
    tracker = ProgressTracker(total=100, name="downloads", use_tqdm=False)
    tracker.completed = 100
    tracker.start_time = time.time() - 20

    report = tracker._generate_report(force_total=True)

    assert "Overall rate: " in report
    assert "downloads/s" in report
    assert " rate: 5.0/s)" not in report


def test_unit_matches_the_tracked_item_name():
    """Different levels (directories/downloads/files checked) must each
    show their own name as the unit, not a hardcoded generic one."""
    for name in ("directories", "downloads", "files checked", "items"):
        tracker = ProgressTracker(total=10, name=name, use_tqdm=False)
        tracker.completed = 5
        tracker.start_time = time.time() - 5

        report = tracker._generate_report()

        assert f"{name}/s" in report


def test_rate_value_itself_is_unaffected_by_the_unit_addition():
    """The fix only appends a unit label -- the numeric rate value and
    its formatting (one decimal place) must be unchanged."""
    tracker = ProgressTracker(total=100, name="files checked", use_tqdm=False)
    tracker.completed = 20
    tracker.start_time = time.time() - 10  # 20 completed / 10s = 2.0/s

    report = tracker._generate_report()

    assert "Rate: 2.0 files checked/s" in report
