"""Capacity credit is cooperative, crash-safe and separate from OS allocation."""

from __future__ import annotations

import json
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mirror_url.capacity import CapacityBudget, _CapacityClaim
from mirror_url.monitoring import DiskSpaceManager

MIB = 1024**2


@pytest.fixture
def private_home(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return tmp_path


def test_remaining_budget_is_not_reused_after_publication(private_home):
    sample = Mock(return_value=SimpleNamespace(free=10 * MIB))
    budget = CapacityBudget(private_home, 1, sample=sample)
    try:
        first, second = private_home / "a", private_home / "b"
        budget.reserve(first, 8 * MIB)
        budget.release(first, 8 * MIB)
        with pytest.raises(OSError, match="Insufficient capacity"):
            budget.reserve(second, 2 * MIB)
        sample.assert_called_once_with(private_home)
    finally:
        budget.close()
    assert not list((private_home / ".mirror-url/locks-v1").glob("capacity-lease-*"))


def test_unknown_lengths_and_removed_staging_return_only_unused_credit(private_home):
    sample = Mock(return_value=SimpleNamespace(free=12 * MIB))
    budget = CapacityBudget(private_home, 1, sample=sample)
    key = private_home / "a"
    try:
        budget.ensure(key, 9 * MIB)
        budget.release(key)  # owned staging has been removed
        budget.reserve(key, 10 * MIB)
        budget.release(key, 10 * MIB)
        budget.reserve(private_home / "empty", 0)
        sample.assert_called_once()
        with pytest.raises(ValueError, match="Negative"):
            budget.reserve(key, -1)
    finally:
        budget.close()


def test_known_size_transfers_periodically_recheck_external_usage(private_home, monkeypatch):
    clock = [1.0]
    monkeypatch.setattr("mirror_url.capacity.time.monotonic", lambda: clock[0])
    sample = Mock(side_effect=[SimpleNamespace(free=20 * MIB), SimpleNamespace(free=0)])
    budget = CapacityBudget(private_home, 1, sample=sample)
    key = private_home / "a"
    try:
        budget.reserve(key, 8 * MIB)
        clock[0] = 32
        with pytest.raises(OSError, match="capacity decreased"):
            budget.ensure(key, 1024)
        with pytest.raises(OSError, match="admission unavailable"):
            budget.ensure(key, 9 * MIB)
        assert sample.call_count == 2
    finally:
        budget.close()


def test_failed_sample_closes_admission_without_repeating_it(private_home):
    sample = Mock(side_effect=OSError("quota unavailable"))
    budget = CapacityBudget(private_home, 1, sample=sample)
    for _ in range(2):
        with pytest.raises(OSError, match="quota unavailable"):
            budget.reserve(private_home / "a", 1)
    sample.assert_called_once()
    budget.close()


def test_closed_budget_cannot_spend_retired_credit(private_home):
    budget = CapacityBudget(private_home, 1, sample=lambda _: SimpleNamespace(free=20 * MIB))
    key = private_home / "a"
    budget.reserve(key, 1)
    budget.close()
    budget.close()
    with pytest.raises(OSError, match="closed"):
        budget.reserve(key, 1)
    with pytest.raises(OSError, match="closed"):
        budget.ensure(key, 1)


def test_claims_share_one_epoch_without_double_charging_written_bytes(private_home):
    first, second, third = [_CapacityClaim(private_home) for _ in range(3)]
    try:
        assert first.add(70, 70, 100) == 70
        with pytest.raises(OSError, match="Insufficient capacity"):
            second.add(31, 31, 100)
        assert second.add(30, 30, 100) == 30
        first.close(50)  # retained bytes now appear in the free-space sample
        assert third.add(20, 20, 50) == 20
        second.close(0)
        third.close(0)
    finally:
        for claim in (first, second, third):
            if not claim.lease.closed:
                claim.close(0)


def _claim_in_child(target, home, pipe):
    # Spawned interpreters use the supplied environment rather than inheriting
    # a parent's monkeypatched Path.home implementation.
    os.environ["HOME"] = home
    os.environ["USERPROFILE"] = home
    claim = _CapacityClaim(Path(target))
    claim.add(70, 70, 100)
    pipe.send("ready")
    pipe.recv()
    os._exit(0)  # exercise kernel lease recovery, without Python cleanup


def test_crashed_process_cannot_leave_a_permanent_claim(private_home):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(
        target=_claim_in_child, args=(str(private_home), str(private_home), child)
    )
    process.start()
    try:
        assert parent.poll(20)
        assert parent.recv() == "ready"
        other = _CapacityClaim(private_home)
        try:
            with pytest.raises(OSError, match="Insufficient capacity"):
                other.add(40, 40, 100)
            parent.send("exit")
            process.join(20)
            assert process.exitcode == 0
            assert other.add(100, 100, 100) == 100
        finally:
            other.close(0)
    finally:
        if process.is_alive():
            process.terminate()
            process.join(10)
        parent.close()
        child.close()


@pytest.mark.parametrize("raw", [b"{", b"[]", b'{"free":1,"spent":0,"claims":{"../../bad":1}}'])
def test_corrupt_registry_fails_closed(private_home, raw):
    claim = _CapacityClaim(private_home)
    claim.state.write_bytes(raw)
    try:
        with pytest.raises((OSError, ValueError)):
            claim.add(1, 1, 100)
        assert claim.state.read_bytes() == raw
    finally:
        claim.close(0)


def test_changed_live_claim_and_oversized_registry_fail_closed(private_home):
    claim = _CapacityClaim(private_home)
    assert claim.add(10, 10, 100) == 10
    data = json.loads(claim.state.read_text())
    data["claims"][claim.token] = 11
    claim.state.write_text(json.dumps(data))
    with pytest.raises(OSError, match="changed unexpectedly"):
        claim.add(1, 1, 100)
    with pytest.raises(OSError, match="before retirement"):
        claim.close(0)
    assert claim.lease.closed
    other = _CapacityClaim(private_home)
    other.state.write_bytes(b" " * (1024 * 1024 + 1))
    try:
        with pytest.raises(OSError, match="too large"):
            other.add(1, 1, 100)
    finally:
        other.close(0)


def test_capacity_check_failure_is_not_reported_as_success(private_home, monkeypatch):
    def unavailable(_):
        raise OSError("unavailable")

    monkeypatch.setattr("mirror_url.monitoring.shutil.disk_usage", unavailable)
    assert DiskSpaceManager(private_home).check_available(1) == (
        False,
        "Cannot determine destination capacity: unavailable",
    )
