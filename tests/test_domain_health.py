"""Tests for DomainHealthTracker (domain_health.py).

Every test constructs its own tracker pointed at a tmp_path file, never
the real user cache dir -- get_domain_health_path() itself is tested
separately and in isolation, with HOME/XDG_CACHE_HOME/LOCALAPPDATA/APPDATA
monkeypatched, so it never touches the real filesystem outside tmp_path
either.
"""

from __future__ import annotations

import json
import os
import time

import pytest

from mirror_url.domain_health import (
    INCIDENT_THRESHOLD,
    INCIDENT_WINDOW_SECONDS,
    MAX_INCIDENTS_STORED_PER_DOMAIN,
    DomainHealthTracker,
    get_domain_health_path,
)

# ---------------------------------------------------------------------------
# get_domain_health_path()
# ---------------------------------------------------------------------------


def test_path_resolution_posix_prefers_xdg_cache_home(monkeypatch, tmp_path):
    monkeypatch.setattr("os.name", "posix")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdgcache"))
    path = get_domain_health_path()
    assert path == tmp_path / "xdgcache" / "mirror-url" / "domain_health.json"


def test_path_resolution_posix_falls_back_to_dot_cache(monkeypatch, tmp_path):
    monkeypatch.setattr("os.name", "posix")
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    path = get_domain_health_path()
    assert path == tmp_path / ".cache" / "mirror-url" / "domain_health.json"


# pathlib.WindowsPath refuses to instantiate on a real POSIX OS regardless
# of an os.name monkeypatch (its own __new__ re-checks os.name), so these
# can only meaningfully run on an actual Windows machine/CI runner --
# skipped everywhere else rather than asserting something pathlib itself
# won't let us construct.
@pytest.mark.skipif(
    os.name != "nt", reason="WindowsPath cannot be instantiated on a non-Windows OS"
)
def test_path_resolution_windows_prefers_localappdata(monkeypatch, tmp_path):
    monkeypatch.setattr("os.name", "nt")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    path = get_domain_health_path()
    assert path == tmp_path / "local" / "mirror-url" / "domain_health.json"


@pytest.mark.skipif(
    os.name != "nt", reason="WindowsPath cannot be instantiated on a non-Windows OS"
)
def test_path_resolution_windows_falls_back_to_appdata(monkeypatch, tmp_path):
    monkeypatch.setattr("os.name", "nt")
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setenv("APPDATA", str(tmp_path / "roaming"))
    path = get_domain_health_path()
    assert path == tmp_path / "roaming" / "mirror-url" / "domain_health.json"


# ---------------------------------------------------------------------------
# Core incident tracking / threshold logic
# ---------------------------------------------------------------------------


def test_fresh_domain_is_not_throttled(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    assert tracker.is_throttled("example.test") is False


def test_below_threshold_incidents_do_not_throttle(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    for _ in range(INCIDENT_THRESHOLD - 1):
        tracker.record_incident("example.test")
    assert tracker.is_throttled("example.test") is False


def test_reaching_threshold_throttles(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    for _ in range(INCIDENT_THRESHOLD):
        tracker.record_incident("example.test")
    assert tracker.is_throttled("example.test") is True


def test_domain_matching_is_case_insensitive(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    for _ in range(INCIDENT_THRESHOLD):
        tracker.record_incident("Example.TEST")
    assert tracker.is_throttled("example.test") is True
    assert tracker.is_throttled("EXAMPLE.test") is True


def test_incidents_outside_window_do_not_count(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    old_time = time.time() - INCIDENT_WINDOW_SECONDS - 3600  # just past the window
    with tracker.lock:
        tracker._load()
        tracker._data["example.test"] = [old_time] * INCIDENT_THRESHOLD
        tracker._save()
    assert tracker.is_throttled("example.test") is False


def test_mixed_old_and_recent_incidents_only_recent_count(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    old_time = time.time() - INCIDENT_WINDOW_SECONDS - 3600
    with tracker.lock:
        tracker._load()
        # 10 old (out of window) + (threshold - 1) recent -> not throttled
        tracker._data["example.test"] = [old_time] * 10 + [time.time()] * (INCIDENT_THRESHOLD - 1)
        tracker._save()
    assert tracker.is_throttled("example.test") is False


def test_different_domains_are_tracked_independently(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    for _ in range(INCIDENT_THRESHOLD):
        tracker.record_incident("bad.test")
    assert tracker.is_throttled("bad.test") is True
    assert tracker.is_throttled("good.test") is False


def test_incident_storage_is_bounded_per_domain(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    for _ in range(MAX_INCIDENTS_STORED_PER_DOMAIN + 10):
        tracker.record_incident("example.test")
    with tracker.lock:
        assert len(tracker._data["example.test"]) == MAX_INCIDENTS_STORED_PER_DOMAIN


def test_empty_domain_is_never_throttled(tmp_path):
    tracker = DomainHealthTracker(path=tmp_path / "health.json")
    tracker.record_incident("")  # must not raise, must not be stored usefully
    assert tracker.is_throttled("") is False
    assert tracker.is_throttled(None) is False  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Persistence across instances (simulating separate process runs)
# ---------------------------------------------------------------------------


def test_state_persists_across_tracker_instances(tmp_path):
    """The whole point: a second, independent instance (as a later
    mirror-url invocation would create) must see incidents recorded by
    an earlier instance."""
    path = tmp_path / "health.json"
    first = DomainHealthTracker(path=path)
    for _ in range(INCIDENT_THRESHOLD):
        first.record_incident("p3sc.oma.be")

    second = DomainHealthTracker(path=path)
    assert second.is_throttled("p3sc.oma.be") is True


def test_writes_use_atomic_replace_no_tmp_file_left_behind(tmp_path):
    path = tmp_path / "health.json"
    tracker = DomainHealthTracker(path=path)
    tracker.record_incident("example.test")
    assert path.exists()
    assert not path.with_suffix(".json.tmp").exists()


def test_persisted_file_is_valid_json_with_expected_shape(tmp_path):
    path = tmp_path / "health.json"
    tracker = DomainHealthTracker(path=path)
    tracker.record_incident("example.test")

    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    assert raw["_meta"]["version"] == 1
    assert "example.test" in raw["domains"]
    assert isinstance(raw["domains"]["example.test"], list)


# ---------------------------------------------------------------------------
# Graceful degradation -- never allowed to crash a run
# ---------------------------------------------------------------------------


def test_corrupt_file_is_ignored_not_raised(tmp_path):
    path = tmp_path / "health.json"
    path.write_text("{not valid json at all", encoding="utf-8")
    tracker = DomainHealthTracker(path=path)
    assert tracker.is_throttled("example.test") is False  # must not raise


def test_unexpected_schema_version_is_ignored_not_raised(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps({"_meta": {"version": 999}, "domains": {"example.test": [time.time()] * 10}}),
        encoding="utf-8",
    )
    tracker = DomainHealthTracker(path=path)
    # Future/unknown schema -> starts fresh rather than misinterpreting it.
    assert tracker.is_throttled("example.test") is False


def test_record_incident_survives_unwritable_directory(tmp_path, monkeypatch):
    """If the cache directory can't be created/written (permissions,
    read-only filesystem, etc.), record_incident() must not raise --
    this is a best-effort optimization, never a hard dependency."""
    unwritable_parent = tmp_path / "does" / "not" / "exist"
    tracker = DomainHealthTracker(path=unwritable_parent / "health.json")

    def _boom(*args, **kwargs):
        raise PermissionError("simulated: cannot create directory")

    monkeypatch.setattr("pathlib.Path.mkdir", _boom)

    tracker.record_incident("example.test")  # must not raise
    assert tracker.is_throttled("example.test") is False  # never persisted, so not counted


def test_malformed_domain_entries_in_file_are_skipped(tmp_path):
    """A hand-edited or corrupted file with wrong types for some domains
    must not crash loading -- just skip those entries."""
    path = tmp_path / "health.json"
    path.write_text(
        json.dumps(
            {
                "_meta": {"version": 1},
                "domains": {
                    "good.test": [time.time()] * INCIDENT_THRESHOLD,
                    "bad.test": "not-a-list",
                    123: ["also-wrong-key-type"],
                },
            }
        ),
        encoding="utf-8",
    )
    tracker = DomainHealthTracker(path=path)
    assert tracker.is_throttled("good.test") is True
    assert tracker.is_throttled("bad.test") is False


# ---------------------------------------------------------------------------
# Process-wide singleton accessor
# ---------------------------------------------------------------------------


def test_get_domain_health_tracker_returns_same_instance(monkeypatch, tmp_path):
    import mirror_url.domain_health as dh_module

    monkeypatch.setattr(dh_module, "_default_tracker", None)
    first = dh_module.get_domain_health_tracker()
    second = dh_module.get_domain_health_tracker()
    assert first is second
