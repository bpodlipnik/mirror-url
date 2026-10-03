"""Discovery, URL identity and cache boundaries feeding obsolete-file cleanup."""

from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import scanner as scanner_module
from mirror_url.enums import MemoryPressure
from mirror_url.exceptions import ParsingError, PathTraversalError
from safety_helpers import ROOT, SafetyMirror


@pytest.fixture
def mirror(tmp_path, monkeypatch):
    monkeypatch.setattr("socket.gethostbyname", lambda host: "8.8.8.8")
    return SafetyMirror(tmp_path)


def test_scanner_periodic_pruning_and_persisted_cache(tmp_path):
    mirror = SafetyMirror(tmp_path, no_cache=False)
    scanner = mirror.scanner
    result = ([ROOT + "keep"], [])
    mirror.cache_manager.get_html_cache.return_value = result
    assert scanner.scan_directory_sequential(ROOT) == result
    assert scanner.scan_directory_sequential(ROOT) == result
    mirror.connection_manager.request.assert_not_called()
    assert scanner.get_parse_stats()["parse_cache_lookups"]["hits"] == 1
    for i in range(4):
        scanner.parse_cache.put(str(i), ([], []))
    scanner._last_cache_cleanup -= 301
    scanner._maybe_cleanup_cache()
    assert len(scanner.parse_cache.cache) < 5
    assert scanner.get_parse_stats()["html_cache"]["size"] == 0
    del mirror.cache_manager.html_cache
    assert scanner.get_parse_stats()["html_cache"] == {}


@pytest.mark.parametrize("fallback", [False, True])
def test_failed_xpath_is_rejected_or_recovered(mirror, monkeypatch, fallback):
    mirror.config.fast_parsing_fallback = fallback
    mirror.listing('<html><a href="keep.bin">keep</a></html>')
    monkeypatch.setattr(scanner_module, "LXML_AVAILABLE", True)
    monkeypatch.setattr(scanner_module, "should_use_fast_parser", lambda *args: False)
    monkeypatch.setattr(mirror.scanner, "LINK_XPATH", None)
    if fallback:
        assert mirror.scanner.scan_directory_sequential(ROOT) == ([ROOT + "keep.bin"], [])
    else:
        with pytest.raises(ParsingError):
            mirror.scanner.scan_directory_sequential(ROOT)
        assert mirror.scanner.parse_cache.get(ROOT) is None


def test_lightweight_selection_without_lxml(mirror, monkeypatch):
    mirror.listing('<a href="keep.bin">keep</a>')
    monkeypatch.setattr(scanner_module, "LXML_AVAILABLE", False)
    # Exercise the defensive selection path if accelerator availability changes.
    monkeypatch.setattr(scanner_module, "should_use_fast_parser", lambda *args: False)
    assert mirror.scanner.scan_directory_sequential(ROOT) == ([ROOT + "keep.bin"], [])
    assert mirror.scanner.fast_parse_count == 1


def test_scanner_scope_filters_and_empty_href(mirror):
    mirror.config.file_filters = [".bin"]
    mirror.config.dir_suffix = "selected"
    from urllib.parse import urlparse

    mirror.target_parsed = urlparse(ROOT + "selected/")
    mirror.listing("""<html><a href="">empty</a><a href="./">self</a>
        <a href="../">parent</a><a href="data:text/plain,hello">data</a>
        <a href="https://other.com/root/keep.bin">foreign</a>
        <a href="other/">outside target</a><a href="selected/">selected</a>
        <a href="keep.bin">keep</a><a href="omit.txt">omit</a></html>""")
    assert mirror.scanner.scan_directory_sequential(ROOT) == (
        [ROOT + "keep.bin"],
        [ROOT + "selected/"],
    )


def test_malformed_anchor_fails_discovery_instead_of_confirming_empty(mirror):
    mirror.listing('<a href="http://[broken">bad</a>')
    assert mirror.get_remote_files() == []
    assert mirror.scan_incomplete
    assert mirror.scanner.parse_cache.get(ROOT) is None


def test_real_lxml_failure_uses_lightweight_parser(mirror):
    mirror.listing("")
    assert mirror.scanner.scan_directory_sequential(ROOT) == ([], [])
    assert mirror.scanner.fast_parse_count == 1


@pytest.mark.parametrize("pressure", [MemoryPressure.WARNING, MemoryPressure.CRITICAL])
def test_memory_pressure_never_changes_discovered_files(mirror, pressure):
    mirror.listing('<a href="keep.bin">keep</a>')
    mirror.memory_monitor.check_pressure.return_value = pressure
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    mirror.cache_manager.handle_memory_pressure.assert_called_once_with(pressure)


def test_cache_load_save_failure_does_not_erase_complete_scan(tmp_path, monkeypatch):
    monkeypatch.setattr("socket.gethostbyname", lambda host: "8.8.8.8")
    mirror = SafetyMirror(tmp_path, no_cache=False)
    mirror.listing('<a href="keep.bin">keep</a>')
    mirror.cache_manager.load.return_value = (True, {ROOT: "old"})
    mirror.cache_manager.save.side_effect = OSError("injected disk full")
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    assert not mirror.scan_incomplete
    assert mirror.scanner.cached_signatures == {ROOT: "old"}
    mirror.cache_manager.save.assert_called_once()
    mirror.cache_manager.load.side_effect = OSError("injected read failure")
    assert mirror.get_remote_files() is None
    assert mirror.scan_incomplete


@pytest.mark.parametrize(
    "headers,status,expected",
    [
        ({"ETag": "tag"}, 200, "etag:tag"),
        ({"Last-Modified": "date"}, 200, "mtime:date"),
        ({}, 503, "url:" + ROOT),
    ],
)
def test_directory_signature_uses_response_metadata(mirror, headers, status, expected):
    mirror.connection_manager.request.side_effect = lambda *args, **kw: httpx.Response(
        status, headers=headers
    )
    assert mirror.get_directory_signature(ROOT) == expected
    digest = hashlib.md5(b"listing").hexdigest()
    assert mirror.get_directory_signature(ROOT, "listing") == "content:" + digest


def test_signature_network_failure_has_url_fallback(mirror):
    mirror.connection_manager.request.side_effect = OSError("injected network failure")
    assert mirror.get_directory_signature(ROOT).startswith("url:" + ROOT + ":")


def test_invalid_filters_and_directory_exclusions_are_bounded(mirror):
    mirror.config.file_filters = ["["]
    assert not mirror.matches_filter(ROOT + "keep.bin")
    assert not mirror.matches_filter(ROOT)
    assert not mirror._validate_url_scheme("ftp://example.com/")
    mirror.config.exclude_dirs = ["", "outside", "skip"]
    assert not mirror._is_dir_excluded(ROOT)
    assert not mirror._is_dir_excluded("https://other.com/skip/")
    assert mirror._is_dir_excluded(ROOT + "skip/")
    mirror.config.dir_suffix = "../escape"
    with pytest.raises(PathTraversalError):
        mirror._get_target_base_url()
    mirror.target_parsed = None
    assert mirror._is_within_target_scope(ROOT)


@pytest.mark.parametrize(
    "guard", ["depth", "loop", "bomb", "other", "failure", "no_tracker", "allowed"]
)
def test_symlink_policy_is_observable_and_bounded(mirror, guard):
    mirror.config.handle_symlinks = True
    depth = mirror.config.max_symlink_depth if guard == "depth" else 0
    mirror.symlink_tracker = (
        None
        if guard == "no_tracker"
        else SimpleNamespace(can_follow=Mock(return_value=(guard == "allowed", guard)))
    )
    if guard == "failure":
        mirror.symlink_tracker.can_follow.side_effect = OSError("injected tracker failure")
    linked, target = mirror.is_symlink(ROOT + "link", depth=depth)
    assert linked == (guard in {"depth", "loop", "bomb", "other"})
    assert target is None
    mirror.config.symlink_mode = "follow"
    mirror.symlink_tracker = SimpleNamespace(record_follow=Mock(), record_skip=Mock())
    mirror.record_symlink(ROOT + "link", ROOT, mirror.target_dir)
    mirror.symlink_tracker.record_follow.assert_called_once()
    mirror.config.symlink_mode = "skip"
    mirror.record_symlink(ROOT + "link", ROOT, mirror.target_dir)
    mirror.symlink_tracker.record_skip.assert_called_once()
    mirror.symlink_tracker = None
    mirror.record_symlink(ROOT + "link", ROOT, mirror.target_dir)
    mirror.config.symlink_mode = "follow"
    mirror.record_symlink(ROOT + "link", ROOT, mirror.target_dir)
    mirror.config.symlink_mode = "detect"
    mirror.record_symlink(ROOT + "link", ROOT, mirror.target_dir)


@pytest.mark.parametrize("field,value", [("connection_ok", False), ("target_base_url", None)])
def test_unavailable_discovery_yields_nothing(mirror, field, value):
    setattr(mirror, field, value)
    assert list(mirror._discover_directories_bfs()) == []


def test_malformed_signature_and_missing_header_evidence(mirror):
    assert mirror._dir_entry_signature([None], []) is None
    assert mirror._check_directory_symlink(ROOT, [None], [], {}) == (False, None)
    assert mirror._symlink_confidence_note(ROOT, None) == ""
    mirror.scanner.dir_response_headers = {ROOT: ("date", "etag")}
    assert "target path" in mirror._symlink_confidence_note(ROOT, ROOT + "old/")
    mirror.scanner.dir_response_headers[ROOT + "old/"] = ("other", "etag")
    assert mirror._symlink_confidence_note(ROOT, ROOT + "old/") == " [ETag matches]"


@pytest.mark.parametrize("field", ["target_dir", "target_parsed", "_target_dir_path"])
def test_missing_mapping_state_rejects_destination(mirror, field):
    setattr(mirror, field, None)
    assert mirror._get_local_path_from_url(ROOT + "keep") is None


def test_mapping_failure_cannot_return_outside_path(mirror, monkeypatch):
    mirror._target_dir_path = mirror.target_dir / "different-root"
    assert mirror._get_local_path_from_url(ROOT + "keep") is None
    monkeypatch.setattr(
        "mirror_url._core.scan._relative_url_path",
        Mock(side_effect=ValueError("injected mapping failure")),
    )
    assert mirror._get_local_path_from_url(ROOT + "keep") is None


def test_empty_discovery_and_duplicate_signature(mirror):
    mirror.connection_ok = False
    assert mirror.get_remote_files() == []
    signatures = {}
    for _ in range(2):
        assert mirror._check_directory_symlink(ROOT, [ROOT + "keep"], [], signatures) == (
            False,
            None,
        )


def test_tracker_can_reject_without_a_reason(mirror):
    mirror.config.handle_symlinks = True
    mirror.symlink_tracker = SimpleNamespace(can_follow=lambda *args: (False, None))
    assert mirror.is_symlink(ROOT + "keep") == (True, None)


def test_disabled_symlink_handling_does_not_query_tracker(mirror):
    mirror.config.handle_symlinks = False
    mirror.symlink_tracker = SimpleNamespace(
        can_follow=Mock(side_effect=AssertionError("tracker should not be queried"))
    )
    assert mirror.is_symlink(ROOT + "keep") == (False, None)


@pytest.mark.parametrize("root", ["https://example.com/root/../", "http:///root/"])
def test_malformed_discovery_root_does_not_raise(mirror, root):
    mirror.target_base_url = root
    assert list(mirror._discover_directories_bfs()) == ([] if ".." in root else [root])


@pytest.mark.parametrize(
    "mode,tracker,outside",
    [
        ("skip", True, False),
        ("skip", True, True),
        ("follow", False, False),
        ("follow", True, False),
        ("follow", True, True),
    ],
)
def test_bfs_duplicate_subtree_policy(mirror, monkeypatch, mode, tracker, outside):
    from urllib.parse import urlparse

    from mirror_url.security import SymlinkTracker

    mirror.config.handle_symlinks = True
    mirror.config.symlink_mode = mode
    if outside:
        mirror.target_parsed = urlparse(ROOT + "b/")
    mirror.symlink_tracker = SymlinkTracker() if tracker else None
    if mode == "follow" and tracker and not outside:
        mirror.symlink_tracker.max_depth = 0
    tree = {
        ROOT: ([], [ROOT + "a/", ROOT + "b/"]),
        ROOT + "a/": ([ROOT + "a/keep"], []),
        ROOT + "b/": ([ROOT + "b/keep"], []),
    }
    monkeypatch.setattr(mirror.scanner, "scan_directory_sequential", lambda url: tree[url])
    found = list(mirror._discover_directories_bfs())
    assert ROOT + "a/" in found
    skipped = mode == "skip" or outside or tracker
    assert (ROOT + "b/" not in found) == skipped
    assert mirror.metrics.metrics["symlinks_detected"] == 1


def test_pressure_without_eviction_still_preserves_complete_listing(mirror):
    mirror.listing('<a href="keep">keep</a>')
    mirror.memory_monitor.check_pressure.return_value = MemoryPressure.NORMAL
    assert mirror.get_remote_files() == [ROOT + "keep"]
