"""Current discovery GET provenance, content signatures and cleanup safety."""

from __future__ import annotations

import hashlib

import httpx
import pytest

from mirror_url import MirrorURL
from mirror_url.enums import CleanupPolicy, ScanMode
from mirror_url.metrics import MetricsCollector
from safety_helpers import ROOT, SafetyMirror
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote


@pytest.fixture
def mirror(tmp_path, monkeypatch):
    monkeypatch.setattr("socket.gethostbyname", lambda host: "8.8.8.8")
    return SafetyMirror(tmp_path, no_cache=False)


@pytest.mark.parametrize(
    "headers,expected",
    [
        ({"ETag": '"get"', "Last-Modified": "date"}, 'etag:"get"'),
        ({"Last-Modified": "date"}, "mtime:date"),
        ({}, None),
        ({"ETag": "", "Last-Modified": ""}, None),
    ],
)
def test_successful_get_signature_precedence_and_headerless_content(mirror, headers, expected):
    document = '<a href="keep.bin">keep</a>'
    if expected is None:
        expected = mirror.get_directory_signature(ROOT, html_content=document)

    def request(url, method="GET", **kwargs):
        return httpx.Response(
            200,
            headers=headers if method == "GET" else {"ETag": '"head"'},
            content=document.encode(),
        )

    mirror.connection_manager.request.side_effect = request
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    assert mirror._scan_signatures == {ROOT: expected}
    summary = mirror.metrics.get_summary()
    assert summary["listing_fetches"] == 1
    assert summary["directory_signatures_from_get"] == 1
    assert summary["directory_signature_head_calls"] == 0
    assert not any(
        call.kwargs.get("method") == "HEAD"
        for call in mirror.connection_manager.request.call_args_list
    )
    assert bool(mirror.scanner.fresh_listing_content_signatures) == (not any(headers.values()))
    assert summary["listing_get_seconds"] >= 0
    assert not mirror.scan_incomplete


@pytest.mark.parametrize("proof", ["header", "content"])
def test_cached_listing_never_reuses_previous_discovery_get_proof(mirror, proof):
    mirror.connection_manager.request.side_effect = lambda *args, **kw: httpx.Response(
        200,
        headers={"ETag": '"old"'} if proof == "header" else {},
        content=b'<a href="keep.bin">keep</a>',
    )
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    old_signature = mirror._scan_signatures[ROOT]
    assert old_signature.startswith("etag:" if proof == "header" else "content:")
    mirror.connection_manager.request.reset_mock()
    mirror.connection_manager.request.side_effect = lambda *args, **kw: httpx.Response(
        200, headers={"ETag": '"new"'}
    )
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    assert mirror._scan_signatures == {ROOT: 'etag:"new"'}
    mirror.connection_manager.request.assert_called_once_with(ROOT, method="HEAD", timeout=15)
    assert mirror.scanner.fresh_listing_headers == {}
    assert mirror.scanner.fresh_listing_content_signatures == {}


@pytest.mark.parametrize("failure", ["http", "parse", "network"])
@pytest.mark.parametrize("proof", ["header", "content"])
def test_failed_refetch_discards_get_proof_and_preserves_local_payload(mirror, failure, proof):
    mirror.config.no_cache = True
    keep = mirror.target_dir / "keep.bin"
    keep.write_bytes(b"must survive incomplete discovery")
    get_calls = 0

    def request(url, method="GET", **kwargs):
        nonlocal get_calls
        if method == "HEAD":
            return httpx.Response(200, headers={"ETag": '"head"'})
        get_calls += 1
        if get_calls == 1:
            return httpx.Response(
                200,
                headers={"ETag": '"first"'} if proof == "header" else {},
                content=b'<a href="keep.bin">keep</a>',
            )
        if failure == "network":
            raise OSError("injected listing failure")
        return httpx.Response(
            503 if failure == "http" else 200,
            headers={"ETag": '"failed"'},
            content=b'<a href="http://[broken">bad</a>',
        )

    mirror.connection_manager.request.side_effect = request
    files = mirror.get_remote_files()
    assert files == []
    assert mirror.scan_incomplete
    assert mirror.scanner.fresh_listing_headers == {}
    assert mirror.scanner.fresh_listing_content_signatures == {}
    assert mirror._scan_signatures == {ROOT: 'etag:"head"'}
    mirror.clean_obsolete(set(files))
    assert keep.read_bytes() == b"must survive incomplete discovery"
    mirror.cache_manager.save.assert_not_called()
    summary = mirror.metrics.get_summary()
    assert summary["listing_fetches"] == 2
    assert summary["directory_signature_head_calls"] == 1
    assert summary["directory_signatures_from_get"] == 0


@pytest.mark.parametrize("hash_algorithm", ["md5", "sha256"])
def test_content_signature_tracks_complete_decoded_listing(mirror, hash_algorithm):
    mirror.config.refresh_cache = True
    mirror.config.hash_algorithm = hash_algorithm
    document = '<a href="keep.bin">café</a>'

    def request(url, method="GET", **kwargs):
        assert method == "GET"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/html; charset=iso-8859-1"},
            content=document.encode("iso-8859-1"),
        )

    mirror.connection_manager.request.side_effect = request
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    initial = mirror._scan_signatures[ROOT]
    assert initial == "content:" + hashlib.new(hash_algorithm, document.encode("utf-8")).hexdigest()
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    assert mirror._scan_signatures[ROOT] == initial
    # The discovered paths stay the same; the complete listing text changes.
    document += "<!-- changed listing -->"
    assert mirror.get_remote_files() == [ROOT + "keep.bin"]
    assert mirror._scan_signatures[ROOT] != initial
    assert mirror._scan_signatures[ROOT] == mirror.get_directory_signature(
        ROOT, html_content=document
    )
    assert mirror.metrics.get_summary()["directory_signature_head_calls"] == 0


def test_successful_empty_headerless_listing_needs_no_signature_head(mirror):
    mirror.connection_manager.request.side_effect = lambda *args, **kw: httpx.Response(
        200, content=b""
    )
    assert mirror.get_remote_files() == []
    assert mirror._scan_signatures == {ROOT: mirror.get_directory_signature(ROOT, html_content="")}
    assert not mirror.scan_incomplete
    assert mirror.metrics.get_summary()["directory_signature_head_calls"] == 0


def test_directory_request_reporting_uses_current_sync_deltas():
    metrics = MetricsCollector()
    metrics.increment("listing_fetches", 7)
    metrics.increment("directory_signature_head_calls", 6)
    metrics.increment("directory_signatures_from_get", 5)
    metrics.add_directory_request_time("GET", 10)
    metrics.add_directory_request_time("HEAD", 9)
    metrics.run_report.start(metrics.metrics, 0)
    metrics.increment("listing_fetches", 2)
    metrics.increment("directory_signature_head_calls")
    metrics.increment("directory_signatures_from_get")
    metrics.add_directory_request_time("GET", 0.125)
    metrics.add_directory_request_time("HEAD", 0.25)
    metrics.run_report.finish("success")
    report = metrics.report()
    assert "Directory listing GET calls: 2 (0.125s sum)" in report
    assert "Directory signature HEAD calls: 1 (0.250s sum)" in report
    assert "Directory signatures reused from fresh GET: 1" in report
    assert metrics.get_summary()["listing_fetches"] == 9
    assert metrics.get_summary()["listing_get_seconds"] == 10.125


@pytest.mark.integration
@pytest.mark.parametrize("validators", [True, False])
@pytest.mark.parametrize("scan_mode", [ScanMode.ADAPTIVE, ScanMode.PARALLEL])
def test_42_directory_discovery_reuses_successful_get_without_signature_head(
    remote, config, validators, scan_mode
):
    config.scan_mode = scan_mode
    remote.files = {f"dir{index}/file": b"payload" for index in range(41)}
    remote.directory_headers = {"ETag": '"listing"'} if validators else {}
    with MirrorURL(config) as mirror:
        # Initial connection probing precedes the measured discovery pass.
        remote.requests.clear()
        assert set(mirror.get_remote_files()) == {remote.url + name for name in remote.files}
        calls = [(method, path) for method, path, _ in remote.requests]
        assert len(calls) == 42
        assert all(method == "GET" for method, _ in calls)
        summary = mirror.metrics.get_summary()
        assert summary["listing_fetches"] == summary["directory_signatures_from_get"] == 42
        assert summary["directory_signature_head_calls"] == 0
        assert all(
            signature.startswith("etag:" if validators else "content:")
            for signature in mirror._scan_signatures.values()
        )
        assert not mirror.scan_incomplete


@pytest.mark.integration
@pytest.mark.parametrize("validators", [True, False])
@pytest.mark.parametrize("scan_mode", [ScanMode.ADAPTIVE, ScanMode.PARALLEL])
def test_unchanged_directory_get_signature_does_not_hide_changed_child(
    remote, config, validators, scan_mode
):
    config.scan_mode = scan_mode
    remote.files = {"selected": b"first", "unchecked": b"keep"}
    remote.directory_headers = {"ETag": '"unchanged-directory"'} if validators else {}
    config.cache_html = False
    config.missing_files = True
    config.check_files = ["selected"]
    with MirrorURL(config) as mirror:
        assert mirror.sync(), mirror.metrics.get_summary()
        initial_signature = mirror._scan_signatures[remote.url]
        remote.files["selected"] = b"changed payload"
        remote.requests.clear()
        assert mirror.sync(), mirror.metrics.get_summary()
        assert mirror._scan_signatures[remote.url] == initial_signature
        assert (config.dest_path / "selected").read_bytes() == b"changed payload"
        assert (config.dest_path / "unchecked").read_bytes() == b"keep"
        assert any(method == "HEAD" and path == "selected" for method, path, _ in remote.requests)
        assert not any(method == "HEAD" and not path for method, path, _ in remote.requests)
        result = mirror.metrics.get_summary()["run"]
        assert result["changed_files_downloaded"] == 1
        assert result["freshness_checks_skipped"] == 1


@pytest.mark.integration
@pytest.mark.parametrize("validators", [True, False])
@pytest.mark.parametrize("scan_mode", [ScanMode.ADAPTIVE, ScanMode.PARALLEL])
def test_failed_listing_with_get_proof_preserves_failed_subtree(
    remote, config, validators, scan_mode
):
    config.scan_mode = scan_mode
    remote.files = {"flaky/keep": b"keep", "ok/keep": b"ok"}
    remote.directory_headers = (
        {"Last-Modified": "Wed, 30 Sep 2026 12:00:00 GMT"} if validators else {}
    )
    config.cache_html = False
    config.cleanup_policy = CleanupPolicy.DELETE
    with MirrorURL(config) as mirror:
        assert mirror.sync(), mirror.metrics.get_summary()
        remote.failures["flaky/"] = 503
        mirror.sync()
        assert mirror.scan_incomplete
        assert (config.dest_path / "flaky/keep").read_bytes() == b"keep"
        assert (config.dest_path / "ok/keep").read_bytes() == b"ok"
        assert remote.url + "flaky/" not in mirror.scanner.fresh_listing_headers
        assert remote.url + "flaky/" not in mirror.scanner.fresh_listing_content_signatures
