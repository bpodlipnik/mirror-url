"""Planning bounds never replace HTTP representation or byte-range validation."""

from __future__ import annotations

from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL
from mirror_url.capacity import CapacityError
from mirror_url.download_integrity import save_resume_metadata
from mirror_url.enums import DownloadMethod
from mirror_url.planning import SizeHint, listing_size_hints, parse_listing_size
from test_download_failure_contract import manager as manager
from test_http_mirror_workflows import remote as remote
from test_immediate_audit_fixes import mirror as mirror
from test_immediate_audit_fixes import parallel as parallel
from test_scanner_header_capture import URL, _make_scanner
from test_transfer_batch_logging import scope as scope


@pytest.mark.parametrize(
    "text,upper,rounded",
    [
        ("0", 0, False),
        ("968", 968, False),
        ("2108160", 2108160, False),
        ("2.0M", 2202010, True),
        ("1.0M", 1153434, True),
        ("12K", 13312, True),
        (" 1G ", 2 * 1024**3, True),
    ],
)
def test_listing_size_uses_conservative_bounds(text, upper, rounded):
    assert parse_listing_size(text) == SizeHint(upper, rounded)


@pytest.mark.parametrize("text", ["", "-", "-1", "nan", "1,024", "1.00M", "1m", "1e20", "1" * 200])
def test_ambiguous_size_is_unknown(text):
    assert parse_listing_size(text) is None


def row(name="a.fts", size="2.0M"):
    return f'<tr><td></td><td><a href="{name}">file</a></td><td>2026-10-30 20:03</td><td>{size}</td><td></td></tr>'


def test_table_pre_and_ambiguous_rows():
    document = (
        "<table>"
        + row()
        + row("b", "-")
        + row("conflict", "1K")
        + row("conflict", "2K")
        + '<tr><td><a href="unrelated">file</a></td><td>123</td><td>55</td></tr></table>'
        + '<pre><a href="c">c</a> 30-Oct-2026 20:03 1234\n'
        + '<a href="d">d</a> 2026-10-30 20:03 12K\n</pre>'
    )
    assert listing_size_hints(document) == {
        "a.fts": SizeHint(2202010, True),
        "c": SizeHint(1234),
        "d": SizeHint(13312, True),
    }


def test_hint_collection_is_bounded_and_does_not_limit_discovery(monkeypatch):
    monkeypatch.setattr("mirror_url.planning.MAX_IN_MEMORY_CACHE_SIZE", 2)
    document = "<table>" + row("a") + row("b") + row("c") + row("a", "1K") + row("d") + "</table>"
    assert listing_size_hints(document) == {"b": SizeHint(2202010, True)}
    monkeypatch.setattr("mirror_url.planning.MAX_IN_MEMORY_CACHE_SIZE", 3)
    monkeypatch.setattr("mirror_url.scanner.MAX_IN_MEMORY_CACHE_SIZE", 2)
    scanner, client = _make_scanner({})
    scanner.mirror.matches_filter = lambda _: True
    client._response.content = ("<table>" + row("a") + row("b") + row("c") + "</table>").encode()
    files, _ = scanner._perform_scan(URL)
    assert len(files) == 3
    assert len(scanner.size_hints) == 2
    # A different listing referring to already-cached URLs does not create
    # unbounded duplicate source records.
    client._response.content = ("<table>" + row(URL + "a") + "</table>").encode()
    scanner._perform_scan(URL + "other/")
    assert len(scanner._hint_urls) == 1
    client._response.content = b'<a href="a">a</a>'
    scanner._perform_scan(URL)
    assert scanner.size_hints == {} and scanner._hint_urls == {}


def test_large_or_unrecognized_listing_sizes_remain_unknown():
    assert parse_listing_size("999999999999999999E") is None
    assert listing_size_hints('<a href="a">1K</a>') == {}


def test_unknown_sizes_do_not_trigger_synthetic_speed_probes(manager):
    manager.config.parallel_downloads = False
    manager.config.streaming_parallel = False
    manager.config.sequential_downloads = False
    for name in ("_detect_ssd", "_estimate_network_speed", "_check_range_support"):
        setattr(manager, name, Mock(side_effect=AssertionError("unnecessary probe")))
    assert manager.auto_select_method([None, 0, None], 3, []) == DownloadMethod.TRADITIONAL_PARALLEL


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("sizes", [("2.0M", "1.0M"), ("1", "0"), ("-", "-")])
def test_get_is_authoritative_for_rounded_stale_and_missing_hints(
    tmp_path, remote, scope, backend, sizes
):
    remote.files = {"a.fts": b"a" * 8192, "b.fts": b"b" * 3072}
    remote.listings[""] = "<table>" + row("a.fts", sizes[0]) + row("b.fts", sizes[1]) + "</table>"
    config = MirrorConfig(
        base_url=scope,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        backend=backend,
        missing_files=True,
        security_validation=False,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
    )
    with MirrorURL(config) as mirror:
        assert mirror.sync(), mirror.metrics.get_summary()
        for name, payload in remote.files.items():
            assert (config.dest_path / name).read_bytes() == payload
        assert not any(
            method == "HEAD" and name in remote.files for method, name, _ in remote.requests
        )
        if sizes[0] != "-":
            assert mirror.scanner.size_hints[scope + "a.fts"] == parse_listing_size(sizes[0])
        # A fresh listing without sizes must not retain hints for earlier rows.
        mirror.config.no_cache = True
        remote.listings[""] = '<a href="a.fts">a</a><a href="b.fts">b</a>'
        mirror.scanner.scan_directory_sequential(scope)
        assert scope + "a.fts" not in mirror.scanner.size_hints


def test_lazy_probe_can_be_explicitly_bypassed(manager, mirror):
    mirror.parallel_manager = manager
    manager.enabled = True
    mirror._get_file_size = Mock(side_effect=AssertionError("unnecessary HEAD"))
    mirror._download_file_single = Mock(return_value=True)
    assert mirror.download_file_with_resume(
        "https://example.com/root/a", mirror.target_dir / "a", probe_size=False
    )


def test_capacity_failure_preserves_validated_resume_prefix(mirror):
    url = "https://example.com/root/a"
    final = mirror.target_dir / "a"
    final.write_bytes(b"old destination")
    partial = mirror.partial_manager.register_partial(final, url)
    partial.write_bytes(b"abc")
    save_resume_metadata(partial, url, '"v1"', 6)
    mirror.connection_manager.request.return_value = httpx.Response(
        206,
        headers={"Content-Range": "bytes 3-5/6", "Content-Length": "3", "ETag": '"v1"'},
        content=b"def",
        request=httpx.Request("GET", url),
    )
    mirror._capacity_budget = Mock()
    mirror._capacity_budget.reserve.side_effect = CapacityError("capacity unavailable")
    assert not mirror._download_file_single(url, final)
    assert final.read_bytes() == b"old destination"
    assert partial.read_bytes() == b"abc"
    assert mirror.connection_manager.request.call_args.kwargs["headers"]["Range"] == "bytes=3-"


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_rounded_large_hint_never_becomes_a_range_length(tmp_path, remote, scope, backend):
    remote.files = {"a.fts": b"abc", "b.fts": b"def"}
    remote.listings[""] = "<table>" + row("a.fts", "20M") + row("b.fts", "1") + "</table>"
    config = MirrorConfig(
        base_url=scope,
        dest_path=tmp_path / "mirror",
        log_path=tmp_path / "logs",
        backend=backend,
        missing_files=True,
        security_validation=False,
        parallel_downloads=True,
        min_chunk_size_mb=1,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
    )
    with MirrorURL(config) as mirror:
        assert mirror.sync(), mirror.metrics.get_summary()
        for name, payload in remote.files.items():
            assert (config.dest_path / name).read_bytes() == payload
        assert any(method == "HEAD" and name == "a.fts" for method, name, _ in remote.requests)
        assert not any(method == "HEAD" and name == "b.fts" for method, name, _ in remote.requests)
        assert not any(
            "Range" in headers for method, name, headers in remote.requests if name in remote.files
        )
