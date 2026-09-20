"""Test for DirectoryScanner's Last-Modified/ETag header capture.

Background: Borut asked whether matching Last-Modified/ETag between two
directories is also a valid symlink signal, on top of the basename
fingerprint. It is -- Apache's autoindex commonly derives a directory
listing's Last-Modified from the most-recently-modified entry it's
showing, and a symlinked directory resolves straight through to the
same underlying files as its target, so the two paths often report
identical Last-Modified (and sometimes ETag). The headers are already
present on the same GET response scan_directory_sequential() makes to
fetch the HTML to parse -- this locks in that they get captured (into
DirectoryScanner.dir_response_headers) rather than silently discarded,
at zero extra request cost.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url.metrics import MetricsCollector
from mirror_url.scanner import DirectoryScanner

URL = "https://example.test/data/somedir/"


class _FakeResponse:
    def __init__(self, headers: dict):
        self.status_code = 200
        self.content = b"<html><body></body></html>"
        self.headers = headers


class _FakeClient:
    def __init__(self, response: _FakeResponse):
        self._response = response
        self.requested_urls: list[str] = []

    def request(self, url: str, method: str = "GET", timeout: int = 30):
        self.requested_urls.append(url)
        return self._response


def _make_scanner(headers: dict) -> tuple[DirectoryScanner, _FakeClient]:
    response = _FakeResponse(headers)
    client = _FakeClient(response)
    mirror_instance = SimpleNamespace(
        connection_manager=client,
        base_url="https://example.test",
        target_base_url="https://example.test/data/",
        target_dir=None,
        config=SimpleNamespace(cache_html=False, hash_algorithm="sha256"),
        metrics=MetricsCollector(),
        cache_manager=SimpleNamespace(get_html_cache=lambda url: None),
    )
    scanner = DirectoryScanner(mirror_instance)
    return scanner, client


def test_scan_captures_last_modified_and_etag():
    scanner, client = _make_scanner(
        {"Last-Modified": "Fri, 28 Feb 2020 12:00:00 GMT", "ETag": '"abc123"'}
    )

    scanner.scan_directory_sequential(URL)

    assert client.requested_urls == [URL]
    assert scanner.dir_response_headers[URL] == (
        "Fri, 28 Feb 2020 12:00:00 GMT",
        '"abc123"',
    )


def test_scan_records_none_when_headers_absent():
    """A server that sends neither header must not crash or leave a
    stale/wrong entry -- explicit (None, None), not a missing key, so
    callers can distinguish 'no data' from 'not scanned yet'."""
    scanner, _client = _make_scanner({})

    scanner.scan_directory_sequential(URL)

    assert scanner.dir_response_headers[URL] == (None, None)


def test_two_directories_with_matching_headers_are_captured_independently():
    """The actual use case: two different URLs, each scanned once, each
    keeping its own headers -- so ScanMixin can compare them afterward."""
    mirror_instance_a = SimpleNamespace(
        connection_manager=_FakeClient(
            _FakeResponse({"Last-Modified": "Fri, 28 Feb 2020 12:00:00 GMT"})
        ),
        base_url="https://example.test",
        target_base_url="https://example.test/data/",
        target_dir=None,
        config=SimpleNamespace(cache_html=False, hash_algorithm="sha256"),
        metrics=MetricsCollector(),
        cache_manager=SimpleNamespace(get_html_cache=lambda url: None),
    )
    scanner = DirectoryScanner(mirror_instance_a)

    scanner.scan_directory_sequential("https://example.test/data/idl/")
    # Swap in a second response for the same scanner instance -- as
    # happens across two different URLs in one real BFS run.
    scanner.client._response = _FakeResponse({"Last-Modified": "Fri, 28 Feb 2020 12:00:00 GMT"})
    scanner.scan_directory_sequential("https://example.test/data/lasco/")

    assert (
        scanner.dir_response_headers["https://example.test/data/idl/"]
        == scanner.dir_response_headers["https://example.test/data/lasco/"]
    )
