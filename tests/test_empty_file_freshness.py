"""Empty remote files whose HEAD response omits Content-Length stay current."""

from __future__ import annotations

import hashlib
import logging
import os
from email.utils import parsedate_to_datetime
from unittest.mock import Mock

import httpx
import pytest

from mirror_url import MirrorURL
from mirror_url.cache import CacheManager
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote
from test_immediate_audit_fixes import BASE, check_async
from test_immediate_audit_fixes import mirror as mirror

MODIFIED = "Sun, 15 Mar 2015 05:00:50 GMT"
MTIME = parsedate_to_datetime(MODIFIED).timestamp()
ETAG = '"empty-v1"'


class Body(httpx.SyncByteStream):
    def __init__(self, content=b"", fail=False, before_read=None):
        self.content = content
        self.fail = fail
        self.started = False
        self.closed = False
        self.before_read = before_read

    def __iter__(self):
        self.started = True
        if self.before_read:
            self.before_read()
        if self.fail:
            raise httpx.ReadError("probe disconnected")
        if self.content:
            yield self.content
            pytest.fail("An empty-file probe must stop after the first byte")

    def close(self):
        self.closed = True


def local_file(mirror, payload=b""):
    path = mirror.target_dir / "a"
    path.write_bytes(payload)
    os.utime(path, (MTIME, MTIME))
    return path


def head(headers=None, status=200):
    return httpx.Response(status, headers=headers or {"Last-Modified": MODIFIED})


def probe(headers=None, *, content=b"", status=200, fail=False, before_read=None):
    body = Body(content, fail, before_read)
    result = httpx.Response(
        status,
        headers={"Last-Modified": MODIFIED, **(headers or {})},
        stream=body,
        request=httpx.Request("GET", BASE + "a"),
    )
    return result, body


def check(mirror, path, response, asynchronous):
    if asynchronous:
        return not check_async(mirror, [(BASE + "a", path)], response)[0]
    mirror.connection_manager.request.side_effect = [
        response,
        *mirror.connection_manager.request.side_effect,
    ]
    return mirror.file_exists_and_up_to_date(path, BASE + "a")


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("length", [None, "0"])
def test_confirmed_empty_file_is_current_without_replacing_it(mirror, asynchronous, length, caplog):
    path = local_file(mirror)
    before = path.stat()
    result, body = probe({} if length is None else {"Content-Length": length})
    mirror.connection_manager.request.side_effect = [result]
    with caplog.at_level(logging.DEBUG):
        assert check(mirror, path, head(), asynchronous)
    after = path.stat()
    assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
        before.st_ino,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    assert path.read_bytes() == b""
    assert body.closed
    get = mirror.connection_manager.request.call_args
    assert get.kwargs["method"] == "GET" and get.kwargs["stream"]
    assert get.kwargs["headers"] == {"Accept-Encoding": "identity"}
    run = mirror.metrics.run_report.snapshot()
    assert run["existing_files_current"] == 1
    assert run["metadata_checks_unresolved"] == run["uncertain_freshness_downloads"] == 0
    assert run["downloads_planned"] == 0
    assert "Revalidating:" not in caplog.text


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("length", [None, "4"])
def test_truncated_local_file_is_not_mistaken_for_an_empty_remote(mirror, asynchronous, length):
    path = local_file(mirror)
    result, body = probe({} if length is None else {"Content-Length": length}, content=b"data")
    mirror.connection_manager.request.side_effect = [result]
    assert not check(mirror, path, head(), asynchronous)
    assert body.closed
    assert body.started is (length is None)
    assert mirror.metrics.run_report.snapshot()["downloads_planned"] == 1
    assert path.read_bytes() == b""


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "headers,status,fail",
    [
        ({"Content-Length": "bad"}, 200, False),
        ({"Content-Length": "-1"}, 200, False),
        ({"Content-Encoding": "gzip", "Content-Length": "0"}, 200, False),
        ({"Content-Length": "0"}, 206, False),
        ({"Content-Length": "0"}, 304, False),
        ({"Content-Length": "0"}, 503, False),
        ({}, 200, True),
    ],
)
def test_unproven_empty_response_never_skips(mirror, asynchronous, headers, status, fail):
    path = local_file(mirror)
    result, body = probe(headers, status=status, fail=fail)
    mirror.connection_manager.request.side_effect = [result]
    assert not check(mirror, path, head(), asynchronous)
    assert body.closed
    assert mirror.metrics.run_report.snapshot()["metadata_checks_unresolved"] == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_empty_file_with_newer_remote_timestamp_still_updates(mirror, asynchronous):
    path = local_file(mirror)
    result, body = probe({"Content-Length": "0", "Last-Modified": "Mon, 16 Mar 2015 05:00:50 GMT"})
    mirror.connection_manager.request.side_effect = [result]
    assert not check(mirror, path, head(), asynchronous)
    assert body.closed
    assert mirror.metrics.run_report.snapshot()["downloads_planned"] == 1


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "case", ["explicit-zero", "nonempty-local", "newer", "invalid-size", "receipt"]
)
def test_probe_is_limited_to_unknown_size_of_existing_empty_files(mirror, asynchronous, case):
    path = local_file(mirror, b"data" if case == "nonempty-local" else b"")
    headers = {"Last-Modified": MODIFIED}
    if case == "explicit-zero":
        headers["Content-Length"] = "0"
    elif case == "newer":
        headers["Last-Modified"] = "Mon, 16 Mar 2015 05:00:50 GMT"
    elif case == "invalid-size":
        headers["Content-Length"] = "bad"
    elif case == "receipt":
        mirror.config.verify_content = True
    mirror.connection_manager.request.side_effect = []
    assert check(mirror, path, head(headers), asynchronous) is (case == "explicit-zero")
    assert mirror.connection_manager.request.call_count == (0 if asynchronous else 1)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("verify", [False, True])
def test_verified_probe_caches_etag_for_future_head_only_checks(mirror, asynchronous, verify):
    path = local_file(mirror)
    mirror.cache_manager = CacheManager(
        mirror.config.log_path / "empty-file-cache.json", mirror.config, mirror.metrics
    )
    mirror.config.verify_content = verify
    digest = hashlib.sha256(b"").hexdigest()
    if verify:
        mirror.cache_manager.save_file_metadata(path, None, MTIME, 0, sha256=digest)
    result, body = probe({"Content-Length": "0", "ETag": ETAG})
    mirror.connection_manager.request.side_effect = [result]
    assert check(mirror, path, head(), asynchronous)
    assert body.closed
    stored = mirror.cache_manager.get_file_metadata(path)
    assert stored["etag"] == ETAG
    assert stored.get("sha256") == (digest if verify else None)
    mirror.connection_manager.request.reset_mock(side_effect=True)
    mirror.connection_manager.request.side_effect = []
    assert check(mirror, path, head({"ETag": ETAG}), asynchronous)
    assert mirror.connection_manager.request.call_count == (0 if asynchronous else 1)


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("option", ["no_cache", "no_etag"])
def test_disabled_cache_or_etag_still_skips_verified_empty_file(mirror, asynchronous, option):
    path = local_file(mirror)
    setattr(mirror.config, option, True)
    result, body = probe({"Content-Length": "0", "ETag": ETAG})
    mirror.connection_manager.request.side_effect = [result]
    assert check(mirror, path, head(), asynchronous)
    assert body.closed


@pytest.mark.parametrize("asynchronous", [False, True])
def test_local_change_during_probe_cannot_acquire_a_trusted_validator(mirror, asynchronous):
    path = local_file(mirror)
    result, body = probe(
        {"Content-Length": "0", "ETag": ETAG},
        before_read=lambda: path.write_bytes(b"local change"),
    )
    mirror.connection_manager.request.side_effect = [result]
    assert not check(mirror, path, head(), asynchronous)
    assert body.closed
    assert path.read_bytes() == b"local change"
    assert mirror.metrics.run_report.snapshot()["metadata_checks_unresolved"] == 1


@pytest.mark.parametrize("asynchronous", [False, True])
def test_cache_failure_does_not_force_replacement_of_verified_empty_file(mirror, asynchronous):
    path = local_file(mirror)
    mirror.cache_manager.save_file_metadata = Mock(side_effect=OSError("cache unavailable"))
    result, body = probe({"Content-Length": "0", "ETag": ETAG})
    mirror.connection_manager.request.side_effect = [result]
    assert check(mirror, path, head(), asynchronous)
    assert body.closed


@pytest.mark.integration
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("no_etag", [False, True])
def test_real_sync_skips_unchanged_empty_file_and_still_downloads_remote_growth(
    remote, config, asynchronous, no_etag, caplog
):
    remote.files = {"empty.dat": b""}
    if asynchronous:
        # Exercise the real async coordinator, which starts above 80 files.
        remote.files.update({f"current-{i:03}.dat": b"current" for i in range(81)})
    remote.head_without_length.add("empty.dat")
    config.async_metadata = asynchronous
    config.adaptive_async = False
    config.no_etag = no_etag
    config.cache_html = False
    config.use_shared_log = True
    config.request_delay = 0
    config.requests_per_second = 0
    config.check_files = ["empty.dat"]
    stamp = parsedate_to_datetime("Wed, 30 Sep 2026 12:00:00 GMT").timestamp()
    with MirrorURL(config) as mirror:
        for name, payload in remote.files.items():
            path = mirror.target_dir / name
            path.write_bytes(payload)
            os.utime(path, (stamp, stamp))
        path = mirror.target_dir / "empty.dat"
        before = path.stat()
        logging.getLogger().addHandler(caplog.handler)
        with caplog.at_level(logging.INFO):
            assert mirror.sync(), mirror.metrics.get_summary()
        run = mirror.metrics.get_summary()["run"]
        assert run["existing_files_current"] == len(remote.files)
        assert run["downloads_planned"] == run["uncertain_freshness_downloads"] == 0
        assert run["metadata_checks_unresolved"] == 0
        assert mirror.files_processed.value() == 0
        assert "Current: empty.dat" in caplog.text
        assert not any(record.getMessage().startswith("Downloaded: ") for record in caplog.records)
        assert "  Downloaded: 0\n" in caplog.text
        assert (
            sum(method == "GET" and name == "empty.dat" for method, name, _ in remote.requests) == 1
        )
        remote.requests.clear()
        assert mirror.sync(), mirror.metrics.get_summary()
        assert sum(
            method == "GET" and name == "empty.dat" for method, name, _ in remote.requests
        ) == int(no_etag)
        after = path.stat()
        assert (after.st_ino, after.st_mtime_ns, after.st_ctime_ns) == (
            before.st_ino,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        remote.files["empty.dat"] = b"now nonempty"
        assert mirror.sync(), mirror.metrics.get_summary()
        assert path.read_bytes() == b"now nonempty"
        assert mirror.metrics.get_summary()["run"]["changed_files_downloaded"] == 1
