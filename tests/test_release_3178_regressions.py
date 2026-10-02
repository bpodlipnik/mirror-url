"""Preservation and configuration contracts for the 3.1.78 audit fixes."""

from __future__ import annotations

import json
import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import unquote, urlsplit

import httpx
import pytest

from mirror_url import MirrorConfig, MirrorURL, cli
from mirror_url.concurrency import UnifiedConcurrencyManager
from mirror_url.enums import CleanupPolicy
from mirror_url.exceptions import ConcurrencyLimitError, ConfigError, PathTraversalError
from mirror_url.utils import _relative_url_path, url_within_scope


@pytest.fixture
def build(tmp_path, monkeypatch):
    monkeypatch.setattr(MirrorURL, "test_connection", lambda self: True)
    monkeypatch.setattr("socket.gethostbyname", lambda _: "93.184.216.34")
    mirrors = []

    def create(**options):
        config = MirrorConfig(
            base_url=options.pop("base_url", "https://example.com/root/"),
            dest_path=tmp_path / "data",
            log_path=tmp_path / "logs",
            async_metadata=False,
            sequential_downloads=True,
            connection_pool_prewarm=False,
            max_retries=0,
            no_cache=options.pop("no_cache", True),
            cleanup_policy=CleanupPolicy.DELETE,
            **options,
        )
        mirror = MirrorURL(config)
        mirrors.append(mirror)
        return mirror

    yield create
    for mirror in mirrors:
        mirror.cleanup()


def serve(monkeypatch, mirror, listings, files, failures=()):
    requests = []

    def respond(url, method="GET", **kwargs):
        requests.append((method, url))
        path = unquote(urlsplit(url).path)
        status = 503 if path in failures else 200
        body = listings[path].encode() if path in listings else files[path]
        return httpx.Response(
            status,
            request=httpx.Request(method, url),
            headers={
                "Content-Type": "text/html" if path in listings else "application/octet-stream",
                "Content-Length": str(len(body)),
                "ETag": '"audit"',
            },
            content=body if method == "GET" else b"",
        )

    monkeypatch.setattr(mirror.connection_manager, "request", respond)
    return requests


@pytest.mark.parametrize("kind", ["suffix", "destination", "ancestor", "broken"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_selected_root_symlink_is_rejected_before_managers(tmp_path, monkeypatch, kind, dry_run):
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "unrelated.txt"
    victim.write_bytes(b"preserve")
    link = tmp_path / "linked"
    link.symlink_to(outside if kind != "broken" else tmp_path / "absent", target_is_directory=True)
    config = MirrorConfig(
        base_url="https://example.com/root/",
        dest_path=tmp_path if kind == "suffix" else link / "data" if kind == "ancestor" else link,
        dir_suffix="linked" if kind == "suffix" else "",
        log_path=tmp_path / "logs",
        dry_run=dry_run,
        cleanup_policy=CleanupPolicy.DELETE,
    )
    coordinator = Mock()
    monkeypatch.setattr("mirror_url._core._base.UnifiedConcurrencyManager", coordinator)
    with pytest.raises(PathTraversalError, match="symlink"):
        MirrorURL(config)
    coordinator.assert_not_called()
    assert victim.read_bytes() == b"preserve"
    assert link.is_symlink()
    assert not (tmp_path / "logs").exists()
    assert not (outside / "data").exists()


def test_relative_destination_is_valid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(MirrorURL, "test_connection", lambda self: True)
    config = MirrorConfig(
        base_url="https://example.com/root/",
        dest_path=Path("data"),
        log_path=tmp_path / "logs",
        dir_suffix="nested",
        async_metadata=False,
        connection_pool_prewarm=False,
    )
    with MirrorURL(config) as mirror:
        assert mirror.target_dir == (tmp_path / "data" / "nested").resolve()


@pytest.mark.skipif(sys.platform != "darwin", reason="macOS system-directory aliases")
def test_macos_system_alias_is_allowed(tmp_path, monkeypatch):
    from mirror_url.security import PathSafety

    canonical = tmp_path.resolve()
    for name in ("var", "tmp"):
        if canonical.is_relative_to(Path("/private") / name):
            alias = Path("/") / name / canonical.relative_to(Path("/private") / name) / "data"
            break
    else:
        pytest.skip("temporary directory is outside the macOS system aliases")
    assert PathSafety._resolve_destination_root(alias) == canonical / "data"
    assert not alias.exists()


@pytest.mark.parametrize("root", ["root;v=1", "root%3Bv=1"])
def test_root_query_and_semicolon_keep_request_and_cleanup_identity(build, monkeypatch, root):
    mirror = build(base_url=f"https://example.com/{root}/?sort=name")
    decoded_root = "/" + unquote(root) + "/"
    requests = serve(
        monkeypatch,
        mirror,
        {decoded_root: '<a href="keep.bin">keep</a>'},
        {decoded_root + "keep.bin": b"remote"},
    )
    assert mirror.sync()
    assert (mirror.target_dir / "keep.bin").read_bytes() == b"remote"
    assert any(url.endswith("/?sort=name") for method, url in requests if method == "GET")


@pytest.mark.parametrize(
    "root,href",
    [
        ("root", "/%72oot/keep.bin"),
        ("%72oot", "/root/keep.bin"),
        ("my%20files", "/my files/keep.bin"),
    ],
)
@pytest.mark.parametrize("exists", [False, True])
def test_equivalent_root_link_downloads_and_preserves(build, monkeypatch, root, href, exists):
    mirror = build(base_url=f"https://example.com/{root}/")
    local = mirror.target_dir / "keep.bin"
    if exists:
        local.write_bytes(b"original")
    decoded_root = "/" + unquote(root) + "/"
    requests = serve(
        monkeypatch,
        mirror,
        {decoded_root: f'<a href="{href}">keep.bin</a>'},
        {decoded_root + "keep.bin": b"remote"},
    )
    assert mirror.sync()
    assert not mirror.scan_incomplete
    assert local.read_bytes() == b"remote"
    assert any(url == "https://example.com" + href for method, url in requests if method == "GET")


def test_encoded_directory_root_is_traversed_and_exclusions_are_consistent(build, monkeypatch):
    mirror = build(exclude_dirs=["skip"])
    skipped = mirror.target_dir / "skip" / "preserve.bin"
    skipped.parent.mkdir()
    skipped.write_bytes(b"preserve")
    serve(
        monkeypatch,
        mirror,
        {
            "/root/": '<a href="/%72oot/child/">child</a><a href="/%72oot/skip/">skip</a>',
            "/root/child/": '<a href="keep.bin">keep</a>',
        },
        {"/root/child/keep.bin": b"remote"},
    )
    assert mirror.sync()
    assert (mirror.target_dir / "child" / "keep.bin").read_bytes() == b"remote"
    assert skipped.read_bytes() == b"preserve"


@pytest.mark.parametrize("fail_listing", [False, True])
def test_query_directory_is_discovered_or_cleanup_is_suppressed(build, monkeypatch, fail_listing):
    mirror = build()
    local = mirror.target_dir / "child" / "keep.bin"
    local.parent.mkdir()
    local.write_bytes(b"original")
    requests = serve(
        monkeypatch,
        mirror,
        {
            "/root/": '<a href="child/?sort=name">child</a>',
            "/root/child/": '<a href="keep.bin">keep</a>',
        },
        {"/root/child/keep.bin": b"remote"},
        failures=["/root/child/"] if fail_listing else [],
    )
    assert mirror.sync() is not fail_listing
    assert mirror.scan_incomplete is fail_listing
    assert local.read_bytes() == (b"original" if fail_listing else b"remote")
    assert any(url.endswith("child/?sort=name") for method, url in requests if method == "GET")


def test_query_aliases_do_not_repeat_directory_traversal(build, monkeypatch):
    mirror = build(no_cache=False)
    requests = serve(
        monkeypatch,
        mirror,
        {
            "/root/": '<a href="child/?sort=a">a</a><a href="child/?sort=b">b</a>',
            "/root/child/": '<a href="/root/child/?sort=c">self</a><a href="keep.bin">keep</a>',
        },
        {"/root/child/keep.bin": b"remote"},
    )
    assert mirror.sync()
    child_gets = [
        url for method, url in requests if method == "GET" and urlsplit(url).path == "/root/child/"
    ]
    assert child_gets == ["https://example.com/root/child/?sort=a"]


@pytest.mark.parametrize("href", ["raw;v=1.bin", "raw%3Bv=1.bin"])
@pytest.mark.parametrize("pattern", [".bin", r"v=1\.bin$"])
def test_semicolon_filename_preserves_name_and_filter(build, monkeypatch, href, pattern):
    mirror = build(file_filters=[pattern])
    local = mirror.target_dir / "raw;v=1.bin"
    local.write_bytes(b"original")
    serve(
        monkeypatch,
        mirror,
        {"/root/": f'<a href="{href}">file</a>'},
        {"/root/raw;v=1.bin": b"remote"},
    )
    assert mirror.sync()
    assert local.read_bytes() == b"remote"
    assert not (mirror.target_dir / "raw").exists()


def test_literal_percent_escape_has_one_local_decoding(build, monkeypatch):
    mirror = build()
    serve(
        monkeypatch,
        mirror,
        {"/root/": '<a href="raw%253Bv=1.bin">file</a>'},
        {"/root/raw%3Bv=1.bin": b"remote"},
    )
    assert mirror.sync()
    assert (mirror.target_dir / "raw%3Bv=1.bin").read_bytes() == b"remote"
    assert not (mirror.target_dir / "raw;v=1.bin").exists()


@pytest.mark.parametrize("path", ["../file", "%2e%2e/file", "%252e%252e/file", "%255cfile"])
def test_scoped_mapping_still_blocks_traversal(path):
    assert (
        _relative_url_path("https://example.com/root/" + path, "https://example.com/root/") is None
    )


def test_semicolon_does_not_expand_scope():
    base = "https://example.com/root/"
    for url in (
        "https://example.com/root;other/a",
        "https://evil.example/root/a",
        "http://example.com/root/a",
    ):
        assert not url_within_scope(url, base)


def test_invalid_expected_path_blocks_cleanup(build):
    mirror = build()
    victim = mirror.target_dir / "keep.bin"
    victim.write_bytes(b"preserve")
    mirror.clean_obsolete({"https://example.com/other/keep.bin"})
    assert mirror.scan_incomplete
    assert victim.read_bytes() == b"preserve"


@pytest.mark.parametrize("mode,other", [("list_files", "list_dirs"), ("list_dirs", "list_files")])
def test_cli_listing_mode_overrides_other_file_mode(tmp_path, monkeypatch, mode, other):
    monkeypatch.setattr(logging.getLogger(), "level", logging.getLogger().level)
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "base_url": "https://example.com/root/",
                "dest_path": str(tmp_path / "data"),
                "log_path": str(tmp_path / "logs"),
                other: True,
            }
        )
    )
    called = []

    class Mirror:
        def __init__(self, config, **kwargs):
            assert getattr(config, mode) is True
            assert getattr(config, other) is False
            assert getattr(config, mode + "_n") == 3
            self.connection_manager = object()
            self.connection_ok = True

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def install_signal_handlers(self):
            pass

        def list_directories(self):
            called.append("list_dirs")
            return True

        def list_files(self):
            called.append("list_files")
            return True

    monkeypatch.setattr(cli, "MirrorURL", Mirror)
    monkeypatch.setattr(
        sys, "argv", ["mirror-url", "--config", str(config), "--" + mode.replace("_", "-"), "3"]
    )
    with pytest.raises(SystemExit) as outcome:
        cli.main()
    assert outcome.value.code == 0
    assert called == [mode]


def test_conflicting_listing_modes_fail_configuration(tmp_path):
    with pytest.raises(ConfigError, match="list_dirs and list_files"):
        MirrorConfig(
            base_url="https://example.com/root/",
            dest_path=tmp_path,
            log_path=tmp_path,
            list_dirs=True,
            list_files=True,
        )


def test_no_lxml_selection_with_fallback_disabled(build, monkeypatch):
    monkeypatch.setattr("mirror_url.parsing.LXML_AVAILABLE", False)
    monkeypatch.setattr("mirror_url.scanner.LXML_AVAILABLE", False)
    mirror = build(fast_parsing_fallback=False)
    serve(
        monkeypatch,
        mirror,
        {"/root/": '<a href="keep.bin">keep</a>'},
        {"/root/keep.bin": b"remote"},
    )
    assert mirror.sync()
    assert (mirror.target_dir / "keep.bin").read_bytes() == b"remote"
    assert mirror.scanner.fast_parse_count > 0


@pytest.fixture
def pool():
    manager = UnifiedConcurrencyManager(max_total_threads=2)
    manager.shared_pool_enabled = True
    manager.start()
    yield manager
    manager.shutdown()


def test_multiple_shared_pool_waiters_recheck_budget(pool, monkeypatch):
    assert pool.acquire_thread()
    assert pool.acquire_thread()
    waiting = threading.Event()
    first_started, both_started, release = threading.Event(), threading.Event(), threading.Event()
    original_wait = pool.thread_condition.wait
    waiters = set()
    jobs = []
    futures = []

    def tracked_wait(timeout):
        waiters.add(threading.get_ident())
        if len(waiters) == 2:
            waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(pool.thread_condition, "wait", tracked_wait)

    def job():
        with pool.thread_condition:
            jobs.append(threading.get_ident())
            first_started.set()
            if len(jobs) == 2:
                both_started.set()
        assert release.wait(5)

    def submit():
        futures.append(pool.submit_to_shared_pool(job))

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    try:
        assert waiting.wait(3)
        pool.release_thread()
        assert first_started.wait(3)
        with pool.thread_condition:
            assert pool.active_threads == 2
            assert len(jobs) == 1
        pool.release_thread()
        assert both_started.wait(3)
    finally:
        release.set()
        for thread in threads:
            thread.join(3)
    for future in futures:
        future.result(3)
    pool.shared_pool.shutdown(wait=True)
    assert pool.get_stats()["active_threads"] == 0
    assert pool.get_stats()["pending_operations"] == 0


@pytest.mark.parametrize("error", [ValueError, SystemExit])
def test_shared_pool_counts_each_failure_once(pool, error):
    def fail():
        raise error("audit")

    future = pool.submit_to_shared_pool(fail)
    with pytest.raises(error):
        future.result(3)
    pool.shared_pool.shutdown(wait=True)
    stats = pool.get_stats()
    assert stats["total_submitted"] == stats["total_completed"] == stats["total_failed"] == 1
    assert stats["active_threads"] == 0


def test_cancelled_shared_pool_task_releases_one_lease():
    manager = UnifiedConcurrencyManager(max_total_threads=2)
    manager.shared_pool_enabled = True
    manager.shared_pool = ThreadPoolExecutor(max_workers=1)
    manager.start()
    release, started = threading.Event(), threading.Event()

    def blocking():
        started.set()
        assert release.wait(5)

    try:
        first = manager.submit_to_shared_pool(blocking)
        assert started.wait(3)
        second = manager.submit_to_shared_pool(lambda: None)
        assert second.cancel()
        assert manager.get_stats()["active_threads"] == 1
        release.set()
        first.result(3)
    finally:
        release.set()
        manager.shutdown()
    stats = manager.get_stats()
    assert stats["total_failed"] == 1
    assert stats["total_completed"] == 2
    assert stats["active_threads"] == 0


def test_waiting_submission_stops_on_shutdown(pool, monkeypatch):
    pool.max_total_threads = 1
    assert pool.acquire_thread()
    waiting = threading.Event()
    original_wait = pool.thread_condition.wait
    failures = []

    def tracked_wait(timeout):
        waiting.set()
        return original_wait(timeout)

    monkeypatch.setattr(pool.thread_condition, "wait", tracked_wait)

    def submit():
        try:
            pool.submit_to_shared_pool(lambda: None)
        except ConcurrencyLimitError as error:
            failures.append(error)

    worker = threading.Thread(target=submit)
    worker.start()
    assert waiting.wait(3)
    pool.shutdown()
    worker.join(3)
    assert not worker.is_alive()
    assert len(failures) == 1
    pool.release_thread()
    assert pool.get_stats()["active_threads"] == 0
    assert pool.get_stats()["pending_operations"] == 0
