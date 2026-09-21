"""Regression tests for a real, confirmed data-loss bug Borut identified:
a per-directory scan that failed (timeout, connection reset, non-200)
was silently converted into "this directory has zero files/subdirs",
indistinguishable from a genuinely empty directory -- and that bogus
empty result fed straight into clean_obsolete(), which could then
delete every locally-mirrored file under that subtree as "no longer on
the remote", when the remote state was never actually confirmed.

Root cause, traced precisely:
  1. DirectoryScanner._perform_scan() correctly classifies every failure
     (non-200, timeout, connection reset, ...) as ParsingError and
     raises it -- this part was already correct.
  2. But scan_directory_sequential() (scanner.py) caught that
     ParsingError and swallowed it, `return [], []` -- the exception
     never reached either of its two callers.
  3. ScanMixin._discover_directories_bfs() (scan.py) DOES have a
     try/except that sets self.scan_incomplete = True on failure --
     but since scan_directory_sequential() never let the exception
     through, that guard was dead code for this exact failure mode.
     tests/test_bfs_depth_boundary_scan.py's existing
     test_scan_exception_at_boundary_depth_is_not_reached only proves
     the guard behaves correctly *if* an exception reaches it, using a
     fake scanner that raises directly -- it never exercised the real
     scanner.py, so it never caught the swallow bug.
  4. ScanMixin.get_remote_files()'s second per-directory loop (used to
     build the real remote_files list clean_obsolete() consumes) had NO
     exception handling AT ALL -- a second, independent way the same
     silent-failure-as-empty problem could reach clean_obsolete(), on
     top of (2)/(3).
  5. tests/test_cleanup_partial_scan.py already proves clean_obsolete()
     correctly refuses to run *once* scan_incomplete is True -- that's
     real, valid coverage of the consumer side. It never tested whether
     a real failure actually *sets* scan_incomplete, which is the half
     that was broken.

Fix: scan_directory_sequential() now re-raises instead of swallowing
(still skips caching the failure, which was the one thing the old
except block correctly did). get_remote_files()'s second loop gained
its own try/except mirroring the BFS one. This file closes exactly the
testing gap described above: it exercises the real DirectoryScanner and
the real ScanMixin methods, not doubles that bypass the actual bug.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mirror_url._core.scan import ScanMixin
from mirror_url.enums import MemoryPressure
from mirror_url.exceptions import ParsingError
from mirror_url.metrics import MetricsCollector
from mirror_url.scanner import DirectoryScanner

ROOT = "https://example.test/data/"


# ---------------------------------------------------------------------------
# Layer 1: DirectoryScanner.scan_directory_sequential() must raise, not swallow
# ---------------------------------------------------------------------------


class _FailingResponse:
    def __init__(self, status_code=500):
        self.status_code = status_code
        self.content = b""
        self.headers = {}


class _FailingClient:
    """Simulates a request that fails outright (e.g. connection reset) --
    the client itself raises, rather than returning an error response."""

    def request(self, url, method="GET", timeout=30):
        raise ConnectionError("Server disconnected without sending a response")


class _NonOKClient:
    def __init__(self, status_code=500):
        self.status_code = status_code

    def request(self, url, method="GET", timeout=30):
        return _FailingResponse(self.status_code)


def _make_scanner(client) -> DirectoryScanner:
    mirror_instance = SimpleNamespace(
        connection_manager=client,
        base_url="https://example.test",
        target_base_url=ROOT,
        target_dir=None,
        config=SimpleNamespace(cache_html=False, hash_algorithm="sha256"),
        metrics=MetricsCollector(),
        cache_manager=SimpleNamespace(get_html_cache=lambda url: None),
    )
    return DirectoryScanner(mirror_instance)


def test_scan_directory_sequential_raises_on_connection_failure():
    """The exact scenario Borut hit against the real NASA archive
    ('Server disconnected without sending a response') must now raise,
    not silently return ([], [])."""
    scanner = _make_scanner(_FailingClient())

    with pytest.raises(ParsingError):
        scanner.scan_directory_sequential(ROOT + "flaky/")


def test_scan_directory_sequential_raises_on_non_200():
    scanner = _make_scanner(_NonOKClient(503))

    with pytest.raises(ParsingError):
        scanner.scan_directory_sequential(ROOT + "flaky/")


def test_failed_scan_is_not_cached():
    """The one thing the old except block got right, preserved: a
    failure must not be cached as if it were a confirmed result, or a
    transient error would poison every subsequent lookup this run."""
    scanner = _make_scanner(_FailingClient())

    with pytest.raises(ParsingError):
        scanner.scan_directory_sequential(ROOT + "flaky/")

    assert scanner.parse_cache.get(ROOT + "flaky/") is None


# ---------------------------------------------------------------------------
# Layer 2: ScanMixin._discover_directories_bfs() must set scan_incomplete
# when a REAL failure (now correctly raised) reaches it
# ---------------------------------------------------------------------------


class _RaisingRealScanner:
    """A scanner whose scan_directory_sequential() fails for one
    specific URL exactly the way the real, fixed scanner.py now does
    (raises), and succeeds normally for everything else."""

    def __init__(self, tree, failing_url):
        self.tree = tree
        self.failing_url = failing_url
        self.dir_response_headers: dict = {}

    def scan_directory_sequential(self, url: str):
        if url == self.failing_url:
            raise ParsingError(f"Scan failed for {url}: connection reset")
        return self.tree.get(url, ([], []))


class _FakeMetrics:
    def __init__(self):
        self.metrics: dict = {}

    def increment(self, metric: str, value: int = 1) -> None:
        self.metrics[metric] = self.metrics.get(metric, 0) + value


class _BFSStub(ScanMixin):
    def __init__(self, tree, failing_url):
        self.target_base_url = ROOT
        self.connection_ok = True
        self.scanner = _RaisingRealScanner(tree, failing_url)
        self.config = SimpleNamespace(
            max_depth=50,
            exclude_dirs=[],
            handle_symlinks=False,
            symlink_mode="skip",
        )
        self.per_ip_limiter = SimpleNamespace(wait=lambda ip: None)
        self.scan_incomplete = False
        self.metrics = _FakeMetrics()
        self.symlink_tracker = None

    def _get_prefix(self) -> str:
        return ""

    def _is_dir_excluded(self, url: str) -> bool:
        return False


def test_bfs_sets_scan_incomplete_when_a_directory_scan_fails(monkeypatch):
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    tree = {
        ROOT: ([], [ROOT + "ok/", ROOT + "flaky/"]),
        ROOT + "ok/": ([ROOT + "ok/keep.dat"], []),
        ROOT + "flaky/": ([ROOT + "flaky/also_still_remote.dat"], []),
    }
    mirror = _BFSStub(tree, failing_url=ROOT + "flaky/")

    yielded = list(mirror._discover_directories_bfs())

    assert mirror.scan_incomplete is True
    # The run must still continue and yield the directories that did
    # scan successfully, rather than aborting entirely over one failure.
    assert ROOT in yielded
    assert ROOT + "ok/" in yielded


def test_bfs_scan_incomplete_stays_false_when_nothing_fails(monkeypatch):
    """Sanity check: the fix doesn't make every run look incomplete."""
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    tree = {
        ROOT: ([], [ROOT + "ok/"]),
        ROOT + "ok/": ([ROOT + "ok/keep.dat"], []),
    }
    mirror = _BFSStub(tree, failing_url="https://never-hit.test/")

    list(mirror._discover_directories_bfs())

    assert mirror.scan_incomplete is False


# ---------------------------------------------------------------------------
# Layer 3: get_remote_files()'s second per-directory loop -- previously had
# NO exception handling at all, a second independent path to the same bug
# ---------------------------------------------------------------------------


class _FullPipelineStub(ScanMixin):
    """Extends the BFS stub with what get_remote_files()'s second loop
    (the file-collection pass, used for normal non-detect runs) needs."""

    def __init__(self, tree, failing_url):
        self.target_base_url = ROOT
        self.connection_ok = True
        self.scanner = _RaisingRealScanner(tree, failing_url)
        self.config = SimpleNamespace(
            max_depth=50,
            exclude_dirs=[],
            handle_symlinks=False,
            symlink_mode="skip",
            no_cache=True,
            dry_run=True,
            progress_bar=False,
        )
        self.per_ip_limiter = SimpleNamespace(wait=lambda ip: None)
        self.scan_incomplete = False
        self.metrics = _FakeMetrics()
        self.metrics.add_error = lambda *a, **kw: None
        self.symlink_tracker = None
        self.cache_manager = SimpleNamespace(load=lambda: (False, {}), save=lambda *a, **kw: None)
        self.multi_progress = SimpleNamespace(
            add_level=lambda *a, **kw: None, update=lambda *a, **kw: None
        )
        self.memory_monitor = SimpleNamespace(check_pressure=lambda: MemoryPressure.NORMAL)

    def _get_prefix(self) -> str:
        return ""

    def _is_dir_excluded(self, url: str) -> bool:
        return False

    def _is_within_target_scope(self, url: str) -> bool:
        return True

    def get_directory_signature(self, url: str) -> str:
        return "sig"


def test_get_remote_files_sets_scan_incomplete_and_keeps_going(monkeypatch):
    """The actual bug in its most consequential form: get_remote_files()
    is what feeds clean_obsolete() directly. A directory whose scan
    fails here must not silently vanish from the count as if empty --
    scan_incomplete must be set, and files from OTHER, successfully
    scanned directories must still make it into the returned list."""
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    tree = {
        ROOT: ([], [ROOT + "ok/", ROOT + "flaky/"]),
        ROOT + "ok/": ([ROOT + "ok/keep.dat"], []),
        ROOT + "flaky/": ([ROOT + "flaky/also_still_remote.dat"], []),
    }
    mirror = _FullPipelineStub(tree, failing_url=ROOT + "flaky/")

    remote_files = mirror.get_remote_files()

    assert mirror.scan_incomplete is True
    assert remote_files is not None
    assert ROOT + "ok/keep.dat" in remote_files
    # The file under the directory whose scan failed is correctly
    # absent from this run's list -- but that's fine specifically
    # *because* scan_incomplete is also True, which stops
    # clean_obsolete() from ever treating its absence as confirmation
    # of deletion (see test_cleanup_partial_scan.py).
    assert ROOT + "flaky/also_still_remote.dat" not in remote_files


def test_get_remote_files_scan_incomplete_stays_false_when_nothing_fails(monkeypatch):
    monkeypatch.setattr("mirror_url._core.scan.socket.gethostbyname", lambda host: "127.0.0.1")
    tree = {
        ROOT: ([], [ROOT + "ok/"]),
        ROOT + "ok/": ([ROOT + "ok/keep.dat"], []),
    }
    mirror = _FullPipelineStub(tree, failing_url="https://never-hit.test/")

    remote_files = mirror.get_remote_files()

    assert mirror.scan_incomplete is False
    assert remote_files == [ROOT + "ok/keep.dat"]
