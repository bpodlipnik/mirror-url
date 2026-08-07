"""Regression test for ReportMixin.sync()'s connection-failure early exit.

Verifies the guarantee the user specifically asked about: when the remote
server is unreachable (either the connection test never even succeeded,
or a later check finds connection_ok False), sync() must return early
before calling get_remote_files() or clean_obsolete() at all -- so a
server being offline can never, under any circumstance, lead to local
files being deleted or moved as "obsolete". This is a stronger guarantee
than the partial-scan guard in test_cleanup_partial_scan.py (which
protects against a scan that started but failed partway through): here,
the scan never starts, so clean_obsolete() must never even be invoked.

This is the one path in ReportMixin.sync() that had no dedicated test
before this file -- the underlying logic (see report.py's two early
`return False` branches near the top of sync()) was already correct, but
nothing locked it in, so a future refactor could silently break it
without any test catching the regression.

Builds a ReportMixin instance directly (bypassing the network layer),
with clean_obsolete()/get_remote_files() replaced by spies that record
whether they were ever called, rather than the real implementations --
the point here is proving sync() never reaches them, not exercising what
they do.
"""

from __future__ import annotations

from types import SimpleNamespace

from mirror_url._core.report import ReportMixin


class _SpyCalled(Exception):
    """Raised by a stub method to prove it was reached -- used as an
    unmistakable failure signal if sync() ever calls something it must
    not call in these scenarios."""


class _StubMirror(ReportMixin):
    """Minimal stand-in for MirrorURL exposing only what sync()'s early
    branches touch, plus spies on the methods that must never run."""

    def __init__(self, connection_manager, connection_ok):
        self.connection_manager = connection_manager
        self.connection_ok = connection_ok
        self.suffix_index = 0
        self.total_suffixes = 1
        self.metrics = SimpleNamespace(
            metrics={"files_downloaded": 0, "files_skipped": 0, "files_failed": 0}
        )
        self.files_processed = SimpleNamespace(value=lambda: 0)
        self.files_skipped = SimpleNamespace(value=lambda: 0)
        self.files_failed = SimpleNamespace(value=lambda: 0)
        self.total_downloaded_size = SimpleNamespace(value=lambda: 0)
        self.get_remote_files_called = False
        self.clean_obsolete_called = False

    def _get_prefix(self) -> str:
        return ""

    def get_remote_files(self):
        self.get_remote_files_called = True
        raise _SpyCalled("get_remote_files() must not be called when the connection failed")

    def clean_obsolete(self, remote_files):
        self.clean_obsolete_called = True
        raise _SpyCalled("clean_obsolete() must not be called when the connection failed")


def test_sync_exits_before_cleanup_when_connection_manager_missing():
    """The connection_manager was never even created (e.g. the initial
    connection attempt raised before a manager could be constructed)."""
    mirror = _StubMirror(connection_manager=None, connection_ok=True)

    result = mirror.sync()

    assert result is False
    assert mirror.get_remote_files_called is False
    assert mirror.clean_obsolete_called is False


def test_sync_exits_before_cleanup_when_connection_not_ok():
    """A connection_manager exists, but the connection test itself
    failed (server offline, DNS failure, timeout, refused, ...) --
    exactly the scenario the user was concerned about."""
    mirror = _StubMirror(connection_manager=object(), connection_ok=False)

    result = mirror.sync()

    assert result is False
    assert mirror.get_remote_files_called is False
    assert mirror.clean_obsolete_called is False
