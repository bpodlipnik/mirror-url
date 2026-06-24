"""Shared pytest fixtures for the MirrorURL test suite."""

from __future__ import annotations

import http.server
import threading
from pathlib import Path

import pytest


@pytest.fixture
def tmp_mirror_dir(tmp_path: Path) -> Path:
    """A clean local destination directory for mirror runs."""
    d = tmp_path / "mirror"
    d.mkdir()
    return d


@pytest.fixture
def static_http_server(tmp_path: Path):
    """Serve ``tmp_path`` over HTTP on a random port for integration tests.

    Yields the base URL (e.g. ``http://127.0.0.1:54321/``).
    """
    handler = http.server.SimpleHTTPRequestHandler
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.directory = str(tmp_path)  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    try:
        yield f"http://{host}:{port}/"
    finally:
        server.shutdown()
        thread.join(timeout=5)
