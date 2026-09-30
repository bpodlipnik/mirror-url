"""Mirror real HTTP files with a transport bypass scoped to this test."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("in_worker", [False, True])
def test_full_mirror_run(static_http_server, tmp_mirror_dir, tmp_path, monkeypatch, in_worker):
    # Populate the 'remote' tree the static server exposes.
    import httpx

    from mirror_url import MirrorConfig, MirrorURL
    from mirror_url.transport import SecureTransport

    monkeypatch.setattr(SecureTransport, "handle_request", httpx.HTTPTransport.handle_request)
    monkeypatch.setattr("mirror_url._core._base.HealthCheckServer.start", lambda _: None)
    served_root = tmp_path / "served"
    (served_root / "a.txt").write_text("alpha")
    sub = served_root / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("bravo")

    cfg = MirrorConfig(
        base_url=static_http_server,  # e.g. http://127.0.0.1:PORT/
        dest_path=tmp_mirror_dir,
        log_path=tmp_path / "logs",
        security_validation=False,  # local server; bypass exists only in this test
        async_metadata=False,
        no_cache=True,
        sequential_downloads=True,
        connection_pool_prewarm=False,
    )

    def run():
        with MirrorURL(cfg) as mirror:
            assert mirror.sync() is True

    if in_worker:
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(run).result(timeout=20)
    else:
        run()

    assert (tmp_mirror_dir / "a.txt").read_text() == "alpha"
    assert (tmp_mirror_dir / "sub" / "b.txt").read_text() == "bravo"
