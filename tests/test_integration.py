"""End-to-end integration test (full HTTP mirror run).

Most subsystem-level integration coverage lives in ``test_subsystems.py`` and
runs in the normal lane. This file holds the *true* end-to-end case: drive a
real ``MirrorURL.sync()`` against a live local HTTP server and assert files land
on disk.

It is marked ``integration`` (deselected by ``pytest -m "not integration"``) and
currently **skipped**, because it requires bypassing the SSRF guard:

``SecureTransport`` intentionally refuses to connect to loopback/private hosts
(``127.0.0.1``, ``localhost``, RFC-1918, …). That protection is correct in
production but blocks a localhost test server. The transport already supports a
``test_mode`` flag that skips IP validation — it is just not wired through from
config yet. To enable this test, add an opt-in (e.g. a private
``_allow_local_targets`` config flag that ``ConnectionPool._create_client``
forwards as ``SecureTransport(..., test_mode=True)``) and remove the skip.

Once enabled, the body below is the intended shape and should pass unchanged.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skip(reason="needs SSRF-guard test bypass for loopback targets (see module docstring)")
def test_full_mirror_run(static_http_server, tmp_mirror_dir, tmp_path):
    from mirror_url import MirrorConfig, MirrorURL

    # Populate the 'remote' tree the static server exposes.
    served_root = tmp_path  # static_http_server serves tmp_path
    (served_root / "a.txt").write_text("alpha")
    sub = served_root / "sub"
    sub.mkdir()
    (sub / "b.txt").write_text("bravo")

    cfg = MirrorConfig(
        base_url=static_http_server,  # e.g. http://127.0.0.1:PORT/
        dest_path=tmp_mirror_dir,
        log_path=tmp_path / "logs",
        security_validation=False,  # plus the test_mode bypass noted above
        async_metadata=False,
        no_cache=True,
    )

    with MirrorURL(cfg) as mirror:
        assert mirror.sync() is True

    assert (tmp_mirror_dir / "a.txt").read_text() == "alpha"
    assert (tmp_mirror_dir / "sub" / "b.txt").read_text() == "bravo"
