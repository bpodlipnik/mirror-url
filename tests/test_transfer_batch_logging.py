"""Both coordinators report fallback reasons and completed batch outcomes."""

from __future__ import annotations

import logging
import socket

import pytest

from mirror_url import MirrorConfig, MirrorURL, download_url_list
from mirror_url.metrics import log_whole_file_fallback
from mirror_url.security import SecurityValidator
from test_http_mirror_workflows import remote as remote
from test_transfer_backends import backend as backend


@pytest.fixture
def scope(remote, monkeypatch):
    # Restrict the loopback DNS exception to this owned fixture hostname.
    resolver = SecurityValidator.resolve_and_validate_hostname
    lookup = socket.gethostbyname
    addresses = socket.getaddrinfo
    monkeypatch.setattr(
        SecurityValidator,
        "resolve_and_validate_hostname",
        lambda host: "127.0.0.1" if host == "files.example" else resolver(host),
    )
    monkeypatch.setattr(
        socket,
        "gethostbyname",
        lambda host: "127.0.0.1" if host == "files.example" else lookup(host),
    )
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, *args, **kwargs: addresses(
            "127.0.0.1" if host in ("files.example", b"files.example") else host,
            *args,
            **kwargs,
        ),
    )
    return remote.url.replace("127.0.0.1", "files.example")


@pytest.mark.parametrize("mode", ["mirror", "download"])
@pytest.mark.parametrize("strategy", ["sequential", "parallel", "streaming"])
@pytest.mark.parametrize("failed", [False, True])
def test_mixed_batch_reports_actual_outcomes_once(
    tmp_path, remote, scope, backend, mode, strategy, failed, caplog
):
    caplog.set_level(logging.INFO)
    large = b"large" * (500 * 1024)
    remote.files = {"large.bin": large, "small.bin": b"small"}
    if failed:
        remote.files["bad.bin"] = b"bad"
        remote.failures["bad.bin"] = 500
    config = MirrorConfig(
        mode=mode,
        backend=backend,
        base_url=scope,
        download_files=list(remote.files) if mode == "download" else [],
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        min_chunk_size_mb=1,
        max_chunks_per_file=2,
        sequential_downloads=strategy == "sequential",
        parallel_downloads=strategy == "parallel",
        streaming_parallel=strategy == "streaming",
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
        max_retries=0,
        retry_delay=1,
    )
    if mode == "download":
        result = download_url_list(config)
        assert result["success"] is not failed
    else:
        with MirrorURL(config) as mirror:
            assert mirror.sync() is not failed
    if mode == "mirror":
        text = "\n".join(path.read_text(encoding="utf-8") for path in config.log_path.glob("*.log"))
        messages = [
            line.split(" [INFO] ", 1)[1] for line in text.splitlines() if " [INFO] " in line
        ]
    else:
        messages = [record.getMessage() for record in caplog.records]
    assert (config.dest_path / "large.bin").read_bytes() == large
    assert (config.dest_path / "small.bin").read_bytes() == b"small"
    assert not (config.dest_path / "bad.bin").exists()
    assert sum(message.startswith("Downloaded:") for message in messages) == 2
    if strategy != "sequential":
        fallback = [message for message in messages if "Whole-file fallback for" in message]
        assert sum("small.bin:" in message for message in fallback) == 1
        assert any("below the 2.00 MiB needed for two chunks" in message for message in fallback)
        assert any("minimum chunk size 1.00 MiB" in message for message in fallback)
    else:
        assert not any("Whole-file fallback for" in message for message in messages)
    expected = f"File download batch completed: 2 downloaded, {int(failed)} failed"
    assert messages.count(expected) == 1


def test_repeated_mirror_batches_report_current_publications(tmp_path, remote, scope, backend):
    remote.files = {"a": b"a", "b": b"b"}
    config = MirrorConfig(
        backend=backend,
        base_url=scope,
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        sequential_downloads=True,
        connection_pool_prewarm=False,
        request_delay=0,
        requests_per_second=0,
    )
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        (config.dest_path / "a").unlink()
        assert mirror.sync()
    text = "\n".join(path.read_text(encoding="utf-8") for path in config.log_path.glob("*.log"))
    assert text.count("File download batch completed: 2 downloaded, 0 failed") == 1
    assert text.count("File download batch completed: 1 downloaded, 0 failed") == 1


def test_fallback_message_redacts_remote_secrets(caplog):
    caplog.set_level(logging.INFO)
    log_whole_file_fallback(
        "https://user:password@files.example/a?token=secret#private",
        "HEAD returned HTTP 405",
    )
    assert "Whole-file fallback for https://files.example/a?<redacted>:" in caplog.text
    assert "using single-file transfer" in caplog.text
    for secret in ("user:password", "token=secret", "#private"):
        assert secret not in caplog.text
