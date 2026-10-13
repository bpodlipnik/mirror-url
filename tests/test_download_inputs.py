"""Root-relative download inputs share scope, mapping and transfer policy."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.parse import quote

import pytest

from mirror_url import MirrorConfig, cli, download_url_list
from mirror_url.exceptions import ConfigError, SecurityError, URLScopeError
from mirror_url.transfers import resolve_download_files
from test_download_mode_chunks import chunk_mode as chunk_mode
from test_download_mode_chunks import configuration
from test_download_mode_chunks import ranges as ranges
from test_transfer_backends import assert_no_scratch, job
from test_transfer_backends import backend as backend
from test_transfer_backends import origin as origin


@pytest.mark.parametrize(
    "root", ["https://example.org/data", "https://example.org/data/?listing=1"]
)
def test_relative_inputs_are_literal_paths_and_absolute_urls_retain_queries(root):
    values = ["sub/a b#?.bin", "sub/literal%20.bin", "https://example.org/data/absolute?token=x"]
    assert resolve_download_files(values, root) == [
        "https://example.org/data/sub/a%20b%23%3F.bin",
        "https://example.org/data/sub/literal%2520.bin",
        values[-1],
    ]


@pytest.mark.parametrize("source", ["list", "files"])
@pytest.mark.integration
def test_root_relative_inputs_preserve_names_bytes_and_receipts(tmp_path, origin, backend, source):
    values = {"a": b"alpha", "nested/a b#.bin": b"binary\x00", "literal%20.bin": b"percent"}
    for name, content in values.items():
        origin.routes["/root/" + quote(name, safe="/")] = (200, {"ETag": '"original"'}, content)
    config = job(tmp_path, origin, backend)
    if source == "list":
        config.url_list.write_text(
            "\ufeff# selected files\n\n" + "\n".join(values) + "\n", encoding="utf-8"
        )
    else:
        config.url_list = None
        config.download_files = list(values)
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 3
    assert result["bytes_downloaded"] == sum(map(len, values.values()))
    receipts = json.loads(Path(result["receipts"]).read_text())["files"]
    for name, content in values.items():
        local = config.dest_path / name
        assert local.read_bytes() == content
        assert receipts[str(local)]["url"] == origin.base + quote(name, safe="/")
        assert receipts[str(local)]["sha256"] == hashlib.sha256(content).hexdigest()
    assert {request[1] for request in origin.requests} == {
        "/root/" + quote(name, safe="/") for name in values
    }
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("source", ["list", "files"])
@pytest.mark.integration
def test_suffix_selects_files_without_re_rooting_input_paths(tmp_path, origin, backend, source):
    config = job(tmp_path, origin, backend, dir_suffix="selected")
    entries = ["selected/a", origin.base + "selected/nested/b"]
    if source == "list":
        config.url_list.write_text("\n".join(entries))
    else:
        config.url_list, config.download_files = None, entries
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 2
    assert (config.dest_path / "selected/a").read_bytes() == b"bytes"
    assert (config.dest_path / "selected/nested/b").read_bytes() == b"bytes"
    assert not (config.dest_path / "selected/selected").exists()
    assert {request[1] for request in origin.requests} == {
        "/root/selected/a",
        "/root/selected/nested/b",
    }


@pytest.mark.parametrize("broad_root", [False, True])
@pytest.mark.integration
def test_root_choice_controls_the_retained_local_prefix(tmp_path, origin, backend, broad_root):
    root = origin.base.removesuffix("root/") if broad_root else origin.base
    name = "root/a" if broad_root else "a"
    config = job(tmp_path, origin, backend, base_url=root, url_list=None, download_files=[name])
    result = download_url_list(config)
    assert result["success"]
    assert (config.dest_path / name).read_bytes() == b"bytes"
    assert [request[1] for request in origin.requests] == ["/root/a"]


@pytest.mark.parametrize(
    "unsafe",
    [
        "../outside",
        "nested/../a",
        "./a",
        "/root/a",
        "//other.example/a",
        "a//b",
        "a/",
        "a\\b",
        "%2e%2e/a",
        "%252e%252e/a",
        "a\x00b",
        "a\nb",
        "https://other.example/root/a",
        "http://127.0.0.1/a",
        ".mirror-url-state/a",
    ],
)
@pytest.mark.integration
def test_invalid_later_input_blocks_entire_batch_and_preserves_existing_file(
    tmp_path, origin, unsafe
):
    config = job(tmp_path, origin, url_list=None, download_files=["valid", unsafe])
    config.dest_path.mkdir()
    kept = config.dest_path / "valid"
    kept.write_bytes(b"keep original")
    with pytest.raises((ConfigError, SecurityError, URLScopeError, ValueError)):
        download_url_list(config)
    assert not origin.requests
    assert kept.read_bytes() == b"keep original"
    assert not (config.dest_path / ".mirror-url-state").exists()


@pytest.mark.parametrize("entries", [["a", "a"], ["a", "absolute"], ["a", "nested/b"]])
@pytest.mark.integration
def test_aliases_and_out_of_suffix_inputs_are_rejected_before_requests(tmp_path, origin, entries):
    if entries[1] == "absolute":
        entries = ["a", origin.base + "a"]
    options = {"dir_suffix": "selected"} if entries[1] == "nested/b" else {}
    config = job(tmp_path, origin, url_list=None, download_files=entries, **options)
    with pytest.raises((ValueError, URLScopeError)):
        download_url_list(config)
    assert not origin.requests


@pytest.mark.integration
def test_relative_batch_uses_the_existing_parallel_range_engine(
    tmp_path, ranges, chunk_mode, backend
):
    config = configuration(
        tmp_path,
        ranges,
        chunk_mode,
        backend=backend,
        url_list=None,
        download_files=["a", "nested/b"],
    )
    result = download_url_list(config)
    assert result["success"] and result["files_downloaded"] == 2
    assert result["chunk_metrics"]["total_chunks"] == 6
    assert result["chunk_metrics"]["chunk_downloads"] == 6
    assert result["chunk_metrics"]["chunk_assemblies"] == 2
    for name in ("a", "nested/b"):
        assert (config.dest_path / name).read_bytes() == ranges.payload
    assert_no_scratch(config.dest_path)


@pytest.mark.parametrize("inputs", [["a", "nested/b"], ["absolute", "absolute-nested"]])
@pytest.mark.integration
def test_cli_multiple_positionals_keep_root_relative_layout(
    tmp_path, origin, inputs, monkeypatch, capsys
):
    if inputs[0] == "absolute":
        inputs = [origin.base + "a", origin.base + "nested/b"]
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            "--url",
            origin.base,
            *inputs,
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
        ],
    )
    cli.main()
    result = json.loads(capsys.readouterr().out)
    assert result["success"] and result["files_downloaded"] == 2
    assert (tmp_path / "dest/a").read_bytes() == b"bytes"
    assert (tmp_path / "dest/nested/b").read_bytes() == b"bytes"


@pytest.mark.integration
def test_cli_header_names_the_root_used_for_list_mapping(tmp_path, origin, monkeypatch, capsys):
    listing = tmp_path / "files.txt"
    listing.write_text("nested/a\n")
    monkeypatch.setattr(
        "sys.argv",
        [
            "mirror-url",
            "--mode",
            "download",
            "--url",
            origin.base,
            "--url-list",
            str(listing),
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
            "--print-logs",
        ],
    )
    cli.main()
    output = capsys.readouterr()
    assert "Remote root: " + origin.base.rstrip("/") in output.err
    assert (tmp_path / "dest/nested/a").read_bytes() == b"bytes"


@pytest.mark.integration
def test_same_parent_absolute_batch_keeps_shortcut_defaults(tmp_path, origin, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(cli.tempfile, "gettempdir", lambda: str(tmp_path / "logs"))
    monkeypatch.setattr(
        "sys.argv", ["mirror-url", "--mode", "download", origin.base + "a", origin.base + "b"]
    )
    cli.main()
    assert json.loads(capsys.readouterr().out)["files_downloaded"] == 2
    assert (tmp_path / "a").read_bytes() == (tmp_path / "b").read_bytes() == b"bytes"


@pytest.mark.parametrize("source", ["files", "list", "config-files"])
@pytest.mark.integration
def test_cli_source_selection_replaces_other_configured_sources(
    tmp_path, origin, monkeypatch, capsys, source
):
    config = job(tmp_path, origin)
    values = config.model_dump(mode="json")
    path = tmp_path / "job.json"
    argv = ["mirror-url", "--config", str(path)]
    if source == "files":
        argv += ["b", "nested/c"]
        expected = ["b", "nested/c"]
    elif source == "list":
        values.update(url_list=None, download_files=["b", "nested/c"])
        argv += ["--url-list", str(config.url_list)]
        expected = ["a"]
    else:
        values.update(url_list=None, download_files=["b", "nested/c"])
        expected = ["b", "nested/c"]
    path.write_text(json.dumps(values))
    monkeypatch.setattr("sys.argv", argv)
    cli.main()
    assert json.loads(capsys.readouterr().out)["files_downloaded"] == len(expected)
    assert {request[1] for request in origin.requests} == {"/root/" + name for name in expected}


@pytest.mark.parametrize(
    "arguments,message",
    [
        (["--mode", "download", "a"], "relative download files require --url"),
        (
            ["--mode", "download", "https://example.org/a", "https://example.org/nested/b"],
            "different directories require --url",
        ),
        (
            ["--mode", "download", "--url", "https://example.org/", "a", "--url-list", "files.txt"],
            "mutually exclusive",
        ),
        (["--mode", "mirror", "--url", "https://example.org/", "a"], "requires --mode download"),
    ],
)
def test_ambiguous_cli_sources_fail_before_creating_files(
    tmp_path, arguments, message, monkeypatch, capsys
):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", ["mirror-url", *arguments])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2 and message in capsys.readouterr().err
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "options",
    [
        {"download_files": [""]},
        {"download_files": ["a"], "download_url": "https://example.org/a"},
        {"download_files": ["a"], "url_list": "files.txt"},
    ],
)
def test_configuration_rejects_empty_files_or_multiple_sources(tmp_path, options):
    with pytest.raises(ConfigError):
        MirrorConfig(
            mode="download",
            base_url="https://example.org/",
            dest_path=tmp_path,
            log_path=tmp_path,
            **options,
        )
