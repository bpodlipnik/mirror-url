"""Remote directory selectors retain literal paths and owned discovery policy."""

from __future__ import annotations

import argparse
import json
import logging
import socket
import sys
from contextlib import closing, nullcontext
from unittest.mock import Mock

import pytest

from mirror_url import MirrorConfig, cli
from mirror_url.destination_lock import DestinationLock
from mirror_url.directory_selection import compile_directory_patterns, resolve_directory_selection
from mirror_url.enums import CleanupPolicy
from mirror_url.exceptions import ConfigError, PathTraversalError
from mirror_url.security import SecurityValidator
from mirror_url.transport import SecureAsyncTransport, SecureTransport
from test_cli_config_precedence import run_main as run_main
from test_http_mirror_workflows import remote as remote

ROOT = "https://example.com/archive/"
_SECURE_REQUEST = SecureTransport.handle_request
_SECURE_ASYNC_REQUEST = SecureAsyncTransport.handle_async_request


@pytest.fixture
def directory_archive(remote, monkeypatch):
    # Both payload backends require a hostname. Pin only this fixture's test
    # hostname to its owned server, using the normal scoped transports.
    monkeypatch.setattr(SecureTransport, "handle_request", _SECURE_REQUEST)
    monkeypatch.setattr(SecureAsyncTransport, "handle_async_request", _SECURE_ASYNC_REQUEST)
    monkeypatch.setattr(SecurityValidator, "resolve_and_validate_hostname", lambda _: "127.0.0.1")
    monkeypatch.setattr(socket, "gethostbyname", lambda _: "127.0.0.1")
    remote.url = remote.url.replace("127.0.0.1", "files.example")
    return remote


@pytest.fixture
def parser(monkeypatch):
    captured = []

    class Captured(BaseException):
        pass

    def capture(self, *args, **kwargs):
        captured.append(self)
        raise Captured

    with monkeypatch.context() as patch:
        patch.setattr(argparse.ArgumentParser, "parse_args", capture)
        with pytest.raises(Captured):
            cli.main()
    return captured[0]


@pytest.mark.parametrize("flag", ["--dir", "--dir-suffix"])
def test_literal_flags_share_config_destination(parser, flag):
    parsed = parser.parse_args([flag, "one", "two/nested"])
    assert parsed.dir_suffix == ["one", "two/nested"]
    assert cli._explicit_cli_dests(parser, [flag, "one"]) == {"dir_suffix"}


def test_new_directory_names_are_in_one_help_section(parser):
    help_text = parser.format_help()
    assert "--dir PATH [PATH ...]" in help_text
    assert "--dir-regex PATTERN [PATTERN ...]" in help_text
    assert "--dir-suffix" not in help_text
    directory = next(group for group in parser._action_groups if group.title == "Directory Options")
    assert {"dir_suffix", "dir_regex"} <= {action.dest for action in directory._group_actions}


@pytest.mark.parametrize("flag", ["--dir", "--dir-suffix"])
@pytest.mark.parametrize("yaml", [None, "dir_suffix: old\nworkers: 3\n"])
def test_new_literal_flag_keeps_cli_and_config_precedence(run_main, flag, yaml):
    config = run_main([flag, "new/nested"], yaml=yaml)
    assert config.dir_suffix == "new/nested"
    if yaml:
        assert config.workers == 3


@pytest.mark.parametrize("pattern", ["", "[", "("])
def test_invalid_patterns_fail_without_discovery(pattern):
    with pytest.raises(ConfigError, match="--dir-regex"):
        compile_directory_patterns([pattern])


def test_regex_matching_is_case_sensitive_and_full_path():
    pattern = compile_directory_patterns([r"level_05/2610[0-9]{2}"])[0]
    assert pattern.fullmatch("level_05/261015")
    assert not pattern.fullmatch("LEVEL_05/261015")
    assert not pattern.fullmatch("other/level_05/261015")
    assert not pattern.fullmatch("level_05/261015/nested")


@pytest.fixture
def selection(tmp_path):
    def run(paths, patterns, *, incomplete=False, connected=True, depth=4):
        config = MirrorConfig(
            base_url=ROOT,
            dest_path=tmp_path / "dest",
            log_path=tmp_path / "logs",
            max_depth=depth,
            cleanup_policy=CleanupPolicy.DELETE,
        )
        mirror = Mock()
        mirror.__enter__ = Mock(return_value=mirror)
        mirror.__exit__ = Mock(return_value=False)
        mirror.connection_ok = connected
        mirror.scan_incomplete = incomplete
        mirror._discover_directories_bfs.return_value = (url for url in paths)
        mirror._destination_lock.operation.return_value = nullcontext()
        factory = Mock(return_value=mirror)
        try:
            result = resolve_directory_selection(config, patterns, mirror_factory=factory)
        finally:
            mirror.__exit__.assert_called_once()
        prepared = factory.call_args.args[0]
        assert prepared.dry_run and prepared.no_cache and not prepared.cache_html
        assert prepared.cleanup_policy == CleanupPolicy.SAFE_NO_DELETE
        assert prepared.dir_suffix == "" and prepared.use_shared_log
        assert prepared.metrics_json is None and prepared.chunk_assembly_dir is None
        assert not (tmp_path / "dest").exists()
        return result

    return run


def test_plan_unions_regexes_without_overlapping_roots(selection):
    result = selection(
        [ROOT, ROOT + "a/", ROOT + "a/sub/", ROOT + "ab/", ROOT + "z/"],
        [r"a(?:/sub)?", "ab", "a", "z"],
    )
    assert result == {"a": 3, "ab": 3, "z": 3}


def test_regex_never_implicitly_selects_root(selection):
    assert selection([ROOT, ROOT + "a/"], [".*"]) == {"a": 3}


def test_decoded_paths_and_depth_budget(selection):
    assert selection([ROOT, ROOT + "caf%C3%A9%20data/day/"], ["café data/day"], depth=2) == {
        "café data/day": 0
    }


def test_deep_link_cannot_bypass_path_depth_budget(selection):
    with pytest.raises(ConfigError, match="No directories matched"):
        selection([ROOT + "a/b/c/"], [".*"], depth=2)


@pytest.mark.parametrize("paths", [[], [ROOT]])
def test_no_match_never_falls_back_to_root(selection, paths):
    with pytest.raises(ConfigError, match="nothing was processed"):
        selection(paths, ["missing"])


@pytest.mark.parametrize(
    "options,message",
    [
        ({"incomplete": True}, "incomplete"),
        ({"connected": False}, "unavailable"),
    ],
)
def test_failed_discovery_does_not_return_partial_selection(selection, options, message):
    with pytest.raises(ConfigError, match=message):
        selection([ROOT + "a/"], ["a"], **options)


def test_outside_url_cannot_be_selected(selection):
    with pytest.raises(ConfigError, match="outside --url"):
        selection(["https://other.test/a/"], [".*"])


@pytest.mark.parametrize("path", ["CON", ".mirror-url-state", "foo..bar"])
def test_unsafe_selected_roots_fail_before_mirroring(selection, path):
    with pytest.raises(ConfigError, match="Unsafe directory selection"):
        selection([ROOT + path + "/"], [".*"])


def test_local_directory_aliases_are_rejected_before_mirroring(selection, monkeypatch):
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: False)
    with pytest.raises(ConfigError, match="collide"):
        selection([ROOT + "A/", ROOT + "a/sub/"], [".*"])


def test_existing_directory_symlink_is_rejected_before_mirroring(selection, tmp_path):
    dest = tmp_path / "dest"
    dest.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (dest / "alias").symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"Directory symlinks unavailable: {error}")
    with pytest.raises(PathTraversalError):
        selection([ROOT + "alias/"], ["alias"])


@pytest.mark.parametrize(
    "arguments",
    [
        ["--dir-regex", "["],
        ["--dir", "literal", "--dir-regex", ".*"],
        ["--dir-suffix", "literal", "--dir-regex", ".*"],
        ["--mode", "download", "--dir-regex", ".*"],
        ["--benchmark", "--dir-regex", ".*"],
    ],
)
def test_rejected_cli_selection_never_acquires_ownership(monkeypatch, capsys, arguments):
    ownership = Mock(side_effect=AssertionError("must not acquire ownership"))
    monkeypatch.setattr(cli, "run_ownership", ownership)
    monkeypatch.setattr(sys, "argv", ["mirror-url", "--url", ROOT, *arguments])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert "--dir-regex" in capsys.readouterr().err
    ownership.assert_not_called()


def invoke(monkeypatch, argv):
    logger = logging.getLogger()
    handlers, level = list(logger.handlers), logger.level
    monkeypatch.setattr(sys, "argv", ["mirror-url", *argv])
    try:
        with pytest.raises(SystemExit) as error:
            cli.main()
        return error.value.code
    finally:
        logger.handlers[:] = handlers
        logger.setLevel(level)


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
@pytest.mark.parametrize("from_config", [False, True])
def test_real_regex_selection_preserves_scope_depth_exclusions_and_cleanup(
    directory_archive, tmp_path, monkeypatch, backend, from_config
):
    remote = directory_archive
    remote.files.update(
        {
            "level/day1/a.bin": b"selected",
            "level/day1/deeper/skip.bin": b"too deep",
            "level/day2/b.bin": b"excluded",
            "level/day3/c.bin": b"unselected",
        }
    )
    dest, logs = tmp_path / "dest", tmp_path / "logs"
    for relative in [
        "level/day1/old.bin",
        "level/day1/deeper/keep.bin",
        "level/day2/keep.bin",
        "level/day3/keep.bin",
    ]:
        target = dest / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"old")
    values = {
        "base_url": remote.url,
        "dest_path": str(dest),
        "log_path": str(logs),
        "dir_suffix": "level/day3",
        "backend": backend,
        "max_depth": 2,
        "exclude_dirs": ["level/day2"],
        "cleanup_policy": "delete",
        "connection_pool_prewarm": False,
        "request_delay": 0,
        "requests_per_second": 0,
    }
    if from_config:
        file = tmp_path / "settings.json"
        file.write_text(json.dumps(values))
        argv = ["--config", str(file)]
    else:
        argv = [
            "--url",
            remote.url,
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--backend",
            backend,
            "--max-depth",
            "2",
            "--exclude-dir",
            "level/day2",
            "--cleanup",
            "delete",
            "--no-connection-pool-prewarm",
            "--request-delay",
            "0",
        ]
    argv += ["--dir-regex", r"level/day[12]"]
    assert invoke(monkeypatch, argv) == 0
    assert (dest / "level/day1/a.bin").read_bytes() == b"selected"
    assert not (dest / "level/day1/old.bin").exists()
    assert not (dest / "level/day1/deeper/skip.bin").exists()
    assert (dest / "level/day1/deeper/keep.bin").read_bytes() == b"old"
    assert (dest / "level/day2/keep.bin").read_bytes() == b"old"
    assert (dest / "level/day3/keep.bin").read_bytes() == b"old"
    bodies = [
        path for method, path, _ in remote.requests if method == "GET" and path.endswith(".bin")
    ]
    assert bodies == ["level/day1/a.bin"]
    assert not any(path.startswith("level/day2/") for _, path, _ in remote.requests)
    with closing(DestinationLock([dest, logs])):
        pass


@pytest.mark.integration
@pytest.mark.parametrize("backend", ["httpx", "aiohttp"])
def test_normal_mirror_directory_depth_includes_filename(
    directory_archive, tmp_path, monkeypatch, backend
):
    remote = directory_archive
    remote.files.update(
        {"parent/inside.bin": b"depth boundary", "parent/deeper/skip.bin": b"outside"}
    )
    dest = tmp_path / "dest"
    assert (
        invoke(
            monkeypatch,
            [
                "--url",
                remote.url,
                "--dest-path",
                str(dest),
                "--log-path",
                str(tmp_path / "logs"),
                "--backend",
                backend,
                "--max-depth",
                "1",
                "--no-connection-pool-prewarm",
            ],
        )
        == 0
    )
    assert (dest / "parent/inside.bin").read_bytes() == b"depth boundary"
    assert not (dest / "parent/deeper/skip.bin").exists()


@pytest.mark.integration
def test_regex_selection_owns_destination_before_discovery(
    directory_archive, tmp_path, monkeypatch
):
    dest = tmp_path / "dest"
    with closing(DestinationLock([dest / "busy"])):
        assert (
            invoke(
                monkeypatch,
                [
                    "--url",
                    directory_archive.url,
                    "--dest-path",
                    str(dest),
                    "--log-path",
                    str(tmp_path / "logs"),
                    "--dir-regex",
                    ".*",
                ],
            )
            == 1
        )
    assert not directory_archive.requests


@pytest.mark.integration
def test_incomplete_regex_discovery_never_downloads_or_cleans(
    directory_archive, tmp_path, monkeypatch
):
    remote = directory_archive
    remote.files.update({"a/file.bin": b"new", "broken/file.bin": b"unknown"})
    remote.failures["broken/"] = 503
    dest = tmp_path / "dest"
    dest.mkdir()
    sentinel = dest / "keep.bin"
    sentinel.write_bytes(b"must remain")
    assert (
        invoke(
            monkeypatch,
            [
                "--url",
                remote.url,
                "--dest-path",
                str(dest),
                "--log-path",
                str(tmp_path / "logs"),
                "--dir-regex",
                ".*",
                "--cleanup",
                "delete",
                "--no-connection-pool-prewarm",
                "--max-retries",
                "0",
            ],
        )
        == 1
    )
    assert sentinel.read_bytes() == b"must remain"
    assert not (dest / "a/file.bin").exists()
    assert not any(path.endswith(".bin") for _, path, _ in remote.requests)


@pytest.mark.integration
def test_no_regex_match_does_not_mirror_root(directory_archive, tmp_path, monkeypatch, capsys):
    remote = directory_archive
    remote.files["a/file.bin"] = b"outside selection"
    dest = tmp_path / "dest"
    assert (
        invoke(
            monkeypatch,
            [
                "--url",
                remote.url,
                "--dest-path",
                str(dest),
                "--log-path",
                str(tmp_path / "logs"),
                "--dir-regex",
                "missing",
                "--no-connection-pool-prewarm",
            ],
        )
        == 1
    )
    assert "No directories matched" in capsys.readouterr().err
    assert not dest.exists()


def test_empty_selector_never_starts_discovery(tmp_path):
    factory = Mock()
    config = MirrorConfig(base_url=ROOT, dest_path=tmp_path / "dest", log_path=tmp_path / "logs")
    with pytest.raises(ConfigError, match="at least one pattern"):
        resolve_directory_selection(config, [], mirror_factory=factory)
    factory.assert_not_called()


def test_interrupted_selection_closes_discovery_before_releasing_ownership(tmp_path):
    from contextlib import contextmanager

    events = []

    @contextmanager
    def lease():
        events.append("leased")
        try:
            yield
        finally:
            events.append("released")

    def discovery():
        try:
            yield "https://outside.test/escape/"
        finally:
            events.append("discovery drained")

    mirror = Mock()
    mirror.__enter__ = Mock(return_value=mirror)
    mirror.__exit__ = Mock(side_effect=lambda *args: events.append("closed"))
    mirror.connection_ok = True
    mirror._destination_lock.operation.return_value = lease()
    mirror._discover_directories_bfs.return_value = discovery()
    config = MirrorConfig(base_url=ROOT, dest_path=tmp_path / "dest", log_path=tmp_path / "logs")
    with pytest.raises(ConfigError, match="outside --url"):
        resolve_directory_selection(config, [".*"], mirror_factory=Mock(return_value=mirror))
    assert events == ["leased", "discovery drained", "released", "closed"]


def test_selection_requires_owned_discovery(tmp_path):
    mirror = Mock()
    mirror.__enter__ = Mock(return_value=mirror)
    mirror.__exit__ = Mock(return_value=False)
    mirror._destination_lock = None
    config = MirrorConfig(base_url=ROOT, dest_path=tmp_path / "dest", log_path=tmp_path / "logs")
    with pytest.raises(ConfigError, match="ownership"):
        resolve_directory_selection(config, [".*"], mirror_factory=Mock(return_value=mirror))
    mirror._discover_directories_bfs.assert_not_called()


@pytest.mark.integration
@pytest.mark.parametrize("listing", ["--list-dirs", "--list-files"])
def test_listing_modes_preview_the_same_selected_roots(
    directory_archive, tmp_path, monkeypatch, capsys, listing
):
    remote = directory_archive
    remote.files.update(
        {"level/day1/a.bin": b"a", "level/day2/b.bin": b"b", "level/day3/c.txt": b"c"}
    )
    assert (
        invoke(
            monkeypatch,
            [
                "--url",
                remote.url,
                "--dest-path",
                str(tmp_path / "dest"),
                "--log-path",
                str(tmp_path / "logs"),
                "--dir-regex",
                r"level/day[13]",
                listing,
                "--max-depth",
                "2",
                "--filter",
                ".bin",
                "--no-connection-pool-prewarm",
            ],
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "level/day1\t" in output and "day2" not in output
    if listing == "--list-dirs":
        assert "level/day3\t" in output and "# Directories" in output
    else:
        assert "a.bin" in output and "c.txt" not in output and "# Files" in output
    assert not any(path.endswith((".bin", ".txt")) for _, path, _ in remote.requests)
