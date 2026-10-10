"""Literal URL-relative selectors and explicit local list inputs."""

from pathlib import Path
from urllib.parse import quote

import pytest
from pydantic import ValidationError

from mirror_url.config import MirrorConfig
from mirror_url.exceptions import ConfigError


def config(tmp_path, **options):
    return MirrorConfig(
        base_url="https://files.example/sdb/",
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "logs",
        **options,
    )


@pytest.mark.parametrize("suffix", ["", "soho/gen", "soho/lasco/monthly"])
def test_selectors_keep_the_same_url_identity_with_every_suffix(tmp_path, suffix):
    selected = ["soho/gen/file1", "soho/gen/file2", "soho/lasco/monthly/file3"]
    cfg = config(tmp_path, dir_suffix=suffix, check_files=selected)
    for path in selected:
        assert cfg.check_file_selected(cfg.base_url + "/" + path)
    assert not cfg.check_file_selected(cfg.base_url + "/file1")
    assert not cfg.check_file_selected(cfg.base_url + "/soho/gen/soho/gen/file1")
    assert not cfg.check_file_selected(cfg.base_url + "/soho/gen/file10")
    assert not cfg.check_file_selected("https://other.example/sdb/soho/gen/file1")
    assert not cfg.check_file_selected("https://files.example/sdb-other/soho/gen/file1")


@pytest.mark.parametrize(
    "name",
    ["dir/space name", "dir/é.bin", "dir/literal%20.bin", "dir/a;b", "dir/UPPER", "dir/a?b#c"],
)
def test_matching_uses_decoded_remote_names_not_sanitized_local_names(tmp_path, name):
    cfg = config(tmp_path, check_files=[name])
    assert cfg.check_file_selected(cfg.base_url + "/" + quote(name, safe="/"))
    assert not cfg.check_file_selected(cfg.base_url + "/dir/other")
    if name != name.lower():
        assert not cfg.check_file_selected(cfg.base_url + "/" + quote(name.lower(), safe="/"))


def test_explicit_lists_merge_deduplicate_and_ignore_comment_lines(tmp_path):
    listing = tmp_path / "selection.txt"
    listing.write_text(
        "\ufeff# selected files\n\nsoho/gen/file1\n  # comment\nsoho/gen/file2\nsoho/gen/file1\n",
        encoding="utf-8",
    )
    cfg = config(
        tmp_path, check_files=["soho/gen/file1", "@" + str(listing), "soho/lasco/monthly/file3"]
    )
    assert cfg.check_files == ["soho/gen/file1", "soho/gen/file2", "soho/lasco/monthly/file3"]
    listing.unlink()
    rebuilt = MirrorConfig(**cfg.model_dump())
    assert rebuilt.check_files == cfg.check_files


def test_txt_name_is_a_remote_selector_without_at_prefix(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("check-files.txt").write_text("different/file\n")
    cfg = config(tmp_path, check_files=["check-files.txt"])
    assert cfg.check_files == ["check-files.txt"]
    assert cfg.check_file_selected(cfg.base_url + "/check-files.txt")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/abs/file",
        "../outside",
        "a/../b",
        "a/./b",
        "a//b",
        "a/",
        "a\\b",
        "a\x00b",
        "a\x01b",
        "https://evil.example/file",
        "C:/file",
        "a/%2e%2e/b",
        "a/%252e%252e/b",
        "a/%00b",
    ],
)
def test_invalid_relative_selectors_are_rejected(tmp_path, path):
    with pytest.raises(ConfigError, match="relative to --url"):
        config(tmp_path, check_files=[path])


@pytest.mark.parametrize(
    "contents", [b"", b"# only comments\n", b"@nested.txt\n", b"../outside\n", b"\xff"]
)
def test_invalid_list_contents_are_rejected(tmp_path, contents):
    listing = tmp_path / "selection.txt"
    listing.write_bytes(contents)
    with pytest.raises(ConfigError):
        config(tmp_path, check_files=["@" + str(listing)])


@pytest.mark.parametrize("filename", ["", "absent.txt", "."])
def test_unreadable_list_reference_is_rejected(tmp_path, filename, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigError):
        config(tmp_path, check_files=["@" + filename])


@pytest.mark.parametrize("value", ["file1 file2", [123], {"file1": True}])
def test_configuration_requires_a_list_of_paths(tmp_path, value):
    with pytest.raises(ValidationError):
        config(tmp_path, check_files=value)


def test_selection_is_mirror_only_and_preserves_content_verification_conflict(tmp_path):
    with pytest.raises(ConfigError, match="exact URLs"):
        config(
            tmp_path, mode="download", download_url="https://files.example/sdb/a", check_files=["a"]
        )
    with pytest.raises(ConfigError, match="verify_content with missing_files"):
        config(tmp_path, missing_files=True, verify_content=True, check_files=["a"])


@pytest.mark.parametrize("from_config", [False, True])
def test_cli_reads_selection_file_once_for_all_suffixes(tmp_path, monkeypatch, from_config):
    import sys
    from types import SimpleNamespace

    from mirror_url import cli

    listing = tmp_path / "selection.txt"
    listing.write_text("soho/gen/a\nsoho/lasco/monthly/b\n")
    reads = []
    real_read = Path.read_text

    def read(path, *args, **kwargs):
        if path == listing:
            reads.append(path)
        return real_read(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    captured = []

    class Mirror:
        def __init__(self, cfg, **kwargs):
            captured.append(cfg)
            self.connection_manager = SimpleNamespace()
            self.connection_ok = True

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def install_signal_handlers(self):
            pass

        def sync(self):
            # Changing the input after the first suffix must not change the run.
            listing.write_text("different/file\n")
            return True

    monkeypatch.setattr(cli, "MirrorURL", Mirror)
    argv = ["mirror-url", "--dir-suffix", "soho/gen", "soho/lasco/monthly"]
    if from_config:
        yaml = tmp_path / "cfg.yaml"
        yaml.write_text(
            f"base_url: https://files.example/sdb/\ndest_path: {tmp_path / 'dest'}\nlog_path: {tmp_path / 'logs'}\nmissing_files: true\ncheck_files: ['@{listing}']\n"
        )
        argv += ["--config", str(yaml)]
    else:
        argv += [
            "--url",
            "https://files.example/sdb/",
            "--dest-path",
            str(tmp_path / "dest"),
            "--log-path",
            str(tmp_path / "logs"),
            "--missing-files",
            "--check-files",
            "@" + str(listing),
        ]
    monkeypatch.setattr(sys, "argv", argv)
    with pytest.raises(SystemExit) as exit:
        cli.main()
    assert exit.value.code == 0
    assert reads == [listing]
    assert [cfg.dir_suffix for cfg in captured] == ["soho/gen", "soho/lasco/monthly"]
    assert all(cfg.check_files == ["soho/gen/a", "soho/lasco/monthly/b"] for cfg in captured)


def test_invalid_cli_list_fails_before_destination_or_log_creation(tmp_path, monkeypatch):
    import sys

    from mirror_url import cli

    dest, logs = tmp_path / "dest", tmp_path / "logs"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            "https://files.example/",
            "--dest-path",
            str(dest),
            "--log-path",
            str(logs),
            "--check-files",
            "@" + str(tmp_path / "absent.txt"),
        ],
    )
    with pytest.raises(SystemExit) as exit:
        cli.main()
    assert exit.value.code == 2
    assert not dest.exists() and not logs.exists()


def test_malformed_url_selector_reports_configuration_error(tmp_path):
    with pytest.raises(ConfigError, match="Invalid check_files path"):
        config(tmp_path, check_files=["https://["])
