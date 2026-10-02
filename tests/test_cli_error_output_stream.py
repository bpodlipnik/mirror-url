"""CLI errors belong on stderr; valid parser selection must not emit errors."""

from __future__ import annotations

import inspect
import logging
import sys

import pytest

from mirror_url import cli


def test_config_error_print_targets_stderr():
    src = inspect.getsource(cli)
    assert "print(f\"Configuration error for {suf or 'ROOT'}: {e}\", file=sys.stderr)" in src, (
        "the ConfigError print() fallback must target stderr, or a "
        "stderr-only cron redirect will silently miss config errors"
    )


def test_generic_config_creation_error_print_targets_stderr():
    src = inspect.getsource(cli)
    assert "print(f\"Error creating config for {suf or 'ROOT'}: {e}\", file=sys.stderr)" in src, (
        "the generic config-creation-error print() fallback must target "
        "stderr, or a stderr-only cron redirect will silently miss it"
    )


def test_no_lxml_flag_does_not_emit_configuration_warning(monkeypatch, capsys):
    class Stop(BaseException):
        pass

    def capture(config, **kwargs):
        assert config.fast_parsing_fallback is False
        raise Stop()

    monkeypatch.setattr(logging.getLogger(), "level", logging.getLogger().level)
    monkeypatch.setattr("mirror_url.parsing.LXML_AVAILABLE", False)
    monkeypatch.setattr(cli, "MirrorURL", capture)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror-url",
            "--url",
            "https://example.com/root/",
            "--list-files",
            "--no-fast-parsing-fallback",
        ],
    )
    with pytest.raises(Stop):
        cli.main()
    output = capsys.readouterr()
    assert output.out == output.err == ""


def test_no_bare_stdout_print_calls_remain_in_cli_error_paths():
    """Belt-and-suspenders: none of the historical messages should still
    exist in their old, stdout-defaulting form anywhere in the file."""
    src = inspect.getsource(cli)
    assert "print(f\"Configuration error for {suf or 'ROOT'}: {e}\")\n" not in src
    assert "print(f\"Error creating config for {suf or 'ROOT'}: {e}\")\n" not in src
    assert 'print("WARNING: lxml not available, falling back to fast parser")\n' not in src
