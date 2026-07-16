"""Regression test: CLI error-path print() fallbacks must target stderr.

Without --print-logs, mirror-url's informational logging only ever goes
to the file at --log-path -- never to stdout/stderr. That makes a
stderr-only cron redirect (``2>> errors.log``, deliberately not touching
stdout) an attractive way to catch crashes without duplicating the main
log file. But three print() fallbacks in cli.py (config-creation errors,
and the lxml-availability warning) defaulted to stdout, since bare
print() writes there unless told otherwise -- so a stderr-only redirect
would silently miss these genuine error/warning conditions, while an
unhandled Python exception (which always goes to stderr) would be
caught. The existing convention elsewhere in the codebase (_base.py's
log-directory-creation-failure fallback) already correctly used
``file=sys.stderr`` -- these three just hadn't followed it.

This test inspects the source directly for the exact print() call sites
rather than driving the full CLI (which would need real config files,
argument parsing, and triggering ConfigError/Exception paths deep inside
a large orchestration function) -- the defect is a missing `file=
sys.stderr` keyword argument on three specific, known call sites.
"""

from __future__ import annotations

import inspect

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


def test_lxml_fallback_warning_targets_stderr():
    src = inspect.getsource(cli)
    assert (
        'print("WARNING: lxml not available, falling back to fast parser", file=sys.stderr)' in src
    ), "the lxml-fallback warning must target stderr for the same reason"


def test_no_bare_stdout_print_calls_remain_in_cli_error_paths():
    """Belt-and-suspenders: none of the three fixed messages should still
    exist in their old, stdout-defaulting form anywhere in the file."""
    src = inspect.getsource(cli)
    assert "print(f\"Configuration error for {suf or 'ROOT'}: {e}\")\n" not in src
    assert "print(f\"Error creating config for {suf or 'ROOT'}: {e}\")\n" not in src
    assert 'print("WARNING: lxml not available, falling back to fast parser")\n' not in src
