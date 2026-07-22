"""Regression test: --no-etag must reach MirrorConfig from every place
cli.py constructs one.

Bug: the same defect as --missing-files (see
test_missing_files_cli_threading.py), pre-existing and unrelated to that
feature's addition -- discovered as a side effect while fixing it. Two
spots in cli.py never threaded --no-etag through:

1. The plain-CLI path's direct ``MirrorConfig(...)`` constructor call
   (used whenever the user does NOT pass --config) never listed
   ``no_etag=`` among its keyword arguments at all, so the field
   silently fell back to the pydantic default of False -- the flag was
   accepted by argparse but had no effect.
2. The ``--config`` branch's "override with command line arguments"
   section had no entry for the flag either, so passing --no-etag on
   the CLI *alongside* --config was also silently ignored (only
   setting it in the YAML file itself worked).

This is a source-level structural check (same approach as
test_missing_files_cli_threading.py) rather than a full CLI
invocation, since driving cli.py's main() end-to-end requires either
live network or the SSRF-guard test bypass tracked in
test_integration.py.
"""

from __future__ import annotations

import inspect

from mirror_url import cli as cli_module


def _direct_constructor_block() -> str:
    """Return the source of the plain-CLI (no --config) MirrorConfig(...)
    call -- the ``else:`` branch paired with ``suffix_config =
    MirrorConfig.from_dict(config_dict, ...)``."""
    src = inspect.getsource(cli_module)
    marker = "suffix_config = MirrorConfig(\n"
    start = src.index(marker)
    # The constructor call is a single (long) statement; grab a generous
    # window and cut it off at the matching top-level closing paren line.
    window = src[start : start + 12000]
    end = window.index("\n                )\n")
    return window[:end]


def test_direct_mirrorconfig_constructor_includes_no_etag():
    block = _direct_constructor_block()
    assert 'no_etag=getattr(args, "no_etag", False)' in block, (
        "--no-etag is not threaded through the plain-CLI (no --config) "
        "MirrorConfig(...) constructor -- the flag would be silently ignored"
    )


def test_config_branch_has_cli_override_for_no_etag():
    src = inspect.getsource(cli_module)
    assert (
        'if getattr(args, "no_etag", False):\n                    config_dict["no_etag"] = True'
        in src
    ), (
        "--no-etag passed alongside --config has no CLI-override entry "
        "in config_dict -- only the YAML file's value would ever be used"
    )
