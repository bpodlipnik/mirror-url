"""Regression test: --missing-files must reach MirrorConfig from every
place cli.py constructs one.

Bug: when --missing-files was added (v3.1.26), cli.py's config
construction was updated in the ``--config`` (YAML) branch's initial
``config_dict`` population (base_config's value flows through), but two
spots were missed:

1. The plain-CLI path's direct ``MirrorConfig(...)`` constructor call
   (used whenever the user does NOT pass --config) never listed
   ``missing_files=`` among its keyword arguments at all, so the field
   silently fell back to the pydantic default of False -- the flag was
   accepted by argparse but had no effect.
2. The ``--config`` branch's "override with command line arguments"
   section had no entry for the flag either, so passing
   --missing-files on the CLI *alongside* --config was also silently
   ignored (only setting it in the YAML file itself worked).

The bug was invisible on a first run (every file is missing regardless
of the flag), and only showed up on a second run against an
already-populated destination, where every existing file went through
a full freshness check again as if --missing-files had never been
passed.

This is a source-level structural check (following the existing
convention in test_missing_files.py's
test_async_check_one_has_the_same_missing_files_fast_path) rather than
a full CLI invocation, since driving cli.py's main() end-to-end
requires either live network or the SSRF-guard test bypass tracked in
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


def test_direct_mirrorconfig_constructor_includes_missing_files():
    block = _direct_constructor_block()
    assert 'missing_files=getattr(args, "missing_files", False)' in block, (
        "--missing-files is not threaded through the plain-CLI (no --config) "
        "MirrorConfig(...) constructor -- the flag would be silently ignored"
    )


def test_config_branch_has_cli_override_for_missing_files():
    src = inspect.getsource(cli_module)
    assert (
        'if getattr(args, "missing_files", False):\n                    config_dict["missing_files"] = True'
        in src
    ), (
        "--missing-files passed alongside --config has no CLI-override entry "
        "in config_dict -- only the YAML file's value would ever be used"
    )
