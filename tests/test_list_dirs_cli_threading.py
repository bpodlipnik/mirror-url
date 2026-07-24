"""Regression test: --list-dirs must reach MirrorConfig from every place
cli.py constructs one.

This follows the exact convention established in
test_missing_files_cli_threading.py / test_no_etag_cli_threading.py,
written specifically to guard against repeating that bug: a new boolean
CLI flag has to be threaded through *three* separate spots in cli.py
(plus two spots in config.py) to actually take effect --

1. The plain-CLI path's direct ``MirrorConfig(...)`` constructor call
   (used whenever the user does NOT pass --config).
2. The ``--config`` branch's initial ``config_dict`` population from
   ``base_config`` (so setting it in the YAML file works).
3. The ``--config`` branch's "override with command line arguments"
   section (so passing the flag on the CLI *alongside* --config also
   works, not just setting it in the YAML file).

Missing any one of these makes the flag look like it works (argparse
accepts it, --help documents it) while silently having no effect in
that particular invocation style -- exactly what happened with
--missing-files and --no-etag before they were fixed.
"""

from __future__ import annotations

import inspect

from mirror_url import cli as cli_module
from mirror_url import config as config_module


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


def test_direct_mirrorconfig_constructor_includes_list_dirs():
    block = _direct_constructor_block()
    assert 'list_dirs=getattr(args, "list_dirs", False)' in block, (
        "--list-dirs is not threaded through the plain-CLI (no --config) "
        "MirrorConfig(...) constructor -- the flag would be silently ignored"
    )


def test_config_branch_populates_list_dirs_from_base_config():
    src = inspect.getsource(cli_module)
    assert '"list_dirs": getattr(base_config, "list_dirs", False)' in src, (
        "--list-dirs is not populated into config_dict from base_config in the "
        "--config (YAML) branch -- setting list_dirs in the YAML file would be "
        "silently ignored"
    )


def test_config_branch_has_cli_override_for_list_dirs():
    src = inspect.getsource(cli_module)
    assert (
        'if getattr(args, "list_dirs", False):\n                    config_dict["list_dirs"] = True'
        in src
    ), (
        "--list-dirs passed alongside --config has no CLI-override entry "
        "in config_dict -- only the YAML file's value would ever be used"
    )


def test_configschema_and_mirrorconfig_both_declare_list_dirs():
    schema_src = inspect.getsource(config_module.ConfigSchema)
    runtime_src = inspect.getsource(config_module.MirrorConfig)
    assert "list_dirs: bool = False" in schema_src, (
        "ConfigSchema (YAML validation model) is missing the list_dirs field -- "
        "a YAML config setting list_dirs would fail validation or be silently dropped"
    )
    assert "list_dirs: bool = False" in runtime_src, (
        "MirrorConfig (runtime model) is missing the list_dirs field"
    )


def test_load_config_from_args_includes_list_dirs():
    src = inspect.getsource(config_module)
    assert '"list_dirs": getattr(args, "list_dirs", False)' in src, (
        "load_config_from_args() does not thread --list-dirs through -- used by "
        "the benchmark config path, would silently ignore the flag there"
    )


def test_main_loop_dispatches_to_list_directories():
    src = inspect.getsource(cli_module)
    assert "mirror.list_directories()" in src, (
        "cli.py's main suffix-processing loop never calls mirror.list_directories() "
        "-- the flag would be parsed and threaded through config but never actually "
        "change what the tool does"
    )
