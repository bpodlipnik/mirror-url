"""Regression test: --list-files must reach MirrorConfig from every place
cli.py constructs one.

Same convention as test_list_dirs_cli_threading.py, written for the same
reason: a new CLI flag has to be threaded through *three* separate spots
in cli.py (plus two spots in config.py) to actually take effect --

1. The plain-CLI path's direct ``MirrorConfig(...)`` constructor call
   (used whenever the user does NOT pass --config).
2. The ``--config`` branch's initial ``config_dict`` population from
   ``base_config`` (so setting it in the YAML file works).
3. The ``--config`` branch's "override with command line arguments"
   section (so passing the flag on the CLI *alongside* --config also
   works, not just setting it in the YAML file).

Missing any one of these makes the flag look like it works (argparse
accepts it, --help documents it) while silently having no effect in that
particular invocation style -- exactly what happened with --missing-files
and --no-etag before they were fixed, and what test_list_dirs_cli_threading.py
guards against for --list-dirs.

--list-files carries both a boolean (list_files) and an int (list_files_n,
from the optional N argument), so each of the checks below covers both.
"""

from __future__ import annotations

import inspect

from mirror_url import cli as cli_module
from mirror_url import config as config_module


def _direct_constructor_block() -> str:
    src = inspect.getsource(cli_module)
    marker = "suffix_config = MirrorConfig(\n"
    start = src.index(marker)
    window = src[start : start + 12000]
    end = window.index("\n                )\n")
    return window[:end]


def test_direct_mirrorconfig_constructor_includes_list_files():
    block = _direct_constructor_block()
    assert 'list_files=getattr(args, "list_files", None) is not None' in block, (
        "--list-files is not threaded through the plain-CLI (no --config) "
        "MirrorConfig(...) constructor -- the flag would be silently ignored"
    )
    assert 'list_files_n=getattr(args, "list_files", None) or 0' in block, (
        "--list-files N's value is not threaded through the plain-CLI "
        "MirrorConfig(...) constructor -- N would be silently dropped"
    )


def test_config_branch_populates_list_files_from_base_config():
    src = inspect.getsource(cli_module)
    assert '"list_files": getattr(base_config, "list_files", False)' in src, (
        "--list-files is not populated into config_dict from base_config in the "
        "--config (YAML) branch -- setting list_files in the YAML file would be "
        "silently ignored"
    )
    assert '"list_files_n": getattr(base_config, "list_files_n", 0)' in src, (
        "list_files_n is not populated into config_dict from base_config in the "
        "--config (YAML) branch"
    )


def test_config_branch_has_cli_override_for_list_files():
    src = inspect.getsource(cli_module)
    assert (
        'if getattr(args, "list_files", None) is not None:\n'
        '                    config_dict["list_files"] = True\n'
        '                    config_dict["list_files_n"] = args.list_files' in src
    ), (
        "--list-files passed alongside --config has no CLI-override entry "
        "in config_dict -- only the YAML file's value would ever be used"
    )


def test_configschema_and_mirrorconfig_both_declare_list_files():
    schema_src = inspect.getsource(config_module.ConfigSchema)
    runtime_src = inspect.getsource(config_module.MirrorConfig)
    for src, label in ((schema_src, "ConfigSchema"), (runtime_src, "MirrorConfig")):
        assert "list_files: bool = False" in src, (
            f"{label} is missing the list_files field -- a YAML config setting "
            "list_files would fail validation or be silently dropped"
        )
        assert "list_files_n: int = 0" in src, f"{label} is missing the list_files_n field"


def test_load_config_from_args_includes_list_files():
    src = inspect.getsource(config_module)
    assert '"list_files": getattr(args, "list_files", None) is not None' in src, (
        "load_config_from_args() does not thread --list-files through -- used by "
        "the benchmark config path, would silently ignore the flag there"
    )
    assert '"list_files_n": getattr(args, "list_files", None) or 0' in src, (
        "load_config_from_args() does not thread --list-files N through"
    )


def test_main_loop_dispatches_to_list_files():
    src = inspect.getsource(cli_module)
    assert "mirror.list_files()" in src, (
        "cli.py's main suffix-processing loop never calls mirror.list_files() "
        "-- the flag would be parsed and threaded through config but never "
        "actually change what the tool does"
    )


def test_list_dirs_and_list_files_are_mutually_exclusive():
    src = inspect.getsource(cli_module)
    assert 'getattr(args, "list_dirs", None) is not None' in src
    assert '"--list-dirs and --list-files are mutually exclusive"' in src, (
        "no mutual-exclusion guard found for --list-dirs / --list-files"
    )
    # Confirm both conditions are joined with `and` in the same guard, not
    # just present somewhere independently in the file.
    guard_start = src.index('"--list-dirs and --list-files are mutually exclusive"')
    preceding = src[max(0, guard_start - 400) : guard_start]
    assert "and" in preceding and 'getattr(args, "list_files", None) is not None' in preceding


def test_dest_and_log_path_defaults_cover_list_files_too():
    src = inspect.getsource(cli_module)
    marker = 'args.dest_path = Path(tempfile.gettempdir()) / "mirror-url-list-dirs"'
    assert marker in src, (
        "--list-files does not share --list-dirs's --dest-path/--log-path "
        "scratch-dir fallback -- a --list-files-only run without --config "
        "would hit the '--dest-path is required' error"
    )
    preceding = src[max(0, src.index(marker) - 700) : src.index(marker)]
    assert (
        "or" in preceding
        and 'getattr(args, "list_dirs", None) is not None' in preceding
        and 'getattr(args, "list_files", None) is not None' in preceding
    )
