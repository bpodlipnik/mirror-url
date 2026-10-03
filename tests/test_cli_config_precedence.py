"""Behavioral tests: command-line options vs. ``--config`` YAML vs. defaults.

These drive the real ``main()`` with a stubbed ``MirrorURL`` that captures the
exact ``MirrorConfig`` main() built (no network, no filesystem sync), then
assert on that config.

Bugs these guard against (all pre-existing before v3.1.66):

* With ``--config``, a YAML value was silently overwritten by an argparse
  *default*: ``hasattr(args, "streaming_parallel")`` and
  ``args.adaptive_batch_processing is not None`` are always true, so
  ``streaming_parallel: true`` became ``False`` and
  ``adaptive_batch_processing: false`` became ``True``.
* The "override" logic used ``args.x != DEFAULT`` to mean "the user passed
  --x", so ``--workers 8`` (== the default) could not override a YAML
  ``workers: 4``.
* About two dozen options (``--max-retries``, ``--max-depth``, ``--stats``,
  ``--no-http2`` ...) had no override entry at all and were ignored whenever
  ``--config`` was used.
* ``--hash-algorithm`` was never read by main() -- ignored even without
  ``--config``.
* The hand-copied YAML -> config_dict block dropped ``health_check_port``,
  ``parallel_optimization_mode``, ``disable_rate_scaling``,
  ``use_dedicated_download_pool`` and ``use_shared_thread_pool``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pytest

from mirror_url import cli
from mirror_url.config import MirrorConfig
from mirror_url.enums import CleanupPolicy, ScanMode


class _Stop(BaseException):
    """Raised by the stub to abort main() right after the config is built."""


@pytest.fixture
def run_main(tmp_path, monkeypatch):
    """Return ``run(argv, yaml=None) -> MirrorConfig`` (the config main() built)."""
    captured: list = []

    class _StubMirror:
        def __init__(self, config, *args, **kwargs):
            captured.append(config)
            raise _Stop()

    monkeypatch.setattr(cli, "MirrorURL", _StubMirror)

    root = logging.getLogger()
    saved_handlers, saved_level = list(root.handlers), root.level

    def run(argv, yaml=None):
        captured.clear()
        if yaml is not None:
            cfg = tmp_path / "c.yaml"
            cfg.write_text(
                "base_url: https://example.com/files/\n"
                f"dest_path: {tmp_path / 'dest'}\n"
                f"log_path: {tmp_path / 'log'}\n" + yaml
            )
            full = ["--config", str(cfg), *argv]
        else:
            full = [
                "--url",
                "https://example.com/files/",
                "--dest-path",
                str(tmp_path / "dest"),
                "--log-path",
                str(tmp_path / "log"),
                *argv,
            ]
        monkeypatch.setattr(sys, "argv", ["mirror-url", *full])
        try:
            cli.main()
        except _Stop:
            pass
        assert captured, "main() exited before building a MirrorConfig"
        return captured[0]

    yield run

    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)


def _plain(value):
    """Normalise enums while preserving native Path comparisons."""
    return value.value if hasattr(value, "value") else value


# --------------------------------------------------------------------------
# 1. YAML values must survive when the flag is not on the command line
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "yaml, field, expected",
    [
        ("streaming_parallel: true\n", "streaming_parallel", True),
        ("parallel_downloads: true\n", "parallel_downloads", True),
        ("sequential_downloads: true\n", "sequential_downloads", True),
        ("adaptive_batch_processing: false\n", "adaptive_batch_processing", False),
        ("cache_html: false\n", "cache_html", False),
        ("async_metadata: false\n", "async_metadata", False),
        ("http2: false\n", "http2", False),
        ("security_validation: false\n", "security_validation", False),
        ("circuit_breaker_enabled: false\n", "circuit_breaker_enabled", False),
        ("max_retries: 9\n", "max_retries", 9),
        ("max_depth: 4\n", "max_depth", 4),
        ("hash_algorithm: sha256\n", "hash_algorithm", "sha256"),
        ("scan_mode: sequential\n", "scan_mode", "sequential"),
        ("symlink_mode: follow\n", "symlink_mode", "follow"),
        ("stats: true\n", "stats", True),
    ],
)
def test_yaml_value_survives_when_flag_absent(run_main, yaml, field, expected):
    cfg = run_main([], yaml)
    assert _plain(getattr(cfg, field)) == expected


@pytest.mark.parametrize(
    "yaml, field, expected",
    [
        ("health_check_port: 9191\n", "health_check_port", 9191),
        ("parallel_optimization_mode: aggressive\n", "parallel_optimization_mode", "aggressive"),
        ("disable_rate_scaling: true\n", "disable_rate_scaling", True),
        ("use_dedicated_download_pool: false\n", "use_dedicated_download_pool", False),
        ("use_shared_thread_pool: true\n", "use_shared_thread_pool", True),
    ],
)
def test_yaml_fields_formerly_dropped_are_kept(run_main, yaml, field, expected):
    """These MirrorConfig fields were missing from the hand-copied config_dict."""
    cfg = run_main([], yaml)
    assert getattr(cfg, field) == expected


def test_every_mirrorconfig_field_round_trips_from_yaml_when_no_flags(run_main):
    """No CLI option, non-default YAML: nothing may be reset to a default."""
    reference = run_main([], "")
    tweaks = {
        "workers": 3,
        "timeout": 61,
        "max_retries": 7,
        "retry_delay": 5,
        "max_depth": 6,
        "download_queue_size": 177,
        "health_check_port": 9001,
        "async_workers": 12,
        "initial_batch_size": 33,
        "max_batch_size": 444,
        "memory_cache_size": 5555,
        "max_chunks_per_file": 2,
        "max_parallel_chunks_total": 12,
        "max_concurrent_downloads": 4,
    }
    yaml = "".join(f"{k}: {v}\n" for k, v in tweaks.items())
    cfg = run_main([], yaml)
    for name, value in tweaks.items():
        assert getattr(cfg, name) == value, name
        assert getattr(reference, name) != value, f"tweak for {name} equals the default"


# --------------------------------------------------------------------------
# 2. An explicit flag must win -- even when it equals the parser default
# --------------------------------------------------------------------------


def test_explicit_flag_equal_to_default_still_overrides_yaml(run_main):
    cfg = run_main(["--workers", "8", "--timeout", "30"], "workers: 4\ntimeout: 99\n")
    assert (cfg.workers, cfg.timeout) == (8, 30)


def test_explicit_max_concurrent_downloads_equal_to_default_overrides_yaml(run_main):
    cfg = run_main(["--max-concurrent-downloads", "10"], "max_concurrent_downloads: 3\n")
    assert cfg.max_concurrent_downloads == 10


@pytest.mark.parametrize(
    "argv, yaml, field, expected",
    [
        (["--max-retries", "7"], "max_retries: 2\n", "max_retries", 7),
        (["--retry-delay", "9"], "retry_delay: 1\n", "retry_delay", 9),
        (["--max-depth", "3"], "max_depth: 20\n", "max_depth", 3),
        (["--hash-algorithm", "sha256"], "hash_algorithm: md5\n", "hash_algorithm", "sha256"),
        (["--no-security-validation"], "security_validation: true\n", "security_validation", False),
        (
            ["--no-circuit-breaker"],
            "circuit_breaker_enabled: true\n",
            "circuit_breaker_enabled",
            False,
        ),
        (
            ["--no-circuit-breaker-downloads"],
            "circuit_breaker_downloads: true\n",
            "circuit_breaker_downloads",
            False,
        ),
        (["--no-http2"], "http2: true\n", "http2", False),
        (["--no-adaptive-async"], "adaptive_async: true\n", "adaptive_async", False),
        (
            ["--no-adaptive-batch-processing"],
            "adaptive_batch_processing: true\n",
            "adaptive_batch_processing",
            False,
        ),
        (
            ["--no-content-hash"],
            "content_hash_small_files: true\n",
            "content_hash_small_files",
            False,
        ),
        (["--no-http2-pipelining"], "http2_pipelining: true\n", "http2_pipelining", False),
        (
            ["--no-connection-pool-prewarm"],
            "connection_pool_prewarm: true\n",
            "connection_pool_prewarm",
            False,
        ),
        (["--no-rget-list"], "no_rget_list: false\n", "no_rget_list", True),
        (["--force-rget-list"], "force_rget_list: false\n", "force_rget_list", True),
        (["--rget-list-max-age", "3"], "rget_list_max_age: 7\n", "rget_list_max_age", 3),
        (["--max-filename-len", "100"], "max_filename_len: 255\n", "max_filename_len", 100),
        (
            ["--download-queue-size", "150"],
            "download_queue_size: 1000\n",
            "download_queue_size",
            150,
        ),
        (["--async-workers", "3"], "async_workers: 20\n", "async_workers", 3),
        (["--parallel-threshold", "2"], "parallel_threshold: 10\n", "parallel_threshold", 2),
        (["--stats"], "stats: false\n", "stats", True),
        (["--progress-bar"], "progress_bar: false\n", "progress_bar", True),
        (["--metrics-json", "m.json"], "", "metrics_json", Path("m.json")),
        (["--health-check-port", "9999"], "health_check_port: 8080\n", "health_check_port", 9999),
        (["--max-symlink-depth", "2"], "max_symlink_depth: 5\n", "max_symlink_depth", 2),
        (["--cleanup", "preview"], "cleanup_policy: safe\n", "cleanup_policy", "preview"),
        (["--scan-mode", "async"], "scan_mode: sequential\n", "scan_mode", "async"),
        (["--cache-html"], "cache_html: false\n", "cache_html", True),
        (["--no-cache-html"], "cache_html: true\n", "cache_html", False),
    ],
)
def test_cli_flag_overrides_yaml(run_main, tmp_path, argv, yaml, field, expected):
    if isinstance(expected, Path):
        expected = tmp_path / expected
        argv = [*argv[:-1], str(expected)]
    cfg = run_main(argv, yaml)
    assert _plain(getattr(cfg, field)) == expected


def test_bare_filter_does_not_wipe_yaml_filters(run_main):
    """``--filter`` with no patterns (nargs='*') means "not given"."""
    cfg = run_main(["--filter"], "file_filters: ['.fits']\n")
    assert cfg.file_filters == [".fits"]


def test_filter_replaces_yaml_and_keeps_case(run_main):
    cfg = run_main(["--filter", ".FITS", "L1"], "file_filters: ['.png']\n")
    assert cfg.file_filters == [".FITS", "L1"]


@pytest.mark.parametrize("with_config", [False, True], ids=["no-config", "config"])
@pytest.mark.parametrize(
    "pattern, name, expected",
    [
        (r"^\D+\.fits$", "abc.fits", True),
        (r"^\D+\.fits$", "abc123.fits", False),
        (r"\S+_L1\.fits", "x_L1.fits", True),
        (r"^\w+\.fits\Z", "x.fits", True),
        (r"^[A-Z]+\.fits$", "ABC.fits", True),
        (".FITS", "img.fits", True),  # case-insensitive, matcher lowercases both sides
        ("20260619T073", "a_20260619T073111_b.fts", True),
    ],
)
def test_filter_regex_escapes_are_not_lowercased(run_main, with_config, pattern, name, expected):
    """main() used to lowercase --filter before storing it, turning the regex
    escapes ``\\D``/``\\S``/``\\W`` into ``\\d``/``\\s``/``\\w`` (inverting the
    match) and ``\\Z`` into the invalid ``\\z``. matches_filter() already
    compares case-insensitively, so the pattern must be stored verbatim."""
    from mirror_url._core.scan import ScanMixin

    class _Matcher(ScanMixin):
        def __init__(self, config):
            self.config = config

        def _get_filename_fast(self, url):
            return url.rsplit("/", 1)[-1]

    cfg = run_main(["--filter", pattern], "" if with_config else None)
    assert cfg.file_filters == [pattern]
    assert _Matcher(cfg).matches_filter("https://e.com/f/" + name) is expected


def test_url_from_cli_wins_and_trailing_slash_is_stripped(run_main):
    cfg = run_main(["--url", "https://other.example/x/"], "")
    assert cfg.base_url == "https://other.example/x"


# --------------------------------------------------------------------------
# 3. The three download modes are mutually exclusive
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "yaml, flag, expected",
    [
        ("parallel_downloads: true\n", "--sequential-downloads", (False, False, True)),
        ("sequential_downloads: true\n", "--parallel-downloads", (True, False, False)),
        ("streaming_parallel: true\n", "--parallel-downloads", (True, False, False)),
        ("parallel_downloads: true\n", "--streaming-parallel", (False, True, False)),
    ],
)
def test_cli_download_mode_replaces_yaml_mode(run_main, yaml, flag, expected):
    cfg = run_main([flag], yaml)
    assert (cfg.parallel_downloads, cfg.streaming_parallel, cfg.sequential_downloads) == expected


# --------------------------------------------------------------------------
# 4. --list-dirs / --list-files / --missing-files / --no-etag, YAML and CLI
#    (replaces the old tests that string-matched cli.py's source text)
# --------------------------------------------------------------------------


def test_list_dirs_and_files_from_yaml_survive(run_main):
    cfg = run_main([], "list_dirs: true\nlist_dirs_n: 5\n")
    assert (cfg.list_dirs, cfg.list_dirs_n) == (True, 5)
    cfg = run_main([], "list_files: true\nlist_files_n: 2\n")
    assert (cfg.list_files, cfg.list_files_n) == (True, 2)


def test_list_dirs_and_files_from_cli_override_yaml(run_main):
    cfg = run_main(["--list-dirs", "4"], "")
    assert (cfg.list_dirs, cfg.list_dirs_n) == (True, 4)
    cfg = run_main(["--list-files"], "")
    assert (cfg.list_files, cfg.list_files_n) == (True, 0)


@pytest.mark.parametrize(
    "flag, field", [("--missing-files", "missing_files"), ("--no-etag", "no_etag")]
)
def test_missing_files_and_no_etag_from_cli_and_yaml(run_main, flag, field):
    assert getattr(run_main([flag], ""), field) is True
    assert getattr(run_main([], f"{field}: true\n"), field) is True
    assert getattr(run_main([], ""), field) is False


# --------------------------------------------------------------------------
# 5. Without --config: every option that maps to a MirrorConfig field reaches it
# --------------------------------------------------------------------------

# (option, cli value, MirrorConfig field, expected value)
TYPED = [
    ("--max-chunks", "3", "max_chunks_per_file", 3),
    ("--min-chunk-size", "5", "min_chunk_size_mb", 5),
    ("--max-parallel-chunks", "12", "max_parallel_chunks_total", 12),
    ("--max-concurrent-downloads", "4", "max_concurrent_downloads", 4),
    ("--chunk-assembly-dir", "asm", "chunk_assembly_dir", Path("asm")),
    ("--chunk-timeout-multiplier", "2.5", "chunk_timeout_multiplier", 2.5),
    ("--workers", "5", "workers", 5),
    ("--timeout", "45", "timeout", 45),
    ("--max-retries", "6", "max_retries", 6),
    ("--retry-delay", "4", "retry_delay", 4),
    ("--request-delay", "0.5", "request_delay", 0.5),
    ("--bandwidth-limit", "2.5", "bandwidth_limit", 2.5),
    ("--cache-max-age", "11", "cache_max_age", 11),
    ("--html-cache-max-age", "12", "html_cache_max_age", 12),
    ("--hash-algorithm", "sha256", "hash_algorithm", "sha256"),
    ("--rget-list-max-age", "3", "rget_list_max_age", 3),
    ("--async-workers", "9", "async_workers", 9),
    ("--adaptive-start-concurrency", "6", "adaptive_start_concurrency", 6),
    ("--adaptive-error-threshold", "0.2", "adaptive_error_threshold", 0.2),
    ("--symlink-mode", "follow", "symlink_mode", "follow"),
    ("--max-symlink-depth", "3", "max_symlink_depth", 3),
    ("--max-symlinks-per-dir", "30", "max_symlinks_per_dir", 30),
    ("--symlink-bomb-threshold", "500", "symlink_bomb_threshold", 500),
    ("--metrics-json", "m.json", "metrics_json", Path("m.json")),
    ("--scan-mode", "sequential", "scan_mode", "sequential"),
    ("--parallel-threshold", "4", "parallel_threshold", 4),
    ("--max-depth", "3", "max_depth", 3),
    ("--max-filename-len", "100", "max_filename_len", 100),
    ("--download-queue-size", "150", "download_queue_size", 150),
    ("--initial-batch-size", "20", "initial_batch_size", 20),
    ("--max-batch-size", "300", "max_batch_size", 300),
    ("--target-batch-time", "2.5", "target_batch_time", 2.5),
    ("--memory-cache-size", "1234", "memory_cache_size", 1234),
    ("--disk-cache-dir", "dc", "disk_cache_dir", Path("dc")),
    ("--fs-cache-ttl", "9.5", "fs_cache_ttl", 9.5),
    ("--health-check-port", "9099", "health_check_port", 9099),
    ("--cleanup", "preview", "cleanup_policy", "preview"),
]

# Options that are covered by dedicated tests above, or are not config fields.
_SEPARATELY_TESTED = {
    "url", "dest_path", "log_path", "config", "dir_suffix", "filter", "exclude_dir",
    "list_dirs", "list_files", "log_file", "benchmark",
}  # fmt: skip


def _parser() -> argparse.ArgumentParser:
    """Grab main()'s real parser without running it."""
    holder = {}
    real = argparse.ArgumentParser.parse_args

    def grab(self, *a, **k):
        holder["p"] = self
        raise _Stop()

    argparse.ArgumentParser.parse_args = grab  # type: ignore[method-assign]
    old_argv = sys.argv
    sys.argv = ["mirror-url"]
    try:
        try:
            cli.main()
        except _Stop:
            pass
    finally:
        argparse.ArgumentParser.parse_args = real  # type: ignore[method-assign]
        sys.argv = old_argv
    return holder["p"]


def _option_actions():
    for a in _parser()._actions:
        if a.option_strings and a.dest not in _SEPARATELY_TESTED:
            if not isinstance(a, (argparse._HelpAction, argparse._VersionAction)):
                yield a


def test_typed_table_covers_every_valued_option():
    """Adding a valued CLI option without a row in TYPED fails here on purpose."""
    known = {opt for opt, *_ in TYPED}
    missing = []
    for a in _option_actions():
        if isinstance(a, (argparse._StoreTrueAction, argparse._StoreFalseAction)):
            continue
        if a.option_strings[0] not in known:
            missing.append(a.option_strings[0])
    assert not missing, f"add these options to TYPED in {__file__}: {missing}"


@pytest.mark.parametrize("opt, value, field, expected", TYPED, ids=[t[0] for t in TYPED])
def test_valued_option_reaches_config_without_config_file(
    run_main, tmp_path, opt, value, field, expected
):
    if isinstance(expected, Path):
        expected = tmp_path / expected
        value = str(expected)
    cfg = run_main([opt, value])
    assert _plain(getattr(cfg, field)) == expected


@pytest.mark.parametrize("opt, value, field, expected", TYPED, ids=[t[0] for t in TYPED])
def test_valued_option_reaches_config_with_config_file(
    run_main, tmp_path, opt, value, field, expected
):
    if isinstance(expected, Path):
        expected = tmp_path / expected
        value = str(expected)
    cfg = run_main([opt, value], "")
    assert _plain(getattr(cfg, field)) == expected


def _boolean_flags():
    out = []
    for a in _option_actions():
        if isinstance(a, argparse._StoreTrueAction) and a.default is False:
            out.append((a.option_strings[0], a.dest, True))
        elif isinstance(a, argparse._StoreFalseAction):
            out.append((a.option_strings[0], a.dest, False))
    return out


_KEY = {
    "parallel_downloads": "parallel_downloads",
    "streaming_parallel": "streaming_parallel",
    "sequential_downloads": "sequential_downloads",
}


@pytest.mark.parametrize(
    "flag, dest, expected", _boolean_flags(), ids=[f[0] for f in _boolean_flags()]
)
@pytest.mark.parametrize("with_config", [False, True], ids=["no-config", "config"])
def test_boolean_flag_reaches_config(run_main, flag, dest, expected, with_config, monkeypatch):
    if dest not in MirrorConfig.model_fields:
        pytest.skip(f"{flag} is not a MirrorConfig field")
    cfg = run_main([flag], "" if with_config else None)
    assert getattr(cfg, dest) is expected


# --------------------------------------------------------------------------
# 6. Unit tests for the explicit-flag detector
# --------------------------------------------------------------------------


def test_no_fast_parsing_fallback_is_preserved_without_lxml(run_main, monkeypatch):
    """Parser selection without lxml is independent of error-fallback policy."""
    monkeypatch.setattr("mirror_url.parsing.LXML_AVAILABLE", False)
    assert run_main(["--no-fast-parsing-fallback"]).fast_parsing_fallback is False


def test_explicit_cli_dests_reports_only_typed_options():
    p = _parser()
    assert cli._explicit_cli_dests(p, []) == set()
    assert cli._explicit_cli_dests(p, ["--workers", "8", "--stats", "--no-http2"]) == {
        "workers",
        "stats",
        "http2",
    }
    assert cli._explicit_cli_dests(p, ["--filter"]) == {"filter"}


def test_explicit_cli_dests_does_not_mutate_the_real_parser():
    p = _parser()
    cli._explicit_cli_dests(p, ["--workers", "8"])
    assert p.parse_args(["--url", "u"]).workers == 8  # default intact
    assert p.get_default("workers") == 8


def test_cli_overrides_maps_enums_and_paths():
    p = _parser()
    argv = ["--cleanup", "move", "--scan-mode", "async", "--url", "http://x/y/"]
    args = p.parse_args(argv)
    out = cli._cli_overrides(args, cli._explicit_cli_dests(p, argv))
    assert out["cleanup_policy"] is CleanupPolicy.MOVE
    assert out["scan_mode"] is ScanMode.ASYNC
    assert out["base_url"] == "http://x/y"


# --------------------------------------------------------------------------
# 7. load_config_from_args (public API) with a Namespace from the real parser
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "extra, field, expected",
    [
        ([], "cleanup_policy", "safe"),
        (["--cleanup", "delete"], "cleanup_policy", "delete"),
        (["--hash-algorithm", "sha256"], "hash_algorithm", "sha256"),
        ([], "max_depth", 50),
        (["--max-depth", "7"], "max_depth", 7),
        (["--list-dirs"], "max_depth", 1),
        (["--no-http2"], "http2", False),
    ],
)
def test_load_config_from_args_accepts_real_parser_namespace(tmp_path, extra, field, expected):
    """It used to raise AttributeError ('cleanup', because --cleanup uses
    default=SUPPRESS) or a pydantic error (max_depth=None)."""
    from mirror_url import load_config_from_args

    base = [
        "--url",
        "https://example.com/files/",
        "--dest-path",
        str(tmp_path / "d"),
        "--log-path",
        str(tmp_path / "l"),
    ]
    ns = _parser().parse_args([*base, *extra])
    cfg = load_config_from_args(ns)
    assert _plain(getattr(cfg, field)) == expected
