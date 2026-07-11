# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [3.1.19] - 2026-07-11

### Fixed
- Spurious "Cleanup thread did not stop within timeout" and "Monitor
  thread did not stop within timeout" warnings on every shutdown. Both
  background threads used an uninterruptible `time.sleep(N)` (10s / 30s)
  in their loops, while `shutdown()` only waited 5s before warning — so
  a thread that had just started sleeping wouldn't notice the shutdown
  signal until its full interval elapsed, firing the warning on most
  runs (not a rare race) despite the thread being completely healthy.
  Both loops now wait on a `threading.Event` that `shutdown()` sets,
  waking them immediately instead of waiting out the sleep interval.

## [3.1.18] - 2026-07-10

### Fixed
- `--help` formatting: box banner ("USAGE GUIDE") was off-center and
  would drift with future version-string lengths; REGEX PATTERNS and
  PARALLEL DOWNLOAD OPTIONS comment/description columns were
  inconsistently aligned. All three now computed against the longest
  line in each block instead of hand-counted spacing.

## [3.1.17] - 2026-07-10

### Fixed
- **'--help' showing stale v3.1.14 and removed outdated benchmarks section.

## [3.1.16] - 2026-07-03

### Fixed
- **`--dry-run` silently created the target directory**: `PathSafety.safe_join()`
  unconditionally called `base.mkdir()` whenever the base directory didn't
  exist, with no way for a caller to opt out. Both
  `ScanMixin._get_local_path_from_url()` and `CleanupMixin`'s expected-files
  builders call `safe_join(self.target_dir, ...)` once per remote file, so
  during a dry run the very first file checked silently created the (empty)
  target directory on disk — even though the dry-run log had already
  reported it as "not created". Nothing was downloaded into it, but
  `--dry-run` was no longer side-effect-free. `safe_join()` now takes a
  `create_base: bool = True` flag; both call sites pass
  `create_base=not self.config.dry_run`.

### Added
- `tests/test_dry_run_no_side_effects.py` and three new cases in
  `tests/test_security.py` covering the `create_base` flag directly.

## [3.1.15] - 2026-07-03

### Fixed
- **`clean_obsolete` partial-scan guard**: `_discover_directories_bfs()` used
  to catch per-directory scan exceptions and silently substitute an empty
  file/subdir list for the failed directory, so a single transient error
  (timeout, connection reset, transient 5xx) while scanning one subdirectory
  caused `get_remote_files()` to return an incomplete-but-non-empty listing.
  `clean_obsolete()` had no way to tell that listing apart from a complete
  one, so every local file under the failed subtree was reported as
  obsolete and deleted or moved — even though it still existed on the
  remote. A failed directory scan now sets a `scan_incomplete` flag (reset
  at the start of each `get_remote_files()` run); `clean_obsolete()` checks
  it first and refuses to delete, move, or preview anything while it's set,
  logging a warning instead. Also raised the per-directory scan failure log
  level from `debug` to `warning` so the underlying error is no longer
  silent.

### Added
- `tests/test_cleanup_partial_scan.py`: regression coverage for the guard
  above (`test_clean_obsolete_skips_everything_when_scan_incomplete`) plus a
  sanity check that cleanup still runs normally on a complete scan
  (`test_clean_obsolete_still_runs_when_scan_complete`).

## [3.1.14] - 2026-06-25

### Fixed
- **Cache filename regression**: restored the base-URL hash suffix in the on-disk
  cache filename. v3.1.6 wrote `mirror_url_<suffix>_<hash>.json`, where `<hash>`
  is the first 16 hex chars of `sha256(base_url)`. A change in 3.1.13 dropped the
  hash, producing `mirror_url_<suffix>.json` — which collapses distinct base URLs
  that happen to share a directory suffix onto the same cache file. Filenames now
  match v3.1.6 again (e.g. `mirror_url_generic_kernels_112368ef5f2e84e4.json`).
  Existing hash-less caches are not auto-migrated; the first run after upgrading
  recreates the hash-named file and re-scans once.

## [3.1.13] - 2026-06-24

### Changed
- **Repackaged the single-file `mirror_url.py` (~15k lines) into a modular
  `src/mirror_url/` package** (30 modules across 7 dependency layers). The
  migration is behavior-preserving: code was moved verbatim, with class/function
  method sets verified identical to the original via AST comparison.
- Public API is now importable from the package root:
  `from mirror_url import MirrorURL, MirrorConfig, load_config_from_args, main`
  plus the exception hierarchy.
- Added a console entry point (`mirror-url`) and `python -m mirror_url`.

### Added
- Packaging and OSS scaffolding: `pyproject.toml` (src-layout, pinned deps,
  optional extras `fast`/`progress`/`monitor`/`all`, `dev` toolchain), `LICENSE`
  (MIT), `README.md`, `CONTRIBUTING.md`, this changelog, `.gitignore`,
  `.pre-commit-config.yaml`.
- Test suite (`pytest`): smoke, utility, security, and subsystem-integration
  tests (thread-safe primitives under load, circuit-breaker state machine,
  disk-backed set spill, config round-trip). 52 passing; a full end-to-end HTTP
  test is included but skipped pending a test-only SSRF bypass (see
  `tests/test_integration.py`).
- CI workflow (GitHub Actions): ruff + black + mypy, and pytest across
  Python 3.9–3.12.

### Fixed
- Removed a small number of provably-dead local assignments flagged by the
  linter (e.g. unused `domain`/`elapsed`/`rtt`/`results`/`shutdown_task`
  bindings). These were no-ops; runtime behavior is unchanged.
- Rewired `ConnectionManager`'s scope check to import `MirrorURL` from within the
  package (`.core`) instead of the original module-level self-import.

### Notes
- The legacy `mirror_url.py` is retained as a frozen reference and will be
  removed once downstreams have migrated to the package.
- `mypy` runs as an advisory signal (not a gate); it reports pre-existing
  annotation imprecision inherited from the original code. A dedicated typing
  pass is planned.
- Behavioral version remains **3.1.13** — this release is a structural
  repackaging, not a functional change.

---

Older history (pre-package) is recorded in the changelog block at the top of the
legacy `mirror_url.py`.
