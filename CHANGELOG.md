# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
