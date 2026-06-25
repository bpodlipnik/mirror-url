# MirrorURL — Developer Guide

This guide is for **contributors** working on the MirrorURL codebase itself. It
is a self-contained architecture deep-dive: how the package is structured, why it
is layered the way it is, how the `MirrorURL` orchestrator is composed, how data
flows through a run, and how to extend the system without breaking its invariants.

If you only want to *use* MirrorURL (install, CLI, config, Python API), read
[`USER_GUIDE.md`](./USER_GUIDE.md) instead. For day-to-day contribution mechanics
(branching, PR etiquette) see [`CONTRIBUTING.md`](../CONTRIBUTING.md); this guide
repeats the essentials so you can work from it alone.

- **Package:** `mirror_url` (src-layout under `src/`)
- **Version:** 3.1.14
- **Python:** 3.9 – 3.12
- **Runtime deps:** `httpx`, `pydantic` v2, `PyYAML` (optional: `stringzilla`,
  `lxml`, `tqdm`, `psutil`)

---

## Table of contents

- [Background: the monolith and the refactor](#background-the-monolith-and-the-refactor)
- [Design principles](#design-principles)
- [Repository layout](#repository-layout)
- [The dependency-layer architecture](#the-dependency-layer-architecture)
- [Module reference (by layer)](#module-reference-by-layer)
- [The MirrorURL orchestrator and its mixins](#the-mirrorurl-orchestrator-and-its-mixins)
- [Runtime data flow: anatomy of a sync()](#runtime-data-flow-anatomy-of-a-sync)
- [The configuration system](#the-configuration-system)
- [Subsystem deep-dives](#subsystem-deep-dives)
- [Extension recipes](#extension-recipes)
- [Coding conventions](#coding-conventions)
- [Testing](#testing)
- [Build, lint, and type-check](#build-lint-and-type-check)
- [Release process](#release-process)
- [Known hazards and gotchas](#known-hazards-and-gotchas)
- [Quick "where do I find…" map](#quick-where-do-i-find-map)

---

## Background: the monolith and the refactor

MirrorURL began as a single `mirror_url.py` of ~15,000 lines containing ~70
classes and ~25 module-level functions. It was split into the modular
`src/mirror_url/` package (30 modules across 7 dependency layers) by a
**behavior-preserving** migration: code was relocated verbatim and class/function
method sets were verified identical to the original via AST comparison. Logic
changes were kept out of the migration and made only in separate, reviewable
commits.

Two consequences shape how you should work in this codebase:

1. **Verbatim heritage.** Much of the code is a faithful port of working,
   audited, sometimes idiosyncratic logic (including hard-won bug fixes recorded
   in the changelog). When touching migrated code, prefer surgical changes over
   "cleanups" — the original behavior is the contract.
2. **Python 3.9 baseline.** The package still supports 3.9, so it uses classic
   typing (`Dict`, `Optional`, `List`) and `from __future__ import annotations`
   rather than 3.10+ syntax. Lint rules that would modernize syntax (pyupgrade,
   most of SIM) are intentionally disabled — see [Coding conventions](#coding-conventions).

The legacy `mirror_url.py` is retained as a frozen reference and is excluded from
lint, type-checking, and packaging. It will be deleted once downstreams have
migrated.

---

## Design principles

The package is built on four rules. Internalize them before making structural
changes — most review feedback traces back to one of these.

1. **Behavior-preserving by default.** Relocation and refactoring must not change
   runtime behavior. Functional fixes are separate commits with their own tests
   and changelog entries.
2. **One concern per module.** Each module is independently readable and
   testable. If a change makes a module "about two things," that is a signal to
   split it.
3. **Acyclic dependency layering.** A module may import only from *strictly
   lower* layers (plus stdlib / third-party). No sideways cycles, no upward
   imports. This is the property that keeps the package importable and testable
   in isolation. See [the layer architecture](#the-dependency-layer-architecture).
4. **Small, verifiable steps.** Keep the fast test lane green after every change.

---

## Repository layout

```
mirror-url/
├── src/mirror_url/          # the package (30 modules, dependency-layered)
│   ├── __init__.py          # public API re-exports
│   ├── __main__.py          # `python -m mirror_url`
│   ├── _version.py          # __version__, __author__  (one of two version sources)
│   ├── cli.py               # argument parsing, shared logging, main()
│   ├── core.py              # MirrorURL — composed from the _core mixins
│   ├── _core/               # the MirrorURL class, split into mixins (private)
│   │   ├── _base.py         # __init__, shared state, lifecycle, logging
│   │   ├── urls.py          # URL scheme/scope validation, path helpers
│   │   ├── scan.py          # remote discovery (BFS), filtering
│   │   ├── compare.py       # is-up-to-date checks (sync + async)
│   │   ├── downloads.py     # per-file download orchestration
│   │   ├── cleanup.py       # obsolete-file cleanup policies
│   │   └── report.py        # sync() entry, summaries, benchmark
│   ├── config.py            # ConfigSchema, MirrorConfig, load_config_from_args
│   ├── … (subsystem modules, see the module reference)
├── tests/                   # pytest suite (smoke, utils, security, subsystems, integration)
├── docs/                    # USER_GUIDE, DEVELOPER_GUIDE (this file), HTML renders
├── mirror_url.py            # legacy monolith (frozen reference, pending removal)
├── pyproject.toml           # packaging, deps, tool config  (the other version source)
├── REFACTORING_PLAN.md      # the migration plan + module/line map
├── CHANGELOG.md             # Keep a Changelog format
└── .github/workflows/       # ci.yml (lint+test matrix), release.yml (tag → build → PyPI)
```

---

## The dependency-layer architecture

Every module is assigned to a layer. **Imports only point downward.** This is the
single most important structural invariant in the project.

```
Layer 0  _version · compat · constants · exceptions · enums
Layer 1  models · decorators · utils
Layer 2  primitives · parsing · security
Layer 3  transport · storage · circuit_breaker · rate_limiter · queue
Layer 4  metrics · progress · monitoring · connection · async_connection · concurrency · cache
Layer 5  download · scanner · health · config · tuner
Layer 6  core   (MirrorURL — the only place that wires many subsystems together)
Layer 7  cli  →  __main__ / console entry point
```

**The rule:** a module in layer *N* may import from layers *0…N-1*, from the
standard library, and from third-party deps — never from layer *N* in a cycle,
and never from a higher layer. `core.py` (layer 6) is the only module permitted
to compose many layer-4/5 subsystems at once; that is its job.

**Why it matters for you:**

- It keeps the package importable at every step and lets you unit-test low layers
  with zero setup (no network, no config).
- If you find yourself wanting to import "upward" (e.g. a layer-3 module needing
  something from `core`), that is almost always a design smell — the dependency
  belongs lower, or should be injected as a parameter/callback.
- The one historical exception is documented: the monolith did
  `from mirror_url import MirrorURL` inside `ConnectionManager`'s scope check.
  After packaging this became an intra-package import of `.core`. Avoid
  reintroducing this pattern; prefer dependency injection.

To sanity-check the graph after a change, you can compile every module and import
the package; a broken layer shows up as an `ImportError` or a circular-import
failure immediately.

---

## Module reference (by layer)

**Layer 0 — foundations (no intra-package imports).**

- `_version.py` — `__version__`, `__author__`. One of the two places the version
  lives (the other is `pyproject.toml`); they must stay in sync (a test enforces
  it). `__version__` also flows into the cache file's `version_code` and the
  `--version` output.
- `compat.py` — optional-dependency flags (`STRINGZILLA_AVAILABLE`,
  `TQDM_AVAILABLE`, `LXML_AVAILABLE`, `PSUTIL_AVAILABLE`) and the StringZilla
  `Str` fallback. **Import the flag, not the try/except** — these are centralized
  here precisely so the rest of the code never re-implements the probe.
- `constants.py` — all tuning constants and static tables (`DEFAULT_*`, cache/
  scan/async/safety limits, `KNOWN_THROTTLED_DOMAINS`, `WINDOWS_RESERVED_NAMES`).
- `exceptions.py` — the `MirrorError` hierarchy (~19 classes). New error types go
  here, subclassing the closest existing base.
- `enums.py` — `LogLevel`, `ScanMode`, `CleanupPolicy`, `DownloadPriority`,
  `CircuitBreakerState`, `MemoryPressure`, `ConcurrencyType`, `DownloadMethod`.

**Layer 1 — pure helpers.**

- `models.py` — dataclasses: `DownloadTask`, `ServerProfile`, `HealthStatus`,
  `ChunkInfo`, `ParallelFileDownload`.
- `decorators.py` — `retry_with_backoff`, `log_performance`.
- `utils.py` — formatting (`format_bytes`, `format_duration`), URL helpers
  (`normalize_url_path`, `sanitize_url_for_log`, `safe_url_encode`,
  `normalize_etag`), hashing (`compute_file_hash`), cache validation
  (`_validate_and_sanitize_cache`), and process-level log bookkeeping.

**Layer 2 — stateless/low-state building blocks.**

- `primitives.py` — `LRUCache`, `AtomicCounter`, `AtomicSize` (thread-safe).
- `parsing.py` — `extract_links_fast`, `should_use_fast_parser`,
  `AdaptiveBatchProcessor` (HTML directory-listing parsing, lxml/stringzilla
  accelerated when available).
- `security.py` — `SymlinkTracker`, `SecurityValidator`, `PathSafety`,
  `FastURLValidator` (path-traversal, symlink-bomb, and filename defenses).

**Layer 3 — transport & resilience primitives.**

- `transport.py` — `SecureTransport`, `SecureAsyncTransport`: httpx transports
  that block loopback/private IPs (SSRF hardening). Both honor a `test_mode` flag
  used to relax the guard in tests.
- `storage.py` — `FileSystemCache`, `DiskBackedSet` (memory-bounded set that
  spills to disk).
- `circuit_breaker.py` — `CircuitBreaker`, `AsyncCircuitBreaker`,
  `ChunkCircuitBreaker`, `CircuitBreakerManager` (per-domain). Keep each base and
  its subclasses in this one module.
- `rate_limiter.py` — `BandwidthLimiter`, `RateLimiter`, `PerIPRateLimiter`,
  `ChunkAwareRateLimiter`.
- `queue.py` — `DownloadQueue`.

**Layer 4 — managers & observability.**

- `metrics.py` — `MetricsCollector` (thread-safe counters/aggregates; emits the
  metrics JSON).
- `progress.py` — `ProgressTracker`, `MultiLevelProgress`.
- `monitoring.py` — `MemoryMonitor`, `DiskSpaceManager`, `PerformanceMonitor`.
- `connection.py` — `ConnectionPool`, `ConnectionManager` (synchronous request
  path, retries, redirects-preserving-headers).
- `async_connection.py` — `AsyncConnectionManager`, `AdaptiveAsyncManager`
  (self-tuning concurrency), `AsyncTaskManager` (async metadata checks).
- `concurrency.py` — `UnifiedConcurrencyManager` (single global cap across sync/
  async/parallel thread pools).
- `cache.py` — `CacheManager` (the on-disk JSON cache: load/validate/save,
  schema-version checks, corrupted-file backup).

**Layer 5 — feature engines & config.**

- `download.py` — `ParallelDownloadManager` (chunked/streaming/parallel download
  engine, including `auto_select_method`) and `PartialDownloadManager` (resume).
- `scanner.py` — `DirectoryScanner`.
- `health.py` — `HealthCheckHandler`, `HealthCheckServer`, `HealthChecker`
  (optional HTTP health endpoint).
- `config.py` — `ConfigSchema` (file-facing pydantic schema), `MirrorConfig`
  (the runtime config object), `validate_config_file`, `expand_env_vars`,
  `load_config_from_args`.
- `tuner.py` — `AutoConcurrencyTuner`.

**Layer 6 — orchestration.**

- `core.py` + `_core/` — the `MirrorURL` class. See the next section.

**Layer 7 — entry point.**

- `cli.py` — `add_parallel_arguments`, `setup_shared_logging`, `main`.
- `__main__.py` — thin wrapper so `python -m mirror_url` calls `cli.main`.

---

## The MirrorURL orchestrator and its mixins

`MirrorURL` was ~3,600 lines as a single class. It is now composed from focused
**mixins** under the private `_core/` subpackage. `core.py` is a thin composer:

```python
class MirrorURL(
    UrlMixin,        # _core/urls.py
    ScanMixin,       # _core/scan.py
    CompareMixin,    # _core/compare.py
    DownloadMixin,   # _core/downloads.py
    CleanupMixin,    # _core/cleanup.py
    ReportMixin,     # _core/report.py
    _MirrorBase,     # _core/_base.py — __init__ + shared state, listed LAST
):
    ...
```

**Why this is safe and unambiguous:** every method is defined in exactly one
mixin, so the MRO never has to disambiguate. `_MirrorBase` is listed **last** so
that it sits at the base of the MRO and owns `__init__` plus all shared instance
state; the feature mixins are "above" it and call into the state it sets up.
Behavior is identical to the pre-split class — `from mirror_url.core import
MirrorURL` is unchanged for callers.

**Responsibilities and key methods per mixin:**

| Mixin (`_core/…`) | Responsibility | Representative methods |
|---|---|---|
| `_MirrorBase` (`_base.py`) | Construction, shared state, lifecycle, logging, connection bring-up, the on-disk caches, disk-space checks | `__init__`, `__enter__`/`__exit__`, `cleanup`, `setup_logging`, `test_connection`, `_warm_up_connections`, `get_html_cache`/`set_html_cache`, `get_file_metadata`/`save_file_metadata`, `handle_memory_pressure`, `_get_cached_filename`, `check_disk_space` |
| `UrlMixin` (`urls.py`) | URL scheme/scope validation, path extraction | `_validate_url_scheme*`, `_is_url_within_scope`, `_is_within_target_scope`, `_is_dir_excluded`, `_get_target_base_url`, `_parse_url_cached`, `_get_url_path_fast`, `_get_filename_fast` |
| `ScanMixin` (`scan.py`) | Remote discovery, filtering, symlink tracking | `get_remote_files`, `_discover_directories_bfs`, `matches_filter`, `get_directory_signature`, `is_symlink`/`record_symlink`, `_get_local_path_from_url` |
| `CompareMixin` (`compare.py`) | "Is the local copy up to date?" — size/timestamp/ETag/hash, sync and async | `file_exists_and_up_to_date`, `_check_files_sync`, `_check_files_async`, `check_file`, `check_one`, `get_remote_timestamp`, `get_directory_size` |
| `DownloadMixin` (`downloads.py`) | Per-file download orchestration (delegates to the `download.py` engines) | `download_file_with_resume`, `_download_file_single` |
| `CleanupMixin` (`cleanup.py`) | Removing/moving local files no longer present remotely | `clean_obsolete`, `_count_obsolete_files` |
| `ReportMixin` (`report.py`) | The top-level `sync()` driver, summaries, benchmarking | `sync`, `async_warm_up_worker`, `_print_early_exit_summary`, `benchmark` |

**Working rule:** when you add a method to `MirrorURL`, put it in the mixin whose
responsibility it matches, and keep shared attributes initialized in
`_MirrorBase.__init__`. Don't add a second `__init__` to a feature mixin.

---

## Runtime data flow: anatomy of a sync()

A full mirror run is driven by `ReportMixin.sync()`. The high-level path:

1. **Construction (`_MirrorBase.__init__`).** Parse the base URL, compute the
   destination/target paths, build the cache file path
   (`mirror_url_<suffix>_<hash>.json`, where the hash is the first 16 hex chars of
   `sha256(base_url)` — this disambiguates different base URLs that share a
   directory suffix), set up logging, and instantiate the subsystem managers
   (connection, async, concurrency, metrics, circuit breakers, rate limiter,
   caches).
2. **Connect (`test_connection`, `_warm_up_connections`).** Validate
   reachability and resolve the target scope. Until the target is resolved,
   scope checks fall back to the base URL (so the scanner doesn't drop every
   subdirectory).
3. **Scan (`ScanMixin.get_remote_files`).** Breadth-first discovery of the remote
   tree, honoring `max_depth`, `exclude_dirs`, scope enforcement, and a visited
   set (cycle-safe). Directory listings are parsed by `parsing.py`. Results feed
   the HTML cache.
4. **Compare (`CompareMixin`).** For each remote file, decide whether the local
   copy is current using size, timestamp, ETag, and (for small files) content
   hashing. When `async_metadata` is enabled, HEAD checks run through the async
   manager for throughput; otherwise the sync path is used.
5. **Download (`DownloadMixin` → `download.py`).** Missing/changed files are
   fetched. `ParallelDownloadManager.auto_select_method` (or an explicit
   `DownloadMethod`) picks sequential vs. streaming-parallel vs.
   traditional-parallel chunking; `PartialDownloadManager` provides resume. The
   `UnifiedConcurrencyManager` enforces a single global thread cap; per-domain
   `CircuitBreakerManager` and the rate limiter throttle on errors/bandwidth.
6. **Cleanup (`CleanupMixin.clean_obsolete`).** Optionally preview/move/delete
   local files no longer present remotely, per `CleanupPolicy`.
7. **Report (`ReportMixin`).** Render the summary, persist the cache
   (`CacheManager.save`), and optionally emit metrics JSON.

`MirrorURL` is a context manager — use `with MirrorURL(cfg) as mirror:` so
`__exit__`/`cleanup` tears down pools, async loops, and the health server.

---

## The configuration system

There are **two** config objects; know which is which:

- **`ConfigSchema`** (pydantic `BaseModel`) — the *file-facing* schema. It mirrors
  what a user may put in a YAML/JSON config file and applies validation bounds
  (`ge`/`le`) on values like `workers`, `timeout`, `cache_max_age`.
- **`MirrorConfig`** (pydantic `BaseModel`) — the *runtime* config consumed by
  `MirrorURL` and every subsystem. It is richer than `ConfigSchema`: it carries
  resolved `Path` objects, enum-typed fields (`cleanup_policy: CleanupPolicy`,
  `scan_mode: ScanMode`), and many runtime-only flags (`dry_run`, `quiet`,
  `async_metadata`, `adaptive_async`, `circuit_breaker_enabled`, …).

The assembly path:

```
CLI args ──┐
           ├─► load_config_from_args() ─► MirrorConfig  ─► MirrorURL(cfg)
config file┘   (expand_env_vars, validate_config_file)
```

- `expand_env_vars` resolves `${VAR}` placeholders in the file before validation.
- `validate_config_file` loads YAML/JSON and checks it against `ConfigSchema`.
- `load_config_from_args` merges CLI flags with any file values and produces the
  final `MirrorConfig`.

**When you add a setting**, you usually touch all three: a field on the config
model(s), a CLI flag in `cli.py`, and the code that reads it. See the
[extension recipes](#extension-recipes).

---

## Subsystem deep-dives

**SSRF-hardened transport (`transport.py`).** `SecureTransport` /
`SecureAsyncTransport` wrap httpx and reject requests whose resolved address is
loopback or private. This is a security boundary: do not weaken it for
convenience. Both accept a `test_mode` flag that relaxes the guard; this is how
integration tests hit a local server (see [Testing](#testing)). Note the flag is
not currently wired from `MirrorConfig` — that wiring is the documented
prerequisite for the end-to-end test.

**Circuit breakers (`circuit_breaker.py`).** `CircuitBreakerManager` keeps one
breaker per domain, created lazily via `get_breaker(domain)`. State transitions
are `CLOSED → OPEN → HALF_OPEN → CLOSED`. A historical bug (fixed in 3.1.13) was
that the manager's `record_*`/`can_execute` methods didn't lazily create the
breaker, so production domains never tripped — when changing this code, keep the
lazy-creation path intact and covered by tests.

**Concurrency (`concurrency.py`).** `UnifiedConcurrencyManager` enforces a single
global thread budget shared across sync, async, and parallel-chunk work, so the
process can't oversubscribe. Acquire/release are explicit; always release in a
`finally`.

**Async path (`async_connection.py`).** `AdaptiveAsyncManager` tunes its
concurrency from measured RTT, throughput, and error rate; `AsyncTaskManager`
runs the metadata HEAD checks. The non-adaptive `AsyncConnectionManager` shares
the client/semaphore plumbing — historically several "phantom attribute" bugs
came from methods that assumed adaptive-only state, so keep the two classes'
responsibilities distinct.

**Cache (`cache.py`).** `CacheManager` owns the JSON cache lifecycle: load +
validate (`_validate_and_sanitize_cache`), schema-version gating
(`CACHE_SCHEMA_VERSION`), atomic save via a temp file, and corrupted-file backup.
The cache *filename* is built in `_MirrorBase.__init__`, not here.

---

## Extension recipes

These are the common changes and the exact touch-points.

### Add a configuration option

1. Add the field to `MirrorConfig` in `config.py` (and to `ConfigSchema` too if
   it should be settable from a config file), with a sensible default and any
   pydantic validation bounds.
2. If it should be settable from the CLI, add a flag in `cli.py` and map it in
   `load_config_from_args`.
3. Read `self.config.<field>` where the behavior lives (a mixin or a subsystem).
4. Add a test (a config round-trip test for the field; a behavior test for the
   effect). Document it in `USER_GUIDE.md` if user-facing.

### Add a CLI flag

Flags are defined in `cli.py` (download-related ones via
`add_parallel_arguments`). Add the `argparse` argument, then ensure
`load_config_from_args` translates it onto `MirrorConfig`. Keep `--help` text
consistent with the User Guide's option tables.

### Add a download mode

1. Add a value to `DownloadMethod` in `enums.py`.
2. Implement the mechanism in `download.py` (`ParallelDownloadManager`), and make
   `auto_select_method` able to return it when appropriate.
3. Handle the new method where methods are dispatched in
   `_core/report.py`/`_core/downloads.py` (the `if method == DownloadMethod.…`
   branches).
4. Expose a CLI flag (see above) if users should be able to force it.
5. Add tests covering selection and the download path.

### Add a new exception type

Add it to `exceptions.py`, subclassing the nearest existing base in the
`MirrorError` hierarchy. If it is part of the public surface, re-export it from
`__init__.py` and add it to `__all__`.

### Add a method to MirrorURL

Pick the mixin matching the method's responsibility (scan/compare/download/
cleanup/report/urls) and add it there. Use shared state initialized in
`_MirrorBase.__init__`; don't introduce a second `__init__`. If the method is
public API, consider whether it belongs on the documented surface.

### Add a new subsystem module

Place it in the correct layer (it may import only downward). Wire it into
`MirrorURL` from `_MirrorBase.__init__` (layer 6 is where composition happens).
Add unit tests at its own layer with no higher-layer setup.

---

## Coding conventions

- **`from __future__ import annotations`** at the top of every module. Annotations
  are lazy strings, which lets us reference types without import cycles and use
  modern annotation forms while still running on 3.9.
- **Classic typing** (`Dict`, `List`, `Optional`, `Union` from `typing`) — the
  package targets 3.9. Do not "modernize" to `dict[...]`/`X | None` in runtime
  positions; the lint config deliberately omits pyupgrade (`UP`) and most `SIM`
  rules for this reason.
- **`TYPE_CHECKING` guards** for imports needed only for annotations, to keep the
  import graph acyclic.
- **Lint rule set:** ruff with `E, F, W, I, B, C4`. A few bugbear rules
  (`B904`, `B007`, `B019`) and `E501`/`B008` are ignored — see the rationale
  comments in `pyproject.toml`. `B019` in particular marks a real latent
  `lru_cache`-on-method issue inherited verbatim; it's preserved, not silently
  rewritten.
- **Formatting:** `black` (line length 100) or `ruff format`. The legacy
  `mirror_url.py` is excluded from both.
- **Type-checking:** `mypy` runs as an advisory signal (CI `continue-on-error`),
  not a gate. It is lenient by design (`no_implicit_optional = false`,
  untyped-defs allowed) because the port is largely untyped. Tightening it is a
  welcome dedicated follow-up, not something to do piecemeal mid-feature.
- **The layering rule** ([above](#the-dependency-layer-architecture)) is a hard
  convention: never import upward or create an import cycle.

---

## Testing

The suite lives in `tests/` and runs under `pytest`. Two lanes:

- **Fast lane** (`pytest -m "not integration"`) — smoke, utilities, security, and
  subsystem-integration tests that exercise real in-process I/O (thread-safe
  primitives under concurrent load, circuit-breaker timing, `DiskBackedSet`
  spill-to-disk, pydantic/YAML config round-trips). No network. This is what CI
  gates on across Python 3.9–3.12.
- **Integration lane** (`pytest -m integration`) — end-to-end runs against the
  local `static_http_server` fixture in `conftest.py`.

**The SSRF caveat for end-to-end tests:** the secure transport refuses
loopback/private targets, so a real local-server run requires the transport's
`test_mode` bypass. That wiring from `MirrorConfig` is not yet in place, so the
full end-to-end test is currently skipped and documents the requirement (see
`tests/test_integration.py`). If you implement `test_mode` plumbing, you can
enable it.

**Where to add tests:**

- Pure-logic, low-layer code (`utils`, `security`, `primitives`,
  `circuit_breaker`, `rate_limiter`, `parsing`) → fast unit tests; this is where
  the bulk of coverage should live.
- Managers → component tests injecting a fake/test httpx transport.
- Whole-run behavior → integration tests against the fixture server.

Markers are declared in `pyproject.toml` under `[tool.pytest.ini_options]`
(`--strict-markers` is on, so register new markers there).

---

## Build, lint, and type-check

```bash
# editable install with the dev toolchain
pip install -e ".[dev]"
pre-commit install        # optional but recommended

ruff check .              # lint
ruff format --check .     # or: black --check .
mypy                      # advisory type-check of src/mirror_url
pytest -m "not integration"   # fast lane
pytest                        # full suite (includes integration)

# build distributions
pip install build
python -m build           # wheel + sdist into dist/
```

CI (`.github/workflows/ci.yml`) runs lint + tests across 3.9–3.12. The HTML docs
are produced from the Markdown with pandoc (embedded CSS + TOC) — regenerate
`docs/*.html` after editing the corresponding `.md`.

---

## Release process

1. **Bump the version in both sources** (a test asserts they match):
   - `pyproject.toml` → `version = "X.Y.Z"`
   - `src/mirror_url/_version.py` → `__version__ = "X.Y.Z"`
   - Also update the user-facing version strings in `cli.py` (banner/description)
     and the version references in `docs/USER_GUIDE.{md,html}`.
2. **Update `CHANGELOG.md`** — add a new section at the top following Keep a
   Changelog (`### Added/Changed/Fixed`).
3. **Commit everything** (`git add -A` — don't forget docs/changelog/cli), push.
4. **Tag and push the tag:**
   ```bash
   git tag -a vX.Y.Z -m "mirror-url X.Y.Z"
   git push origin vX.Y.Z
   ```
5. The tag triggers `.github/workflows/release.yml`, which builds the wheel/sdist,
   creates a GitHub Release, and (when configured) publishes to PyPI via **Trusted
   Publishing** (OIDC — no API token). The PyPI step requires a one-time setup: a
   pending publisher on PyPI (`owner` = repo owner, workflow `release.yml`,
   environment `pypi`) and a matching `pypi` Environment in the repo settings.
   Until that exists, the tag still builds the wheel and creates the GitHub
   Release; only the PyPI upload is skipped/red.

If you push a tag pointing at an incomplete commit, move it with
`git tag -f -a vX.Y.Z … && git push origin vX.Y.Z --force` to re-trigger the
release from the corrected commit.

---

## Known hazards and gotchas

These bit the project before; the migration plan calls them out explicitly.

- **Don't reintroduce the self-import.** `ConnectionManager`'s scope check imports
  `MirrorURL` from `.core`; the monolith's `from mirror_url import MirrorURL` was
  a packaging hazard. Prefer dependency injection over reaching up to `core`.
- **Keep subclass families in one module.** `PerIPRateLimiter`/
  `ChunkAwareRateLimiter` subclass `RateLimiter`; `ChunkCircuitBreaker` subclasses
  `CircuitBreaker`. Splitting a base from its subclasses across modules invites
  import cycles.
- **Centralize optional-dep probes in `compat.py`.** Import the
  `*_AVAILABLE` flag; never re-do the `try/except import` elsewhere.
- **Shared module globals** (e.g. log bookkeeping used by `setup_shared_logging`/
  `cleanup_log_files`) must live in one module and be imported, not re-declared,
  or you get divergent copies.
- **Preserve verbatim bug fixes.** The changelog documents subtle
  attribute/phantom-method fixes (async HEAD, async transport `test_mode`,
  circuit-breaker lazy creation, redirect header preservation, MOVE-mode cleanup).
  Don't "tidy" these away while refactoring nearby code.
- **One real latent issue is preserved on purpose:** mypy flags
  `ConnectionManager._is_url_within_scope` reading `self.target_parsed` in a
  branch `__init__` never sets (the branch is never taken today). Fix it
  deliberately, with a test, during a typing pass — not as a drive-by.

---

## Quick "where do I find…" map

| I want to change… | Go to |
|---|---|
| A tuning default or limit | `constants.py` |
| An error type | `exceptions.py` (+ `__init__.py` if public) |
| A run-mode / state enum | `enums.py` |
| A config field | `config.py` (`MirrorConfig`/`ConfigSchema`) + `cli.py` |
| URL scope/validation logic | `_core/urls.py` |
| How the remote tree is discovered | `_core/scan.py` (+ `parsing.py`) |
| "Is the file up to date?" logic | `_core/compare.py` |
| How a file is actually downloaded | `_core/downloads.py` → `download.py` |
| Obsolete-file cleanup behavior | `_core/cleanup.py` |
| The top-level run / summary | `_core/report.py` (`sync()`) |
| Construction / shared state / logging | `_core/_base.py` |
| The on-disk cache format/lifecycle | `cache.py` (filename in `_core/_base.py`) |
| SSRF / network security boundary | `transport.py`, `security.py` |
| Throttling / retries / breakers | `rate_limiter.py`, `connection.py`, `circuit_breaker.py` |
| CLI flags / entry point | `cli.py`, `__main__.py` |
| The version number | `_version.py` **and** `pyproject.toml` |

---

*This guide describes the architecture as of version 3.1.14. When you change the
structure, update this document in the same PR.*
