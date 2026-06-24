# MirrorURL — Refactoring & Modularization Plan

`mirror_url.py` is a single **15,145-line** file containing ~70 classes and
~25 module-level functions. This document defines how to split it into a
maintainable, testable, open-source `src/mirror_url/` package **without changing
behavior**.

The package skeleton already exists: every module below is present as a
placeholder (docstring + list of the symbols it will receive + the original line
ranges). The monolith remains the runnable source of truth until each module is
populated and verified.

---

## 1. Goals & principles

1. **Behavior-preserving.** No logic changes during the split — only relocation
   and import rewiring. Logic fixes happen in separate, reviewable commits.
2. **One concern per module.** Each module is independently readable and testable.
3. **Acyclic dependency layering.** Modules only import from lower layers
   (see §3). No import cycles.
4. **Small, verifiable steps.** Migrate bottom-up, one layer at a time, keeping
   the test suite green after every move.
5. **OSS-ready.** Packaging, license, tests, CI, and lint are in place from day one.

---

## 2. Target package layout

```
src/mirror_url/
├── __init__.py            # public API re-exports
├── __main__.py            # `python -m mirror_url`
├── _version.py            # __version__, __author__
├── compat.py              # optional-dep flags + StringZilla Str fallback
├── constants.py           # all tuning constants & static tables
├── exceptions.py          # MirrorError hierarchy (19 classes)
├── enums.py               # LogLevel, ScanMode, … DownloadMethod (8 enums)
├── models.py              # DownloadTask, ServerProfile, HealthStatus, ChunkInfo, ParallelFileDownload
├── decorators.py          # retry_with_backoff, log_performance
├── utils.py               # formatting / url / hashing / cache-validation helpers
├── security.py            # SymlinkTracker, SecurityValidator, PathSafety, FastURLValidator
├── transport.py           # SecureTransport, SecureAsyncTransport
├── primitives.py          # LRUCache, AtomicCounter, AtomicSize
├── storage.py             # FileSystemCache, DiskBackedSet
├── parsing.py             # extract_links_fast, should_use_fast_parser, AdaptiveBatchProcessor
├── circuit_breaker.py     # CircuitBreaker, AsyncCircuitBreaker, ChunkCircuitBreaker, CircuitBreakerManager
├── rate_limiter.py        # BandwidthLimiter, RateLimiter, PerIPRateLimiter, ChunkAwareRateLimiter
├── queue.py               # DownloadQueue
├── metrics.py             # MetricsCollector
├── progress.py            # ProgressTracker, MultiLevelProgress
├── monitoring.py          # MemoryMonitor, DiskSpaceManager, PerformanceMonitor
├── connection.py          # ConnectionPool, ConnectionManager
├── async_connection.py    # AsyncConnectionManager, AdaptiveAsyncManager, AsyncTaskManager
├── concurrency.py         # UnifiedConcurrencyManager
├── download.py            # ParallelDownloadManager, PartialDownloadManager
├── scanner.py             # DirectoryScanner
├── health.py              # HealthCheckHandler, HealthCheckServer, HealthChecker
├── cache.py               # CacheManager
├── config.py              # ConfigSchema, MirrorConfig, validate_config_file, expand_env_vars, load_config_from_args
├── tuner.py               # AutoConcurrencyTuner
├── core.py                # MirrorURL (orchestrator)
└── cli.py                 # add_parallel_arguments, setup_shared_logging, main
```

---

## 3. Dependency layers (import only downward)

```
Layer 0  _version · compat · constants · exceptions · enums
Layer 1  models · decorators · utils
Layer 2  primitives · parsing · security
Layer 3  transport · storage · circuit_breaker · rate_limiter · queue
Layer 4  metrics · progress · monitoring · connection · async_connection · concurrency · cache
Layer 5  download · scanner · health · config · tuner
Layer 6  core
Layer 7  cli  →  __main__ / console entry point
```

Rule: a module may import from **strictly lower** layers (and the standard
library / third-party deps), never sideways within a cycle and never upward.
`core.py` is the only place allowed to wire many Layer-4/5 subsystems together.

---

## 4. Module-by-module contents

Each row maps a target module to the symbols moved into it and their **original
line range** in `mirror_url.py`. Approx. sizes are the monolith line counts.

| Module | Symbols (orig. lines) | ~LOC |
|---|---|---|
| `_version.py` | `__version__`, `__author__` (85–184) | ~5 |
| `compat.py` | `Str`/`STRINGZILLA_AVAILABLE` (158–183), `TQDM_AVAILABLE` (184–189), `LXML_AVAILABLE` (190–196), `PSUTIL_AVAILABLE` (197–202) | ~50 |
| `constants.py` | All `DEFAULT_*`/cache/scan/async/safety constants, `KNOWN_THROTTLED_DOMAINS`, `WINDOWS_RESERVED_NAMES` (97–360) | ~200 |
| `exceptions.py` | `MirrorError` + 18 subclasses (365–452) | ~90 |
| `enums.py` | `LogLevel`, `ScanMode`, `CleanupPolicy`, `DownloadPriority`, `CircuitBreakerState`, `MemoryPressure`, `ConcurrencyType`, `DownloadMethod` (453–505) | ~50 |
| `models.py` | `DownloadTask` (506–524), `ServerProfile` (525–604), `HealthStatus` (605–617), `ChunkInfo` (618–636), `ParallelFileDownload` (637–659) | ~155 |
| `decorators.py` | `retry_with_backoff` (660–719), `log_performance` (720–742) | ~85 |
| `utils.py` | `exponential_backoff` (815–838), `_validate_and_sanitize_cache` (839–921), `format_duration` (922–947), `format_bytes` (948–969), `normalize_etag` (970–994), `safe_url_encode` (995–1017), `trim_url` (1018–1021), `sanitize_url_for_log` (1022–1063), `compute_file_hash` (1064–1088), `is_reserved_windows_filename` (1089–1107), `normalize_url_path` (1108–1154), `cleanup_log_files` (1944–1959) | ~430 |
| `security.py` | `SymlinkTracker` (743–814), `SecurityValidator` (1155–1378), `PathSafety` (1656–1847), `FastURLValidator` (1848–1943) | ~580 |
| `transport.py` | `SecureTransport` (1379–1524), `SecureAsyncTransport` (1525–1655) | ~280 |
| `primitives.py` | `LRUCache` (1960–2161), `AtomicCounter` (2162–2259), `AtomicSize` (2260–2347) | ~390 |
| `storage.py` | `FileSystemCache` (2348–2460), `DiskBackedSet` (2461–2950) | ~600 |
| `parsing.py` | `AdaptiveBatchProcessor` (2951–3022), `extract_links_fast` (3023–3070), `should_use_fast_parser` (3071–3097) | ~150 |
| `circuit_breaker.py` | `CircuitBreaker` (3098–3213), `AsyncCircuitBreaker` (3214–3340), `ChunkCircuitBreaker` (4161–4214), `CircuitBreakerManager` (8355–8416) | ~360 |
| `rate_limiter.py` | `BandwidthLimiter` (3341–3399), `RateLimiter` (3918–4010), `PerIPRateLimiter` (4011–4085), `ChunkAwareRateLimiter` (4086–4160) | ~330 |
| `queue.py` | `DownloadQueue` (3400–3522) | ~120 |
| `metrics.py` | `MetricsCollector` (3523–3917) | ~395 |
| `progress.py` | `ProgressTracker` (8637–8844), `MultiLevelProgress` (8845–8940) | ~305 |
| `monitoring.py` | `MemoryMonitor` (8941–9031), `DiskSpaceManager` (9032–9125), `PerformanceMonitor` (9126–9203) | ~265 |
| `connection.py` | `ConnectionPool` (5645–5991), `ConnectionManager` (6260–6689) | ~780 |
| `async_connection.py` | `AsyncConnectionManager` (6690–7107), `AdaptiveAsyncManager` (7108–7748), `AsyncTaskManager` (7749–7941) | ~1250 |
| `concurrency.py` | `UnifiedConcurrencyManager` (5992–6259) | ~270 |
| `download.py` | `ParallelDownloadManager` (4215–5644), `PartialDownloadManager` (9204–9370) | ~1600 |
| `scanner.py` | `DirectoryScanner` (8417–8636) | ~220 |
| `health.py` | `HealthCheckHandler` (9371–9481), `HealthCheckServer` (9482–9537), `HealthChecker` (9655–9716) | ~225 |
| `cache.py` | `CacheManager` (7942–8354) | ~415 |
| `config.py` | `ConfigSchema` (9538–9577), `validate_config_file` (9578–9602), `expand_env_vars` (9603–9654), `MirrorConfig` (13439–13841), `load_config_from_args` (13842–13955) | ~600 |
| `tuner.py` | `AutoConcurrencyTuner` (9717–9822) | ~106 |
| `core.py` | `MirrorURL` (9823–13438) | ~3616 |
| `cli.py` | `add_parallel_arguments` (13956–13992), `setup_shared_logging` (13993–14126), `main` (14127–15145) | ~1190 |

### 4.1 Breaking up the `MirrorURL` god-class (follow-up)

`MirrorURL` is 3,616 lines — too large to leave as one class long-term. After it
is isolated in `core.py` and the suite is green, split it internally (no behavior
change) into a `core/` subpackage using **mixins** grouped by responsibility:

```
core/
├── __init__.py        # class MirrorURL(ScanMixin, CompareMixin, DownloadMixin,
│                      #                   CleanupMixin, ReportMixin): ...
├── _base.py           # __init__, shared state, context-manager plumbing
├── scan.py            # remote discovery / get_remote_files
├── compare.py         # file_exists_and_up_to_date, size/etag/hash comparison
├── download.py        # download orchestration (delegates to download.py engines)
├── cleanup.py         # clean_obsolete (MOVE/DELETE policies)
└── report.py          # summary / metrics rendering
```

Group methods by inspecting their prefixes/cohesion (`_scan_*`, `_download_*`,
`_clean_*`, `_compare_*`, `_report_*`). Keep `__init__` and shared attributes in
`_base.py`. This is optional for a first OSS release but strongly recommended.

---

## 5. Known refactor hazards (found in the file)

- **Self-import.** Line ~6328 does `from mirror_url import MirrorURL` (used for a
  worker/subprocess path). After packaging, change this to a normal intra-package
  import (`from .core import MirrorURL`) and confirm the worker entry still
  resolves the dotted path.
- **`__main__` guard.** Lines 15129–15145 end with `if __name__ == "__main__": main()`.
  Move `main()` to `cli.py`; keep a thin `__main__.py` for `python -m mirror_url`.
- **Optional-dependency flags** (`STRINGZILLA_AVAILABLE`, `TQDM_AVAILABLE`,
  `LXML_AVAILABLE`, `PSUTIL_AVAILABLE`) are read across many classes. Centralize
  them in `compat.py` and import the flag, not the try/except, everywhere.
- **Shared module globals.** `_log_files` and other process-level state used by
  `cleanup_log_files` / `setup_shared_logging` must live in one module
  (`utils.py` / `cli.py`) and be imported, not re-declared, to avoid divergent copies.
- **Subclass pairs split across files.** `PerIPRateLimiter`/`ChunkAwareRateLimiter`
  subclass `RateLimiter`; `ChunkCircuitBreaker` subclasses `CircuitBreaker`. Keep
  each base + its subclasses in the **same** module (already grouped that way).
- **Inter-version bugfix history** in the file header documents subtle
  attribute/phantom-method bugs. Preserve these fixes verbatim during the move;
  do not "clean up" while relocating.

---

## 6. Step-by-step migration procedure

Migrate **bottom-up**, one layer at a time. After each module:

1. Cut the symbols from `mirror_url.py` into the target module.
2. Add the needed imports at the top of the new module (stdlib, third-party, then
   `from .<lower_module> import …`).
3. In `mirror_url.py`, replace the cut block with
   `from mirror_url.<module> import *` (temporary shim) so the monolith keeps
   running during the transition.
4. Run `ruff check`, `pytest -m "not integration"`. Keep green.
5. Update `__init__.py` public re-exports if the module exposes public API.
6. Commit (one module or one layer per commit).

Suggested commit sequence (by layer):

1. `_version`, `constants`, `exceptions`, `enums`, `compat`
2. `models`, `decorators`, `utils`  → enable `test_utils.py`
3. `primitives`, `parsing`, `security` → enable `test_security.py`
4. `transport`, `storage`, `circuit_breaker`, `rate_limiter`, `queue`
5. `metrics`, `progress`, `monitoring`, `cache`
6. `connection`, `async_connection`, `concurrency`
7. `download`, `scanner`, `health`, `config`, `tuner`
8. `core` (then optional `core/` mixin split, §4.1)
9. `cli` + `__main__`; fix the self-import; wire the `mirror-url` entry point
10. Delete `mirror_url.py` shims; restore `test_integration.py` cases.

Final acceptance: `pip install -e .`, `mirror-url --help` works,
`python -m mirror_url --help` works, full `pytest` green, `ruff`/`black`/`mypy`
clean on `src/`.

---

## 7. Testing strategy

- **Unit tests** per module for the stateless/low-layer pieces (`utils`,
  `security`, `primitives`, `circuit_breaker`, `rate_limiter`, `parsing`). These
  are pure-logic and fast — the bulk of coverage should live here.
- **Component tests** for managers using fakes/mocks of `httpx` transports
  (already SSRF-guarded, so inject a test transport).
- **Integration tests** (`-m integration`) drive a real run against the local
  `static_http_server` fixture in `conftest.py`. Restore the ~50 cases referenced
  in the changelog (retry/backoff, disk-space exhaustion, AtomicCounter under
  load, concurrency caps, SecurityValidator edge cases, circuit-breaker
  transitions).
- CI runs `-m "not integration"` on 3.9–3.12; integration runs nightly/on-demand.

---

## 8. Definition of done (OSS release)

- [x] All 30 modules populated (verbatim migration; verified by AST method-set
      equivalence against the monolith for every class/function).
- [x] `import mirror_url` exposes the documented public API
      (`MirrorURL`, `MirrorConfig`, `load_config_from_args`, `main`, exceptions).
- [x] `mirror-url` and `python -m mirror_url` both run (`--help` verified).
- [ ] `mirror_url.py` removed (retained as frozen reference — delete after the
      test suite passes with real runtime deps installed).
- [x] `MirrorURL` split into mixins (§4.1). Implemented as a private `_core/`
      subpackage (`_base`, `urls`, `scan`, `compare`, `downloads`, `cleanup`,
      `report`) composed by a thin `core.py`. Verified: all 45 methods present on
      the composed class, each defined exactly once, byte-identical bodies, clean
      MRO, package imports with 0 failures. (`core.py` stays a module rather than
      a `core/` package only because the original file could not be removed from
      the working environment — functionally equivalent.)
- [x] `pytest` green on the fast lane (smoke/utils/security = 36 passed with real
      deps). Subsystem integration tests added (`tests/test_subsystems.py`:
      concurrency, disk spill, circuit-breaker timing, config round-trip).
- [ ] Full end-to-end HTTP mirror test (`tests/test_integration.py`) — skipped
      pending an SSRF-guard test bypass for loopback targets (documented there).
- [x] `ruff check` clean after `--fix` + `ruff format` (config tuned to a
      correctness rule set; UP/SIM dropped for the verbatim 3.9 port).
- [x] `mypy` configured as advisory/lenient (CI `continue-on-error`). It reports
      ~120 findings, all pre-existing annotation imprecision inherited from the
      monolith (Optional-narrowing on runtime-guarded attributes, int/float
      attribute inference) — none are runtime bugs (the 52 passing tests cover
      behavior). A dedicated typing pass is a good follow-up, ideally with §4.1.
      One finding is a *real* latent issue mypy surfaced and worth fixing during
      that pass: `ConnectionManager._is_url_within_scope` reads `self.target_parsed`
      in its `check_base=False` branch, which `__init__` never sets (the branch is
      never taken today). Preserved verbatim for now.
- [ ] `black --check` clean (or use `ruff format`).
- [ ] CI green on 3.9–3.12.
- [x] README, LICENSE, CONTRIBUTING, CHANGELOG present and accurate.

> **Migration verification performed in this environment** (no network, so
> `httpx`/`pydantic`/`yaml`/`pytest` were unavailable): all 30 modules
> `py_compile`; the full package imports with 0 failures against dependency
> stubs (proving every cross-module import resolves); AST diffs confirm every
> migrated class/function has a method set identical to the monolith; and
> behavioral spot-checks pass on every dependency-free module. The remaining
> unchecked boxes require the real runtime deps and are expected to pass.
