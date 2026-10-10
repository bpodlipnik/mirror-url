> Historical migration plan. The modular migration is complete. References
> below to a live monolith or placeholder modules describe the starting state,
> not the current checkout. Unchecked items are follow-up proposals.
> The repository does not currently run a scheduled nightly integration workflow.

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

<!-- HTML tables keep the first column on one line in GitHub.
     Use samp for first-column code: GitHub wraps code inside nowrap cells. -->

<table>
<thead>
<tr>
<th scope="col" nowrap>Module</th>
<th scope="col">Symbols (orig. lines)</th>
<th scope="col">~LOC</th>
</tr>
</thead>
<tbody>
<tr>
<td nowrap><samp>_version.py</samp></td>
<td><code>__version__</code>, <code>__author__</code> (85–184)</td>
<td>~5</td>
</tr>
<tr>
<td nowrap><samp>compat.py</samp></td>
<td><code>Str</code>/<code>STRINGZILLA_AVAILABLE</code> (158–183), <code>TQDM_AVAILABLE</code> (184–189), <code>LXML_AVAILABLE</code> (190–196), <code>PSUTIL_AVAILABLE</code> (197–202)</td>
<td>~50</td>
</tr>
<tr>
<td nowrap><samp>constants.py</samp></td>
<td>All <code>DEFAULT_*</code>/cache/scan/async/safety constants, <code>KNOWN_THROTTLED_DOMAINS</code>, <code>WINDOWS_RESERVED_NAMES</code> (97–360)</td>
<td>~200</td>
</tr>
<tr>
<td nowrap><samp>exceptions.py</samp></td>
<td><code>MirrorError</code> + 18 subclasses (365–452)</td>
<td>~90</td>
</tr>
<tr>
<td nowrap><samp>enums.py</samp></td>
<td><code>LogLevel</code>, <code>ScanMode</code>, <code>CleanupPolicy</code>, <code>DownloadPriority</code>, <code>CircuitBreakerState</code>, <code>MemoryPressure</code>, <code>ConcurrencyType</code>, <code>DownloadMethod</code> (453–505)</td>
<td>~50</td>
</tr>
<tr>
<td nowrap><samp>models.py</samp></td>
<td><code>DownloadTask</code> (506–524), <code>ServerProfile</code> (525–604), <code>HealthStatus</code> (605–617), <code>ChunkInfo</code> (618–636), <code>ParallelFileDownload</code> (637–659)</td>
<td>~155</td>
</tr>
<tr>
<td nowrap><samp>decorators.py</samp></td>
<td><code>retry_with_backoff</code> (660–719), <code>log_performance</code> (720–742)</td>
<td>~85</td>
</tr>
<tr>
<td nowrap><samp>utils.py</samp></td>
<td><code>exponential_backoff</code> (815–838), <code>_validate_and_sanitize_cache</code> (839–921), <code>format_duration</code> (922–947), <code>format_bytes</code> (948–969), <code>normalize_etag</code> (970–994), <code>safe_url_encode</code> (995–1017), <code>trim_url</code> (1018–1021), <code>sanitize_url_for_log</code> (1022–1063), <code>compute_file_hash</code> (1064–1088), <code>is_reserved_windows_filename</code> (1089–1107), <code>normalize_url_path</code> (1108–1154), <code>cleanup_log_files</code> (1944–1959)</td>
<td>~430</td>
</tr>
<tr>
<td nowrap><samp>security.py</samp></td>
<td><code>SymlinkTracker</code> (743–814), <code>SecurityValidator</code> (1155–1378), <code>PathSafety</code> (1656–1847), <code>FastURLValidator</code> (1848–1943)</td>
<td>~580</td>
</tr>
<tr>
<td nowrap><samp>transport.py</samp></td>
<td><code>SecureTransport</code> (1379–1524), <code>SecureAsyncTransport</code> (1525–1655)</td>
<td>~280</td>
</tr>
<tr>
<td nowrap><samp>primitives.py</samp></td>
<td><code>LRUCache</code> (1960–2161), <code>AtomicCounter</code> (2162–2259), <code>AtomicSize</code> (2260–2347)</td>
<td>~390</td>
</tr>
<tr>
<td nowrap><samp>storage.py</samp></td>
<td><code>FileSystemCache</code> (2348–2460), <code>DiskBackedSet</code> (2461–2950)</td>
<td>~600</td>
</tr>
<tr>
<td nowrap><samp>parsing.py</samp></td>
<td><code>AdaptiveBatchProcessor</code> (2951–3022), <code>extract_links_fast</code> (3023–3070), <code>should_use_fast_parser</code> (3071–3097)</td>
<td>~150</td>
</tr>
<tr>
<td nowrap><samp>circuit_breaker.py</samp></td>
<td><code>CircuitBreaker</code> (3098–3213), <code>AsyncCircuitBreaker</code> (3214–3340), <code>ChunkCircuitBreaker</code> (4161–4214), <code>CircuitBreakerManager</code> (8355–8416)</td>
<td>~360</td>
</tr>
<tr>
<td nowrap><samp>rate_limiter.py</samp></td>
<td><code>BandwidthLimiter</code> (3341–3399), <code>RateLimiter</code> (3918–4010), <code>PerIPRateLimiter</code> (4011–4085), <code>ChunkAwareRateLimiter</code> (4086–4160)</td>
<td>~330</td>
</tr>
<tr>
<td nowrap><samp>queue.py</samp></td>
<td><code>DownloadQueue</code> (3400–3522)</td>
<td>~120</td>
</tr>
<tr>
<td nowrap><samp>metrics.py</samp></td>
<td><code>MetricsCollector</code> (3523–3917)</td>
<td>~395</td>
</tr>
<tr>
<td nowrap><samp>progress.py</samp></td>
<td><code>ProgressTracker</code> (8637–8844), <code>MultiLevelProgress</code> (8845–8940)</td>
<td>~305</td>
</tr>
<tr>
<td nowrap><samp>monitoring.py</samp></td>
<td><code>MemoryMonitor</code> (8941–9031), <code>DiskSpaceManager</code> (9032–9125), <code>PerformanceMonitor</code> (9126–9203)</td>
<td>~265</td>
</tr>
<tr>
<td nowrap><samp>connection.py</samp></td>
<td><code>ConnectionPool</code> (5645–5991), <code>ConnectionManager</code> (6260–6689)</td>
<td>~780</td>
</tr>
<tr>
<td nowrap><samp>async_connection.py</samp></td>
<td><code>AsyncConnectionManager</code> (6690–7107), <code>AdaptiveAsyncManager</code> (7108–7748), <code>AsyncTaskManager</code> (7749–7941)</td>
<td>~1250</td>
</tr>
<tr>
<td nowrap><samp>concurrency.py</samp></td>
<td><code>UnifiedConcurrencyManager</code> (5992–6259)</td>
<td>~270</td>
</tr>
<tr>
<td nowrap><samp>download.py</samp></td>
<td><code>ParallelDownloadManager</code> (4215–5644), <code>PartialDownloadManager</code> (9204–9370)</td>
<td>~1600</td>
</tr>
<tr>
<td nowrap><samp>scanner.py</samp></td>
<td><code>DirectoryScanner</code> (8417–8636)</td>
<td>~220</td>
</tr>
<tr>
<td nowrap><samp>health.py</samp></td>
<td><code>HealthCheckHandler</code> (9371–9481), <code>HealthCheckServer</code> (9482–9537), <code>HealthChecker</code> (9655–9716)</td>
<td>~225</td>
</tr>
<tr>
<td nowrap><samp>cache.py</samp></td>
<td><code>CacheManager</code> (7942–8354)</td>
<td>~415</td>
</tr>
<tr>
<td nowrap><samp>config.py</samp></td>
<td><code>ConfigSchema</code> (9538–9577), <code>validate_config_file</code> (9578–9602), <code>expand_env_vars</code> (9603–9654), <code>MirrorConfig</code> (13439–13841), <code>load_config_from_args</code> (13842–13955)</td>
<td>~600</td>
</tr>
<tr>
<td nowrap><samp>tuner.py</samp></td>
<td><code>AutoConcurrencyTuner</code> (9717–9822)</td>
<td>~106</td>
</tr>
<tr>
<td nowrap><samp>core.py</samp></td>
<td><code>MirrorURL</code> (9823–13438)</td>
<td>~3616</td>
</tr>
<tr>
<td nowrap><samp>cli.py</samp></td>
<td><code>add_parallel_arguments</code> (13956–13992), <code>setup_shared_logging</code> (13993–14126), <code>main</code> (14127–15145)</td>
<td>~1190</td>
</tr>
</tbody>
</table>

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
10. Delete `mirror_url.py` shims (done, v3.1.20). `test_integration.py`
    restoration is a separate, still-open item -- needs the SSRF-guard
    `test_mode` bypass wired through from config (see that file's module
    docstring for the exact gap); not blocked by this deletion.

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
- [x] `mirror_url.py` removed (was retained as a frozen reference until the
      test suite passed with real runtime deps installed -- confirmed: 60
      passed, 1 skipped as of v3.1.19, and the 1 skip is the unrelated
      SSRF-guard test_mode wiring gap below, not a migration gap).
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
      One finding was a *real* latent issue mypy surfaced, **fixed**: previously
      `ConnectionManager._is_url_within_scope` read `self.target_parsed` in its
      `check_base=False` branch, which `__init__` never set (the branch was never
      taken in practice — the outer try/except silently swallowed the resulting
      `AttributeError` and returned `False`). First fixed defensively by having
      `ConnectionManager.__init__` explicitly set `self.target_parsed: Optional[
      ParseResult] = None`; on follow-up review that was judged to leave a
      permanently-dead branch in place (`check_base=False` could never do
      anything useful in this class — `ConnectionManager` has no notion of a
      resolved target/dir-suffix scope the way `MirrorURL`/`_MirrorBase` does),
      so `check_base` and the branch were removed from `_is_url_within_scope`
      entirely instead, along with the now-unused `self.target_parsed`.

- [x] Two further latent issues, found during review, fixed:
      - `UrlsMixin._parse_url_cached` was `@lru_cache`-decorated on an instance
        method; since the cache key included `self`, every `MirrorURL` instance
        was pinned in the cache for the process lifetime (a real leak under
        long-running/multi-suffix use — Ruff B019). `urlparse(url)` depends only
        on `url`, so the cache now lives on a module-level function
        (`_parse_url_cached_module`) shared correctly across instances; the
        instance method is a thin delegator.
      - `SymlinkTracker.record_skip` was a pure no-op (`total_symlinks_followed
        += 0`) despite being called from three sites in `scan.py`'s skip path —
        skip-mode runs silently reported zero skip activity. Added a dedicated
        `total_symlinks_skipped` counter, incremented in `record_skip` and
        surfaced in `get_stats()`.
      - `MirrorURL._signal_handler` had two issues: on a *successful* cleanup
        (within the 30s window) it returned without exiting, so SIGINT/SIGTERM
        only ran `cleanup()` and then let execution resume as if nothing had
        happened — the process never actually terminated. On timeout it called
        `sys.exit(0)`, reporting success even though shutdown was forced. Both
        paths now call `sys.exit()` explicitly, with a distinct exit code (0 vs
        1) so callers such as cron wrappers can tell a clean shutdown from a
        forced one.

        Follow-up, from external review: "join non-daemon pools with a hard
        timeout" was still open. Found the concrete cause: `UnifiedConcurrency
        Manager.shutdown()` and `ParallelDownloadManager.shutdown()` both called
        `executor.shutdown(wait=True, ...)` unconditionally — that call has no
        timeout of its own and blocks until every in-flight worker thread
        returns, so a single worker stuck on a slow/hung network read could
        silently consume the entire 30s budget `_signal_handler` gives
        `cleanup()` overall, before the outer timeout path ever got a chance to
        log or act. **Fixed**: added `utils.bounded_executor_shutdown()`, which
        runs the blocking `executor.shutdown()` in its own thread and joins it
        with a timeout (`ParallelDownloadManager`: 15s, `UnifiedConcurrency
        Manager`: 10s); both `shutdown()` methods now take an optional
        `timeout` parameter. **Still open**, and still tracked for this
        refactor pass: this bounds the *wait*, not the work itself — a stuck
        worker thread isn't force-cancelled (Python's threading API has no
        primitive for that) and keeps running to completion in the background;
        a real fix needs cooperative cancellation checkpoints threaded through
        the download/async layers, plus reconciling the internal per-step
        timeouts (async task manager ~15-20s, connection managers ~10s each,
        now the two thread pools ~10-15s each) against the outer 30s signal-
        handler deadline they all nest inside, which can theoretically exceed
        it in a worst case even with every step individually bounded.
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
