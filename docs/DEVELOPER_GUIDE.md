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
- **Version:** 3.2.1
- **Python:** 3.10 or newer; CI tests Python 3.10–3.14
- **Runtime deps:** `httpx[http2]` (including `h2`), `pydantic` v2, `PyYAML`, `portalocker` 3.x (optional: `stringzilla`,
  `lxml`, `tqdm`, `psutil`, `aiohttp`)

---

## Table of contents

- [Background: the monolith and the refactor](#background-the-monolith-and-the-refactor)
- [Design principles](#design-principles)
- [Repository layout](#repository-layout)
- [The dependency-layer architecture](#the-dependency-layer-architecture)
- [Module reference (by layer)](#module-reference-by-layer)
- [The MirrorURL orchestrator and its mixins](#the-mirrorurl-orchestrator-and-its-mixins)
- [Runtime data flow: anatomy of a sync()](#runtime-data-flow-anatomy-of-a-sync)
- [Runtime guarantees and compatibility](#runtime-guarantees-and-compatibility)
- [The configuration system](#the-configuration-system)
- [Subsystem deep-dives](#subsystem-deep-dives)
- [Extension recipes](#extension-recipes)
- [Coding conventions](#coding-conventions)
- [Testing](#testing)
- [Unused code review](#unused-code-review)
- [Build, lint, and type-check](#build-lint-and-type-check)
- [Release process](#release-process)
- [Known hazards and gotchas](#known-hazards-and-gotchas)
- [Quick "where do I find…" map](#quick-where-do-i-find-map)

---

## Background: the monolith and the refactor

MirrorURL began as a single `mirror_url.py` of ~15,000 lines containing ~70
classes and ~25 module-level functions. It was split into the modular
`src/mirror_url/` package (46 Python files, organized by responsibilities) by a
**behavior-preserving** migration: code was relocated verbatim and class/function
method sets were verified identical to the original via AST comparison. Logic
changes were kept out of the migration and made only in separate, reviewable
commits.

Two consequences shape how you should work in this codebase:

1. **Verbatim heritage.** Much of the code is a faithful port of working,
   audited, sometimes idiosyncratic logic (including hard-won bug fixes recorded
   in the changelog). When touching migrated code, prefer surgical changes over
   "cleanups" — the original behavior is the contract.
2. **Python 3.10 baseline.** Python 3.10 is the minimum supported runtime.
   Existing code retains classic typing (`Dict`, `Optional`, `List`) and
   `from __future__ import annotations`. Lint rules that would modernize syntax
   (pyupgrade, most of SIM) remain disabled to keep unrelated rewrites separate
   — see [Coding conventions](#coding-conventions).

The legacy `mirror_url.py` was retained as a frozen reference, excluded from
lint and packaging, until the package's test suite passed with real runtime
dependencies installed — then deleted before the v3.1.20 release.

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
3. **Acyclic dependencies.** Keep imports acyclic. Same-layer imports already
   exist (for example, `connection.py` imports `metrics.py` and `concurrency.py`),
   so the diagram below is a responsibility map rather than an enforced
   strictly downward layering invariant.
4. **Small, verifiable steps.** Keep the fast test lane green after every change.

---

## Repository layout

```
mirror-url/
├── src/mirror_url/          # the package (48 Python files including private helpers)
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
├── pyproject.toml           # packaging, deps, tool config  (the other version source)
├── REFACTORING_PLAN.md      # the migration plan + module/line map
├── CHANGELOG.md             # Keep a Changelog format
└── .github/workflows/       # ci.yml (lint+test matrix), release.yml (tag → build → GitHub Release, optional PyPI)
```

---

## The dependency-layer architecture

The historical layers summarize responsibilities; runtime imports can cross
those groupings. The important invariant is an acyclic runtime import graph.

```
Foundations: _version · compat · constants · exceptions · enums · utils
Data and primitives: models · async_primitives · primitives · parsing · security · destination_lock
Managers: transport · storage · circuit_breaker · rate_limiter · queue · cache
Runtime support: metrics · progress · monitoring · connection · async_connection · concurrency
Engines: download · download_integrity · scanner · health · domain_health · config · tuner
Composition: core · _core/{_base,urls,scan,compare,downloads,cleanup,report}
Static contract: _core/_typing (TYPE_CHECKING only)
Entry points: cli · __main__
```

**Why it matters for you:**

- It keeps the package importable at every step and lets you unit-test low layers
  with zero setup (no network, no config).
- If you find yourself wanting to import "upward" (e.g. a layer-3 module needing
  something from `core`), that is almost always a design smell — the dependency
  belongs lower, or should be injected as a parameter/callback.
- The one historical exception is documented: the monolith did
  `from mirror_url import MirrorURL` inside `ConnectionManager`'s scope check.
  After packaging this became an intra-package import of `.core`; it is now
  replaced with the shared `utils.url_within_scope` helper. Avoid reintroducing
  this pattern; prefer dependency injection.

Compile and import the package after an import change to catch syntax errors
and common missing/circular-import failures. These checks do not prove that
every runtime path is acyclic: inspect function-local and dynamically invoked
imports separately. Both connection implementations share
`utils.url_within_scope` for their scheme, authority and decoded-path boundary;
pool warm-up checks each redirect before contacting its target.

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
- `async_primitives.py` — `LoopLocalPrimitive` and `ResizableSemaphore`: bind
  synchronization to the running event loop and resize admission without
  abandoning active leases or waiters.
- `parsing.py` — `extract_links_fast`, `should_use_fast_parser`,
  `AdaptiveBatchProcessor` (HTML directory-listing parsing, lxml/stringzilla
  accelerated when available).
- `security.py` — `SymlinkTracker`, `SecurityValidator`, `PathSafety`,
  `FastURLValidator` (path-traversal, symlink-bomb, and filename defenses).

**Layer 3 — transport & resilience primitives.**

- `transport.py` — `SecureTransport`, `SecureAsyncTransport`: httpx transports
  that block loopback/private IPs (SSRF hardening). Both honor a `test_mode` flag
  used to relax the guard in tests.
- `destination_lock.py` — per-user shared ancestor locks and exclusive resource
  locks. `_MirrorBase` acquires ownership before managers start; cache and log
  paths are added before access. Operation leases retain the handles until
  cleanup and active writers finish, including after a bounded shutdown timeout.
  `DestinationLockError` rejects overlap and use after cleanup. Lock files remain
  in `~/.mirror-url/locks-v1/`; never unlink them during a run. Portalocker 3.x
  provides native Windows/POSIX locks. This is cooperative local-filesystem
  protection with a common home directory, not multi-host coordination.
  The CLI's `run_ownership` context reserves all selected roots and the log tree
  before shared logging; `ContextVar` supplies an explicit parent to each
  suffix instance. Child cleanup ends its lifecycle without closing the run's
  handles. Child writer and cleanup leases also retain parent ownership.
- `storage.py` — `FileSystemCache`, `DiskBackedSet` (tracking with disk spills,
  memory-only duplicate suppression and pruning; no persistent membership index).
- `scratch.py` — flat private parallel workspaces with exact manifests and
  per-work OS leases. A chunk wait timeout retains its lease until every
  submitted future finishes, and late cleanup preserves a newer transfer's
  active tracking entry. Recovery holds destination/state ownership, rejects live
  leases and validates every entry before deletion. It never age-deletes
  arbitrary directories. Current work can retire its own lease after IO ends
  during bounded shutdown. Root ownership, malformed records, links and unknown
  children fail closed; unmarked legacy artifacts require manual review.
- `circuit_breaker.py` — `CircuitBreaker`, `AsyncCircuitBreaker`,
  `ChunkCircuitBreaker`, `CircuitBreakerManager` (per-domain). Keep each base and
  its subclasses in this one module.
- `rate_limiter.py` — `BandwidthLimiter`, `RateLimiter`, `PerIPRateLimiter`,
  `ChunkAwareRateLimiter`.
- `queue.py` — standalone `DownloadQueue`; the normal sync pipeline does not
  enqueue downloads through it. Both `get()` and `get_batch()` reserve each
  URL/path identity until `complete()` releases it. Complete each retrieved
  task exactly once on success or failure, keeping its URL/path unchanged.
  `active_tasks` counts waiting and in-flight tasks; `size` and `max_size`
  describe only waiting tasks.

**Layer 4 — managers & observability.**

- `metrics.py` — `MetricsCollector` (thread-safe counters/aggregates; emits the
  metrics JSON).
- `progress.py` — `ProgressTracker`, `MultiLevelProgress`.
- `monitoring.py` — `MemoryMonitor`, `DiskSpaceManager`, `PerformanceMonitor`.
- `connection.py` — `ConnectionPool`, `ConnectionManager` (synchronous request
  path, retries, redirects-preserving-headers).
- `async_connection.py` — `AsyncConnectionManager`, `AdaptiveAsyncManager`
  (self-tuning concurrency), `AsyncTaskManager` (async metadata checks).
- `concurrency.py` — `UnifiedConcurrencyManager` (coordinator used by selected sync/chunk paths).
- `cache.py` — `CacheManager` (the on-disk JSON cache: load/validate/save,
  schema-version checks, corrupted-file backup).

**Layer 5 — feature engines & config.**

- `download.py` — `ParallelDownloadManager` (chunked/streaming/parallel download
  engine, including `auto_select_method`) and `PartialDownloadManager` (resume).
- `scanner.py` — `DirectoryScanner`.
- `health.py` — `HealthCheckHandler`, `HealthCheckServer`, `HealthChecker`
  (optional HTTP health endpoint).
- `config.py` — `MirrorConfig` (the validated model), `ConfigSchema` (an alias
  of that model), `validate_config_file`, `expand_env_vars`, and the public
  argparse-namespace helper `load_config_from_args`. `cli.main()` does its own
  file/explicit-CLI merge; it does not call that helper.
- `tuner.py` — `AutoConcurrencyTuner`.
- `download_integrity.py` — strong ETag parsing, exact `Content-Range`/length
  validation, and atomic whole-file resume-metadata load/save/clear helpers.
- `domain_health.py` — persistent per-user `DomainHealthTracker`; sync and
  async request paths record 429/503 incidents to recognize throttled domains.

**Layer 6 — orchestration.**

- `core.py` + `_core/` — the `MirrorURL` class. See the next section.

**Layer 7 — entry point.**

- `cli.py` — `setup_shared_logging`, `main` (plus its helpers `_explicit_cli_dests`,
  `_cli_overrides`, and `_effective_args`, which decide what a `--config` run overrides).
- `__main__.py` — thin wrapper so `python -m mirror_url` calls `cli.main`.

---

## The MirrorURL orchestrator and its mixins

`MirrorURL` was ~3,600 lines as a single class. It is now composed from focused
**mixins** under the private `_core/` subpackage. `core.py` is a thin composer:

```python
class MirrorURL(
    UrlMixin,  # _core/urls.py
    ScanMixin,  # _core/scan.py
    CompareMixin,  # _core/compare.py
    DownloadMixin,  # _core/downloads.py
    CleanupMixin,  # _core/cleanup.py
    ReportMixin,  # _core/report.py
    _MirrorBase,  # _core/_base.py — __init__ + shared state, listed LAST
): ...
```

**Why this is safe and unambiguous:** every method is defined in exactly one
mixin, so the MRO never has to disambiguate. `_MirrorBase` is listed **last** so
that it sits at the base of the MRO and owns `__init__` plus all shared instance
state; the feature mixins are "above" it and call into the state it sets up.
Behavior is identical to the pre-split class — `from mirror_url.core import
MirrorURL` is unchanged for callers.

**Responsibilities and key methods per mixin:**

<!-- HTML tables keep the first column on one line in GitHub.
     Use samp for first-column code: GitHub wraps code inside nowrap cells. -->

<table>
<thead>
<tr>
<th scope="col" nowrap>Mixin (<samp>_core/…</samp>)</th>
<th scope="col">Responsibility</th>
<th scope="col">Representative methods</th>
</tr>
</thead>
<tbody>
<tr>
<td nowrap><samp>_MirrorBase</samp> (<samp>_base.py</samp>)</td>
<td>Construction, shared state, lifecycle, logging, connection bring-up, the on-disk caches, disk-space checks</td>
<td><code>__init__</code>, <code>__enter__</code>/<code>__exit__</code>, <code>cleanup</code>, <code>setup_logging</code>, <code>test_connection</code>, <code>_warm_up_connections</code>, <code>check_disk_space</code>, <code>install_signal_handlers</code></td>
</tr>
<tr>
<td nowrap><samp>UrlMixin</samp> (<samp>urls.py</samp>)</td>
<td>URL scheme/scope validation, path extraction</td>
<td><code>_validate_url_scheme</code>, <code>_is_url_within_scope</code>, <code>_is_within_target_scope</code>, <code>_is_dir_excluded</code>, <code>_get_target_base_url</code>, <code>_parse_url_cached</code>, <code>_get_filename_fast</code></td>
</tr>
<tr>
<td nowrap><samp>ScanMixin</samp> (<samp>scan.py</samp>)</td>
<td>Remote discovery, filtering, symlink tracking</td>
<td><code>get_remote_files</code>, <code>_discover_directories_bfs</code>, <code>matches_filter</code>, <code>get_directory_signature</code>, <code>is_symlink</code>/<code>record_symlink</code>, <code>_get_local_path_from_url</code></td>
</tr>
<tr>
<td nowrap><samp>CompareMixin</samp> (<samp>compare.py</samp>)</td>
<td>"Is the local copy up to date?" — local identity plus remote size/timestamp/ETag, sync and async</td>
<td><code>file_exists_and_up_to_date</code>, <code>_check_files_sync</code>, <code>_check_files_async</code>, <code>_comparison_metadata</code>, <code>_response_is_current</code>, <code>get_remote_timestamp</code>, <code>get_directory_size</code></td>
</tr>
<tr>
<td nowrap><samp>DownloadMixin</samp> (<samp>downloads.py</samp>)</td>
<td>Per-file download orchestration (delegates to the <code>download.py</code> engines)</td>
<td><code>download_file_with_resume</code>, <code>_download_file_single</code></td>
</tr>
<tr>
<td nowrap><samp>CleanupMixin</samp> (<samp>cleanup.py</samp>)</td>
<td>Removing/moving local files no longer present remotely</td>
<td><code>clean_obsolete</code>, <code>_scan_local_tree</code>, <code>_cleanup_path_selected</code></td>
</tr>
<tr>
<td nowrap><samp>ReportMixin</samp> (<samp>report.py</samp>)</td>
<td>The top-level <code>sync()</code> driver, summaries, benchmarking</td>
<td><code>sync</code>, <code>_print_early_exit_summary</code>, <code>benchmark</code></td>
</tr>
</tbody>
</table>

**Working rule:** when you add a method to `MirrorURL`, put it in the mixin whose
responsibility it matches, and keep shared attributes initialized in
`_MirrorBase.__init__`. Don't add a second `__init__` to a feature mixin.

`_core/_typing.py` declares the shared state and method signatures as the
`MirrorHost` protocol. Each mixin and `_MirrorBase` inherits this contract only
while type checking; its runtime base remains `object`. Update the contract
alongside changes to shared fields or cross-mixin signatures. Optional managers
and paths must be narrowed before use. The contract has no runtime methods,
state, or imports, and the composed class keeps the same MRO.

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
   in-memory parsed-listing caches. The resulting remote file list is
   deduplicated and preflighted for unsafe, reserved, or colliding local paths.
   `ReportMixin.sync()` calls `_validate_remote_paths()` before per-file
   comparison and downloads. `filename_mapping.FilenameMap` checks every file
   and directory prefix. Unicode-normalized aliases and exact file/directory
   conflicts remain rejected. Case-folded candidate pairs trigger an actual
   filesystem probe in their parent (or nearest existing ancestor). A confirmed
   case-sensitive parent permits distinct original spellings; an insensitive
   parent fails. Parent caches use spelling-preserving tuples because Windows
   `Path` equality folds case even for case-sensitive directories. Existing
   directory entries are checked against requested names so an insensitive
   lookup cannot overwrite or skip a differently capitalized local file.
   The random exclusive probe is removed only after confirming its regular
   file type, single link and unchanged device, inode, size and timestamps;
   unexpected entries are preserved and inspection/probe failures fail closed. A preflight
   exception is caught by `sync()`, which logs a fatal error and returns
   `False`: no file in the affected suffix is downloaded, and obsolete-file
   cleanup is not reached. Discovery-only listing modes bypass this check.
   Lossy-name, reserved-state and unsafe-path rejection remain in place.
   Probe checks also run during dry-run preflight.
   The [User Guide](./USER_GUIDE.md#filename-collisions-and-download-behavior)
   documents the NASA pair and the storage and preflight requirements for
   preserving both original files. Directory exclusion reduces the mirrored
   scope and does not resolve complete mirroring.
4. **Compare (`CompareMixin`).** For each remote file, decide whether the local
   copy is current using a shared sync/async size, timestamp, and ETag policy.
   Cached ETags require matching local size/mtime/ctime metadata, and directory
   signatures never validate child file contents. When `async_metadata` is
   enabled and there are more than 80 remote files, HEAD checks can use the
   async manager; smaller batches, dry runs, and fallback paths use sync checks.
5. **Download (`DownloadMixin` → `download.py`).** Missing/changed files are
   fetched. `ParallelDownloadManager.auto_select_method` (or an explicit
   `DownloadMethod`) picks sequential vs. streaming-parallel vs.
   traditional-parallel chunking; `download_integrity.py` validates strong ETags,
   exact ranges, and persistent whole-file resume metadata. Streaming chunks
   write to a staging path and publish atomically after verification.
   Worker pools and the coordinator impose separate limits. Per-domain
   `CircuitBreakerManager` handles request failures; the bandwidth limiter
   throttles bytes read, and chunk work also uses `ChunkCircuitBreaker`.
6. **Cleanup (`CleanupMixin.clean_obsolete`).** Optionally preview/move/delete
   local files no longer present remotely, per `CleanupPolicy`. Preserve paths
   omitted by the scan selection and never traverse local symlinks. MOVE
   failures leave source paths intact.
7. **Report (`ReportMixin`).** Save cache metadata again after download and
   cleanup work when the scan is complete, render the normal summary, and
   optionally export metrics JSON. A failed download, incomplete scan, or
   recorded cleanup-operation failure makes `sync()` return `False`.

`MirrorURL` is a context manager — use `with MirrorURL(cfg) as mirror:` so
`__exit__`/`cleanup` tears down pools, async loops, and the health server.

---

## Runtime guarantees and compatibility

These invariants are part of the current implementation:

- **URL identity:** discovery, mapping, exclusions and cleanup share
  `utils._relative_url_path()`: parse with `urlsplit`, validate origin and
  traversal, and decode path identity once. Repeated decoding only detects
  traversal; query/fragment text never becomes a local filename. Directory
  classification uses the parsed path, and BFS deduplicates directory aliases
  independently of query strings. URL decode errors reject the request.
- **Local paths:** construction validates the selected destination before
  starting managers or resolving local symlinks away. The fixed macOS `/var`,
  `/tmp` and `/etc` aliases are accepted; user destination/suffix symlinks are
  rejected, and descendant/leaf symlinks block mapping. Remote-path preflight
  rejects truncation, control-character removal and reserved-name rewriting
  before downloads. Cooperative locks coordinate participating MirrorURL
  processes; local path checks do not isolate the filesystem from external
  writers changing the tree.
- **Publication:** whole-file partials use an owned `.mirror-url-state/` below
  the target directory; final assembly and streaming staging stay on the
  destination filesystem. Verification precedes `os.replace`, so failures
  preserve an existing destination. Temporary chunk files can use another
  filesystem. Remote collisions with reserved state or sanitized local paths
  fail preflight.
- **Integrity:** chunk responses need a strong ETag and exact ranges/lengths.
  Whole-file resume metadata binds URL, size, and strong ETag. A 200 response
  restarts a resumed file; 416 causes a fresh request. Directory validators
  never establish child-file freshness, and no remote cryptographic digest
  comparison is performed.
- **Cleanup:** walk the local tree once, preserve excluded/depth-limited and
  skipped-symlink paths, local symlinks, and reserved state. Incomplete scans
  suppress obsolete-file actions. A complete empty scan may clean the selected
  local files. Unsafe expected paths fail the run and suppress cleanup. MOVE
  checks archive destinations before creating parents and again after selecting
  a collision name; symlinked archives and occupied timestamp names fail while
  preserving source/archive bytes. Directory inspection and removal exceptions
  contribute to cleanup failure metrics and fail the sync. MOVE failures
  preserve their source and do not become deletion.
- **Response ownership:** streamed sync requests keep their coordinator lease
  until the body/response is closed. Always close responses on success, error,
  cancellation, and retry paths.
- **Async admission:** `LoopLocalPrimitive` binds primitives only in the running
  loop and rejects sharing across two open loops. `ResizableSemaphore` changes
  the live limit without losing waiters or leases. Non-adaptive metadata uses
  `async_workers`; adaptive admission is also bounded by its maximum of 50.
- **Lifecycle:** library construction does not install process signal handlers.
  The CLI calls `install_signal_handlers()` on the main thread; cleanup restores
  previous handlers. Graceful CLI interruption currently exits 0, while forced
  cleanup timeout exits 1; neither implies that an interrupted sync completed.

The sync pipeline scans directories sequentially and materializes the remote
file list in memory. `scan_mode`, `parallel_threshold`, `download_queue_size`,
the batch-size settings, and disk-backed remote tracking do not select another
pipeline or impose a list-memory bound. `FileSystemCache` is available as a
component, but freshness checks use direct filesystem stats. The scanner's
`batch_processor` supplies parser statistics; it is not a bounded download
batch scheduler.

Other accepted compatibility settings include RGET-LIST options,
`http2_pipelining`, `circuit_breaker_downloads`, `chunk_timeout_multiplier`,
`stats`, and `content_hash_small_files`. Check actual readers before documenting
an effect. Seven reserved model fields also emit warnings when explicitly
changed: see `_UNUSED_CONFIG_FIELDS` and `warn_unused_fields()` in `config.py`.

Cached file ETags are bound to size, `st_mtime_ns`, and `st_ctime_ns` by
`CompareMixin._comparison_metadata`. These stat fields do not prove content
identity: same-size local edits with unchanged timestamps can be missed.
Metadata-change regressions must explicitly advance the file timestamp rather
than rely on native clock resolution. With `verify_content=True`, the size and
SHA-256 receipt instead validate local identity; timestamp-only changes do not
invalidate an otherwise matching receipt. Missing or invalid receipts fail
closed even when a HEAD response reports matching size/time. Async checks run
the hashing step in `_meta_check_executor` to avoid blocking the event loop.
Content-verification runs disable the enclosing 30-second check and 120-second
batch deadlines while retaining the 15-second HEAD timeout. Otherwise a hash
can outlive its cancelled task and be repeated by fallback workers, causing
unnecessary downloads of large unchanged files.

`download_integrity.file_sha256` reads regular files in 1 MiB chunks and rejects
observable stat changes during hashing. The single, assembled and streaming
download paths hash the completed staging file before `os.replace`, then pass
the digest to `CacheManager.save_file_metadata(..., sha256=digest)`. Cache entries
can contain a receipt without an ETag. Cache saves use `os.replace` so existing
JSON can be replaced on Windows. Legacy receipts are not invented from an
unverified destination: missing receipts require downloading the file again.
This mode adds local verification; remote freshness retains the HTTP policy.

The regression contracts live in `test_release_audit_regressions.py`, the
download failure/integrity/storage-fault tests, HTTP workflow tests, and config
precedence tests. Keep those observable contracts intact during refactoring.

---

## The configuration system

Transfer configuration includes `mode` (`mirror` or `download`),
`backend` (`httpx` or `aiohttp`), `url_list`, `overwrite` and
`requests_per_second`. The default rate remains 20 requests/second with 50 ms
minimum spacing. `effective_request_interval` is the maximum of both limits;
zero for both explicitly requests unpaced traffic. Neither option changes
security policy. CLI overrides use the existing explicit-argument precedence
rules for YAML/JSON too.

The single-URL shortcut uses `download_url` as an alternative to
`url_list`. `--mode download FILE_URL` supplies the parent URL, current working
directory and a destination-specific system temporary log folder only for
omitted target fields. Explicit scope/paths win. CLI source selection clears
the other configured source; the model and transfer entry point reject two
sources or no source. Standalone models still require all three target fields.

`transfers.py` owns the shared async whole-file pipeline. Its backend adapters
provide raw bytes and response headers; the controller owns redirects, a
monotonic request budget, aggregate bandwidth, fixed workers, full filename
preflight, disk headroom, retry bounds, scratch leases and atomic publication.
The HTTPX adapter uses `SecureAsyncTransport`; the optional aiohttp resolver
returns the validated public address directly to its connector, preserving the
original hostname for Host and TLS. aiohttp requests retain encoded URL paths,
use HTTP/1.1 and disable implicit redirects, decompression, cookies and proxies.

`download_url_list(MirrorConfig(mode="download", ...))` takes destination/log
ownership before creating state. It consumes either `url_list` or one
`download_url` through the same plan, backend, scratch and publication path.
Receipts are namespaced by the scoped base URL
under `.mirror-url-state`, and existing payloads require a matching receipt or
explicit overwrite. It never calls the scanner or metadata freshness layer.
`ReportMixin.sync()` uses the same pipeline for `backend="aiohttp"` after its
normal discovery and checks, then saves results through `CacheManager`.
HTTPX remains the default mirror implementation, including ranged resume and
chunks. The whole-file pipeline preserves those existing partials.

The new candidate needs its own CI and full qualification runs. Frozen
validation of an older source snapshot does not cover this implementation.

`MirrorConfig` is the validated pydantic runtime model, including Paths, enums,
flags and numeric bounds. `ConfigSchema` is a compatibility alias for this same
model, so standalone file validation and runtime construction agree.

```
CLI args ──┐
           ├─► cli.main(): merge explicit overrides ─► MirrorConfig ─► MirrorURL
config file┘   (expand environment variables, then validate merged values)
```

`validate_config_file()` validates a standalone complete YAML/JSON file.
`load_config_from_args()` remains a separate public helper. The CLI allows
required URL/path values to come from explicit arguments before validating
its final merged model. Benchmark mode uses the same merge precedence.
`load_config_from_args()` maps an already populated argparse namespace; it does
not read `args.config` or implement the CLI's explicit-override detection.

Model bounds can raise pydantic `ValidationError`; cross-field/URL checks can
raise `ConfigError`. `extra="forbid"` rejects unknown fields. Environment
expansion leaves unset `${VAR}` placeholders intact. Logging handler setup is
controlled by CLI logging flags, rather than config-file verbosity fields.
For listing modes, CLI-only runs supply scratch paths and a shallow directory
depth; config-file runs still require URL/destination/log fields and use their
model/file depth unless explicitly overridden. Library/config-file listing
modes are validated, and an explicit CLI listing choice disables the opposing
file mode.

---

## Subsystem deep-dives

**SSRF-hardened transport (`transport.py`).** `SecureTransport` /
`SecureAsyncTransport` wrap httpx and reject requests whose resolved address is
loopback or private. This is a security boundary: do not weaken it for
convenience. Both accept a `test_mode` flag that relaxes the guard; this is how
integration tests hit a local server (see [Testing](#testing)). Note the flag is
not wired from `MirrorConfig` and should remain test-only. Existing HTTP tests
install scoped `monkeypatch` transport bypasses or replace the fixture's pooled
client; they need no new production configuration flag. DNS validation
classifies every returned answer, including IPv6 link-local addresses, and
rejects mixed public/non-public results. URL decode exceptions reject requests.

**Directory parsing (`parsing.py`).** Lightweight-parser selection when lxml
is unavailable is independent of the lxml-failure fallback flag. Disabling
`fast_parsing_fallback` does not require lxml to be installed. Scanner HTML
statistics read the live `CacheManager` cache.

**Circuit breakers (`circuit_breaker.py`).** `CircuitBreakerManager` keeps one
breaker per domain, created lazily via `get_breaker(domain)`. State transitions
are `CLOSED → OPEN → HALF_OPEN → CLOSED`. A historical bug (fixed in 3.1.13) was
that the manager's `record_*`/`can_execute` methods didn't lazily create the
breaker, so production domains never tripped — when changing this code, keep the
lazy-creation path intact and covered by tests.

**Concurrency (`concurrency.py`).** `UnifiedConcurrencyManager` coordinates selected sync and chunk work.
Metadata and file executors also have independent limits; there is no single
cap covering every thread and async task. Streamed requests keep their
coordinator lease until their response is closed. Always close streamed
responses in a `finally`.

The standalone `submit_to_shared_pool()` helper rechecks its condition after
every wakeup and rejects submission during shutdown. Completion includes failed
and cancelled jobs; each contributes once to the failure count. Its `queue_size`
argument is accepted for compatibility and does not bound pending submissions.
`use_shared_thread_pool` enables the coordinator's chunk pool; file transfers
and metadata comparisons retain separate executors. The ordinary chunk path
reuses the raw executor and acquires its own coordinator leases.

**Async path (`async_connection.py`).** `AdaptiveAsyncManager` tunes its
concurrency from measured RTT, throughput, and error rate; `AsyncTaskManager`
runs the metadata HEAD checks. The non-adaptive `AsyncConnectionManager` has
its own fixed admission and client lifecycle; do not assume adaptive-only
attributes exist on it. Both request paths validate redirect scope and apply
retry/backoff handling to 429 and retryable 5xx responses.

**Cache (`cache.py`).** `CacheManager` owns the JSON cache lifecycle: load +
validate (`_validate_and_sanitize_cache`), schema-version gating
(`CACHE_SCHEMA_VERSION`), atomic save via a temp file, and corrupted-file backup.
The cache *filename* is built in `_MirrorBase.__init__`, not here. Directory
metadata is saved after a completed scan and again at sync completion; file
identity metadata is updated after successful publication. Parsed-listing LRU
caches are in memory, not persisted/restored HTML caches. `refresh_timestamp()`
changes expiry metadata only and is used by `quick` mode.

---

## Extension recipes

These are the common changes and the exact touch-points.

### Add a configuration option

1. Add the field once to `MirrorConfig` in `config.py`, with a sensible default
   and pydantic validation bounds. `ConfigSchema` is the same class, so there
   is no second schema to edit.
2. If it should be settable from the CLI, follow the flag recipe below: update
   parser/mapping and direct constructors as well as the public namespace helper.
3. Read `self.config.<field>` where the behavior lives (a mixin or a subsystem).
4. Add a test (a config round-trip test for the field; a behavior test for the
   effect). Document it in `USER_GUIDE.md` if user-facing.

### Add a CLI flag

Flags are defined directly on `parser`/the argument groups in `cli.main()`.
Add the `argparse` argument there, matching its `dest` to the `MirrorConfig`
field name whenever possible — `main()`'s `--config` branch picks up any flag
whose `dest` equals a `MirrorConfig` field automatically (via
`_cli_overrides`); only a `dest` that differs from the field name (or that
needs special handling, e.g. the three mutually-exclusive download-mode
flags) needs an entry in `_CLI_DEST_TO_CONFIG_KEY` or a branch in
`_cli_overrides`. The non-`--config` branch (`MirrorConfig(...)` call further
down `main()`), its benchmark constructor, and `load_config_from_args()`
(the separate public namespace helper) also need the field passed explicitly.
Test no-config, config-file override, and benchmark paths. Keep
`--help` text consistent with the User Guide's option tables, and add the new
option's row to the `TYPED`/boolean-flag tables in
`tests/test_cli_config_precedence.py` (a missing row fails
`test_typed_table_covers_every_valued_option`).
Review the complete `python -m mirror_url --help` output too: both the option
descriptions and the parser's example epilog must agree with current behavior
and the User Guide. Flag-existence checks alone do not verify those claims.

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

Place it with related responsibilities and keep module-level runtime imports
acyclic; the historical layer numbers do not prohibit all same-layer imports.
Wire it into
`MirrorURL` from `_MirrorBase.__init__` (layer 6 is where composition happens).
Add unit tests at its own layer with no higher-layer setup.

---

## Coding conventions

- **`from __future__ import annotations`** at the top of every module. Annotations
  are lazy strings, which lets us reference types without import cycles and use
  modern annotation forms on the supported Python 3.10+ runtimes.
- **Typing style:** follow the existing `Dict`, `List`, `Optional`, and `Union`
  conventions in existing modules. Python 3.10 supports `dict[...]` and
  `X | None`; the lint config still omits pyupgrade (`UP`) and most `SIM` rules
  to avoid unrelated typing rewrites.
- **`TYPE_CHECKING` guards** for imports needed only for annotations, to keep the
  import graph acyclic.
- **Lint rule set:** ruff with `E, F, W, I, B, C4`. A few bugbear rules
  (`B007`) and `E501`/`B008` are ignored — see `pyproject.toml`.
  `B019` and `B904` are enforced; URL parsing is cached at module scope.
- **Formatting:** Ruff 0.16.8 (`ruff format`, line length 100), matching CI and pre-commit.
- **Type-checking:** `mypy` is required in CI and must report zero errors. The
  settings remain lenient (`no_implicit_optional = false`, untyped definitions
  allowed, untyped bodies unchecked). This is not a strict-typing guarantee;
  tightening the settings is a dedicated follow-up. Its checking target is
  Python 3.10, matching the package's runtime minimum.
- **Imports:** keep the runtime graph acyclic. The layer diagram is a guide to
  responsibilities; inspect actual imports rather than treating the numbers as
  a strict dependency rule.

---

## Testing

The suite lives in `tests/` and runs under `pytest`. Test lanes:

- **Fast lane** (`pytest -m "not integration"`) — smoke, utilities, security, and
  subsystem-integration tests that exercise real in-process I/O (thread-safe
  primitives under concurrent load, circuit-breaker timing, `DiskBackedSet`
  spill-to-disk, pydantic/YAML config round-trips). This lane also includes the
  unmarked streaming-concurrency tests, which bind a local HTTP server. It
  therefore needs loopback sockets, although it does not require a live public
  archive. CI runs it across Python 3.10–3.14.
- **Integration lane** (`pytest -m integration`) — end-to-end mirrors in
  `test_integration.py` and `test_http_mirror_workflows.py`, using the static
  server fixture or a controllable Range/ETag/failure server.
- **Full coverage lane** — both lanes together, plus 100% statement/branch
  gates for the two download modules, eight safety modules and shared URL scope
  helpers (see below). It requires all optional dependencies
  and local socket binding. A sandbox socket denial is an environment error,
  not a successful full-suite run.

**End-to-end tests:** `tests/test_integration.py` runs a real local HTTP
mirror with a transport bypass installed only by `monkeypatch` in that test.
The server fixture serves a dedicated `served/` directory. No production
configuration flag bypasses the transport's private-IP guard.

**Where to add tests:**

- Pure-logic, low-layer code (`utils`, `security`, `primitives`,
  `circuit_breaker`, `rate_limiter`, `parsing`) → fast unit tests; this is where
  the bulk of coverage should live.
- Managers → component tests injecting a fake/test httpx transport.
- Whole-run behavior → integration tests against the fixture server.

Markers are declared in `pyproject.toml` under `[tool.pytest.ini_options]`
(`--strict-markers` is on, so register new markers there).

---

## Unused code review

Use static findings as candidates, then verify calls and attribute receivers:

```bash
python -m pip install vulture
python -m vulture src/mirror_url --min-confidence 80
python -m vulture src/mirror_url tests --min-confidence 80
```

For each candidate, inspect repository call sites, `__all__` exports, inheritance,
dynamic lookup, and framework dispatch. Pydantic validators, HTTP/HTML handler
hooks, signals, context managers, and pytest fixtures may have no ordinary call
expression. Required callback parameters remain even when unused. An export or
test-only call does not prove participation in the production sync workflow.

Record whether a finding is internal dead state, an intentionally dormant public
helper, a compatibility field, or a framework hook. Do not remove a public API
solely because the repository has no production caller. Examples retained for
library users include `get_remote_timestamp`, `retry_with_backoff`,
`compute_file_hash`, `AdaptiveBatchProcessor.record_batch`, and
`FileSystemCache.get_stat`.

Match receivers precisely: `DirectoryScanner.batch_processor` has parser-stat
readers; that does not establish a reader for an attribute with the same name
on a `MirrorURL` instance. Verify actual per-IP semaphores and cache locks before
removing synchronization. Vulture is a review aid, not a zero-findings CI gate.

---

## Build, lint, and type-check

```bash
# editable install with the dev toolchain
pip install -e ".[all,dev]"
pre-commit install        # optional but recommended

ruff check .              # lint
ruff format --check .     # canonical formatter
mypy                      # required type-check of src/mirror_url (zero errors)
pytest -m "not integration"   # fast lane
pytest                        # full suite (includes integration)
pytest --cov=mirror_url --cov-branch --cov-fail-under=80 \
  --cov-report=term-missing --cov-report=xml:coverage.xml \
  --cov-report=json:coverage.json --cov-report=html
python scripts/check_download_coverage.py coverage.json  # each download module: 100%
python scripts/check_safety_coverage.py coverage.json
python scripts/check_safety_mutations.py

# build distributions
pip install build twine
python -m build           # wheel + sdist into dist/
python -m twine check dist/*

# regenerate both HTML guides after Markdown edits (requires pandoc)
bash scripts/render_guides.sh
```

CI (`.github/workflows/ci.yml`) runs lint/format checks on Python 3.12 and the
fast test lane across Python 3.10–3.14.
A separate Python 3.12 coverage job installs `[all,dev]`, runs every test,
including real local HTTP mirroring, and requires at least 80% combined
statement/branch coverage overall. It additionally requires 100% statement and
branch coverage separately for `download.py` and `download_integrity.py`, using
the exact missing counts in the JSON report. It uploads HTML, JSON, and XML
reports. The safety gate separately requires 100% statements and branches for
`scanner.py`, `_core/scan.py`, `_core/urls.py`, `_core/cleanup.py`, `security.py`,
`filename_mapping.py`, `transport.py`, `destination_lock.py` and `scratch.py`,
plus `utils.url_within_scope` and `utils._relative_url_path`.
It uses exact missing counts and checks helper exclusions; unrelated uncovered
utility functions do not get hidden or counted as covered.

Hypothesis generates once-decoded path identities, origin boundaries and nested
traversal encodings. Filesystem tests inject permission, resolution, listing and
move failures, and assert preserved source and archive bytes. Local HTTP tests
assert that malformed listings and lossy filenames fail before destructive
cleanup or publication. `test_filename_mapping.py` checks the native filesystem
and can use a separately mounted case-sensitive APFS test image through
`MIRROR_URL_CASE_TEST_VOLUME`. Real HTTP tests preserve both NASA-style names,
different contents and separate receipts through all modes and repeat syncs;
insensitive filesystem tests require failure before any selected file payload.
Additional tests preserve preexisting aliases and replaced/shared probe entries.
The mutation check temporarily weakens twenty-one guards:
mixed DNS rejection, archive collision rejection, incomplete-scan cleanup,
same-origin scope, address pinning, lossy filename preflight and content receipt
verification, the long-hash deadline policy, destination locking and retention
of abandoned-worker ownership, scratch manifest ownership, live work leases and
late chunk writer cleanup, case-insensitive filename collision rejection,
probe inode/metadata identity, timestamp file-handle identity and preservation
of existing filename aliases. Each selected
test must first pass against the original source and then fail an assertion
against the mutation. Collection or environment errors do not count as success.
These checks improve evidence of correctness; neither 100% coverage nor this
small mutation set guarantees that all bugs have been found.

`test_process_recovery.py` starts real subprocesses against the local HTTP
fixture, kills them at deterministic IO checkpoints, then restarts the same
destination. Checkpoints cover partial/chunk writes, both sides of publication,
partial cache JSON serialization and a completed MOVE before cleanup finishes.
All three download modes assert complete destination bytes, preserved obsolete
bytes, persisted receipts and a subsequent sync without another file download.
`process_recovery_worker.py` supplies test-only checkpoints; production has no
crash hooks or loopback security bypass. Process kill tests do not simulate
power loss or arbitrary external writers. A competing process must fail at
every checkpoint before the owner is killed. `test_process_destination_lock.py`
also checks simultaneous starts, parent/child overlap, independent trees, shared
state, normal release, hard-kill recovery and cleanup while an HTTP writer is
blocked. Native CI runs these tests on Windows and macOS too.

For a 24-hour synthetic soak, use the dev environment and a new output directory:

```bash
PYTHONPATH=src python scripts/run_local_soak.py \
  --output /path/to/new-soak-directory --duration-hours 24 --interval 60
```

The harness cycles through all download modes, repairs same-size corruption,
preserves files on failed listings, checks MOVE collisions and recovers dropped
connections and process kills. It writes atomic `status.json`, source hashes,
resource samples and bounded logs. Freeze `src/`, `tests/` and `scripts/` for a
long run: source changes invalidate the result. With optional psutil, resource
checks include RSS and descriptors/handles; thread counts are always checked.
Each completed cycle also asserts zero remaining owned temporary workspaces and
bytes, and checks MOVE archive payload against the fixture's known legitimate
files. Filesystem free space is sampled separately because other applications
can change it. Crash worker output is appended with cycle/checkpoint markers and
bounded rotation, allowing a full review of retained worker and main logs.
An elapsed short smoke run is not long-duration evidence. Local synthetic tests
also do not replace a representative archive/destination soak.

Native macOS and Windows CI jobs also run the full suite on Python 3.12 with
core dependencies and with all optional dependencies. The coverage gate runs
on Linux with optional dependencies; its results do not claim native Windows
branches were executed on a different OS.

`scripts/render_guides.sh` renders both Markdown guides with Pandoc and
the committed `docs/guide-style.html`. Its Lua filter points cross-guide links
to HTML copies. It keeps the Markdown's table of contents and emits one title
per guide; run it after editing either guide and review the
HTML too. Keep Ruff's version in `pyproject.toml`, `.pre-commit-config.yaml`,
and documentation synchronized when upgrading the formatter.

---

## Release process

1. **Bump the version with `bash scripts/bump_version.sh X.Y.Z`.** It updates
   `pyproject.toml`, `src/mirror_url/_version.py`, and the version references in
   both Markdown/HTML guides. The CLI imports the shared version; no separate
   banner edit is needed. A test checks that the two version sources agree.
   The script replaces every occurrence of the current version in those files,
   including historical mentions of that exact version; review the diff and
   preserve history manually when needed. Regenerate HTML after content edits.
2. **Update `CHANGELOG.md`.** Replace the release's `[Unreleased]` heading with
   `## [X.Y.Z] - YYYY-MM-DD`, retain its notes, and remove any empty duplicate
   `[Unreleased]` section. For a release with no pending changes, leave no
   `[Unreleased]` heading. Follow Keep a Changelog (`### Added/Changed/Fixed`).
3. **Validate and commit the release.** Run `mypy`, `ruff check .`,
   `ruff format --check .`, the full `pytest` suite, `python -m build`,
   `twine check dist/*`, and `git diff --check`. All checks must pass.
   Include the full branch-coverage run, both coverage gates and the safety
   mutation check shown above, and require release PR CI to pass before tagging.
   Commit the source metadata, changelog, and guides; push the release branch
   and merge its reviewed pull request.
4. **Tag the merged release on `main` and push the tag:**
   ```bash
   git switch main
   git pull --ff-only origin main
   git tag -a vX.Y.Z -m "mirror-url X.Y.Z"
   git push origin vX.Y.Z
   ```
5. The tag triggers `.github/workflows/release.yml`, which builds the wheel/sdist,
   creates a GitHub Release, and (when configured) publishes to PyPI via **Trusted
   Publishing** (OIDC — no API token). The PyPI step requires a one-time setup: a
   pending publisher on PyPI (`owner` = repo owner, workflow `release.yml`,
   environment `pypi`) and a matching `pypi` Environment in the repo settings.
   The PyPI job also requires the repository variable `PUBLISH_TO_PYPI=true`.
   Without that opt-in, it is skipped; the tag still builds the wheel and
   creates the GitHub Release. Verify the version and changelog before creating
   the tag.

---

## Known hazards and gotchas

Preserve these constraints when extending or refactoring the current code.

- **Don't reintroduce the self-import.** The monolith's scope check imported
  `MirrorURL`, which was a packaging hazard. `ConnectionManager` now uses the
  stateless scope helper in `utils`; prefer injection over reaching up to `core`.
- **Keep subclass families in one module.** `PerIPRateLimiter`/
  `ChunkAwareRateLimiter` subclass `RateLimiter`; `ChunkCircuitBreaker` subclasses
  `CircuitBreaker`. Splitting a base from its subclasses across modules invites
  import cycles.
- **Centralize optional-dep probes in `compat.py`.** Import the
  `*_AVAILABLE` flag; never re-do the `try/except import` elsewhere.
- **Shared module globals** (e.g. log bookkeeping used by `setup_shared_logging`/
  `cleanup_log_files`) must live in one module and be imported, not re-declared,
  or you get divergent copies.
- **Preserve bug-fix behavior.** The changelog documents subtle
  attribute/phantom-method fixes (async HEAD, async transport `test_mode`,
  circuit-breaker lazy creation, redirect header preservation, MOVE-mode cleanup).
  Don't "tidy" these away while refactoring nearby code.


---

## Quick "where do I find…" map

<table>
<thead>
<tr>
<th scope="col" nowrap>I want to change…</th>
<th scope="col">Go to</th>
</tr>
</thead>
<tbody>
<tr>
<td nowrap>A tuning default or limit</td>
<td><code>constants.py</code></td>
</tr>
<tr>
<td nowrap>An error type</td>
<td><code>exceptions.py</code> (+ <code>__init__.py</code> if public)</td>
</tr>
<tr>
<td nowrap>A run-mode / state enum</td>
<td><code>enums.py</code></td>
</tr>
<tr>
<td nowrap>A config field</td>
<td><code>config.py</code> (<code>MirrorConfig</code>; <code>ConfigSchema</code> is its alias) + <code>cli.py</code></td>
</tr>
<tr>
<td nowrap>URL scope/validation logic</td>
<td><code>_core/urls.py</code></td>
</tr>
<tr>
<td nowrap>How the remote tree is discovered</td>
<td><code>_core/scan.py</code> (+ <code>parsing.py</code>)</td>
</tr>
<tr>
<td nowrap>"Is the file up to date?" logic</td>
<td><code>_core/compare.py</code></td>
</tr>
<tr>
<td nowrap>How a file is actually downloaded</td>
<td><code>_core/downloads.py</code> → <code>download.py</code></td>
</tr>
<tr>
<td nowrap>Range validation / resume metadata</td>
<td><code>download_integrity.py</code></td>
</tr>
<tr>
<td nowrap>Loop binding / live async admission</td>
<td><code>async_primitives.py</code></td>
</tr>
<tr>
<td nowrap>Persistent throttled-domain knowledge</td>
<td><code>domain_health.py</code></td>
</tr>
<tr>
<td nowrap>Obsolete-file cleanup behavior</td>
<td><code>_core/cleanup.py</code></td>
</tr>
<tr>
<td nowrap>The top-level run / summary</td>
<td><code>_core/report.py</code> (<code>sync()</code>)</td>
</tr>
<tr>
<td nowrap>Construction / shared state / logging</td>
<td><code>_core/_base.py</code></td>
</tr>
<tr>
<td nowrap>The on-disk cache format/lifecycle</td>
<td><code>cache.py</code> (filename in <code>_core/_base.py</code>)</td>
</tr>
<tr>
<td nowrap>SSRF / network security boundary</td>
<td><code>transport.py</code>, <code>security.py</code></td>
</tr>
<tr>
<td nowrap>Throttling / retries / breakers</td>
<td><code>rate_limiter.py</code>, <code>connection.py</code>, <code>circuit_breaker.py</code></td>
</tr>
<tr>
<td nowrap>CLI flags / entry point</td>
<td><code>cli.py</code>, <code>__main__.py</code></td>
</tr>
<tr>
<td nowrap>The version number</td>
<td><code>_version.py</code> <strong>and</strong> <code>pyproject.toml</code></td>
</tr>
</tbody>
</table>

---

*This guide describes the architecture as of version 3.2.1. When you change the
structure, update this document in the same PR.*
