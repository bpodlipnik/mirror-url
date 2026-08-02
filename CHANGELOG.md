# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.41] - 2026-08-02

### Fixed
- **`h2` was never declared as a dependency anywhere** (base
  dependencies, `[dev]`, or any other extra), even though
  `config.http2` defaults to `True`. Without `h2` installed,
  `httpx.AsyncClient(http2=True, ...)` raises on construction inside
  `AdaptiveAsyncManager._init_client()`; `_ensure_client()` catches
  that and returns `False`, and every caller (`profile_server()`,
  `head()`, etc.) then silently and *permanently* falls back to the
  slower sync path for the entire run -- with no error ever surfaced
  to the user. Found while building tests for the concurrent
  profiling change below: a fresh venv built strictly from
  `pyproject.toml` reproduced this exactly. Fixed by depending on
  `httpx[http2]` instead of bare `httpx` in base `dependencies` --
  `h2` is a hard runtime requirement given the tool's own default,
  not an optional extra. New regression test
  (`test_ensure_client_succeeds_with_default_http2_setting`) drives
  the real `_ensure_client()`/`_init_client()` path with the
  unmodified default config (no mock transport substitution),
  verified to fail with the exact original error message when run
  against an `h2`-less environment, and to pass once `h2` is present.

  If you're on an existing install, run `pip install --upgrade
  --no-cache-dir mirror-url` to pick up the dependency, or `pip
  install h2` directly to fix it immediately without waiting for the
  next release. Check whether you were affected with `pip show h2`.

### Changed
- `AdaptiveAsyncManager.profile_server()` fired its up-to-20
  ("`PROFILE_SAMPLE_SIZE`") warm-up HEAD samples in a strictly
  sequential `for` loop, `await`-ing each one to completion before
  starting the next -- so N samples cost N × (real round-trip time),
  turning a handful of quick probes into several real seconds of pure
  warm-up before a single file transfer even started, on any server
  with non-trivial latency, despite the underlying `httpx.AsyncClient`
  already having HTTP/2 multiplexing enabled and able to genuinely
  run several requests concurrently over one connection. Samples are
  now fired concurrently via `asyncio.gather()`, bounded by a
  semaphore sized to `self._current_concurrency` -- the same value
  `_get_profile()` already set for this domain (
  `ADAPTIVE_START_CONCURRENCY` by default, or the conservative
  fallback for a `KNOWN_THROTTLED_DOMAINS`/domain-health-learned-
  throttled domain, see 3.1.38). A domain already flagged as
  sensitive is therefore still probed gently -- concurrently within
  that same conservative bound, not at a burst of up to
  `PROFILE_SAMPLE_SIZE` simultaneous requests regardless of its
  throttle history.

  9 new tests in `tests/test_profile_server_concurrency.py` prove
  genuine concurrency rather than just "still works": a mock
  transport records peak simultaneous in-flight requests and enforces
  an artificial per-request delay, so a still-sequential
  implementation would show `peak_in_flight == 1` and take
  `N × delay` wall-clock time -- versus the fixed version's
  `peak_in_flight > 1`, bounded by the configured concurrency, and
  `~ceil(N / concurrency) × delay` wall-clock time (directly asserted
  with a generous margin for CI jitter). Also covers: the concurrency
  bound is respected even when a `KNOWN_THROTTLED_DOMAINS`-style
  conservative value is set, success/error counting stays correct
  despite results arriving out of `asyncio.gather()` order, the
  existing high-error-rate-triggers-sync-fallback behavior is
  unaffected, a slow/timing-out sample doesn't block the rest of the
  batch, and the `PROFILE_SAMPLE_SIZE` cap still applies.

## [3.1.40] - 2026-08-02

### Removed
- Deleted both `get_small_content()` implementations in
  `async_connection.py` (`AsyncConnectionManager.get_small_content()`
  and `AdaptiveAsyncManager.get_small_content()`) -- confirmed zero
  callers anywhere in the codebase or tests, and not part of the
  public API (not re-exported from `__init__.py`, not documented).
  Same class of finding as the earlier removal of
  `AdaptiveAsyncManager._do_head_request()`.

  Found while investigating whether integrating `aiodns` (a c-ares-
  based true async DNS resolver) would meaningfully speed up DNS
  resolution: `AdaptiveAsyncManager.get_small_content()` had an
  uncached `socket.gethostbyname()` call on every invocation, unlike
  the tool's two other DNS-lookup call sites, which both check the
  existing 5-minute-TTL `_dns_cache` first. Before "fixing" that gap,
  checking who actually calls the method revealed the answer: nobody.
  `aiodns` itself was assessed and not pursued -- this tool mirrors
  one domain per run with many requests against it, so the existing
  cached + executor-offloaded DNS resolution already amortizes to a
  handful of real lookups per run; a native async resolver would
  shave microseconds off an operation that's already essentially free
  in this workload pattern, at the cost of a C-extension dependency
  that works against the project's "pure Python, any OS/architecture"
  design goal.

  No functional change: removed code was unreachable. Removing it
  also dropped `CONTENT_HASH_LIMIT` from the file's imports (no
  longer referenced anywhere in it). mypy's error count for the file
  dropped by 2 (12 -> 10) with the dead code gone.

## [3.1.39] - 2026-08-02

### Changed
- `clean_obsolete()` (`_core/cleanup.py`) now walks the local
  `--dest-path` tree exactly **once** per run, via a new
  `os.scandir()`-based `_scan_local_tree()` helper, instead of up to 4
  separate `Path.rglob("*")` calls that each re-walked the *entire*
  tree from scratch: the preview file-check, the preview empty-dir
  check, the main DELETE/MOVE collection, and the (now-removed)
  `_count_obsolete_files()` used only for the `--confirm-delete`
  prompt. For a large local mirror this meant walking the whole tree
  2-4x for a single `clean_obsolete()` call. Also avoids `rglob()`'s
  per-item `is_file()`/`is_dir()` calls, each of which costs its own
  `stat()` syscall even though `os.scandir()`'s `DirEntry` objects
  already carry that type information from the directory read itself
  on most platforms.

  A parallel worker pool for the walk itself was considered and
  deliberately not built: this walks the *local* destination
  filesystem, not the network, and on typical local SSD storage a
  sequential `scandir()` walk is already fast -- parallelizing local
  directory traversal adds real complexity (a thread-safe work queue,
  more failure modes) for a benefit that's speculative without an
  established local-I/O bottleneck. Can be revisited if one shows up
  in practice.

  Symlinks are still followed for file/directory classification
  (matching the previous `Path.is_file()`/`is_dir()` behavior), but a
  symlinked directory is now only ever descended into once: each
  directory's resolved real path is tracked in a `visited` set, so a
  symlink cycle terminates cleanly. This replaces (and is more
  explicit/robust than) the previous code's blanket `except
  RuntimeError` around `rglob()`, which relied on whatever
  version-dependent loop detection pathlib happened to raise.

  An unreadable subdirectory (permission error, or it disappears
  mid-walk) is logged at debug level and skipped, same as before --
  the rest of the walk continues rather than aborting entirely.

  No behavior change for callers: `clean_obsolete()`'s preview
  output, confirm-delete prompt, and DELETE/MOVE results are
  unchanged -- this only removes redundant re-walking of the same
  local tree.

  33 new tests in `tests/test_cleanup_scandir_walk.py`: the walk
  helper in isolation (file/dir collection, empty directories, a real
  symlink-cycle regression test, an unreadable-directory case via a
  monkeypatched `os.scandir()` since tests run as root in CI/sandboxes
  where real `chmod`-based permission denial doesn't apply), plus
  `clean_obsolete()` end-to-end for PREVIEW, `--dry-run`, DELETE (with
  and without `--confirm-delete`, including a regression guard that
  the confirm-prompt count and the actual deletion count can never
  diverge now that they share one walk), and MOVE mode -- the last of
  which had no dedicated test at all before this change.

## [3.1.38] - 2026-08-02

### Added
- New persistent, self-learning domain-health tracker
  (`domain_health.py`): remote domains that repeatedly send 429/503
  responses across separate mirror-url invocations are now
  automatically treated as throttled -- getting a conservative
  starting concurrency -- without needing to be hardcoded into
  `KNOWN_THROTTLED_DOMAINS` (constants.py) first. Every observed
  429 or 503 (not other 5xx like 500/502/504, which usually indicate
  a server bug or gateway issue rather than throttling) is recorded
  as an incident for that domain in a small JSON file in the user's
  cache directory (`~/.cache/mirror-url/domain_health.json` on POSIX,
  `%LOCALAPPDATA%\mirror-url\domain_health.json` on Windows) --
  shared across every invocation regardless of `--dir-suffix`/
  `--log-path`, since domain health is a property of the server, not
  of any specific mirrored subtree. A domain is considered throttled
  once it has 5 or more incidents within a trailing 14-day window;
  this decays automatically and gradually as old incidents age out,
  with no manual reset or separate expiry logic needed. The existing
  `KNOWN_THROTTLED_DOMAINS` list still applies as a day-one default
  for a handful of well-known archives -- either signal (hardcoded or
  learned) is sufficient to start conservative.

  Deliberately 429/503-only, not RTT-variance-based: a hard status
  code is an unambiguous signal, whereas RTT variance is noisy
  (network jitter, local load, a brief server hiccup all look similar
  statistically) and would need real-world calibration this project
  doesn't have yet -- can be added later once this simpler mechanism
  has proven itself.

  Thread-safe within a process; cross-process safety relies on atomic
  replace-on-write (`Path.replace`, not `Path.rename` -- the latter
  fails on Windows if the destination already exists) so a concurrent
  writer never observes a half-written or corrupted file. A lost
  update between two simultaneous writers (e.g. two mirror-url
  processes mirroring different `--dir-suffix` values of the same
  domain at once) is possible but not guarded against explicitly --
  it self-corrects over subsequent incidents, at a low cost compared
  to full file locking. Every operation degrades gracefully to a
  no-op on any I/O or parse failure (missing permissions, corrupt
  file, read-only filesystem, etc.) -- this is a best-effort
  optimization hint, never a hard dependency, and never the reason a
  mirror run fails.

  39 new tests: 21 for the tracker itself
  (`tests/test_domain_health.py` -- path resolution on POSIX/Windows,
  threshold/window logic, cross-instance persistence, corrupt-file
  and malformed-entry handling, unwritable-directory handling) plus
  18 in `tests/test_429_retry_after.py` covering incident recording
  end-to-end through the real `ConnectionManager.request()` retry
  loop (429 and 503 both recorded, other 5xx and 404 are not).

## [3.1.37] - 2026-08-01

### Fixed
- HTTP 429 (Too Many Requests) responses were treated identically to any
  other 4xx client error in the primary sync download/HEAD path
  (`connection.py`): given up on immediately, no retry, `Retry-After`
  header never read. 429 is an explicit "come back later" signal from
  the server, not a permanent failure -- for a mirroring tool that
  expects to eventually succeed, giving up on the first 429 was the
  wrong behavior. Now retries 429 specifically, honoring `Retry-After`
  when the server sends one (both the integer-seconds and HTTP-date
  forms per RFC 9110 §10.2.3, capped at the same ceiling
  `exponential_backoff()` already uses so a hostile/misconfigured
  server can't stall the run indefinitely), falling back to the usual
  exponential backoff otherwise. New `parse_retry_after()` helper in
  `utils.py`. Found while reviewing `async_connection.py` for possible
  performance work -- 429/`Retry-After` handling turned out to be the
  one genuine gap in an otherwise fairly sophisticated adaptive
  concurrency/circuit-breaker/DNS-caching setup that already existed.

  `async_connection.py`'s two `HTTPStatusError` handlers were
  deliberately left untouched: neither `AsyncConnectionManager.head()`
  nor `AdaptiveAsyncManager.head()` ever calls
  `response.raise_for_status()`, so those branches are unreachable
  dead code there -- a 429 flows through the success path instead
  (returned as-is with `status_code=429`). The caller
  (`compare.py`'s async metadata check) already falls back to the sync
  path above for any status that isn't 200/304/a safe-to-skip 4xx, so
  the fix applies end-to-end without touching the async layer.

### Removed
- Deleted `AdaptiveAsyncManager._do_head_request()` -- an entire unused
  method (own retry loop, DNS lookup, semaphore handling) with zero
  callers anywhere in the codebase, fully superseded by `head()`'s own
  inline retry logic. No functional change; confirmed via mypy's error
  count actually *dropping* by one with it gone.

16 new tests: `parse_retry_after()` unit-tested directly (both header
forms, cap, past/future dates, malformed/missing input), plus
`ConnectionManager.request()`'s new 429 branch driven end-to-end
through the real retry loop via `httpx.MockTransport` (retry-then-
succeed with and without `Retry-After`, retries exhausted raises
`MirrorConnectionError`, a longer server-requested wait isn't
shortened by the computed backoff, 404 still isn't retried).

## [3.1.36] - 2026-07-30

### Fixed
- Fixed an off-by-one in `_discover_directories_bfs()` (the shared BFS
  walk behind real syncs, `--list-dirs`, and `--list-files`): a
  directory sitting exactly *at* `--max-depth` was still fetched
  (`scanner.scan_directory_sequential()`, a real HTTP request) purely
  to discover its children -- but those children land one level past
  `max_depth`, so they get silently discarded the instant they're
  popped, without ever being scanned. The fetch was 100% wasted I/O:
  a full network round-trip per directory at the deepest listed level,
  for a result nothing ever used.

  Invisible at the old default of `max_depth=50` on typical (shallow)
  trees, this became glaring with 3.1.35's new `--list-dirs` default
  of `max_depth=1`: reported in production as `--list-dirs` against a
  271-subdirectory archive taking 42s, vs. ~2s for an equivalent
  single-page `curl` fetch of the same listing -- because it was
  fetching all 271 children's pages just to list the children
  themselves, turning a one-request "what's in this folder" probe
  into 272 requests.

  Fix: only scan a directory (and only apply the per-request
  rate-limit wait) when `depth < self.config.max_depth` -- i.e. only
  when there's still depth budget left to use whatever children it
  might yield. The directory itself is still yielded/listed
  regardless; only the now-provably-unnecessary network request is
  skipped. Real syncs and `--list-files` are unaffected in terms of
  files collected (each independently re-scans every yielded
  directory for its own files/content), so this changes total real
  HTTP requests only where such compensating re-scans don't exist --
  which is exactly `--list-dirs`'s own bare directory-discovery pass.

  5 new tests in `tests/test_bfs_depth_boundary_scan.py`, asserting
  exact scan-call counts at several `--max-depth` values against a
  stubbed scanner (including the max_depth=0 edge case and confirming
  a scan exception can't fire for a directory that's never scanned).

## [3.1.35] - 2026-07-30

### Changed
- `--list-dirs` now defaults `--max-depth` to `1` (the current folder's
  immediate children only) instead of the usual `MAX_DIRECTORY_DEPTH`
  (50). Reported in production: `--list-dirs` against a multi-level
  archive (date directories each containing further subdirectories)
  silently recursed the full 50 levels, printing every nested
  subdirectory instead of just the top level -- "what's in this folder"
  is the overwhelmingly common ask. An explicit `--max-depth` always
  overrides this, for every mode including `--list-dirs`. Every other
  mode (`--list-files`, real syncs) is unaffected and keeps the original
  default. New `LIST_DIRS_DEFAULT_MAX_DEPTH` constant; `--max-depth`'s
  argparse default became the sentinel `None` so `cli.py` can tell
  whether it was passed explicitly, resolved right after parsing.

### Added
- `--list-dirs`'s output is now always followed by a
  `# Directories N/total` summary line on stdout, mirroring
  `--list-files`' existing `# Files N/total` convention -- including
  unrestricted (no-`N`) runs, where `N == total`. Drop it with
  `grep -v '^#'` for a pure one-directory-per-line stream.

5 new tests for the max-depth default resolution
(`tests/test_list_dirs_default_max_depth.py`); existing stdout/log
assertions in `test_list_dirs.py`, `test_list_dirs_stdout.py`, and
`test_list_dirs_n.py` updated for the new summary line. USER_GUIDE.md
updated (table rows + detailed section); USER_GUIDE.html regenerated.

## [3.1.34] - 2026-07-29

### Added
- `--list-dirs` now accepts an optional `N`, mirroring `--list-files [N]`:
  `--list-dirs 3` prints only the last 3 discovered directories. With no
  `N`, behavior is unchanged (every directory, in discovery order).
  Directories have no natural per-parent grouping the way files do, so `N`
  ranks lexicographically across the *entire* discovered tree for a
  suffix, always excluding the root (`.`) from that ranking (it isn't a
  real `--dir-suffix` candidate). This directly replaces the common
  `--list-dirs | grep -v '^\.$' | sort | tail -n N` pipeline with a
  built-in equivalent. New `list_dirs_n` field on `MirrorConfig`/
  `ConfigSchema`, threaded through the plain-CLI constructor, the
  `--config` (YAML) branch (base_config + CLI override), and
  `load_config_from_args()`. 8 new tests in `tests/test_list_dirs_n.py`;
  existing CLI-threading tests updated for the new argparse shape.

## [3.1.33] - 2026-07-28

### Fixed
- Fixed a crash when `--log-path` collides with an existing regular file
  (e.g. a shell wrapper doing `mirror-url ... --log-path "$LOG" >> "$LOG"`,
  which creates `$LOG` as a plain file before mirror-url runs). The
  constructor already detected this and fell back to a temp directory for
  the log file itself, but the cache-file path was computed from
  `self.log_path` *before* that fallback ran, so `CacheManager.__init__`
  still tried to create its parent directory at the broken original path
  and raised the same `FileExistsError`, aborting the run instead of just
  warning and continuing. The cache-file path is now derived after the
  log-directory fallback, so it always follows the corrected path. Found
  in production via `sync_last3_orbits.sh` accidentally passing the same
  path to both a shell log redirect and `--log-path`. 1 new regression
  test in `tests/test_subsystems.py`.

## [3.1.32] - 2026-07-28

### Changed
- Renamed `--log_file` to `--log-file` -- it was the only CLI flag in the
  tool that used an underscore instead of a hyphen, and was undocumented
  in `USER_GUIDE.md`/`--help`. The old spelling is no longer accepted
  (argparse rejects it as an unrecognized argument); scripts using it
  need to update to the new spelling. Now documented in `USER_GUIDE.md`.

### Fixed
- `--log-file`'s generated filename was missing a separator between the
  custom prefix and the `--dir-suffix` portion:
  `f"{args.log_file}{suffixes_str}_{timestamp}.log"` produced e.g.
  `mirror_url_lasco_ql_nrl260727_...log` (prefix and suffix jammed
  together) instead of the intended
  `mirror_url_lasco_ql_nrl_260727_...log`. Found via a real wrapper
  script (`mirror_url_lasco_ql.sh`) trying to set a custom log-file
  prefix per mirrored server for the first time. 6 new tests in
  `tests/test_log_file_flag.py`.

## [3.1.31] - 2026-07-26

### Fixed
- `--filter` substring and extension matching was not actually
  case-insensitive: `matches_filter()` lowercased the *pattern* before
  comparing, but never lowercased the *filename* it was comparing
  against. This was invisible for patterns matching an
  already-lowercase portion of a filename, but silently failed to
  match whenever the matching portion contained an uppercase
  character -- notably the `T` time separator in ISO-8601-style
  timestamps that PROBA-3/STEREO filenames use (e.g.
  `..._20260619T073111_...`). Reported via a real production command:
  `--filter 20260619T073` returned zero matches against a directory
  that visibly contained several. Same root cause affected the
  extension-check branch (`.FITS` vs `.fits`), covered too though not
  part of the original report. Fixed by lowercasing the filename once
  and comparing both sides consistently; the regex branch already
  passed `re.IGNORECASE` and was unaffected. This bug predates
  `--list-files` -- it affects `--filter` in every mode, including
  real syncs -- and was only surfaced now because `--list-files` is
  the first mode where `--filter` output is immediately visible on
  stdout rather than only showing up as an absence of downloads.
  6 new regression tests in `tests/test_filter_case_insensitivity.py`.

## [3.1.30] - 2026-07-26

### Added
- `--list-files [N]`: discover and print files under the target URL /
  `--dir-suffix`, then exit -- without comparing freshness or
  downloading/deleting anything. Reuses the same BFS directory-discovery
  walk and per-directory file scan as a real sync, so it respects
  `--exclude-dir`, `--max-depth`, and `--filter` (unlike `--list-dirs`,
  `--filter` *does* apply here, since it matches filenames). With no `N`,
  prints every matching file; with `N`, prints only the last `N` files
  *per directory*, sorted lexicographically by relative path -- a
  deliberate, server-independent, zero-extra-request name sort rather
  than a true timestamp sort (see `docs/USER_GUIDE.md` for the full
  rationale). Each file is printed to stdout as its full path relative to
  the target URL/`--dir-suffix`, one per line; every directory's block of
  files is followed by a `# Files N/total` comment line, always -- even
  on an unrestricted run -- so scripts can rely on the marker
  unconditionally. Like `--list-dirs`, it doesn't require `--dest-path`/
  `--log-path`, and `--list-dirs`/`--list-files` are mutually exclusive.

### Fixed
- `docs/USER_GUIDE.html` had drifted out of sync with
  `docs/USER_GUIDE.md` -- it predated `--list-dirs` entirely and was
  missing several other sections that had since been added to the
  Markdown source. Regenerated from the current Markdown (preserving the
  existing page's `<head>`/stylesheet) so both docs are back in sync;
  going forward this needs regenerating on every `USER_GUIDE.md` content
  change, not just on version bumps.

## [3.1.29] - 2026-07-25

### Fixed
- `--list-dirs` incorrectly required `--dest-path`/`--log-path` when used
  without `--config`, even though it never writes to `dest_path` and only
  needs `log_path` for its own run log/cache-file bookkeeping. Both now
  default to a scratch directory under the system temp dir when omitted;
  an explicitly supplied `--dest-path`/`--log-path` still takes priority.
  Non-`--list-dirs` runs are unaffected and still require both.
- `docs/USER_GUIDE.md` didn't document `--list-dirs` at all despite being
  version-stamped for the release it shipped in.

### Added
- `--list-dirs` now prints each discovered directory to stdout as a bare
  relative path, one per line, independent of the existing `📁`-prefixed
  logging (which stays subject to `--print-logs`/`--quiet` as before).
  Makes the output pipeable/scriptable without needing `--print-logs` or
  filtering log noise out of it. When mirroring more than one
  `--dir-suffix` in the same run, each line gets a tab-separated suffix
  column prepended (`L1/v2\t.`) instead of a bare path.

## [3.1.28] - 2026-07-24

### Added
- `--list-dirs`: discover and print the directory tree under the target
  URL / `--dir-suffix`, then exit -- without scanning files, comparing
  freshness, or downloading/deleting anything. Reuses the existing BFS
  directory-discovery walk, so it respects `--exclude-dir` and
  `--max-depth` exactly like a real sync would (`--filter` does not
  apply, since it only matches files). Useful for seeing what's
  available on a remote server before picking a `--dir-suffix`.

## [3.1.27] - 2026-07-22

### Fixed
- `--missing-files` and `--no-etag` were silently ignored whenever
  mirror-url was invoked via plain CLI args without `--config`: the
  direct `MirrorConfig(...)` constructor used on that path never
  passed either field through, so both silently fell back to their
  pydantic defaults (`False`) regardless of the flag. The `--config`
  YAML branch was also missing a CLI-override entry for both flags, so
  passing either on the CLI alongside `--config` was ignored too (only
  setting it in the YAML file itself worked). The `--missing-files`
  bug was invisible on a first run (every file is missing anyway) and
  only showed up on a second run against an already-populated
  destination, where every existing file went through a full
  ETag/size/mtime freshness check again as if the flag had never been
  passed.

## [3.1.26] - 2026-07-19

### Added
- `--missing-files` flag: skip per-file freshness checks (ETag/size/mtime)
  for files that already exist locally, downloading only what's absent.
  Much faster on large, largely-static datasets where a full per-file
  network check on every run is expensive but rarely finds anything —
  the directory-signature fix (v3.1.22) working correctly, just costly
  at scale (a 71,992-file production run was taking ~1 hour, entirely
  spent on verification). Trades detection of in-place file changes
  (same filename, different server-side content) for speed; intended
  for frequent runs alongside occasional full runs (without the flag)
  on a longer cadence to still catch in-place changes.

## [3.1.25] - 2026-07-16

### Fixed
- Three `print()` error/warning fallbacks in the CLI (`ConfigError`,
  generic config-creation errors, and the lxml-availability warning)
  defaulted to stdout instead of stderr. Without `--print-logs`,
  mirror-url's informational logging only ever goes to the file at
  `--log-path`, never to stdout/stderr — so a stderr-only redirect
  (`2>> errors.log`, deliberately not touching stdout, to avoid
  duplicating the main per-run log) is an effective way to catch
  crashes without noise. An unhandled Python exception always goes to
  stderr and was already caught this way, but these three specific
  error/warning paths were not, since bare `print()` defaults to
  stdout. Now consistently directed to stderr, matching the existing
  convention elsewhere in the codebase.

## [3.1.24] - 2026-07-16

### Fixed
- Duplicate startup and auto-select log lines. Every startup log line
  (cache max age, rate limiting, HTML caching, adaptive async, etc.) was
  logged twice — once in `setup_logging()`, once in `__init__`'s config
  summary block. "Auto-selected: SEQUENTIAL ..." was similarly
  double-logged with different wording. `setup_logging()` now only logs
  the startup banner; the config summary and auto-select reasoning each
  have a single source.

### Added
- Authors section in README.md.

## [3.1.23] - 2026-07-15

### Fixed
- `--filter` crashed with `TypeError: 'in <string>' requires string as
  left operand, not stringzilla.Str` on any filter pattern that wasn't
  a file extension (leading `.`) or a regex (containing `*?+[]{}()|\^$`)
  — e.g. `--filter _fe_` — whenever the real `stringzilla` package is
  installed. Root cause: `_get_url_path_fast()` explicitly converts its
  StringZilla result back to a plain `str` before returning, so
  `matches_filter()`'s substring-match branch was comparing a real
  `stringzilla.Str` pattern against a plain `str` filename. Invisible
  under the pure-Python fallback `Str` (a `str` subclass), which is why
  it went unnoticed until a live run against `p3sc.oma.be` hit it.
  Fixed by reusing the already-computed plain-`str` filename for the
  substring check instead. Added `tests/test_filter_stringzilla_typemix.py`
  (6 tests), verified to reproduce the exact reported crash against the
  pre-fix code.

## [3.1.22] - 2026-07-14

### Fixed
- Directory-signature cache-hit shortcut in `file_exists_and_up_to_date()`
  (`_core/compare.py`) now verifies a directory's current signature
  against what was cached, instead of trusting any cached signature
  unconditionally just because the directory URL was present. Previously,
  once a directory was cached, in-place file changes on the server (same
  filename, different content) went undetected indefinitely — every file
  under that directory short-circuited to "up to date" with no HEAD
  request at all. A signature mismatch (or a missing fresh signature) now
  falls through to the existing real per-file HEAD/ETag check. The
  non-deterministic `url:<url>:<timestamp>` fallback signature (used when
  a server provides no ETag/Last-Modified) is never trusted, even when
  byte-identical across runs, since it carries no real change signal.
  Added `dir_signature_changed_forced_recheck` metric for observability.
  3 new regression tests (`test_dir_signature_verification.py`), verified
  to fail against the pre-fix code.

## [3.1.21] - 2026-07-14

### Changed
- Removed the redundant per-file progress log line in `_check_files_sync`
  (`Checked N/total files, X need download`, every 100 files). It
  duplicated `ProgressTracker`'s own percentage-milestone logging
  (25/50/75/90/100%) and scaled with dataset size instead of staying
  constant, producing 700+ near-simultaneous log lines on large
  cache-hit runs (e.g. the 72k-file `L1/v03` cron sync). No behavior
  change — the final "Sync check complete" summary still reports the
  need-download count.

## [3.1.20] - 2026-07-11

### Removed
- The frozen legacy `mirror_url.py` monolith (15,145 lines), retained
  since the refactor as a historical reference. Deletion condition from
  `REFACTORING_PLAN.md` was met: full test suite passes with real
  runtime dependencies installed. Also fixes a real bug: `import
  mirror_url` / `python -m mirror_url` run from inside a clone of this
  repo used to silently resolve against this file instead of the
  installed package, returning its frozen version number (3.1.13)
  instead of the real one.

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
