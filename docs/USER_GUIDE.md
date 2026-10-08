# MirrorURL — User Guide

MirrorURL is a security-hardened command-line tool and Python library for
mirroring files behind an HTTP(S) **directory listing** to local disk. It walks
the remote directory tree, decides which files are new or changed, and downloads
them efficiently — with adaptive concurrency, resumable/parallel downloads,
integrity checks, incremental caching, and an SSRF-hardened transport layer.

- **Version:** 3.2.0
- **Python:** 3.10 or newer; CI tests Python 3.10–3.14
- **License:** MIT

---

## Table of contents

- [What it does](#what-it-does)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Command-line usage](#command-line-usage)
- [Configuration files (YAML/JSON)](#configuration-files-yamljson)
- [Download modes](#download-modes)
- [Known-URL downloads](#known-url-downloads)
- [Filtering and scope](#filtering-and-scope)
- [Filename collisions and download behavior](#filename-collisions-and-download-behavior)
- [Caching and incremental sync](#caching-and-incremental-sync)
- [Cleaning up obsolete files](#cleaning-up-obsolete-files)
- [Security](#security)
- [Symlink handling](#symlink-handling)
- [Monitoring and metrics](#monitoring-and-metrics)
- [Using MirrorURL from Python](#using-mirrorurl-from-python)
- [Exit codes](#exit-codes)
- [Troubleshooting](#troubleshooting)
- [Uninstalling](#uninstalling)

---

## What it does

Given a base URL that serves an HTML directory index (e.g. an Apache/nginx
"Index of /…" page, a data archive, an artifact server), MirrorURL:

1. **Discovers** the remote tree by recursively parsing directory listings
   (breadth-first, with depth and exclusion limits, cycle-safe).
2. **Validates local names** before file transfers. Distinct selected remote
   URLs must map to safe, unchanged, non-conflicting local paths.
3. **Compares** each remote file against the local copy using size, timestamp,
   and ETag. Directory listing validators never stand in for file checks.
4. **Downloads** the missing/changed files, optionally in parallel (multiple
   files and/or multiple chunks per file) with resume support.
5. **Optionally cleans up** local files that no longer exist remotely
   (preview / move / delete policies).

Highlights: adaptive async metadata checks, per-domain circuit breakers,
bandwidth limiting, integrity verification, a persistent cache for fast
incremental runs, and strong SSRF/path-traversal protections.

---

## Requirements

- **Python 3.10 or newer.**
- Runtime dependencies (installed automatically): `httpx` (with the
  `http2` extra, which pulls in `h2` -- HTTP/2 is on by default, see
  `--no-http2`), `pydantic` (v2), `PyYAML`, `portalocker` 3.x (and `pywin32` on Windows).
- Optional accelerators (install via extras, see below): `stringzilla` + `lxml`
  (faster parsing), `tqdm` (progress bars), `psutil` (memory/disk monitoring).

---

## Installation

> Always install into a **virtual environment** to keep dependencies isolated.

### From PyPI

```bash
pip install mirror-url
```

### From a built wheel (recommended for servers)

From a checkout of the repository on a build machine:

```bash
pip install build
python -m build          # produces dist/mirror_url-3.2.0-py3-none-any.whl
```

Copy the wheel to the target server and install it:

```bash
python3 -m venv /opt/mirror-url
/opt/mirror-url/bin/pip install /tmp/mirror_url-3.2.0-py3-none-any.whl
/opt/mirror-url/bin/mirror-url --help
```

To include the optional speed extras:

```bash
/opt/mirror-url/bin/pip install "/tmp/mirror_url-3.2.0-py3-none-any.whl[fast]"
```

Available extras: `fast` (stringzilla + lxml), `progress` (tqdm),
`monitor` (psutil), `all` (everything), `dev` (test/lint toolchain).

### From a Git repository

```bash
pip install "git+https://github.com/bpodlipnik/mirror-url.git@v3.2.0"
# private repo over SSH:
pip install "git+ssh://git@github.com/bpodlipnik/mirror-url.git@v3.2.0"
```

### As an isolated CLI with pipx

```bash
pipx install /tmp/mirror_url-3.2.0-py3-none-any.whl
# or:  pipx install "git+https://github.com/bpodlipnik/mirror-url.git@v3.2.0"
```

### With Docker

```dockerfile
FROM python:3.12-slim
COPY dist/mirror_url-3.2.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir "/tmp/mirror_url-3.2.0-py3-none-any.whl[fast]"
ENTRYPOINT ["mirror-url"]
```

### Verify the install

```bash
mirror-url --help
python -c "import mirror_url; print(mirror_url.__version__)"
```

The package exposes two equivalent entry points: the `mirror-url` console
command and `python -m mirror_url`.

---

## Quick start

Replace the example URL with your archive's directory-listing URL, then mirror
it to a local folder:

```bash
mirror-url \
  --url https://example.com/datasets/ \
  --dest-path ./mirror \
  --log-path ./logs
```

- `--url` — the base URL to mirror (must serve an HTML directory listing).
- `--dest-path` — where files are written locally.
- `--log-path` — where run logs and the incremental cache are stored.

Re-running the same command later performs an **incremental sync**: only new or
changed files are downloaded.

Prefer a config file for anything non-trivial:

```bash
mirror-url --config mirror.yaml
```

---

## Command-line usage

Either supply `--url`, `--dest-path`, and `--log-path`, **or** point at a config
file with `--config`. Run `mirror-url --help` for the complete, authoritative
list of options. The most commonly used options:

> `--list-dirs` and `--list-files` are exceptions: since they only discover
> and print the remote tree and never download or delete anything, neither
> requires `--dest-path` or `--log-path` when using `--url` without `--config`.
> A config-file run still requires `base_url`, `dest_path`, and `log_path`.
> See their entries in "Filtering and scope" below.

### Targets

| Option | Description |
|---|---|
| `--url URL` | Base URL to mirror (required unless `--config` is used). |
| `--dest-path DIR` | Local destination directory. |
| `--log-path DIR` | Directory for logs and the cache file. |
| `--config FILE` | YAML or JSON configuration file (see below). |
| `--mode mirror\|download` | Normal mirroring (default) or the exact list in `--url-list`. |
| `--url-list FILE` | One absolute file URL per line, for download mode. Blank lines and lines beginning with `#` are ignored. |
| `--overwrite` | Allow replacing regular, owned local files without a matching URL-list receipt; download mode only. |
| `--dir-suffix S [S ...]` | Mirror one or more subpaths under the base URL (e.g. `L1/v1 L2/v2`). |

### Download method

| Option | Description |
|---|---|
| *(default)* | Auto-select the best method at runtime. |
| `--sequential-downloads` | Download one file at a time; metadata and size probes may still run concurrently. |
| `--parallel-downloads` | Parallel chunks via temp files, verified before assembly. |
| `--streaming-parallel` | Parallel chunks written into a staging file, then atomically published. |
| `--max-concurrent-downloads N` | Max files downloaded at once (default 10). |
| `--concurrency N` | Alias for `--max-concurrent-downloads`; range 1–50. |
| `--backend httpx\|aiohttp` | HTTPX is the default. aiohttp requires the optional extra and uses HTTP/1.1 whole-file streaming. Normal mirroring keeps discovery and freshness checks with either backend. |
| `--max-chunks N` | Max chunks per file (default 8). |
| `--min-chunk-size MB` | Minimum chunk size in MB (default 10). |
| `--auto-concurrency` | Tune parallel concurrency from measured throughput. |
| `--bandwidth-limit MB/S` | Cap total download bandwidth. |
| `--max-parallel-chunks N` | Max chunks in flight across *all* files at once (default 50; `--max-chunks` above caps chunks *per file*). |
| `--chunk-assembly-dir DIR` | Parent for an owned, destination-specific chunk workspace (defaults to `.mirror-url-state/chunks/` in the target). Assembly and streaming staging use reserved target state on the destination filesystem. |
| `--chunk-timeout-multiplier MULT` | *Currently has no effect* (accepted for backward compatibility). Chunk requests use fixed multiples of `--timeout`. |

### Performance and networking

| Option | Description |
|---|---|
| `--workers N` | Sync worker threads (default 8). |
| `--async-workers N` | Async metadata-check admission limit (default 50). Normal sync uses async checks only for more than 80 remote files; smaller runs and dry runs use sync checks. |
| `--no-async-metadata` | Disable async metadata checks (use on throttled servers). |
| `--timeout SECS` | Base request timeout (default 30; range 3–300). Some request paths use fixed limits or multiples of this value, so this is not a whole-run deadline. |
| `--max-retries N` | Connection-request retry budget (default 3). Chunk retries also have their own fixed budget. |
| `--retry-delay SECS` | Base delay for retry backoff (default 2). |
| `--requests-per-second N` | Request ceiling (default 20; zero removes this ceiling). |
| `--request-delay SECS` | Minimum request spacing (default 0.05; range 0–1.0). Effective spacing is the larger of this value and `1 / requests_per_second`. Set both controls to zero for unpaced transfers. |
| `--trusted-server` | Relax chunk concurrency and rate-scaling limits; `--request-delay` still controls pacing (default 50 ms). |
| `--no-http2` | Disable HTTP/2. |
| `--no-http2-pipelining` | *Currently has no effect* (accepted for backward compatibility); the HTTP/2 client does not read this setting. |
| `--no-connection-pool-prewarm` | Don't pre-warm connection pools at startup. |
| `--no-circuit-breaker` | Disable the circuit breaker everywhere it is used: per-domain for metadata/scan requests and for chunked file downloads. |
| `--no-circuit-breaker-downloads` | *Currently has no effect* (accepted for backward compatibility). Use `--no-circuit-breaker` to disable the download circuit breaker. |
| `--adaptive-start-concurrency N` | Starting async metadata concurrency (default 5), bounded by `--async-workers` and the adaptive maximum of 50. |
| `--adaptive-error-threshold RATE` | Error-rate threshold (0–1, default 0.05) for adaptive metadata fallback; moderate errors also reduce concurrency. |
| `--no-adaptive-async` | Disable adaptive async concurrency; use a fixed `--async-workers` count. |

### Caching

| Option | Description |
|---|---|
| `--no-cache` | Bypass saved metadata and parsed-listing caches; existing files are still checked for freshness. |
| `--refresh-cache` | Force a full cache refresh this run. |
| `--cache-max-age DAYS` | Max cache age before auto-refresh (default 7). |
| `--no-etag` | Disable ETag-based change detection. |
| `--verify-content` | Verify existing local files against saved SHA-256 receipts before checking remote freshness. Hash completed downloads before publication. Default: disabled; incompatible with `--missing-files`. |
| `--no-verify-content` | Disable content verification, including when enabled in a config file. |
| `--missing-files` | Skip per-file freshness checks for files that already exist locally — only download what's absent. Much faster on large, largely-static datasets, but won't detect a file that changed in place on the server under the same name. Pair with occasional full runs (without this flag) to still catch in-place changes. |
| `--quick` | Quick mode: refresh the cache timestamp only. |
| `--no-cache-html` | Disable caching of parsed HTML directory listings (HTML caching is on by default). |
| `--html-cache-max-age HOURS` | Max age of cached HTML listings before a re-fetch (default 24). |
| `--hash-algorithm {md5,sha256,blake2b}` | Hash used for directory/cache signatures (default `md5`); downloaded file contents are not compared with a remote cryptographic digest. |
| `--no-rget-list` | *Currently has no effect* (accepted for backward compatibility): `RGET-LIST` files are not used for directory discovery. |
| `--force-rget-list` | *Currently has no effect* (accepted for backward compatibility). |
| `--rget-list-max-age DAYS` | *Currently has no effect* (accepted for backward compatibility). |
| `--no-content-hash` | Currently has no effect on file freshness checks. |

### Filtering and scope

| Option | Description |
|---|---|
| `--filter P [P ...]` | Only download matching files. Patterns can be extensions (`.fits`), plain substrings (`_fe_`), or regexes (`'2024.*\.fits$'`). Matching is case-insensitive and patterns are OR'd. |
| `--exclude-dir D [D ...]` | Skip directories, each matched as an exact path relative to `--url` (not a suffix at any depth — see "Filtering and scope" below). |
| `--max-depth N` | Maximum directory recursion depth (default 50; CLI-only `--list-dirs` defaults to 1). With a config file, its `max_depth` or the model default of 50 applies unless explicitly overridden. |
| `--scan-mode {adaptive,sequential,parallel,async}` | Accepted for compatibility; directory discovery and parsing currently use sequential scanning regardless of this value. Async workers apply to file metadata checks. |
| `--parallel-threshold N` | *Currently has no effect* (accepted for backward compatibility); the value is parsed but not used to choose a scan strategy. |
| `--max-filename-len N` | Maximum local filename length (default 255). Sync rejects filenames that would require truncation or sanitization, including Windows reserved names, before downloading. Distinct URLs mapping to the same local name are also rejected; use a larger limit or a narrower scope. |
| `--download-queue-size N` | Accepted for compatibility; the current sync pipeline collects the full remote file list and does not enqueue downloads through the bounded queue. |
| `--max-symlink-depth N` | With `--handle-symlinks`, how many symlink hops deep to follow before stopping (default 10). |
| `--list-dirs [N]` | Discover and print the directory tree under `--url`/`--dir-suffix`, then exit — no file scanning, freshness checks, or downloads/deletes. Respects `--exclude-dir`/`--max-depth` (without `--config`, defaults to `1` — the current folder's immediate children only; config-file runs use the file/model depth unless `--max-depth` is explicit); `--filter` doesn't apply (files only). With `N`, shows only the last `N` directories overall, sorted **lexicographically by relative path** (a name sort, not a true timestamp sort), with the root (`.`) excluded from that ranking. Always followed by a `# Directories N/total` summary line, including unrestricted runs (`N == total`). Without `--config`, doesn't require `--dest-path`/`--log-path`. |
| `--list-files [N]` | Discover and print files under `--url`/`--dir-suffix`, then exit — no freshness checks or downloads/deletes. Respects `--exclude-dir`/`--max-depth`/`--filter`. With `N`, shows only the last `N` files *per directory*, sorted **lexicographically by filename** (a name sort, not a true timestamp sort — see "Filtering and scope" below). Without `--config`, doesn't require `--dest-path`/`--log-path`. |

### Cleanup of obsolete local files

| Option | Description |
|---|---|
| `--cleanup safe` | **Default.** Preserve obsolete local files; changed files can still be replaced. |
| `--cleanup preview` | Report obsolete-file actions without moving/deleting those files; downloads still run. Add `--dry-run` to prevent mirrored-file changes. |
| `--cleanup move` | Move obsolete files into the sibling `<dest>_obsolete/` folder. |
| `--cleanup delete` | Delete obsolete files. |
| `--confirm-delete` | Require interactive confirmation (delete mode). |
| `--dry-run` | Simulate the whole run without downloading or deleting. |

### Output and diagnostics

| Option | Description |
|---|---|
| `--progress-bar` | Show a tqdm progress bar (needs the `progress` extra). |
| `--stats` | Accepted for compatibility; does not change the normal completed-sync metrics summary. Quick mode and other early exits use shorter summaries. |
| `--metrics-json FILE` | Export run metrics to a JSON file. |
| `--log-file NAME` | Custom base name for the run's log file, replacing the default `mirror_url` prefix. See below for the exact filename format. |
| `--verbose` / `--debug` | More logging. |
| `--quiet` | Warnings and errors only. |
| `--health-check-port N` | Local health/metrics server port (default 8080). The server starts only when `--metrics-json` is set and the run is not a dry run. |
| `--print-logs` | Echo run logs to the console as well as the log file. |
| `--version` | Print version and exit. |
| `--no-adaptive-batch-processing` | Accepted for compatibility; the adaptive batch processor is not used by the current sync pipeline. |
| `--initial-batch-size N` | Accepted for compatibility; currently does not change sync batching. |
| `--max-batch-size N` | Accepted for compatibility; currently does not change sync batching. |
| `--target-batch-time SECS` | Accepted for compatibility; currently does not change sync batching. |
| `--memory-cache-size N` | Sets the memory threshold of the optional disk-backed tracking component, which the current sync pipeline does not populate. The active metadata-cache capacities are constants; this option does not bound the remote file list. |
| `--disk-cache-dir DIR` | Configures cache/tracking components; does not spill the current sync pipeline's full remote file list to disk. |
| `--no-fast-parsing-fallback` | Disables fallback after an lxml failure; large listings or installations without lxml still select the lightweight HTML parser. |
| `--fs-cache-ttl SECS` | Configures the standalone filesystem cache; freshness checks in the current pipeline use filesystem stats directly. |
| `--benchmark` | Run a built-in performance benchmark instead of a normal sync. |

Without `--log-file`, each `--dir-suffix` gets its own log file named
`mirror_url_<suffix>_<timestamp>.log`. With `--log-file NAME`, the filename
normally follows `NAME_<suffix>_<timestamp>.log` — where `<suffix>` is
every `--dir-suffix` value joined with underscores (or `all` if none were
given), and `<timestamp>` is `YYYYMMDD_HHMMSS`. With more than one
`--dir-suffix`, all of them share this single log file rather than each
getting a separate one. Unsafe characters in the combined name are replaced
with underscores, and long names are shortened with a digest to fit the
filename limit. This is handy for wrapper scripts that invoke
`mirror-url` once per date/target and want a recognizable, greppable
filename prefix — e.g. `--log-file mirror_url_lasco_ql_nrl` on a run with
`--dir-suffix 260727` produces
`mirror_url_lasco_ql_nrl_260727_20260727_030308.log`.

### Examples

Mirror only FITS files, large parallel downloads, export metrics:

```bash
mirror-url \
  --url https://archive.example.org/mission/ \
  --dest-path /data/mission \
  --log-path /var/log/mirror \
  --filter .fits \
  --streaming-parallel --max-concurrent-downloads 6 \
  --metrics-json /var/log/mirror/run.json
```

Preview obsolete-file actions without downloading or changing mirrored files:

```bash
mirror-url --config mirror.yaml --cleanup preview --dry-run
```

Dry-run to see what a first sync would download:

```bash
mirror-url --url https://example.com/files/ --dest-path ./m --log-path ./l --dry-run
```

See what's available under a base URL before picking a `--dir-suffix`
(no `--dest-path`/`--log-path` needed):

```bash
mirror-url --url https://archive.example.org/mission/ --list-dirs
```

List the last 5 files in each directory (no `--dest-path`/`--log-path` needed):

```bash
mirror-url --url https://archive.example.org/mission/L3_png/v03/ --list-files 5
```

Mirror several versioned subdirectories in one run:

```bash
mirror-url --url https://example.com/product/ \
  --dest-path ./mirror --log-path ./logs \
  --dir-suffix L1/v2 L2/v1
```

Conservative settings for a slow/throttled server:

```bash
mirror-url --config mirror.yaml \
  --sequential-downloads --no-async-metadata --workers 2 --request-delay 0.2
```

---

## Configuration files (YAML/JSON)

For repeatable jobs, put settings in a YAML (or JSON) file and run
`mirror-url --config mirror.yaml`. Any CLI flag you actually type on the
command line overrides the same setting in the file — including a flag whose
value happens to equal its own default (e.g. `--workers 8` overrides a file's
`workers: 4` even though 8 is also the built-in default). A flag you don't
type is left alone at whatever the file says. Only `base_url`, `dest_path`,
and `log_path` are required for a config-file run, including listing modes.
Unknown keys are rejected. `dir_suffix` is one string in a config file; the
CLI's `--dir-suffix` accepts several values. Logging controls such as
`--print-logs`, `--quiet`, `--verbose`, `--debug`, and `--log-file` should be
passed on the CLI to control its shared logging handlers.

Choosing `--list-files` or `--list-dirs` switches off the opposing listing
mode inherited from the file. A configuration that enables both modes is rejected.

To enable a boolean explicitly over a false config-file value, use its positive
flag, for example `--async-metadata`, `--adaptive-async`, `--cache-html`,
`--http2`, `--security-validation`, `--circuit-breaker-enabled`,
`--connection-pool-prewarm`, or `--fast-parsing-fallback`.

```yaml
# mirror.yaml
base_url: https://archive.example.org/mission/
dest_path: /data/mission
log_path: /var/log/mirror

# Performance
workers: 8
async_metadata: true
async_workers: 50
timeout: 30
max_retries: 3
request_delay: 0.05
http2: true
trusted_server: false

# Download method (pick at most one; omit for auto-select)
parallel_downloads: false
streaming_parallel: false
sequential_downloads: false
max_concurrent_downloads: 10
max_chunks_per_file: 8
min_chunk_size_mb: 10
bandwidth_limit: null          # e.g. 50  (MB/s)
enable_resume: true            # whole-file transfers; no CLI toggle

# Filtering
file_filters: [".fits", ".txt"]
exclude_dirs: ["thumbnails", "old"]
max_depth: 50

# Caching
no_cache: false
refresh_cache: false
cache_max_age: 7               # days
cache_html: true
html_cache_max_age: 24         # hours
no_etag: false

# Cleanup of obsolete local files: safe | preview | move | delete
cleanup_policy: safe
confirm_delete: false

# Integrity / security
hash_algorithm: md5            # md5 | sha256 | blake2b
security_validation: true
circuit_breaker_enabled: true

# Output
progress_bar: false
metrics_json: null             # also enables the local health server when set
health_check_port: 8080
```

### Environment variables in config

String values may contain `${VAR}` placeholders, expanded from the environment
at load time. An unset variable leaves its placeholder unchanged; export the
variables before running and check the resulting paths/URL:

```yaml
base_url: ${ARCHIVE_BASE}/mission/
dest_path: /srv/${SERVICE_USER}/mirror
```

### Validate a config without running

```bash
python -c "from mirror_url.config import validate_config_file; \
from pathlib import Path; print(validate_config_file(Path('mirror.yaml')))"
# -> (True, None)  on success, or (False, '<error message>')
```

---

## Download modes

Choose one of the three explicit modes, or omit the mode flags for automatic
selection. Auto-selection considers file count and sizes, a disk-speed probe,
an estimated network speed, and server Range support.

| Mode | Flag | Behavior |
|---|---|---|
| **Sequential** | `--sequential-downloads` | Download one file at a time. Metadata and size probes may still run concurrently. |
| **Traditional parallel** | `--parallel-downloads` | Download several files at once; eligible files use chunks stored in temporary files and verified before assembly. |
| **Streaming parallel** | `--streaming-parallel` | Download several files at once; eligible chunks write to a pre-allocated staging file and publish after verification. |
| **Auto** | *(default)* | Select sequential, traditional parallel, or streaming parallel for this run. |

`--max-concurrent-downloads` caps parallel files; `--max-chunks` caps chunks per
file and `--max-parallel-chunks` caps chunk work across files. These are separate
from metadata-worker limits. `--auto-concurrency` tunes the admitted file count
within `--max-concurrent-downloads` for either parallel mode, including when
auto selection chooses it. If the chunk manager cannot initialize, auto mode
falls back to sequential whole-file downloads.

Parallel chunking requires a known size at least `--min-chunk-size`, byte Range
support, and a **strong ETag**. Without these, the file uses a whole-file
transfer, which may still run alongside other files. Each chunk sends
`If-Range` and must return the exact requested `Content-Range`, length, and ETag.
Existing destination files stay intact until verification and atomic
replacement succeed. These checks do not compare against a server-provided
cryptographic content digest; `--hash-algorithm` does not add such a comparison.

Traditional chunk files default to owned `.mirror-url-state/chunks/` state,
or a destination-specific owned child of `--chunk-assembly-dir`. Final assembly
and streaming staging use `.mirror-url-state/parallel/` on the destination's
filesystem. Traditional mode can need roughly
two additional file-sized copies across the temporary and destination storage;
streaming needs roughly one additional file-sized staging allocation. Existing
destination copies also continue to occupy space until replacement. Assembly
reads bounded blocks rather than loading an entire chunk into memory. Both
chunk modes restore valid server `Last-Modified` timestamps before saving cache
metadata; an unsupported local timestamp update is logged without failing an
otherwise successful transfer.

### Interrupted downloads and reserved state

Whole-file transfers use an owned `.mirror-url-state/` directory inside each
target directory (`--dest-path` plus any `--dir-suffix`). A `.partial.json`
sidecar records the source URL, total size, and strong ETag. With
`enable_resume: true` (the default), matching validated partials can resume.
Partials without a strong validator restart from zero. A response of 200 to a
resume request restarts the transfer; 416 triggers a fresh full request and
never certifies the partial as complete.

The `.mirror-url-state` name is reserved case-insensitively. A conflicting
remote path fails the sync before downloading. An existing directory without
MirrorURL's valid ownership marker is preserved and rejected when partial
state is initialized; obsolete-file cleanup never enters this directory.

Legacy adjacent `*.mirror-partial` files are not resumed. Review or archive
them before enabling obsolete-file cleanup, which may remove them if they are
absent from the selected remote listing.

Traditional and streaming chunk state is not resumed across runs; failed chunks
can retry within a run. The next owner reclaims recorded abandoned work after
acquiring destination/state locks. Per-work OS leases protect writers still
finishing after bounded cleanup, including within a multi-suffix CLI run.
Manifests are written before transfer bytes; unknown entries, malformed markers,
symlinks and hardlinks are preserved for review. Incomplete manifests may leave
small unrecognized workspaces requiring manual review. Legacy unmarked chunk
directories and adjacent `.assembling`/`.streaming` files are not automatically
reclaimed; requested obsolete-file cleanup can still archive adjacent legacy
files. Legitimate MOVE archive bytes are preserved. Nested mount points must
share the reserved staging state's filesystem for atomic publication.

Failed-sync summaries identify download failures, incomplete remote scans and
cleanup failures separately. A zero failed-download count does not certify a
complete scan or successful cleanup.
`--no-etag` disables freshness comparisons, while range transfers still require
ETags to verify that all bytes belong to the same remote representation.

### Concurrent runs and destination ownership

The development checkout acquires cooperative OS locks before starting a
mirror. Overlapping destinations (including parent/child trees), shared cache
or log files, configured chunk/disk cache directories, metrics files and MOVE
archives reject a competing run with `DestinationLockError`; the CLI exits
with status 1. Separate trees and state paths can run concurrently. Dry runs
also acquire ownership because cache loading can repair existing cache state.
CLI runs reserve their log directory and all selected suffixes before shared
logging starts, and retain them until the whole run finishes. Destination keys
are conservatively case-folded and Unicode-normalized, so equivalent names
conflict even on a case-sensitive filesystem.

Lock files live in `~/.mirror-url/locks-v1/`, outside the destination. They
remain after cleanup; **do not delete them while any mirror is running**.
Process termination releases the OS locks automatically, including after a
hard kill. No stale PID file needs removal. Use `with MirrorURL(config)` or
call `cleanup()` explicitly. Cleanup rejects new work; a writer still running
after a shutdown timeout retains ownership until it stops.

This protects cooperating new-version processes using the same account, home
directory and local filesystem. Version 3.1.79 and older versions and other
applications do not participate. Network filesystems, multiple hosts, mount
aliases and external writers need separate coordination.

---

## Known-URL downloads

This mode is available from version 3.2.0. It downloads every URL in
the list with a whole-file GET; it performs no discovery, HEAD freshness checks,
ETag reuse or ranged resume. It never deletes obsolete local files. Use normal
mirror mode when you need incremental synchronization.

```text
# urls.txt
https://example.org/files/a.bin
https://example.org/files/nested/b.bin
```

```bash
python -m mirror_url --mode download --backend httpx \
  --url https://example.org/files/ --url-list urls.txt \
  --dest-path ./downloads --log-path ./logs --concurrency 20 \
  --requests-per-second 0 --request-delay 0 --verify-content
```

To use aiohttp, install `python -m pip install -e ".[aiohttp]"` from the checkout
in your development environment, then change `--backend httpx` to
`--backend aiohttp`. Both clients use the same publication and receipt policy.
Do not expect bare-client benchmark timings: filesystem checks, durable staging
and optional SHA-256 hashing are included here.

All URLs and redirect hops must stay below the selected base URL, on its exact
origin. One optional `--dir-suffix` narrows that scope and appends the suffix
to the destination. Nested file paths are retained. The full list is checked
for filename collisions before any GET. Direct IP URLs, private DNS answers,
unsafe paths, symlinks, hard links and reserved-state collisions are rejected.
TLS certificates remain verified, and proxy environment variables are ignored.
`--trusted-server` and the legacy security toggle do not remove these checks.

An existing file is redownloaded only when its identity matches the receipt
from this mode, or `--overwrite` explicitly permits replacement. With
`--verify-content`, its saved SHA-256 must match too. Failed transfers leave
the original bytes intact. Connection and payload interruptions have bounded
whole-file retries; HTTP failures are reported without status retries. Only a
complete HTTP 200 response with a matching declared length is published. When
the server omits the length, the backend must report a clean stream end;
SHA-256 records local bytes, not proof of a remote reference digest.

Owned temporary work is removed after success, failure or cooperative
cancellation. Unknown state and pre-existing partial downloads are preserved.
Receipts for published files are saved at the end of the run, including during
cooperative cancellation. An abrupt process kill between publication and this
save may leave a file without a receipt: the next run refuses to replace it
without `--overwrite`. This mode does not offer resumable partial transfers.

The CLI writes a run log and prints a JSON summary with downloaded/failed file
counts, bytes, elapsed time, pacing waits, fsync/hash time and any failures. A
failed file makes the command exit nonzero. `--dry-run` validates the list
without network access or download state creation. Discovery/listing/filter
flags, chunk modes, auto-concurrency, missing-only mode and cleanup policies
other than `safe` are rejected in download mode. For normal mirroring,
`--backend aiohttp` supports whole-file transfers; chunk modes and
auto-concurrency require HTTPX, and existing partials are preserved.

## Filtering and scope

- **`--filter`** accepts one or more case-insensitive filename patterns. A bare
  extension (`.fits`) matches by suffix. Plain text such as `_fe_` matches a
  substring; patterns containing regex metacharacters use `re.search`. **Multiple patterns are OR'd** — a
  file matches if *any* pattern matches, not all of them:

  ```bash
  --filter .fits .txt                 # any .fits or .txt
  --filter '.*\.fits$'                # regex: files ending in .fits
  --filter '2024.*\.fits$' .png       # mixed regex + extension
  ```

  For **AND** (a file must match multiple independent conditions at once —
  e.g. a channel *and* a date range), pass a single pattern combining them
  with regex lookaheads instead of multiple `--filter` values — the engine
  already falls back to full `re` support (including lookaheads) for any
  pattern containing regex metacharacters:

  ```bash
  # (fe OR pb channel) AND (18-20 June, 03-05h) -- one --filter value
  --filter '(?=.*(?:fe|pb))(?=.*_202606(?:1[89]|20)T0[3-5]\d{4}_)'
  ```

  If you combine `--filter` with `--list-files [N]`/`--list-dirs [N]`'s "last
  `N`" ranking, see the callout below `--list-files [N]` about what happens
  when a filter matches more than one filename prefix in the same run.

- **`--exclude-dir`** skips one or more directories, each matched as an
  **exact path relative to `--url`** — not a suffix match at any depth.
  `--exclude-dir lasco` excludes only `<root>/lasco/`, never
  `<root>/setup/lasco/` or any other directory elsewhere in the tree that
  happens to share that name. `--exclude-dir idl/beta` excludes only that
  specific two-level path. Pass several to exclude several:
  `--exclude-dir lasco idl/beta` excludes exactly those two root-relative
  paths. A pattern containing `*` is the explicit escape hatch for matching
  at any depth (`--exclude-dir '*/lasco'` also catches `<root>/setup/lasco/`)
  — use both `lasco` and `*/lasco` if you want the root-level directory and
  nested directories with that name. Patterns stay relative to `--url` when
  `--dir-suffix` is used.
- **`--dir-suffix`** restricts mirroring to one or more subpaths under the base
  URL and mirrors each in turn.
- **`--max-depth`** counts directory levels below the target root (depth 0);
  files in its immediate child directories are eligible at depth 1. The
  crawler stays within the configured host/path. Duplicate file URLs are
  downloaded once, and conflicting sanitized local names fail before downloads.
- **`--list-dirs [N]`** discovers and prints the directory tree under `--url`/
  `--dir-suffix`, then exits — it reuses the same directory-discovery walk as
  a real sync, so it respects `--exclude-dir` and `--max-depth`, but never
  scans files, checks freshness, or downloads/deletes anything. `--filter`
  doesn't apply, since it only matches filenames, not directories. Handy for
  seeing what's on a remote server before choosing a `--dir-suffix`. Unlike
  download modes, a CLI-only run does **not** require `--dest-path` or
  `--log-path`; a config-file run still requires both paths.

  Without `--config`, `--list-dirs` defaults `--max-depth` to `1`: the current
  folder's immediate children. With `--config`, the file's `max_depth` applies
  (or 50 when omitted). Pass `--max-depth` explicitly to override either:

  ```bash
  # Immediate children only (the default)
  mirror-url --url https://archive.example.org/mission/ --list-dirs

  # Walk 3 levels deep instead
  mirror-url --url https://archive.example.org/mission/ --list-dirs \
    --max-depth 3
  ```

  Each directory is printed to **stdout** as a bare relative path, one per
  line (`.` for the root), independent of `--print-logs`/`--quiet` and the
  usual banner/summary logging — pipe or capture it directly:

  ```bash
  mirror-url --url https://archive.example.org/mission/ --list-dirs \
    | xargs -I{} echo "found: {}"
  ```

  The listing is always followed by a `# Directories N/total` comment line
  on stdout — including an unrestricted (no-`N`) run, where `N == total` —
  mirroring `--list-files`' `# Files N/total` convention. Drop it with
  `grep -v '^#'` for a pure one-directory-per-line stream.

  With no `N`, every directory (within `--max-depth`) is printed in
  discovery order, as above. With `N`, only the last `N` directories are
  printed, sorted **lexicographically** by relative path, with the root
  (`.`) excluded from that ranking (it isn't a real `--dir-suffix`
  candidate, and would otherwise dilute the "last N real directories" a
  caller typically wants). Unlike `--list-files [N]`, which ranks *per
  directory* (files are naturally grouped by the directory that contains
  them), `--list-dirs [N]` ranks across the **entire** discovered tree for
  this suffix, since directories have no equivalent natural grouping:

  ```bash
  mirror-url --url https://archive.example.org/mission/L3_png/v03/ \
    --list-dirs 3
  ```

  This directly replaces the common pattern of piping `--list-dirs` through
  `grep -v '^\.$' | sort | tail -n N` to pick the most recent N
  directories to mirror next.

  If you mirror more than one `--dir-suffix` in the same run, each line gets
  a tab-separated suffix column prepended instead of a bare path (e.g.
  `L1/v2\tsome/subdir`), so you can tell which subtree it came from while
  keeping the path itself easy to `cut -f2`/`awk -F'\t'` out.

- **`--list-files [N]`** discovers and prints the files under `--url`/
  `--dir-suffix`, then exits — it reuses the same directory-discovery walk
  and per-directory file scan as a real sync, so it respects
  `--exclude-dir`, `--max-depth`, **and** `--filter` (unlike `--list-dirs`,
  `--filter` *does* apply here, since it matches filenames). It never
  compares freshness or downloads/deletes anything, and — like
  `--list-dirs` — a CLI-only run does **not** require `--dest-path` or
  `--log-path`. Config-file runs still require both paths.

  With no `N`, every matching file is printed. With `N`, only the last `N`
  files **per directory** are printed (not N total across the whole run —
  a directory with 200 files and one with 3 each contribute up to `N`).

  Each file is printed to **stdout** as its full path relative to
  `--url`/`--dir-suffix` (e.g. `v03/orbit_0042/file_20260722_003.fits`),
  one per line, independent of `--print-logs`/`--quiet`. Every directory's
  block of files is followed by a `# Files N/total` comment line — always,
  even on an unrestricted (no-`N`) run — so scripts can rely on the marker
  being present unconditionally rather than only when truncated. Comment
  lines start with `#` and can be dropped with `grep -v '^#'` for a pure
  one-line-per-file stream:

  ```bash
  mirror-url --url https://archive.example.org/mission/L3_png/v03/ \
    --list-files 5 | grep -v '^#'
  ```

  When more than one `--dir-suffix` is mirrored in the same run, each file
  line gets the same tab-separated suffix column as `--list-dirs`. Comment
  lines are never suffix-qualified, since they aren't file paths.

  > **⚠️ "Last N" is a filename/path sort, not a timestamp sort.** With
  > `N`, entries are ranked by sorting their relative paths
  > **lexicographically** (plain string/alphabetical order) and keeping the
  > last `N` — per directory for `--list-files [N]`, across the whole tree
  > for `--list-dirs [N]` — it is **not** based on any server-reported
  > modification time.
  >
  > This avoids extra per-file metadata requests. It reflects chronological
  > order only when filenames contain sortable dates or sequence numbers.
  > For arbitrary names, it means alphabetically last, regardless of file age.
  >
  > **A sharper version of the same tradeoff bites when `--filter`
  > matches more than one filename prefix/channel in the same run** — e.g.
  > `--filter fe pb` for two instrument channels named `..._fe_l3_...` and
  > `..._pb_l3_...`. Every `fe`-file sorts before every `pb`-file (`f` <
  > `p`), **regardless of timestamp** — so once a directory has at least
  > `N` `pb`-files, "last `N`" is `N` `pb`-files, full stop, no matter how
  > recent the newest `fe`-file is. The `fe` channel doesn't just rank
  > lower — with more than `N` `pb`-files present, it's invisible.
  > Workarounds:
  >
  > - **Query each channel separately** — `--filter fe` and `--filter pb`
  >   as two separate runs — sidesteps the collision entirely, since each
  >   run's "last N" only ever ranks within one prefix.
  > - **Or request enough files to be sure**, then sort/filter by
  >   timestamp yourself instead of relying on the filename-prefix order:
  >   ```bash
  >   mirror-url --url https://archive.example.org/mission/L3_png/v03/ \
  >     --list-files 200 --filter fe pb --quiet \
  >     | grep -v '^#' | sort -t_ -k4 | tail -3
  >   ```
  >   (`sort -t_ -k4` sorts from the 4th underscore-delimited field
  >   onward — i.e. from the embedded timestamp, not the channel prefix
  >   sitting before it. This assumes that filename layout; adapt the field
  >   number for your archive.)

---

## Filename collisions and download behavior

**A filename collision fails the entire affected suffix before file downloads.**
MirrorURL does not choose one of the conflicting files, rename either file,
or download the other files in that suffix while silently skipping the pair.
Existing mirrored file contents are preserved, and obsolete-file cleanup is
not reached. Directory listings, connection/metadata requests and log/cache
bookkeeping can still occur before the failure.

For example, NASA SolarSoft has these two different files under `stereo`:

```text
secchi/idl/daily/Deep_Field_v3.pro
secchi/idl/daily/deep_field_v3.pro
```

They have different contents. Common case-insensitive macOS volumes cannot
store both names as separate files. A case-sensitive Linux filesystem or
case-sensitive APFS volume can store both; an existing Linux mirror can
therefore contain both originals.

**Filesystem-aware behavior in 3.2.0:** MirrorURL checks the destination
when selected names differ only by case. On a confirmed case-sensitive
destination, it downloads both files with their original capitalization and
keeps their content receipts separate. On a case-insensitive destination, the
pair fails preflight before any file payload in that suffix is downloaded.
There is no option to choose whichever file arrives first or overwrite one
with the other. The 3.1.79 preflight rejects selected case-only pairs on every
filesystem; upgrade to 3.2.0 for filesystem-aware handling.

The check also covers directory names such as `DAILY/one.pro` and
`daily/two.pro`. If a requested filename already resolves to a differently
capitalized local entry, preflight fails and preserves that entry, even when
only one remote file is selected or `--missing-files` is used. Otherwise a
missing-only run could mistake different local data for the requested file.

For an ambiguous pair, MirrorURL creates an exclusively owned, randomly named
empty probe in its actual parent directory, or the nearest existing ancestor
when the parent does not exist yet. It checks the differently capitalized
probe name and removes only its own unchanged regular file. This check also
runs during a dry run. If the directory cannot be inspected or probed safely,
the suffix fails instead of guessing its filesystem behavior. Ordinary unique
names do not require a case probe.

**Preserving the complete selected tree:** use a destination that can store
both original filenames, and the updated filesystem-aware MirrorURL
implementation. On macOS, an APFS (Case-sensitive) disk image
or volume provides the required storage behavior. Apple's
[disk-image instructions](https://support.apple.com/guide/disk-utility/create-a-disk-image-dskutl11888/mac)
describe creating a blank image with that format; a sparse image grows as files
are added. This avoids reformatting the existing startup volume. Choose a new
destination inside the mounted image for the complete mirror; an ordinary
case-insensitive destination cannot hold both originals. Size the image for
the selected payload and temporary transfer space. With the updated code,
retain the same URL and suffix and include `daily` instead of excluding it.
Moving an existing mirror or changing the destination of a running job is a
separate operation; do not change its filesystem while it is writing.

Excluding `daily` does **not** solve complete mirroring: it discards an entire
remote subtree from the selected scope. Automatically renaming one file would
also change the archive's original names and may break references between
files. Neither behavior is a substitute for preserving both originals.

| Selected scope | What is downloaded? | Result |
|---|---|---|
| Both case-distinct files are included on a confirmed case-sensitive destination, with the updated code | Both original files, plus other eligible files in the suffix | Separate filenames and receipts are preserved |
| Both case-distinct files are included on a case-insensitive destination | No mirrored file payload from that suffix, including unrelated files | The suffix fails; neither conflicting file is chosen |
| `stereo/secchi/idl/daily` is explicitly excluded | Only eligible missing/changed files outside that entire subtree | Sync can proceed if all remaining paths and checks pass |
| A filter excludes both `.pro` files, for example `--filter .fits` | Only matching files in the remaining selection | Sync can proceed if no other mapping conflicts or failures occur |

If a deliberately reduced scope is acceptable, directory exclusions are
relative to `--url`: with the SolarSoft root URL, excluding
`stereo/secchi/idl/daily` omits **every file and subdirectory under `daily`**,
not just the two conflicting names. Files already present under an excluded
subtree are preserved by cleanup. Use this only when that omission is intended.

Other mapping problems also fail preflight: Unicode-normalized name aliases,
names that would be truncated or rewritten by sanitization, unsafe local paths,
and collisions with reserved `.mirror-url-state` storage. A repeated identical
remote URL is deduplicated; distinct URLs that collide are not deduplicated by
content, even if their bytes happen to match. Case-insensitive `--filter`
matching cannot select just one of the two example filenames by capitalization.

For a multi-suffix CLI invocation, failure of one suffix does not roll back
earlier successful suffixes. Other suffixes may still run, and the overall CLI
exits `1` when a suffix fails. The Python `sync()` call returns `False`; inspect
the log for `Distinct remote URLs map to the same local filename`,
`Remote filename would be changed by local path sanitization`, or
`Remote file has an unsafe or reserved local path`.

This describes normal synchronization and its dry run. `--list-files` is
discovery-only and can list both remote URLs; it does not prove that the selected
files can be mirrored. `--quick` also skips remote-file validation and transfers.

---

## Caching and incremental sync

MirrorURL stores file identity metadata and directory signatures in a JSON
cache under `--log-path`. Keep this path stable between runs. The file name
contains the directory suffix and a hash of the base URL, so different remote
roots do not share one cache accidentally.

Normal runs still discover the remote tree and check existing files. By default, a cached
file ETag is trusted only when the recorded local size, modification time, and
filesystem change/creation timestamp match the current file. Otherwise the
file is checked again. Same-size local edits that leave these timestamps
unchanged can be missed, including rapid writes on filesystems with coarse
timestamp resolution. This check does not hash local file contents.
Directory-listing ETags and signatures do not establish that child file
contents are unchanged. Without usable file ETags, checks fall back to size and
the server's `Last-Modified` header when available; changes that preserve those
values can be missed. No remote cryptographic digest comparison is performed.

The development checkout adds `--verify-content` (or `verify_content: true` in
YAML) to detect local changes
even when file sizes and timestamps still match. This mode saves a SHA-256
receipt of each completed staging file **before** atomic publication, then
hashes existing local files before trusting freshness metadata. A missing,
expired, malformed or mismatched receipt requires a new download. Enabling it
for an existing mirror therefore redownloads files whose cache has no receipt.
Keep `--log-path` stable so those receipts persist between runs.

Hashing reads every verified file in 1 MiB chunks, adding disk IO proportional
to the selected data size on every full run. Async metadata checks perform
hashing on worker threads. `--hash-algorithm` does not change these SHA-256
receipts, and the compatibility flag `--no-content-hash` does not disable them;
use `--no-verify-content` for that.
Network request timeouts still apply; local hashing is allowed to finish without
the metadata-only async check/batch deadlines. Slow storage can lengthen a run.

Content verification proves agreement with the saved local receipt. Remote
freshness still depends on the server's HTTP validators, size and modification
time; a remote change that preserves those values can be missed. This mode does
not compare against a server-provided cryptographic checksum. Give each
destination to one mirror process at a time and avoid external edits during a
run: there is no interprocess destination lock, and arbitrary concurrent writes
are outside the guarantee.

Parsed HTML listings also have bounded **in-memory** caches. They are not
restored from disk on a new process launch. `--html-cache-max-age` controls their
lifetime within the process; it does not mean a fresh CLI invocation will skip
fetching the remote listing.

- `--cache-max-age DAYS`: discard expired JSON metadata (default 7 days).
- `--refresh-cache`: ignore saved metadata and cached listing results for this run.
- `--no-cache`: bypass those caches; existing files are still checked. With
  `--verify-content`, unavailable receipts require downloading existing files.
- `--no-etag`: use size/time rather than file ETags for freshness checks.
- `--missing-files`: download only absent files. Existing files are not checked
  for freshness, so in-place remote changes will be missed. Use occasional
  normal runs when those changes matter.
  It cannot be combined with `--verify-content`.
- `--quick`: refresh an existing JSON cache's expiry timestamp, without scanning
  or downloading. It does not verify that local or remote files are current and
  does not create a missing cache. Connection setup can still contact the server.

The cache can be written after a complete scan and updated again after the
normal download/cleanup path. It is separate from resumable file state in `.mirror-url-state/`.

Process-interruption tests cover all three download modes. Killing a process
before publication preserves the previous complete destination; killing it
after publication leaves the complete replacement. A restart checks receipts
again, resumes validated sequential partials where possible, and can redownload
files published before their receipt was persisted. Interrupted cache writes
leave the previous complete JSON authoritative. MOVE cleanup can be restarted
with obsolete bytes preserved in their source or archive. These tests cover
process termination, not power loss or failure of the storage device.

`--use-disk-backed-sets`, `--memory-cache-size`, and `--download-queue-size` do
not bound the current workflow's remote file list: discovery collects that list
in memory. Scope a large archive with `--dir-suffix`, `--exclude-dir`, or
`--max-depth` when you need smaller runs.

---

## Cleaning up obsolete files

By default MirrorURL preserves obsolete local files (`--cleanup safe`). It
still replaces files that need updating. Choose `preview`, `move`, or `delete`
to handle files absent from the current remote selection. `preview` reports
obsolete-file actions but still allows normal downloads; add `--dry-run` for
an observation-only run. Keep `--log-path` outside the destination tree so
cleanup does not treat your logs and cache as obsolete mirrored files.

Cleanup only acts within the current scan selection. Files excluded by
filters, directory exclusions, depth limits, or skipped symlink subtrees are
preserved, as are local symlinks and `.mirror-url-state/`. A complete empty scan can
clean the selected local files; an incomplete scan skips cleanup and fails the
run. Cleanup operation failures also fail the run.
If the MOVE archive cannot be created, cleanup stops; a failed move leaves
the source in place and never falls back to deletion.
MOVE also refuses symlinked archive paths and existing timestamp collision
names. It preserves the source and previous archives and reports a failed
cleanup operation; inspect the archive before retrying.

```bash
# Observe cleanup without downloads or changes to mirrored files
mirror-url --config mirror.yaml --cleanup preview --dry-run

# Move obsolete files into sibling <dest>_obsolete/ instead of deleting
mirror-url --config mirror.yaml --cleanup move

# Actually delete, with a confirmation prompt
mirror-url --config mirror.yaml --cleanup delete --confirm-delete
```

Combine any of these with `--dry-run` to simulate the entire run (scan +
download + cleanup) without downloading or changing mirrored files. The run
still requests directory listings and metadata; log/cache bookkeeping may
create directories outside the mirrored data tree.

---

## Security

MirrorURL ships with security protections **enabled by default**:

- **SSRF / private-network protection.** The HTTP transport resolves and
  validates target IPs and **refuses to connect to loopback or private
  addresses** (`127.0.0.1`, `localhost`, RFC-1918 ranges, link-local, etc.),
  blocks direct-IP URLs, dangerous ports, and IDN/homograph and URL-smuggling
  tricks.
- **URL-scope enforcement** keeps the crawler within the configured base
  host/path and blocks path-traversal (including encoded/double-encoded forms).
- **Filesystem safety** — filename sanitization, path-traversal rejection,
  Windows reserved-name handling, and symlink-loop/bomb defenses.

> The secure transport rejects direct-IP, private, and loopback targets,
> including `localhost`. `--no-security-validation` disables the extra URL
> validation layer and per-IP request pacing; it does not disable transport IP
> validation. There is no production option for mirroring private or local
> servers.

Symlink handling is off by default; see [Symlink handling](#symlink-handling)
below for how it works and how to use it.

---

## Symlink handling

An HTTP directory listing usually does not identify server-side symlinks.
`--handle-symlinks` enables a heuristic: compare the basenames of each visited
directory's immediate files and subdirectories. Two non-empty directories with
the same entries are reported as possible duplicates, with the first visited
directory treated as the target. For example, two paths exposing the same
archive subtree may be detected this way.

The comparison uses listings already fetched by the scan. Directory-listing
`Last-Modified` and ETag headers, when available, add confidence notes to the
log; they do not decide whether the directory is flagged and do not verify
individual files. Descendants of an already flagged subtree are not repeatedly
reported as separate duplicates.

This heuristic can flag distinct directories that merely use the same entry
names, cannot detect a target outside the visited tree, and cannot identify
individual file symlinks. Empty directories are excluded from matching. Review
the reported paths before choosing exclusions or enabling automatic skipping.

| Mode | Behavior with `--handle-symlinks` |
|---|---|
| `--symlink-mode detect` | Report possible duplicates and continue scanning. Implies `--dry-run`, so mirrored files are not downloaded or cleaned up. |
| `--symlink-mode skip` | Skip detected duplicate subtrees; this is the default when handling is enabled. |
| `--symlink-mode follow` | Mirror a detected duplicate only when its inferred target is inside the target scope and tracker limits allow it. |
| `--symlink-mode treat-as-file` | Accepted for compatibility; behaves like `skip` for directory duplicates. |

Start with a survey:

```bash
mirror-url --config mirror.yaml --handle-symlinks --symlink-mode detect --print-logs
```

Review the `Symlink detected` log lines. To avoid a known duplicate permanently,
use `--exclude-dir` with its exact path relative to `--url`, or an explicit `*`
glob for nested matches. Plain exclusions do not match arbitrary path suffixes.
`--max-symlink-depth`, `--max-symlinks-per-dir`, and `--symlink-bomb-threshold`
limit tracker behavior; they do not make the heuristic a definitive detector.

---

## Monitoring and metrics

- **Metrics summary.** A run logs a `METRICS SUMMARY` block at INFO level
  (files downloaded/skipped/failed, bytes, speed, cache hit rates, ETag stats,
  etc.). `--stats` is accepted for backward compatibility but currently has no
  effect; the summary is emitted in full on the normal completed-sync path.
  Early exits such as quick mode and connection failures have shorter summaries.
- **`--metrics-json FILE`** writes the full metrics summary to JSON (skipped in
  `--dry-run`).
- **`--progress-bar`** shows a live tqdm bar (requires the `progress` extra).
- **Health/metrics HTTP endpoints.** Setting `--metrics-json FILE` also starts
  a local HTTP server for a non-dry-run instance, until it is cleaned up.
  `--health-check-port` alone does not enable it. The server binds to `localhost`
  (default port 8080); both endpoints are rate-limited and serve:
  - `GET /health` → JSON health status. Returns HTTP
    200 when connected with fewer than 10 failed files, otherwise HTTP 503
    with `degraded` status.
  - `GET /metrics` → JSON counters (files downloaded/failed/skipped, bytes,
    elapsed).

  ```bash
  curl http://localhost:8080/health
  curl http://localhost:8080/metrics
  ```

- **Logging.** Each run writes a timestamped log under `--log-path`. Use
  `--verbose`/`--debug` for more detail, `--quiet` for less, and `--print-logs`
  to also echo to the console.

---

## Using MirrorURL from Python

MirrorURL is a library as well as a CLI. The public API:

```python
from pathlib import Path
from mirror_url import MirrorURL, MirrorConfig

config = MirrorConfig(
    base_url="https://archive.example.org/mission/",
    dest_path=Path("/data/mission"),
    log_path=Path("/var/log/mirror"),
    file_filters=[".fits"],
    parallel_downloads=True,
    cache_max_age=7,
)

with MirrorURL(config) as mirror:
    ok = mirror.sync()  # returns True on success, False on failure

print("sync succeeded" if ok else "sync failed")
```

Use the context manager (`with`) to clean up connection pools, background
workers, async components, and any health server. `sync()` is blocking; library
construction leaves signal handlers unchanged. The CLI separately opts in to
SIGINT/SIGTERM handling. Freshness checks, resume rules, and cleanup policies
are the same for CLI and library runs.

Load configuration from a YAML/JSON file (`from_yaml` accepts JSON as YAML-compatible syntax):

```python
from pathlib import Path
from mirror_url import MirrorConfig, MirrorURL

config = MirrorConfig.from_yaml(Path("mirror.yaml"))
with MirrorURL(config) as mirror:
    mirror.sync()
```

Handle configuration errors:

```python
from pathlib import Path
from pydantic import ValidationError
from mirror_url import MirrorConfig
from mirror_url.exceptions import ConfigError

try:
    cfg = MirrorConfig(base_url="ftp://nope", dest_path=Path("d"), log_path=Path("l"))
except (ConfigError, ValidationError) as e:
    print("bad config:", e)
```

Useful exported names: `MirrorURL`, `MirrorConfig`, `load_config_from_args`,
`main`, and the exception types (`MirrorError`, `ConfigError`,
`MirrorConnectionError`, `SecurityError`, `DownloadError`, `DestinationLockError`,
`PathTraversalError`, `URLScopeError`).

For a configuration dictionary, use `MirrorConfig.model_validate(data)` to
validate it and obtain a model. To obtain informational warnings about an
existing configuration, use `MirrorConfig.validation_warnings(config)`.
Code that previously used `MirrorConfig.validate(config)` for warnings must
switch to `validation_warnings`; `validate` now follows Pydantic's model
validation API, where `model_validate` is preferred.

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | The CLI finished without a recorded suffix failure, or handled a shutdown signal and completed cleanup. |
| `1` | A recorded suffix/download/scan/cleanup failure, failed benchmark, or forced shutdown after the cleanup timeout. |
| `2` | Command-line parsing or a configuration-file validation error reported by the argument parser. |

The CLI currently exits `0` after graceful SIGINT/SIGTERM cleanup, even if the
sync was interrupted. For scheduled jobs, review the completion summary when
an interruption occurred rather than treating that exit code as proof that all
files were mirrored. The Python API returns a boolean from `sync()` instead of
setting a process exit code.

```bash
mirror-url --config mirror.yaml && echo "OK" || echo "FAILED ($?)"
```

---

## Troubleshooting

**"Hostname resolves to private IP" / connection refused to localhost.**
The secure transport blocks private/loopback targets even with
`--no-security-validation`. Use a public hostname resolving to a public address.
The loopback bypass used by integration tests is confined to those tests.

**Server returns 403/429 or downloads are slow/failing.**
The remote may be throttling you. Omit `--trusted-server` (or set
`trusted_server: false` in your config), increase
`--request-delay`, reduce `--workers`/`--max-concurrent-downloads`, add
`--no-async-metadata`, or switch to `--sequential-downloads`.

**Nothing is downloaded / "0 directories".**
Confirm `--url` actually serves an HTML directory listing (not a single file or
a JS-rendered page). Check your `--filter` isn't excluding everything, and try
`--debug` to see the parsed links and scope decisions.

**"Distinct remote URLs map to the same local filename" / case-only names.**
The selected suffix stopped before file downloads. Neither conflicting file
is chosen; other files in that suffix are also not transferred. See
[Filename collisions and download behavior](#filename-collisions-and-download-behavior)
for the NASA example, the filesystem-aware behavior, and how a case-sensitive
destination preserves both original files. A probe/inspection failure also
requires resolving destination access before retrying.

**Parallel mode isn't kicking in.**
Chunking needs a known file size of at least `--min-chunk-size`, byte Range
support, and a strong ETag. Otherwise MirrorURL uses whole-file transfers.
Check the log with `--print-logs --debug` for selection and fallback messages.

**Progress bar / memory stats missing.**
Install the optional extras: `pip install "mirror-url[progress,monitor]"`
(tqdm / psutil).

**Re-runs re-download everything.**
Make sure `--log-path` is stable between runs (that's where the cache lives) and
you're not passing `--no-cache` / `--refresh-cache`.

---

## Uninstalling

```bash
pip uninstall mirror-url
# or, if installed with pipx:
pipx uninstall mirror-url
```

Generated logs, the JSON cache under `--log-path`, mirrored data and partial
state under `--dest-path`, and per-user domain-health metadata are left in
place. Domain-health metadata lives under `$XDG_CACHE_HOME/mirror-url/`
(or `~/.cache/mirror-url/`) on POSIX and under the local application-data
`mirror-url/` directory on Windows. Remove these manually if desired.

## Release 3.1.78 behavior

Version 3.1.78 uses the same validated, once-decoded URL paths for discovery,
local mapping and cleanup. Equivalent percent-encoded roots are accepted;
literal percent escapes and semicolons in filenames are preserved. Directory
links with query parameters are traversed as directories. Unsafe expected
paths suppress obsolete-file cleanup and fail the run.

Local symlinks in a selected destination root are rejected before manager
startup; leaf and descendant symlinks block remote file mapping. The fixed
macOS `/var`, `/tmp` and `/etc` system aliases are accepted. Remote duplicate
directory handling does not authorize local symlink writes.

CLI listing choices override the opposing config-file mode. Disabling lxml
error fallback still permits lightweight-parser selection when lxml is absent.
The standalone shared-pool submission helper rechecks its capacity after
wakeup and counts failed or cancelled jobs once. `use_shared_thread_pool`
enables the coordinator's chunk pool; file transfers and metadata comparisons
retain their separate executors.
Local path checks assume the destination tree is not concurrently mutated by
another process; they do not provide filesystem isolation against such a process.

## Release 3.1.79 behavior

Sync now rejects filenames that would need truncation or sanitization before
starting downloads. This includes control characters and Windows reserved
names. Increase `--max-filename-len` or narrow the selection when the configured
length limit is too small.

MOVE cleanup rejects symlinked archive paths and occupied timestamp collision
names, preserving the source and previous archives. Directory inspection or
removal failures are reported as cleanup failures. The transport rejects mixed
public and link-local DNS results, and URL decoding failures reject the request.
