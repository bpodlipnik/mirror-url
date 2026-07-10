# MirrorURL — User Guide

MirrorURL is an enterprise-grade command-line tool and Python library for
mirroring files behind an HTTP(S) **directory listing** to local disk. It walks
the remote directory tree, decides which files are new or changed, and downloads
them efficiently — with adaptive concurrency, resumable/parallel downloads,
integrity checks, incremental caching, and an SSRF-hardened transport layer.

- **Version:** 3.1.18
- **Python:** 3.9 – 3.12 (pure Python, any OS/architecture)
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
- [Filtering and scope](#filtering-and-scope)
- [Caching and incremental sync](#caching-and-incremental-sync)
- [Cleaning up obsolete files](#cleaning-up-obsolete-files)
- [Security](#security)
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
2. **Compares** each remote file against the local copy using size, timestamp,
   ETag, and optional content hashing — so re-runs only fetch what changed.
3. **Downloads** the missing/changed files, optionally in parallel (multiple
   files and/or multiple chunks per file) with resume support.
4. **Optionally cleans up** local files that no longer exist remotely
   (preview / move / delete policies).

Highlights: adaptive async metadata checks, per-domain circuit breakers,
bandwidth limiting, integrity verification, a persistent cache for fast
incremental runs, and strong SSRF/path-traversal protections.

---

## Requirements

- **Python 3.9 or newer.**
- Runtime dependencies (installed automatically): `httpx`, `pydantic` (v2),
  `PyYAML`.
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

On a build machine:

```bash
pip install build
python -m build          # produces dist/mirror_url-3.1.18-py3-none-any.whl
```

Copy the wheel to the target server and install it:

```bash
python3 -m venv /opt/mirror-url
/opt/mirror-url/bin/pip install /tmp/mirror_url-3.1.18-py3-none-any.whl
/opt/mirror-url/bin/mirror-url --help
```

To include the optional speed extras:

```bash
/opt/mirror-url/bin/pip install "/tmp/mirror_url-3.1.18-py3-none-any.whl[fast]"
```

Available extras: `fast` (stringzilla + lxml), `progress` (tqdm),
`monitor` (psutil), `all` (everything), `dev` (test/lint toolchain).

### From a Git repository

```bash
pip install "git+https://github.com/bpodlipnik/mirror-url.git@v3.1.18"
# private repo over SSH:
pip install "git+ssh://git@github.com/bpodlipnik/mirror-url.git@v3.1.18"
```

### As an isolated CLI with pipx

```bash
pipx install /tmp/mirror_url-3.1.18-py3-none-any.whl
# or:  pipx install "git+https://github.com/bpodlipnik/mirror-url.git@v3.1.18"
```

### With Docker

```dockerfile
FROM python:3.12-slim
COPY dist/mirror_url-3.1.18-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir "/tmp/mirror_url-3.1.18-py3-none-any.whl[fast]"
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

Mirror a remote directory to a local folder:

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

### Targets

| Option | Description |
|---|---|
| `--url URL` | Base URL to mirror (required unless `--config` is used). |
| `--dest-path DIR` | Local destination directory. |
| `--log-path DIR` | Directory for logs and the cache file. |
| `--config FILE` | YAML or JSON configuration file (see below). |
| `--dir-suffix S [S ...]` | Mirror one or more subpaths under the base URL (e.g. `L1/v1 L2/v2`). |

### Download method

| Option | Description |
|---|---|
| *(default)* | Auto-select the best method at runtime. |
| `--sequential-downloads` | One file at a time, no parallelism (most conservative). |
| `--parallel-downloads` | Traditional parallel chunks via temp files (safe, resumable). |
| `--streaming-parallel` | Parallel chunks written directly into the final file (fastest for huge files). |
| `--max-concurrent-downloads N` | Max files downloaded at once (default 10). |
| `--max-chunks N` | Max chunks per file (default 8). |
| `--min-chunk-size MB` | Minimum chunk size in MB (default 10). |
| `--auto-concurrency` | Tune parallel concurrency from measured throughput. |
| `--bandwidth-limit MB/S` | Cap total download bandwidth. |

### Performance and networking

| Option | Description |
|---|---|
| `--workers N` | Sync worker threads (default 8). |
| `--async-workers N` | Async metadata-check workers (default 50). |
| `--no-async-metadata` | Disable async metadata checks (use on throttled servers). |
| `--timeout SECS` | Per-request timeout (default 30). |
| `--max-retries N` | Retries per request (default 3). |
| `--trusted-server` | Use faster rate limiting (10 ms vs 50 ms between requests). |
| `--no-http2` | Disable HTTP/2. |

### Caching

| Option | Description |
|---|---|
| `--no-cache` | Disable the on-disk cache (always re-scan). |
| `--refresh-cache` | Force a full cache refresh this run. |
| `--cache-max-age DAYS` | Max cache age before auto-refresh (default 7). |
| `--no-etag` | Disable ETag-based change detection. |
| `--quick` | Quick mode: refresh the cache timestamp only. |

### Filtering and scope

| Option | Description |
|---|---|
| `--filter P [P ...]` | Only download matching files. Each pattern is a plain extension (`.fits`) or a regex (`'2024.*\.fits$'`). |
| `--exclude-dir D [D ...]` | Skip matching directories. |
| `--max-depth N` | Maximum directory recursion depth (default 50). |

### Cleanup of obsolete local files

| Option | Description |
|---|---|
| `--cleanup safe` | **Default.** Never delete anything. |
| `--cleanup preview` | Show what *would* be deleted/moved, but do nothing. |
| `--cleanup move` | Move obsolete files into an `_obsolete/` folder. |
| `--cleanup delete` | Delete obsolete files. |
| `--confirm-delete` | Require interactive confirmation (delete mode). |
| `--dry-run` | Simulate the whole run without downloading or deleting. |

### Output and diagnostics

| Option | Description |
|---|---|
| `--progress-bar` | Show a tqdm progress bar (needs the `progress` extra). |
| `--stats` | Print detailed statistics at the end. |
| `--metrics-json FILE` | Export run metrics to a JSON file. |
| `--verbose` / `--debug` | More logging. |
| `--quiet` | Warnings and errors only. |
| `--health-check-port N` | Port for the health/metrics HTTP server (default 8080). |
| `--version` | Print version and exit. |

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

Preview what a cleanup would remove, without touching anything:

```bash
mirror-url --config mirror.yaml --cleanup preview
```

Dry-run to see what a first sync would download:

```bash
mirror-url --url https://example.com/files/ --dest-path ./m --log-path ./l --dry-run
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
`mirror-url --config mirror.yaml`. CLI flags still work and take precedence
where applicable. Only `base_url`, `dest_path`, and `log_path` are required.

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
trusted_server: false

# Download method (pick at most one; omit for auto-select)
parallel_downloads: false
streaming_parallel: false
sequential_downloads: false
max_concurrent_downloads: 10
max_chunks_per_file: 8
min_chunk_size_mb: 10
bandwidth_limit: null          # e.g. 50  (MB/s)

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
stats: false
metrics_json: null             # e.g. /var/log/mirror/metrics.json
health_check_port: 8080
```

### Environment variables in config

String values may contain `${VAR}` placeholders, expanded from the environment
at load time:

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

MirrorURL supports four strategies. If you specify none, it **auto-selects**
based on file count, average size, disk type, network speed, and server Range
support.

| Mode | Flag | Best for |
|---|---|---|
| **Sequential** | `--sequential-downloads` | Small jobs, fragile/throttled servers, debugging. |
| **Traditional parallel** | `--parallel-downloads` | Many files; chunks written to temp files then assembled (safe, resumable, needs ~2× disk headroom per in-flight file). |
| **Streaming parallel** | `--streaming-parallel` | A few very large files; chunks written directly into the pre-allocated final file (fastest, ~1× disk). |
| **Auto** | *(default)* | Let MirrorURL choose; good general default. |

Parallel chunking requires the server to support HTTP **Range** requests; if it
doesn't, MirrorURL falls back to whole-file downloads automatically. Interrupted
downloads can resume from a partial file on the next run (`enable_resume`,
on by default).

---

## Filtering and scope

- **`--filter`** accepts one or more patterns. A pattern that looks like a bare
  extension (`.fits`) matches by suffix; anything else is treated as a regular
  expression matched against the filename. Examples:

  ```bash
  --filter .fits .txt                 # any .fits or .txt
  --filter '.*\.fits$'                # regex: files ending in .fits
  --filter '2024.*\.fits' .png        # mixed regex + extension
  ```

- **`--exclude-dir`** skips directories by name/path suffix (simple `*` globs
  supported).
- **`--dir-suffix`** restricts mirroring to one or more subpaths under the base
  URL and mirrors each in turn.
- **`--max-depth`** bounds recursion. The crawler also enforces URL-scope checks
  so it never wanders outside the configured base host/path.

---

## Caching and incremental sync

MirrorURL keeps a JSON cache (in `--log-path`) describing the last run, plus an
in-memory/disk HTML-listing cache. On subsequent runs it uses this — together
with ETags, sizes, and timestamps — to skip unchanged files and avoid
re-fetching directory listings.

- `--cache-max-age DAYS` — after this age the cache auto-refreshes.
- `--refresh-cache` — force a full refresh now.
- `--no-cache` — ignore the cache entirely (always full scan).
- `--no-etag` — don't use ETags for change detection (size/time only).
- `--quick` — only bump the cache timestamp (no scanning/downloading).

For very large trees, `--use-disk-backed-sets` keeps the file-set on disk to
bound memory use.

---

## Cleaning up obsolete files

By default MirrorURL **never deletes** anything (`--cleanup safe`). To mirror
deletions from the remote side, choose a stronger policy:

```bash
# See what would be removed — safe to run anytime
mirror-url --config mirror.yaml --cleanup preview

# Move obsolete files into <dest>/_obsolete/ instead of deleting
mirror-url --config mirror.yaml --cleanup move

# Actually delete, with a confirmation prompt
mirror-url --config mirror.yaml --cleanup delete --confirm-delete
```

Combine any of these with `--dry-run` to simulate the entire run (scan +
download + cleanup) without making changes.

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

> Because of the SSRF guard, MirrorURL **cannot mirror a server on
> `localhost`/`127.0.0.1`** unless you explicitly disable validation with
> `--no-security-validation` (or `security_validation: false`). Disabling this
> removes SSRF protection — only do so for trusted, local targets.

Symlink handling is off by default; enable with `--handle-symlinks` (with
`--symlink-mode`, depth, per-directory, and bomb-threshold limits).

---

## Monitoring and metrics

- **`--stats`** prints a detailed summary (files downloaded/skipped/failed,
  bytes, speed, cache hit rates, ETag stats, etc.) at the end of a run.
- **`--metrics-json FILE`** writes the full metrics summary to JSON (skipped in
  `--dry-run`).
- **`--progress-bar`** shows a live tqdm bar (requires the `progress` extra).
- **Health/metrics HTTP endpoints.** During a (non-dry-run) sync, a small HTTP
  server listens on `--health-check-port` (default 8080) and serves:
  - `GET /health` → JSON health status (rate-limited).
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
    ok = mirror.sync()      # returns True on success, False on failure

print("sync succeeded" if ok else "sync failed")
```

Using the context manager (`with`) ensures background threads, connection pools,
and the health server are cleaned up. Always run inside one.

Load configuration from a YAML/JSON file:

```python
from pathlib import Path
from mirror_url import MirrorConfig, MirrorURL

config = MirrorConfig.from_yaml(Path("mirror.yaml"))
with MirrorURL(config) as mirror:
    mirror.sync()
```

Handle configuration errors:

```python
from mirror_url import MirrorConfig
from mirror_url.exceptions import ConfigError

try:
    cfg = MirrorConfig(base_url="ftp://nope", dest_path=Path("d"), log_path=Path("l"))
except ConfigError as e:
    print("bad config:", e)
```

Useful exported names: `MirrorURL`, `MirrorConfig`, `load_config_from_args`,
`main`, and the exception types (`MirrorError`, `ConfigError`,
`MirrorConnectionError`, `SecurityError`, `DownloadError`,
`PathTraversalError`, `URLScopeError`).

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success — all requested suffixes synced without fatal errors. |
| `1` | One or more suffixes failed (connection failure, fatal error). |

This makes MirrorURL easy to drive from cron, systemd timers, or CI:

```bash
mirror-url --config mirror.yaml && echo "OK" || echo "FAILED ($?)"
```

---

## Troubleshooting

**"Hostname resolves to private IP" / connection refused to localhost.**
The SSRF guard is blocking a private/loopback target. For a trusted local
server, add `--no-security-validation` (or `security_validation: false`).

**Server returns 403/429 or downloads are slow/failing.**
The remote may be throttling you. Try `--trusted-server` off, increase
`--request-delay`, reduce `--workers`/`--max-concurrent-downloads`, add
`--no-async-metadata`, or switch to `--sequential-downloads`.

**Nothing is downloaded / "0 directories".**
Confirm `--url` actually serves an HTML directory listing (not a single file or
a JS-rendered page). Check your `--filter` isn't excluding everything, and try
`--debug` to see the parsed links and scope decisions.

**Parallel mode isn't kicking in.**
The server must support HTTP Range requests and files must exceed
`--min-chunk-size`. Otherwise MirrorURL downloads whole files. Use `--debug` to
see the auto-select decision.

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

Generated logs, cache files (in your `--log-path`), and mirrored data (in your
`--dest-path`) are left in place — remove them manually if desired.
