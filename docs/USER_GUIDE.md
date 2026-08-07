# MirrorURL — User Guide

MirrorURL is an enterprise-grade command-line tool and Python library for
mirroring files behind an HTTP(S) **directory listing** to local disk. It walks
the remote directory tree, decides which files are new or changed, and downloads
them efficiently — with adaptive concurrency, resumable/parallel downloads,
integrity checks, incremental caching, and an SSRF-hardened transport layer.

- **Version:** 3.1.44
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
- Runtime dependencies (installed automatically): `httpx` (with the
  `http2` extra, which pulls in `h2` -- HTTP/2 is on by default, see
  `--no-http2`), `pydantic` (v2), `PyYAML`.
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
python -m build          # produces dist/mirror_url-3.1.44-py3-none-any.whl
```

Copy the wheel to the target server and install it:

```bash
python3 -m venv /opt/mirror-url
/opt/mirror-url/bin/pip install /tmp/mirror_url-3.1.44-py3-none-any.whl
/opt/mirror-url/bin/mirror-url --help
```

To include the optional speed extras:

```bash
/opt/mirror-url/bin/pip install "/tmp/mirror_url-3.1.44-py3-none-any.whl[fast]"
```

Available extras: `fast` (stringzilla + lxml), `progress` (tqdm),
`monitor` (psutil), `all` (everything), `dev` (test/lint toolchain).

### From a Git repository

```bash
pip install "git+https://github.com/bpodlipnik/mirror-url.git@v3.1.44"
# private repo over SSH:
pip install "git+ssh://git@github.com/bpodlipnik/mirror-url.git@v3.1.44"
```

### As an isolated CLI with pipx

```bash
pipx install /tmp/mirror_url-3.1.44-py3-none-any.whl
# or:  pipx install "git+https://github.com/bpodlipnik/mirror-url.git@v3.1.44"
```

### With Docker

```dockerfile
FROM python:3.12-slim
COPY dist/mirror_url-3.1.44-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir "/tmp/mirror_url-3.1.44-py3-none-any.whl[fast]"
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

> `--list-dirs` and `--list-files` are exceptions: since they only discover
> and print the remote tree and never download or delete anything, neither
> requires `--dest-path` or `--log-path` — see their entries in "Filtering
> and scope" below.

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
| `--missing-files` | Skip per-file freshness checks for files that already exist locally — only download what's absent. Much faster on large, largely-static datasets, but won't detect a file that changed in place on the server under the same name. Pair with occasional full runs (without this flag) to still catch in-place changes. |
| `--quick` | Quick mode: refresh the cache timestamp only. |

### Filtering and scope

| Option | Description |
|---|---|
| `--filter P [P ...]` | Only download matching files. Each pattern is a plain extension (`.fits`) or a regex (`'2024.*\.fits$'`). |
| `--exclude-dir D [D ...]` | Skip matching directories. |
| `--max-depth N` | Maximum directory recursion depth (default 50; `--list-dirs` defaults to 1 instead — see below). |
| `--list-dirs [N]` | Discover and print the directory tree under `--url`/`--dir-suffix`, then exit — no file scanning, freshness checks, or downloads/deletes. Respects `--exclude-dir`/`--max-depth` (defaults to `1` — the current folder's immediate children only — unless `--max-depth` is given explicitly; every other mode still defaults to 50); `--filter` doesn't apply (files only). With `N`, shows only the last `N` directories overall, sorted **lexicographically by relative path** (a name sort, not a true timestamp sort), with the root (`.`) excluded from that ranking. Always followed by a `# Directories N/total` summary line, including unrestricted runs (`N == total`). Doesn't require `--dest-path`/`--log-path`. |
| `--list-files [N]` | Discover and print files under `--url`/`--dir-suffix`, then exit — no freshness checks or downloads/deletes. Respects `--exclude-dir`/`--max-depth`/`--filter`. With `N`, shows only the last `N` files *per directory*, sorted **lexicographically by filename** (a name sort, not a true timestamp sort — see "Filtering and scope" below). Doesn't require `--dest-path`/`--log-path`. |

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
| `--log-file NAME` | Custom base name for the run's log file, replacing the default `mirror_url` prefix. See below for the exact filename format. |
| `--verbose` / `--debug` | More logging. |
| `--quiet` | Warnings and errors only. |
| `--health-check-port N` | Port for the health/metrics HTTP server (default 8080). |
| `--version` | Print version and exit. |

Without `--log-file`, each `--dir-suffix` gets its own log file named
`mirror_url_<suffix>_<timestamp>.log`. With `--log-file NAME`, the filename
is always `NAME_<suffix>_<timestamp>.log` instead — where `<suffix>` is
every `--dir-suffix` value joined with underscores (or `all` if none were
given), and `<timestamp>` is `YYYYMMDD_HHMMSS`. With more than one
`--dir-suffix`, all of them share this single log file rather than each
getting a separate one. This is handy for wrapper scripts that invoke
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

Preview what a cleanup would remove, without touching anything:

```bash
mirror-url --config mirror.yaml --cleanup preview
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
  expression matched against the filename. **Multiple patterns are OR'd** — a
  file matches if *any* pattern matches, not all of them:

  ```bash
  --filter .fits .txt                 # any .fits or .txt
  --filter '.*\.fits$'                # regex: files ending in .fits
  --filter '2024.*\.fits' .png        # mixed regex + extension
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

- **`--exclude-dir`** skips directories by name/path suffix (simple `*` globs
  supported).
- **`--dir-suffix`** restricts mirroring to one or more subpaths under the base
  URL and mirrors each in turn.
- **`--max-depth`** bounds recursion. The crawler also enforces URL-scope checks
  so it never wanders outside the configured base host/path.
- **`--list-dirs [N]`** discovers and prints the directory tree under `--url`/
  `--dir-suffix`, then exits — it reuses the same directory-discovery walk as
  a real sync, so it respects `--exclude-dir` and `--max-depth`, but never
  scans files, checks freshness, or downloads/deletes anything. `--filter`
  doesn't apply, since it only matches filenames, not directories. Handy for
  seeing what's on a remote server before choosing a `--dir-suffix`. Unlike
  every other mode, it does **not** require `--dest-path` or `--log-path`.

  Unlike every other mode, `--list-dirs` also defaults `--max-depth` to `1`
  — the current folder's immediate children only — instead of the usual 50.
  "What's in this folder" is the overwhelmingly common ask, and recursing
  the full tree by default is easy to be surprised by on a deep archive.
  Pass `--max-depth` explicitly to go deeper (or shallower); it always wins
  over this default, for every mode including `--list-dirs`:

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
  `--list-dirs` — does **not** require `--dest-path` or `--log-path`.

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
  > This is a deliberate tradeoff, not an oversight. Getting a true
  > per-file timestamp would need one of two things, and both were
  > rejected:
  >
  > - **Parsing the "Last modified" column some directory-listing HTML
  >   formats include** — but that format is server-specific (Apache,
  >   nginx, IIS/Microsoft, lighttpd, etc. all differ, and some servers
  >   don't expose one at all), so this would make `--list-files`'s
  >   correctness depend on which web server happens to be on the other
  >   end.
  > - **An extra HTTP `HEAD` request per file** to read its
  >   `Last-Modified` header — fully server-independent, but exactly the
  >   per-file network round-trip that `--missing-files` (see "Caching"
  >   above) was built to *avoid*, because it doesn't scale: a directory
  >   with tens of thousands of files would turn a `--list-files` probe
  >   into a run lasting as long as a full sync.
  >
  > A lexicographic sort needs neither: it costs **zero extra network
  > requests** beyond the directory listing itself, and works identically
  > regardless of which web server is serving the files. The tradeoff is
  > that it only reflects true chronological order when filenames embed a
  > sortable date or sequence number — e.g. `..._20260722_003.fits`. That
  > holds for the PROBA-3/STEREO archives this tool targets, where
  > filenames are date/sequence-stamped, but is **not guaranteed** for an
  > arbitrary directory with inconsistent naming — there, "last N" means
  > "alphabetically last N", which may not be "most recent N".
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
  >   sitting before it — so the result is chronological across channels
  >   instead of "whichever channel's name sorts higher, wins".)

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
- `--missing-files` — skip freshness verification for files that already exist
  locally; only download what's absent.
- `--quick` — only bump the cache timestamp (no scanning/downloading).

For very large trees, `--use-disk-backed-sets` keeps the file-set on disk to
bound memory use.

`--missing-files` trades correctness for speed on datasets where a per-file
network check on every run is expensive but rarely finds anything (a large,
largely-static remote tree). It skips ETag/size/mtime verification entirely
once a file's local existence is confirmed — so a file that's replaced or
corrected in place on the server, under the same name, will not be
re-downloaded. A reasonable pattern is running `--missing-files` on a
frequent schedule (e.g. daily) and an occasional full run without it (e.g.
weekly) to still catch in-place changes on a bounded delay.

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
    ok = mirror.sync()  # returns True on success, False on failure

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
