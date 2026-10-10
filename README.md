# MirrorURL

[![CI](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml/badge.svg)](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/bpodlipnik/mirror-url/blob/main/LICENSE)

MirrorURL is a Python command-line tool and library for mirroring and
incrementally syncing public HTTP(S) directory trees to local disk. It also
downloads individual file URLs or exact URL lists, with resumable transfers,
parallel downloads, caching and integrity checks. URL-scope, private-network
and filesystem protections guard discovery and transfers.

Version 3.2.1 adds direct file URLs with `mirror-url --mode download FILE_URL`,
using the same guarded HTTPX or optional aiohttp transfer pipeline as URL lists.
See [the 3.2.1 changelog](CHANGELOG.md#321---2026-10-08).

## Who it is for

MirrorURL is for people and organizations that regularly need to maintain
local copies of files published over public HTTP(S):

- Researchers maintaining local copies of scientific data archives.
- Organizations downloading bulk files from public directory indexes.
- Maintainers of software, package, artifact and documentation mirrors.
- Sysadmins and data engineers maintaining local datasets.
- Public-data preservation communities.
- Users moving from `wget --mirror`, `lftp mirror` or custom scripts.

Scientific mission archives used in the examples illustrate workflows that
also apply to these other sources. Recursive mirroring requires an HTML
directory index; direct file and URL-list downloads do not require a listing.

## Features

- **Recursive discovery** of remote directory trees (BFS, depth/exclude limits, cycle-safe).
- **True parallel downloads** — multiple files and multiple chunks per file concurrently.
- **Adaptive async concurrency** that tunes itself to server RTT, throughput, and error rate.
- **Resumable & partial downloads** with HTTP range requests and chunk assembly.
- **Integrity checks** — size/timestamp comparison, ETag handling, verified byte ranges, and opt-in `--verify-content` SHA-256 checks of local files; no comparison against a remote cryptographic content digest.
- **Resilience** — per-domain circuit breakers, exponential backoff, rate limiting.
- **Destination ownership** — cooperating processes reject overlapping local trees and shared cache/state paths; hard process termination releases ownership automatically.
- **Crash recovery** — the next owner reclaims recorded abandoned parallel chunks and staging under reserved state; live writers, unknown files and legitimate MOVE archives are preserved.
- **Security** — path-traversal and symlink-bomb defenses, private-IP/SSRF guards, URL-scope enforcement.
- **Filename preflight** — preserve distinct original names on a confirmed case-sensitive destination; reject case collisions on a case-insensitive destination before downloads. Rewritten, unsafe and Unicode-aliased paths remain blocked; see [download behavior and preserving original names](docs/USER_GUIDE.md#filename-collisions-and-download-behavior).
- **Operability** — metrics collection, multi-level progress, optional HTTP health-check server.
- **Caching** — directory listings and file metadata; discovery currently keeps the remote file list in memory.
- **Known-URL downloads** — `--mode download FILE_URL` downloads one file, or `--mode download --url-list urls.txt` streams an exact list, without discovery or freshness probes. Both HTTPX and optional aiohttp use the same destination, scope, staging and receipt checks.
- **Selected freshness checks (next release)** — combine `--missing-files` with `--check-files path1 path2` or `--check-files @list.txt` to refresh selected existing files while downloading all missing files in scope. Paths stay relative to `--url`, including any suffix.
- **Explicit pacing** — `--requests-per-second` and `--request-delay` control the request budget independently of security checks.

## Installation

Python 3.10 or newer is required. Install the published package from PyPI in a
virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install mirror-url
```

To include all optional acceleration, progress, and monitoring packages:

```bash
python -m pip install "mirror-url[all]"
```

Upgrade an existing installation:

```bash
python -m pip install --upgrade mirror-url
```

Check the installed package and CLI:

```bash
python -m pip check
mirror-url --version
mirror-url --help
```

Core dependencies: `httpx[http2]`, `pydantic` (v2), `PyYAML`, `portalocker`
(including `pywin32` on Windows). Optional extras:
`fast` (stringzilla, lxml), `progress` (tqdm), `monitor` (psutil),
`aiohttp` (the whole-file backend), and `all`
(all optional runtime packages). Contributor setup is described below.

## Usage

For an exact, pre-built list of URLs:

```bash
# Install the optional backend in your virtual environment.
python -m pip install "mirror-url[aiohttp]"
python -m mirror_url --mode download --backend aiohttp \
  --url https://example.org/files/ --url-list urls.txt \
  --dest-path ./downloads --log-path ./logs --concurrency 20 \
  --requests-per-second 0 --request-delay 0 --verify-content
```

Version 3.2.1 also accepts a single file URL directly:

```bash
mirror-url --mode download https://example.org/files/a.fits
# Optional destination and log paths:
mirror-url --mode download https://example.org/files/a.fits \
  --dest-path ./downloads --log-path ./logs
```

The shortcut defaults to the current directory, with logs in a destination-specific
system temporary folder. It infers the file URL's parent as the allowed remote
scope; an explicit `--url` overrides that scope. Choose either a positional file
URL or `--url-list`, and keep `--mode download` for both.

`urls.txt` contains one absolute URL per line below the selected base URL.
The two zero pacing values explicitly remove the default 20 requests/second
ceiling and 50 ms spacing. TLS verification, public-IP DNS validation, redirect
scope, safe filename mapping and atomic publication stay enabled. Existing
files need a matching ownership receipt or explicit `--overwrite`. See
[known-URL downloads](docs/USER_GUIDE.md#known-url-downloads) for limits
and the HTTPX equivalent.

Run via the console entry point or the module:

```bash
mirror-url --url https://example.com/files/ --dest-path ./mirror --log-path ./mirror-log
# or
python -m mirror_url --url https://example.com/files/ --dest-path ./mirror --log-path ./mirror-log
```

Run `mirror-url --help` for the full option list.

Configuration can also be supplied via a YAML file (see `MirrorConfig` /
`load_config_from_args`).

📖 **Full documentation:** see the [User Guide](https://github.com/bpodlipnik/mirror-url/blob/main/docs/USER_GUIDE.md)
([HTML version](https://github.com/bpodlipnik/mirror-url/blob/main/docs/USER_GUIDE.html)) for detailed installation, CLI
reference, config-file format, download modes, security notes, the Python API,
and troubleshooting.

🛠 **Contributing to the code?** The [Developer Guide](https://github.com/bpodlipnik/mirror-url/blob/main/docs/DEVELOPER_GUIDE.md)
([HTML version](https://github.com/bpodlipnik/mirror-url/blob/main/docs/DEVELOPER_GUIDE.html)) is an architecture deep-dive:
dependency layers, the `MirrorURL` mixin design, runtime data flow, and
step-by-step extension recipes.

## Development

From a repository checkout, create and activate a virtual environment, then
install the editable package with its runtime extras and development tools:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[all,dev]"
pre-commit install

ruff check .                  # lint
ruff format --check .         # format check (Ruff 0.16.8)
mypy                          # type-check the package
pytest -m "not integration"   # fast test lane
pytest                        # full suite (includes integration)
```

Continuous integration runs lint + tests across Python 3.10–3.14 (see
`.github/workflows/ci.yml`).

## Project layout

```
src/mirror_url/          # the package (dependency-layered)
tests/                   # pytest suite
REFACTORING_PLAN.md      # module breakdown + migration roadmap
CHANGELOG.md             # notable changes (Keep a Changelog format)
CONTRIBUTING.md          # dev setup, checks, conventions
pyproject.toml           # packaging, deps, tool config
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](https://github.com/bpodlipnik/mirror-url/blob/main/CONTRIBUTING.md) for the dev
setup, the checks CI runs, and the project conventions (dependency layering,
lint policy, behavior-preserving refactors). Notable changes are tracked in
[CHANGELOG.md](https://github.com/bpodlipnik/mirror-url/blob/main/CHANGELOG.md).

## Authors

Borut Podlipnik, Max-Planck-Institute for Solar System Research, podlipnik@mps.mpg.de

## License

[MIT](https://github.com/bpodlipnik/mirror-url/blob/main/LICENSE) © BP
