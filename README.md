# MirrorURL

[![CI](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml/badge.svg)](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/bpodlipnik/mirror-url/blob/main/LICENSE)

Security-hardened remote directory mirroring tool. MirrorURL recursively
discovers files behind an HTTP(S) directory listing and mirrors them locally
with adaptive concurrency, resumable/partial downloads, integrity verification,
and an SSRF-hardened transport layer.

## Features

- **Recursive discovery** of remote directory trees (BFS, depth/exclude limits, cycle-safe).
- **True parallel downloads** — multiple files and multiple chunks per file concurrently.
- **Adaptive async concurrency** that tunes itself to server RTT, throughput, and error rate.
- **Resumable & partial downloads** with HTTP range requests and chunk assembly.
- **Integrity checks** — size/timestamp comparison, ETag handling, content hashing.
- **Resilience** — per-domain circuit breakers, exponential backoff, rate limiting.
- **Security** — path-traversal and symlink-bomb defenses, private-IP/SSRF guards, URL-scope enforcement.
- **Operability** — metrics collection, multi-level progress, optional HTTP health-check server.
- **Caching** — directory listings and file metadata; discovery currently keeps the remote file list in memory.

## Installation

Python 3.9 or newer is required. Install the published package from PyPI in a
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

Core dependencies: `httpx[http2]`, `pydantic` (v2), `PyYAML`. Optional extras:
`fast` (stringzilla, lxml), `progress` (tqdm), `monitor` (psutil), and `all`
(all optional runtime packages). Contributor setup is described below.

## Usage

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

Continuous integration runs lint + tests across Python 3.9–3.12 (see
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
