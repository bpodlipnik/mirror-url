# MirrorURL

[![CI](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml/badge.svg)](https://github.com/bpodlipnik/mirror-url/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.9%E2%80%933.12-blue)](https://www.python.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

Enterprise-grade remote directory mirroring tool. MirrorURL recursively
discovers files behind an HTTP(S) directory listing and mirrors them locally
with adaptive concurrency, resumable/partial downloads, integrity verification,
and an SSRF-hardened transport layer.

> **Status:** Refactor complete. The full implementation lives in the
> [`src/mirror_url/`](./src/mirror_url) package (30 modules), migrated verbatim
> from the original single-file script per [`REFACTORING_PLAN.md`](./REFACTORING_PLAN.md).
> The legacy single-file script was retained as a frozen reference until the
> test suite passed against the package with real runtime dependencies
> installed, then removed (v3.1.20).

## Features

- **Recursive discovery** of remote directory trees (BFS, depth/exclude limits, cycle-safe).
- **True parallel downloads** — multiple files and multiple chunks per file concurrently.
- **Adaptive async concurrency** that tunes itself to server RTT, throughput, and error rate.
- **Resumable & partial downloads** with HTTP range requests and chunk assembly.
- **Integrity checks** — size/timestamp comparison, ETag handling, content hashing.
- **Resilience** — per-domain circuit breakers, exponential backoff, rate limiting.
- **Security** — path-traversal and symlink-bomb defenses, private-IP/SSRF guards, URL-scope enforcement.
- **Operability** — metrics collection, multi-level progress, optional HTTP health-check server.
- **Caching** — filesystem and disk-backed indexes to skip unchanged content.

## Installation

```bash
# From source (editable, recommended during the refactor)
pip install -e ".[all,dev]"
```

Python 3.9+ is required. Core dependencies: `httpx`, `pydantic` (v2), `PyYAML`.
Optional extras: `fast` (stringzilla, lxml), `progress` (tqdm), `monitor` (psutil).

## Usage

Run via the console entry point or the module:

```bash
mirror-url https://example.com/files/ --output ./mirror
# or
python -m mirror_url https://example.com/files/ --output ./mirror
```

Run `mirror-url --help` for the full option list.

Configuration can also be supplied via a YAML file (see `MirrorConfig` /
`load_config_from_args`).

📖 **Full documentation:** see the [User Guide](./docs/USER_GUIDE.md)
([HTML version](./docs/USER_GUIDE.html)) for detailed installation, CLI
reference, config-file format, download modes, security notes, the Python API,
and troubleshooting.

🛠 **Contributing to the code?** The [Developer Guide](./docs/DEVELOPER_GUIDE.md)
([HTML version](./docs/DEVELOPER_GUIDE.html)) is an architecture deep-dive:
dependency layers, the `MirrorURL` mixin design, runtime data flow, and
step-by-step extension recipes.

## Development

```bash
pip install -e ".[dev]"
pre-commit install

ruff check .          # lint
black --check .       # format check
mypy                  # type-check the new package
pytest -m "not integration"   # fast test lane
pytest                # full suite (includes integration)
```

Continuous integration runs lint + tests across Python 3.9–3.12 (see
`.github/workflows/ci.yml`).

## Project layout

```
src/mirror_url/          # the package (30 modules, dependency-layered)
tests/                   # pytest suite
REFACTORING_PLAN.md      # module breakdown + migration roadmap
CHANGELOG.md             # notable changes (Keep a Changelog format)
CONTRIBUTING.md          # dev setup, checks, conventions
pyproject.toml           # packaging, deps, tool config
```

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](./CONTRIBUTING.md) for the dev
setup, the checks CI runs, and the project conventions (dependency layering,
lint policy, behavior-preserving refactors). Notable changes are tracked in
[CHANGELOG.md](./CHANGELOG.md).

## Authors

Borut Podlipnik, Max-Planck-Institute for Solar System Research, podlipnik@mps.mpg.de

## License

[MIT](./LICENSE) © BP
