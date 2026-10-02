# Contributing to MirrorURL

Thanks for your interest in improving MirrorURL. This guide covers the
development setup, the checks we run, and the conventions specific to this
codebase.

> **New to the codebase?** Read the [Developer Guide](./docs/DEVELOPER_GUIDE.md)
> ([HTML](./docs/DEVELOPER_GUIDE.html)) first — it's a self-contained
> architecture deep-dive (dependency layers, the `MirrorURL` mixin design, data
> flow, and step-by-step extension recipes). This file covers the day-to-day
> contribution mechanics.

## Development setup

```bash
git clone <your-fork-url> mirror-url
cd mirror-url
python -m venv .venv && source .venv/bin/activate   # Python 3.9+
pip install -e ".[all,dev]"
pre-commit install
```

## Before you open a PR

Run the same checks CI runs:

```bash
ruff check .          # lint
ruff format .         # formatting (Ruff 0.16.8)
mypy                  # required type-check (zero errors)
pytest                # full suite, including live HTTP integration
```

`pytest -m "not integration"` runs the fast lane across Python 3.9–3.14 in CI.
The separate coverage job runs the full suite on Python 3.12 with all optional
dependencies, including the local HTTP tests, and gates combined statement and
branch coverage at 70%:

```bash
pip install -e ".[all,dev]"
pytest --cov=mirror_url --cov-branch --cov-fail-under=70 \
  --cov-report=term-missing --cov-report=json:coverage.json --cov-report=html
python scripts/check_download_coverage.py coverage.json
```

Open `htmlcov/index.html` to inspect missed lines and branches. CI also saves
HTML, JSON, and XML coverage reports as an artifact.
Each of `download.py` and `download_integrity.py` must also have 100% statement
and branch coverage. The gate checks missing counts separately for each module;
rounded percentages and the overall average cannot hide a gap. Exercise real
filesystem operations and inject network or disk faults to verify preservation
of existing files, response closure, retry boundaries, and resource cleanup.

A change is ready to merge when `ruff check` is clean, the formatter reports no
diffs, and `pytest` passes.

## Project layout

```
src/mirror_url/        # the package (44 Python files, including private mixins and helpers)
tests/                 # pytest suite
REFACTORING_PLAN.md    # module map, dependency layering, and roadmap
```

The dependency map groups module responsibilities; existing runtime imports
also cross these historical layers. Keep the runtime graph acyclic and inject
orchestrator state into managers. The [Developer Guide](./docs/DEVELOPER_GUIDE.md)
describes the current composition; `REFACTORING_PLAN.md` records the archived
migration plan. Type-only references use `if TYPE_CHECKING:` to avoid cycles.

## Conventions

- **Style/format:** Ruff 0.16.8, 100-column lines. Run `ruff format .` before
  committing; pre-commit will catch the rest.
- **Lint rule set:** `E, F, W, I, B, C4`. `UP` (pyupgrade) and `SIM` are
  intentionally *off* — the package targets Python 3.9 with classic typing
  (`Dict`/`Optional`), and we avoid churning audited logic for syntax
  modernization. If 3.9 support is ever dropped, re-enable `UP` and modernize in
  one deliberate commit.
- **Typing / mypy:** required in CI; `mypy` must report zero errors. The existing
  settings remain lenient and do not check untyped function bodies. Update the
  shared contract in `src/mirror_url/_core/_typing.py` when changing mixin state
  or cross-mixin method signatures. Keep its imports under `TYPE_CHECKING` so
  runtime inheritance and imports stay unchanged.
- **Behavior-preserving moves:** if you relocate or split existing code (e.g. the
  planned `core/` mixin refactor, `REFACTORING_PLAN.md` §4.1), keep it verbatim
  and prove equivalence — don't mix refactors with behavior changes in one PR.

## Tests

- Put fast, deterministic tests in the normal lane so CI runs them.
- Reserve the `@pytest.mark.integration` marker for slow or
  external/network-dependent end-to-end cases.
- The SSRF-hardened transport refuses loopback/private targets. The local HTTP
  fixtures in `tests/test_integration.py` and `tests/test_http_mirror_workflows.py`
  install scoped transport bypasses that pytest restores after each test.
- Assert observable outcomes: final file contents, preservation of existing
  data on failures, request headers and retries, response closure, and actual
  concurrency. Exercise failure and cancellation paths as well as successful
  requests; executing a function alone does not establish its correctness.

## Security

MirrorURL includes SSRF protections (private-IP/loopback blocking, URL-scope
enforcement, path-traversal and symlink-bomb defenses). Please do not weaken
these to make tests easier — gate any test-only relaxation behind an explicit,
non-default flag. Report security issues privately to the maintainer rather than
in a public issue.

## Commit / PR hygiene

- Keep PRs focused; separate refactors from behavior changes.
- Update `CHANGELOG.md` under "Unreleased" for user-visible changes.
- Reference the relevant `REFACTORING_PLAN.md` section when working on the
  migration/refactor roadmap.
