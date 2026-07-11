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
ruff format .         # formatting (or: black .)
mypy                  # type-check (advisory — see below)
pytest                # full suite (the lone end-to-end test is skipped by default)
```

`pytest -m "not integration"` runs the fast lane only (what CI gates on).

A change is ready to merge when `ruff check` is clean, the formatter reports no
diffs, and `pytest` passes.

## Project layout

```
src/mirror_url/        # the package (30 modules, dependency-layered)
tests/                 # pytest suite
REFACTORING_PLAN.md    # module map, dependency layering, and roadmap
```

The package is organized into strict dependency layers (constants/exceptions →
utils → security → transport/limiters → managers → `core` → `cli`). **Imports
only ever point "downward"** — a module may import from lower layers, never from
a higher one. When in doubt, see the layering table in `REFACTORING_PLAN.md` §3,
or the fuller treatment in the [Developer Guide](./docs/DEVELOPER_GUIDE.md).
Cross-layer type-only references use `if TYPE_CHECKING:` to avoid import cycles.

## Conventions

- **Style/format:** ruff + black, 100-column lines. Run `ruff format .` before
  committing; pre-commit will catch the rest.
- **Lint rule set:** `E, F, W, I, B, C4`. `UP` (pyupgrade) and `SIM` are
  intentionally *off* — the package targets Python 3.9 with classic typing
  (`Dict`/`Optional`), and we avoid churning audited logic for syntax
  modernization. If 3.9 support is ever dropped, re-enable `UP` and modernize in
  one deliberate commit.
- **Typing / mypy:** advisory, not a gate (CI uses `continue-on-error`). New code
  should be reasonably typed, but you are not expected to fix the inherited
  annotation backlog to land an unrelated change.
- **Behavior-preserving moves:** if you relocate or split existing code (e.g. the
  planned `core/` mixin refactor, `REFACTORING_PLAN.md` §4.1), keep it verbatim
  and prove equivalence — don't mix refactors with behavior changes in one PR.

## Tests

- Put fast, deterministic tests in the normal lane so CI runs them.
- Reserve the `@pytest.mark.integration` marker for slow or
  external/network-dependent end-to-end cases.
- The SSRF-hardened transport refuses loopback/private targets, so a full local
  HTTP mirror test needs a test-only bypass — see the docstring in
  `tests/test_integration.py` for the one small wiring change required to enable
  it.

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
