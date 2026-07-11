#!/usr/bin/env bash
# Bump the project version across every file that carries it, in one shot.
#
# Usage:
#   scripts/bump_version.sh 3.1.17
#
# What it touches (exact-string replacement of the current version only,
# so it never rewrites unrelated historical version mentions like
# "fixed in 3.1.13" in DEVELOPER_GUIDE.md):
#   - pyproject.toml            (version = "...")
#   - src/mirror_url/_version.py (__version__ = "...")
#   - docs/USER_GUIDE.md        ("**Version:**" line + install examples)
#   - docs/USER_GUIDE.html      (same, HTML-escaped)
#   - docs/DEVELOPER_GUIDE.md   ("**Version:**" line + closing note)
#   - docs/DEVELOPER_GUIDE.html (same, HTML-escaped)
#
# What it does NOT touch (on purpose):
#   - CHANGELOG.md                -- needs a human-written entry describing
#                                    *what* changed, not just a version bump;
#                                    this script only reminds you
#
# Safe by construction: it replaces the exact current-version string, not a
# version-shaped regex, so it can't accidentally touch a different, older
# version number mentioned elsewhere as history.

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <new-version>" >&2
    echo "Example: $0 3.1.17" >&2
    exit 1
fi

NEW_VERSION="$1"

if [[ ! "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: '$NEW_VERSION' doesn't look like a semver version (expected e.g. 3.1.17)" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

PYPROJECT="pyproject.toml"
if [[ ! -f "$PYPROJECT" ]]; then
    echo "Error: $PYPROJECT not found -- run this from within the repo" >&2
    exit 1
fi

CURRENT_VERSION="$(grep -m1 -E '^version = "' "$PYPROJECT" | sed -E 's/version = "([^"]+)"/\1/')"

if [[ -z "$CURRENT_VERSION" ]]; then
    echo "Error: could not read current version from $PYPROJECT" >&2
    exit 1
fi

if [[ "$CURRENT_VERSION" == "$NEW_VERSION" ]]; then
    echo "Error: new version ($NEW_VERSION) is the same as the current version -- nothing to do" >&2
    exit 1
fi

echo "Bumping version: $CURRENT_VERSION -> $NEW_VERSION"
echo

FILES=(
    "pyproject.toml"
    "src/mirror_url/_version.py"
    "docs/USER_GUIDE.md"
    "docs/USER_GUIDE.html"
    "docs/DEVELOPER_GUIDE.md"
    "docs/DEVELOPER_GUIDE.html"
)

# Escape dots for the sed pattern (literal match, not "any character").
ESCAPED_CURRENT="${CURRENT_VERSION//./\\.}"

CHANGED_FILES=()
for f in "${FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "  skip (not found): $f"
        continue
    fi
    HITS=$(grep -c "$ESCAPED_CURRENT" "$f" || true)
    if [[ "$HITS" -eq 0 ]]; then
        echo "  skip (no occurrences of $CURRENT_VERSION): $f"
        continue
    fi
    sed -i.bak "s/$ESCAPED_CURRENT/$NEW_VERSION/g" "$f"
    rm -f "${f}.bak"
    echo "  updated ($HITS occurrence(s)): $f"
    CHANGED_FILES+=("$f")
done

echo
echo "Verifying no '$CURRENT_VERSION' references remain in the updated files..."
REMAINING=0
for f in "${CHANGED_FILES[@]}"; do
    if grep -q "$ESCAPED_CURRENT" "$f"; then
        echo "  WARNING: $f still contains $CURRENT_VERSION somewhere -- check manually"
        REMAINING=1
    fi
done
if [[ "$REMAINING" -eq 0 ]]; then
    echo "  clean."
fi

echo
echo "NOT touched (on purpose):"
echo "  CHANGELOG.md    -- add a ## [$NEW_VERSION] - $(date +%Y-%m-%d) entry by hand,"
echo "                     describing what actually changed in this release"
echo
echo "Next steps:"
echo "  1. Add the CHANGELOG.md entry"
echo "  2. Review the diff:            git diff"
echo "  3. Run the test suite:         pytest"
echo "  4. Commit, PR, merge as usual"
echo "  5. Tag once merged to main:    git tag v$NEW_VERSION && git push origin v$NEW_VERSION"
