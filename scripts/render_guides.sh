#!/usr/bin/env bash
# Render the committed Markdown guides with their shared CSS.
set -euo pipefail

guide_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if ! command -v pandoc >/dev/null 2>&1; then
    echo 'Pandoc is required to render the HTML guides.' >&2
    exit 1
fi

for guide_name in USER_GUIDE DEVELOPER_GUIDE; do
    case "$guide_name" in
        USER_GUIDE) guide_title='MirrorURL — User Guide' ;;
        DEVELOPER_GUIDE) guide_title='MirrorURL — Developer Guide' ;;
    esac
    pandoc "$guide_repo_root/docs/$guide_name.md" \
        --from=gfm --to=html5 --standalone \
        --metadata "pagetitle=$guide_title" --metadata lang=en \
        --lua-filter="$guide_repo_root/scripts/guide_html_links.lua" \
        --include-in-header="$guide_repo_root/docs/guide-style.html" \
        --output="$guide_repo_root/docs/$guide_name.html"
done
