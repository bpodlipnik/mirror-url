#!/usr/bin/env python3
"""Refresh release references and the generated CLI table in index.html."""

from __future__ import annotations

import argparse
import html
import re
import sys
from datetime import date
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
BEGIN = "<!-- BEGIN GENERATED CLI REFERENCE -->"
END = "<!-- END GENERATED CLI REFERENCE -->"


class _ParserCaptured(Exception):
    pass


def capture_parser() -> argparse.ArgumentParser:
    """Stop the real CLI at parsing, before logging, ownership or network work."""
    from mirror_url import cli

    captured = []

    def capture(parser, *args, **kwargs):
        captured.append(parser)
        raise _ParserCaptured

    with patch.object(argparse.ArgumentParser, "parse_args", capture):
        try:
            cli.main()
        except _ParserCaptured:
            pass
    if len(captured) != 1:
        raise ValueError("Expected to capture exactly one CLI parser")
    return captured[0]


def default_label(action: argparse.Action) -> str:
    if action.dest in {"help", "version"}:
        return "—"
    if action.dest == "cleanup":
        return "safe (without --config)"
    if action.dest == "max_depth":
        from mirror_url.constants import LIST_DIRS_DEFAULT_MAX_DEPTH, MAX_DIRECTORY_DEPTH

        return f"{MAX_DIRECTORY_DEPTH}; CLI-only --list-dirs: {LIST_DIRS_DEFAULT_MAX_DEPTH}"
    value = action.default
    if value is None or value == argparse.SUPPRESS:
        return "Not set"
    if isinstance(value, bool):
        return "Enabled" if value else "Disabled"
    return str(value)


def render_cli(parser: argparse.ArgumentParser, version: str) -> str:
    formatter = parser._get_formatter()
    lines = [
        BEGIN,
        '<div id="argument-groups">',
        f"<!-- CLI reference generated from the v{version} argument parser. -->",
    ]
    for group in parser._action_groups:
        actions = [action for action in group._group_actions if action.help != argparse.SUPPRESS]
        if not actions:
            continue
        title = "Help" if group.title == "options" else group.title
        default_open = title in {"Target Options", "Cleanup & Safety Options"}
        open_attribute = " open" if default_open else ""
        lines.extend(
            [
                f'<details class="argument-group" data-default-open="{str(default_open).lower()}"{open_attribute}>',
                f"<summary>{html.escape(title)}</summary>",
                '<div class="table-wrapper" tabindex="0" role="region" aria-label="Argument reference"><table>',
                '<thead><tr><th scope="col">Argument</th><th scope="col">Current behavior</th><th scope="col">Default setting</th></tr></thead><tbody>',
            ]
        )
        for action in actions:
            invocation = formatter._format_action_invocation(action)
            if not action.option_strings:
                invocation = formatter._format_args(action, action.metavar or action.dest.upper())
            description = action.help or ""
            if "%(default)" in description:
                description = description % {"default": action.default}
            lines.append(
                f"<tr><td><code>{html.escape(invocation)}</code></td>"
                f"<td>{html.escape(description)}</td>"
                f"<td>{html.escape(default_label(action))}</td></tr>"
            )
        lines.extend(["</tbody></table></div>", "</details>"])
    lines.extend(["</div>", END])
    return "\n".join(lines)


def release_date(changelog: str, version: str, override: str | None) -> date:
    if override:
        return date.fromisoformat(override)
    match = re.search(
        rf"^## \[{re.escape(version)}\] - (\d{{4}}-\d{{2}}-\d{{2}})$", changelog, re.M
    )
    if not match:
        raise ValueError("Add a dated changelog entry for this release or pass --date YYYY-MM-DD")
    return date.fromisoformat(match[1])


def update_site(source: str, parser: argparse.ArgumentParser, version: str, released: date) -> str:
    match = re.search(r"Documentation for v(\d+\.\d+\.\d+)", source)
    if not match:
        raise ValueError("Cannot find the documentation version in index.html")
    updated = source.replace(match[1], version)
    pattern = re.escape(BEGIN) + r".*?" + re.escape(END)
    updated, count = re.subn(pattern, lambda _: render_cli(parser, version), updated, flags=re.S)
    if count != 1:
        raise ValueError("index.html must contain exactly one generated CLI reference block")
    time_element = (
        f'<time datetime="{released.isoformat()}">'
        f"{released.day} {released.strftime('%B %Y')}</time>"
    )
    updated, count = re.subn(r'<time datetime="[^"]+">.*?</time>', time_element, updated)
    if count != 1:
        raise ValueError("Expected exactly one documentation update date")
    return updated


def main() -> int:
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument(
        "--check", action="store_true", help="Fail if index.html needs regeneration"
    )
    arguments.add_argument("--date", help="Release date YYYY-MM-DD; defaults to the changelog date")
    args = arguments.parse_args()
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from mirror_url._version import __version__

    path = REPO_ROOT / "index.html"
    source = path.read_text(encoding="utf-8")
    try:
        released = release_date(
            (REPO_ROOT / "CHANGELOG.md").read_text(encoding="utf-8"), __version__, args.date
        )
        updated = update_site(source, capture_parser(), __version__, released)
    except ValueError as error:
        arguments.error(str(error))
    if args.check:
        if source != updated:
            print("index.html is stale; run python scripts/render_site.py", file=sys.stderr)
            return 1
        print(f"index.html matches MirrorURL v{__version__} and its CLI parser")
    else:
        path.write_text(updated, encoding="utf-8")
        print(f"Updated index.html for MirrorURL v{__version__}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
