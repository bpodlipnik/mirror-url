"""Untrusted listing sizes used for admission, never for transfer integrity."""

from __future__ import annotations

import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Dict, Optional

from .constants import MAX_IN_MEMORY_CACHE_SIZE


@dataclass(frozen=True)
class SizeHint:
    """A planning upper bound; GET headers still identify the representation."""

    upper: int
    rounded: bool = False


def parse_listing_size(value: str) -> Optional[SizeHint]:
    value = value.strip()
    # Bound both parsing work and arithmetic on untrusted remote text.
    if not re.fullmatch(r"[0-9]{1,18}(?:\.[0-9])?[KMGTPE]?", value):
        return None
    if value[-1].isdigit():
        return SizeHint(int(value))
    scale = 10 if "." in value else 1
    number = int(value[:-1].replace(".", ""))
    multiplier = 1024 ** ("KMGTPE".index(value[-1]) + 1)
    # Apache APR rounds its display after discarding low-order bits. One
    # complete display quantum safely covers both operations.
    upper = ((number + 1) * multiplier + scale - 1) // scale
    return SizeHint(upper, True) if upper <= 2**63 - 1 else None


class _ListingSizes(HTMLParser):
    """Recognize Apache table/pre and nginx pre rows; ignore ambiguous rows."""

    _date = re.compile(r"(?:\d{4}-\d{2}-\d{2}|\d{2}-[A-Za-z]{3}-\d{4})\s+\d{2}:\d{2}")

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.sizes: Dict[str, SizeHint] = {}
        self.ambiguous: set[str] = set()
        self.cells: Optional[list[str]] = None
        self.cell: Optional[list[str]] = None
        self.links: list[tuple[str, int]] = []
        self.in_pre = False
        self.pre_href: Optional[str] = None
        self.pre_tail = ""
        self.in_anchor = False

    def _add(self, href: str, value: str):
        hint = parse_listing_size(value)
        if hint is None or href in self.ambiguous:
            return
        if (
            href not in self.sizes
            and len(self.sizes) + len(self.ambiguous) >= MAX_IN_MEMORY_CACHE_SIZE
        ):
            return
        if href in self.sizes and self.sizes[href] != hint:
            self.sizes.pop(href)
            self.ambiguous.add(href)
        else:
            self.sizes[href] = hint

    def _pre_line(self):
        if self.pre_href:
            match = re.fullmatch(self._date.pattern + r"\s+(\S+)\s*", self.pre_tail.strip())
            if match:
                self._add(self.pre_href, match[1])
        self.pre_href, self.pre_tail = None, ""

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.cells, self.links, self.cell = [], [], None
        elif tag == "td" and self.cells is not None:
            self.cell = []
        elif tag == "pre":
            self.in_pre = True
        elif tag == "a":
            href = dict(attrs).get("href")
            self.in_anchor = True
            if href:
                if self.cells is not None and self.cell is not None:
                    self.links.append((href, len(self.cells)))
                if self.in_pre:
                    if self.pre_href:
                        self._pre_line()
                    self.pre_href = href

    def handle_endtag(self, tag):
        if tag == "td" and self.cells is not None and self.cell is not None:
            self.cells.append("".join(self.cell).strip())
            self.cell = None
        elif tag == "tr":
            if self.cells is not None and len(self.links) == 1:
                href, index = self.links[0]
                if len(self.cells) > index + 2 and self._date.fullmatch(self.cells[index + 1]):
                    self._add(href, self.cells[index + 2])
            self.cells, self.cell, self.links = None, None, []
        elif tag == "a":
            self.in_anchor = False
        elif tag == "pre":
            self._pre_line()
            self.in_pre = False

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)
        if self.in_pre and not self.in_anchor:
            pieces = data.split("\n")
            self.pre_tail += pieces[0]
            if len(pieces) > 1:
                self._pre_line()


def listing_size_hints(document: str) -> Dict[str, SizeHint]:
    if re.search(r"<(?:table|pre)(?:\s|>)", document, re.I) is None:
        return {}
    parser = _ListingSizes()
    parser.feed(document)
    parser.close()
    return parser.sizes
