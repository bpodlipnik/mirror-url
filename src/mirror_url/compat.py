"""Optional-dependency detection and the StringZilla ``Str`` fallback.

Centralizes every ``try: import …`` guard from the legacy monolith
(orig. lines 124-176) so the rest of the package imports a single boolean flag
(``*_AVAILABLE``) or the resolved symbol, never its own try/except block.
"""

from __future__ import annotations

# StringZilla with fallback for environments without it
try:
    from stringzilla import Str

    STRINGZILLA_AVAILABLE = True
except ImportError:
    STRINGZILLA_AVAILABLE = False

    # Provide a fallback implementation that mimics Str interface
    class Str(str):  # type: ignore[no-redef]
        __slots__ = ()

        def startswith(self, prefix, start=0, end=None):
            if end is not None:
                return super().startswith(str(prefix), start, end)
            return super().startswith(str(prefix), start)

        def find(self, sub, start=0, end=None):
            if end is not None:
                return super().find(str(sub), start, end)
            return super().find(str(sub), start)

        def rfind(self, sub, start=0, end=None):
            if end is not None:
                return super().rfind(str(sub), start, end)
            return super().rfind(str(sub), start)

        def endswith(self, suffix, start=0, end=None):
            if end is not None:
                return super().endswith(str(suffix), start, end)
            return super().endswith(str(suffix), start)


# Optional dependencies
try:
    from tqdm import tqdm

    TQDM_AVAILABLE = True
except ImportError:
    tqdm = None  # type: ignore[assignment]
    TQDM_AVAILABLE = False

try:
    from lxml import html
    from lxml.etree import XPath

    LXML_AVAILABLE = True
except ImportError:
    html = None  # type: ignore[assignment]
    XPath = None  # type: ignore[assignment]
    LXML_AVAILABLE = False

try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None  # type: ignore[assignment]
    PSUTIL_AVAILABLE = False


__all__ = [
    "Str",
    "STRINGZILLA_AVAILABLE",
    "tqdm",
    "TQDM_AVAILABLE",
    "html",
    "XPath",
    "LXML_AVAILABLE",
    "psutil",
    "PSUTIL_AVAILABLE",
]
