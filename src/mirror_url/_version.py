"""Single source of version/author metadata.

Kept in sync with the ``version`` field in ``pyproject.toml`` and
``__version__`` in the legacy ``mirror_url.py`` (orig. line 184).
"""

from __future__ import annotations

__version__ = "3.1.17"
__author__ = "BP"

__all__ = ["__version__", "__author__"]
