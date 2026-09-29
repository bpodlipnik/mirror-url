"""Single source of version/author metadata.

Kept in sync with the ``version`` field in ``pyproject.toml`` by
``scripts/bump_version.sh``. (The legacy monolithic ``mirror_url.py`` this
value once also tracked was removed at v3.1.20.)
"""

from __future__ import annotations

__version__ = "3.1.66"
__author__ = "BP"

__all__ = ["__version__", "__author__"]
