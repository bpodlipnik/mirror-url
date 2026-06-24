"""MirrorURL — enterprise-grade remote directory mirroring tool.

This package is the modular successor to the single-file ``mirror_url.py``
script. During the refactor the monolith remains the runnable source of truth;
the submodules here are being populated incrementally (see
``REFACTORING_PLAN.md``).

Public API (intended, once migration completes)::

    from mirror_url import MirrorURL, MirrorConfig, main

The re-exports below are commented out until the corresponding modules are
populated, so that ``import mirror_url`` succeeds at every step of the migration.
"""

from __future__ import annotations

from ._version import __author__, __version__

# --- Public API --------------------------------------------------------------
from .cli import main
from .config import MirrorConfig, load_config_from_args
from .core import MirrorURL
from .exceptions import (
    ConfigError,
    DownloadError,
    MirrorConnectionError,
    MirrorError,
    PathTraversalError,
    SecurityError,
    URLScopeError,
)

__all__ = [
    "__version__",
    "__author__",
    "MirrorURL",
    "MirrorConfig",
    "load_config_from_args",
    "main",
    "MirrorError",
    "MirrorConnectionError",
    "PathTraversalError",
    "URLScopeError",
    "ConfigError",
    "SecurityError",
    "DownloadError",
]
