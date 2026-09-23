"""MirrorURL — security-hardened remote directory mirroring tool.

Recursively discovers files behind an HTTP(S) directory listing and mirrors
them locally with adaptive concurrency, resumable/partial downloads, integrity
verification, and an SSRF-hardened transport layer.

Public API::

    from mirror_url import MirrorURL, MirrorConfig, main, load_config_from_args
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
