"""MirrorError hierarchy.

Migrated verbatim from ``mirror_url.py`` (orig. lines 365-448).
"""

from __future__ import annotations


# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================
class MirrorError(Exception):
    """Base exception for MirrorURL"""

    pass


class MirrorConnectionError(MirrorError):
    """Raised when connection fails"""

    pass


class PathTraversalError(MirrorError):
    """Raised when path traversal attempt detected"""

    pass


class URLScopeError(MirrorError):
    """Raised when URL is outside configured scope"""

    pass


class ConfigError(MirrorError):
    """Raised when configuration is invalid.

    NOTE: This deliberately does NOT inherit from ``ValueError``. Pydantic v2
    catches ``ValueError`` raised inside ``@model_validator`` and rewraps it
    as ``pydantic_core.ValidationError`` — that would mask ``ConfigError`` for
    every caller that does ``pytest.raises(ConfigError, ...)`` or ``except
    ConfigError`` against a model-validator code path.
    """

    pass


class CacheError(MirrorError):
    """Raised when cache operations fail"""

    pass


class ParsingError(MirrorError):
    """Raised when HTML parsing fails"""

    pass


class AdaptiveAsyncError(MirrorError):
    """Raised when adaptive async encounters critical failure"""

    pass


class SecurityError(MirrorError):
    """Raised when security validation fails"""

    pass


class DownloadError(MirrorError):
    """Raised when download fails"""

    pass


class HealthCheckError(MirrorError):
    """Raised when health check fails"""

    pass


class SymlinkLoopError(MirrorError):
    """Raised when symlink loop is detected"""

    pass


class SymlinkBombError(MirrorError):
    """Raised when symlink bomb is detected"""

    pass


# NEW v2.0.0 exceptions
class DiskSpaceError(MirrorError):
    """Raised when disk space is insufficient"""

    pass


class MemoryPressureError(MirrorError):
    """Raised when memory pressure is critical"""

    pass


# NEW v3.0.0 exceptions
class ChunkDownloadError(MirrorError):
    """Raised when chunk download fails"""

    pass


class ChunkAssemblyError(MirrorError):
    """Raised when chunk assembly fails"""

    pass


class RangeNotSupportedError(MirrorError):
    """Raised when server doesn't support Range requests"""

    pass


class ConcurrencyLimitError(MirrorError):
    """Raised when concurrency limits are exceeded"""

    pass


__all__ = [
    "MirrorError",
    "MirrorConnectionError",
    "PathTraversalError",
    "URLScopeError",
    "ConfigError",
    "CacheError",
    "ParsingError",
    "AdaptiveAsyncError",
    "SecurityError",
    "DownloadError",
    "HealthCheckError",
    "SymlinkLoopError",
    "SymlinkBombError",
    "DiskSpaceError",
    "MemoryPressureError",
    "ChunkDownloadError",
    "ChunkAssemblyError",
    "RangeNotSupportedError",
    "ConcurrencyLimitError",
]
