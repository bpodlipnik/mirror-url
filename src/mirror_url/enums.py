"""Enum types describing run modes and state machines.

Migrated verbatim from ``mirror_url.py`` (orig. lines 453-500).
"""

from __future__ import annotations

import logging
from enum import Enum


class LogLevel(Enum):
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL


class ScanMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    ADAPTIVE = "adaptive"
    ASYNC = "async"


class CleanupPolicy(Enum):
    SAFE_NO_DELETE = "safe"
    PREVIEW = "preview"
    DELETE = "delete"
    MOVE = "move"


class DownloadPriority(Enum):
    HIGH = 0
    NORMAL = 1
    LOW = 2


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# NEW v2.0.0 enums
class MemoryPressure(Enum):
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


class ConcurrencyType(Enum):
    SYNC = "sync"
    ASYNC = "async"
    PARALLEL = "parallel"


# Add after existing enums, before data classes
class DownloadMethod(Enum):
    """Download method selection"""

    SEQUENTIAL = "sequential"
    PARALLEL_FILES = "parallel_files"  # Multiple files, no chunking
    STREAMING_PARALLEL = "streaming_parallel"  # Chunks with direct write
    TRADITIONAL_PARALLEL = "traditional_parallel"  # Chunks with temp assembly
    AUTO = "auto"  # Let system decide


__all__ = [
    "LogLevel",
    "ScanMode",
    "CleanupPolicy",
    "DownloadPriority",
    "CircuitBreakerState",
    "MemoryPressure",
    "ConcurrencyType",
    "DownloadMethod",
]
