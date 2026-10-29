"""Resolve CLI directory patterns through the existing owned discovery walk."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from contextlib import ExitStack, closing

from .config import MirrorConfig
from .core import MirrorURL
from .enums import CleanupPolicy
from .exceptions import ConfigError
from .filename_mapping import selected_directory_targets
from .utils import _relative_url_path


def compile_directory_patterns(patterns: Sequence[str]) -> list[re.Pattern[str]]:
    """Compile explicit full-path, case-sensitive selectors before any I/O."""
    compiled = []
    for pattern in patterns:
        if not pattern:
            raise ConfigError("--dir-regex requires nonempty patterns")
        try:
            compiled.append(re.compile(pattern))
        except re.error as error:
            raise ConfigError(f"Invalid --dir-regex pattern {pattern!r}: {error}") from error
    return compiled


def resolve_directory_selection(
    config: MirrorConfig,
    patterns: Sequence[str],
    *,
    mirror_factory: Callable[..., MirrorURL] = MirrorURL,
) -> dict[str, int]:
    """Return nonoverlapping roots and their remaining traversal budgets.

    Regex discovery and its selected subtrees share the original root-relative
    depth budget. Literal selection remains a separate CLI mode.
    Never return a partial plan after a failed listing or implicitly select ROOT.
    """
    compiled = compile_directory_patterns(patterns)
    if not compiled:
        raise ConfigError("Directory regex selection requires at least one pattern")
    selection = config.model_copy(
        update={
            "dir_suffix": "",
            "dry_run": True,
            "cleanup_policy": CleanupPolicy.SAFE_NO_DELETE,
            "no_cache": True,
            "cache_html": False,
            "use_shared_log": True,
            "metrics_json": None,
            "use_disk_backed_sets": False,
            "disk_cache_dir": None,
            "chunk_assembly_dir": None,
            "parallel_downloads": False,
            "streaming_parallel": False,
            "auto_concurrency": False,
            "connection_pool_prewarm": False,
            "progress_bar": False,
        }
    )
    roots: dict[str, int] = {}
    matches = 0
    with mirror_factory(selection, log_configuration=False) as mirror, ExitStack() as operations:
        guard = mirror._destination_lock
        if guard is None:
            raise ConfigError("Directory selection requires destination ownership")
        operations.enter_context(guard.operation())
        if not mirror.connection_ok:
            raise ConfigError("Directory selection failed: remote root is unavailable")
        mirror.install_signal_handlers()
        with closing(mirror._discover_directories_bfs()) as discovered:
            for url in discovered:
                relative = _relative_url_path(url, str(config.base_url))
                if relative is None:
                    raise ConfigError("Directory selection returned a path outside --url")
                relative = relative.rstrip("/")
                depth = len(relative.split("/"))
                if (
                    relative
                    and depth <= config.max_depth
                    and any(pattern.fullmatch(relative) for pattern in compiled)
                ):
                    matches += 1
                    roots.setdefault(relative, config.max_depth - depth)
        if mirror.scan_incomplete:
            raise ConfigError("Directory selection failed: directory listing is incomplete")
        selected_directory_targets(
            config.dest_path, roots, max_filename_len=config.max_filename_len
        )
    if not matches:
        raise ConfigError("No directories matched --dir-regex; nothing was processed")
    # Ancestor membership uses path components, not a textual prefix (a != ab).
    selected = {}
    for path, budget in roots.items():
        ancestors = ["/".join(path.split("/")[:index]) for index in range(len(path.split("/")))]
        if not any(ancestor in roots and ancestor != path for ancestor in ancestors):
            selected[path] = budget
    return selected
