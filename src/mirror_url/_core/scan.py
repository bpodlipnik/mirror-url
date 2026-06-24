"""ScanMixin: Remote directory discovery, filtering, and URL→path mapping.

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import hashlib
import logging
import re
import socket
import time
from collections import deque
from pathlib import Path
from re import error as re_error
from typing import Dict, Generator, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

import httpx

from ..compat import Str
from ..decorators import log_performance
from ..enums import MemoryPressure
from ..security import PathSafety
from ..utils import sanitize_url_for_log, trim_url


class ScanMixin:
    def matches_filter(self, url: str) -> bool:
        """Optimized filter matching using StringZilla for all pattern types."""
        if not self.config.file_filters:
            return True

        # Fast filename extraction using StringZilla
        filename_sz = self._get_filename_fast(url)
        if not filename_sz:
            return False

        # Convert to string for operations that need it (endswith with tuple)
        filename = str(filename_sz)

        for pattern in self.config.file_filters:
            pattern_lower = pattern.lower()

            if pattern.startswith("."):
                # Fast extension check - use string version for compatibility
                if filename.endswith(pattern_lower):
                    return True
            else:
                # Check if pattern contains regex special characters
                has_regex = any(c in pattern for c in "*?+[]{}()|\\^$")

                if has_regex:
                    # Regex pattern - fall back to re
                    try:
                        if re.search(pattern, filename, re.IGNORECASE):
                            return True
                    except re_error:
                        pass
                else:
                    # Simple substring - use StringZilla for SIMD acceleration
                    pattern_sz = Str(pattern_lower)
                    if pattern_sz in filename_sz:
                        return True

        return False

    def get_directory_signature(self, url: str, html_content: str = None) -> str:
        """Get directory signature for cache."""
        url = trim_url(url)

        if html_content is not None:
            content_hash = hashlib.new(
                self.config.hash_algorithm, html_content.encode("utf-8")
            ).hexdigest()
            return f"content:{content_hash}"

        try:
            r = self.connection_manager.request(url, method="HEAD", timeout=15)

            if r.status_code != 200:
                return f"url:{url}"

            if "ETag" in r.headers:
                return f"etag:{r.headers['ETag']}"

            if "Last-Modified" in r.headers:
                return f"mtime:{r.headers['Last-Modified']}"

            return f"url:{url}:{int(time.time())}"
        except Exception as e:
            logging.debug(f"Error getting signature for {sanitize_url_for_log(url)}: {e}")
            return f"url:{url}:{int(time.time())}"

    def is_symlink(
        self, url: str, existing_response: Optional[httpx.Response] = None, depth: int = 0
    ) -> Tuple[bool, Optional[str]]:
        """Check if a URL points to a symlink."""
        try:
            if not self.config.handle_symlinks:
                return False, None

            if depth >= self.config.max_symlink_depth:
                self.metrics.increment("symlink_depth_exceeded")
                return True, None

            if self.symlink_tracker:
                dir_url = url.rsplit("/", 1)[0] + "/"
                can_follow, reason = self.symlink_tracker.can_follow(url, dir_url, depth)

                if not can_follow:
                    if "loop" in reason.lower():
                        self.metrics.increment("symlink_loops_detected")
                    elif "bomb" in reason.lower():
                        self.metrics.increment("symlink_bomb_prevented")
                    return True, None

            return False, None
        except Exception as e:
            logging.debug(f"Error checking symlink for {sanitize_url_for_log(url)}: {e}")
            return False, None

    def record_symlink(
        self, symlink_url: str, target_url: str, local_path: Path, depth: int = 0
    ) -> None:
        """Record symlink handling."""
        if self.config.symlink_mode == "follow":
            self.metrics.increment("symlinks_followed")
            if self.symlink_tracker:
                dir_url = symlink_url.rsplit("/", 1)[0] + "/"
                self.symlink_tracker.record_follow(symlink_url, dir_url, depth)
        elif self.config.symlink_mode == "skip":
            self.metrics.increment("symlinks_skipped")
            if self.symlink_tracker:
                self.symlink_tracker.record_skip(symlink_url)

    @log_performance("get_remote_files")
    def get_remote_files(self) -> Optional[List[str]]:
        """Get remote files list through directory discovery."""
        prefix = self._get_prefix()

        try:
            # NOTE: Both the dir_suffix/target case AND the root-level case go
            # through _discover_directories_bfs(). That generator already uses
            # self.target_base_url as its BFS root, so it handles the suffix
            # case correctly.
            #
            # FIX (v3.1.8): the previous code had a separate `if
            # self.target_base_url:` branch that scanned only the root plus
            # ONE level of immediate subdirectories (it discarded each
            # subdir's own subdirs via `sub_files, _ = ...`). That silently
            # dropped every file nested two or more levels deep, ignored
            # exclude_dirs and max_depth, and had no visited-set guard against
            # duplicate/cyclic scans. Removing the special case fixes all
            # three: BFS recurses to max_depth, applies _is_dir_excluded, and
            # dedupes via processed_dirs.

            cache_loaded, cached_signatures = self.cache_manager.load()
            if cache_loaded:
                self.scanner.cached_signatures = cached_signatures
                logging.info(
                    f"{prefix}📖 Loaded {len(cached_signatures)} directory signatures from cache"
                )

            directories = list(self._discover_directories_bfs())
            if not directories:
                logging.info(f"{prefix}No directories discovered")
                return []

            logging.info(f"{prefix}Discovered {len(directories)} directories")

            all_files: List[str] = []
            dir_signatures: Dict[str, str] = {}

            self.multi_progress.add_level(
                "directories", len(directories), prefix, self.config.progress_bar, self.config
            )

            for i, url in enumerate(directories):
                files, subdirs = self.scanner.scan_directory_sequential(url)
                all_files.extend(files)
                sig = self.get_directory_signature(url)
                dir_signatures[url] = sig
                self.multi_progress.update("directories")

                if i % 100 == 0:
                    pressure = self.memory_monitor.check_pressure()
                    if pressure != MemoryPressure.NORMAL:
                        self.metrics.increment("memory_pressure_events")

                        if pressure == MemoryPressure.WARNING:
                            freed_parse = self.scanner.parse_cache.shrink_to(0.7)
                            # FIX (inconsistency): the previous code only
                            # asked the scanner's parse_cache to shrink under
                            # WARNING pressure and ignored cache_manager
                            # entirely, even though cache_manager owns its
                            # own LRU caches that *also* need to shrink.
                            # Mirror what the CRITICAL branch does so both
                            # caches respond to memory pressure consistently.
                            freed_cache = self.cache_manager.handle_memory_pressure(pressure)
                            logging.info(
                                f"Memory pressure (warning): freed "
                                f"{freed_parse + freed_cache} cache entries"
                            )
                        elif pressure == MemoryPressure.CRITICAL:
                            freed_parse = self.scanner.parse_cache.shrink_to(0.3)
                            freed_html = self.scanner.html_cache.shrink_to(0.3)
                            freed_cache = self.cache_manager.handle_memory_pressure(pressure)
                            logging.warning(
                                f"Emergency cache clear: freed {freed_parse + freed_html + freed_cache} items"
                            )

            if not self.config.no_cache and dir_signatures and not self.config.dry_run:
                try:
                    self.cache_manager.save(dir_signatures, len(all_files))
                    logging.info(
                        f"{prefix}💾 Saved cache with {len(dir_signatures)} directory signatures"
                    )
                except Exception as e:
                    logging.warning(f"{prefix}Failed to save cache: {e}")

            logging.info(f"{prefix}Collected {len(all_files)} files")
            return all_files if all_files else []

        except Exception as e:
            logging.error(f"{prefix}Failed to get remote files: {e}")
            self.metrics.add_error(str(e), "file_discovery")
            return None

    def _discover_directories_bfs(self) -> Generator[str, None, None]:
        """BFS directory discovery - strictly within target scope."""
        if not self.connection_ok:
            logging.debug("Skipping directory discovery - connection not OK")
            return

        # Use target_base_url as the root for discovery
        root_url = self.target_base_url
        if not root_url:
            logging.warning("No target_base_url available for directory discovery")
            return

        # Ensure root_url ends with /
        if not root_url.endswith("/"):
            root_url += "/"

        logging.debug(f"BFS discovery root: {sanitize_url_for_log(root_url)}")

        queue = deque([(root_url, 0)])
        processed_dirs: Set[str] = set()

        while queue:
            url, depth = queue.popleft()

            # Skip if not within root_url
            if not url.startswith(root_url):
                logging.debug(f"Skipping URL outside root scope: {url}")
                continue

            if url in processed_dirs or depth > self.config.max_depth:
                continue

            processed_dirs.add(url)

            try:
                files, subdirs = self.scanner.scan_directory_sequential(url)
            except Exception as e:
                logging.debug(f"Error scanning {url}: {e}")
                _files, subdirs = [], []

            yield url

            for subdir in subdirs:
                # Only add subdirs that start with root_url
                if subdir not in processed_dirs and subdir.startswith(root_url):
                    if self._is_dir_excluded(subdir):
                        logging.debug(f"Excluding directory: {sanitize_url_for_log(subdir)}")
                        continue
                    queue.append((subdir, depth + 1))

            # Rate limiting
            parsed = urlparse(url)
            try:
                ip = socket.gethostbyname(parsed.hostname)
                self.per_ip_limiter.wait(ip)
            except Exception:
                pass

    def _get_local_path_from_url(self, url: str) -> Optional[Path]:
        """
        Convert URL to local path with security checks.

        Args:
            url: Remote URL

        Returns:
            Local path or None if invalid/unsafe
        """
        if self.target_dir is None:
            return None

        if self._target_dir_path is None:
            logging.debug("_target_dir_path is None, cannot compute local path")
            return None

        try:
            parsed = self._parse_url_cached(url)

            if not parsed.path.startswith(self.target_parsed.path):
                return None

            rel_path = parsed.path[len(self.target_parsed.path) :].lstrip("/")

            if ".." in rel_path or ".." in unquote(rel_path).split("/"):
                logging.warning(
                    f"Path traversal attempt detected in URL: {sanitize_url_for_log(url)}"
                )
                return None

            rel_path = unquote(rel_path)

            local_path = PathSafety.safe_join(
                self.target_dir,
                *rel_path.split("/"),
                max_depth=self.config.max_depth,
                max_filename_len=self.config.max_filename_len,
            )

            if local_path is None:
                return None

            if not PathSafety.is_subpath(self._target_dir_path, local_path):
                logging.warning(f"Security check failed: {local_path} outside {self.target_dir}")
                return None

            return local_path
        except Exception as e:
            logging.debug(f"Error converting URL to local path: {e}")
            return None
