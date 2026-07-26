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
import sys
import time
from collections import deque
from pathlib import Path
from re import error as re_error
from typing import Dict, Generator, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

import httpx

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
                    # Simple substring match. NOTE: this deliberately uses
                    # the plain-str `filename` (computed above), not a
                    # StringZilla Str-vs-Str comparison. _get_filename_fast()
                    # is typed as returning a StringZilla Str, but
                    # _get_url_path_fast() (urls.py) explicitly converts
                    # back to a plain str before returning it, so filename_sz
                    # is actually a plain str here despite its name and type
                    # hint. Comparing a real stringzilla.Str pattern against
                    # a plain str with `in` raises TypeError: "'in <string>'
                    # requires string as left operand, not stringzilla.Str"
                    # -- reproducible for any non-extension, non-regex
                    # --filter pattern (e.g. --filter _fe_) whenever the
                    # real stringzilla package is installed (the pure-Python
                    # compat.py fallback Str subclasses str, so it never hit
                    # this). Plain str `in` is correct and simple here; a
                    # few extra bytes of filename comparison is not where
                    # StringZilla's SIMD advantage would matter anyway.
                    if pattern_lower in filename:
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
        # FIX (partial-scan guard): fresh state for this run -- see
        # _discover_directories_bfs and CleanupMixin.clean_obsolete.
        self.scan_incomplete = False

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

            # Retain this run's freshly computed signatures (not just the
            # ones we're about to save to disk for *next* run) so the
            # compare step can tell whether a directory actually changed
            # since the cache was last written, instead of just trusting
            # any previously-cached entry unconditionally regardless of
            # whether its value is still current.
            self.scanner.fresh_dir_signatures = dir_signatures

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

    def list_directories(self) -> bool:
        """Discover, log, and print the directory tree under the target URL /
        --dir-suffix, without scanning files, comparing freshness, or
        downloading/deleting anything.

        Enabled via --list-dirs. Reuses the same BFS walk as
        get_remote_files() -- so it respects --exclude-dir and --max-depth
        exactly like a real sync would -- but stops after discovering each
        directory instead of also scanning it for files. --filter does not
        apply here since it only matches files, never directory names.

        Each discovered directory is both logged (with the usual run
        banner/prefix, subject to --print-logs/--quiet like any other log
        line) AND printed as a bare path to stdout -- one per line, no
        prefix, no icon, no summary line -- so the output can be piped or
        captured directly (e.g. ``mirror-url --list-dirs ... | xargs -I{}
        ...``) without needing --print-logs or filtering out banner/log
        noise. The two are independent: stdout stays clean even when
        --print-logs sends the full banner to stderr. When more than one
        --dir-suffix is mirrored in the same run, each stdout line is
        prefixed with a tab-separated suffix column (``L1/v2\t.``) instead
        of a bare path, since a relative path alone would be ambiguous
        about which suffix it came from.

        Read-only: never touches the on-disk cache and is safe to run
        regardless of --dry-run.
        """
        prefix = self._get_prefix()
        if not hasattr(self, "connection_manager") or not self.connection_manager:
            logging.warning(f"{prefix}Skipping --list-dirs (connection failed)")
            return False

        if not self.connection_ok:
            logging.info(f"{prefix}Skipping --list-dirs - remote directory not available")
            return False

        # When mirroring more than one --dir-suffix in the same run, a bare
        # relative path is ambiguous about which suffix it came from -- the
        # log prefix disambiguates via "[i/total]", but that index is
        # meaningless on stdout without the surrounding log context. Qualify
        # each printed line with a tab-separated suffix column instead, so
        # each line is still a single, easily awk/cut-able path plus its
        # suffix rather than a glued-together compound path.
        stdout_qualifier = ""
        total_suffixes = getattr(self, "total_suffixes", 1)
        dir_suffix = getattr(getattr(self, "config", None), "dir_suffix", None)
        if total_suffixes > 1 and dir_suffix:
            stdout_qualifier = dir_suffix.strip("/")

        root = self.target_base_url or ""
        safe_root = sanitize_url_for_log(root) if root else ""
        count = 0
        for url in self._discover_directories_bfs():
            safe_url = sanitize_url_for_log(url)
            rel = (
                safe_url[len(safe_root) :].strip("/")
                if safe_root and safe_url.startswith(safe_root)
                else safe_url
            )
            label = rel if rel else "."
            logging.info(f"{prefix}📁 {label}")
            print(f"{stdout_qualifier}\t{label}" if stdout_qualifier else label, file=sys.stdout)
            count += 1

        logging.info(f"{prefix}Found {count} director{'y' if count == 1 else 'ies'}")
        return True

    def list_files(self) -> bool:
        """Discover, log, and print the files under the target URL /
        --dir-suffix, without comparing freshness or downloading/deleting
        anything.

        Enabled via --list-files[=N]. Reuses the same BFS directory walk and
        per-directory file scan as a real sync -- so it respects
        --exclude-dir, --max-depth, and --filter exactly like a real sync
        would -- but never touches the on-disk cache, never compares
        freshness, and never downloads or deletes anything.

        With no N (self.config.list_files_n == 0), every file matching
        --filter is printed. With N > 0, only the last N files *per
        directory* are printed, where "last" means the lexicographically
        greatest filenames within that directory -- NOT a true timestamp
        sort. This is deliberate: getting a real server timestamp would
        need either parsing a directory-listing HTML format that varies by
        web server (Apache/nginx/IIS/lighttpd all differ, and some don't
        expose one at all) or an extra HEAD request per file, which does
        not scale to the file counts these runs see (the exact problem
        --missing-files was built to avoid at v3.1.26). A lexicographic
        sort needs neither: it works identically regardless of web server,
        and costs zero extra requests, but it only reflects chronological
        order if filenames embed a sortable date/sequence -- true for the
        PROBA-3/STEREO archives this tool targets, but not guaranteed for
        an arbitrary directory. See USER_GUIDE.md.

        Every directory's file block is followed by a "# Files N/total"
        comment line, always -- including an unrestricted (no-N) run --
        so downstream parsing can rely on the marker being present
        unconditionally rather than only appearing when truncated.
        Comment lines start with "#" and can be dropped with
        ``grep -v '^#'`` for a pure one-line-per-file stream.

        Each printed file uses its full path relative to the target
        URL/--dir-suffix (e.g. ``v03/orbit_0042/file_20260722_003.fits``),
        one per line -- unqualified, since the path itself already encodes
        which directory (and, transitively, which --dir-suffix subtree) it
        came from. When more than one --dir-suffix is mirrored in the same
        run, each line is additionally prefixed with a tab-separated
        suffix column, exactly like --list-dirs.

        Read-only: never touches the on-disk cache and is safe to run
        regardless of --dry-run.
        """
        prefix = self._get_prefix()
        if not hasattr(self, "connection_manager") or not self.connection_manager:
            logging.warning(f"{prefix}Skipping --list-files (connection failed)")
            return False

        if not self.connection_ok:
            logging.info(f"{prefix}Skipping --list-files - remote directory not available")
            return False

        stdout_qualifier = ""
        total_suffixes = getattr(self, "total_suffixes", 1)
        dir_suffix = getattr(getattr(self, "config", None), "dir_suffix", None)
        if total_suffixes > 1 and dir_suffix:
            stdout_qualifier = dir_suffix.strip("/")

        limit = getattr(getattr(self, "config", None), "list_files_n", 0) or 0

        root = self.target_base_url or ""
        safe_root = sanitize_url_for_log(root) if root else ""

        def _rel(url: str) -> str:
            safe_url = sanitize_url_for_log(url)
            return (
                safe_url[len(safe_root) :].strip("/")
                if safe_root and safe_url.startswith(safe_root)
                else safe_url
            )

        total_count = 0
        printed_count = 0
        for dir_url in self._discover_directories_bfs():
            try:
                files, _subdirs = self.scanner.scan_directory_sequential(dir_url)
            except Exception as e:
                logging.warning(f"{prefix}Error scanning {sanitize_url_for_log(dir_url)}: {e}")
                continue

            if not files:
                continue

            rel_files = sorted(_rel(f) for f in files)
            dir_total = len(rel_files)
            shown = rel_files[-limit:] if limit > 0 else rel_files

            for label in shown:
                logging.info(f"{prefix}📄 {label}")
                print(
                    f"{stdout_qualifier}\t{label}" if stdout_qualifier else label,
                    file=sys.stdout,
                )
                printed_count += 1

            print(f"# Files {len(shown)}/{dir_total}", file=sys.stdout)
            total_count += dir_total

        logging.info(
            f"{prefix}Listed {printed_count} of {total_count} file{'s' if total_count != 1 else ''}"
        )
        return True

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
                # FIX (partial-scan guard): this directory's files/subdirs are
                # unknown, not empty -- treating them as [] previously made
                # get_remote_files() return a silently-incomplete listing
                # that clean_obsolete() couldn't distinguish from a complete
                # one. Flag the run as incomplete so cleanup is skipped.
                logging.warning(
                    f"Error scanning {url}: {e} -- directory listing incomplete, "
                    f"cleanup will be skipped this run"
                )
                self.scan_incomplete = True
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
                create_base=not self.config.dry_run,
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
