"""Remote directory discovery.

Migrated verbatim from ``mirror_url.py`` (orig. lines 8417-8632): ``DirectoryScanner``.
"""

from __future__ import annotations

import hashlib
import logging
import time
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin, urlparse

from .compat import LXML_AVAILABLE, XPath, html
from .constants import HTML_CACHE_MAX_AGE_HOURS, MAX_HTML_CACHE_SIZE, MAX_IN_MEMORY_CACHE_SIZE
from .decorators import log_performance
from .exceptions import ParsingError
from .parsing import AdaptiveBatchProcessor, extract_links_fast, should_use_fast_parser
from .primitives import LRUCache
from .storage import FileSystemCache
from .utils import sanitize_url_for_log, trim_url


class DirectoryScanner:
    """High-performance directory scanner"""

    LINK_XPATH = XPath("//a[@href]") if LXML_AVAILABLE else None

    def __init__(self, mirror_instance):
        self.mirror = mirror_instance
        self.client = (
            mirror_instance.connection_manager
            if hasattr(mirror_instance, "connection_manager")
            else None
        )
        self.base_url = getattr(mirror_instance, "base_url", "")
        # FIX: Use getattr with default to handle MockMirror
        self.target_base_url = getattr(mirror_instance, "target_base_url", self.base_url)
        self.target_dir = getattr(mirror_instance, "target_dir", None)
        self.config = getattr(mirror_instance, "config", None)
        self.metrics = getattr(mirror_instance, "metrics", None)
        self.parse_cache = LRUCache(
            maxsize=MAX_IN_MEMORY_CACHE_SIZE, ttl_seconds=3600, name="parse_cache"
        )
        self._last_cache_cleanup = time.time()
        self._cache_cleanup_interval = 300
        # FIX: html_cache should be LRUCache, not a dict
        self.html_cache = LRUCache(
            maxsize=MAX_HTML_CACHE_SIZE,
            ttl_seconds=HTML_CACHE_MAX_AGE_HOURS * 3600,
            name="html_cache",
        )
        self.batch_processor = AdaptiveBatchProcessor()
        self.fs_cache = FileSystemCache()
        self.fast_parse_count = 0
        self.lxml_parse_count = 0
        self.cached_signatures: Dict[str, str] = {}
        self.adaptive_manager = None
        self.scan_count = 0

    def _maybe_cleanup_cache(self) -> None:
        """Periodically clean up cache"""
        now = time.time()
        if now - self._last_cache_cleanup > self._cache_cleanup_interval:
            self.parse_cache.shrink_to(0.5)
            self._last_cache_cleanup = now
            logging.debug("Parse cache shrunk to 50%")

    @log_performance("directory_scan")
    def scan_directory_sequential(self, url: str) -> Tuple[List[str], List[str]]:
        """
        Scan a single directory sequentially.

        Args:
            url: Directory URL to scan

        Returns:
            Tuple of (files, subdirs)
        """
        self._maybe_cleanup_cache()
        url = trim_url(url)

        cached = self.parse_cache.get(url)
        if cached:
            self.metrics.increment("cache_hits")
            return cached

        cached_result = self.mirror.cache_manager.get_html_cache(url)
        if cached_result:
            files, subdirs = cached_result
            self.parse_cache.put(url, (files, subdirs))
            logging.debug(f"HTML cache hit for {sanitize_url_for_log(url)}")
            return files, subdirs

        try:
            files, subdirs = self._perform_scan(url)
        except ParsingError as e:
            # FIX: scan failed (non-200 or exception). Do NOT cache an empty
            # result — neither in parse_cache nor the persisted html_cache —
            # so a transient error doesn't mask real files for the rest of
            # this run or future runs. Return empty for this call only.
            logging.debug(f"Not caching failed scan for {sanitize_url_for_log(url)}: {e}")
            return [], []

        self.parse_cache.put(url, (files, subdirs))

        if self.config.cache_html:
            content_hash = hashlib.new(
                self.config.hash_algorithm, str(files + subdirs).encode()
            ).hexdigest()
            self.mirror.cache_manager.set_html_cache(url, files, subdirs, content_hash)

        self.scan_count += 1
        return files, subdirs

    def _perform_scan(self, url: str) -> Tuple[List[str], List[str]]:
        """Perform actual directory scanning"""
        files = []
        subdirs = []

        try:
            self.metrics.start_parse_timer()
            start = time.time()

            response = self.client.request(url, method="GET", timeout=30)
            self.metrics.add_request_time(time.time() - start)

            if response.status_code != 200:
                self.metrics.stop_parse_timer()
                logging.debug(
                    f"Directory scan returned {response.status_code}: {sanitize_url_for_log(url)}"
                )
                # FIX: a non-200 is a SCAN FAILURE, not a confirmed-empty
                # directory. Returning ([], []) here let the caller cache an
                # empty result (in parse_cache AND the persisted html_cache),
                # poisoning the rest of the run — and future runs — with a
                # bogus "no files here". Raise so scan_directory_sequential
                # can return empty WITHOUT caching it.
                raise ParsingError(f"HTTP {response.status_code} scanning {url}")

            content_length = len(response.content)

            if should_use_fast_parser(content_length, self.config):
                links = extract_links_fast(response.content)
                self.fast_parse_count += 1
                self.metrics.increment("fast_parses")
                logging.debug(
                    f"Fast parser used for {sanitize_url_for_log(url)} ({content_length} bytes)"
                )
            else:
                if not LXML_AVAILABLE:
                    logging.warning(
                        f"lxml not available, falling back to fast parser for {sanitize_url_for_log(url)}"
                    )
                    links = extract_links_fast(response.content)
                    self.fast_parse_count += 1
                    self.metrics.increment("fast_parses")
                else:
                    tree = html.fromstring(response.content)
                    links = []
                    for link in self.LINK_XPATH(tree):
                        href = link.get("href")
                        if href:
                            links.append(href)
                    self.lxml_parse_count += 1
                    self.metrics.increment("lxml_parses")
                    logging.debug(
                        f"LXML parser used for {sanitize_url_for_log(url)} ({content_length} bytes)"
                    )

            # Pre-parse the canonical base scope ONCE per call so the per-link
            # check below is just a string compare on the (already-parsed)
            # netloc and a path-prefix check on the (already-parsed) path.
            base_parsed_for_scope = urlparse(self.base_url)
            base_netloc = base_parsed_for_scope.netloc
            base_path = base_parsed_for_scope.path or "/"
            if not base_path.endswith("/"):
                base_path = base_path + "/"

            for href in links:
                if href in ("../", "./") or href.startswith(("?", "#", "javascript:", "mailto:")):
                    continue

                full_url = trim_url(urljoin(url, href).split("#")[0])

                # FIX (scope bypass): the previous check was
                #     if not full_url.startswith(self.base_url): continue
                # which is a textual prefix match. With the canonical
                # ``self.base_url`` stored without a trailing slash (CLI args
                # strip it), a base of ``https://example.com`` matched
                # ``https://example.com.attacker.com/...`` — a real scope
                # bypass for any HTML page the scanner parsed. Compare the
                # parsed netloc and a slash-terminated path prefix instead.
                try:
                    full_parsed = urlparse(full_url)
                except Exception:
                    continue
                if full_parsed.scheme not in ("http", "https"):
                    continue
                if full_parsed.netloc != base_netloc:
                    continue
                full_path = full_parsed.path or "/"
                # Allow exact match of base_path's parent (e.g. base "/files/"
                # should also accept the bare "/files" link).
                if not (full_path == base_path.rstrip("/") or full_path.startswith(base_path)):
                    continue

                if full_url.endswith("/"):
                    if self.mirror._is_within_target_scope(full_url):
                        subdirs.append(full_url)
                else:
                    if self.mirror.matches_filter(full_url):
                        files.append(full_url)

            self.metrics.stop_parse_timer()
            self.metrics.increment("directories_processed")
            self.metrics.increment("directories_scanned_sequential")

            logging.debug(
                f"Scan complete for {sanitize_url_for_log(url)}: {len(files)} files, {len(subdirs)} subdirs"
            )
            return files, subdirs

        except ParsingError:
            # Already-classified scan failure (e.g. non-200). Re-raise so the
            # caller does not cache an empty result.
            self.metrics.stop_parse_timer()
            raise
        except Exception as e:
            self.metrics.stop_parse_timer()
            logging.error(f"Error scanning {sanitize_url_for_log(url)}: {e}")
            self.metrics.add_error(str(e), "directory_scan")
            # FIX: surface the failure instead of returning ([], []), which
            # would be cached as an authoritative empty directory.
            raise ParsingError(f"Scan failed for {url}: {e}") from e

    def get_parse_stats(self) -> Dict[str, Any]:
        """Get parser statistics"""
        return {
            "fast_parses": self.fast_parse_count,
            "lxml_parses": self.lxml_parse_count,
            "unique_directories_cached": len(set(self.parse_cache.cache.keys())),
            "total_scans": self.scan_count,
            "parse_cache_lookups": {
                "hits": self.parse_cache.hits,
                "misses": self.parse_cache.misses,
                "hit_rate": self.parse_cache.get_stats()["hit_rate"],
            },
            "html_cache": self.html_cache.get_stats(),
            "batch_processor": {"current_batch_size": self.batch_processor.get_batch_size()},
        }


__all__ = ["DirectoryScanner"]
