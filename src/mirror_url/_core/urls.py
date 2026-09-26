"""UrlMixin: URL scheme/scope validation and path helpers.

Methods extracted verbatim from the original ``MirrorURL`` class
(see ``REFACTORING_PLAN.md`` §4.1). Composed into ``MirrorURL`` in
``core/__init__.py``; relies on shared state set up by ``_MirrorBase.__init__``.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from urllib.parse import ParseResult, unquote, urljoin, urlparse

from ..compat import Str
from ..exceptions import PathTraversalError
from ..security import PathSafety
from ..utils import sanitize_url_for_log


@lru_cache(maxsize=10000)
def _parse_url_cached_module(url: str) -> ParseResult:
    """Module-level, instance-independent cache for urlparse(url).

    Kept separate from UrlsMixin._parse_url_cached so the cache key is the
    URL string alone -- not (self, url) -- which would otherwise pin every
    MirrorURL instance in the cache for the process lifetime. See
    REFACTORING_PLAN.md §4.1.
    """
    return urlparse(url)


class UrlMixin:
    @staticmethod
    def _validate_url_scheme(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla (SIMD-accelerated).

        Args:
            url: URL to validate

        Returns:
            True if scheme is http or https
        """
        url_sz = Str(url)

        # SIMD-accelerated prefix checks
        if url_sz.startswith(Str("http://")):
            return True

        if url_sz.startswith(Str("https://")):
            return True

        return False

    @staticmethod
    def _validate_url_scheme_fast(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla (SIMD-accelerated).

        Args:
            url: URL to validate

        Returns:
            True if scheme is http or https
        """
        if not url:
            return False

        url_sz = Str(url)

        # Check for http:// (SIMD-accelerated)
        if url_sz.startswith(Str("http://")):
            return True

        # Check for https:// (SIMD-accelerated)
        if url_sz.startswith(Str("https://")):
            return True

        return False

    @staticmethod
    def _validate_url_scheme_fallback(url: str) -> bool:
        """
        Fallback URL scheme validation using urllib.parse.

        Args:
            url: URL to validate

        Returns:
            True if scheme is http or https
        """
        parsed = urlparse(url)
        return parsed.scheme in ["http", "https"]

    def _get_last_path_component(self, url: str) -> str:
        """
        Get last path component from URL.

        Args:
            url: URL to extract from

        Returns:
            Last path component or "root" if empty
        """
        parsed = urlparse(url)
        path = parsed.path.rstrip("/")
        return os.path.basename(path) if path else "root"

    def _get_target_base_url(self) -> str:
        """
        Get target base URL for current suffix.

        Returns:
            Full target URL with suffix appended

        Raises:
            PathTraversalError: If suffix contains path traversal
        """
        base = str(self.config.base_url).rstrip("/") + "/"
        suffix = self.config.dir_suffix.strip("/") if self.config.dir_suffix else ""

        if suffix:
            # FIX: Replace pathlib check with direct string validation.
            # pathlib normalizes paths differently across OSes (Windows vs POSIX),
            # which can lead to false negatives for URL-based traversal attacks.
            # A raw string check is faster, OS-agnostic, and strictly catches URL traversal.
            if ".." in suffix or suffix.startswith("/") or "//" in suffix:
                raise PathTraversalError(f"Invalid directory suffix: {suffix}")

            safe_parts = []
            for part in suffix.split("/"):
                if part:
                    safe_parts.append(
                        PathSafety._safe_filename(part, max_len=self.config.max_filename_len)
                    )
            safe_suffix = "/".join(safe_parts)
            return urljoin(base, safe_suffix + "/")

        return base

    def _is_url_within_scope(self, url: str, check_base: bool = True) -> bool:
        """
        Optimized URL scope checking using StringZilla.
        """
        try:
            # Use fast validation
            if not self._validate_url_scheme_fast(url):
                return False

            # Fast path extraction using StringZilla
            url_path = self._get_url_path_fast(url)
            if not url_path:
                return False

            # Get scope path
            if check_base:
                scope_path = self.base_parsed.path
            else:
                if not self.target_parsed:
                    return False
                scope_path = self.target_parsed.path

            # Ensure scope_path ends with / for proper prefix matching
            if scope_path and not scope_path.endswith("/"):
                scope_path = scope_path + "/"

            # Convert url_path to string for comparison (StringZilla Str works with startswith)
            url_path_str = str(url_path) if hasattr(url_path, "__str__") else url_path

            # Check if url_path starts with scope_path
            if url_path_str.startswith(scope_path):
                return True

            # Also check without trailing slash for root-level files
            if scope_path.endswith("/"):
                scope_path_no_slash = scope_path[:-1]
                if url_path_str == scope_path_no_slash:
                    return True
                if url_path_str.startswith(scope_path_no_slash + "/"):
                    return True

            # Get remaining path for traversal detection
            remaining = url_path_str[len(scope_path.rstrip("/")) :] if scope_path else url_path_str

            # Fast path traversal detection using StringZilla
            remaining_sz = Str(remaining)
            if remaining_sz.find("..") >= 0:
                logging.warning(f"Path traversal attempt in URL: {sanitize_url_for_log(url)}")
                return False

            # Check for dot segments
            if remaining_sz.find("/.") >= 0 or remaining_sz.find("./") >= 0:
                logging.warning(f"Current directory reference in URL: {sanitize_url_for_log(url)}")
                return False

            # Check for encoded path traversal
            remaining_str = str(remaining_sz)
            if "%2e" in remaining_str.lower() or "%2f" in remaining_str.lower():
                try:
                    decoded = unquote(remaining_str)
                    if ".." in decoded or "/." in decoded:
                        logging.warning(
                            f"Encoded path traversal in URL: {sanitize_url_for_log(url)}"
                        )
                        return False
                except Exception:
                    pass

            return True

        except Exception as e:
            logging.debug(f"Error in URL scope check: {e}")
            return False

    def _is_within_target_scope(self, url: str) -> bool:
        """
        Check if URL is within target scope.

        If a target has not yet been resolved (e.g. when ``test_connection``
        hasn't been called or didn't succeed — typical in unit tests that
        patch the network after ``__init__``), fall back to the base-URL
        scope check rather than treating every URL as out of scope. Without
        this fallback, the directory scanner silently drops every subdir.

        Args:
            url: URL to check

        Returns:
            True if URL is within target (or base, when target is unset) scope
        """
        if getattr(self, "target_parsed", None) is None:
            return self._is_url_within_scope(url, check_base=True)
        return self._is_url_within_scope(url, check_base=False)

    def _is_dir_excluded(self, url: str) -> bool:
        """
        Check if a directory URL should be excluded based on exclude_dirs
        config.

        Each --exclude-dir pattern is matched as an EXACT path relative to
        the scan root (--url), never a suffix match against arbitrary
        depth in the tree. `--exclude-dir lasco` excludes only
        `<root>/lasco/` -- never `<root>/setup/lasco/`, or any other
        nested directory that happens to also be named "lasco" somewhere
        else in a large tree. `--exclude-dir idl/beta` likewise excludes
        only the specific two-level path `<root>/idl/beta/`, not any
        "idl/beta" found elsewhere.

        This replaces an earlier suffix-match design (`path.endswith(pattern)`)
        that silently matched a short pattern anywhere in the crawled
        tree, at any depth -- a demonstrated, unbounded, silent
        data-loss risk with no warning when it fired on an unintended
        directory (see CHANGELOG). There is no "match at any depth"
        fallback: if a pattern isn't found relative to root, it isn't
        excluded, full stop -- being unable to exclude something is a
        visible, harmless no-op; silently excluding the wrong thing
        elsewhere in the tree is not.

        Escape hatch for genuinely wanting "this name anywhere": a glob
        pattern is still matched against the full root-relative path
        (not anchored to being rooted at depth 0 the way a plain pattern
        is), so `--exclude-dir '*/lasco'` matches `<root>/setup/lasco/`
        (and `<root>/lasco/` itself, and any other depth) -- opt-in,
        explicit, and visible in the pattern itself, rather than being
        every plain pattern's silent default behavior.

        Args:
            url: Directory URL to check
        Returns:
            True if directory should be excluded
        """
        if not self.config.exclude_dirs:
            return False

        # Root is --url itself, never --url + --dir-suffix. target_base_url
        # already has --dir-suffix appended (see _get_target_base_url), so
        # using it here would silently re-root every pattern under the
        # suffix instead of under the documented root -- e.g. with
        # --dir-suffix 20260908, --exclude-dir lasco would need to (and
        # silently start to) mean <root>/20260908/lasco/ instead of the
        # documented <root>/lasco/, while --url stays what CHANGELOG.md and
        # USER_GUIDE.md both promise "relative to --url" means.
        root_url = str(getattr(self.config, "base_url", "") or "")
        if root_url and not root_url.endswith("/"):
            root_url += "/"

        url_normalized = url.rstrip("/") + "/"
        if not root_url or not url_normalized.startswith(root_url):
            # Candidate isn't under the scan root at all -- shouldn't
            # normally happen, since BFS only ever enqueues URLs within
            # target_base_url, but fail closed: no relative path means
            # no match, never an accidental broad one.
            return False

        relative_path = url_normalized[len(root_url) :].rstrip("/")
        if not relative_path:
            return False  # the root itself is never excludable this way

        for pattern in self.config.exclude_dirs:
            pattern_clean = pattern.strip("/")
            if not pattern_clean:
                continue

            if "*" in pattern_clean:
                regex_pattern = re.escape(pattern_clean).replace(r"\*", ".*")
                if re.fullmatch(regex_pattern, relative_path):
                    return True
            elif relative_path == pattern_clean:
                return True

        return False

    def _parse_url_cached(self, url: str) -> ParseResult:
        """
        Cached URL parsing for performance.

        Delegates to a module-level cached function rather than caching on
        the method directly: urlparse(url) depends only on ``url``, not on
        ``self``, so an instance-method-level ``@lru_cache`` includes ``self``
        in the cache key. That pins every instance in the cache indefinitely
        (each instance is a distinct, never-evicted key) and gains nothing
        from sharing, since the same URL parses identically regardless of
        which instance asks. See REFACTORING_PLAN.md §4.1.

        Args:
            url: URL to parse

        Returns:
            Parsed URL result
        """
        return _parse_url_cached_module(url)

    def _get_url_path_fast(self, url: str) -> str:
        """Fast path extraction using StringZilla - returns string."""
        if not url:
            return ""

        url_sz = Str(url)
        # Find the path part after the domain
        after_protocol = url_sz.find("://")
        if after_protocol < 0:
            return ""

        path_start = url_sz.find("/", after_protocol + 3)
        if path_start < 0:
            return ""

        # Return as string for easier comparison
        return str(url_sz[path_start:])

    def _get_filename_fast(self, url: str) -> Str:
        """
        Fast filename extraction using StringZilla.
        """
        path_sz = self._get_url_path_fast(url)
        if not path_sz:
            return Str("")

        last_slash = path_sz.rfind("/")
        if last_slash >= 0:
            return path_sz[last_slash + 1 :]

        return path_sz
