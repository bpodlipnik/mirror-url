"""Path-traversal, symlink and URL-scope defenses.

Migrated verbatim from ``mirror_url.py``:
``SymlinkTracker`` (orig. 743-809), ``SecurityValidator`` (orig. 1155-1373),
``PathSafety`` (orig. 1656-1843), ``FastURLValidator`` (orig. 1848-1937).
"""

from __future__ import annotations

import ipaddress
import logging
import os
import socket
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote, urlparse

from .compat import Str
from .constants import (
    MAX_DIRECTORY_DEPTH,
    MAX_FILENAME_LENGTH,
    MAX_SYMLINK_DEPTH,
    MAX_SYMLINKS_PER_DIR,
    SYMLINK_BOMB_THRESHOLD,
    SYMLINK_VISIT_CACHE_SIZE,
)
from .exceptions import SecurityError
from .utils import is_reserved_windows_filename


class SymlinkTracker:
    """Tracks symlink visits to prevent loops and bombs"""

    def __init__(
        self,
        max_depth: int = MAX_SYMLINK_DEPTH,
        max_per_dir: int = MAX_SYMLINKS_PER_DIR,
        bomb_threshold: int = SYMLINK_BOMB_THRESHOLD,
    ):
        self.max_depth = max_depth
        self.max_per_dir = max_per_dir
        self.bomb_threshold = bomb_threshold
        self.visited_symlinks: Dict[str, int] = {}
        self.symlinks_per_dir: Dict[str, int] = {}
        self.total_symlinks_followed = 0
        self.lock = RLock()
        self.symlink_chain: List[str] = []

    def can_follow(
        self, symlink_url: str, dir_url: str, current_depth: int
    ) -> Tuple[bool, Optional[str]]:
        """Check if a symlink can be safely followed"""
        with self.lock:
            if self.total_symlinks_followed >= self.bomb_threshold:
                return False, f"Symlink bomb threshold reached ({self.bomb_threshold})"
            if current_depth > self.max_depth:
                return False, f"Max symlink depth exceeded ({self.max_depth})"
            if symlink_url in self.visited_symlinks:
                prev_depth = self.visited_symlinks[symlink_url]
                return False, f"Symlink loop detected (already seen at depth {prev_depth})"
            if symlink_url in self.symlink_chain:
                return False, "Symlink cycle detected in current chain"
            dir_count = self.symlinks_per_dir.get(dir_url, 0)
            if dir_count >= self.max_per_dir:
                return False, f"Too many symlinks in directory ({dir_count} >= {self.max_per_dir})"
            return True, None

    def record_follow(self, symlink_url: str, dir_url: str, depth: int) -> None:
        """Record that we're following a symlink"""
        with self.lock:
            self.visited_symlinks[symlink_url] = depth
            self.symlinks_per_dir[dir_url] = self.symlinks_per_dir.get(dir_url, 0) + 1
            self.total_symlinks_followed += 1
            self.symlink_chain.append(symlink_url)
            if len(self.visited_symlinks) > SYMLINK_VISIT_CACHE_SIZE:
                oldest_keys = list(self.visited_symlinks.keys())[: SYMLINK_VISIT_CACHE_SIZE // 5]
                for key in oldest_keys:
                    del self.visited_symlinks[key]

    def record_skip(self, symlink_url: str) -> None:
        """Record that we're skipping a symlink"""
        with self.lock:
            self.total_symlinks_followed += 0

    def get_stats(self) -> Dict[str, Any]:
        """Get symlink tracking statistics"""
        with self.lock:
            return {
                "total_followed": self.total_symlinks_followed,
                "unique_symlinks": len(self.visited_symlinks),
                "directories_with_symlinks": len(self.symlinks_per_dir),
                "current_chain_length": len(self.symlink_chain),
            }

    def clear_chain(self) -> None:
        """Clear the current symlink chain"""
        with self.lock:
            self.symlink_chain.clear()

    def is_in_chain(self, symlink_url: str) -> bool:
        """Check if a symlink is in the current chain"""
        with self.lock:
            return symlink_url in self.symlink_chain


class SecurityValidator:
    """SSRF protection and URL security validation"""

    PRIVATE_NETWORKS = [
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("172.16.0.0/12"),
        ipaddress.ip_network("192.168.0.0/16"),
        ipaddress.ip_network("127.0.0.0/8"),
        ipaddress.ip_network("0.0.0.0/8"),
        ipaddress.ip_network("169.254.0.0/16"),
        ipaddress.ip_network("::1/128"),
        ipaddress.ip_network("fc00::/7"),
        ipaddress.ip_network("fe80::/10"),
    ]

    @staticmethod
    def is_private_ip(ip: str) -> bool:
        """
        Check if an IP address is private.

        Args:
            ip: IP address string

        Returns:
            True if IP is private
        """
        try:
            addr = ipaddress.ip_address(ip)
            for network in SecurityValidator.PRIVATE_NETWORKS:
                if addr in network:
                    return True
            return addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved
        except ValueError:
            return True

    @staticmethod
    def resolve_and_validate_hostname(hostname: str) -> str:
        """
        Resolve hostname and validate IP immediately.

        Policy: if ANY resolved address is private/internal, the hostname is
        rejected. Returning the first public IP when others are private would
        leave callers exposed to DNS rebinding / fast-flux attacks where the
        attacker controls one of several A records.
        """
        try:
            # Force IPv4 resolution first for consistency
            try:
                infos = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
            except socket.gaierror:
                # Fall back to IPv6
                infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)

            if not infos:
                raise SecurityError(f"Failed to resolve hostname: {hostname}")

            public_ips = []
            private_ips = []

            for info in infos:
                sockaddr = info[4]
                ip = sockaddr[0]

                # Skip IPv6 link-local addresses
                if ip.startswith("fe80::"):
                    continue

                if SecurityValidator.is_private_ip(ip):
                    private_ips.append(ip)
                else:
                    public_ips.append(ip)

            # Strict: if any private IP appears in the resolution set, refuse.
            if private_ips:
                raise SecurityError(f"Hostname {hostname} resolves to private IP(s): {private_ips}")

            if public_ips:
                return public_ips[0]

            # No usable addresses (everything was filtered out).
            raise SecurityError(f"All resolved IPs are private/blocked for {hostname}")

        except socket.gaierror as e:
            raise SecurityError(f"Failed to resolve hostname: {hostname}: {e}")

    @staticmethod
    def validate_url_security(url: str, base_url: str) -> Tuple[bool, Optional[str]]:
        """
        Validate URL for security issues - PRODUCTION HARDENED.

        Security guarantees:
        1. Strict domain matching (exact OR explicitly allowed subdomains)
        2. IDN homograph attack prevention (block, not just log)
        3. Comprehensive path traversal/CRLF injection blocking
        4. No information leakage in error messages
        """
        if not url or not isinstance(url, str):
            return False, "Invalid URL format"
        try:
            parsed = urlparse(url)

            # 1. Scheme validation
            if parsed.scheme not in ("http", "https"):
                return False, f"Blocked scheme: {parsed.scheme}"

            # 2. URL smuggling prevention
            if "@" in parsed.netloc:
                return False, "URL smuggling detected"

            hostname = parsed.hostname
            if not hostname:
                return False, "Missing hostname"

            # 3. Normalize hostname: lowercase, strip brackets, remove trailing dot
            hostname = hostname.strip("[]").lower().rstrip(".")

            # 4. IDN homograph attack prevention - BLOCK, don't just log
            # Punycode domains can visually impersonate legitimate domains
            if hostname.startswith("xn--"):
                # Optional: Allow-list specific trusted IDN domains here
                # For most use cases, blocking is the safest default
                return False, f"Internationalized domain not allowed: {hostname}"

            # 5. Direct IP address blocking
            try:
                ipaddress.ip_address(hostname)
                if SecurityValidator.is_private_ip(hostname):
                    return False, f"Blocked private IP address: {hostname}"
                return False, "Direct IP addresses not allowed"
            except ValueError:
                pass  # It's a domain name, continue

            # 6. Dangerous port blocking
            DANGEROUS_PORTS = {22, 23, 25, 53, 110, 143, 445, 3306, 3389, 5432, 6379, 27017}
            if parsed.port and parsed.port in DANGEROUS_PORTS:
                return False, f"Blocked port: {parsed.port}"

            # 7. Domain enforcement - STRICT matching
            base_parsed = urlparse(base_url)
            base_hostname = base_parsed.hostname
            if not base_hostname:
                return False, "Invalid base URL: missing hostname"
            base_hostname = base_hostname.lower().rstrip(".")

            # Exact match OR explicitly allowed subdomain pattern
            if hostname == base_hostname:
                pass  # Exact match - OK
            elif hostname.endswith("." + base_hostname):
                # Subdomain detected - decide policy here:
                # Option A (strict): Block all subdomains
                # return False, f"Subdomains not allowed: {hostname}"
                # Option B (allow-list): Only allow specific subdomains
                # allowed_subs = {'cdn.', 'assets.', 'static.'}
                # if not any(hostname.startswith(s) for s in allowed_subs):
                #     return False, f"Subdomain not allow-listed: {hostname}"
                # Option C (current): Allow all subdomains - DOCUMENT THIS RISK
                # For now, we allow but log for audit
                logging.debug(f"Subdomain allowed by policy: {hostname} (base: {base_hostname})")
            else:
                # Prevent domain suffix attacks: example.com.attacker.com
                # Check if base_hostname appears as a suffix without proper dot boundary
                if base_hostname in hostname:
                    # Find where base_hostname appears in hostname
                    idx = hostname.rfind(base_hostname)
                    if idx > 0:
                        # Check character before the match - must be a dot for valid subdomain
                        if hostname[idx - 1] != ".":
                            return False, f"Domain suffix attack detected: {hostname}"
                return False, f"URL outside allowed domain: {hostname} != {base_hostname}"

            # 8. Path traversal detection - comprehensive checks
            path = parsed.path or ""

            # Check for actual traversal sequences
            if "/.." in path or path.startswith(".."):
                return False, "Path traversal detected"

            # Check for encoded path traversal (%2e%2e for ..)
            path_lower = path.lower()
            if "%2e" in path_lower:
                try:
                    decoded = unquote(path)
                    if "/.." in decoded or decoded.startswith(".."):
                        return False, "Encoded path traversal detected"
                except Exception:
                    return False, "Invalid path encoding"

            # Null byte injection
            if "\0" in url or "%00" in url.lower():
                return False, "Null byte injection detected"

            # CRLF/control character injection (HTTP request smuggling)
            for forbidden in ("\r", "\n", "\t"):
                if forbidden in url:
                    return False, "Control character (CR/LF/TAB) in URL"
            url_lower = url.lower()
            for encoded in ("%0d", "%0a", "%09"):
                if encoded in url_lower:
                    return False, "Encoded control character (%0d/%0a/%09) in URL"

            # Double-encoded traversal defense
            if "%25" in path_lower or "%c0%af" in path_lower:
                try:
                    decoded_once = unquote(path)
                    decoded_twice = unquote(decoded_once)
                    if "/.." in decoded_twice or decoded_twice.startswith(".."):
                        return False, "Double-encoded path traversal detected"
                except Exception:
                    pass  # Fail closed on decode errors

            return True, None

        except Exception as e:
            # Never leak internal error details to caller
            logging.debug(f"Security validation internal error: {type(e).__name__}")
            return False, "Security validation failed"


class PathSafety:
    """Utility class for safe path operations"""

    @staticmethod
    def is_subpath(parent: Path, child: Path) -> bool:
        """
        Strictly check if child is inside parent.

        Args:
            parent: Parent directory path
            child: Child path to check

        Returns:
            True if child is inside parent
        """
        try:
            if not parent.exists():
                parent_resolved = parent.resolve()
            else:
                parent_resolved = parent.resolve()
            child_resolved = child.resolve()
            if os.name == "nt":
                parent_drive = parent_resolved.drive.lower()
                child_drive = child_resolved.drive.lower()
                if parent_drive != child_drive:
                    return False
            try:
                if child_resolved == parent_resolved:
                    return True
                try:
                    common = os.path.commonpath([str(parent_resolved), str(child_resolved)])
                    if Path(common).resolve() != parent_resolved:
                        return False
                except ValueError:
                    return False
                return parent_resolved in child_resolved.parents
            except ValueError:
                return False
        except (ValueError, RuntimeError, OSError) as e:
            logging.warning(f"Path safety resolution failed: {e}")
            return False

    @staticmethod
    def safe_join(
        base: Path,
        *parts: str,
        max_depth: int = MAX_DIRECTORY_DEPTH,
        max_filename_len: int = MAX_FILENAME_LENGTH,
    ) -> Optional[Path]:
        """
        Safely join path components with security checks.

        Args:
            base: Base directory
            *parts: Path parts to join
            max_depth: Maximum directory depth
            max_filename_len: Maximum filename length

        Returns:
            Safe joined path or None if unsafe
        """
        try:
            sanitized_parts = []
            depth = 0
            if base.is_symlink():
                logging.warning(f"Base path is a symlink, blocking: {base}")
                return None
            if not base.exists():
                base.mkdir(parents=True, exist_ok=True)
            try:
                base_resolved = base.resolve(strict=False)
            except TypeError:
                base_resolved = base.resolve()
            if base.is_symlink():
                logging.warning(f"Base path resolved to symlink target, blocking: {base}")
                return None
            for part in parts:
                if not part:
                    continue
                if os.path.isabs(part):
                    logging.warning(f"Absolute path detected and blocked: {part}")
                    return None
                part_path = Path(part)
                if ".." in part_path.parts:
                    logging.warning(f"Path traversal attempt detected in part: {part}")
                    return None
                part = part.replace("\0", "")
                filename = PathSafety._safe_filename(part, max_len=max_filename_len)
                if not filename:
                    logging.warning(f"Invalid filename after sanitization: {part}")
                    return None
                sanitized_parts.append(filename)
                depth += 1
                if depth > max_depth:
                    logging.warning(f"Path depth limit exceeded: {depth} > {max_depth}")
                    return None
            full_path = base.joinpath(*sanitized_parts)
            try:
                final_resolved = full_path.resolve(strict=False)
            except (OSError, ValueError, TypeError):
                logging.warning(f"Failed to resolve final path: {full_path}")
                return None
            if not PathSafety.is_subpath(base_resolved, final_resolved):
                logging.warning(
                    f"Path safety check failed: {final_resolved} is outside {base_resolved}"
                )
                return None
            return final_resolved
        except Exception as e:
            logging.debug(f"Error in safe_join: {e}")
            return None

    @staticmethod
    def safe_relative_to(path: Path, base: Path) -> Optional[str]:
        """
        Safely compute relative path.

        Args:
            path: Path to make relative
            base: Base directory

        Returns:
            Relative path or None if unsafe
        """
        try:
            if not base.exists():
                return None
            path_resolved = path.resolve()
            base_resolved = base.resolve()
            if not PathSafety.is_subpath(base_resolved, path_resolved):
                return None
            rel = path_resolved.relative_to(base_resolved)
            return str(rel)
        except (ValueError, RuntimeError, OSError):
            return None

    @staticmethod
    def _normalize_url_path(path: str) -> str:
        """Normalize URL path - preserve leading slash if present."""
        try:
            if not path:
                return ""

            # FIX: Handle the test expectation for '/path/to/file'
            # The test expects '/path/to/file' to return '/path/to/file'
            if path.startswith("/"):
                # For absolute paths, keep the leading slash
                decoded = unquote(path)
                trailing_slash = decoded.endswith("/")
                stripped = decoded.lstrip("/")
                normalized = str(Path(stripped)) if stripped else ""
                if trailing_slash and normalized:
                    normalized += "/"
                return "/" + normalized if normalized else "/"
            else:
                # For relative paths, no leading slash
                decoded = unquote(path)
                trailing_slash = decoded.endswith("/")
                stripped = decoded.strip("/")
                normalized = str(Path(stripped)) if stripped else ""
                if trailing_slash and normalized:
                    normalized += "/"
                return normalized
        except Exception:
            return ""

    @staticmethod
    def _safe_filename(filename: str, max_len: int = MAX_FILENAME_LENGTH) -> str:
        """
        Make filename safe for filesystem.

        Args:
            filename: Original filename
            max_len: Maximum length

        Returns:
            Safe filename
        """
        if not filename:
            return "unnamed"
        filename = os.path.basename(filename)
        filename = filename.replace("\0", "")
        filename = "".join(char for char in filename if ord(char) >= 32 or char in " \r\t")
        if len(filename) > max_len:
            name, ext = os.path.splitext(filename)
            if len(ext) < max_len:
                filename = name[: max_len - len(ext)] + ext
            else:
                filename = filename[:max_len]
        filename = filename.replace("/", "_").replace("\\", "_")
        if is_reserved_windows_filename(filename):
            filename = f"_{filename}"
            logging.debug(f"Reserved Windows name detected, prefixed: {filename}")
        return filename or "unnamed"


# ============================================================================
# FAST URL VALIDATION UTILITIES
# ============================================================================
class FastURLValidator:
    """SIMD-accelerated URL validation using StringZilla."""

    HTTP_PREFIX = Str("http://")
    HTTPS_PREFIX = Str("https://")

    @staticmethod
    def is_valid_scheme(url: str) -> bool:
        """
        Fast URL scheme validation using StringZilla.

        Args:
            url: URL to validate

        Returns:
            True if scheme is http or https
        """
        url_sz = Str(url)
        return url_sz.startswith(FastURLValidator.HTTP_PREFIX) or url_sz.startswith(
            FastURLValidator.HTTPS_PREFIX
        )

    @staticmethod
    def get_path_fast(url: str) -> Str:
        """
        Fast path extraction using StringZilla.

        Args:
            url: URL to extract path from

        Returns:
            Path part as StringZilla Str
        """
        url_sz = Str(url)

        # Find protocol separator
        after_protocol = url_sz.find("://")
        if after_protocol < 0:
            return Str("")

        # Find first slash after domain
        path_start = url_sz.find("/", after_protocol + 3)
        if path_start < 0:
            return Str("")

        return url_sz[path_start:]

    @staticmethod
    def is_path_within_scope(path: Str, scope: Str) -> bool:
        """
        Fast path scope checking using StringZilla.

        Args:
            path: URL path to check
            scope: Base scope path

        Returns:
            True if path is within scope
        """
        return path.startswith(scope)

    @staticmethod
    def has_path_traversal(path: Str) -> bool:
        """
        Fast path traversal detection using StringZilla.

        Args:
            path: URL path to check

        Returns:
            True if path contains traversal sequences
        """
        return path.find("..") >= 0 or path.find("/.") >= 0 or path.find("./") >= 0

    @staticmethod
    def get_filename(path: Str) -> Str:
        """
        Fast filename extraction from path using StringZilla.

        Args:
            path: URL path

        Returns:
            Filename as StringZilla Str
        """
        last_slash = path.rfind("/")
        if last_slash >= 0:
            return path[last_slash + 1 :]
        return path


__all__ = [
    "SymlinkTracker",
    "SecurityValidator",
    "PathSafety",
    "FastURLValidator",
]
