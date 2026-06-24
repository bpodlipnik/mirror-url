"""Security-focused tests (SSRF, path traversal, URL scope).

Exercises the real API migrated into ``mirror_url.security``. These encode the
invariants the monolith already enforces.
"""

from __future__ import annotations

from pathlib import Path

import pytest

security = pytest.importorskip("mirror_url.security")

SecurityValidator = security.SecurityValidator
PathSafety = security.PathSafety
FastURLValidator = security.FastURLValidator
SymlinkTracker = security.SymlinkTracker


# --- SecurityValidator: private-IP / SSRF -----------------------------------
@pytest.mark.parametrize("ip", ["10.0.0.1", "127.0.0.1", "192.168.1.1", "169.254.1.1", "::1"])
def test_private_ips_detected(ip):
    assert SecurityValidator.is_private_ip(ip) is True


@pytest.mark.parametrize("ip", ["8.8.8.8", "1.1.1.1"])
def test_public_ips_allowed(ip):
    assert SecurityValidator.is_private_ip(ip) is False


def test_invalid_ip_fails_closed():
    # Unparseable input is treated as private (fail closed).
    assert SecurityValidator.is_private_ip("not-an-ip") is True


# --- SecurityValidator.validate_url_security --------------------------------
BASE = "https://example.com/files/"


def test_same_domain_allowed():
    ok, _ = SecurityValidator.validate_url_security("https://example.com/files/a.txt", BASE)
    assert ok is True


def test_offdomain_blocked():
    ok, reason = SecurityValidator.validate_url_security("https://evil.com/x", BASE)
    assert ok is False and reason


def test_domain_suffix_attack_blocked():
    ok, reason = SecurityValidator.validate_url_security("https://example.com.attacker.com/x", BASE)
    assert ok is False
    assert "suffix attack" in reason or "outside allowed domain" in reason


def test_non_http_scheme_blocked():
    ok, _ = SecurityValidator.validate_url_security("file:///etc/passwd", BASE)
    assert ok is False


def test_url_smuggling_blocked():
    ok, _ = SecurityValidator.validate_url_security("https://example.com@evil.com/x", BASE)
    assert ok is False


def test_path_traversal_blocked():
    ok, _ = SecurityValidator.validate_url_security("https://example.com/files/../../etc", BASE)
    assert ok is False


# --- PathSafety -------------------------------------------------------------
def test_safe_join_blocks_traversal(tmp_path: Path):
    assert PathSafety.safe_join(tmp_path, "..", "..", "etc", "passwd") is None


def test_safe_join_blocks_absolute(tmp_path: Path):
    assert PathSafety.safe_join(tmp_path, "/etc/passwd") is None


def test_safe_join_in_scope(tmp_path: Path):
    result = PathSafety.safe_join(tmp_path, "sub", "file.txt")
    assert result is not None
    assert str(result).startswith(str(tmp_path.resolve()))


def test_is_subpath(tmp_path: Path):
    child = tmp_path / "a" / "b"
    assert PathSafety.is_subpath(tmp_path, child) is True
    assert PathSafety.is_subpath(child, tmp_path) is False


def test_safe_filename_strips_separators():
    assert "/" not in PathSafety._safe_filename("a/b/c.txt")


def test_safe_filename_reserved_windows():
    assert PathSafety._safe_filename("CON").startswith("_")


# --- FastURLValidator -------------------------------------------------------
def test_fast_scheme_validation():
    assert FastURLValidator.is_valid_scheme("https://x/") is True
    assert FastURLValidator.is_valid_scheme("ftp://x/") is False


def test_fast_path_traversal_detection():
    assert FastURLValidator.has_path_traversal(security.Str("/a/../b")) is True
    assert FastURLValidator.has_path_traversal(security.Str("/a/b")) is False


# --- SymlinkTracker ---------------------------------------------------------
def test_symlink_loop_detected():
    t = SymlinkTracker()
    ok, _ = t.can_follow("s1", "d1", current_depth=1)
    assert ok is True
    t.record_follow("s1", "d1", depth=1)
    ok, reason = t.can_follow("s1", "d1", current_depth=2)
    assert ok is False and "loop" in reason.lower()
