"""Security decisions, DNS boundaries and filesystem preservation contracts."""

from __future__ import annotations

import socket
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace

import pytest

from mirror_url import security
from mirror_url.exceptions import SecurityError
from mirror_url.security import FastURLValidator, PathSafety, SecurityValidator, SymlinkTracker


@pytest.mark.parametrize(
    "addresses", [["2606:4700:4700::1111", "fe80::1"], ["fe80::1", "2606:4700:4700::1111"]]
)
def test_every_dns_answer_must_be_public(monkeypatch, addresses):
    def resolve(hostname, port, family, kind):
        if family == socket.AF_INET:
            raise socket.gaierror("IPv6-only hostname")
        assert family == socket.AF_UNSPEC
        return [(socket.AF_INET6, kind, 0, "", (ip, 0, 0, 0)) for ip in addresses]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    with pytest.raises(SecurityError, match="private"):
        SecurityValidator.resolve_and_validate_hostname("example.com")


@pytest.mark.parametrize("path", ["%2ename", "%25name"])
def test_url_decode_failure_rejects_request(monkeypatch, path):
    def fail_decode(value):
        raise ValueError("injected decoder failure")

    monkeypatch.setattr(security, "unquote", fail_decode)
    ok, reason = SecurityValidator.validate_url_security(
        "https://example.com/" + path, "https://example.com/"
    )
    assert not ok and "encoding" in reason.lower()


@pytest.mark.parametrize("addresses", [["8.8.8.8"], ["2606:4700:4700::1111"]])
def test_public_resolution_and_ipv6_fallback(monkeypatch, addresses):
    families = []

    def resolve(hostname, port, family, kind):
        families.append(family)
        if ":" in addresses[0] and family == socket.AF_INET:
            raise socket.gaierror("no IPv4")
        return [(family, kind, 0, "", (ip, 0)) for ip in addresses]

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    assert SecurityValidator.resolve_and_validate_hostname("example.com") == addresses[0]
    assert families == (
        [socket.AF_INET, socket.AF_UNSPEC] if ":" in addresses[0] else [socket.AF_INET]
    )


@pytest.mark.parametrize("answers", [[], ["127.0.0.1"], ["8.8.8.8", "10.0.0.1"], ["bad-ip"]])
def test_unusable_dns_answers_reject_connection(monkeypatch, answers):
    monkeypatch.setattr(
        socket, "getaddrinfo", lambda *args: [(0, 0, 0, "", (ip, 0)) for ip in answers]
    )
    with pytest.raises(SecurityError):
        SecurityValidator.resolve_and_validate_hostname("example.com")


def test_dns_failure_is_classified(monkeypatch):
    def resolve(*args):
        raise socket.gaierror("unavailable")

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    with pytest.raises(SecurityError, match="Failed to resolve") as exc:
        SecurityValidator.resolve_and_validate_hostname("example.com")
    assert isinstance(exc.value.__cause__, socket.gaierror)


@pytest.mark.parametrize(
    "url,base,reason",
    [
        (None, "https://example.com/", "format"),
        (12, "https://example.com/", "format"),
        ("", "https://example.com/", "format"),
        ("ftp://example.com/a", "https://example.com/", "scheme"),
        ("https://user@example.com/a", "https://example.com/", "smuggling"),
        ("https:///a", "https://example.com/", "hostname"),
        ("https://xn--example.com/a", "https://example.com/", "Internationalized"),
        ("https://127.0.0.1/a", "https://example.com/", "private IP"),
        ("https://8.8.8.8/a", "https://example.com/", "Direct IP"),
        ("https://example.com:22/a", "https://example.com/", "port"),
        ("https://example.com/a", "https:///", "base URL"),
        ("https://badexample.com/a", "https://example.com/", "suffix attack"),
        ("https://example.com.evil/a", "https://example.com/", "outside"),
        ("https://a.example.com.evil/a", "https://example.com/", "outside"),
        ("https://evil.com/a", "https://example.com/", "outside"),
        ("https://example.com/a/../b", "https://example.com/", "traversal"),
        ("https://example.com/%2e%2e/b", "https://example.com/", "traversal"),
        ("https://example.com/%252e%252e/b", "https://example.com/", "traversal"),
        ("https://example.com/a%00", "https://example.com/", "Null"),
        ("https://example.com/a\0", "https://example.com/", "Null"),
        ("https://example.com:bad/a", "https://example.com/", "validation failed"),
        ("https://[broken/a", "https://example.com/", "validation failed"),
    ],
)
def test_url_security_rejects_unsafe_inputs(url, base, reason):
    ok, message = SecurityValidator.validate_url_security(url, base)
    assert not ok and reason in message


@pytest.mark.parametrize("control", ["\r", "\n", "\t", "%0d", "%0A", "%09"])
def test_control_characters_are_rejected(control):
    assert not SecurityValidator.validate_url_security(
        "https://example.com/a" + control, "https://example.com/"
    )[0]


@pytest.mark.parametrize(
    "url",
    [
        "https://EXAMPLE.com./a",
        "https://cdn.example.com/a",
        "https://example.com/%2ename",
        "https://example.com/%25name",
        "https://example.com/%c0%af",
    ],
)
def test_valid_domain_policy_and_literal_encoded_names(url):
    assert SecurityValidator.validate_url_security(url, "https://example.com/") == (True, None)


def test_symlink_tracker_limits_chain_pruning_and_statistics(monkeypatch):
    tracker = SymlinkTracker(max_depth=2, max_per_dir=1, bomb_threshold=3)
    assert not tracker.can_follow("deep", "dir", 3)[0]
    tracker.symlink_chain.append("cycle")
    assert "cycle" in tracker.can_follow("cycle", "dir", 1)[1]
    tracker.record_follow("one", "dir", 1)
    assert "directory" in tracker.can_follow("two", "dir", 1)[1]
    tracker.record_skip("two")
    assert tracker.is_in_chain("one")
    tracker.clear_chain()
    assert not tracker.is_in_chain("one")
    tracker.record_follow("two", "other", 1)
    tracker.record_follow("three", "third", 1)
    assert "bomb" in tracker.can_follow("four", "fourth", 1)[1]
    assert tracker.get_stats() == {
        "total_followed": 3,
        "total_skipped": 1,
        "unique_symlinks": 3,
        "directories_with_symlinks": 3,
        "current_chain_length": 2,
    }
    monkeypatch.setattr(security, "SYMLINK_VISIT_CACHE_SIZE", 5)
    tracker = SymlinkTracker()
    for i in range(6):
        tracker.record_follow(str(i), "dir", 1)
    assert list(tracker.visited_symlinks) == ["1", "2", "3", "4", "5"]


@pytest.mark.parametrize(
    "url,path", [("plain", ""), ("https://example.com", ""), ("https://example.com/a/b", "/a/b")]
)
def test_fast_path_extraction(url, path):
    assert str(FastURLValidator.get_path_fast(url)) == path


@pytest.mark.parametrize("path,filename", [("/a/b", "b"), ("name", "name")])
def test_fast_filename_and_scope(path, filename):
    assert str(FastURLValidator.get_filename(security.Str(path))) == filename
    assert FastURLValidator.is_path_within_scope(
        security.Str(path), security.Str("/a/")
    ) == path.startswith("/a/")


@pytest.mark.parametrize(
    "value,limit,expected",
    [
        ("", 8, "unnamed"),
        ("abcdefgh.txt", 8, "abcd.txt"),
        ("a.longextension", 4, "a.lo"),
        ("\x01", 8, "unnamed"),
        ("CON.txt", 20, "_CON.txt"),
    ],
)
def test_safe_filename_limits_and_reserved_names(value, limit, expected):
    assert PathSafety._safe_filename(value, max_len=limit) == expected


def test_safe_join_depth_limit_preserves_existing_files(tmp_path):
    keep = tmp_path / "keep"
    keep.write_bytes(b"original")
    assert PathSafety.safe_join(tmp_path, "sub", "keep", max_depth=1) is None
    assert keep.read_bytes() == b"original"
    assert not (tmp_path / "sub").exists()


def test_safe_relative_path_and_unavailable_paths(tmp_path, monkeypatch):
    assert PathSafety.safe_relative_to(tmp_path / "a", tmp_path / "absent") is None
    assert PathSafety.safe_relative_to(tmp_path / "a", tmp_path) == "a"
    assert PathSafety.safe_relative_to(tmp_path.parent, tmp_path) is None
    original = Path.resolve

    def fail(path, *args, **kwargs):
        if path.name == "broken":
            raise OSError("injected filesystem failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", fail)
    assert not PathSafety.is_subpath(tmp_path, tmp_path / "broken")
    assert PathSafety.safe_relative_to(tmp_path / "broken", tmp_path) is None
    assert PathSafety.safe_join(tmp_path, "broken") is None


@pytest.mark.parametrize(
    "parent,child,within",
    [
        ("C:/root", "c:/ROOT/file", True),
        ("C:/root", "D:/root/file", False),
        ("C:/root", "C:/rooted/file", False),
    ],
)
def test_windows_ancestry_semantics(parent, child, within):
    parent_path, child_path = PureWindowsPath(parent), PureWindowsPath(child)
    # PureWindowsPath tests Windows drive/case semantics on every platform.
    assert (
        PathSafety.is_subpath(
            SimpleNamespace(resolve=lambda: parent_path),
            SimpleNamespace(resolve=lambda: child_path),
        )
        == within
    )


def test_selected_root_and_leaf_links_are_rejected(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "keep"
    victim.write_bytes(b"original")
    link = tmp_path / "linked"
    link.symlink_to(outside, target_is_directory=True)
    assert PathSafety.safe_join(link, "keep") is None
    assert PathSafety.safe_join(tmp_path, "linked", "keep") is None
    assert victim.read_bytes() == b"original"


@pytest.mark.parametrize("phase", ["base", "leaf"])
def test_resolution_race_cannot_admit_outside_destination(tmp_path, monkeypatch, phase):
    base = tmp_path / "base"
    base.mkdir()
    (base / "sub").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    victim = outside / "keep"
    victim.write_bytes(b"original")
    original = Path.resolve
    switched = False

    def resolve(path, *args, **kwargs):
        nonlocal switched
        if not switched and path == (base if phase == "base" else base / "sub" / "keep"):
            switched = True
            if phase == "base":
                result = original(path, *args, **kwargs)
                (base / "sub").rmdir()
                base.rmdir()
                base.symlink_to(outside, target_is_directory=True)
                return result
            (base / "sub").rmdir()
            (base / "sub").symlink_to(outside, target_is_directory=True)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    assert PathSafety.safe_join(base, "sub", "keep") is None
    assert switched and victim.read_bytes() == b"original"


def test_creation_failure_and_invalid_component_fail_closed(tmp_path, monkeypatch):
    base = tmp_path / "new"
    original = Path.mkdir

    def mkdir(path, *args, **kwargs):
        if path == base:
            raise PermissionError("injected mkdir failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    assert PathSafety.safe_join(base, "keep") is None
    assert not base.exists()
    assert PathSafety.safe_join(tmp_path, None) is None


@pytest.mark.parametrize("alias", ["var", "tmp", "etc"])
@pytest.mark.parametrize(
    "platform,target,allowed",
    [
        ("darwin", "private/{alias}", True),
        ("darwin", "outside", False),
        ("linux", "private/{alias}", False),
    ],
)
def test_fixed_system_alias_policy_on_every_runner(monkeypatch, alias, platform, target, allowed):
    from pathlib import PurePosixPath

    class AliasPath(PurePosixPath):
        def is_symlink(self):
            return str(self) == "/" + alias

        def resolve(self):
            return PurePosixPath("/private") / str(self).lstrip("/")

    monkeypatch.setattr(security, "Path", AliasPath)
    monkeypatch.setattr(security, "sys", SimpleNamespace(platform=platform))
    monkeypatch.setattr(security.os, "readlink", lambda path: target.format(alias=alias))
    path = AliasPath("/" + alias + "/mirror")
    if allowed:
        assert (
            PathSafety._resolve_destination_root(path)
            == PurePosixPath("/private") / alias / "mirror"
        )
    else:
        with pytest.raises(security.PathTraversalError, match="symlink"):
            PathSafety._resolve_destination_root(path)
