"""Filesystem-aware filename preflight and original-name preservation."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from mirror_url import MirrorURL
from mirror_url.exceptions import PathTraversalError
from mirror_url.filename_mapping import FilenameMap, _stat_identity, case_sensitive
from test_http_mirror_workflows import config as config
from test_http_mirror_workflows import remote as remote


@pytest.fixture
def case_directory(tmp_path):
    """An optional real APFS mount supplements the native CI filesystem."""
    mount = os.environ.get("MIRROR_URL_CASE_TEST_VOLUME")
    if mount:
        with tempfile.TemporaryDirectory(dir=mount) as directory:
            yield Path(directory)
    else:
        yield tmp_path


def test_probe_detects_actual_filename_behavior_without_leftovers(tmp_path):
    original = tmp_path / "Existing.txt"
    original.write_bytes(b"keep original")
    expected = not (tmp_path / "existing.txt").exists()
    assert case_sensitive(tmp_path) == expected
    assert {path.name for path in tmp_path.iterdir()} == {"Existing.txt"}
    assert original.read_bytes() == b"keep original"
    assert case_sensitive(tmp_path / "not-created/child") == expected
    assert not (tmp_path / "not-created").exists()


def test_probe_failure_preserves_existing_files(tmp_path, monkeypatch):
    original = tmp_path / "keep.txt"
    original.write_bytes(b"keep")

    def denied(**kwargs):
        raise PermissionError("read-only destination")

    monkeypatch.setattr("mirror_url.filename_mapping.tempfile.mkstemp", denied)
    with pytest.raises(ValueError, match="Cannot determine"):
        case_sensitive(tmp_path)
    assert original.read_bytes() == b"keep"
    assert list(tmp_path.iterdir()) == [original]


def test_probe_fails_closed_when_no_ancestor_is_accessible(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "exists", lambda _: False)
    with pytest.raises(ValueError, match="No existing filename parent"):
        case_sensitive(tmp_path / "missing")


@pytest.mark.parametrize("replacement", ["file", "hardlink", "symlink", "same_inode", "metadata"])
def test_probe_never_deletes_replaced_or_shared_entries(tmp_path, monkeypatch, replacement):
    original = tempfile.mkstemp
    original_close = os.close
    probe_descriptor = None
    outside = tmp_path / "user-data"
    outside.write_bytes(b"preserve user data")
    created = []

    def changed(**kwargs):
        nonlocal probe_descriptor
        fd, name = original(**kwargs)
        probe_descriptor = fd
        created.append(Path(name))
        return fd, name

    def replace_closed_probe(descriptor):
        nonlocal probe_descriptor
        original_close(descriptor)
        if descriptor != probe_descriptor:
            return
        probe_descriptor = None
        path = created[0]
        # Windows forbids unlinking an open file. Replace after the production
        # probe has captured its original identity and closed its handle.
        if replacement == "hardlink":
            os.link(path, tmp_path / "second-link")
        elif replacement == "same_inode":
            path.write_bytes(b"replacement must survive")
        elif replacement == "metadata":
            info = path.stat()
            os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns - 1_000_000_000))
        elif replacement == "symlink":
            path.unlink()
            path.symlink_to(outside)
        else:
            # Allocate the replacement while the original inode still exists.
            new_file = tmp_path / "replacement"
            new_file.write_bytes(b"replacement must survive")
            assert new_file.stat().st_ino != path.stat().st_ino
            os.replace(new_file, path)

    monkeypatch.setattr("mirror_url.filename_mapping.tempfile.mkstemp", changed)
    monkeypatch.setattr("mirror_url.filename_mapping.os.close", replace_closed_probe)
    with pytest.raises(ValueError, match="preserving unexpected entry"):
        case_sensitive(tmp_path)
    assert created[0].exists()
    assert outside.read_bytes() == b"preserve user data"
    if replacement in {"file", "same_inode"}:
        assert created[0].read_bytes() == b"replacement must survive"
    if replacement == "symlink":
        assert created[0].is_symlink()
    if replacement == "hardlink":
        assert created[0].stat().st_nlink == 2


def test_probe_refuses_symlink_parent_or_file_parent(tmp_path):
    actual = tmp_path / "real"
    actual.mkdir()
    linked = tmp_path / "link"
    linked.symlink_to(actual, target_is_directory=True)
    with pytest.raises(PathTraversalError):
        case_sensitive(linked)
    file = tmp_path / "file"
    file.write_bytes(b"keep")
    with pytest.raises(ValueError, match="not a directory"):
        case_sensitive(file)
    assert file.read_bytes() == b"keep"


@pytest.mark.parametrize(
    "names",
    [
        ("Deep_Field_v3.pro", "deep_field_v3.pro"),
        ("DAILY/first.pro", "daily/second.pro"),
        ("FILE", "file/child.pro"),
    ],
)
def test_case_insensitive_preflight_rejects_file_and_directory_aliases(
    tmp_path, monkeypatch, names
):
    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", lambda _: False)
    mapping = FilenameMap(tmp_path)
    mapping.key(tmp_path / names[0])
    with pytest.raises(ValueError, match="case-insensitive"):
        mapping.key(tmp_path / names[1])
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "names",
    [
        ("Deep_Field_v3.pro", "deep_field_v3.pro"),
        ("DAILY/first.pro", "daily/second.pro"),
        ("FILE", "file/child.pro"),
    ],
)
def test_case_sensitive_preflight_accepts_distinct_original_names(case_directory, names):
    if not case_sensitive(case_directory):
        pytest.skip("requires a case-sensitive destination")
    mapping = FilenameMap(case_directory)
    first, second = (case_directory / name for name in names)
    assert mapping.key(first) != mapping.key(second)
    assert not list(case_directory.iterdir())


@pytest.mark.parametrize(
    "names", [("dir", "dir/child"), ("dir/child", "dir"), ("가/one", "가/two")]
)
def test_exact_file_directory_and_unicode_aliases_remain_rejected(tmp_path, names):
    mapping = FilenameMap(tmp_path)
    mapping.key(tmp_path / names[0])
    with pytest.raises(ValueError, match="same local filename"):
        mapping.key(tmp_path / names[1])


def test_existing_case_alias_is_preserved(tmp_path):
    old = tmp_path / "Deep_Field_v3.pro"
    old.write_bytes(b"different original")
    if not (tmp_path / "deep_field_v3.pro").exists():
        pytest.skip("requires case-insensitive filesystem")
    with pytest.raises(ValueError, match="existing local filename"):
        FilenameMap(tmp_path).key(tmp_path / "deep_field_v3.pro")
    assert old.read_bytes() == b"different original"


def test_existing_alias_lookup_cannot_bypass_preflight(tmp_path, monkeypatch):
    """Exercise an insensitive lookup independently of the CI volume format."""
    original = tmp_path / "Deep_Field_v3.pro"
    original.write_bytes(b"keep existing original")
    requested = tmp_path / "deep_field_v3.pro"
    exists = Path.exists
    monkeypatch.setattr(Path, "exists", lambda path: path == requested or exists(path))
    with pytest.raises(ValueError, match="existing local filename"):
        FilenameMap(tmp_path).key(requested)
    assert original.read_bytes() == b"keep existing original"


def test_case_behavior_is_checked_in_each_actual_parent(case_directory, monkeypatch):
    if not case_sensitive(case_directory):
        pytest.skip("requires distinct case-sensitive parent directories")
    upper, lower = case_directory / "DAILY", case_directory / "daily"
    upper.mkdir()
    lower.mkdir()
    (upper / "existing.pro").write_bytes(b"upper only")
    mapping = FilenameMap(case_directory)
    assert mapping._existing_names(upper) == {"existing.pro"}
    assert mapping._existing_names(lower) == set()
    observed = []

    def per_directory(parent):
        observed.append(parent)
        return parent != lower

    monkeypatch.setattr("mirror_url.filename_mapping.case_sensitive", per_directory)
    mapping.key(upper / "Name.pro")
    mapping.key(upper / "name.pro")
    mapping.key(lower / "Name.pro")
    with pytest.raises(ValueError, match="case-insensitive"):
        mapping.key(lower / "name.pro")
    assert observed == [upper, case_directory, lower]
    assert (upper / "existing.pro").read_bytes() == b"upper only"


def test_unreadable_existing_parent_blocks_preflight(tmp_path, monkeypatch):
    original = tmp_path / "keep.txt"
    original.write_bytes(b"keep")

    def denied(*args):
        raise PermissionError("cannot inspect local names")

    monkeypatch.setattr("mirror_url.filename_mapping.os.scandir", denied)
    with pytest.raises(ValueError, match="Cannot inspect"):
        FilenameMap(tmp_path).key(tmp_path / "new.txt")
    assert original.read_bytes() == b"keep"


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_real_http_preserves_both_nasa_names_and_receipts(remote, config, case_directory, mode):
    if not case_sensitive(case_directory):
        pytest.skip("requires a case-sensitive destination")
    remote.files = {
        "secchi/idl/daily/Deep_Field_v3.pro": b"upper original" * 160000,
        "secchi/idl/daily/deep_field_v3.pro": b"lower original" * 160003,
        "DAILY/one.pro": b"upper directory original",
        "daily/two.pro": b"lower directory original",
    }
    config.dest_path = case_directory / "mirror"
    config.verify_content = True
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    config.min_chunk_size_mb = 1
    config.max_chunks_per_file = 2
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        paths = [mirror.target_dir / name for name in remote.files]
        assert not paths[0].samefile(paths[1])
        for name, payload in remote.files.items():
            path = mirror.target_dir / name
            assert path.read_bytes() == payload
            assert mirror.cache_manager.get_file_metadata(path)["sha256"] == (
                hashlib.sha256(payload).hexdigest()
            )
        assert not (mirror.target_dir / "DAILY").samefile(mirror.target_dir / "daily")
        if mode != "sequential":
            for name in list(remote.files)[:2]:
                ranges = [
                    headers["Range"]
                    for method, path, headers in remote.requests
                    if method == "GET" and path == name and "Range" in headers
                ]
                assert len(ranges) == 2
        gets = sum(method == "GET" and path in remote.files for method, path, _ in remote.requests)
        assert mirror.sync()
        assert (
            sum(method == "GET" and path in remote.files for method, path, _ in remote.requests)
            == gets
        )
        assert not any(mirror.target_dir.rglob(".mirror-url-case-*"))
    config.verify_content = False
    config.missing_files = True
    with MirrorURL(config) as mirror:
        assert mirror.sync()
        for name, payload in remote.files.items():
            assert (mirror.target_dir / name).read_bytes() == payload


@pytest.mark.integration
@pytest.mark.parametrize("mode", ["sequential", "parallel", "streaming"])
def test_real_http_case_insensitive_failure_preserves_suffix_and_existing_data(
    remote, config, tmp_path, mode
):
    if case_sensitive(tmp_path):
        pytest.skip("requires a case-insensitive destination")
    remote.files = {
        "Deep_Field_v3.pro": b"upper",
        "deep_field_v3.pro": b"lower",
        "unrelated.txt": b"remote",
    }
    config.sequential_downloads = mode == "sequential"
    config.parallel_downloads = mode == "parallel"
    config.streaming_parallel = mode == "streaming"
    with MirrorURL(config) as mirror:
        keep = mirror.target_dir / "old.txt"
        keep.write_bytes(b"preserve")
        assert not mirror.sync()
        assert keep.read_bytes() == b"preserve"
        assert not any(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )
        assert not (mirror.target_dir / "unrelated.txt").exists()
        assert not any(mirror.target_dir.rglob(".mirror-url-case-*"))


def test_probe_classifies_alias_by_inode(tmp_path, monkeypatch):
    original_create = tempfile.mkstemp
    original_stat = Path.lstat
    created = []

    def create(**kwargs):
        result = original_create(**kwargs)
        created.append(Path(result[1]))
        return result

    def alias_stat(path):
        if created and path == created[0].with_name(created[0].name.swapcase()):
            return original_stat(created[0])
        return original_stat(path)

    monkeypatch.setattr("mirror_url.filename_mapping.tempfile.mkstemp", create)
    monkeypatch.setattr(Path, "lstat", alias_stat)
    assert not case_sensitive(tmp_path)
    assert not list(tmp_path.iterdir())


def test_probe_preserves_unrelated_alias(case_directory, monkeypatch):
    if not case_sensitive(case_directory):
        pytest.skip("requires case-sensitive destination for two unrelated probe spellings")
    original = tempfile.mkstemp
    aliases = []

    def create(**kwargs):
        fd, name = original(**kwargs)
        alias = Path(name).with_name(Path(name).name.swapcase())
        alias.write_bytes(b"unrelated file must survive")
        aliases.append(alias)
        return fd, name

    monkeypatch.setattr("mirror_url.filename_mapping.tempfile.mkstemp", create)
    with pytest.raises(ValueError, match="unrelated filename"):
        case_sensitive(case_directory)
    assert list(case_directory.iterdir()) == aliases
    assert aliases[0].read_bytes() == b"unrelated file must survive"


def test_multiple_case_variants_use_same_confirmed_parent(case_directory):
    if not case_sensitive(case_directory):
        pytest.skip("requires case-sensitive destination")
    mapping = FilenameMap(case_directory)
    keys = {mapping.key(case_directory / name) for name in ("Name.pro", "name.pro", "NAME.pro")}
    assert len(keys) == 3
    assert not list(case_directory.iterdir())


@pytest.mark.integration
def test_existing_local_case_alias_never_overwritten_or_skipped(remote, config, tmp_path):
    if case_sensitive(tmp_path):
        pytest.skip("requires a case-insensitive destination")
    remote.files = {"deep_field_v3.pro": b"different remote data"}
    config.missing_files = True
    with MirrorURL(config) as mirror:
        original = mirror.target_dir / "Deep_Field_v3.pro"
        original.write_bytes(b"keep existing original")
        assert not mirror.sync()
        assert original.read_bytes() == b"keep existing original"
        assert not any(
            method == "GET" and path in remote.files for method, path, _ in remote.requests
        )


@pytest.mark.parametrize(
    "platform,with_birthtime",
    [("linux", False), ("linux", True), ("win32", False), ("win32", True)],
)
def test_stat_identity_respects_windows_path_and_handle_time_semantics(
    monkeypatch, platform, with_birthtime
):
    # Python 3.12's Windows path stat keeps ctime=creation time; handle stat
    # reports ctime=change time. This pair describes one unchanged owned file.
    common = {"st_dev": 1, "st_ino": 2, "st_size": 3, "st_mtime_ns": 4, "st_ctime_ns": 5}
    path = SimpleNamespace(**common)
    handle = SimpleNamespace(**common)
    if with_birthtime:
        path.st_birthtime_ns = handle.st_birthtime_ns = 5
        handle.st_ctime_ns = 6
    monkeypatch.setattr("mirror_url.filename_mapping.sys", SimpleNamespace(platform=platform))
    equal = _stat_identity(path) == _stat_identity(handle)
    assert equal == (platform == "win32" or not with_birthtime)
    # Normalizing the timestamp namespace cannot clear a changed file identity.
    for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns"):
        changed = SimpleNamespace(**vars(path))
        setattr(changed, field, getattr(changed, field) + 1)
        assert _stat_identity(path) != _stat_identity(changed)
