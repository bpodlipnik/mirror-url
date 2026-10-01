"""Persistent cache recovery, atomic publication, expiry, and invalidation."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from mirror_url import MirrorConfig
from mirror_url.cache import CacheManager
from mirror_url.constants import CACHE_SCHEMA_VERSION
from mirror_url.enums import MemoryPressure
from mirror_url.metrics import MetricsCollector

BASE = "https://example.com/root/"


@pytest.fixture
def cache(tmp_path):
    config = MirrorConfig(base_url=BASE, dest_path=tmp_path / "mirror", log_path=tmp_path / "logs")
    return CacheManager(tmp_path / "cache" / "listing.json", config, MetricsCollector())


def write_cache(path, *, age=0, version=CACHE_SCHEMA_VERSION, files=None):
    data = {
        "_meta": {
            "version": version,
            "last_full_run": (datetime.now() - timedelta(days=age)).isoformat(),
            "file_count": 1,
            "dir_signatures": {BASE: "signature"},
        },
        BASE: "signature",
    }
    if files is not None:
        data["_files"] = files
    path.write_text(json.dumps(data))
    return data


def test_save_load_roundtrip_keeps_file_identity_and_redacts_credentials(cache, tmp_path):
    file = tmp_path / "a.txt"
    file.write_bytes(b"alpha")
    cache.config.base_url = "https://user:password@example.com/root/?token=secret"
    cache.dir_signatures[BASE] = "signature"
    cache.save_file_metadata(file, '"v1"', 1234, 5)
    assert cache.save({BASE: "signature"}, 1)
    raw = cache.cache_file.read_text()
    assert "password" not in raw and "secret" not in raw
    restored = CacheManager(cache.cache_file, cache.config, MetricsCollector())
    assert restored.load() == (True, {BASE: "signature"})
    metadata = restored.get_file_metadata(file)
    assert metadata["etag"] == '"v1"'
    assert metadata["size"] == 5
    assert metadata["local_mtime_ns"] == file.stat().st_mtime_ns
    assert restored.dir_signatures == {BASE: "signature"}
    assert not cache.cache_file.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize("option", ["no_cache", "refresh_cache"])
def test_disabled_or_forced_refresh_rejects_existing_cache(cache, option):
    write_cache(cache.cache_file)
    original = cache.cache_file.read_bytes()
    setattr(cache.config, option, True)
    assert cache.load() == (False, None)
    assert cache.cache_file.read_bytes() == original
    if option == "no_cache":
        assert cache.save({}, 0) is False
        assert cache.refresh_timestamp() is False


@pytest.mark.parametrize("age,version", [(30, CACHE_SCHEMA_VERSION), (0, 999)])
def test_stale_or_incompatible_cache_is_rebuilt_without_deleting_it(cache, age, version):
    write_cache(cache.cache_file, age=age, version=version)
    original = cache.cache_file.read_bytes()
    assert cache.load() == (False, None)
    assert cache.cache_file.read_bytes() == original


def test_legacy_cache_without_metadata_remains_readable(cache):
    cache.cache_file.write_text(json.dumps({BASE: "old-signature"}))
    assert cache.load() == (True, {BASE: "old-signature"})


def test_load_prunes_oversized_metadata(cache, monkeypatch):
    monkeypatch.setattr("mirror_url.cache.MAX_CACHE_METADATA_ENTRIES", 2)
    entries = {f"/file-{i}": {"size": i} for i in range(5)}
    write_cache(cache.cache_file, files=entries)
    assert cache.load()[0]
    assert list(cache.file_metadata_cache) == ["/file-3", "/file-4"]


@pytest.mark.parametrize("depth,attempts", [(4, 0), (0, 3)])
def test_recovery_limits_leave_cache_untouched(cache, depth, attempts):
    write_cache(cache.cache_file)
    original = cache.cache_file.read_bytes()
    assert cache.load(depth, attempts) == (False, None)
    assert cache.cache_file.read_bytes() == original


def test_corrupted_cache_restores_oldest_valid_backup(cache):
    cache.cache_file.write_text("{broken")
    invalid = cache.cache_file.with_name("listing.json.corrupted.1")
    invalid.write_text("not json")
    valid = cache.cache_file.with_name("listing.json.corrupted.2")
    write_cache(valid)
    os.utime(invalid, (1, 1))
    os.utime(valid, (2, 2))
    assert cache.load() == (True, {BASE: "signature"})
    assert not valid.exists()
    assert invalid.read_text() == "not json"
    assert any(
        path.read_text() == "{broken" for path in cache.cache_file.parent.glob("*.corrupted.*")
    )


def test_corruption_without_valid_backup_is_preserved_for_diagnosis(cache):
    cache.cache_file.write_text("{broken")
    assert cache.load() == (False, None)
    assert not cache.cache_file.exists()
    backups = list(cache.cache_file.parent.glob("*.corrupted.*"))
    assert len(backups) == 1
    assert backups[0].read_text() == "{broken"


@pytest.mark.parametrize("deny_delete", [False, True])
def test_corruption_recovery_handles_read_only_backup_directory(cache, monkeypatch, deny_delete):
    cache.cache_file.write_text("{broken")
    rename = Path.rename
    unlink = Path.unlink

    def fail_rename(path, target):
        if path == cache.cache_file:
            raise PermissionError("backup directory unavailable")
        return rename(path, target)

    def maybe_unlink(path, *args, **kwargs):
        if path == cache.cache_file and deny_delete:
            raise PermissionError("read only")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "rename", fail_rename)
    monkeypatch.setattr(Path, "unlink", maybe_unlink)
    assert cache.load() == (False, None)
    assert cache.cache_file.exists() is deny_delete
    if deny_delete:
        assert cache.cache_file.read_text() == "{broken"


def test_restore_failure_preserves_valid_backup(cache, monkeypatch):
    cache.cache_file.write_text("{broken")
    valid = cache.cache_file.with_name("listing.json.corrupted.1")
    write_cache(valid)
    original = valid.read_bytes()
    rename = Path.rename

    def fail_restore(path, target):
        if path == valid:
            raise PermissionError("cannot restore")
        return rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_restore)
    assert cache.load() == (False, None)
    assert valid.read_bytes() == original


@pytest.mark.parametrize("stage", ["serialize", "fsync", "rename"])
def test_failed_save_preserves_previous_cache_and_removes_temporary(cache, monkeypatch, stage):
    write_cache(cache.cache_file)
    original = cache.cache_file.read_bytes()

    def fail(*args, **kwargs):
        raise OSError("disk failure")

    target = {
        "serialize": "mirror_url.cache.json.dump",
        "fsync": "mirror_url.cache.os.fsync",
        "rename": "pathlib.Path.rename",
    }[stage]
    monkeypatch.setattr(target, fail)
    assert cache.save({BASE: "new"}, 2) is False
    assert cache.cache_file.read_bytes() == original
    assert not cache.cache_file.with_suffix(".json.tmp").exists()


@pytest.mark.parametrize("payload", ["{broken", "[]", "{}", '{"_meta": null}'])
def test_timestamp_refresh_rejects_invalid_data_without_replacing_it(cache, payload):
    cache.cache_file.write_text(payload)
    assert cache.refresh_timestamp() is False
    assert cache.cache_file.read_text() == payload
    assert list(cache.cache_file.parent.iterdir()) == [cache.cache_file]


def test_failed_timestamp_publication_cleans_up_unique_temporary(cache, monkeypatch):
    write_cache(cache.cache_file)
    original = cache.cache_file.read_bytes()
    monkeypatch.setattr(
        "mirror_url.cache.os.replace", lambda *args: (_ for _ in ()).throw(OSError("read only"))
    )
    assert cache.refresh_timestamp() is False
    assert cache.cache_file.read_bytes() == original
    assert list(cache.cache_file.parent.iterdir()) == [cache.cache_file]


@pytest.mark.parametrize(
    "entry", [{"files": ["a"], "subdirs": ["sub/"]}, (["a"], ["sub/"]), [["a"], ["sub/"]]]
)
def test_html_cache_supports_persisted_formats(cache, entry):
    cache.html_cache.put(BASE, entry)
    assert cache.get_html_cache(BASE) == (["a"], ["sub/"])
    assert cache.metrics.metrics["html_cache_hits"] == 1


@pytest.mark.parametrize("entry", [None, "invalid", ["only-one"]])
def test_invalid_html_cache_entry_is_a_miss(cache, entry):
    if entry is not None:
        cache.html_cache.put(BASE, entry)
    assert cache.get_html_cache(BASE) is None
    assert cache.metrics.metrics["html_cache_misses"] == 1


def test_html_cache_disable_and_directory_invalidation(cache):
    cache.config.cache_html = False
    cache.set_html_cache(BASE, ["a"], [])
    assert cache.get_html_cache(BASE) is None
    cache.config.cache_html = True
    assert cache.invalidate_directory(BASE, "first")
    cache.set_html_cache(BASE, ["a"], [], content_hash="hash")
    assert not cache.invalidate_directory(BASE, "first")
    assert cache.get_html_cache(BASE) == (["a"], [])
    assert cache.invalidate_directory(BASE, "second")
    assert cache.get_html_cache(BASE) is None


def test_metadata_pruning_and_cleanup_invalidate_lru_entries(cache, tmp_path, monkeypatch):
    monkeypatch.setattr("mirror_url.cache.MAX_CACHE_METADATA_ENTRIES", 2)
    files = [tmp_path / name for name in ("a", "b", "c")]
    for index, file in enumerate(files):
        cache.save_file_metadata(file, f'"{index}"', 0, index)
    assert list(cache.file_metadata_cache) == [str(file.resolve()) for file in files[1:]]
    cache.lru_file_cache.clear()
    assert cache.get_file_metadata(files[1])["size"] == 1
    assert cache.cleanup_stale_metadata({files[2]}) == 1
    assert cache.get_file_metadata(files[1]) is None
    cache.cleanup_file_metadata(files[2])
    cache.cleanup_file_metadata(files[2])
    assert cache.get_file_metadata(files[2]) is None


@pytest.mark.parametrize("pressure", [MemoryPressure.WARNING, MemoryPressure.CRITICAL, None])
def test_memory_pressure_evicts_cache_entries(cache, pressure):
    for i in range(10):
        cache.lru_file_cache.put(str(i), {"size": i})
        cache.html_cache.put(str(i), {"files": [str(i)], "subdirs": []})
    removed = cache.handle_memory_pressure(pressure=pressure)
    if pressure == MemoryPressure.WARNING:
        assert removed == 3 and len(cache.lru_file_cache) == 7
        assert len(cache.html_cache) == 10
    elif pressure == MemoryPressure.CRITICAL:
        assert removed == 14
        assert len(cache.lru_file_cache) == len(cache.html_cache) == 3
    else:
        assert removed == 0 and len(cache.lru_file_cache) == 10
