"""Resolved Windows path aliases must not break containment or partial identity."""

from __future__ import annotations

import hashlib
from pathlib import Path, PureWindowsPath
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mirror_url.download import PartialDownloadManager
from mirror_url.security import PathSafety


def resolved(value):
    return SimpleNamespace(resolve=lambda: PureWindowsPath(value), exists=lambda: True)


@pytest.mark.parametrize(
    "root,child",
    [
        ("C:/mirror", "//?/C:/mirror/c2/a.fts"),
        ("//?/C:/mirror", "C:/mirror/c2/a.fts"),
        ("//?/C:/mirror", "//?/C:/mirror/c2/a.fts"),
        ("c:/mirror", "//?/C:/MIRROR/c2/a.fts"),
        ("//server/share/mirror", "//?/UNC/server/share/mirror/c2/a.fts"),
        ("//?/UNC/server/share/mirror", "//server/share/mirror/c2/a.fts"),
        ("//?/UNC/server/share/mirror", "//?/UNC/server/share/mirror/c2/a.fts"),
    ],
)
def test_equivalent_resolved_windows_namespaces_share_containment(root, child):
    parent, file = resolved(root), resolved(child)
    assert PathSafety.resolved_relative_to(file, parent) == PureWindowsPath("c2/a.fts")
    assert PathSafety.is_subpath(parent, file)
    assert PathSafety.safe_relative_to(file, parent) == str(PureWindowsPath("c2/a.fts"))


@pytest.mark.parametrize(
    "root,child",
    [
        ("C:/mirror", "//?/D:/mirror/a"),
        ("C:/mirror", "//?/C:/mirror-sibling/a"),
        ("//?/C:/mirror", "C:/outside/a"),
        ("//server/share/mirror", "//?/UNC/server/other/mirror/a"),
        ("//server/share/mirror", "//?/UNC/other/share/mirror/a"),
        ("C:/mirror", "//?/GLOBALROOT/Device/HarddiskVolume1/mirror/a"),
        ("C:/mirror", "//./C:/mirror/a"),
        ("C:/mirror", "//?/C:mirror/a"),
        ("C:/mirror", "//?/UNC/"),
    ],
)
def test_other_drives_shares_siblings_and_device_namespaces_remain_outside(root, child):
    parent, file = resolved(root), resolved(child)
    with pytest.raises(ValueError):
        PathSafety.resolved_relative_to(file, parent)
    assert not PathSafety.is_subpath(parent, file)
    assert PathSafety.safe_relative_to(file, parent) is None


@pytest.mark.parametrize(
    "prefixed_child,prefixed_root", [(True, False), (False, True), (True, True)]
)
def test_partial_names_stay_stable_across_resolved_windows_aliases(
    tmp_path, monkeypatch, prefixed_child, prefixed_root
):
    manager = PartialDownloadManager(tmp_path / "mirror")
    final = manager.download_dir / "c2" / "a.fts"
    original = Path.resolve

    def resolve(path, *args, **kwargs):
        if path == final:
            return PureWindowsPath(("//?/" if prefixed_child else "") + "C:/mirror/c2/a.fts")
        if path == manager.download_dir:
            return PureWindowsPath(("//?/" if prefixed_root else "") + "C:/mirror")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", resolve)
    state = tmp_path / "state"
    monkeypatch.setattr(manager, "_state_directory", lambda: state)
    expected = hashlib.sha256(str(PureWindowsPath("c2/a.fts")).encode("utf-8")).hexdigest()
    assert manager.get_partial_path(final) == state / (expected + manager.partial_suffix)
    assert manager.register_partial(final, "https://files.example/c2/a.fts").parent == state
    assert next(iter(manager.active_partials.values()))["final_path"] == final


@pytest.mark.parametrize("relative", ["../outside/a", "."])
def test_partial_paths_must_name_a_file_below_the_destination(tmp_path, monkeypatch, relative):
    manager = PartialDownloadManager(tmp_path / "mirror")
    state = Mock(side_effect=AssertionError("outside path must not create state"))
    monkeypatch.setattr(manager, "_state_directory", state)
    with pytest.raises(ValueError):
        manager.get_partial_path(manager.download_dir / relative)
    state.assert_not_called()
