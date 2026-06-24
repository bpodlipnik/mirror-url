"""Smoke tests — verify the package is importable and basic metadata is sane.

These run today against the skeleton. As modules are populated, un-skip the
relevant blocks below.
"""

from __future__ import annotations

import importlib

import pytest


def test_package_imports():
    pkg = importlib.import_module("mirror_url")
    assert pkg.__version__
    assert isinstance(pkg.__version__, str)


def test_version_matches_pyproject():
    import pathlib
    import re

    pkg = importlib.import_module("mirror_url")
    root = pathlib.Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    m = re.search(r'^version\s*=\s*"([^"]+)"', pyproject, re.MULTILINE)
    assert m, "version not found in pyproject.toml"
    assert m.group(1) == pkg.__version__


@pytest.mark.parametrize(
    "module",
    [
        "mirror_url.exceptions",
        "mirror_url.enums",
        "mirror_url.constants",
        "mirror_url.utils",
        "mirror_url.security",
        "mirror_url.config",
        "mirror_url.core",
        "mirror_url.cli",
    ],
)
def test_submodules_importable(module):
    """Every planned submodule must at least import (placeholder or populated)."""
    importlib.import_module(module)
