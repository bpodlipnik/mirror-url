"""Accepted-but-unread config options are flagged, and no phantom CLI lookups remain.

Seven ``MirrorConfig`` fields are declared but read by no code: ``auto_select_method``,
``force_method``, ``use_dedicated_download_pool``, ``parallel_files_min_files``,
``streaming_min_file_size_mb``, ``streaming_min_files``, ``traditional_min_files``.
They stay accepted (``extra="forbid"`` would break existing YAML), but setting one
to a non-default value must not be silent.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

import pytest

from mirror_url import config as config_module
from mirror_url.config import _UNUSED_CONFIG_FIELDS, MirrorConfig

SRC = Path(config_module.__file__).parent


def _cfg(tmp_path, **kw):
    return MirrorConfig(
        base_url="http://example.org/data",
        dest_path=tmp_path / "d",
        log_path=tmp_path / "l",
        **kw,
    )


@pytest.fixture(autouse=True)
def _reset_warned():
    config_module._WARNED_UNUSED_FIELDS.clear()
    yield
    config_module._WARNED_UNUSED_FIELDS.clear()


def test_non_default_value_warns(tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        _cfg(tmp_path, force_method="sequential")
    assert "force_method" in caplog.text
    assert "no effect" in caplog.text


def test_default_values_do_not_warn(tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        _cfg(tmp_path)
        _cfg(tmp_path, force_method=None, streaming_min_files=4)  # explicit but default
    assert "no effect" not in caplog.text


def test_warns_once_per_field(tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        _cfg(tmp_path, streaming_min_files=9)
        _cfg(tmp_path, streaming_min_files=9)
    assert caplog.text.count("streaming_min_files") == 1


def test_fields_still_accepted(tmp_path):
    cfg = _cfg(tmp_path, traditional_min_files=7)
    assert cfg.traditional_min_files == 7


def test_declared_unused_fields_really_are_unread():
    """Guard: if code starts reading one of these, drop it from the list."""
    for name in _UNUSED_CONFIG_FIELDS:
        # attribute access only: ``x.name(...)`` is a method call (auto_select_method()),
        # not a config read
        pat = re.compile(rf"(?:\.{name}\b(?!\s*\()|getattr\([^)]*[\"']{name}[\"'])")
        hits = [
            str(f.relative_to(SRC))
            for f in SRC.rglob("*.py")
            if f.name not in ("config.py", "cli.py") and pat.search(f.read_text(encoding="utf-8"))
        ]
        assert not hits, f"{name} is read in {hits}; remove it from _UNUSED_CONFIG_FIELDS"


def test_no_phantom_cli_lookups():
    text = (SRC / "cli.py").read_text(encoding="utf-8") + (SRC / "config.py").read_text(
        encoding="utf-8"
    )
    for phantom in ("streaming_min_size", '"auto_select"', '"network_speed"'):
        assert phantom not in text
