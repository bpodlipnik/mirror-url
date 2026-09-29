"""Regression: ``PathSafety.safe_join`` must validate the NUL-stripped part.

Previously ``..`` / absolute-path checks ran on the raw part and NULs were
stripped only afterwards, so e.g. ``".\\0."`` slipped past the traversal check
as one odd component (and collapsed to ``..`` later); only the final
containment check stopped it. Stripping first makes every check see the string
that is actually used.
"""

from __future__ import annotations

import logging
from pathlib import Path

from mirror_url.security import PathSafety


def test_nul_split_dotdot_rejected_by_traversal_check(tmp_path: Path, caplog):
    with caplog.at_level(logging.WARNING):
        assert PathSafety.safe_join(tmp_path, ".\0.") is None
    assert "Path traversal attempt detected" in caplog.text


def test_nul_before_slash_dotdot_rejected(tmp_path: Path):
    assert PathSafety.safe_join(tmp_path, "a\0/..", "b") is None


def test_nul_in_absolute_path_rejected(tmp_path: Path, caplog):
    with caplog.at_level(logging.WARNING):
        assert PathSafety.safe_join(tmp_path, "\0/etc/passwd") is None
    assert "Absolute path detected" in caplog.text


def test_part_that_is_only_nul_is_skipped(tmp_path: Path):
    assert PathSafety.safe_join(tmp_path, "\0", "a.fits") == (tmp_path / "a.fits").resolve()


def test_nul_inside_normal_name_is_removed(tmp_path: Path):
    assert PathSafety.safe_join(tmp_path, "im\0age.fits") == (tmp_path / "image.fits").resolve()
