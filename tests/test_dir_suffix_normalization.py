"""``dir_suffix`` is normalised inside ``MirrorConfig`` (no leading/trailing '/').

Previously only the non-``--config`` CLI path stripped slashes, so the same
suffix gave differently named log files depending on how the run was started.
"""

from pathlib import Path

import pytest

from mirror_url.config import MirrorConfig


def _cfg(tmp_path: Path, **kw) -> MirrorConfig:
    return MirrorConfig(
        base_url="http://example.org/data",
        dest_path=tmp_path / "dest",
        log_path=tmp_path / "log",
        **kw,
    )


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("2026", "2026"),
        ("/2026/", "2026"),
        ("2026/", "2026"),
        ("/2026", "2026"),
        ("a/b/", "a/b"),
        ("", ""),
        ("/", ""),
        (None, ""),
    ],
)
def test_constructor_normalizes(tmp_path, raw, expected):
    assert _cfg(tmp_path, dir_suffix=raw).dir_suffix == expected


def test_default_is_empty(tmp_path):
    assert _cfg(tmp_path).dir_suffix == ""


def test_from_dict_normalizes(tmp_path):
    cfg = MirrorConfig.from_dict(
        {
            "base_url": "http://example.org/data",
            "dest_path": tmp_path / "dest",
            "log_path": tmp_path / "log",
            "dir_suffix": "/2026/",
        },
        silent=True,
    )
    assert cfg.dir_suffix == "2026"


def test_yaml_and_constructor_agree(tmp_path):
    y = tmp_path / "c.yaml"
    y.write_text(
        f"base_url: http://example.org/data\ndest_path: {tmp_path}/dest\n"
        f"log_path: {tmp_path}/log\ndir_suffix: /2026/\n"
    )
    assert (
        MirrorConfig.from_yaml(y, silent=True).dir_suffix
        == _cfg(tmp_path, dir_suffix="/2026/").dir_suffix
    )
