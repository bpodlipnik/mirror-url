"""Runtime behavior at the boundaries clarified by the shared typing contract."""

from unittest.mock import Mock

import pytest
from pydantic import ValidationError
from pydantic.warnings import PydanticDeprecatedSince20

from mirror_url import MirrorConfig, MirrorURL
from mirror_url._core._base import _MirrorBase
from mirror_url._core.cleanup import CleanupMixin
from mirror_url._core.compare import CompareMixin
from mirror_url._core.downloads import DownloadMixin
from mirror_url._core.report import ReportMixin
from mirror_url._core.scan import ScanMixin
from mirror_url._core.urls import UrlMixin
from mirror_url.rate_limiter import PerIPRateLimiter
from test_immediate_audit_fixes import BASE
from test_immediate_audit_fixes import mirror as mirror


def test_typed_contract_preserves_runtime_bases_and_method_resolution():
    mixins = (UrlMixin, ScanMixin, CompareMixin, DownloadMixin, CleanupMixin, ReportMixin)
    assert MirrorURL.__mro__ == (MirrorURL, *mixins, _MirrorBase, object)
    assert all(cls.__bases__ == (object,) for cls in (*mixins, _MirrorBase))


def test_config_validation_retains_pydantic_contract_and_separate_warnings(tmp_path):
    data = {
        "base_url": BASE,
        "dest_path": tmp_path / "mirror",
        "log_path": tmp_path / "logs",
        "workers": 21,
        "hash_algorithm": "md5",
    }
    config = MirrorConfig.model_validate(data)
    with pytest.warns(PydanticDeprecatedSince20):
        validated = MirrorConfig.validate(data)
    assert isinstance(validated, MirrorConfig)
    assert validated == config
    assert MirrorConfig.model_validate(config) is config
    warnings = MirrorConfig.validation_warnings(config)
    assert "High worker count may cause server issues" in warnings
    assert any("MD5" in warning for warning in warnings)
    with pytest.raises(ValidationError):
        MirrorConfig.model_validate({**data, "workers": 0})


def test_missing_partial_manager_fails_without_request_or_publication(mirror):
    final = mirror.target_dir / "a"
    final.write_bytes(b"original")
    mirror.partial_manager = None
    assert mirror._download_file_single(BASE + "a", final) is False
    assert mirror.files_failed.value() == 1
    assert final.read_bytes() == b"original"
    mirror.connection_manager.request.assert_not_called()


def test_disk_space_check_without_manager_fails_closed(mirror):
    mirror.disk_manager = None
    assert mirror.check_disk_space(1) is False
    assert mirror.metrics.metrics["disk_space_checks"] == 1


@pytest.mark.parametrize("field", ["target_dir", "target_parsed"])
def test_missing_target_disables_path_mapping_and_cleanup(mirror, field):
    final = mirror.target_dir / "a"
    final.write_bytes(b"original")
    setattr(mirror, field, None)
    assert mirror._get_local_path_from_url(BASE + "a") is None
    assert mirror._cleanup_path_selected(final) is False
    if field == "target_dir":
        assert mirror._scan_local_tree() == ([], [])
    assert final.read_bytes() == b"original"


@pytest.mark.parametrize("explicit_none", [False, True])
def test_per_ip_limiter_without_ip_uses_global_budget(monkeypatch, explicit_none):
    sleeps = Mock()
    monkeypatch.setattr("mirror_url.rate_limiter.time.time", Mock(return_value=10.0))
    monkeypatch.setattr("mirror_url.rate_limiter.time.sleep", sleeps)
    limiter = PerIPRateLimiter(requests_per_second=2)
    limiter.last_request = 9.75
    if explicit_none:
        limiter.wait(None)
    else:
        limiter.wait()
    sleeps.assert_called_once_with(0.25)
    assert limiter.total_delays == 1
    assert limiter.last_requests == {}
