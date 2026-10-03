"""Real discovery/mapping/cleanup components with controlled I/O boundaries."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import urlparse

import httpx

from mirror_url import MirrorConfig
from mirror_url._core.cleanup import CleanupMixin
from mirror_url._core.scan import ScanMixin
from mirror_url._core.urls import UrlMixin
from mirror_url.enums import CleanupPolicy, MemoryPressure
from mirror_url.metrics import MetricsCollector
from mirror_url.primitives import LRUCache
from mirror_url.scanner import DirectoryScanner
from mirror_url.security import SymlinkTracker

ROOT = "https://example.com/root/"


class SafetyMirror(UrlMixin, ScanMixin, CleanupMixin):
    def __init__(self, tmp_path: Path, **options):
        self.config = MirrorConfig(
            base_url=ROOT,
            dest_path=tmp_path / "mirror",
            log_path=tmp_path / "logs",
            no_cache=options.pop("no_cache", True),
            cleanup_policy=options.pop("cleanup_policy", CleanupPolicy.DELETE),
            **options,
        )
        self.target_base_url = self._get_target_base_url()
        self.base_url = ROOT
        self.base_parsed = urlparse(ROOT)
        self.target_parsed = urlparse(self.target_base_url)
        self.target_dir = self.config.dest_path
        self.target_dir.mkdir(exist_ok=True)
        self._target_dir_path = self.target_dir.resolve()
        self.connection_ok = True
        self.scan_incomplete = False
        self.cleanup_protected_prefixes = set()
        self.metrics = MetricsCollector()
        self.cache_manager = SimpleNamespace(
            load=Mock(return_value=(False, None)),
            save=Mock(),
            get_html_cache=Mock(return_value=None),
            set_html_cache=Mock(),
            handle_memory_pressure=Mock(return_value=0),
            cleanup_file_metadata=Mock(),
            cleanup_stale_metadata=Mock(return_value=0),
            html_cache=LRUCache(10, ttl_seconds=3600, name="safety-html"),
        )
        self.fs_cache = SimpleNamespace(invalidate=Mock())
        self.multi_progress = SimpleNamespace(add_level=Mock(), update=Mock())
        self.memory_monitor = SimpleNamespace(
            check_pressure=Mock(return_value=MemoryPressure.NORMAL)
        )
        self.per_ip_limiter = SimpleNamespace(wait=Mock())
        self.symlink_tracker = SymlinkTracker()
        self.connection_manager = SimpleNamespace(request=Mock(side_effect=self._empty_response))
        self.scanner = DirectoryScanner(self)
        self.total_suffixes = 1

    @staticmethod
    def _get_prefix():
        return ""

    @staticmethod
    def _empty_response(url, method="GET", **kwargs):
        return httpx.Response(200, request=httpx.Request(method, url), content=b"<html></html>")

    def listing(self, body, status=200):
        self.connection_manager.request.side_effect = lambda url, method="GET", **kw: (
            httpx.Response(
                status,
                request=httpx.Request(method, url),
                content=body.encode(),
                headers={"Content-Type": "text/html"},
            )
        )
