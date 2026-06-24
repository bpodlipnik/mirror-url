"""MirrorURL orchestrator, composed from focused mixins.

The original ~3,600-line ``MirrorURL`` class was split into behavior-identical
mixins under the private ``_core`` subpackage (see ``REFACTORING_PLAN.md`` §4.1).
Each method lives in exactly one mixin; ``_MirrorBase`` owns ``__init__`` and all
shared instance state. The composed class below has the exact same public
surface as before, so ``from mirror_url.core import MirrorURL`` is unchanged for
callers (e.g. ``ConnectionManager``'s scope check, ``cli``, ``health``).
"""

from __future__ import annotations

from ._core._base import _MirrorBase
from ._core.cleanup import CleanupMixin
from ._core.compare import CompareMixin
from ._core.downloads import DownloadMixin
from ._core.report import ReportMixin
from ._core.scan import ScanMixin
from ._core.urls import UrlMixin


class MirrorURL(
    UrlMixin,
    ScanMixin,
    CompareMixin,
    DownloadMixin,
    CleanupMixin,
    ReportMixin,
    _MirrorBase,
):
    """Main mirroring class.

    Composed from mixins; method resolution is unambiguous because every method
    is defined in exactly one mixin and ``__init__``/shared state live in
    ``_MirrorBase``. Behavior is identical to the pre-split v3.1.13 class.
    """


__all__ = ["MirrorURL"]
