"""Regression test for duplicate log lines during startup and download-method
auto-selection.

Reported symptom (real production log): every startup line in the config
summary -- "SAFE MODE: Deletion DISABLED", "Cache max age", "Rate limiting",
"HTML caching enabled", "Adaptive async", etc. -- was printed twice per run,
and "Auto-selected: SEQUENTIAL ..." appeared twice with different wording
("(single file detected)" vs "downloads").

Root cause (startup block): ``MirrorURL.__init__`` calls ``self.
setup_logging()`` early on, which logged the *entire* config summary as
part of setting up log handlers. Later in the same ``__init__``, a second,
separately-written block (labeled "24. LOG INITIAL CONFIGURATION") logged
nearly the same information again -- in several cases (cache max age,
ETag support, URL sanitization, parallel chunk downloads) the second
block's version was actually more correct (properly conditional) than
setup_logging()'s simpler, sometimes-misleading unconditional version.

Root cause (auto-select): ``ParallelDownloadManager.auto_select_method()``
(download.py) already logs a detailed message explaining *why* it picked
a method (e.g. "SEQUENTIAL (single file detected)"). Its caller in
report.py's ``sync()`` then logged its own, less-detailed confirmation of
the same decision it had just received back.

Fix: setup_logging() now only logs the startup banner (suffix, command,
version, job start, paths, workers/retries) and configures the actual
logging handlers; the full config summary has a single home in __init__'s
"24" block. report.py no longer re-logs the auto-select decision;
download.py's more detailed message is the sole source.

This test inspects the source directly rather than reconstructing a full
MirrorURL instantiation (which needs a live connection, health-check
server, etc.) -- the defect is static duplicate logging.info() call sites
for the same concept, which source inspection tests directly and robustly.
"""

from __future__ import annotations

import inspect

from mirror_url._core import _base, report


def _source(module) -> str:
    return inspect.getsource(module)


# Fragments that were emitted twice in the real production log. Each must
# now appear as a logging.info(...) call exactly once in _base.py.
FORMERLY_DUPLICATED_STARTUP_FRAGMENTS = [
    "Rate limiting: {delay_ms",
    "HTML caching enabled (",
    "Resume capability enabled",
    "Adaptive batch processing: initial=",
    "Fast parsing fallback enabled",
    "Connection pool pre-warming enabled",
    "Content hash: files <",
]


def test_startup_config_fragments_logged_exactly_once():
    src = _base.__file__ and inspect.getsource(_base)
    for fragment in FORMERLY_DUPLICATED_STARTUP_FRAGMENTS:
        count = src.count(fragment)
        assert count == 1, (
            f"{fragment!r} should appear exactly once as a logging call in "
            f"_base.py (setup_logging() must not re-log what __init__'s "
            f"'24. LOG INITIAL CONFIGURATION' block already logs), found "
            f"{count} times"
        )


def test_log_cleanup_policy_called_exactly_once_in_init_path():
    """_log_cleanup_policy() (the source of the doubled 'SAFE MODE:
    Deletion DISABLED' line) must only be invoked once during startup."""
    src = _source(_base)
    assert src.count("self._log_cleanup_policy()") == 1


def test_memory_and_per_ip_rate_limiting_logged_exactly_once():
    src = _source(_base)
    assert src.count('logging.info(f"{prefix}📊 Memory monitoring: ENABLED")') == 1
    assert src.count('logging.info(f"{prefix}🔒 Per-IP rate limiting: ENABLED")') == 1


def test_auto_select_decision_not_re_logged_by_report():
    """report.py must not re-log the auto-select decision; download.py's
    auto_select_method() already logs a more detailed message explaining
    the reasoning at the moment it makes the decision."""
    src = _source(report)
    assert "Auto-selected: SEQUENTIAL downloads" not in src
    assert "Auto-selected: STREAMING PARALLEL downloads" not in src
    assert "Auto-selected: TRADITIONAL PARALLEL downloads" not in src


def test_auto_select_still_applies_the_chosen_method():
    """Sanity check the fix didn't remove the actual config-flag-setting
    logic alongside the redundant log lines -- only the logging."""
    src = _source(report)
    assert "self.config.sequential_downloads = True" in src
    assert "self.config.streaming_parallel = True" in src
    assert "self.config.parallel_downloads = True" in src
