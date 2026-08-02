"""Persistent, cross-run domain-health tracking for adaptive throttling.

Learns which remote domains are rate-limiting mirror-url (repeated 429/503
responses) across separate invocations, so a domain doesn't need to be
hardcoded into ``KNOWN_THROTTLED_DOMAINS`` (constants.py) to get a
conservative starting concurrency the next time it's mirrored -- it's
inferred from real observed behavior instead.

Deliberately 429/503-only, not RTT-variance-based: a hard status code from
the server is an unambiguous signal, whereas RTT variance is noisy (network
jitter, local load, a brief server hiccup all look similar statistically)
and would need real-world calibration this project doesn't have yet. RTT
variance can be added later once the simpler mechanism has proven itself.

Storage: a single small JSON file in the user's cache directory
(``get_domain_health_path()``), shared across every mirror-url invocation
regardless of --dir-suffix/--log-path -- unlike the tool's other on-disk
cache (``cache.py``'s ``CacheManager``), which is scoped per
(base_url, dir_suffix), domain health is inherently cross-run and
cross-suffix: it's a property of the server, not of any specific mirrored
subtree. A single archive mirrored via several --dir-suffix values (e.g.
different orbit numbers under the same domain) shares one throttle
history.

Threshold model: keeps up to MAX_INCIDENTS_STORED_PER_DOMAIN recent
incident timestamps per domain. A domain is considered throttled if at
least INCIDENT_THRESHOLD of those timestamps fall within the trailing
INCIDENT_WINDOW_SECONDS. This decays automatically and gradually as old
incidents age out of the window -- no separate "throttled_until" field or
expiry-extension logic needed, and no manual reset is ever required.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from threading import RLock
from typing import Dict, List, Optional

DOMAIN_HEALTH_SCHEMA_VERSION = 1

# A domain is "throttled" once it has racked up this many 429/503
# responses within the trailing window below. Deliberately conservative
# (a fairly long window, a real threshold rather than a hair-trigger) since
# the underlying server infrastructure this targets (institutional/
# scientific data archives) changes rate-limiting behavior slowly, if
# ever -- a false positive here just means starting a bit more cautiously
# than strictly necessary, which is a low-cost mistake; a false negative
# means no protection at all, the more costly mistake.
INCIDENT_WINDOW_SECONDS = 14 * 24 * 3600  # 14 days
INCIDENT_THRESHOLD = 5

# Bounds the file's per-domain growth regardless of how long a domain has
# been observed; old entries beyond this count are pruned on write.
MAX_INCIDENTS_STORED_PER_DOMAIN = 20


def get_domain_health_path() -> Path:
    """Resolve the cross-platform per-user cache path for the domain-health file.

    Windows: %LOCALAPPDATA%\\mirror-url\\domain_health.json (falls back to
    %APPDATA%, then ~\\AppData\\Local, if neither env var is set).
    POSIX: $XDG_CACHE_HOME/mirror-url/domain_health.json, falling back to
    ~/.cache/mirror-url/domain_health.json per the XDG Base Directory spec.
    """
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        root = Path(base) if base else Path.home() / "AppData" / "Local"
    else:
        xdg = os.environ.get("XDG_CACHE_HOME")
        root = Path(xdg) if xdg else Path.home() / ".cache"
    return root / "mirror-url" / "domain_health.json"


class DomainHealthTracker:
    """Tracks 429/503 incidents per domain, persisted across runs.

    Thread-safe within a single process (RLock guards all state access).
    Cross-process safety relies on atomic replace-on-write (``Path.replace``,
    which maps to ``os.replace`` -- atomic AND overwrites the destination
    on both POSIX and Windows, unlike ``Path.rename``) so a concurrent
    writer never observes a half-written or corrupted file. A lost update
    between two simultaneous writers (e.g. two mirror-url processes hitting
    the same domain via different --dir-suffix values at the same moment)
    is possible -- one process's increment can be overwritten by the
    other's -- but is not guarded against explicitly: it self-corrects
    over subsequent runs/incidents, and the cost of an occasional
    undercount is low compared to the complexity of proper file locking.

    Every operation degrades gracefully to a no-op on any I/O or parse
    failure (missing permissions, corrupt file, read-only filesystem,
    unwritable home directory, etc.) -- this is a best-effort optimization
    hint, never a hard dependency, and must never be the reason a mirror
    run fails.
    """

    def __init__(self, path: Optional[Path] = None):
        self.path = path or get_domain_health_path()
        self.lock = RLock()
        self._data: Dict[str, List[float]] = {}
        self._loaded = False

    def _load(self) -> None:
        """Load state from disk once per instance lifetime. Must be called
        with self.lock held."""
        if self._loaded:
            return
        self._loaded = True
        try:
            if not self.path.exists():
                return
            with open(self.path, encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return
            if raw.get("_meta", {}).get("version") != DOMAIN_HEALTH_SCHEMA_VERSION:
                # Unknown/future schema -- don't guess at its shape, start fresh.
                return
            domains = raw.get("domains", {})
            if not isinstance(domains, dict):
                return
            self._data = {
                domain: [t for t in timestamps if isinstance(t, (int, float))]
                for domain, timestamps in domains.items()
                if isinstance(domain, str) and isinstance(timestamps, list)
            }
        except Exception as e:
            logging.debug(f"Could not load domain health file ({self.path}): {e}")
            self._data = {}

    def record_incident(self, domain: str) -> None:
        """Record a 429/503 response observed from `domain` (right now) and
        persist immediately -- incidents are rare enough in practice
        (only real rate-limit/server-overload responses) that writing on
        every one is cheap and keeps the on-disk state always current."""
        if not domain:
            return
        domain = domain.lower()
        with self.lock:
            self._load()
            timestamps = self._data.setdefault(domain, [])
            timestamps.append(time.time())
            if len(timestamps) > MAX_INCIDENTS_STORED_PER_DOMAIN:
                del timestamps[: len(timestamps) - MAX_INCIDENTS_STORED_PER_DOMAIN]
            self._save()

    def is_throttled(self, domain: str) -> bool:
        """True if `domain` has racked up INCIDENT_THRESHOLD or more
        429/503 incidents within the trailing INCIDENT_WINDOW_SECONDS."""
        if not domain:
            return False
        domain = domain.lower()
        with self.lock:
            self._load()
            timestamps = self._data.get(domain, [])
            cutoff = time.time() - INCIDENT_WINDOW_SECONDS
            recent_count = sum(1 for t in timestamps if t >= cutoff)
            return recent_count >= INCIDENT_THRESHOLD

    def _save(self) -> None:
        """Must be called with self.lock held."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "_meta": {"version": DOMAIN_HEALTH_SCHEMA_VERSION},
                "domains": self._data,
            }
            temp_path = self.path.with_suffix(".json.tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, ensure_ascii=False, separators=(",", ": "))
                f.flush()
                os.fsync(f.fileno())
            temp_path.replace(self.path)
        except Exception as e:
            logging.debug(f"Could not save domain health file ({self.path}): {e}")


_default_tracker: Optional[DomainHealthTracker] = None
_default_tracker_lock = RLock()


def get_domain_health_tracker() -> DomainHealthTracker:
    """Process-wide lazy singleton, analogous to utils.py's process-wide
    ``_log_files`` registry -- domain health is inherently a cross-run,
    cross-component concern, not scoped to any single MirrorURL instance,
    so it's threaded through as a shared accessor rather than a
    constructor parameter passed through every call site."""
    global _default_tracker
    with _default_tracker_lock:
        if _default_tracker is None:
            _default_tracker = DomainHealthTracker()
        return _default_tracker
