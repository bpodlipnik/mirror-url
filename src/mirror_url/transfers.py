"""Bounded whole-file transfers with shared publication and URL-list policy.

Backends only provide response headers and raw bytes. Scope, redirects, pacing,
staging, length checks, receipts and publication belong to this common layer.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import shutil
import socket
import stat
import sys
import tempfile
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlsplit

import httpx

from .config import MirrorConfig
from .destination_lock import DestinationLock, current_run_lock
from .download import PartialDownloadManager
from .download_integrity import content_length, file_sha256, strong_etag
from .exceptions import ConfigError, SecurityError, URLScopeError
from .filename_mapping import FilenameMap, _stat_identity
from .scratch import OwnedScratch
from .security import PathSafety, SecurityValidator
from .transport import SecureAsyncTransport
from .utils import _relative_url_path, sanitize_url_for_log, url_within_scope


@dataclass
class TransferResult:
    url: str
    path: Path
    success: bool
    size: int = 0
    etag: Optional[str] = None
    sha256: Optional[str] = None
    mtime: Optional[float] = None
    error: Optional[str] = None


class AsyncRequestPacer:
    """One monotonic request budget, including redirects and retries.

    Reserve a start slot before yielding; sleeping never holds the lock. A
    single scoped origin shares this budget regardless of its resolved IPs.
    """

    def __init__(self, interval: float):
        self.interval = interval
        self.next_start = 0.0
        self.wait_seconds = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self.interval:
            return
        started = time.monotonic()
        async with self.lock:
            slot = max(started, self.next_start)
            self.next_start = slot + self.interval
        delay = slot - started
        if delay > 0:
            await asyncio.sleep(delay)
            self.wait_seconds += time.monotonic() - started


class AsyncBandwidthBudget:
    """Reserve bytes across every worker, rather than multiplying the limit."""

    def __init__(self, mib_per_second: Optional[float]):
        self.bytes_per_second = (mib_per_second or 0) * 1024**2
        self.next_finish = 0.0
        self.lock = asyncio.Lock()

    async def wait(self, size: int) -> None:
        if not self.bytes_per_second or size <= 0:
            return
        now = time.monotonic()
        async with self.lock:
            self.next_finish = max(now, self.next_finish) + size / self.bytes_per_second
            delay = self.next_finish - now
        await asyncio.sleep(delay)


def validate_url(url: str, scope: str) -> None:
    safe, reason = SecurityValidator.validate_url_security(url, scope)
    if not safe:
        raise SecurityError(reason)
    if not url_within_scope(url, scope):
        raise URLScopeError("Download URL or redirect is outside the selected origin/path")
    host = urlsplit(url).hostname
    try:
        ipaddress.ip_address(host or "")
    except ValueError:
        return
    raise SecurityError("Direct IP connections are forbidden")


def plan_targets(
    urls: Iterable[str], scope: str, target: Path, config: MirrorConfig
) -> List[Tuple[str, Path]]:
    """Reject the complete list before the first GET or publication."""
    names = FilenameMap(target)
    selected: Dict[Tuple[str, ...], str] = {}
    plan = []
    for url in urls:
        validate_url(url, scope)
        relative = _relative_url_path(url, scope)
        if not relative or relative.endswith("/"):
            raise ValueError("URL-list entries must name files below the base URL")
        if relative.split("/")[0].casefold() == ".mirror-url-state":
            raise ValueError("Remote file conflicts with reserved download state")
        local = PathSafety.safe_join(
            target,
            *relative.split("/"),
            max_depth=config.max_depth,
            max_filename_len=config.max_filename_len,
            create_base=False,
        )
        if local is None:
            raise ValueError("Unsafe download destination")
        if local.relative_to(target).parts != tuple(relative.split("/")):
            raise ValueError(
                "Remote filename would be rewritten; preserving original names is required"
            )
        key = names.key(local)
        if key in selected:
            raise ValueError("Duplicate URL or distinct URLs mapping to the same local filename")
        selected[key] = url
        plan.append((url, local))
    return plan


def _identity(path: Path) -> Optional[Tuple[int, ...]]:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_nlink != 1
        or (hasattr(os, "getuid") and info.st_uid != os.getuid())
    ):
        raise ValueError("Download would replace an unsafe or unowned local entry")
    return _stat_identity(info)


def _set_file_timestamp(descriptor: int, timestamp: float) -> None:
    """Set times on the verified open file, without resolving its path again."""
    if sys.platform != "win32":
        os.utime(descriptor, (timestamp, timestamp))
        return

    # Python 3.10-3.12 on Windows has neither fd utime nor no-follow path utime.
    # SetFileTime operates on the same handle whose identity was just checked.
    import ctypes
    import msvcrt
    from ctypes import wintypes

    ticks = int(timestamp * 10_000_000) + 116444736000000000
    if not 0 <= ticks <= 0x7FFFFFFFFFFFFFFF:
        raise ValueError("Last-Modified is outside the Windows FILETIME range")
    value = wintypes.FILETIME(ticks & 0xFFFFFFFF, ticks >> 32)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    set_time = kernel.SetFileTime
    set_time.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
        ctypes.POINTER(wintypes.FILETIME),
    ]
    set_time.restype = wintypes.BOOL
    handle = wintypes.HANDLE(msvcrt.get_osfhandle(descriptor))
    if not set_time(handle, None, ctypes.byref(value), ctypes.byref(value)):
        raise ctypes.WinError(ctypes.get_last_error())


def require_backend(backend: str) -> Any:
    if backend == "httpx":
        return None
    if backend != "aiohttp":
        raise ConfigError("Unknown download backend")
    try:
        import aiohttp
    except ImportError as error:
        raise ConfigError("Install mirror-url[aiohttp] to use --backend aiohttp") from error
    return aiohttp


class _HTTPXBackend:
    retryable = (httpx.TransportError, asyncio.TimeoutError)

    def __init__(self, config: MirrorConfig, concurrency: int):
        limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
        self.client = httpx.AsyncClient(
            transport=SecureAsyncTransport(http2=config.http2, limits=limits),
            http2=config.http2,
            limits=limits,
            timeout=config.timeout,
            trust_env=False,
            follow_redirects=False,
        )

    async def close(self) -> None:
        await self.client.aclose()

    @asynccontextmanager
    async def open(self, url: str):
        async with self.client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as r:
            yield r.status_code, r.headers, r.aiter_raw()


class _AIOHTTPBackend:
    def __init__(self, config: MirrorConfig, concurrency: int):
        aiohttp = require_backend("aiohttp")

        class PublicResolver:
            async def resolve(self, host, port=0, family=socket.AF_INET):
                # Return the validated address to the connector itself, so a
                # second DNS lookup cannot rebind the connection to a private IP.
                ip = await asyncio.to_thread(SecurityValidator.resolve_and_validate_hostname, host)
                return [
                    {
                        "hostname": host,
                        "host": ip,
                        "port": port,
                        "family": socket.AF_INET6 if ":" in ip else socket.AF_INET,
                        "proto": 0,
                        "flags": socket.AI_NUMERICHOST,
                    }
                ]

            async def close(self):
                return None

        self.retryable = (
            aiohttp.ClientConnectionError,
            aiohttp.ClientPayloadError,
            asyncio.TimeoutError,
        )
        connector = aiohttp.TCPConnector(
            limit=concurrency,
            limit_per_host=concurrency,
            resolver=PublicResolver(),
            ttl_dns_cache=300,
        )
        self.client = aiohttp.ClientSession(
            connector=connector,
            auto_decompress=False,
            trust_env=False,
            timeout=aiohttp.ClientTimeout(
                total=None, connect=config.timeout, sock_read=config.timeout
            ),
            cookie_jar=aiohttp.DummyCookieJar(),
        )

    async def close(self) -> None:
        await self.client.close()

    @asynccontextmanager
    async def open(self, url: str):
        from yarl import URL

        async with self.client.get(
            # Match HTTPX's URI escaping while retaining literal percent escapes.
            URL(str(httpx.URL(url)), encoded=True),
            headers={"Accept-Encoding": "identity"},
            allow_redirects=False,
        ) as r:
            yield r.status, r.headers, r.content.iter_chunked(256 * 1024)


class AsyncTransfers:
    """A pooled client and a fixed worker set for one complete transfer batch."""

    def __init__(self, config: MirrorConfig, target: Path, scope: str, scratch: OwnedScratch):
        self.config, self.target, self.scope, self.scratch = config, target, scope, scratch
        self.concurrency = 1 if config.sequential_downloads else config.max_concurrent_downloads
        self.pacer = AsyncRequestPacer(config.effective_request_interval)
        self.bandwidth = AsyncBandwidthBudget(config.bandwidth_limit)
        self.outstanding: Dict[Path, int] = {}
        self.stage_seconds = {"fsync": 0.0, "hash": 0.0}

    def _space(self) -> None:
        needed = sum(self.outstanding.values()) + self.concurrency * 1024 * 1024
        if shutil.disk_usage(self.target).free < needed:
            raise OSError("Insufficient free space for active transfers and write headroom")

    async def _one(self, backend, url: str, local: Path) -> TransferResult:
        original = _identity(local)
        relative = local.relative_to(self.target)
        if (
            PathSafety.safe_join(
                self.target,
                *relative.parts,
                create_base=False,
                max_depth=self.config.max_depth,
                max_filename_len=self.config.max_filename_len,
            )
            != local
        ):
            raise ValueError("Destination path changed before transfer")
        local.parent.mkdir(parents=True, exist_ok=True)
        staging = self.scratch.staging(local, "streaming")
        try:
            for attempt in range(self.config.max_retries + 1):
                try:
                    current = url
                    for hop in range(11):
                        validate_url(current, self.scope)
                        await self.pacer.wait()
                        async with backend.open(current) as (code, headers, chunks):
                            if code in (301, 302, 303, 307, 308):
                                if hop == 10 or not headers.get("Location"):
                                    raise ValueError(
                                        "Missing redirect target or too many redirects"
                                    )
                                current = urljoin(current, headers["Location"])
                                continue
                            if code != 200:
                                raise ValueError(
                                    f"Full-file GET requires HTTP 200, received {code}"
                                )
                            if headers.get("Content-Encoding", "identity").lower() != "identity":
                                raise ValueError(
                                    "Encoded response cannot be published as raw file bytes"
                                )
                            expected = content_length(headers)
                            self.outstanding[local] = expected or 0
                            self._space()
                            # Each retry gets a new file in the same owned workspace.
                            if staging.exists():
                                _identity(staging)
                                staging.unlink()
                            size, checkpoint = 0, 0
                            with staging.open("xb") as file:
                                staged_inode = os.fstat(file.fileno())
                                async for block in chunks:
                                    size += len(block)
                                    if expected is not None and size > expected:
                                        raise ValueError("Response exceeds Content-Length")
                                    file.write(block)
                                    self.outstanding[local] = max(0, (expected or 0) - size)
                                    if size - checkpoint >= 1024 * 1024:
                                        self._space()
                                        checkpoint = size
                                    await self.bandwidth.wait(len(block))
                                if expected is not None and size != expected:
                                    raise ValueError(
                                        "Response length does not match Content-Length"
                                    )
                                started = time.perf_counter()
                                file.flush()
                                os.fsync(file.fileno())
                                self.stage_seconds["fsync"] += time.perf_counter() - started
                            if staging.stat().st_size != size:
                                raise ValueError("Staged length does not match received bytes")
                            staged_identity = _identity(staging)
                            if staged_identity is None or staged_identity[:2] != (
                                staged_inode.st_dev,
                                staged_inode.st_ino,
                            ):
                                raise ValueError("Owned staging file changed during transfer")
                            started = time.perf_counter()
                            digest = file_sha256(staging, self.config.verify_content)
                            self.stage_seconds["hash"] += time.perf_counter() - started
                            if _identity(staging) != staged_identity:
                                raise ValueError("Owned staging file changed during hashing")
                            mtime = None
                            if headers.get("Last-Modified"):
                                try:
                                    mtime = parsedate_to_datetime(
                                        headers["Last-Modified"]
                                    ).timestamp()
                                except (ValueError, TypeError, OverflowError):
                                    pass
                            if mtime is not None:
                                with staging.open("r+b") as file:
                                    info = os.fstat(file.fileno())
                                    opened_identity = _stat_identity(info)
                                    if (
                                        not stat.S_ISREG(info.st_mode)
                                        or info.st_nlink != 1
                                        or opened_identity != staged_identity
                                    ):
                                        raise ValueError(
                                            "Owned staging file changed before timestamp update"
                                        )
                                    _set_file_timestamp(file.fileno(), mtime)
                            current_staging = _identity(staging)
                            if (
                                current_staging is None
                                or current_staging[:3] != staged_identity[:3]
                            ):
                                raise ValueError("Owned staging file changed before publication")
                            checked = PathSafety.safe_join(
                                self.target,
                                *relative.parts,
                                create_base=False,
                                max_depth=self.config.max_depth,
                                max_filename_len=self.config.max_filename_len,
                            )
                            if checked != local or _identity(local) != original:
                                raise ValueError(
                                    "Destination changed during transfer; preserving existing entry"
                                )
                            os.replace(staging, local)
                            return TransferResult(
                                url, local, True, size, strong_etag(headers), digest, mtime
                            )
                except backend.retryable:
                    if attempt == self.config.max_retries:
                        raise
                    logging.warning(
                        "Connection retry for %s (attempt %s)",
                        sanitize_url_for_log(url),
                        attempt + 1,
                    )
                    await asyncio.sleep(self.config.retry_delay * 2**attempt)
            raise RuntimeError("Transfer retry budget exhausted")
        finally:
            self.outstanding.pop(local, None)
            if not self.scratch.release(staging.parent):
                raise ValueError("Owned staging cleanup failed; preserved for investigation")

    async def run(
        self,
        items: List[Tuple[str, Path]],
        published: Optional[Callable[[TransferResult], None]] = None,
    ) -> List[TransferResult]:
        # Revalidate the full mapping, even for library callers passing paths.
        if plan_targets([url for url, _ in items], self.scope, self.target, self.config) != items:
            raise ValueError("Caller-supplied local paths do not match scoped URL paths")
        require_backend(self.config.backend)
        backend = (_AIOHTTPBackend if self.config.backend == "aiohttp" else _HTTPXBackend)(
            self.config, self.concurrency
        )
        iterator, results = iter(items), []

        async def worker():
            for url, local in iterator:
                try:
                    result = await self._one(backend, url, local)
                    if published is not None:
                        published(result)
                except Exception as error:
                    result = TransferResult(url, local, False, error=str(error))
                    logging.error("Download failed for %s: %s", sanitize_url_for_log(url), error)
                results.append(result)

        tasks = [asyncio.create_task(worker()) for _ in range(min(self.concurrency, len(items)))]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await backend.close()
        return results


def _save_receipts(path: Path, data: dict) -> None:
    original = _identity(path)
    descriptor, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            json.dump(data, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        if _identity(path) != original:
            raise ValueError("Receipt file changed during write; preserving it")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def download_url_list(config: MirrorConfig) -> dict:
    """GET the exact list without discovery, HEAD, reuse, resume or cleanup.

    Existing files require a matching prior receipt or explicit overwrite.
    Failed whole-file attempts restart from byte zero. Unknown state is kept.
    """
    if config.mode != "download" or config.url_list is None:
        raise ConfigError("download_url_list requires mode=download and a URL list")
    require_backend(config.backend)
    target = config.dest_path
    for part in filter(None, config.dir_suffix.split("/")):
        target /= PathSafety._safe_filename(part, max_len=config.max_filename_len)
    target = PathSafety._resolve_destination_root(target)
    scope = config.base_url.rstrip("/") + "/"
    if config.dir_suffix:
        scope = urljoin(scope, config.dir_suffix.strip("/") + "/")
    validate_url(scope, config.base_url.rstrip("/") + "/")
    urls = [
        line.strip()
        for line in config.url_list.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not urls:
        raise ValueError("URL list is empty")
    resources = [target, PathSafety._resolve_destination_root(config.log_path)]
    guard = DestinationLock(resources, parent=current_run_lock())
    started = time.perf_counter()
    try:
        with guard.operation():
            items = plan_targets(urls, scope, target, config)
            if config.dry_run:
                return {
                    "success": True,
                    "dry_run": True,
                    "files": len(items),
                    "backend": config.backend,
                }
            target.mkdir(parents=True, exist_ok=True)
            partial = PartialDownloadManager(target)
            state = partial._state_directory()
            receipt_path = state / (
                "url-list-" + hashlib.sha256(scope.encode()).hexdigest()[:16] + ".json"
            )
            receipts: Dict[str, Any] = {
                "format": 1,
                "kind": "url-list-downloads",
                "target": str(target),
                "scope": scope,
                "files": {},
            }
            if receipt_path.exists() or receipt_path.is_symlink():
                _identity(receipt_path)
                previous = json.loads(receipt_path.read_text(encoding="utf-8"))
                if (
                    not isinstance(previous, dict)
                    or any(
                        previous.get(k) != receipts[k]
                        for k in ("format", "kind", "target", "scope")
                    )
                    or not isinstance(previous.get("files"), dict)
                ):
                    raise ValueError("Unrecognized URL-list receipt file; preserving it")
                receipts = previous
            for url, local in items:
                identity = _identity(local)
                if identity is not None and not config.overwrite:
                    old = receipts["files"].get(str(local))
                    if (
                        not isinstance(old, dict)
                        or old.get("url") != url
                        or old.get("identity") != list(identity)
                    ):
                        raise ValueError(
                            "Existing file has no matching ownership receipt; use --overwrite explicitly"
                        )
                    if old.get("sha256") is not None and file_sha256(local) != old["sha256"]:
                        raise ValueError("Existing file no longer matches its SHA-256 receipt")
            scratch = OwnedScratch(target, state, None, guard)
            engine = AsyncTransfers(config, target, scope, scratch)

            def published(result: TransferResult):
                receipts["files"][str(result.path)] = {
                    "url": result.url,
                    "size": result.size,
                    "etag": result.etag,
                    "sha256": result.sha256,
                    "identity": list(_identity(result.path) or ()),
                }

            try:
                results = asyncio.run(engine.run(items, published))
            finally:
                # Retain receipts for already published files even if interrupted.
                _save_receipts(receipt_path, receipts)
            return {
                "success": all(r.success for r in results),
                "backend": config.backend,
                "files_downloaded": sum(r.success for r in results),
                "files_failed": sum(not r.success for r in results),
                "bytes_downloaded": sum(r.size for r in results if r.success),
                "elapsed_seconds": time.perf_counter() - started,
                "pacing_wait_seconds": engine.pacer.wait_seconds,
                "effective_request_interval": config.effective_request_interval,
                "stage_seconds": engine.stage_seconds,
                "receipts": str(receipt_path),
                "failures": [
                    {"url": sanitize_url_for_log(r.url), "error": r.error}
                    for r in results
                    if not r.success
                ],
            }
    finally:
        guard.close()
