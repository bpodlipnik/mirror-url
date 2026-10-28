"""Bounded transfers with shared publication and URL-list policy.

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
import threading
import time
from concurrent.futures import wait
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import httpx

from .config import MirrorConfig, _root_relative_file_path
from .destination_lock import DestinationLock, current_run_lock
from .download import ParallelDownloadManager, PartialDownloadManager
from .download_integrity import content_length, file_sha256, replace_verified_file, strong_etag
from .download_integrity import file_identity as _identity
from .exceptions import ConfigError, SecurityError, URLScopeError
from .filename_mapping import FilenameMap, _stat_identity, selected_directory_targets
from .health import DownloadHealthChecker, HealthCheckServer
from .metrics import (
    MetricsCollector,
    log_file_download_plan,
    log_file_downloaded,
    log_file_downloads_completed,
    log_whole_file_fallback,
)
from .progress import DownloadProgress
from .run_report import RunReport
from .scratch import OwnedScratch
from .security import PathSafety, SecurityValidator
from .transport import SecureAsyncTransport, SecureTransport
from .utils import _relative_url_path, sanitize_log_text, sanitize_url_for_log, url_within_scope


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
            allow_leaf_symlink=True,
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


def resolve_download_files(entries: Iterable[str], root: str) -> List[str]:
    """Resolve literal file paths against --url, retaining absolute URL compatibility."""
    parsed = urlsplit(root)
    base = urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/") + "/", "", ""))
    urls = []
    for entry in entries:
        if not isinstance(entry, str) or not entry.strip():
            raise ConfigError("download files require nonempty strings")
        entry = entry.strip()
        if entry.startswith(("http://", "https://")):
            urls.append(entry)
        else:
            path = _root_relative_file_path(entry, "download files")
            # Quote literal names before joining: ?, # and % belong to names,
            # and the shared validator rejects traversal before URL normalization.
            urls.append(base + quote(path, safe="/"))
    return urls


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
        raise ConfigError(
            "Missing required dependency aiohttp; repair the installation with "
            "python -m pip install --upgrade mirror-url"
        ) from error
    return aiohttp


async def _drain_operation(awaitable, on_cancel=None):
    """Keep resource cleanup owned until it finishes, including repeated cancellation."""
    operation = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(operation)
    except asyncio.CancelledError:
        if on_cancel is not None:
            on_cancel()
        while not operation.done():
            try:
                await asyncio.shield(operation)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        operation.exception()
        raise


class _HTTPXBackend:
    retryable = (httpx.TransportError, asyncio.TimeoutError)

    def __init__(self, config: MirrorConfig, concurrency: int):
        limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
        self.sync_client: Optional[httpx.Client] = None
        self.client: Optional[httpx.AsyncClient] = None
        if concurrency == 1 or config.parallel_downloads or config.streaming_parallel:
            self.sync_client = httpx.Client(
                transport=SecureTransport(http2=config.http2, limits=limits),
                http2=config.http2,
                limits=limits,
                timeout=config.timeout,
                trust_env=False,
                follow_redirects=False,
            )
        else:
            self.client = httpx.AsyncClient(
                transport=SecureAsyncTransport(http2=config.http2, limits=limits),
                http2=config.http2,
                limits=limits,
                timeout=config.timeout,
                trust_env=False,
                follow_redirects=False,
            )

    @staticmethod
    async def _thread_call(
        call: Callable[[], Any], on_cancel: Optional[Callable[[], None]] = None
    ) -> Any:
        # Cancellation cannot abandon an in-flight reader while its response
        # or owned staging is being closed. Drain the bounded network operation
        # first, including repeated cancellation during worker shutdown.
        return await _drain_operation(asyncio.to_thread(call), on_cancel)

    async def close(self) -> None:
        if self.sync_client is not None:
            await self._thread_call(self.sync_client.close)
        else:
            assert self.client is not None
            await self.client.aclose()

    def chunk_request(self, url, method, headers, timeout, loop):
        assert self.sync_client is not None
        request = self.sync_client.build_request(method, url, headers=headers, timeout=timeout)
        return self.sync_client.send(request, stream=True, follow_redirects=False)

    @asynccontextmanager
    async def _open_single(self, url: str):
        assert self.sync_client is not None
        stream = self.sync_client.stream("GET", url, headers={"Accept-Encoding": "identity"})
        response = None

        def enter():
            nonlocal response
            response = stream.__enter__()

        try:
            await self._thread_call(enter)
            assert response is not None
            reader = response.iter_raw()

            def read_batch():
                # Amortize thread handoffs and file writes, with bounded memory.
                # The short batching window also returns slow/trickling streams
                # without waiting to fill a complete 256 KiB block.
                blocks, size = [], 0
                deadline = time.monotonic() + 0.01
                while size < 256 * 1024:
                    block = next(reader, None)
                    if block is None:
                        break
                    blocks.append(block)
                    size += len(block)
                    if time.monotonic() >= deadline:
                        break
                return b"".join(blocks)

            async def chunks():
                while block := await self._thread_call(read_batch):
                    yield block

            yield response.status_code, response.headers, chunks()
        finally:
            if response is not None:
                await self._thread_call(lambda: stream.__exit__(None, None, None))

    @asynccontextmanager
    async def open(self, url: str):
        if self.sync_client is not None:
            async with self._open_single(url) as response:
                yield response
        else:
            assert self.client is not None
            async with self.client.stream("GET", url, headers={"Accept-Encoding": "identity"}) as r:
                yield r.status_code, r.headers, r.aiter_raw()


def _aiohttp_chunk_call(loop, operation, retryable, request):
    """Bridge a chunk worker's bounded IO to its session's existing event loop."""
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if running is not None:
        raise RuntimeError("Chunk IO must run in a worker thread")
    coroutine = operation()
    try:
        pending = asyncio.run_coroutine_threadsafe(coroutine, loop)
    except BaseException:
        coroutine.close()
        raise
    try:
        return pending.result()
    except retryable as error:
        # The shared range engine retries transport failures through this
        # existing exception contract; representation failures remain distinct.
        raise httpx.RequestError(str(error), request=request) from error


class _AIOHTTPChunkResponse:
    """Expose the shared range response interface without buffering a file."""

    def __init__(self, response, loop, retryable, request):
        self.response, self.loop, self.retryable, self.request = response, loop, retryable, request
        self.status_code = response.status
        self.headers = response.headers

    def _call(self, operation):
        return _aiohttp_chunk_call(self.loop, operation, self.retryable, self.request)

    def read(self) -> bytes:
        if self.request.method != "HEAD":
            raise ValueError("Non-streaming chunk metadata requests require HEAD")
        return self._call(self.response.read)

    async def _read_batch(self, size: int) -> bytes:
        """Collect at most one bounded batch, yielding promptly on a trickle."""
        first = await self.response.content.read(size)
        if not first:
            return b""
        if len(first) >= 64 * 1024:
            return first
        blocks, received = [first], len(first)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 0.01
        while received < size:
            remaining = deadline - loop.time()
            if remaining <= 0:
                break
            pending = asyncio.create_task(self.response.content.read(size - received))
            try:
                done, _ = await asyncio.wait((pending,), timeout=remaining)
            except BaseException:
                pending.cancel()
                try:
                    await _drain_operation(asyncio.gather(pending, return_exceptions=True))
                except asyncio.CancelledError:
                    pass
                raise
            if done:
                # Preserve a genuine reader exception, including socket timeout.
                block = pending.result()
            else:
                pending.cancel()
                settled = await _drain_operation(asyncio.gather(pending, return_exceptions=True))
                block = settled[0]
                if isinstance(block, asyncio.CancelledError):
                    break
                if isinstance(block, BaseException):
                    raise block
            if not block:
                break
            blocks.append(block)
            received += len(block)
            if not done:
                # Data won the cancellation race. Retain it, then return.
                break
        return b"".join(blocks)

    def iter_bytes(self, chunk_size: Optional[int] = None) -> Iterator[bytes]:
        size = min(chunk_size or 256 * 1024, 1024**2)
        if size <= 0:
            raise ValueError("Chunk buffer size must be positive")
        while block := self._call(lambda: self._read_batch(size)):
            yield block

    def iter_raw(self, chunk_size: Optional[int] = None) -> Iterator[bytes]:
        return self.iter_bytes(chunk_size)

    def close(self) -> None:
        async def close():
            self.response.close()
            await self.response.wait_for_close()

        self._call(close)


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

    def chunk_request(self, url, method, headers, timeout, loop):
        from yarl import URL

        aiohttp = require_backend("aiohttp")
        timeout = timeout if isinstance(timeout, httpx.Timeout) else httpx.Timeout(timeout)

        async def request():
            return await self.client.request(
                method,
                URL(str(httpx.URL(url)), encoded=True),
                headers=headers,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(
                    total=None, connect=timeout.connect, sock_read=timeout.read
                ),
            )

        context = httpx.Request(method, url)
        response = _aiohttp_chunk_call(loop, request, self.retryable, context)
        return _AIOHTTPChunkResponse(response, loop, self.retryable, context)

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


class _ChunkConnection:
    """Adapt the existing chunk engine to the known-URL client's scoped request budget."""

    def __init__(self, backend, engine: AsyncTransfers):
        self.backend, self.engine = backend, engine
        self.loop = asyncio.get_running_loop()

    def request(self, url, method="GET", stream=False, headers=None, **options):
        for hop in range(11):
            validate_url(url, self.engine.scope)
            asyncio.run_coroutine_threadsafe(self.engine.pacer.wait(), self.loop).result()
            response = self.backend.chunk_request(
                url,
                method,
                headers,
                options.get("timeout", self.engine.config.timeout),
                self.loop,
            )
            if response.status_code in (301, 302, 303, 307, 308):
                location = response.headers.get("Location")
                response.close()
                if hop == 10 or not location:
                    raise ValueError("Missing redirect target or too many redirects")
                url = urljoin(url, location)
                continue
            if not stream:
                try:
                    response.read()
                except BaseException:
                    response.close()
                    raise
            return response
        raise RuntimeError("Redirect budget exhausted")  # pragma: no cover

    def throttle(self, bytes_count):
        if self.engine.bandwidth.bytes_per_second:
            asyncio.run_coroutine_threadsafe(
                self.engine.bandwidth.wait(bytes_count), self.loop
            ).result()


class AsyncTransfers:
    """A pooled client and a fixed worker set for one complete transfer batch."""

    def __init__(
        self,
        config: MirrorConfig,
        target: Path,
        scope: str,
        scratch: OwnedScratch,
        progress: Optional[DownloadProgress] = None,
        *,
        metrics: Optional[MetricsCollector] = None,
    ):
        self.config, self.target, self.scope, self.scratch = config, target, scope, scratch
        self.concurrency = 1 if config.sequential_downloads else config.max_concurrent_downloads
        self.pacer = AsyncRequestPacer(config.effective_request_interval)
        self.bandwidth = AsyncBandwidthBudget(config.bandwidth_limit)
        self._space_remaining: Optional[int] = None
        self._space_reserved: Dict[Path, int] = {}
        self.stage_seconds = {"fsync": 0.0, "hash": 0.0}
        self._stage_lock = threading.Lock()
        self.progress = progress or DownloadProgress(False)
        self.closed_connection_pools = 0
        self.metrics = metrics or MetricsCollector()

    def _reserve_space(self, local: Path, size: int) -> None:
        # One filesystem query per batch, before its first payload write.
        # This is an internal budget, not an OS allocation: write/fsync errors
        # still protect publication if another process consumes the free space.
        if self._space_remaining is None:
            # Leave the budget closed if the single filesystem query fails.
            self._space_remaining = -self.concurrency * 1024 * 1024
            self._space_remaining += shutil.disk_usage(self.target).free
        if self._space_remaining < size:
            raise OSError("Insufficient free space for active transfers and write headroom")
        self._space_remaining -= size
        self._space_reserved[local] = self._space_reserved.get(local, 0) + size

    def _release_space(self, local: Path, retained: int = 0) -> None:
        reserved = self._space_reserved.pop(local, 0)
        if self._space_remaining is not None:
            # Published bytes remain charged for the rest of the batch. Only
            # proven removal of owned staging returns a failed attempt's budget.
            self._space_remaining += reserved - retained

    def _record_stage(self, stage: str, started: float) -> None:
        elapsed = time.perf_counter() - started
        with self._stage_lock:
            self.stage_seconds[stage] += elapsed

    def _publish(
        self, url, local, staging, size, etag, mtime, original, inode, *, cancelled=None
    ) -> TransferResult:
        """Use the same identity, hash, timestamp and atomic-publication checks for every mode."""
        if staging.stat().st_size != size:
            raise ValueError("Staged length does not match received bytes")
        staged_identity = _identity(staging)
        if staged_identity is None or staged_identity[:2] != inode:
            raise ValueError("Owned staging file changed during transfer")
        self.progress.stage(
            local, "hashing (SHA-256)" if self.config.verify_content else "publishing"
        )
        started = time.perf_counter()
        with (
            self.metrics.run_report.transfer_stage("hash")
            if self.config.verify_content
            else nullcontext()
        ):
            digest = file_sha256(staging, self.config.verify_content)
        self._record_stage("hash", started)
        self.progress.stage(local, "publishing")
        if _identity(staging) != staged_identity:
            raise ValueError("Owned staging file changed during hashing")
        if mtime is not None:
            with staging.open("r+b") as file:
                info = os.fstat(file.fileno())
                opened_identity = _stat_identity(info)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_nlink != 1
                    or opened_identity != staged_identity
                ):
                    raise ValueError("Owned staging file changed before timestamp update")
                _set_file_timestamp(file.fileno(), mtime)
        current_staging = _identity(staging)
        if current_staging is None or current_staging[:3] != staged_identity[:3]:
            raise ValueError("Owned staging file changed before publication")
        replace_verified_file(
            staging,
            local,
            target=self.target,
            original=original,
            staged_identity=current_staging,
            max_depth=self.config.max_depth,
            max_filename_len=self.config.max_filename_len,
            cancelled=cancelled,
        )
        return TransferResult(url, local, True, size, etag, digest, mtime)

    async def _one_chunked(self, manager, connection, backend, url, local, published):
        original = _identity(local, allow_symlink=True)
        checked = PathSafety.safe_join(
            self.target,
            *local.relative_to(self.target).parts,
            create_base=False,
            max_depth=self.config.max_depth,
            max_filename_len=self.config.max_filename_len,
            allow_leaf_symlink=True,
        )
        if not PathSafety.same_resolved_path(checked, local):
            raise ValueError("Destination path changed before transfer")
        local.parent.mkdir(parents=True, exist_ok=True)
        self.progress.stage(local, "checking range support")
        probe_response = []

        def probe():
            for attempt in range(self.config.max_retries + 1):
                try:
                    with self.metrics.run_report.transfer_stage("range_metadata"):
                        response = connection.request(
                            url, method="HEAD", headers={"Accept-Encoding": "identity"}
                        )
                    probe_response.append(response)
                    return response
                except (httpx.RequestError, OSError):
                    if attempt == self.config.max_retries:
                        raise
                    time.sleep(self.config.retry_delay * 2**attempt)

        try:
            response = await _HTTPXBackend._thread_call(probe)
        except BaseException:
            if probe_response:
                await _HTTPXBackend._thread_call(probe_response[0].close)
            raise
        try:
            size = content_length(response.headers)
            chunk_count = (
                manager.get_chunk_count(size)
                if response.status_code == 200 and size is not None
                else 0
            )
            if (
                response.status_code != 200
                or size is None
                or chunk_count <= 1
                or response.headers.get("Accept-Ranges", "").lower() != "bytes"
                or strong_etag(response.headers) is None
            ):
                log_whole_file_fallback(
                    url, manager.whole_file_fallback_reason(size, chunk_count, response=response)
                )
                return await self._one(backend, url, local, published)
            self._reserve_space(local, size * (1 if manager.use_streaming else 2))
            cancelled = threading.Event()
            result = None

            def publish(download, staging):
                nonlocal result
                if cancelled.is_set():
                    raise ValueError("Chunk transfer cancelled before publication")
                info = staging.lstat()
                result = self._publish(
                    url,
                    local,
                    staging,
                    size,
                    download.server_etag,
                    download.server_last_modified,
                    original,
                    (info.st_dev, info.st_ino),
                    cancelled=cancelled,
                )
                # The thread can finish during cancellation. Record publication
                # before returning to the event loop so its receipt is retained.
                if published is not None:
                    published(result)
                return result.sha256

            # Each file carries its publication policy; other workers may be
            # creating or finishing their own plans concurrently.
            download = None
            workspaces = []
            try:
                download = manager.create_chunks(url, local, size, response=response)
                if download is None:
                    raise ValueError("Range plan could not be created")
                download.publish = publish
                workspaces = [
                    p
                    for p in (
                        download.temp_dir,
                        download.staging_path.parent if download.staging_path else None,
                    )
                    if p
                ]
                if manager.use_streaming and download.staging_path is None:
                    self._reserve_space(local, size)

                accepted = await _HTTPXBackend._thread_call(
                    lambda: manager.download_parallel(download), on_cancel=cancelled.set
                )
                if not accepted or result is None:
                    raise ValueError("Verified parallel chunk transfer failed")
                return result
            finally:
                if download is not None:
                    await _HTTPXBackend._thread_call(lambda: wait(download.futures))
                    manager.cleanup_chunks(download)
                    if download.assembly_path is not None:
                        workspaces.append(download.assembly_path.parent)
                if any(work.exists() for work in workspaces):
                    raise ValueError("Owned chunk cleanup failed; preserved for investigation")
                self._release_space(local, size if result is not None else 0)
        finally:
            await _HTTPXBackend._thread_call(response.close)

    async def _one(self, backend, url: str, local: Path, published=None) -> TransferResult:
        staging = None
        published_size = None

        def prepare():
            nonlocal staging
            original = _identity(local, allow_symlink=True)
            relative = local.relative_to(self.target)
            if not PathSafety.same_resolved_path(
                PathSafety.safe_join(
                    self.target,
                    *relative.parts,
                    create_base=False,
                    max_depth=self.config.max_depth,
                    max_filename_len=self.config.max_filename_len,
                    allow_leaf_symlink=True,
                ),
                local,
            ):
                raise ValueError("Destination path changed before transfer")
            local.parent.mkdir(parents=True, exist_ok=True)
            staging = self.scratch.staging(local, "streaming")
            return original

        try:
            # Each file worker owns at most one disk operation at a time. Drain
            # it on cancellation before retiring staging or destination locks.
            original = await _drain_operation(asyncio.to_thread(prepare))
            assert staging is not None
            for attempt in range(self.config.max_retries + 1):
                self.progress.begin(local, "waiting for response")
                try:
                    current = url
                    for hop in range(11):
                        validate_url(current, self.scope)
                        await self.pacer.wait()
                        async with AsyncExitStack() as responses:
                            with self.metrics.run_report.transfer_stage("transfer"):
                                code, headers, chunks = await responses.enter_async_context(
                                    backend.open(current)
                                )
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
                            self.progress.received(local, 0, expected)
                            # Each retry gets a new file in the same owned workspace.
                            if staging.exists():
                                _identity(staging)
                                staging.unlink()
                                self._release_space(local)
                            self._reserve_space(local, expected or 0)
                            size = 0
                            with staging.open("xb") as file:
                                staged_inode = os.fstat(file.fileno())
                                with self.metrics.run_report.transfer_stage("transfer"):
                                    async for block in chunks:
                                        size += len(block)
                                        if expected is not None and size > expected:
                                            raise ValueError("Response exceeds Content-Length")
                                        if expected is None:
                                            self._reserve_space(local, len(block))
                                        file.write(block)
                                        self.progress.received(local, size, expected)
                                        await self.bandwidth.wait(len(block))
                                if expected is not None and size != expected:
                                    raise ValueError(
                                        "Response length does not match Content-Length"
                                    )
                                cancelled = threading.Event()

                                def finalize(file, headers, size, staged_inode, cancelled):
                                    nonlocal published_size
                                    self.progress.stage(local, "flushing file")
                                    started = time.perf_counter()
                                    with self.metrics.run_report.transfer_stage("flush"):
                                        file.flush()
                                        os.fsync(file.fileno())
                                        self._record_stage("fsync", started)
                                    file.close()
                                    mtime = None
                                    if headers.get("Last-Modified"):
                                        try:
                                            mtime = parsedate_to_datetime(
                                                headers["Last-Modified"]
                                            ).timestamp()
                                        except (ValueError, TypeError, OverflowError):
                                            pass
                                    result = self._publish(
                                        url,
                                        local,
                                        staging,
                                        size,
                                        # Weak validators are valid for freshness,
                                        # but remain forbidden for byte ranges.
                                        headers.get("ETag"),
                                        mtime,
                                        original,
                                        (staged_inode.st_dev, staged_inode.st_ino),
                                        cancelled=cancelled,
                                    )
                                    published_size = size
                                    # Publication can win a cancellation race.
                                    # Keep its receipt before returning to the loop.
                                    if published is not None:
                                        published(result)
                                    return result

                                result = await _drain_operation(
                                    asyncio.to_thread(
                                        finalize, file, headers, size, staged_inode, cancelled
                                    ),
                                    cancelled.set,
                                )
                            log_file_downloaded(local, size, result.sha256)
                            return result
                except backend.retryable:
                    # A receipt-hook failure after publication cannot retry its GET.
                    if published_size is not None or attempt == self.config.max_retries:
                        raise
                    logging.warning(
                        "Connection retry for %s (attempt %s)",
                        sanitize_url_for_log(url),
                        attempt + 1,
                    )
                    self.progress.stage(local, "waiting to retry")
                    await asyncio.sleep(self.config.retry_delay * 2**attempt)
            raise RuntimeError("Transfer retry budget exhausted")
        finally:
            if staging is not None and not await _drain_operation(
                asyncio.to_thread(self.scratch.release, staging.parent)
            ):
                raise ValueError("Owned staging cleanup failed; preserved for investigation")
            self._release_space(local, published_size or 0)

    async def run(
        self,
        items: List[Tuple[str, Path]],
        published: Optional[Callable[[TransferResult], None]] = None,
    ) -> List[TransferResult]:
        # Revalidate the full mapping, even for library callers passing paths.
        if plan_targets([url for url, _ in items], self.scope, self.target, self.config) != items:
            raise ValueError("Caller-supplied local paths do not match scoped URL paths")
        require_backend(self.config.backend)
        self._space_remaining = None
        self._space_reserved.clear()
        workers = min(self.concurrency, len(items))
        log_file_download_plan(len(items), workers)
        chunked = self.config.parallel_downloads or self.config.streaming_parallel
        backend = (_AIOHTTPBackend if self.config.backend == "aiohttp" else _HTTPXBackend)(
            self.config, max(workers, 1) + (self.config.max_parallel_chunks_total if chunked else 0)
        )
        manager = connection = None
        iterator, results = iter(items), []

        async def worker():
            for url, local in iterator:
                self.progress.begin(local)
                try:
                    if manager is not None:
                        result = await self._one_chunked(
                            manager, connection, backend, url, local, published
                        )
                    else:
                        result = await self._one(backend, url, local, published)
                except Exception as error:
                    result = TransferResult(url, local, False, error=sanitize_log_text(str(error)))
                    logging.error(
                        "Download failed for %s: %s", sanitize_url_for_log(url), result.error
                    )
                results.append(result)
                if self.config.mode == "download":
                    self.metrics.increment_batch(
                        {
                            "files_downloaded": int(result.success),
                            "files_failed": int(not result.success),
                            "bytes_downloaded": result.size if result.success else 0,
                        }
                    )
                self.progress.finish(local, result.success)

        tasks = []
        try:
            if chunked:
                connection = _ChunkConnection(backend, self)
                manager = ParallelDownloadManager(
                    self.config,
                    self.metrics,
                    connection,
                    connection,
                    scratch=self.scratch,
                    progress=self.progress,
                    record_stage=self._record_stage,
                )
            tasks = [asyncio.create_task(worker()) for _ in range(workers)]
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()

            async def close_backend():
                await backend.close()
                self.closed_connection_pools += 1

            try:
                await _drain_operation(asyncio.gather(*tasks, return_exceptions=True))
            finally:
                try:
                    if manager is not None:
                        await _HTTPXBackend._thread_call(manager.shutdown)
                finally:
                    await _drain_operation(close_backend())
        log_file_downloads_completed(
            sum(result.success for result in results), sum(not result.success for result in results)
        )
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


def download_url_list(
    config: MirrorConfig,
    *,
    run_report: Optional[RunReport] = None,
    metrics: Optional[MetricsCollector] = None,
) -> dict:
    """GET exact URLs without discovery/freshness; explicit chunk modes probe range metadata.

    Existing files require a matching prior receipt or explicit overwrite.
    Failed whole-file attempts restart from byte zero. Unknown state is kept.
    """
    sources = sum(
        (config.url_list is not None, config.download_url is not None, bool(config.download_files))
    )
    if config.mode != "download" or sources != 1:
        raise ConfigError("download_url_list requires mode=download and exactly one URL source")
    if not isinstance(config.download_files, list):
        raise ConfigError("download_files requires a list of file paths or URLs")
    require_backend(config.backend)
    target = selected_directory_targets(
        config.dest_path, [config.dir_suffix], max_filename_len=config.max_filename_len
    )[0]
    parsed = urlsplit(config.base_url)
    base = parsed._replace(path=parsed.path.rstrip("/") + "/", query="", fragment="")
    scope = base.geturl()
    if config.dir_suffix:
        scope = base._replace(
            path=base.path + quote(config.dir_suffix.strip("/"), safe="/") + "/"
        ).geturl()
    validate_url(scope, base.geturl())
    if config.download_url is not None:
        urls = [config.download_url]
    else:
        entries = config.download_files
        if config.url_list is not None:
            entries = [
                line.strip()
                for line in config.url_list.read_text(encoding="utf-8-sig").splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            ]
        urls = resolve_download_files(entries, config.base_url)
    if not urls:
        raise ValueError("URL list is empty")
    # This API writes its state inside the destination; its CLI caller owns
    # the actual log file. An otherwise unused log directory is not a resource.
    resources = [target]
    metrics_output = None
    if config.metrics_json is not None:
        metrics_output = PathSafety._resolve_destination_root(config.metrics_json)
        if metrics_output.is_dir() or metrics_output in target.parents or metrics_output == target:
            raise ConfigError("Metrics output must name a file, not a destination directory")
        resources.append(metrics_output)
    guard = DestinationLock(resources, parent=current_run_lock())
    metrics = metrics or MetricsCollector()
    report = run_report if run_report is not None else metrics.run_report
    metrics.run_report = report
    report.start(metrics.metrics, 0, verify_content=config.verify_content)
    report.begin_phase("download preparation")
    outcome: Optional[dict] = None
    health_server = None
    metrics_ready = False
    try:
        with (
            guard.operation(),
            DownloadProgress(config.print_logs and not config.quiet, config) as progress,
        ):
            items = plan_targets(urls, scope, target, config)
            if metrics_output is not None:
                metrics.protect_output(metrics_output, target, (local for _, local in items))
                metrics_ready = True
            progress.plan(len(items))
            report.remote_files = len(items)
            if config.dry_run:
                outcome = {
                    "success": True,
                    "dry_run": True,
                    "files": len(items),
                    "backend": config.backend,
                }
                return outcome
            if config.metrics_json is not None:
                health_server = HealthCheckServer(
                    DownloadHealthChecker(metrics), port=config.health_check_port
                )
                health_server.start()
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
                identity = _identity(local, allow_symlink=True)
                report.record_check(url, "missing" if identity is None else "unclassified")
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
                    if (
                        config.verify_content
                        and old.get("sha256") is not None
                        and file_sha256(local) != old["sha256"]
                    ):
                        raise ValueError("Existing file no longer matches its SHA-256 receipt")
            scratch = OwnedScratch(target, state, None, guard)
            engine = AsyncTransfers(config, target, scope, scratch, progress, metrics=metrics)

            def published(result: TransferResult):
                receipts["files"][str(result.path)] = {
                    "url": result.url,
                    "size": result.size,
                    "etag": result.etag,
                    "sha256": result.sha256,
                    "identity": list(_identity(result.path) or ()),
                }
                report.record_publication(result.url, sha256=result.sha256)

            report.begin_phase("downloads")
            try:
                results = asyncio.run(engine.run(items, published))
            finally:
                # Retain receipts for already published files even if interrupted.
                report.closed_connection_pools += engine.closed_connection_pools
                report.begin_phase("cleanup")
                progress.saving_receipts()
                _save_receipts(receipt_path, receipts)
            outcome = {
                "success": all(r.success for r in results),
                "backend": config.backend,
                "files_downloaded": sum(r.success for r in results),
                "files_failed": sum(not r.success for r in results),
                "bytes_downloaded": sum(r.size for r in results if r.success),
                "sha256_hashes_recorded": report.snapshot()["sha256_hashes_recorded"],
                "elapsed_seconds": 0.0,  # Finalized after timer and ownership cleanup below.
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
            if config.parallel_downloads or config.streaming_parallel:
                outcome["chunk_metrics"] = metrics.chunk_summary()
            return outcome
    except BaseException:
        # Context cleanup can fail after a successful worker result was built.
        outcome = None
        raise
    finally:
        report.begin_phase("cleanup")
        released = False
        try:
            if health_server is not None:
                health_server.stop()
        except BaseException:
            outcome = None
            raise
        finally:

            def export_metrics() -> None:
                if (
                    metrics_ready
                    and metrics_output is not None
                    and not config.dry_run
                    and not report.job_active
                ):
                    report.finish(
                        "success" if outcome is not None and outcome["success"] else "failed"
                    )
                    metrics.export_json(metrics_output, config)

            try:
                guard.close(export_metrics)
                released = True
            finally:
                status = "failed"
                if released and outcome is not None and outcome["success"]:
                    status = "dry run" if outcome.get("dry_run") else "success"
                report.finish(status)
                if not released:
                    # An API export may have frozen the timer before ownership
                    # release. A subsequent release failure still fails the run.
                    report.status = "failed"
                if outcome is not None and not outcome.get("dry_run"):
                    run = report.snapshot()
                    outcome["elapsed_seconds"] = run["elapsed_seconds"]
                    outcome["duration_scope"] = run["duration_scope"]
                    outcome["transfer_stage_seconds"] = run["transfer_stage_seconds"]
                    outcome["transfer_stage_operations"] = run["transfer_stage_operations"]
