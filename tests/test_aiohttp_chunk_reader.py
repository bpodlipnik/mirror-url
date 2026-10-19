from __future__ import annotations

import asyncio
import time
from collections import deque
from types import SimpleNamespace

import aiohttp.streams
import httpx
import pytest

from mirror_url.transfers import _AIOHTTPChunkResponse


class Ready:
    def __init__(self, data, error=None):
        self.data = deque(data)
        self.error = error
        self.requests = []

    async def read(self, size):
        self.requests.append(size)
        if self.data:
            block = self.data.popleft()
            if len(block) > size:
                self.data.appendleft(block[size:])
                block = block[:size]
            return block
        if self.error:
            raise self.error
        return b""


class Waiting:
    def __init__(self, hold_cleanup=False):
        self.queue = asyncio.Queue()
        self.entered = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.active = 0
        self.hold_cleanup = hold_cleanup
        self.eof = False

    async def read(self, size):
        # Match StreamReader: EOF stays terminal even when a batch consumed it.
        if self.eof:
            return b""
        self.active += 1
        assert self.active == 1
        try:
            if self.queue.empty():
                self.entered.set()
            block = await self.queue.get()
            assert len(block) <= size
            if not block:
                self.eof = True
            return block
        except asyncio.CancelledError:
            self.cancelled.set()
            if self.hold_cleanup:
                await self.release.wait()
            raise
        finally:
            self.active -= 1


def adapter(reader):
    response = SimpleNamespace(status=206, headers={}, content=reader)
    return _AIOHTTPChunkResponse(
        response,
        asyncio.get_running_loop(),
        (asyncio.TimeoutError,),
        httpx.Request("GET", "https://files.example/a"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 4096, 256 * 1024, 1024**2, 4 * 1024**2])
async def test_buffer_cap_exact_bytes_and_eof(size):
    payload = bytes(range(256)) * (2 if size == 1 else 8193) + b"last"
    reader = Ready([payload[i : i + 8192] for i in range(0, len(payload), 8192)])
    response = adapter(reader)
    blocks = await asyncio.to_thread(lambda: list(response.iter_bytes(size)))
    assert b"".join(blocks) == payload
    assert max(map(len, blocks)) <= min(size, 1024**2)
    assert max(reader.requests) <= min(size, 1024**2)
    if size >= 256 * 1024:
        assert len(blocks) < len(payload) / 8192 / 4


@pytest.mark.asyncio
async def test_empty_response():
    response = adapter(Ready([]))
    assert await asyncio.to_thread(lambda: list(response.iter_bytes())) == []


@pytest.mark.asyncio
async def test_slow_stream_yields_first_bytes_without_waiting_for_full_batch():
    reader = Waiting()
    reader.queue.put_nowait(b"first")
    response = adapter(reader)
    started = time.monotonic()
    assert await asyncio.wait_for(response._read_batch(1024**2), 0.2) == b"first"
    assert time.monotonic() - started < 0.15
    assert reader.cancelled.is_set() and reader.active == 0
    reader.queue.put_nowait(b"second")
    reader.queue.put_nowait(b"")
    assert await response._read_batch(1024**2) == b"second"
    assert reader.active == 0


@pytest.mark.asyncio
async def test_real_read_timeout_is_not_treated_as_batch_deadline():
    response = adapter(Ready([b"prefix"], asyncio.TimeoutError("socket deadline")))
    with pytest.raises(httpx.RequestError, match="socket deadline") as error:
        await asyncio.to_thread(lambda: list(response.iter_bytes(1024**2)))
    assert isinstance(error.value.__cause__, asyncio.TimeoutError)


@pytest.mark.asyncio
@pytest.mark.parametrize("first", [False, True])
async def test_repeated_cancellation_drains_pending_reader(first):
    reader = Waiting(hold_cleanup=True)
    if first:
        reader.queue.put_nowait(b"first")
    response = adapter(reader)
    baseline = set(asyncio.all_tasks())
    task = asyncio.create_task(response._read_batch(1024**2))
    await asyncio.wait_for(reader.entered.wait(), 0.2)
    task.cancel()
    await asyncio.wait_for(reader.cancelled.wait(), 0.2)
    task.cancel()
    reader.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert reader.active == 0
    assert set(asyncio.all_tasks()) == baseline


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0.008, 0.010, 0.012])
async def test_data_near_deadline_is_never_lost(delay):
    reader = Waiting()
    response = adapter(reader)
    expected = []

    async def feed():
        for index in range(12):
            data = bytes([65 + index]) * 100
            expected.append(data)
            await reader.queue.put(data)
            await asyncio.sleep(delay)
        await reader.queue.put(b"")

    feed_task = asyncio.create_task(feed())
    actual = []
    while block := await response._read_batch(1024**2):
        actual.append(block)
    await feed_task
    assert b"".join(actual) == b"".join(expected)
    assert reader.active == 0


def setup_reader():
    class Protocol:
        connected = True
        _reading_paused = False

        def pause_reading(self):
            self._reading_paused = True

        def resume_reading(self, *, resume_parser=True):
            self._reading_paused = False

    loop = asyncio.get_running_loop()
    reader = aiohttp.streams.StreamReader(Protocol(), 64 * 1024, loop=loop)
    response = SimpleNamespace(content=reader, status=206, headers={})
    adapter = _AIOHTTPChunkResponse(
        response, loop, (asyncio.TimeoutError,), httpx.Request("GET", "https://files.example/a")
    )
    return reader, adapter


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0.008, 0.010, 0.012])
async def test_real_reader_deadline_cancellation_preserves_every_byte(delay):
    reader, response = setup_reader()
    expected = []

    async def feed():
        for i in range(20):
            data = bytes([i]) * 8192
            expected.append(data)
            reader.feed_data(data)
            await asyncio.sleep(delay)
        reader.feed_eof()

    before = set(asyncio.all_tasks())
    producer = asyncio.create_task(feed())
    received = []
    while block := await response._read_batch(1024**2):
        received.append(block)
    await producer
    assert b"".join(received) == b"".join(expected)
    assert reader.at_eof() and reader._waiter is None
    assert set(asyncio.all_tasks()) == before


@pytest.mark.asyncio
async def test_real_reader_set_exception_preserves_timeout_contract():
    reader, response = setup_reader()
    reader.feed_data(b"first")

    async def fail():
        await asyncio.sleep(0.002)
        reader.set_exception(asyncio.TimeoutError("original socket timeout"))

    task = asyncio.create_task(fail())
    with pytest.raises(httpx.RequestError, match="original socket timeout") as error:
        await asyncio.to_thread(lambda: list(response.iter_bytes(1024**2)))
    await task
    assert isinstance(error.value.__cause__, asyncio.TimeoutError)
    assert reader._waiter is None


@pytest.mark.asyncio
async def test_real_reader_cancel_removes_waiter_and_allows_next_read():
    reader, response = setup_reader()
    reader.feed_data(b"first")
    before = set(asyncio.all_tasks())
    task = asyncio.create_task(response._read_batch(1024**2))
    for _ in range(20):
        if reader._waiter is not None:
            break
        await asyncio.sleep(0)
    assert reader._waiter is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert reader._waiter is None
    reader.feed_data(b"next")
    reader.feed_eof()
    assert await response._read_batch(1024**2) == b"next"
    assert set(asyncio.all_tasks()) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [64 * 1024, 64 * 1024 + 1, 256 * 1024, 1024**2])
async def test_large_first_read_returns_unchanged_without_another_read(length, monkeypatch):
    first = b"a" * length
    reader = Ready([first, b"later"])
    response = adapter(reader)

    def unexpected_task(*args, **kwargs):
        raise AssertionError("A large first read must not start a batching task")

    monkeypatch.setattr(asyncio, "create_task", unexpected_task)
    assert await response._read_batch(1024**2) is first
    assert reader.requests == [1024**2]
    assert list(reader.data) == [b"later"]


@pytest.mark.asyncio
async def test_first_read_below_threshold_still_batches():
    first = b"a" * (64 * 1024 - 1)
    reader = Ready([first, b"tail"])
    assert await adapter(reader)._read_batch(1024**2) == first + b"tail"
    assert len(reader.requests) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("length", [64 * 1024, 128 * 1024])
async def test_real_large_first_read_does_not_wait_for_more_data(length):
    reader, response = setup_reader()
    before = set(asyncio.all_tasks())
    first = b"a" * length
    reader.feed_data(first)
    assert await response._read_batch(1024**2) == first
    assert reader._waiter is None and not reader.at_eof()
    assert set(asyncio.all_tasks()) == before
    reader.feed_data(b"tail")
    reader.feed_eof()
    assert await response._read_batch(1024**2) == b"tail"
    assert await response._read_batch(1024**2) == b""
    assert reader._waiter is None and set(asyncio.all_tasks()) == before


@pytest.mark.asyncio
async def test_timeout_after_large_first_read_is_reported_on_next_iteration():
    first = b"a" * (64 * 1024)
    response = adapter(Ready([first], asyncio.TimeoutError("later socket deadline")))
    iterator = response.iter_bytes(1024**2)
    assert await asyncio.to_thread(next, iterator) == first
    with pytest.raises(httpx.RequestError, match="later socket deadline") as error:
        await asyncio.to_thread(next, iterator)
    assert isinstance(error.value.__cause__, asyncio.TimeoutError)
