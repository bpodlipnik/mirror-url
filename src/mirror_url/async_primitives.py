"""Create async synchronization primitives in the loop that actually uses them."""

from __future__ import annotations

import asyncio
from threading import Lock
from typing import Callable, Generic, Optional, TypeVar

_Primitive = TypeVar("_Primitive", asyncio.Lock, asyncio.Semaphore)


class LoopLocalPrimitive(Generic[_Primitive]):
    """Delay loop binding until async use, including on Python 3.9.

    Keep one shared primitive within a loop. A closed loop can be replaced,
    but sharing state across two open loops is unsupported and rejected.
    """

    def __init__(self, factory: Callable[[], _Primitive]):
        self._factory: Callable[[], _Primitive] = factory
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._value: Optional[_Primitive] = None
        self._binding_lock = Lock()

    def get(self) -> _Primitive:
        """Return the primitive belonging to the current running loop."""
        loop = asyncio.get_running_loop()
        with self._binding_lock:
            if loop is not self._loop:
                if self._loop is not None and not self._loop.is_closed():
                    raise RuntimeError("Async primitive cannot be shared across open event loops")
                self._value = self._factory()
                self._loop = loop
            assert self._value is not None
            return self._value


class ResizableSemaphore:
    """Resize admission without discarding existing waiters or active leases."""

    def __init__(self, limit: int):
        self.limit = max(1, limit)
        self.active = 0
        self.condition = asyncio.Condition()

    @property
    def _value(self):
        return max(0, self.limit - self.active)

    async def resize(self, limit: int):
        async with self.condition:
            self.limit = max(1, limit)
            self.condition.notify_all()

    async def __aenter__(self):
        async with self.condition:
            await self.condition.wait_for(lambda: self.active < self.limit)
            self.active += 1
        return self

    async def __aexit__(self, *args):
        async with self.condition:
            self.active -= 1
            self.condition.notify_all()
