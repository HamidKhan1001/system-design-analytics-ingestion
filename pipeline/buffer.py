import asyncio
from collections import deque
from .event import ClickEvent


class EventBuffer:
    def __init__(self, batch_size: int = 100, on_flush=None):
        self.batch_size = batch_size
        self.on_flush = on_flush
        self._buffer: deque = deque()
        self._flushed_batches: list = []
        self._lock = asyncio.Lock()

    async def add(self, event: ClickEvent) -> None:
        async with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= self.batch_size:
                await self._flush_locked()

    async def flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self._buffer:
            return
        batch = list(self._buffer)
        self._buffer.clear()
        self._flushed_batches.append(batch)
        if self.on_flush:
            await self.on_flush(batch)

    @property
    def pending(self) -> int:
        return len(self._buffer)

    @property
    def total_flushed(self) -> int:
        return sum(len(b) for b in self._flushed_batches)
