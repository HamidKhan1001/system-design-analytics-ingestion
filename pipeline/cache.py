import asyncio
import time
from collections import OrderedDict


class RedisCache:
    """In-process LRU cache simulating Redis for dedup + spike absorption."""

    def __init__(self, max_size: int = 10_000, ttl_seconds: int = 300):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self._store: OrderedDict = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str):
        async with self._lock:
            if key not in self._store:
                return None
            value, exp = self._store[key]
            if time.monotonic() > exp:
                del self._store[key]
                return None
            self._store.move_to_end(key)
            return value

    async def set(self, key: str, value: str, ttl: int = None) -> None:
        async with self._lock:
            exp = time.monotonic() + (ttl or self.ttl_seconds)
            self._store[key] = (value, exp)
            self._store.move_to_end(key)
            if len(self._store) > self.max_size:
                self._store.popitem(last=False)

    async def exists(self, key: str) -> bool:
        return await self.get(key) is not None

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    @property
    def size(self) -> int:
        return len(self._store)
