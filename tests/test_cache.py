import pytest
from pipeline import RedisCache


@pytest.mark.asyncio
async def test_set_and_get(cache):
    await cache.set("k1", "v1")
    assert await cache.get("k1") == "v1"


@pytest.mark.asyncio
async def test_missing_returns_none(cache):
    assert await cache.get("no-such-key") is None


@pytest.mark.asyncio
async def test_exists(cache):
    await cache.set("k2", "v2")
    assert await cache.exists("k2") is True
    assert await cache.exists("missing") is False


@pytest.mark.asyncio
async def test_delete(cache):
    await cache.set("k3", "v3")
    await cache.delete("k3")
    assert await cache.get("k3") is None


@pytest.mark.asyncio
async def test_lru_eviction():
    c = RedisCache(max_size=3)
    for i in range(4):
        await c.set(f"k{i}", f"v{i}")
    assert c.size == 3
