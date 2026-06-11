import asyncio
import pytest
from pipeline import EventBuffer, ClickEvent


@pytest.mark.asyncio
async def test_flush_on_batch_size():
    flushed = []

    async def on_flush(batch):
        flushed.append(batch)

    buf = EventBuffer(batch_size=3, on_flush=on_flush)
    for i in range(3):
        await buf.add(ClickEvent("click", f"u{i}", "s", "/"))
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_manual_flush():
    buf = EventBuffer(batch_size=100)
    await buf.add(ClickEvent("click", "u1", "s", "/"))
    assert buf.pending == 1
    await buf.flush()
    assert buf.pending == 0
    assert buf.total_flushed == 1


@pytest.mark.asyncio
async def test_empty_flush_no_error():
    buf = EventBuffer(batch_size=10)
    await buf.flush()
    assert buf.total_flushed == 0


@pytest.mark.asyncio
async def test_total_flushed_tracks_events():
    buf = EventBuffer(batch_size=5)
    for i in range(10):
        await buf.add(ClickEvent("click", f"u{i}", "s", "/"))
    assert buf.total_flushed == 10
