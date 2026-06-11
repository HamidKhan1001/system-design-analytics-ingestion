import pytest
from pipeline import IngestionPipeline, ClickEvent


@pytest.mark.asyncio
async def test_ingest_returns_true(pipeline):
    ev = ClickEvent("page_view", "u1", "s1", "/")
    assert await pipeline.ingest(ev) is True


@pytest.mark.asyncio
async def test_duplicate_returns_false(pipeline):
    ev = ClickEvent("page_view", "u1", "s1", "/")
    await pipeline.ingest(ev)
    assert await pipeline.ingest(ev) is False


@pytest.mark.asyncio
async def test_metrics_count(pipeline):
    for i in range(5):
        await pipeline.ingest(ClickEvent("click", f"u{i}", "s", "/"))
    assert pipeline.metrics.total_events == 5


@pytest.mark.asyncio
async def test_flush_processes_events(pipeline):
    for i in range(3):
        await pipeline.ingest(ClickEvent("click", "u1", "s", f"/p{i}"))
    await pipeline.flush_all()
    assert len(pipeline.processed_events) == 3


@pytest.mark.asyncio
async def test_different_users_no_collision(pipeline):
    for uid in ["alice", "bob", "carol"]:
        ev = ClickEvent("view", uid, "sess", "/page")
        await pipeline.ingest(ev)
    assert pipeline.metrics.total_events == 3
