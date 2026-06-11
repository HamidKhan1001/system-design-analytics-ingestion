# system-design-analytics-ingestion

Real-time clickstream analytics ingestion pipeline (Mixpanel-style). Events flow through a consistent-hash Kafka partitioner into per-partition batching buffers, with a Redis-backed deduplication cache to absorb traffic spikes.

## Architecture

```
Client events
     │
     ▼
IngestionPipeline.ingest(event)
     │
     ├─► RedisCache.exists(event_id)  ──► duplicate? → drop (dedup_rate tracked)
     │
     ├─► KafkaPartitioner.partition(user_id)  ──► consistent hash → partition 0..N
     │
     └─► EventBuffer[partition].add(event)
               │
               ├─ batch_size reached → flush → ClickHouse / downstream
               └─ manual flush_all()
```

## Key design decisions

**Kafka partitioning by `user_id`** — all events from the same user land on the same partition, preserving per-user ordering without coordination.

**Redis dedup cache (LRU, TTL-based)** — absorbs duplicate events during traffic spikes or client retries. O(1) hit/miss via `OrderedDict`.

**Batched flushing** — buffers accumulate events until `batch_size` or explicit flush, reducing write amplification to downstream storage.

## Throughput metrics

`ThroughputMetrics` tracks:
- `events_per_second` — real-time ingestion rate
- `dedup_rate` — fraction of events dropped as duplicates
- `total_batches` — flush operations to downstream

## Usage

```python
from pipeline import IngestionPipeline, ClickEvent

pipe = IngestionPipeline(num_partitions=16, batch_size=100)
ev = ClickEvent(event_type="page_view", user_id="u123", session_id="s1", url="/home")
accepted = await pipe.ingest(ev)   # False if duplicate
await pipe.flush_all()
print(pipe.metrics.events_per_second)
```

## Running tests

```bash
pip install -r requirements.txt
python3 -m pytest tests/ -v   # 29 tests
```
