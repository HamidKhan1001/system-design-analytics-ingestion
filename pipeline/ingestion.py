from .event import ClickEvent
from .partitioner import KafkaPartitioner
from .buffer import EventBuffer
from .cache import RedisCache
from .metrics import ThroughputMetrics


class IngestionPipeline:
    def __init__(self, num_partitions: int = 16, batch_size: int = 100, cache_ttl: int = 300):
        self.partitioner = KafkaPartitioner(num_partitions)
        self.cache = RedisCache(ttl_seconds=cache_ttl)
        self.metrics = ThroughputMetrics()
        self._buffers = {
            p: EventBuffer(batch_size=batch_size, on_flush=self._on_batch_flush)
            for p in range(num_partitions)
        }
        self._processed: list = []

    async def ingest(self, event: ClickEvent) -> bool:
        if await self.cache.exists(f"evt:{event.event_id}"):
            self.metrics.record_duplicate()
            return False
        await self.cache.set(f"evt:{event.event_id}", "1")
        partition = self.partitioner.partition(event.partition_key())
        await self._buffers[partition].add(event)
        self.metrics.record_event()
        return True

    async def flush_all(self) -> None:
        for buf in self._buffers.values():
            await buf.flush()

    async def _on_batch_flush(self, batch: list) -> None:
        self._processed.extend(batch)
        self.metrics.record_batch()

    @property
    def processed_events(self) -> list:
        return list(self._processed)
