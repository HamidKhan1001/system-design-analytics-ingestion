import time
from dataclasses import dataclass, field


@dataclass
class ThroughputMetrics:
    total_events: int = 0
    total_duplicates: int = 0
    total_batches: int = 0
    _start: float = field(default_factory=time.monotonic, repr=False)

    def record_event(self) -> None:
        self.total_events += 1

    def record_duplicate(self) -> None:
        self.total_duplicates += 1

    def record_batch(self) -> None:
        self.total_batches += 1

    @property
    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._start

    @property
    def events_per_second(self) -> float:
        e = self.elapsed_seconds
        return self.total_events / e if e > 0 else 0.0

    @property
    def dedup_rate(self) -> float:
        total = self.total_events + self.total_duplicates
        return self.total_duplicates / total if total > 0 else 0.0
