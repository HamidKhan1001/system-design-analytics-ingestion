import pytest
import time
from pipeline.metrics import ThroughputMetrics


def test_record_event():
    m = ThroughputMetrics()
    m.record_event()
    assert m.total_events == 1


def test_dedup_rate():
    m = ThroughputMetrics()
    m.record_event()
    m.record_event()
    m.record_duplicate()
    assert m.dedup_rate == pytest.approx(1 / 3, abs=0.01)


def test_events_per_second_positive():
    m = ThroughputMetrics()
    for _ in range(100):
        m.record_event()
    assert m.events_per_second > 0


def test_elapsed_grows():
    m = ThroughputMetrics()
    e1 = m.elapsed_seconds
    time.sleep(0.01)
    e2 = m.elapsed_seconds
    assert e2 > e1


def test_batch_tracking():
    m = ThroughputMetrics()
    m.record_batch()
    m.record_batch()
    assert m.total_batches == 2
