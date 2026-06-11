import pytest
from pipeline import KafkaPartitioner


def test_same_key_same_partition(partitioner):
    p1 = partitioner.partition("user-123")
    p2 = partitioner.partition("user-123")
    assert p1 == p2


def test_partition_in_range(partitioner):
    for i in range(100):
        p = partitioner.partition(f"user-{i}")
        assert 0 <= p < partitioner.num_partitions


def test_distribution_covers_partitions():
    p = KafkaPartitioner(num_partitions=8)
    keys = [f"user-{i}" for i in range(1000)]
    dist = p.distribution(keys)
    assert len(dist) >= 6


def test_invalid_partition_count():
    with pytest.raises(ValueError):
        KafkaPartitioner(num_partitions=0)


def test_different_keys_different_partitions():
    p = KafkaPartitioner(num_partitions=16)
    partitions = {p.partition(f"u-{i}") for i in range(50)}
    assert len(partitions) > 1
