import hashlib


class KafkaPartitioner:
    """Consistent hash partitioner — same user always goes to same partition."""

    def __init__(self, num_partitions: int = 16):
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")
        self.num_partitions = num_partitions

    def partition(self, key: str) -> int:
        digest = hashlib.md5(key.encode()).digest()
        return int.from_bytes(digest[:4], "big") % self.num_partitions

    def distribution(self, keys: list) -> dict:
        counts: dict = {}
        for k in keys:
            p = self.partition(k)
            counts[p] = counts.get(p, 0) + 1
        return counts
