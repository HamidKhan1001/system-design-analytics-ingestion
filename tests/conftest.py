import pytest
from pipeline import ClickEvent, IngestionPipeline, KafkaPartitioner, EventBuffer, RedisCache


@pytest.fixture
def event():
    return ClickEvent(event_type="page_view", user_id="u1", session_id="s1", url="/home")


@pytest.fixture
def pipeline():
    return IngestionPipeline(num_partitions=4, batch_size=10)


@pytest.fixture
def partitioner():
    return KafkaPartitioner(num_partitions=8)


@pytest.fixture
def cache():
    return RedisCache(max_size=100)
