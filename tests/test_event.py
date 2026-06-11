from pipeline import ClickEvent


def test_event_has_event_id(event):
    assert len(event.event_id) == 36


def test_event_to_dict(event):
    d = event.to_dict()
    assert d["user_id"] == "u1"
    assert d["event_type"] == "page_view"


def test_partition_key_is_user_id(event):
    assert event.partition_key() == "u1"


def test_timestamp_auto_set(event):
    assert event.timestamp_ms > 0


def test_properties_default_empty(event):
    assert event.properties == {}
