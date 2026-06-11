import time
import uuid
from dataclasses import dataclass, field


@dataclass
class ClickEvent:
    event_type: str
    user_id: str
    session_id: str
    url: str
    timestamp_ms: int = field(default_factory=lambda: int(time.time() * 1000))
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    properties: dict = field(default_factory=dict)

    def partition_key(self) -> str:
        return self.user_id

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "url": self.url,
            "timestamp_ms": self.timestamp_ms,
            "properties": self.properties,
        }
