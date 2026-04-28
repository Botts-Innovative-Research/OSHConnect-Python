#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import datetime
from abc import ABC
from typing import Any, Union

from .core import DefaultEventTypes, Event


class EventBuilder(ABC):
    _event: Event

    def __init__(self):
        self._event: Event = Event.blank_event()

    def with_type(self, event_type: DefaultEventTypes) -> EventBuilder:
        self._event.type = event_type
        return self

    def with_topic(self, topic: str) -> EventBuilder:
        self._event.topic = topic
        return self

    def with_data(self, data: Any) -> EventBuilder:
        self._event.data = data
        return self

    def with_producer(self, producer: Any) -> EventBuilder:
        self._event.producer = producer
        return self

    def with_timestamp(self, timestamp: datetime.datetime) -> EventBuilder:
        self._event.timestamp = timestamp
        return self

    def build(self) -> Event:
        built = self._event.model_copy(deep=True)
        self.reset()
        return built

    def reset(self) -> None:
        self._event = Event.blank_event()

    @staticmethod
    def create_topic(base_topic: DefaultEventTypes, resource_id: Union[str, None] = None) -> str:
        if resource_id:
            return f"{base_topic.value}/{resource_id}"
        else:
            return base_topic.value
