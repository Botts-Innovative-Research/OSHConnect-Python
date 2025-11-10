#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Union

from pydantic import BaseModel, ConfigDict


class Event(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timestamp: datetime.datetime
    type: DefaultEventTypes
    topic: str
    data: Any
    producer: Any

    @classmethod
    def blank_event(cls) -> Event:
        return cls(
            timestamp=datetime.datetime.now(),
            type=DefaultEventTypes.NEW_OBSERVATION,
            topic="",
            data=None,
            producer=None
        )


@dataclass
class IEventListener(ABC):
    """
    Interface for event listeners. They may subscribe to specific topics and/or certain event types.
    """
    topics: list[str]
    types: list[DefaultEventTypes]

    @abstractmethod
    def handle_events(self, event: Event):
        pass


class EventHandler(object):
    """
    Singleton event handler to manage event listeners and publish events.
    """
    listeners: list[IEventListener] = []
    to_add: list[IEventListener] = []
    to_remove: list[IEventListener] = []
    event_queue: deque[Event] = deque()
    publish_lock: bool = False

    def __new__(cls):
        if not hasattr(cls, "instance"):
            cls.instance = super(EventHandler, cls).__new__(cls)
        return cls.instance

    def register_listener(self, listener: IEventListener):
        if listener not in self.listeners:
            if not self.publish_lock:
                self.listeners.append(listener)
            else:
                self.to_add.append(listener)

    def unregister_listener(self, listener: IEventListener):
        if not self.publish_lock:
            self.listeners.remove(listener)
        else:
            self.to_remove.append(listener)

    def publish(self, evt: Event):
        if self.publish_lock:
            self.event_queue.append(evt)
        else:
            self.publish_lock = True

            try:
                for listener in self.listeners:
                    listener.handle_events(evt)
            except Exception as e:
                # TODO: handle a more specific error
                print(f"Error publishing event: {e}")
            finally:
                self.publish_lock = False
                self.commit_changes()

    def commit_changes(self):
        self.commit_removes()
        self.commit_adds()

        while len(self.event_queue) > 0:
            self.publish(self.event_queue.popleft())

    def commit_adds(self):
        for listener in self.to_add:
            self.listeners.append(listener)
        self.to_add.clear()

    def commit_removes(self):
        for listener in self.to_remove:
            self.listeners.remove(listener)
        self.to_remove.clear()

    def clear_listeners(self):
        self.listeners.clear()
        self.to_add.clear()
        self.to_remove.clear()

    def get_num_listeners(self) -> int:
        return len(self.listeners)


class DefaultEventTypes(Enum):
    ADD_NODE: str = "add_node"
    REMOVE_NODE: str = "remove_node"
    ADD_SYSTEM: str = "add_system"
    REMOVE_SYSTEM: str = "remove_system"
    ADD_DATASTREAM: str = "add_datastream"
    REMOVE_DATASTREAM: str = "remove_datastream"
    ADD_CONTROLSTREAM: str = "add_controlstream"
    REMOVE_CONTROLSTREAM: str = "remove_controlstream"
    NEW_OBSERVATION: str = "new_observation"
    NEW_COMMAND: str = "new_command"
    NEW_COMMAND_STATUS: str = "new_command_status"


class AtomicEventTypes(Enum):
    """
    Defines atomic event types

    Attributes:
        CREATE (str): Event type for creating a resource within OSHConnect (local, in-app).
        POST (str): Event type for posting a resource to an external server.
        GET (str): Event type for retrieving a resource from an external server.
        MODIFY (str): Event type for modifying a resource within OSHConnect (local, in-app).
        UPDATE (str): Event type for updating a resource on an external server.
        REMOVE (str): Event type for removing a resource within OSHConnect (local, in-app).
        DELETE (str): Event type for deleting a resource from an external server.
    """
    #
    CREATE: str = "create"
    POST: str = "post"
    GET: str = "get"
    MODIFY: str = "modify"
    UPDATE: str = "update"
    REMOVE: str = "remove"
    DELETE: str = "delete"


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
