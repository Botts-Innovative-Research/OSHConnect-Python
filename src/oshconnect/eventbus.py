#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
import collections
from typing import Any
from uuid import UUID
from abc import ABC


class Event(ABC):
    """
    A base class for events in the event bus system.
    """
    id: UUID
    topic: str
    payload: Any

    def __init__(self, id: UUID, topic: str, payload: Any):
        self.id = id
        self.topic = topic
        self.payload = payload


class EventBus(ABC):
    """
    A base class for an event bus system.
    """
    _deque: collections.deque
