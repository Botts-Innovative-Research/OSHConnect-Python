#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable

from .core import DefaultEventTypes, Event


@dataclass
class IEventListener(ABC):
    """
    Interface for event listeners. Subscribe to specific event types and/or topics.
    Empty lists mean "subscribe to all" — the handler filters before dispatching.
    """
    topics: list[str] = field(default_factory=list)
    types: list[DefaultEventTypes] = field(default_factory=list)

    @abstractmethod
    def handle_events(self, event: Event):
        pass


@dataclass
class CallbackListener(IEventListener):
    """
    Concrete IEventListener that wraps a Python callable.
    The primary user-facing subscription mechanism — no subclassing required.

    Example::

        def my_handler(event: Event):
            print(event.data)

        listener = CallbackListener(
            types=[DefaultEventTypes.NEW_OBSERVATION],
            callback=my_handler,
        )
        EventHandler().register_listener(listener)
    """
    callback: Callable[[Event], None] = field(default=None)

    def handle_events(self, event: Event):
        if self.callback is not None:
            self.callback(event)
