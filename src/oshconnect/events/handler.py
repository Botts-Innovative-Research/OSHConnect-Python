#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import logging
from collections import deque
from typing import Callable

from .core import DefaultEventTypes, Event
from .listeners import CallbackListener, IEventListener


class EventHandler(object):
    """
    Singleton event bus. Manages listener registration and event dispatch.

    Listeners are filtered by type and topic before dispatch — a listener only
    receives events whose type is in ``listener.types`` (empty = all types) AND
    whose topic is in ``listener.topics`` (empty = all topics).

    Usage — functional style (no subclassing)::

        handler = EventHandler()

        def on_obs(event: Event):
            print(event.data)

        listener = handler.subscribe(on_obs, types=[DefaultEventTypes.NEW_OBSERVATION])
        # later: handler.unregister_listener(listener)

    Usage — subclass style::

        class MyListener(IEventListener):
            def handle_events(self, event: Event):
                ...

        handler.register_listener(MyListener(types=[DefaultEventTypes.ADD_SYSTEM]))
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

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_listener(self, listener: IEventListener):
        if listener not in self.listeners:
            if not self.publish_lock:
                self.listeners.append(listener)
            else:
                self.to_add.append(listener)

    def unregister_listener(self, listener: IEventListener):
        if not self.publish_lock:
            if listener in self.listeners:
                self.listeners.remove(listener)
        else:
            self.to_remove.append(listener)

    def subscribe(
        self,
        callback: Callable[[Event], None],
        types: list[DefaultEventTypes] = None,
        topics: list[str] = None,
    ) -> CallbackListener:
        """
        Register a plain callable as a listener.

        :param callback: Function to call when a matching event is published.
        :param types: Event types to filter on. ``None`` / empty = all types.
        :param topics: MQTT/event topics to filter on. ``None`` / empty = all topics.
        :returns: The ``CallbackListener`` — keep a reference to unregister later.
        """
        listener = CallbackListener(
            topics=topics or [],
            types=types or [],
            callback=callback,
        )
        self.register_listener(listener)
        return listener

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def _matches(self, listener: IEventListener, evt: Event) -> bool:
        """Return True if *evt* passes the listener's type and topic filters."""
        type_match = not listener.types or evt.type in listener.types
        topic_match = not listener.topics or evt.topic in listener.topics
        return type_match and topic_match

    def publish(self, evt: Event):
        if self.publish_lock:
            self.event_queue.append(evt)
            return

        self.publish_lock = True
        try:
            for listener in self.listeners:
                if self._matches(listener, evt):
                    try:
                        listener.handle_events(evt)
                    except Exception as e:
                        logging.error("Error in event listener %s: %s", listener, e)
        finally:
            self.publish_lock = False
            self.commit_changes()

    # ------------------------------------------------------------------
    # Deferred add/remove bookkeeping
    # ------------------------------------------------------------------

    def commit_changes(self):
        self.commit_removes()
        self.commit_adds()
        while self.event_queue:
            self.publish(self.event_queue.popleft())

    def commit_adds(self):
        for listener in self.to_add:
            self.listeners.append(listener)
        self.to_add.clear()

    def commit_removes(self):
        for listener in self.to_remove:
            if listener in self.listeners:
                self.listeners.remove(listener)
        self.to_remove.clear()

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def clear_listeners(self):
        self.listeners.clear()
        self.to_add.clear()
        self.to_remove.clear()

    def get_num_listeners(self) -> int:
        return len(self.listeners)
