#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  =============================================================================
#
#  Backward-compatibility shim — all event symbols now live in the `events`
#  sub-package.  Importing from this module continues to work but prefer:
#
#      from oshconnect.events import EventHandler, DefaultEventTypes, ...
#
# -----------------------------------------------------------------------------

from .events.core import Event, DefaultEventTypes, AtomicEventTypes
from .events.handler import EventHandler
from .events.listeners import IEventListener, CallbackListener
from .events.builder import EventBuilder

__all__ = [
    "Event",
    "DefaultEventTypes",
    "AtomicEventTypes",
    "EventHandler",
    "IEventListener",
    "CallbackListener",
    "EventBuilder",
]
