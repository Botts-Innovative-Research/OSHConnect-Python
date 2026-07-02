#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Backward-compatible re-export shim.

The classes that used to live in this module have moved into focused
sibling modules:

- `Node`, `SessionManager`, `OSHClientSession`, `Endpoints`, `Utilities`
  → `oshconnect.node`
- `StreamableResource`, `Status`, `StreamableModes`, `SchemaFetchWarning`
  → `oshconnect.resources.base`
- `System` → `oshconnect.resources.system`
- `Datastream` → `oshconnect.resources.datastream`
- `ControlStream` → `oshconnect.resources.controlstream`

Existing ``from oshconnect.streamableresource import X`` paths continue
to resolve through this shim. Prefer importing from `oshconnect` directly
or from the new sibling modules in new code.
"""
from .node import Endpoints, Node, OSHClientSession, SessionManager, Utilities
from .resources.base import (
    SchemaFetchWarning,
    Status,
    StreamableModes,
    StreamableResource,
)
from .resources.controlstream import ControlStream
from .resources.datastream import Datastream
from .resources.system import System

__all__ = [
    "ControlStream",
    "Datastream",
    "Endpoints",
    "Node",
    "OSHClientSession",
    "SchemaFetchWarning",
    "SessionManager",
    "Status",
    "StreamableModes",
    "StreamableResource",
    "System",
    "Utilities",
]
