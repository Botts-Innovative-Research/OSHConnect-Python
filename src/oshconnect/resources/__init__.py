#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Streamable resource hierarchy: the user-facing wrappers for OSH systems,
datastreams, and control streams.

The streaming-machinery base class (`StreamableResource`) and direction /
lifecycle enums live in `.base`; concrete subclasses live in `.system`,
`.datastream`, and `.controlstream`. Top-level imports continue to work
through `oshconnect.streamableresource` (re-export shim) and the package
`__init__`.
"""
from .base import (
    SchemaFetchWarning,
    Status,
    StreamableModes,
    StreamableResource,
)
from .controlstream import ControlStream
from .datastream import Datastream
from .system import System

__all__ = [
    "SchemaFetchWarning",
    "Status",
    "StreamableModes",
    "StreamableResource",
    "ControlStream",
    "Datastream",
    "System",
]
