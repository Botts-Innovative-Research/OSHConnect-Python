#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

# Core resources
from .oshconnectapi import OSHConnect
from .streamableresource import Node, System, Datastream, ControlStream, StreamableModes, Status

# Time management
from .timemanagement import TimePeriod, TimeInstant, TemporalModes, TimeUtils

# Resource data models
from .resource_datamodels import (
    SystemResource,
    DatastreamResource,
    ControlStreamResource,
    ObservationResource,
)

# SWE schema components
from .swe_components import (
    DataRecordSchema,
    VectorSchema,
    QuantitySchema,
    TimeSchema,
    BooleanSchema,
    CountSchema,
    CategorySchema,
    TextSchema,
    QuantityRangeSchema,
    TimeRangeSchema,
)
from .schema_datamodels import SWEDatastreamRecordSchema, JSONCommandSchema

# Event system
from .eventbus import EventHandler, IEventListener, DefaultEventTypes, AtomicEventTypes, Event, EventBuilder

# CS API constants
from .csapi4py.constants import ObservationFormat, APIResourceTypes, ContentTypes

__all__ = [
    # Core resources
    "OSHConnect",
    "Node",
    "System",
    "Datastream",
    "ControlStream",
    "StreamableModes",
    "Status",
    # Time management
    "TimePeriod",
    "TimeInstant",
    "TemporalModes",
    "TimeUtils",
    # Resource data models
    "SystemResource",
    "DatastreamResource",
    "ControlStreamResource",
    "ObservationResource",
    # SWE schema components
    "DataRecordSchema",
    "VectorSchema",
    "QuantitySchema",
    "TimeSchema",
    "BooleanSchema",
    "CountSchema",
    "CategorySchema",
    "TextSchema",
    "QuantityRangeSchema",
    "TimeRangeSchema",
    "SWEDatastreamRecordSchema",
    "JSONCommandSchema",
    # Event system
    "EventHandler",
    "IEventListener",
    "DefaultEventTypes",
    "AtomicEventTypes",
    "Event",
    "EventBuilder",
    # CS API constants
    "ObservationFormat",
    "APIResourceTypes",
    "ContentTypes",
]