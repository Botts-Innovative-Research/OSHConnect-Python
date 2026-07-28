#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

# Exceptions — every deliberate failure descends from OSHConnectError,
# which subclasses Exception so existing `except Exception:` still works.
from .exceptions import (
    OSHConnectError,
    ConfigurationError,
    ResourceRequestError,
    ResourceInsertError,
    MissingLocationHeaderError,
    ResourceDiscoveryError,
)

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
from .schema_datamodels import (
    SWEDatastreamRecordSchema,
    SWEBinaryDatastreamRecordSchema,
    SWEProtobufDatastreamRecordSchema,
    SWEFlatBuffersDatastreamRecordSchema,
    OMJSONDatastreamRecordSchema,
    SWEJSONCommandSchema,
    JSONCommandSchema,
    AnyDatastreamRecordSchema,
    AnyCommandSchema,
)
from .encoding import (
    Encoding,
    JSONEncoding,
    BinaryEncoding,
    BinaryComponentMember,
    BinaryBlockMember,
    ProtobufEncoding,
    FlatBuffersEncoding,
)
from .swe_binary import SWEBinaryCodec
from .swe_flatbuffers import SWEFlatBuffersCodec
# swe_protobuf is import-guarded — exposing the codec class re-exports the
# `_INSTALL_HINT` error so callers learn what to install when invoking it.
from .swe_protobuf import SWEProtobufCodec

# SensorML structured fields (carried by SystemResource)
from .sensorml import Term, Characteristics, Capabilities

# Event system
from .events import EventHandler, IEventListener, CallbackListener, DefaultEventTypes, AtomicEventTypes, Event, EventBuilder

# DataStore
from .datastore import DataStore
from .datastores import SQLiteDataStore

# CS API constants
from .csapi4py.constants import ObservationFormat, APIResourceTypes, ContentTypes

__all__ = [
    # Exceptions
    "OSHConnectError",
    "ConfigurationError",
    "ResourceRequestError",
    "ResourceInsertError",
    "MissingLocationHeaderError",
    "ResourceDiscoveryError",
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
    "SWEBinaryDatastreamRecordSchema",
    "SWEProtobufDatastreamRecordSchema",
    "SWEFlatBuffersDatastreamRecordSchema",
    "OMJSONDatastreamRecordSchema",
    "SWEJSONCommandSchema",
    "JSONCommandSchema",
    "AnyDatastreamRecordSchema",
    "AnyCommandSchema",
    # Encodings + binary codecs
    "Encoding",
    "JSONEncoding",
    "BinaryEncoding",
    "BinaryComponentMember",
    "BinaryBlockMember",
    "ProtobufEncoding",
    "FlatBuffersEncoding",
    "SWEBinaryCodec",
    "SWEProtobufCodec",
    "SWEFlatBuffersCodec",
    # SensorML structured fields
    "Term",
    "Characteristics",
    "Capabilities",
    # Event system
    "EventHandler",
    "IEventListener",
    "CallbackListener",
    "DefaultEventTypes",
    "AtomicEventTypes",
    "Event",
    "EventBuilder",
    # CS API constants
    "ObservationFormat",
    "APIResourceTypes",
    "ContentTypes",
    # DataStore
    "DataStore",
    "SQLiteDataStore",
]

# ---------------------------------------------------------------------------
# Logging hygiene (kept last so it doesn't push the imports above out of
# top-of-file position, which flake8 flags as E402).
#
# A library must not configure logging for the application embedding it.
# Attaching a NullHandler to the package logger keeps OSHConnect from
# implicitly installing a stderr handler on the ROOT logger the first time
# it warns. Consumers opt in explicitly:
#
#     logging.getLogger("oshconnect").setLevel(logging.DEBUG)
#
# Every module logs to `oshconnect.<module>` via logging.getLogger(__name__),
# so that single call controls the whole library and nothing else.
# ---------------------------------------------------------------------------
import logging as _logging  # noqa: E402

_logging.getLogger(__name__).addHandler(_logging.NullHandler())
