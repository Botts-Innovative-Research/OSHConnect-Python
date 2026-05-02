#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/4/2
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
#
#  Verifies that all public symbols are importable from the top-level package
#  and from the csapi4py subpackage.  Run with:
#      uv run pytest tests/test_imports.py
#
#  Requirements: the package must be installed in the environment first:
#      uv sync   (or)   pip install -e .
#  =============================================================================


# ---------------------------------------------------------------------------
# Top-level package
# ---------------------------------------------------------------------------

def test_core_resources_importable():
    from oshconnect import OSHConnect, Node, System, Datastream, ControlStream
    assert OSHConnect is not None
    assert Node is not None
    assert System is not None
    assert Datastream is not None
    assert ControlStream is not None


def test_streaming_enums_importable():
    from oshconnect import StreamableModes, Status
    assert StreamableModes is not None
    assert Status is not None


def test_time_management_importable():
    from oshconnect import TimePeriod, TimeInstant, TemporalModes, TimeUtils
    assert TimePeriod is not None
    assert TimeInstant is not None
    assert TemporalModes is not None
    assert TimeUtils is not None


def test_resource_datamodels_importable():
    from oshconnect import (
        SystemResource,
        DatastreamResource,
        ControlStreamResource,
        ObservationResource,
    )
    assert SystemResource is not None
    assert DatastreamResource is not None
    assert ControlStreamResource is not None
    assert ObservationResource is not None


def test_swe_schema_components_importable():
    from oshconnect import (
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
    for cls in (DataRecordSchema, VectorSchema, QuantitySchema, TimeSchema,
                BooleanSchema, CountSchema, CategorySchema, TextSchema,
                QuantityRangeSchema, TimeRangeSchema):
        assert cls is not None


def test_schema_datamodels_importable():
    from oshconnect import SWEDatastreamRecordSchema, JSONCommandSchema
    assert SWEDatastreamRecordSchema is not None
    assert JSONCommandSchema is not None


def test_event_system_importable():
    from oshconnect import (
        EventHandler,
        IEventListener,
        DefaultEventTypes,
        AtomicEventTypes,
        Event,
        EventBuilder,
    )
    assert EventHandler is not None
    assert IEventListener is not None
    assert DefaultEventTypes is not None
    assert AtomicEventTypes is not None
    assert Event is not None
    assert EventBuilder is not None


def test_csapi_constants_importable():
    from oshconnect import ObservationFormat, APIResourceTypes, ContentTypes
    assert ObservationFormat is not None
    assert APIResourceTypes is not None
    assert ContentTypes is not None


def test_all_list_present_and_complete():
    import oshconnect
    assert hasattr(oshconnect, "__all__")
    assert len(oshconnect.__all__) > 0
    for name in oshconnect.__all__:
        assert hasattr(oshconnect, name), f"__all__ lists '{name}' but it is not importable"


# ---------------------------------------------------------------------------
# csapi4py subpackage
# ---------------------------------------------------------------------------

def test_csapi4py_constants_importable():
    from oshconnect.csapi4py import APIResourceTypes, ObservationFormat, ContentTypes, APITerms, SystemTypes
    assert APIResourceTypes is not None
    assert ObservationFormat is not None
    assert ContentTypes is not None
    assert APITerms is not None
    assert SystemTypes is not None


def test_csapi4py_request_builder_importable():
    from oshconnect.csapi4py import ConnectedSystemsRequestBuilder, ConnectedSystemAPIRequest
    assert ConnectedSystemsRequestBuilder is not None
    assert ConnectedSystemAPIRequest is not None


def test_csapi4py_mqtt_importable():
    from oshconnect.csapi4py import MQTTCommClient
    assert MQTTCommClient is not None


def test_csapi4py_api_helper_importable():
    from oshconnect.csapi4py import APIHelper
    assert APIHelper is not None


def test_csapi4py_all_list_present_and_complete():
    import oshconnect.csapi4py as csapi4py
    assert hasattr(csapi4py, "__all__")
    for name in csapi4py.__all__:
        assert hasattr(csapi4py, name), f"__all__ lists '{name}' but it is not importable"