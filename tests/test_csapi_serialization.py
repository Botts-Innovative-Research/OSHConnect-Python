"""OGC standard-format (de)serialization for OSHConnect resources.

Three layers per wrapper class:

  - Resource representation (System: SML+JSON / GeoJSON;
    Datastream and ControlStream: application/json).
  - Schema document (Datastream: SWE+JSON / OM+JSON;
    ControlStream: SWE+JSON / JSON).
  - Single record (one observation or one command).

Tests are organized in those sections plus a generic "no behavior drift"
guard that confirms the new convenience methods produce the same output
as a raw `model_dump(by_alias=True, exclude_none=True, mode='json')`.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from oshconnect import Node
from oshconnect.resource_datamodels import (
    ControlStreamResource,
    DatastreamResource,
    ObservationResource,
    SystemResource,
)
from oshconnect.schema_datamodels import (
    CommandJSON,
    JSONCommandSchema,
    JSONDatastreamRecordSchema,
    ObservationOMJSONInline,
    SWEDatastreamRecordSchema,
    SWEJSONCommandSchema,
)
from oshconnect.streamableresource import ControlStream, Datastream, System
from oshconnect.timemanagement import TimeInstant, TimePeriod

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def node() -> Node:
    return Node(protocol="http", address="localhost", port=8282)


# ===========================================================================
# System: SML+JSON, GeoJSON
# ===========================================================================

def test_system_resource_to_smljson_round_trips():
    src = SystemResource(uid="urn:test:s1", label="S1", feature_type="PhysicalSystem")
    dumped = src.to_smljson_dict()
    assert dumped["type"] == "PhysicalSystem"
    assert dumped["uniqueId"] == "urn:test:s1"
    rebuilt = SystemResource.from_smljson_dict(dumped)
    assert rebuilt.uid == "urn:test:s1"


def test_system_resource_to_geojson_round_trips():
    src = SystemResource(
        uid="urn:test:s1", label="S1", feature_type="Feature",
        properties={"name": "S1", "uid": "urn:test:s1"},
    )
    dumped = src.to_geojson_dict()
    assert dumped["type"] == "Feature"
    rebuilt = SystemResource.from_geojson_dict(dumped)
    assert rebuilt.uid == "urn:test:s1"


def test_system_resource_from_csapi_autodetects_smljson():
    payload = {"type": "PhysicalSystem", "uniqueId": "urn:test:auto",
               "label": "Auto"}
    res = SystemResource.from_csapi_dict(payload)
    assert res.feature_type == "PhysicalSystem"
    assert res.uid == "urn:test:auto"


def test_system_resource_from_csapi_autodetects_geojson():
    payload = {"type": "Feature", "properties": {"name": "Auto",
                                                  "uid": "urn:test:auto"}}
    res = SystemResource.from_csapi_dict(payload)
    assert res.feature_type == "Feature"
    assert res.properties["uid"] == "urn:test:auto"


def test_system_smljson_fixture_round_trips():
    raw = json.loads((FIXTURES_DIR / "fake_weather_system_smljson.json").read_text())
    res = SystemResource.from_smljson_dict(raw)
    assert res.feature_type == "PhysicalSystem"
    assert res.uid == "urn:osh:sensor:fakeweather:001"
    re_dumped = res.to_smljson_dict()
    # Required SML fields preserved
    for key in ("type", "uniqueId", "label", "definition"):
        assert key in re_dumped


def test_system_wrapper_from_smljson_dict_builds_attached_to_node(node):
    raw = json.loads((FIXTURES_DIR / "fake_weather_system_smljson.json").read_text())
    sys = System.from_smljson_dict(raw, node)
    assert isinstance(sys, System)
    assert sys.urn == "urn:osh:sensor:fakeweather:001"
    assert sys.get_parent_node() is node


def test_system_wrapper_from_csapi_dict_dispatches_on_type(node):
    raw_sml = json.loads((FIXTURES_DIR / "fake_weather_system_smljson.json").read_text())
    raw_geo = {"type": "Feature", "id": "geo-1",
               "properties": {"name": "GeoSys", "uid": "urn:test:geo"}}
    sys_sml = System.from_csapi_dict(raw_sml, node)
    sys_geo = System.from_csapi_dict(raw_geo, node)
    assert sys_sml.urn == "urn:osh:sensor:fakeweather:001"
    assert sys_geo.urn == "urn:test:geo"


# ===========================================================================
# Datastream: resource representation, schema document, observations
# ===========================================================================

def _datastream_resource_from_swejson_fixture() -> DatastreamResource:
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    schema = SWEDatastreamRecordSchema.from_swejson_dict(raw)
    return DatastreamResource(
        ds_id="ds-001", name="weather",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=schema,
    )


def test_datastream_resource_round_trips():
    src = _datastream_resource_from_swejson_fixture()
    dumped = src.to_csapi_dict()
    assert dumped["id"] == "ds-001"
    assert dumped["schema"]["obsFormat"] == "application/swe+json"
    rebuilt = DatastreamResource.from_csapi_dict(dumped)
    assert rebuilt.ds_id == "ds-001"


def test_datastream_schema_to_swejson_dict_matches_fixture(node):
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    schema = SWEDatastreamRecordSchema.from_swejson_dict(raw)
    ds_resource = DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=schema,
    )
    ds = Datastream(parent_node=node, datastream_resource=ds_resource)
    out = ds.schema_to_swejson_dict()
    assert out["obsFormat"] == "application/swe+json"
    assert out["recordSchema"]["name"] == "weather"


def test_datastream_schema_to_omjson_dict_matches_fixture(node):
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_omjson.json").read_text())
    schema = JSONDatastreamRecordSchema.from_omjson_dict(raw)
    ds_resource = DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=schema,
    )
    ds = Datastream(parent_node=node, datastream_resource=ds_resource)
    out = ds.schema_to_omjson_dict()
    assert out["obsFormat"] == "application/om+json"
    assert out["resultSchema"]["name"] == "weather"


def test_datastream_schema_methods_reject_wrong_variant(node):
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    schema = SWEDatastreamRecordSchema.from_swejson_dict(raw)
    ds = Datastream(parent_node=node, datastream_resource=DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=schema,
    ))
    with pytest.raises(TypeError, match="OM\\+JSON"):
        ds.schema_to_omjson_dict()


def test_observation_to_omjson_round_trips():
    src_time = TimeInstant.from_string("2025-06-01T12:00:00Z")
    obs = ObservationResource(
        result={"temperature": 22.5},
        result_time=src_time,
    )
    dumped = obs.to_omjson_dict(datastream_id="ds-1")
    assert dumped["datastream@id"] == "ds-1"
    assert dumped["result"] == {"temperature": 22.5}
    # resultTime is rendered via TimeUtils.time_to_iso (microsecond ISO 8601 with Z).
    assert dumped["resultTime"].startswith("2025-06-01T12:00:00")
    assert dumped["resultTime"].endswith("Z")
    rebuilt = ObservationResource.from_omjson_dict(dumped)
    assert rebuilt.result == {"temperature": 22.5}
    assert rebuilt.result_time.epoch_time == src_time.epoch_time


def test_observation_to_swejson_round_trips():
    obs = ObservationResource(
        result={"time": "2025-06-01T12:00:00Z", "temperature": 22.5},
        result_time=TimeInstant.from_string("2025-06-01T12:00:00Z"),
    )
    payload = obs.to_swejson_dict()
    assert payload == {"time": "2025-06-01T12:00:00Z", "temperature": 22.5}
    rebuilt = ObservationResource.from_swejson_dict(
        payload, result_time="2025-06-01T12:00:00Z"
    )
    assert rebuilt.result == payload


def test_datastream_observation_methods_attach_datastream_id(node):
    ds_resource = DatastreamResource(
        ds_id="ds-99", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
    )
    ds = Datastream(parent_node=node, datastream_resource=ds_resource)
    payload = ds.observation_to_omjson_dict({"temperature": 22.5})
    assert payload["datastream@id"] == "ds-99"


# ===========================================================================
# ControlStream: resource representation, schema, commands
# ===========================================================================

def _controlstream_resource_with_json_schema() -> ControlStreamResource:
    schema = JSONCommandSchema.from_json_dict({
        "commandFormat": "application/json",
        "parametersSchema": {
            "type": "DataRecord", "name": "params",
            "fields": [{
                "type": "Quantity", "name": "speed", "label": "Speed",
                "definition": "http://example.org/speed", "uom": {"code": "m/s"},
            }],
        },
    })
    return ControlStreamResource(
        cs_id="cs-001", name="motor", input_name="motor",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        command_schema=schema,
    )


def test_controlstream_resource_round_trips():
    src = _controlstream_resource_with_json_schema()
    dumped = src.to_csapi_dict()
    assert dumped["id"] == "cs-001"
    assert dumped["schema"]["commandFormat"] == "application/json"
    rebuilt = ControlStreamResource.from_csapi_dict(dumped)
    assert rebuilt.cs_id == "cs-001"


def test_controlstream_schema_to_json_dict(node):
    cs_resource = _controlstream_resource_with_json_schema()
    cs = ControlStream(node=node, controlstream_resource=cs_resource)
    out = cs.schema_to_json_dict()
    assert out["commandFormat"] == "application/json"
    assert out["parametersSchema"]["name"] == "params"


def test_controlstream_schema_methods_reject_wrong_variant(node):
    cs_resource = _controlstream_resource_with_json_schema()
    cs = ControlStream(node=node, controlstream_resource=cs_resource)
    with pytest.raises(TypeError, match="SWE\\+JSON"):
        cs.schema_to_swejson_dict()


def test_controlstream_command_to_json_dict(node):
    cs_resource = _controlstream_resource_with_json_schema()
    cs = ControlStream(node=node, controlstream_resource=cs_resource)
    out = cs.command_to_json_dict({"speed": 1.5}, sender="tester")
    assert out["control@id"] == "cs-001"
    assert out["sender"] == "tester"
    assert out["params"] == {"speed": 1.5}


def test_controlstream_command_to_swejson_round_trips(node):
    cs_resource = _controlstream_resource_with_json_schema()
    cs = ControlStream(node=node, controlstream_resource=cs_resource)
    payload = cs.command_to_swejson_dict({"speed": 1.5})
    assert payload == {"speed": 1.5}
    rebuilt = ControlStream.command_from_swejson_dict(payload)
    assert rebuilt == payload


def test_command_json_round_trips():
    src = CommandJSON(control_id="cs-1", sender="me", params={"x": 1})
    dumped = src.to_csapi_dict()
    assert dumped["control@id"] == "cs-1"
    rebuilt = CommandJSON.from_csapi_dict(dumped)
    assert rebuilt.params == {"x": 1}


# ===========================================================================
# Generic: no behavior drift from raw model_dump
# ===========================================================================

@pytest.mark.parametrize("build,method", [
    (lambda: SystemResource(uid="urn:test:1", label="X", feature_type="PhysicalSystem"),
     "to_smljson_dict"),
    (lambda: _datastream_resource_from_swejson_fixture(), "to_csapi_dict"),
    (lambda: _controlstream_resource_with_json_schema(), "to_csapi_dict"),
])
def test_resource_to_csapi_matches_raw_model_dump(build, method):
    instance = build()
    new_way = getattr(instance, method)()
    raw_way = instance.model_dump(by_alias=True, exclude_none=True, mode='json')
    assert new_way == raw_way


# ===========================================================================
# Deprecation warnings on the old factories
# ===========================================================================

def test_system_from_system_resource_emits_deprecation_warning(node):
    raw = json.loads((FIXTURES_DIR / "fake_weather_system_smljson.json").read_text())
    res = SystemResource.from_smljson_dict(raw)
    with pytest.warns(DeprecationWarning, match="from_csapi_dict"):
        sys = System.from_system_resource(res, node)
    assert sys.urn == "urn:osh:sensor:fakeweather:001"


def test_datastream_from_resource_emits_deprecation_warning(node):
    ds_resource = DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
    )
    with pytest.warns(DeprecationWarning, match="from_csapi_dict"):
        ds = Datastream.from_resource(ds_resource, node)
    assert ds.get_id() == "ds-1"
