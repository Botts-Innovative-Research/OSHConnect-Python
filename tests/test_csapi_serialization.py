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
    OMJSONDatastreamRecordSchema,
    LogicalDatastreamRecordSchema,
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


def test_system_from_resource_attaches_to_node(node):
    """`from_resource` is the canonical bridge from a parsed SystemResource
    to a System wrapper, mirroring how Datastream/ControlStream's __init__
    accept their parsed resource directly."""
    res = SystemResource(
        uid="urn:test:s1", label="S1", feature_type="PhysicalSystem",
        system_id="ext-id-1",
    )
    sys = System.from_resource(res, node)
    assert isinstance(sys, System)
    assert sys.urn == "urn:test:s1"
    assert sys.label == "S1"
    assert sys.get_parent_node() is node
    assert sys.get_system_resource() is res


def test_system_from_resource_handles_geojson_shape(node):
    """`from_resource` accepts a SystemResource regardless of which CS API
    shape it was parsed from (GeoJSON vs SML+JSON). The properties-block
    GeoJSON case routes name/uid through the `properties` dict."""
    res = SystemResource(
        feature_type="Feature",
        system_id="ext-id-2",
        properties={"name": "GeoSys", "uid": "urn:test:geo"},
    )
    sys = System.from_resource(res, node)
    assert sys.urn == "urn:test:geo"
    assert sys.label == "GeoSys"


def test_system_full_chain_smljson_dict_to_resource_to_wrapper(node):
    """End-to-end JSON -> SystemResource -> System chain. Format
    conversion lives entirely on `SystemResource`; the wrapper only
    knows how to bind a parsed resource to a parent node."""
    raw = json.loads((FIXTURES_DIR / "fake_weather_system_smljson.json").read_text())
    res = SystemResource.from_smljson_dict(raw)
    sys = System.from_resource(res, node)
    assert sys.urn == "urn:osh:sensor:fakeweather:001"
    assert sys.get_system_resource() is res


def test_system_full_chain_geojson_dict_to_resource_to_wrapper(node):
    """End-to-end GeoJSON variant of the chain."""
    raw = {"type": "Feature", "id": "geo-2",
           "properties": {"name": "GeoSys2", "uid": "urn:test:geo:2"}}
    res = SystemResource.from_geojson_dict(raw)
    sys = System.from_resource(res, node)
    assert sys.urn == "urn:test:geo:2"
    assert sys.label == "GeoSys2"


# ---------------------------------------------------------------------------
# SML type preservation and non-mutation
# ---------------------------------------------------------------------------

def test_to_smljson_preserves_non_default_feature_type():
    """A source whose SML type is ``PhysicalComponent`` (which OSH
    surfaces as ``featureType: Sensor``) must round-trip through
    ``to_smljson_dict`` without being collapsed back to
    ``PhysicalSystem``. Regression guard for cross-node sync."""
    src = SystemResource(uid="urn:test:s1", label="S1",
                         feature_type="PhysicalComponent")
    dumped = src.to_smljson_dict()
    assert dumped["type"] == "PhysicalComponent"


def test_to_smljson_defaults_to_physical_system_when_unset():
    """When ``feature_type`` is unset, the SML body still gets a
    sensible default so callers building a bare SystemResource
    continue to produce a valid SML body."""
    src = SystemResource(uid="urn:test:s1", label="S1")
    dumped = src.to_smljson_dict()
    assert dumped["type"] == "PhysicalSystem"


def test_to_smljson_does_not_mutate_feature_type():
    """Pre-fix, ``to_smljson_dict`` set ``self.feature_type`` as a
    side effect, which clobbered the source's SML kind. After the
    fix, the model is untouched."""
    src = SystemResource(uid="urn:test:s1", label="S1",
                         feature_type="PhysicalComponent")
    src.to_smljson_dict()
    assert src.feature_type == "PhysicalComponent"


def test_to_geojson_always_emits_feature_without_mutating():
    """GeoJSON form requires ``type: Feature`` per spec, regardless
    of ``feature_type`` on the model. The model itself stays
    unmutated."""
    src = SystemResource(uid="urn:test:s1", label="S1",
                         feature_type="PhysicalComponent")
    dumped = src.to_geojson_dict()
    assert dumped["type"] == "Feature"
    assert src.feature_type == "PhysicalComponent"


# ---------------------------------------------------------------------------
# System.to_system_resource preserves _underlying_resource
# ---------------------------------------------------------------------------

def test_to_system_resource_preserves_full_underlying(node):
    """When the wrapper carries a full ``_underlying_resource`` (e.g.,
    populated by discovery / ``from_csapi_dict``), the resource
    rendered for POST keeps every field — not just uid/label/type."""
    raw = {
        "type": "PhysicalComponent",
        "id": "src-server-id-abc",
        "uniqueId": "urn:test:source:1",
        "label": "Source Sensor",
        "description": "Original description",
        "definition": "http://www.opengis.net/def/system",
        "keywords": ["thermal", "imaging"],
    }
    res = SystemResource.from_smljson_dict(raw)
    sys = System.from_resource(res, node)

    rendered = sys.to_system_resource()

    # Type preserved (was hardcoded to PhysicalSystem pre-fix).
    assert rendered.feature_type == "PhysicalComponent"
    # Other fields preserved (were silently dropped pre-fix).
    assert rendered.description == "Original description"
    assert rendered.definition == "http://www.opengis.net/def/system"
    assert rendered.keywords == ["thermal", "imaging"]


def test_to_system_resource_thin_shell_for_freshly_constructed(node):
    """A System constructed from scratch (no parsed resource) still
    produces a sensible thin shell with default ``PhysicalSystem``
    type — backward-compat with code that doesn't go through
    discovery."""
    sys = System(label="Fresh", urn="urn:test:fresh:1",
                 parent_node=node)
    rendered = sys.to_system_resource()
    assert rendered.feature_type == "PhysicalSystem"
    assert rendered.uid == "urn:test:fresh:1"


def test_system_name_property_is_deprecated_alias_for_label(node):
    """The wrapper-level `name` field was always populated from the
    same wire string as `label` — the OGC CS API only carries one
    display string per system. `System.name` is now a deprecated
    alias for `.label`; reading or writing it emits
    ``DeprecationWarning`` but still works for one-release back-compat.
    """
    sys = System(label="Original", urn="urn:test:dep:1", parent_node=node)

    # Reading: returns label, emits deprecation warning.
    with pytest.warns(DeprecationWarning, match=r"System\.name.*deprecated"):
        assert sys.name == "Original"

    # Writing: sets label, emits deprecation warning.
    with pytest.warns(DeprecationWarning, match=r"System\.name.*deprecated"):
        sys.name = "Renamed"
    assert sys.label == "Renamed"


def test_system_init_with_name_kwarg_routes_to_label_with_warning(node):
    """Passing the deprecated `name=` kwarg to `System(...)` populates
    `label` (when `label` is not also given) and emits a deprecation
    warning. When both are provided, `label` wins and `name` is dropped.
    """
    with pytest.warns(DeprecationWarning, match=r"System\(name=\.\.\.\)"):
        sys = System(name="LegacyOnly", urn="urn:test:dep:2", parent_node=node)
    assert sys.label == "LegacyOnly"

    with pytest.warns(DeprecationWarning):
        sys2 = System(label="Wins", name="Loses",
                      urn="urn:test:dep:3", parent_node=node)
    assert sys2.label == "Wins"


# ---------------------------------------------------------------------------
# insert_self strips server-assigned fields from the POST body
# ---------------------------------------------------------------------------

class _MockResponse:
    status_code = 201
    ok = True
    text = ""
    headers = {"Location": "http://localhost:8282/sensorhub/api/systems/dest-id-xyz"}


def _capture_post(into: dict):
    def _f(url, params=None, headers=None, auth=None, data=None, json=None, **kwargs):
        into["url"] = str(url)
        into["data"] = data
        into["json"] = json
        return _MockResponse()
    return _f


def test_insert_self_strips_id_and_links_from_body(node, monkeypatch):
    """When re-POSTing a discovered system to a destination node, the
    source's server-assigned ``id`` and ``links`` must not leak into
    the body — the destination assigns its own. Regression guard for
    cross-node sync."""
    raw = {
        "type": "PhysicalComponent",
        "id": "source-side-id",
        "uniqueId": "urn:test:source:1",
        "label": "Source Sensor",
        "links": [{"href": "http://source.example/extra", "rel": "alternate"}],
    }
    res = SystemResource.from_smljson_dict(raw)
    sys = System.from_resource(res, node)

    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.post",
        _capture_post(captured),
    )

    sys.insert_self()

    body = json.loads(captured["data"])
    # Source-assigned identifiers must NOT be present in the POST body.
    assert "id" not in body, (
        "POST body must not carry source's server-assigned id"
    )
    assert "links" not in body, (
        "POST body must not carry source's server-assigned links"
    )
    # But the SML kind from the source IS preserved.
    assert body["type"] == "PhysicalComponent"
    assert body["uniqueId"] == "urn:test:source:1"
    # Wrapper picked up the destination's id from the Location header.
    assert sys._resource_id == "dest-id-xyz"


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


def test_datastream_schema_accessible_via_underlying_resource(node):
    """Schema rendering lives on the schema model, not on the wrapper.
    Users reach it via `ds._underlying_resource.record_schema.to_*_dict()`."""
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    schema = SWEDatastreamRecordSchema.from_swejson_dict(raw)
    ds = Datastream(parent_node=node, datastream_resource=DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=schema,
    ))
    out = ds._underlying_resource.record_schema.to_swejson_dict()
    assert out["obsFormat"] == "application/swe+json"
    assert out["recordSchema"]["name"] == "weather"


def test_swe_datastream_schema_model_dump_json_directly():
    """Regression: prior to the SerializeAsAny -> discriminated-union
    migration, calling `model_dump_json` on a parsed `SWEDatastreamRecordSchema`
    raised `MockValSer is not an instance of SchemaSerializer` because
    pydantic deferred building the serializer for the recursive
    `list["AnyComponent"]` forward refs and never replaced the placeholder.

    The fix combines (a) discriminated unions on `obs_format`/`command_format`
    eliminating SerializeAsAny on the resource models, and (b) explicit
    `model_rebuild(force=True)` on every container. Both `model_dump`
    and `model_dump_json` must now succeed on a parsed schema."""
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    schema = SWEDatastreamRecordSchema.from_swejson_dict(raw)

    py = schema.model_dump(by_alias=True, exclude_none=True)
    assert py["obsFormat"] == "application/swe+json"
    assert py["recordSchema"]["name"] == "weather"

    js = schema.model_dump_json(by_alias=True, exclude_none=True)
    assert json.loads(js)["obsFormat"] == "application/swe+json"


def test_datastream_resource_with_populated_schema_dumps_via_broker_path():
    """Regression covering the broker's exact path: validate a
    DatastreamResource, populate `record_schema` with a parsed SWE+JSON
    schema, then `model_dump_json(by_alias=True, exclude_none=True)`.
    Pre-fix this raised `MockValSer is not an instance of SchemaSerializer`."""
    schema_raw = json.loads(
        (FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text()
    )
    ds = DatastreamResource(
        ds_id="ds-001", name="weather",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
    )
    ds.record_schema = SWEDatastreamRecordSchema.from_swejson_dict(schema_raw)

    payload = ds.model_dump_json(by_alias=True, exclude_none=True)
    parsed = json.loads(payload)
    assert parsed["id"] == "ds-001"
    assert parsed["schema"]["obsFormat"] == "application/swe+json"
    assert parsed["schema"]["recordSchema"]["type"] == "DataRecord"

    # Round-trip: the discriminated union picks the right arm on parse-back.
    rebuilt = DatastreamResource.model_validate_json(payload)
    assert isinstance(rebuilt.record_schema, SWEDatastreamRecordSchema)
    assert rebuilt.record_schema.obs_format == "application/swe+json"


def test_datastream_resource_dispatches_to_omjson_arm_via_discriminator():
    """The `AnyDatastreamRecordSchema` discriminated union must route
    `obsFormat: application/om+json` payloads to `OMJSONDatastreamRecordSchema`."""
    om_schema_raw = json.loads(
        (FIXTURES_DIR / "fake_weather_schema_omjson.json").read_text()
    )
    om = OMJSONDatastreamRecordSchema.from_omjson_dict(om_schema_raw)
    ds = DatastreamResource(
        ds_id="ds-om", name="weather-om",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
        record_schema=om,
    )
    payload = ds.model_dump_json(by_alias=True, exclude_none=True)
    rebuilt = DatastreamResource.model_validate_json(payload)
    assert isinstance(rebuilt.record_schema, OMJSONDatastreamRecordSchema)
    assert rebuilt.record_schema.obs_format in (
        "application/om+json", "application/json",
    )


def test_controlstream_resource_with_populated_schema_dumps_via_broker_path():
    """Same broker-path regression for the control-stream side."""
    cmd_schema = JSONCommandSchema(
        command_format="application/json",
        params_schema={
            "type": "DataRecord",
            "name": "cmd",
            "label": "Cmd",
            "fields": [
                {"type": "Quantity", "name": "speed", "label": "Speed",
                 "definition": "http://example.org/speed",
                 "uom": {"code": "m/s"}},
            ],
        },
    )
    cs = ControlStreamResource(
        cs_id="cs-1", name="set-speed",
        command_schema=cmd_schema,
    )

    payload = cs.model_dump_json(by_alias=True, exclude_none=True)
    parsed = json.loads(payload)
    assert parsed["schema"]["commandFormat"] == "application/json"
    assert parsed["schema"]["parametersSchema"]["name"] == "cmd"

    rebuilt = ControlStreamResource.model_validate_json(payload)
    assert isinstance(rebuilt.command_schema, JSONCommandSchema)
    assert rebuilt.command_schema.command_format == "application/json"


# ---------------------------------------------------------------------------
# Logical schema (OSH's `obsFormat=logical` shape)
# ---------------------------------------------------------------------------

def test_logical_schema_round_trips_from_fixture():
    """Parse OSH's logical schema (JSON Schema with x-ogc-* extensions),
    re-dump it, and confirm the round-trip preserves all fields."""
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_logical.json").read_text())
    schema = LogicalDatastreamRecordSchema.from_logical_dict(raw)

    assert schema.type == "object"
    assert schema.title == "New Simulated Weather Sensor - weather"
    assert set(schema.properties.keys()) == {
        "time", "temperature", "pressure", "windSpeed", "windDirection"
    }

    # OGC extensions parsed via aliases
    temp = schema.properties["temperature"]
    assert temp.type == "number"
    assert temp.title == "Air Temperature"
    assert temp.ogc_definition == "http://mmisw.org/ont/cf/parameter/air_temperature"
    assert temp.ogc_unit == "Cel"

    time = schema.properties["time"]
    assert time.type == "string"
    assert time.format == "date-time"
    assert time.ogc_ref_frame == "http://www.opengis.net/def/trs/BIPM/0/UTC"

    wind_dir = schema.properties["windDirection"]
    assert wind_dir.ogc_axis == "z"

    # Round-trip: dump back into wire form, deep-equal to fixture
    dumped = schema.to_logical_dict()
    assert dumped == raw


def test_logical_schema_distinct_shape_from_swe_and_om():
    """The logical fixture is structurally distinct: no `obsFormat`
    envelope and no `recordSchema` wrapper. Parsing SWE+JSON / OM+JSON
    fixtures through `LogicalDatastreamRecordSchema` (which requires the
    JSON-Schema-style ``type`` + ``properties``) fails — confirming the
    three models target genuinely different shapes."""
    swe_raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    om_raw = json.loads((FIXTURES_DIR / "fake_weather_schema_omjson.json").read_text())
    with pytest.raises(ValidationError):
        LogicalDatastreamRecordSchema.from_logical_dict(swe_raw)
    with pytest.raises(ValidationError):
        LogicalDatastreamRecordSchema.from_logical_dict(om_raw)


def test_logical_schema_permissive_extra_fields():
    """JSON Schema fields we haven't modeled (description, default,
    minimum, maximum, etc.) are accepted via ``extra='allow'`` so future
    OSH additions don't break parsing."""
    raw = {
        "type": "object",
        "title": "Test",
        "description": "extra field, not modeled",
        "properties": {
            "x": {
                "type": "number",
                "minimum": 0,
                "maximum": 100,
                "default": 50,
                "x-ogc-unit": "Cel",
            },
        },
    }
    schema = LogicalDatastreamRecordSchema.from_logical_dict(raw)
    # Extra fields preserved on the model
    dumped = schema.to_logical_dict()
    assert dumped["description"] == "extra field, not modeled"
    assert dumped["properties"]["x"]["minimum"] == 0


def test_retrieve_datastream_schema_logical_obsformat(monkeypatch):
    """Schema retrieval lives as a free function in
    ``oshconnect.api_helpers``, not on ``Datastream``. Callers pick the
    schema variant via the ``obs_format`` query param. Verify the URL,
    ``?obsFormat=logical`` query, and that the body parses as
    ``LogicalDatastreamRecordSchema``.
    """
    from oshconnect.api_helpers import retrieve_datastream_schema

    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_logical.json").read_text())

    captured = {}

    class _MockResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return raw

    def _mock_get(url, params=None, headers=None, auth=None, **kwargs):
        captured["url"] = str(url)
        captured["params"] = params
        captured["auth"] = auth
        return _MockResponse()

    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _mock_get,
    )

    resp = retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "038s1ic7k460",
        obs_format="logical",
    )
    schema = LogicalDatastreamRecordSchema.from_logical_dict(resp.json())

    assert isinstance(schema, LogicalDatastreamRecordSchema)
    assert schema.title == "New Simulated Weather Sensor - weather"
    assert captured["url"].endswith("/datastreams/038s1ic7k460/schema")
    assert captured["params"] == {"obsFormat": "logical"}


def test_retrieve_datastream_schema_swejson_obsformat(monkeypatch):
    """Symmetric to the logical-format test: SWE+JSON variant goes
    through the same ``retrieve_datastream_schema`` helper, picked via
    ``obs_format='application/swe+json'``. The body parses as
    ``SWEDatastreamRecordSchema``.
    """
    from oshconnect.api_helpers import retrieve_datastream_schema

    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())

    captured = {}

    class _MockResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return raw

    def _mock_get(url, params=None, headers=None, auth=None, **kwargs):
        captured["params"] = params
        return _MockResponse()

    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _mock_get,
    )

    resp = retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "ds-x",
        obs_format="application/swe+json",
    )
    schema = SWEDatastreamRecordSchema.from_swejson_dict(resp.json())
    assert isinstance(schema, SWEDatastreamRecordSchema)
    assert captured["params"] == {"obsFormat": "application/swe+json"}


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


def test_observation_omjson_caller_supplies_datastream_id():
    """ObservationResource.to_omjson_dict accepts an optional `datastream_id`
    so the caller (typically wrapping code that knows the parent datastream)
    can stamp it onto the OM+JSON envelope."""
    obs = ObservationResource(
        result={"temperature": 22.5},
        result_time=TimeInstant.from_string("2025-06-01T12:00:00Z"),
    )
    payload = obs.to_omjson_dict(datastream_id="ds-99")
    assert payload["datastream@id"] == "ds-99"
    # When omitted, no datastream@id key in the output.
    payload_bare = obs.to_omjson_dict()
    assert "datastream@id" not in payload_bare


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


def test_controlstream_schema_accessible_via_underlying_resource(node):
    """Command schema rendering lives on the schema model. Users reach
    it via `cs._underlying_resource.command_schema.to_json_dict()`."""
    cs_resource = _controlstream_resource_with_json_schema()
    cs = ControlStream(node=node, controlstream_resource=cs_resource)
    out = cs._underlying_resource.command_schema.to_json_dict()
    assert out["commandFormat"] == "application/json"
    assert out["parametersSchema"]["name"] == "params"


def test_command_json_round_trips():
    src = CommandJSON(control_id="cs-1", sender="me", params={"x": 1})
    dumped = src.to_csapi_dict()
    assert dumped["control@id"] == "cs-1"
    # CS API Part 2 / OSH expects "parameters" on the wire, not "params".
    # OSH returns 500 if the body uses "params" (verified against a live
    # 8282 instance against the controllable-counter sample sensor).
    assert dumped["parameters"] == {"x": 1}
    assert "params" not in dumped, (
        "CommandJSON must serialize as 'parameters' (CS API Part 2), not 'params'"
    )
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
    with pytest.warns(DeprecationWarning, match="from_resource"):
        sys = System.from_system_resource(res, node)
    assert sys.urn == "urn:osh:sensor:fakeweather:001"


def test_datastream_from_resource_emits_deprecation_warning(node):
    ds_resource = DatastreamResource(
        ds_id="ds-1", name="w",
        valid_time=TimePeriod(start="2025-01-01T00:00:00Z",
                              end="2099-12-31T00:00:00Z"),
    )
    with pytest.warns(DeprecationWarning, match="constructor"):
        ds = Datastream.from_resource(ds_resource, node)
    assert ds.get_id() == "ds-1"
