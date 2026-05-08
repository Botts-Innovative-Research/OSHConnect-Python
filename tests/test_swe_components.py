"""SWE Common 3 component models: validators, structural rules, round-trip.

Two sections:

  A. SoftNamedProperty `name` validation — `name` is required wherever a
     component is bound (DataRecord.fields, DataChoice.items, Vector.coordinates,
     DataArray/Matrix.elementType, and the root recordSchema/resultSchema of a
     datastream/controlstream). Names must match NameToken
     `^[A-Za-z][A-Za-z0-9_\\-]*$`. Standalone components do NOT require a name.

  B. Schema conformance — spec-required fields per leaf type, discriminator
     routing, alias/snake_case parity, round-trip fidelity, Vector.coordinates
     element-type restriction, DataRecord.fields minItems:1.

Both sections are anchored against the canonical JSON schemas at:
https://github.com/opengeospatial/ogcapi-connected-systems/tree/master/swecommon/schemas/json
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from oshconnect.schema_datamodels import (
    JSONCommandSchema,
    OMJSONDatastreamRecordSchema,
    SWEDatastreamRecordSchema,
    SWEJSONCommandSchema,
)
from oshconnect.swe_components import (
    AnyComponent,
    BooleanSchema,
    CategoryRangeSchema,
    CategorySchema,
    CountRangeSchema,
    CountSchema,
    DataArraySchema,
    DataChoiceSchema,
    DataRecordSchema,
    GeometrySchema,
    MatrixSchema,
    QuantityRangeSchema,
    QuantitySchema,
    TextSchema,
    TimeRangeSchema,
    TimeSchema,
    VectorSchema,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ANY_COMPONENT = TypeAdapter(AnyComponent)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

VALID_TIME_FIELD = {
    "type": "Time",
    "name": "time",
    "label": "Sampling Time",
    "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
    "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"},
}
VALID_TEMP_FIELD = {
    "type": "Quantity",
    "name": "temperature",
    "label": "Air Temperature",
    "definition": "http://mmisw.org/ont/cf/parameter/air_temperature",
    "uom": {"code": "Cel"},
}


def _quantity_field(name: str = "x") -> dict:
    return {
        "type": "Quantity",
        "name": name,
        "label": "X",
        "definition": "http://example.org/x",
        "uom": {"code": "m"},
    }


# ===========================================================================
# A. SoftNamedProperty `name` validation
# ===========================================================================

# --- A.1 standalone components don't need a name ---------------------------

def test_quantity_standalone_no_name_ok():
    q = QuantitySchema(label="Air Temperature",
                       definition="http://example.org/temperature",
                       uom={"code": "Cel"})
    assert q.name is None


def test_vector_standalone_no_name_ok():
    v = VectorSchema(
        label="Position", definition="http://example.org/position",
        referenceFrame="http://example.org/frames/ENU",
        coordinates=[
            QuantitySchema(name="x", label="X",
                           definition="http://example.org/x", uom={"code": "m"}),
            QuantitySchema(name="y", label="Y",
                           definition="http://example.org/y", uom={"code": "m"}),
        ],
    )
    assert v.name is None


# --- A.2 fixtures: round-trip preserves names ------------------------------

def test_swejson_fixture_preserves_names_on_round_trip():
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    parsed = SWEDatastreamRecordSchema.model_validate(raw)
    re_dumped = parsed.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert re_dumped["recordSchema"]["name"] == "weather"
    assert {f["name"] for f in re_dumped["recordSchema"]["fields"]} == {
        "time", "temperature", "pressure", "windSpeed", "windDirection"
    }


def test_omjson_fixture_preserves_names_on_round_trip():
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_omjson.json").read_text())
    parsed = OMJSONDatastreamRecordSchema.model_validate(raw)
    re_dumped = parsed.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert re_dumped["resultSchema"]["name"] == "weather"


# --- A.3 binding contexts require name on each child -----------------------

def test_record_with_named_fields_ok():
    DataRecordSchema(name="weather",
                     fields=[VALID_TIME_FIELD, VALID_TEMP_FIELD])


def test_record_field_missing_name_raises():
    with pytest.raises(ValidationError, match="DataRecord.fields"):
        DataRecordSchema(name="weather", fields=[
            {"type": "Quantity", "label": "X",
             "definition": "http://example.org/x", "uom": {"code": "Cel"}},
        ])


def test_choice_items_named_ok():
    DataChoiceSchema(
        name="alt",
        choiceValue=CategorySchema(name="picker", label="Picker",
                                   definition="http://example.org/picker",
                                   value="a"),
        items=[_quantity_field("alt_a")],
    )


def test_choice_item_missing_name_raises():
    with pytest.raises(ValidationError, match="DataChoice.items"):
        DataChoiceSchema(
            name="alt",
            choiceValue=CategorySchema(name="picker", label="Picker",
                                       definition="http://example.org/picker",
                                       value="a"),
            items=[
                {"type": "Quantity", "label": "X",
                 "definition": "http://example.org/x", "uom": {"code": "m"}},
            ],
        )


def test_vector_coordinate_missing_name_raises():
    with pytest.raises(ValidationError, match="Vector.coordinates"):
        VectorSchema(
            label="Position", definition="http://example.org/position",
            referenceFrame="http://example.org/frames/ENU",
            coordinates=[
                {"type": "Quantity", "label": "X",
                 "definition": "http://example.org/x", "uom": {"code": "m"}},
            ],
        )


def test_dataarray_element_type_missing_name_raises():
    with pytest.raises(ValidationError, match="DataArray.elementType"):
        DataArraySchema(
            elementCount={"type": "Count", "name": "n", "label": "n",
                          "definition": "http://example.org/n"},
            elementType={"type": "Quantity", "label": "X",
                         "definition": "http://example.org/x", "uom": {"code": "m"}},
            encoding="JSONEncoding",
        )


def test_matrix_element_type_missing_name_raises():
    with pytest.raises(ValidationError, match="Matrix.elementType"):
        MatrixSchema(
            elementCount={"type": "Count", "name": "n", "label": "n",
                          "definition": "http://example.org/n"},
            elementType=[
                {"type": "Quantity", "label": "X",
                 "definition": "http://example.org/x", "uom": {"code": "m"}},
            ],
            encoding="JSONEncoding",
        )


# --- A.4 datastream/controlstream wrappers: root requires name -------------

def test_swe_datastream_root_requires_name():
    with pytest.raises(ValidationError, match="SWEDatastreamRecordSchema.recordSchema"):
        SWEDatastreamRecordSchema.model_validate({
            "obsFormat": "application/swe+json",
            "recordSchema": {
                "type": "DataRecord",
                "definition": "urn:osh:data:weather",
                "fields": [VALID_TIME_FIELD],
            },
        })


def test_json_datastream_optional_when_no_schemas_present():
    # Per CS API Part 2 §16.1.4, JSON form may use resultLink instead of
    # inline schemas, so neither resultSchema nor parametersSchema is required.
    OMJSONDatastreamRecordSchema.model_validate({"obsFormat": "application/json"})


def test_json_datastream_result_schema_requires_name_when_present():
    with pytest.raises(ValidationError, match="OMJSONDatastreamRecordSchema.resultSchema"):
        OMJSONDatastreamRecordSchema.model_validate({
            "obsFormat": "application/json",
            "resultSchema": {
                "type": "DataRecord",
                "definition": "urn:osh:data:weather",
                "fields": [VALID_TIME_FIELD],
            },
        })


def test_swe_command_schema_root_requires_name():
    with pytest.raises(ValidationError, match="SWEJSONCommandSchema.recordSchema"):
        SWEJSONCommandSchema.model_validate({
            "commandFormat": "application/swe+json",
            "encoding": {"type": "JSONEncoding"},
            "recordSchema": {
                "type": "DataRecord",
                "definition": "urn:osh:control:cmd",
                "fields": [VALID_TIME_FIELD],
            },
        })


def test_json_command_schema_params_requires_name():
    with pytest.raises(ValidationError, match="JSONCommandSchema.parametersSchema"):
        JSONCommandSchema.model_validate({
            "commandFormat": "application/json",
            "parametersSchema": {
                "type": "DataRecord",
                "definition": "urn:osh:control:params",
                "fields": [VALID_TIME_FIELD],
            },
        })


def test_nested_aggregate_in_record_fields_validated():
    # Aggregate-in-aggregate: a DataRecord inside another DataRecord's fields[].
    # The inner record must itself be named (it's the bound child); its own
    # fields are validated by the inner record's validator independently.
    DataRecordSchema(name="outer", fields=[
        {"type": "DataRecord", "name": "inner", "fields": [VALID_TIME_FIELD]},
    ])
    with pytest.raises(ValidationError, match="DataRecord.fields"):
        DataRecordSchema(name="outer", fields=[
            {"type": "DataRecord", "fields": [VALID_TIME_FIELD]},
        ])


# --- A.5 NameToken pattern -------------------------------------------------

@pytest.mark.parametrize("good_name",
                         ["a", "ab", "wind_speed", "wind-speed", "x1", "X_1-y"])
def test_valid_name_tokens_accepted(good_name):
    DataRecordSchema(name="root", fields=[_quantity_field(good_name)])


@pytest.mark.parametrize("bad_name",
                         ["", "1leading", "with space", "with:colon",
                          "with.dot", "with/slash"])
def test_invalid_name_tokens_rejected(bad_name):
    with pytest.raises(ValidationError):
        DataRecordSchema(name="root", fields=[_quantity_field(bad_name)])


def test_swe_datastream_root_invalid_name_pattern_raises():
    with pytest.raises(ValidationError, match="NameToken"):
        SWEDatastreamRecordSchema.model_validate({
            "obsFormat": "application/swe+json",
            "recordSchema": {
                "type": "DataRecord",
                "name": "1bad-leading-digit",
                "definition": "urn:osh:data:weather",
                "fields": [VALID_TIME_FIELD],
            },
        })


# ===========================================================================
# B. Schema conformance
# ===========================================================================

# --- B.1 spec `required` arrays per leaf type ------------------------------
# Per the JSON schemas, required arrays per type:
#   Quantity:  [type, definition, label, uom]
#   Boolean:   [type, definition, label]
#   Text:      [type, definition, label]
#   Vector:    [type, definition, referenceFrame, coordinates]
#   DataRecord:[type, fields]
#   Geometry:  [type, srs, definition]
#
# `label` is optional everywhere — SWE Common 3 inherits it from
# AbstractDataComponent as optional. OSH emits labelless components
# in the wild (e.g. the SensorLocation Vector); a required `label`
# here would break record-schema parsing during discovery.


def test_quantity_requires_uom():
    with pytest.raises(ValidationError, match="uom"):
        QuantitySchema(label="X", definition="http://example.org/x")


def test_quantity_label_is_optional():
    q = QuantitySchema(definition="http://example.org/x", uom={"code": "m"})
    assert q.label is None


def test_quantity_requires_definition():
    with pytest.raises(ValidationError, match="definition"):
        QuantitySchema(label="X", uom={"code": "m"})


def test_boolean_label_optional_definition_required():
    BooleanSchema(definition="http://example.org/b")  # no label — OK
    with pytest.raises(ValidationError, match="definition"):
        BooleanSchema(label="X")


def test_text_label_optional_definition_required():
    TextSchema(definition="http://example.org/t")  # no label — OK
    with pytest.raises(ValidationError, match="definition"):
        TextSchema(label="X")


def test_vector_requires_definition_referenceframe_coordinates():
    # `label` is intentionally NOT in the required set: SWE Common 3 inherits
    # it from AbstractDataComponent as optional, and OSH emits labelless
    # Vectors (e.g. SensorLocation). See test_vector_label_is_optional…
    base = dict(
        label="V", definition="http://example.org/v",
        referenceFrame="http://example.org/frames/ENU",
        coordinates=[QuantitySchema(name="x", label="X",
                                    definition="http://example.org/x",
                                    uom={"code": "m"})],
    )
    for missing in ("definition", "referenceFrame", "coordinates"):
        kwargs = {k: v for k, v in base.items() if k != missing}
        with pytest.raises(ValidationError):
            VectorSchema(**kwargs)


def test_datarecord_requires_fields():
    with pytest.raises(ValidationError, match="fields"):
        DataRecordSchema(name="r")


def test_geometry_requires_srs_and_definition():
    # `label` deliberately omitted from required set — SWE Common 3
    # inherits it from AbstractDataComponent as optional.
    base = dict(label="G", definition="http://example.org/g",
                srs="http://www.opengis.net/def/crs/EPSG/0/4326")
    for missing in ("definition", "srs"):
        kwargs = {k: v for k, v in base.items() if k != missing}
        with pytest.raises(ValidationError):
            GeometrySchema(**kwargs)


def test_geometry_label_is_optional():
    g = GeometrySchema(definition="http://example.org/g",
                       srs="http://www.opengis.net/def/crs/EPSG/0/4326")
    assert g.label is None


# --- B.2 discriminator routing ---------------------------------------------

DISCRIMINATOR_CASES = [
    ("Boolean",
     {"type": "Boolean", "label": "B", "definition": "http://example.org/b"},
     BooleanSchema),
    ("Count",
     {"type": "Count", "label": "C", "definition": "http://example.org/c"},
     CountSchema),
    ("Quantity",
     {"type": "Quantity", "label": "Q", "definition": "http://example.org/q",
      "uom": {"code": "m"}},
     QuantitySchema),
    ("Time",
     {"type": "Time", "label": "T", "definition": "http://example.org/t",
      "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}},
     TimeSchema),
    ("Category",
     {"type": "Category", "label": "Cat", "definition": "http://example.org/cat"},
     CategorySchema),
    ("Text",
     {"type": "Text", "label": "Tx", "definition": "http://example.org/tx"},
     TextSchema),
    ("CountRange",
     {"type": "CountRange", "label": "CR", "definition": "http://example.org/cr",
      "uom": {"code": "1"}},
     CountRangeSchema),
    ("QuantityRange",
     {"type": "QuantityRange", "label": "QR",
      "definition": "http://example.org/qr", "uom": {"code": "m"}},
     QuantityRangeSchema),
    ("TimeRange",
     {"type": "TimeRange", "label": "TR", "definition": "http://example.org/tr",
      "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}},
     TimeRangeSchema),
    ("CategoryRange",
     {"type": "CategoryRange", "label": "CatR",
      "definition": "http://example.org/catr"},
     CategoryRangeSchema),
    ("DataRecord",
     {"type": "DataRecord", "fields": [_quantity_field("a")]},
     DataRecordSchema),
    ("Vector",
     {"type": "Vector", "label": "V", "definition": "http://example.org/v",
      "referenceFrame": "http://example.org/frames/ENU",
      "coordinates": [_quantity_field("x")]},
     VectorSchema),
    ("DataArray",
     {"type": "DataArray",
      "elementCount": {"type": "Count", "name": "n", "label": "n",
                       "definition": "http://example.org/n"},
      "elementType": _quantity_field("e"),
      "encoding": "JSONEncoding"},
     DataArraySchema),
    ("Matrix",
     {"type": "Matrix",
      "elementCount": {"type": "Count", "name": "n", "label": "n",
                       "definition": "http://example.org/n"},
      "elementType": [_quantity_field("e")],
      "encoding": "JSONEncoding"},
     MatrixSchema),
    ("DataChoice",
     {"type": "DataChoice",
      "choiceValue": {"type": "Category", "name": "pick", "label": "Pick",
                      "definition": "http://example.org/pick"},
      "items": [_quantity_field("a")]},
     DataChoiceSchema),
    ("Geometry",
     {"type": "Geometry", "label": "G", "definition": "http://example.org/g",
      "srs": "http://www.opengis.net/def/crs/EPSG/0/4326"},
     GeometrySchema),
]


@pytest.mark.parametrize("type_literal,payload,expected_cls",
                         DISCRIMINATOR_CASES,
                         ids=[c[0] for c in DISCRIMINATOR_CASES])
def test_anycomponent_discriminator_routes(type_literal, payload, expected_cls):
    parsed = ANY_COMPONENT.validate_python(payload)
    assert isinstance(parsed, expected_cls)
    assert parsed.type == type_literal


def test_anycomponent_unknown_type_rejected():
    with pytest.raises(ValidationError):
        ANY_COMPONENT.validate_python({"type": "NotAType", "label": "X"})


# --- B.3 alias / snake_case parity -----------------------------------------

def test_quantity_axis_id_alias_parity():
    via_alias = QuantitySchema.model_validate({
        "name": "wd", "label": "Wind Direction",
        "definition": "http://example.org/wd",
        "axisID": "z", "uom": {"code": "deg"},
    })
    via_python = QuantitySchema(
        name="wd", label="Wind Direction",
        definition="http://example.org/wd", axis_id="z", uom={"code": "deg"},
    )
    assert via_alias.axis_id == "z" == via_python.axis_id
    assert "axisID" in via_alias.model_dump(by_alias=True, exclude_none=True)


def test_vector_referenceframe_alias_parity():
    payload = {
        "label": "V", "definition": "http://example.org/v",
        "referenceFrame": "http://example.org/frames/ENU",
        "coordinates": [_quantity_field("x")],
    }
    v = VectorSchema.model_validate(payload)
    assert v.reference_frame == "http://example.org/frames/ENU"
    dumped = v.model_dump(by_alias=True, exclude_none=True)
    assert "referenceFrame" in dumped and "reference_frame" not in dumped


def test_swe_datastream_obsformat_recordschema_alias_parity():
    fixture = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    parsed_camel = SWEDatastreamRecordSchema.model_validate(fixture)
    parsed_snake = SWEDatastreamRecordSchema(
        obs_format=fixture["obsFormat"],
        record_schema=fixture["recordSchema"],
    )
    assert parsed_camel.obs_format == parsed_snake.obs_format
    assert parsed_camel.record_schema.name == parsed_snake.record_schema.name


# --- B.4 round-trip fidelity -----------------------------------------------

@pytest.mark.parametrize("fixture_name,model_cls", [
    ("fake_weather_schema_swejson.json", SWEDatastreamRecordSchema),
    ("fake_weather_schema_omjson.json", OMJSONDatastreamRecordSchema),
])
def test_fixture_round_trip_stable(fixture_name, model_cls):
    raw = json.loads((FIXTURES_DIR / fixture_name).read_text())
    first = model_cls.model_validate(raw)
    first_dump = first.model_dump(mode="json", by_alias=True, exclude_none=True)
    second = model_cls.model_validate(first_dump)
    second_dump = second.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert first_dump == second_dump


def test_anycomponent_round_trip_through_typeadapter():
    # Stable-dump: parse → dump → reparse → dump, second dump matches first.
    # We don't compare against the input dict because pydantic adds explicit
    # default values (updatable=False / optional=False) to the dump.
    payload = _quantity_field("temperature")
    first = ANY_COMPONENT.validate_python(payload)
    first_dump = ANY_COMPONENT.dump_python(first, mode="json", by_alias=True,
                                           exclude_none=True)
    second = ANY_COMPONENT.validate_python(first_dump)
    second_dump = ANY_COMPONENT.dump_python(second, mode="json", by_alias=True,
                                            exclude_none=True)
    assert first_dump == second_dump
    for k, v in payload.items():
        assert first_dump[k] == v


# --- B.5 Vector.coordinates element-type restriction -----------------------

def test_vector_rejects_boolean_in_coordinates():
    with pytest.raises(ValidationError):
        VectorSchema.model_validate({
            "label": "V", "definition": "http://example.org/v",
            "referenceFrame": "http://example.org/frames/ENU",
            "coordinates": [{
                "type": "Boolean", "name": "flag", "label": "F",
                "definition": "http://example.org/f",
            }],
        })


def test_vector_rejects_record_in_coordinates():
    with pytest.raises(ValidationError):
        VectorSchema.model_validate({
            "label": "V", "definition": "http://example.org/v",
            "referenceFrame": "http://example.org/frames/ENU",
            "coordinates": [{
                "type": "DataRecord", "name": "inner",
                "fields": [_quantity_field("a")],
            }],
        })


def test_vector_accepts_quantity_in_coordinates():
    VectorSchema.model_validate({
        "label": "V", "definition": "http://example.org/v",
        "referenceFrame": "http://example.org/frames/ENU",
        "coordinates": [_quantity_field("x")],
    })


def test_vector_label_is_optional_per_swe_common3():
    # SWE Common 3 Vector inherits AbstractDataComponent.label as optional;
    # OSH's SensorLocation datastream emits a labelless Vector. A required
    # `label` here would break SWE+JSON schema discovery for any datastream
    # carrying a Vector — see the discover_datastreams cascade.
    v = VectorSchema.model_validate({
        "type": "Vector",
        "name": "location",
        "definition": "http://www.opengis.net/def/property/OGC/0/SensorLocation",
        "referenceFrame": "http://www.opengis.net/def/crs/EPSG/0/4979",
        "coordinates": [_quantity_field("x")],
    })
    assert v.label is None


def test_swe_datastream_schema_parses_osh_sensor_location_shape():
    # End-to-end shape mirroring `GET /datastreams/{id}/schema` for OSH's
    # built-in `sensorLocation` output (CS API SWE+JSON form).
    payload = {
        "obsFormat": "application/swe+json",
        "recordSchema": {
            "type": "DataRecord",
            "name": "sensorLocation",
            "id": "SENSOR_LOCATION",
            "label": "Sensor Location",
            "fields": [
                {
                    "type": "Time",
                    "name": "time",
                    "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                    "label": "Sampling Time",
                    "referenceFrame": "http://www.opengis.net/def/trs/BIPM/0/UTC",
                    "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"},
                },
                {
                    "type": "Vector",
                    "name": "location",
                    "definition": "http://www.opengis.net/def/property/OGC/0/SensorLocation",
                    "referenceFrame": "http://www.opengis.net/def/crs/EPSG/0/4979",
                    "localFrame": "#REF_FRAME_LOCAL",
                    "coordinates": [
                        {"type": "Quantity", "name": "lat", "label": "Geodetic Latitude",
                         "definition": "http://sensorml.com/ont/swe/property/GeodeticLatitude",
                         "axisID": "Lat", "uom": {"code": "deg"}},
                        {"type": "Quantity", "name": "lon", "label": "Longitude",
                         "definition": "http://sensorml.com/ont/swe/property/Longitude",
                         "axisID": "Lon", "uom": {"code": "deg"}},
                        {"type": "Quantity", "name": "alt", "label": "Ellipsoidal Height",
                         "definition": "http://sensorml.com/ont/swe/property/HeightAboveEllipsoid",
                         "axisID": "h", "uom": {"code": "m"}},
                    ],
                },
            ],
        },
    }
    sw = SWEDatastreamRecordSchema.from_swejson_dict(payload)
    vec = sw.record_schema.fields[1]
    assert vec.type == "Vector"
    assert vec.label is None
    assert vec.reference_frame == "http://www.opengis.net/def/crs/EPSG/0/4979"
    assert [c.name for c in vec.coordinates] == ["lat", "lon", "alt"]


# --- B.6 DataRecord.fields minItems: 1 -------------------------------------

def test_datarecord_empty_fields_rejected():
    with pytest.raises(ValidationError):
        DataRecordSchema(name="r", fields=[])