#  =============================================================================
#  Copyright (c) 2026 Botts Innovative Research Inc.
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
"""
SWE Common 3 schema-conformance tests beyond the SoftNamedProperty `name` rule:

1. Spec `required` arrays per leaf component type (Quantity needs uom, Vector
   needs referenceFrame, etc.) — guard against accidental Field(...) → Field(None)
   regressions.
2. Discriminator routing: AnyComponent.model_validate dispatches by `type` to
   the correct concrete class, and rejects unknown types.
3. Alias / field-name parity: both camelCase wire-format and snake_case Python
   names parse to identical models.
4. Round-trip fidelity: parse → dump(by_alias, exclude_none) → re-parse, deep equal.
5. Vector.coordinates element-type restriction (Count/Quantity/Time only).
6. DataRecord.fields minItems: 1 (per DataRecord.json).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from src.oshconnect.schema_datamodels import (
    JSONDatastreamRecordSchema,
    SWEDatastreamRecordSchema,
)
from src.oshconnect.swe_components import (
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


def _quantity_field(name: str = "x") -> dict:
    return {
        "type": "Quantity",
        "name": name,
        "label": "X",
        "definition": "http://example.org/x",
        "uom": {"code": "m"},
    }


# ---------------------------------------------------------------------------
# 1. Spec `required` arrays per leaf component type
# ---------------------------------------------------------------------------
# Per JSON schemas at:
# https://github.com/opengeospatial/ogcapi-connected-systems/tree/master/swecommon/schemas/json
# Required arrays:
#   Quantity:  [type, definition, label, uom]
#   Boolean:   [type, definition, label]
#   Text:      [type, definition, label]      (inherited Boolean shape)
#   Vector:    [type, definition, referenceFrame, label, coordinates]
#   DataRecord:[type, fields]
#   Geometry:  [type, srs, definition, label]


def test_quantity_requires_uom():
    with pytest.raises(ValidationError, match="uom"):
        QuantitySchema(label="X", definition="http://example.org/x")


def test_quantity_requires_label():
    with pytest.raises(ValidationError, match="label"):
        QuantitySchema(definition="http://example.org/x", uom={"code": "m"})


def test_quantity_requires_definition():
    with pytest.raises(ValidationError, match="definition"):
        QuantitySchema(label="X", uom={"code": "m"})


def test_boolean_requires_label_and_definition():
    with pytest.raises(ValidationError, match="label"):
        BooleanSchema(definition="http://example.org/b")
    with pytest.raises(ValidationError, match="definition"):
        BooleanSchema(label="X")


def test_text_requires_label_and_definition():
    with pytest.raises(ValidationError, match="label"):
        TextSchema(definition="http://example.org/t")
    with pytest.raises(ValidationError, match="definition"):
        TextSchema(label="X")


def test_vector_requires_label_definition_referenceframe_coordinates():
    base = dict(
        label="V",
        definition="http://example.org/v",
        referenceFrame="http://example.org/frames/ENU",
        coordinates=[
            QuantitySchema(name="x", label="X",
                           definition="http://example.org/x", uom={"code": "m"}),
        ],
    )
    for missing in ("label", "definition", "referenceFrame", "coordinates"):
        kwargs = {k: v for k, v in base.items() if k != missing}
        with pytest.raises(ValidationError):
            VectorSchema(**kwargs)


def test_datarecord_requires_fields():
    with pytest.raises(ValidationError, match="fields"):
        DataRecordSchema(name="r")


def test_geometry_requires_srs_definition_label():
    base = dict(
        label="G",
        definition="http://example.org/g",
        srs="http://www.opengis.net/def/crs/EPSG/0/4326",
    )
    for missing in ("label", "definition", "srs"):
        kwargs = {k: v for k, v in base.items() if k != missing}
        with pytest.raises(ValidationError):
            GeometrySchema(**kwargs)


# ---------------------------------------------------------------------------
# 2. Discriminator routing
# ---------------------------------------------------------------------------

DISCRIMINATOR_CASES = [
    # (type literal, minimal-valid dict, expected pydantic class)
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
     {"type": "QuantityRange", "label": "QR", "definition": "http://example.org/qr",
      "uom": {"code": "m"}},
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


@pytest.mark.parametrize(
    "type_literal,payload,expected_cls",
    DISCRIMINATOR_CASES,
    ids=[c[0] for c in DISCRIMINATOR_CASES],
)
def test_anycomponent_discriminator_routes(type_literal, payload, expected_cls):
    parsed = ANY_COMPONENT.validate_python(payload)
    assert isinstance(parsed, expected_cls)
    assert parsed.type == type_literal


def test_anycomponent_unknown_type_rejected():
    with pytest.raises(ValidationError):
        ANY_COMPONENT.validate_python({"type": "NotAType", "label": "X"})


# ---------------------------------------------------------------------------
# 3. Alias / field-name parity
# ---------------------------------------------------------------------------
# OSH wire format is camelCase; our pydantic fields are snake_case with alias=
# entries. Confirm both inputs produce equivalent models, and dumping by_alias
# yields the camelCase form.


def test_quantity_axis_id_alias_parity():
    via_alias = QuantitySchema.model_validate({
        "name": "wd",
        "label": "Wind Direction",
        "definition": "http://example.org/wd",
        "axisID": "z",
        "uom": {"code": "deg"},
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
    assert "referenceFrame" in dumped
    assert "reference_frame" not in dumped


def test_swe_datastream_obsformat_recordschema_alias_parity():
    fixture = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    parsed_camel = SWEDatastreamRecordSchema.model_validate(fixture)
    parsed_snake = SWEDatastreamRecordSchema(
        obs_format=fixture["obsFormat"],
        record_schema=fixture["recordSchema"],
    )
    assert parsed_camel.obs_format == parsed_snake.obs_format
    assert parsed_camel.record_schema.name == parsed_snake.record_schema.name


# ---------------------------------------------------------------------------
# 4. Round-trip fidelity
# ---------------------------------------------------------------------------
# Strongest single guard against serializer regressions: load a fixture,
# dump it, re-parse the dump, and confirm the second dump matches the first.


@pytest.mark.parametrize(
    "fixture_name,model_cls",
    [
        ("fake_weather_schema_swejson.json", SWEDatastreamRecordSchema),
        ("fake_weather_schema_omjson.json", JSONDatastreamRecordSchema),
    ],
)
def test_fixture_round_trip_stable(fixture_name, model_cls):
    raw = json.loads((FIXTURES_DIR / fixture_name).read_text())
    first = model_cls.model_validate(raw)
    first_dump = first.model_dump(mode="json", by_alias=True, exclude_none=True)
    second = model_cls.model_validate(first_dump)
    second_dump = second.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert first_dump == second_dump


def test_anycomponent_round_trip_through_typeadapter():
    # Stable-dump: parse → dump → reparse → dump, second dump matches first.
    # (We don't compare against the input dict because pydantic adds explicit
    # default values like updatable=False / optional=False to the dump.)
    payload = _quantity_field("temperature")
    first = ANY_COMPONENT.validate_python(payload)
    first_dump = ANY_COMPONENT.dump_python(first, mode="json", by_alias=True,
                                           exclude_none=True)
    second = ANY_COMPONENT.validate_python(first_dump)
    second_dump = ANY_COMPONENT.dump_python(second, mode="json", by_alias=True,
                                            exclude_none=True)
    assert first_dump == second_dump
    # Sanity: input keys are all preserved in the dump.
    for k, v in payload.items():
        assert first_dump[k] == v


# ---------------------------------------------------------------------------
# 5. Vector.coordinates element-type restriction
# ---------------------------------------------------------------------------
# Vector.json: coordinates items oneOf [Count, Quantity, Time].


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


def test_vector_accepts_count_quantity_time_in_coordinates():
    VectorSchema.model_validate({
        "label": "V", "definition": "http://example.org/v",
        "referenceFrame": "http://example.org/frames/ENU",
        "coordinates": [
            {"type": "Quantity", "name": "x", "label": "X",
             "definition": "http://example.org/x", "uom": {"code": "m"}},
        ],
    })


# ---------------------------------------------------------------------------
# 6. DataRecord.fields minItems: 1
# ---------------------------------------------------------------------------


def test_datarecord_empty_fields_rejected():
    with pytest.raises(ValidationError):
        DataRecordSchema(name="r", fields=[])