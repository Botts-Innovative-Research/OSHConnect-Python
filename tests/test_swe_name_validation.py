#  =============================================================================
#  Copyright (c) 2026 Botts Innovative Research Inc.
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
"""
SWE Common 3 SoftNamedProperty validation: a `name` is required wherever a
component is bound via SoftNamedProperty (DataRecord.fields, DataChoice.items,
Vector.coordinates, DataArray.elementType, Matrix.elementType, and the root
recordSchema/resultSchema of a datastream/controlstream — i.e.,
DataStream.elementType). Names must match NameToken: ^[A-Za-z][A-Za-z0-9_\\-]*$.

A standalone component (not bound) does NOT require a name; per the spec,
`name` is not a property of any data component itself.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from src.oshconnect.schema_datamodels import (
    JSONDatastreamRecordSchema,
    JSONCommandSchema,
    SWEDatastreamRecordSchema,
    SWEJSONCommandSchema,
)
from src.oshconnect.swe_components import (
    BooleanSchema,
    CategorySchema,
    CountSchema,
    DataArraySchema,
    DataChoiceSchema,
    DataRecordSchema,
    MatrixSchema,
    QuantitySchema,
    TimeSchema,
    VectorSchema,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

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
INVALID_NAMES = ["", "1bad", "with space", "has:colon", "has/slash", "has.dot"]


# ---------------------------------------------------------------------------
# Standalone components do not need a name (positive cases)
# ---------------------------------------------------------------------------

def test_quantity_standalone_no_name_ok():
    q = QuantitySchema(
        label="Air Temperature",
        definition="http://example.org/temperature",
        uom={"code": "Cel"},
    )
    assert q.name is None


def test_vector_standalone_no_name_ok():
    v = VectorSchema(
        label="Position",
        definition="http://example.org/position",
        referenceFrame="http://example.org/frames/ENU",
        coordinates=[
            QuantitySchema(
                name="x", label="X", definition="http://example.org/x", uom={"code": "m"}
            ),
            QuantitySchema(
                name="y", label="Y", definition="http://example.org/y", uom={"code": "m"}
            ),
        ],
    )
    assert v.name is None


def test_existing_swejson_fixture_round_trips():
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text())
    parsed = SWEDatastreamRecordSchema.model_validate(raw)
    re_dumped = parsed.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert re_dumped["recordSchema"]["name"] == "weather"
    assert {f["name"] for f in re_dumped["recordSchema"]["fields"]} == {
        "time", "temperature", "pressure", "windSpeed", "windDirection"
    }


def test_existing_omjson_fixture_round_trips():
    raw = json.loads((FIXTURES_DIR / "fake_weather_schema_omjson.json").read_text())
    parsed = JSONDatastreamRecordSchema.model_validate(raw)
    re_dumped = parsed.model_dump(mode="json", by_alias=True, exclude_none=True)
    assert re_dumped["resultSchema"]["name"] == "weather"


# ---------------------------------------------------------------------------
# DataRecord.fields[*] requires name (negative cases)
# ---------------------------------------------------------------------------

def test_record_with_named_fields_ok():
    DataRecordSchema(
        name="weather",
        fields=[VALID_TIME_FIELD, VALID_TEMP_FIELD],
    )


def test_record_field_missing_name_raises():
    with pytest.raises(ValidationError, match="DataRecord.fields"):
        DataRecordSchema(
            name="weather",
            fields=[
                {
                    "type": "Quantity",
                    "label": "Air Temperature",
                    "definition": "http://example.org/temp",
                    "uom": {"code": "Cel"},
                }
            ],
        )


@pytest.mark.parametrize("bad_name", INVALID_NAMES)
def test_record_field_invalid_name_raises(bad_name):
    with pytest.raises(ValidationError):
        DataRecordSchema(
            name="weather",
            fields=[
                {
                    "type": "Quantity",
                    "name": bad_name,
                    "label": "Air Temperature",
                    "definition": "http://example.org/temp",
                    "uom": {"code": "Cel"},
                }
            ],
        )


# ---------------------------------------------------------------------------
# DataChoice.items[*] requires name
# ---------------------------------------------------------------------------

def test_choice_items_named_ok():
    DataChoiceSchema(
        name="alt",
        choiceValue=CategorySchema(
            name="picker",
            label="Picker",
            definition="http://example.org/picker",
            value="a",
        ),
        items=[
            {
                "type": "Quantity",
                "name": "alt_a",
                "label": "Option A",
                "definition": "http://example.org/a",
                "uom": {"code": "m"},
            }
        ],
    )


def test_choice_item_missing_name_raises():
    with pytest.raises(ValidationError, match="DataChoice.items"):
        DataChoiceSchema(
            name="alt",
            choiceValue=CategorySchema(
                name="picker",
                label="Picker",
                definition="http://example.org/picker",
                value="a",
            ),
            items=[
                {
                    "type": "Quantity",
                    "label": "Option A",
                    "definition": "http://example.org/a",
                    "uom": {"code": "m"},
                }
            ],
        )


# ---------------------------------------------------------------------------
# Vector.coordinates[*] requires name
# ---------------------------------------------------------------------------

def test_vector_coordinate_missing_name_raises():
    with pytest.raises(ValidationError, match="Vector.coordinates"):
        VectorSchema(
            label="Position",
            definition="http://example.org/position",
            referenceFrame="http://example.org/frames/ENU",
            coordinates=[
                {
                    "type": "Quantity",
                    "label": "X",
                    "definition": "http://example.org/x",
                    "uom": {"code": "m"},
                }
            ],
        )


# ---------------------------------------------------------------------------
# DataArray.elementType requires name
# ---------------------------------------------------------------------------

def test_dataarray_element_type_missing_name_raises():
    with pytest.raises(ValidationError, match="DataArray.elementType"):
        DataArraySchema(
            elementCount={"type": "Count", "name": "n", "label": "n",
                          "definition": "http://example.org/n"},
            elementType={
                "type": "Quantity",
                "label": "X",
                "definition": "http://example.org/x",
                "uom": {"code": "m"},
            },
            encoding="JSONEncoding",
        )


# ---------------------------------------------------------------------------
# Matrix.elementType[*] requires name
# ---------------------------------------------------------------------------

def test_matrix_element_type_missing_name_raises():
    with pytest.raises(ValidationError, match="Matrix.elementType"):
        MatrixSchema(
            elementCount={"type": "Count", "name": "n", "label": "n",
                          "definition": "http://example.org/n"},
            elementType=[
                {
                    "type": "Quantity",
                    "label": "X",
                    "definition": "http://example.org/x",
                    "uom": {"code": "m"},
                }
            ],
            encoding="JSONEncoding",
        )


# ---------------------------------------------------------------------------
# Datastream/Controlstream wrappers: root requires name
# ---------------------------------------------------------------------------

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


def test_json_datastream_optional_when_no_schemas_present():
    # Per CS API Part 2 §16.1.4, JSON form may use resultLink instead of
    # inline schemas, so neither resultSchema nor parametersSchema is required.
    JSONDatastreamRecordSchema.model_validate({
        "obsFormat": "application/json",
    })


def test_json_datastream_result_schema_requires_name_when_present():
    with pytest.raises(ValidationError, match="JSONDatastreamRecordSchema.resultSchema"):
        JSONDatastreamRecordSchema.model_validate({
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


# ---------------------------------------------------------------------------
# NameToken pattern coverage
# ---------------------------------------------------------------------------

def test_nested_aggregate_in_record_fields_validated():
    # Aggregate-in-aggregate: a DataRecord inside another DataRecord's fields[]. The
    # inner record must itself be named (it's the bound child); its own fields are then
    # validated by the inner record's validator independently.
    DataRecordSchema(
        name="outer",
        fields=[
            {
                "type": "DataRecord",
                "name": "inner",
                "fields": [VALID_TIME_FIELD],
            }
        ],
    )
    # Inner record present but unnamed → outer's validator catches it.
    with pytest.raises(ValidationError, match="DataRecord.fields"):
        DataRecordSchema(
            name="outer",
            fields=[
                {
                    "type": "DataRecord",
                    "fields": [VALID_TIME_FIELD],
                }
            ],
        )


@pytest.mark.parametrize("good_name", ["a", "ab", "wind_speed", "wind-speed", "x1", "X_1-y"])
def test_valid_name_tokens_accepted(good_name):
    DataRecordSchema(
        name="root",
        fields=[
            {
                "type": "Quantity",
                "name": good_name,
                "label": "X",
                "definition": "http://example.org/x",
                "uom": {"code": "m"},
            }
        ],
    )


@pytest.mark.parametrize("bad_name", ["1leading", "with space", "with:colon", "with.dot", "with/slash"])
def test_invalid_name_tokens_rejected(bad_name):
    with pytest.raises(ValidationError, match="NameToken"):
        DataRecordSchema(
            name="root",
            fields=[
                {
                    "type": "Quantity",
                    "name": bad_name,
                    "label": "X",
                    "definition": "http://example.org/x",
                    "uom": {"code": "m"},
                }
            ],
        )
