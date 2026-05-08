"""SensorML 2.0 JSON-encoding structured-field tests.

Three model classes covered:

- `Term` (identifiers / classifiers)
- `Characteristics` (CharacteristicList — inner `characteristics` array
  of SWE Common components, each requiring a SoftNamedProperty `name`)
- `Capabilities` (CapabilityList — same shape, `capabilities` bucket)

The fixtures mirror what OSH `:8282` returns under
``?f=application/sml+json`` for the bundled Simulated Weather Sensor.
"""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from oshconnect.resource_datamodels import SystemResource
from oshconnect.sensorml import Capabilities, Characteristics, Term
from oshconnect.swe_components import QuantityRangeSchema, QuantitySchema


# ---------------------------------------------------------------------------
# Term
# ---------------------------------------------------------------------------

def test_term_parses_minimum_required_fields():
    t = Term.model_validate({
        "definition": "http://sensorml.com/ont/swe/property/SerialNumber",
        "value": "0123456879",
    })
    assert t.definition == "http://sensorml.com/ont/swe/property/SerialNumber"
    assert t.value == "0123456879"
    assert t.label is None  # optional
    assert t.code_space is None


def test_term_parses_full_osh_shape():
    t = Term.model_validate({
        "definition": "http://sensorml.com/ont/swe/property/SerialNumber",
        "label": "Serial Number",
        "value": "0123456879",
    })
    assert t.label == "Serial Number"


def test_term_round_trips_with_codespace_alias():
    src = Term.model_validate({
        "definition": "http://x/def",
        "value": "abc",
        "codeSpace": "http://x/codes",
    })
    assert src.code_space == "http://x/codes"
    dumped = src.model_dump(by_alias=True, exclude_none=True)
    assert dumped["codeSpace"] == "http://x/codes"
    rebuilt = Term.model_validate(dumped)
    assert rebuilt == src


def test_term_requires_definition():
    with pytest.raises(ValidationError, match="definition"):
        Term.model_validate({"value": "abc"})


def test_term_requires_value():
    with pytest.raises(ValidationError, match="value"):
        Term.model_validate({"definition": "http://x/def"})


def test_term_extra_fields_round_trip():
    """OSH may add fields the spec hasn't standardized — `extra='allow'`
    keeps them on round-trip."""
    src = Term.model_validate({
        "definition": "http://x/def",
        "value": "v",
        "futureField": "preserved",
    })
    dumped = src.model_dump(by_alias=True, exclude_none=True)
    assert dumped["futureField"] == "preserved"


# ---------------------------------------------------------------------------
# Characteristics
# ---------------------------------------------------------------------------

OSH_CHARACTERISTICS = {
    "definition": "http://www.w3.org/ns/ssn/systems/OperatingRange",
    "label": "Operating Characteristics",
    "characteristics": [
        {"type": "QuantityRange", "name": "voltage",
         "definition": "http://qudt.org/vocab/quantitykind/Voltage",
         "label": "Operating Voltage Range",
         "uom": {"code": "V"}, "value": [110.0, 250.0]},
        {"type": "QuantityRange", "name": "temperature",
         "definition": "http://qudt.org/vocab/quantitykind/Temperature",
         "label": "Temperature Range",
         "uom": {"code": "Cel"}, "value": [-20.0, 90.0]},
    ],
}


def test_characteristics_parses_osh_shape():
    c = Characteristics.model_validate(OSH_CHARACTERISTICS)
    assert c.label == "Operating Characteristics"
    assert len(c.characteristics) == 2
    # Inner components are routed via AnyComponent's `type` discriminator
    # to the right concrete subclass.
    assert all(isinstance(x, QuantityRangeSchema) for x in c.characteristics)
    assert c.characteristics[0].name == "voltage"
    assert c.characteristics[0].value == [110.0, 250.0]


def test_characteristics_round_trips_through_json():
    src = Characteristics.model_validate(OSH_CHARACTERISTICS)
    dumped = src.model_dump_json(by_alias=True, exclude_none=True)
    rebuilt = Characteristics.model_validate(json.loads(dumped))
    # Inner component types still resolve correctly post-round-trip.
    assert rebuilt.characteristics[0].name == "voltage"
    assert isinstance(rebuilt.characteristics[0], QuantityRangeSchema)


def test_characteristics_inner_component_must_carry_name():
    """Inner components are bound via SoftNamedProperty — `name` is
    required at the binding site even though it's optional on the
    component class itself. Mirrors `DataRecord.fields` and
    `Vector.coordinates` validation."""
    payload = {
        "definition": "http://x/range",
        "characteristics": [
            {"type": "Quantity",  # missing `name`
             "definition": "http://x/q",
             "uom": {"code": "m"}},
        ],
    }
    with pytest.raises(ValidationError, match="name"):
        Characteristics.model_validate(payload)


def test_characteristics_definition_and_label_optional():
    """The spec marks `definition` and `label` optional on the list
    container itself — only the inner components are required."""
    c = Characteristics.model_validate({
        "characteristics": [
            {"type": "Quantity", "name": "x",
             "definition": "http://x/q", "uom": {"code": "m"}},
        ],
    })
    assert c.definition is None
    assert c.label is None
    assert len(c.characteristics) == 1


# ---------------------------------------------------------------------------
# Capabilities (isomorphic to Characteristics, different bucket name)
# ---------------------------------------------------------------------------

def test_capabilities_parses_with_inner_quantity():
    payload = {
        "definition": "http://example.org/caps/Range",
        "label": "Sensor Caps",
        "capabilities": [
            {"type": "Quantity", "name": "accuracy",
             "definition": "http://example.org/Accuracy",
             "label": "Accuracy", "uom": {"code": "%"},
             "value": 0.5},
        ],
    }
    c = Capabilities.model_validate(payload)
    assert c.label == "Sensor Caps"
    assert isinstance(c.capabilities[0], QuantitySchema)
    assert c.capabilities[0].name == "accuracy"
    assert c.capabilities[0].value == 0.5


def test_capabilities_inner_component_must_carry_name():
    with pytest.raises(ValidationError, match="name"):
        Capabilities.model_validate({
            "capabilities": [
                {"type": "Quantity", "definition": "http://x/q",
                 "uom": {"code": "m"}},
            ],
        })


def test_capabilities_round_trips():
    payload = {
        "label": "Caps",
        "capabilities": [
            {"type": "Quantity", "name": "speed",
             "definition": "http://x/speed", "uom": {"code": "m/s"},
             "value": 12.5},
        ],
    }
    src = Capabilities.model_validate(payload)
    js = src.model_dump_json(by_alias=True, exclude_none=True)
    back = Capabilities.model_validate(json.loads(js))
    assert isinstance(back.capabilities[0], QuantitySchema)
    assert back.capabilities[0].value == 12.5


# ---------------------------------------------------------------------------
# Integration: SystemResource carrying typed identifiers + characteristics
# ---------------------------------------------------------------------------

OSH_LIVE_SYSTEM = {
    "type": "PhysicalSystem",
    "id": "03ie1mkrr9r0",
    "uniqueId": "urn:osh:sensor:simweather:0123456879",
    "definition": "http://www.w3.org/ns/sosa/Sensor",
    "label": "New Simulated Weather Sensor",
    "description": "Simulated weather station generating realistic pseudo-random measurements",
    "identifiers": [
        {"definition": "http://sensorml.com/ont/swe/property/SerialNumber",
         "label": "Serial Number", "value": "0123456879"},
    ],
    "validTime": ["2026-04-05T03:54:09.165Z", "now"],
    "characteristics": [OSH_CHARACTERISTICS],
}


def test_system_resource_typed_identifiers_and_characteristics():
    """End-to-end: parse the OSH live SML+JSON listing payload through
    `SystemResource`, assert identifiers/characteristics arrive as the
    proper typed models, and that round-trip preserves the structure."""
    s = SystemResource.model_validate(OSH_LIVE_SYSTEM, by_alias=True)

    assert isinstance(s.identifiers[0], Term)
    assert s.identifiers[0].value == "0123456879"

    assert isinstance(s.characteristics[0], Characteristics)
    inner = s.characteristics[0].characteristics
    assert len(inner) == 2
    assert isinstance(inner[0], QuantityRangeSchema)
    assert inner[0].name == "voltage"
    assert inner[0].value == [110.0, 250.0]

    # Full round-trip: dump → re-parse → same structure.
    dumped = s.model_dump(by_alias=True, exclude_none=True, mode='json')
    rebuilt = SystemResource.model_validate(dumped, by_alias=True)
    assert isinstance(rebuilt.identifiers[0], Term)
    assert isinstance(rebuilt.characteristics[0], Characteristics)
    assert rebuilt.characteristics[0].characteristics[0].name == "voltage"
