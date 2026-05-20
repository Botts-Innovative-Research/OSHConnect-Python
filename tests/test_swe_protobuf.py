#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Tests for the ``application/swe+proto`` codec.

The generated protobuf bindings (`sweCommon3_pb2` and friends) live in the
separate BinaryEncodings project and are not bundled with OSHConnect.
Tests that round-trip real wire bytes are gated on the modules being
importable — set ``PYTHONPATH`` to include the project's
``gen/protobuf`` directory, or symlink it under any importable path.

Default lookup path: ``$BINARY_ENCODINGS_GEN`` (env var) or
``~/IdeaProjects/BinaryEncodings/gen/protobuf``. Override per-run via
the env var.
"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest

from oshconnect import (
    BooleanSchema, CategorySchema, CountSchema, DataRecordSchema,
    QuantitySchema, SWEProtobufCodec, SWEProtobufDatastreamRecordSchema,
    TextSchema, TimeSchema,
)
from oshconnect.api_utils import UCUMCode, URI
from oshconnect.swe_components import (  # noqa: F401
    DataArraySchema, DataChoiceSchema, VectorSchema,
)


def _ensure_pb_path() -> bool:
    """Prepend the generated protobuf bindings directory to sys.path."""
    candidate = Path(
        os.environ.get(
            "BINARY_ENCODINGS_GEN",
            os.path.expanduser("~/IdeaProjects/BinaryEncodings/gen/protobuf"),
        )
    )
    if (candidate / "sweCommon3_pb2.py").is_file():
        path_str = str(candidate)
        if path_str not in sys.path:
            sys.path.insert(0, path_str)
        return True
    return False


_HAS_PB = _ensure_pb_path()


pytestmark = pytest.mark.skipif(
    not _HAS_PB,
    reason="Generated SWE Common 3 protobuf bindings not found; "
           "set BINARY_ENCODINGS_GEN or generate via "
           "`make protobuf PROTO_LANG=python` in the BinaryEncodings repo.",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _scalar_record() -> DataRecordSchema:
    """A 5-scalar record covering Time/Quantity/Count/Boolean/Text."""
    return DataRecordSchema(
        name='weather', label='Weather',
        definition='http://example.org/weather',
        fields=[
            TimeSchema(name='time', label='Time',
                       definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                       uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
            QuantitySchema(name='temp', label='Temperature',
                           definition='http://example.org/temp',
                           uom=UCUMCode(code='Cel', label='Celsius')),
            CountSchema(name='samples', label='Samples',
                        definition='http://example.org/samples',
                        uom=UCUMCode(code='1', label='dimensionless')),
            BooleanSchema(name='clear_sky', label='Clear Sky',
                          definition='http://example.org/clearsky'),
            TextSchema(name='note', label='Note',
                       definition='http://example.org/note'),
        ],
    )


# ---------------------------------------------------------------------------
# Encoding markers
# ---------------------------------------------------------------------------


def test_schema_carries_protobuf_encoding_marker():
    """The default `record_encoding` should be a ProtobufEncoding marker."""
    from oshconnect import ProtobufEncoding
    schema = SWEProtobufDatastreamRecordSchema(record_schema=_scalar_record())
    assert isinstance(schema.record_encoding, ProtobufEncoding)
    assert schema.obs_format == "application/swe+proto"


def test_schema_dispatches_via_any_datastream_record_schema():
    """Round-trip the protobuf record schema through DatastreamResource —
    the discriminated union has to route the literal `swe+proto` to
    `SWEProtobufDatastreamRecordSchema`."""
    from oshconnect.resource_datamodels import DatastreamResource

    payload = {
        "id": "ds-proto",
        "name": "proto-stream",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": {
            "obsFormat": "application/swe+proto",
            "recordSchema": _scalar_record().model_dump(by_alias=True, exclude_none=True),
        },
        "formats": ["application/swe+proto"],
    }
    ds = DatastreamResource.model_validate(payload, by_alias=True)
    assert isinstance(ds.record_schema, SWEProtobufDatastreamRecordSchema)


# ---------------------------------------------------------------------------
# Scalar round-trips
# ---------------------------------------------------------------------------


def test_round_trip_all_scalars():
    schema = SWEProtobufDatastreamRecordSchema(record_schema=_scalar_record())
    codec = SWEProtobufCodec(schema)
    value = {
        'time': '2026-05-19T19:21:15.807Z',
        'temp': 23.5,
        'samples': 42,
        'clear_sky': True,
        'note': 'sunny',
    }
    wire = codec.encode(value)
    assert isinstance(wire, bytes)
    assert len(wire) > 0
    assert codec.decode(wire) == value


def test_time_accepts_numeric_epoch():
    """`TimeSchema` is wire-permissive: epoch seconds (numeric) or ISO 8601
    string both serialize; the round-trip preserves whichever shape went in."""
    schema = SWEProtobufDatastreamRecordSchema(
        record_schema=DataRecordSchema(
            name='r', fields=[
                TimeSchema(name='t', label='T',
                           definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                           uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
            ],
        )
    )
    codec = SWEProtobufCodec(schema)
    assert codec.decode(codec.encode({'t': 1_779_218_475.807}))['t'] == pytest.approx(1_779_218_475.807)
    assert codec.decode(codec.encode({'t': '2026-05-19T19:21:15Z'}))['t'] == '2026-05-19T19:21:15Z'


def test_category_round_trip():
    rec = DataRecordSchema(
        name='r', fields=[
            CategorySchema(name='state', label='State',
                           definition='http://example.org/state',
                           code_space='http://example.org/codes'),
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=rec))
    assert codec.decode(codec.encode({'state': 'on'}))['state'] == 'on'


# ---------------------------------------------------------------------------
# Composite types
# ---------------------------------------------------------------------------


def test_nested_data_record_round_trip():
    """A DataRecord-in-a-DataRecord should preserve field names across both
    layers — the schema-aware decoder pairs proto fields by name, not order."""
    inner = DataRecordSchema(
        name='inner', label='Inner',
        definition='http://example.org/inner',
        fields=[
            QuantitySchema(name='lat', label='Lat',
                           definition='http://example.org/lat',
                           uom=UCUMCode(code='deg', label='deg')),
            QuantitySchema(name='lon', label='Lon',
                           definition='http://example.org/lon',
                           uom=UCUMCode(code='deg', label='deg')),
        ],
    )
    outer = DataRecordSchema(
        name='outer', label='Outer',
        definition='http://example.org/outer',
        fields=[
            TimeSchema(name='time', label='Time',
                       definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                       uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
            inner,
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=outer))
    value = {'time': '2026-05-19T00:00:00Z',
             'inner': {'lat': 12.5, 'lon': -42.0}}
    assert codec.decode(codec.encode(value)) == value


def test_vector_round_trip():
    schema = DataRecordSchema(
        name='r', fields=[
            VectorSchema(
                name='pos', label='Position',
                definition='http://example.org/pos',
                reference_frame='http://example.org/frame',
                coordinates=[
                    QuantitySchema(name='x', label='X',
                                   definition='http://example.org/x',
                                   uom=UCUMCode(code='m', label='m')),
                    QuantitySchema(name='y', label='Y',
                                   definition='http://example.org/y',
                                   uom=UCUMCode(code='m', label='m')),
                    QuantitySchema(name='z', label='Z',
                                   definition='http://example.org/z',
                                   uom=UCUMCode(code='m', label='m')),
                ],
            ),
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=schema))
    value = {'pos': [1.0, 2.0, 3.0]}
    out = codec.decode(codec.encode(value))
    assert out == value


def test_data_array_round_trip_with_heterogeneous_values():
    """Real round-trip test for DataArray<Quantity>. Wire format mirrors
    OSH's BinaryDataWriter: tightly-packed scalars in EncodedValues.inline_data,
    with the BinaryEncoding declared inline.

    This is the canary against the pre-fix bug where the encoder silently
    dropped all but the first element and the decoder returned [v0]*n.
    """
    rec = DataRecordSchema(
        name='r', fields=[
            DataArraySchema(
                name='samples', label='Samples',
                definition='http://example.org/samples',
                element_count={'value': 3},
                element_type=QuantitySchema(
                    name='x', label='X',
                    definition='http://example.org/x',
                    uom=UCUMCode(code='m', label='m')),
            ),
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=rec))
    value = {'samples': [1.0, 2.0, 3.0]}
    assert codec.decode(codec.encode(value)) == value


def test_data_array_of_counts_round_trip():
    """Default dataType for Count is signedInt (4 bytes BE), matching OSH."""
    rec = DataRecordSchema(
        name='r', fields=[
            DataArraySchema(
                name='ids', label='IDs',
                definition='http://example.org/ids',
                element_count={'value': 4},
                element_type=CountSchema(
                    name='id', label='ID',
                    definition='http://example.org/id',
                    uom=UCUMCode(code='1', label='dimensionless')),
            ),
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=rec))
    value = {'ids': [7, 11, 13, 17]}
    assert codec.decode(codec.encode(value)) == value


def test_data_array_of_records_raises_clear_error():
    """Arrays of records are valid SWE Common 3 but not yet wired in the
    Python codec. Raise rather than silently producing wrong bytes."""
    inner = DataRecordSchema(
        name='inner', label='Inner',
        definition='http://example.org/inner',
        fields=[
            QuantitySchema(name='x', label='X',
                           definition='http://example.org/x',
                           uom=UCUMCode(code='m', label='m')),
        ],
    )
    rec = DataRecordSchema(
        name='r', fields=[
            DataArraySchema(
                name='samples', label='Samples',
                definition='http://example.org/samples',
                element_count={'value': 2},
                element_type=inner,
            ),
        ],
    )
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=rec))
    with pytest.raises(TypeError, match="DataArray.element_type"):
        codec.encode({'samples': [{'x': 1.0}, {'x': 2.0}]})


def test_picker_prefers_proto_over_flatbuffers():
    """When both encodings are advertised, swe+proto wins because the
    flatbuffers codec is currently a stub. This guards against a regression
    where the picker silently routes traffic to the broken codec."""
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/swe+flatbuffers", "application/swe+proto",
    ])
    assert obs_fmt == "application/swe+proto"
    assert parser.__func__ is SWEProtobufDatastreamRecordSchema.from_sweproto_dict.__func__


def test_missing_field_raises_keyerror():
    rec = _scalar_record()
    codec = SWEProtobufCodec(SWEProtobufDatastreamRecordSchema(record_schema=rec))
    with pytest.raises(KeyError, match="missing from value mapping"):
        codec.encode({'time': '2026-01-01T00:00:00Z'})  # other fields absent


# ---------------------------------------------------------------------------
# Wiring through Datastream
# ---------------------------------------------------------------------------


def test_datastream_insert_routes_through_protobuf_codec():
    """`Datastream.insert(...)` dispatches via `_encode_for_wire`, which must
    pick the protobuf codec when the schema's obsFormat is swe+proto."""
    from oshconnect.resource_datamodels import DatastreamResource
    from oshconnect.resources.datastream import Datastream

    class _StubNode:
        def register_streamable(self, _s): pass
        def get_mqtt_client(self): return None

    payload = {
        "id": "ds-proto",
        "name": "proto",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": {
            "obsFormat": "application/swe+proto",
            "recordSchema": _scalar_record().model_dump(by_alias=True, exclude_none=True),
        },
        "formats": ["application/swe+proto"],
    }
    ds_resource = DatastreamResource.model_validate(payload, by_alias=True)
    ds = Datastream(parent_node=_StubNode(), datastream_resource=ds_resource)
    captured: list[bytes] = []
    ds._topic = "t"
    ds._publish_mqtt = lambda topic, p: captured.append(p)

    value = {'time': '2026-01-01T00:00:00Z', 'temp': 7.0,
             'samples': 1, 'clear_sky': False, 'note': 'x'}
    ds.insert(value)
    assert len(captured) == 1
    # Decode it back to confirm wire fidelity end-to-end
    assert ds.decode_observation(captured[0]) == value


def test_pick_schema_format_picks_protobuf_when_present():
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/om+json", "application/swe+proto",
    ])
    assert obs_fmt == "application/swe+proto"
    assert parser.__func__ is SWEProtobufDatastreamRecordSchema.from_sweproto_dict.__func__


def test_pick_schema_format_prefers_swe_json_over_proto():
    """swe+json wins when both are advertised — protobuf is the fallback when
    JSON isn't available, mirroring the swe+binary fallback for video."""
    from oshconnect.resources.system import System
    from oshconnect import SWEDatastreamRecordSchema
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/swe+json", "application/swe+proto",
    ])
    assert obs_fmt == "application/swe+json"
    assert parser.__func__ is SWEDatastreamRecordSchema.from_swejson_dict.__func__