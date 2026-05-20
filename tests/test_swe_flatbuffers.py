#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Tests for the ``application/swe+flatbuffers`` placeholder codec.

The codec is currently blocked by an upstream `flatc --python`
limitation (no vector-of-union support); we test that the SDK still
parses/round-trips schemas naming this format, and that the
codec raises a clear `NotImplementedError` instead of failing silently.
"""
from __future__ import annotations

import pytest

from oshconnect import (
    DataRecordSchema, QuantitySchema, SWEFlatBuffersCodec,
    SWEFlatBuffersDatastreamRecordSchema, TimeSchema,
)
from oshconnect.api_utils import UCUMCode, URI


def _minimal_record() -> DataRecordSchema:
    return DataRecordSchema(
        name='r', fields=[
            TimeSchema(name='time', label='Time',
                       definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                       uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
            QuantitySchema(name='x', label='X',
                           definition='http://example.org/x',
                           uom=UCUMCode(code='m', label='m')),
        ],
    )


def test_schema_round_trips_via_any_datastream_record_schema():
    """SDK can still parse + serialize a swe+flatbuffers schema even though
    no codec is wired — discovery / persistence aren't blocked by the codec
    being unimplemented."""
    from oshconnect.resource_datamodels import DatastreamResource

    payload = {
        "id": "ds-fb",
        "name": "fb-stream",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": {
            "obsFormat": "application/swe+flatbuffers",
            "recordSchema": _minimal_record().model_dump(by_alias=True, exclude_none=True),
        },
        "formats": ["application/swe+flatbuffers"],
    }
    ds = DatastreamResource.model_validate(payload, by_alias=True)
    assert isinstance(ds.record_schema, SWEFlatBuffersDatastreamRecordSchema)


def test_encode_raises_notimplemented_with_helpful_message():
    schema = SWEFlatBuffersDatastreamRecordSchema(record_schema=_minimal_record())
    codec = SWEFlatBuffersCodec(schema)
    with pytest.raises(NotImplementedError, match="vector.*union"):
        codec.encode({"time": "2026-01-01T00:00:00Z", "x": 1.0})


def test_decode_raises_notimplemented_with_helpful_message():
    schema = SWEFlatBuffersDatastreamRecordSchema(record_schema=_minimal_record())
    codec = SWEFlatBuffersCodec(schema)
    with pytest.raises(NotImplementedError, match="vector.*union"):
        codec.decode(b"\x00\x00\x00\x00")


def test_pick_schema_format_picks_flatbuffers_when_present():
    """Format picker should advertise swe+flatbuffers even though the codec
    is stubbed — so consumers can still receive and parse the schema; only
    encode/decode is blocked. swe+flatbuffers wins over swe+binary when both
    are listed (mirrors the proto preference)."""
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/swe+flatbuffers", "application/swe+binary",
    ])
    assert obs_fmt == "application/swe+flatbuffers"
    assert parser.__func__ is SWEFlatBuffersDatastreamRecordSchema.from_sweflatbuffers_dict.__func__


def test_codec_rejects_non_schema_input():
    with pytest.raises(TypeError, match="SWEFlatBuffersDatastreamRecordSchema"):
        SWEFlatBuffersCodec(object())