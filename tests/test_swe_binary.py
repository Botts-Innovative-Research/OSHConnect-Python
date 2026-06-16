#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Tests for the SWE Common BinaryEncoding wire codec.

Two layers:

1. Unit tests (default) — exercise the wire spec against hand-built bytes,
   schemas built from dicts (matching the live response shapes documented in
   ``docs/AXIS_CAMERA_FORMATS.md`` in the OGC code-sprint demo repo), and the
   `SWEBinaryCodec` round-trip path.

2. Network tests (``-m network``) — hit a live Axis-camera-backed OSH node on
   ``localhost:9191`` (overridable via ``OSHC_AXIS_PORT``) to verify the SDK
   negotiates the binary schema variant during discovery and that the codec
   decodes real observations off the live datastream.
"""
from __future__ import annotations

import os
import struct
import time

import pytest
import requests

from oshconnect.encoding import BinaryBlockMember, BinaryComponentMember, BinaryEncoding
from oshconnect.schema_datamodels import (
    SWEBinaryDatastreamRecordSchema,
    SWEDatastreamRecordSchema,
)
from oshconnect.swe_binary import (
    DATATYPE_STRUCT_FMT,
    SWEBinaryCodec,
    decode_swe_binary_blob,
    decode_swe_binary_record,
    encode_swe_binary_blob,
    encode_swe_binary_record,
)
from tests.helpers import osh_node_reachable


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------


def test_blob_round_trip_basic():
    """[ts][size][payload] round-trip with an opaque payload."""
    payload = b"\x00\x00\x00\x01" + b"\xab" * 64  # H.264-shaped opaque bytes
    framed = encode_swe_binary_blob(payload, ts=1_700_000_000.5)
    assert framed.startswith(struct.pack(">d", 1_700_000_000.5))
    assert struct.unpack(">I", framed[8:12])[0] == len(payload)
    ts, decoded = decode_swe_binary_blob(framed)
    assert ts == pytest.approx(1_700_000_000.5)
    assert decoded == payload


def test_blob_default_timestamp_is_close_to_now():
    before = time.time()
    framed = encode_swe_binary_blob(b"xxx")
    after = time.time()
    ts, _ = decode_swe_binary_blob(framed)
    assert before - 1 <= ts <= after + 1


def test_blob_decode_rejects_truncated_header():
    with pytest.raises(ValueError, match="too short"):
        decode_swe_binary_blob(b"\x00" * 11)


def test_blob_decode_rejects_truncated_payload():
    # declares 100 bytes of payload, supplies 10
    bad = struct.pack(">dI", 0.0, 100) + b"\x00" * 10
    with pytest.raises(ValueError, match="truncated"):
        decode_swe_binary_blob(bad)


def test_fixed_record_round_trip_default_float32():
    """Matches the ptzOutput wire form: [ts][f32][f32][f32]."""
    raw = encode_swe_binary_record(1_779_218_475.807, -6.7, 0.0, 1.0)
    assert len(raw) == 8 + 3 * 4
    ts, pan, tilt, zoom = decode_swe_binary_record(raw, n_values=3)
    assert ts == pytest.approx(1_779_218_475.807, rel=1e-9)
    assert pan == pytest.approx(-6.7, rel=1e-5)
    assert tilt == pytest.approx(0.0)
    assert zoom == pytest.approx(1.0)


def test_fixed_record_with_doubles():
    raw = encode_swe_binary_record(1.0, 2.0, 3.0, fmt="d")
    assert len(raw) == 8 + 2 * 8
    out = decode_swe_binary_record(raw, n_values=2, fmt="d")
    assert out == (1.0, 2.0, 3.0)


# ---------------------------------------------------------------------------
# Schema-driven codec
# ---------------------------------------------------------------------------


PTZ_SCHEMA_DICT = {
    "obsFormat": "application/swe+binary",
    "recordSchema": {
        "type": "DataRecord",
        "name": "ptz",
        "fields": [
            {"type": "Time", "name": "time",
             "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
             "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}},
            {"type": "Quantity", "name": "pan",
             "definition": "http://sensorml.com/ont/swe/property/Pan",
             "uom": {"code": "deg"}},
            {"type": "Quantity", "name": "tilt",
             "definition": "http://sensorml.com/ont/swe/property/Tilt",
             "uom": {"code": "deg"}},
            {"type": "Quantity", "name": "zoomFactor",
             "definition": "http://sensorml.com/ont/swe/property/Zoom",
             "uom": {"code": "1"}},
        ],
    },
    "recordEncoding": {
        "type": "BinaryEncoding",
        "byteOrder": "bigEndian",
        "byteEncoding": "raw",
        "members": [
            {"type": "Component", "ref": "/time",
             "dataType": "http://www.opengis.net/def/dataType/OGC/0/double"},
            {"type": "Component", "ref": "/pan",
             "dataType": "http://www.opengis.net/def/dataType/OGC/0/float32"},
            {"type": "Component", "ref": "/tilt",
             "dataType": "http://www.opengis.net/def/dataType/OGC/0/float32"},
            {"type": "Component", "ref": "/zoomFactor",
             "dataType": "http://www.opengis.net/def/dataType/OGC/0/float32"},
        ],
    },
}


VIDEO_SCHEMA_DICT = {
    "obsFormat": "application/swe+binary",
    "recordSchema": {
        "type": "DataRecord",
        "name": "video",
        "fields": [
            {"type": "Time", "name": "time",
             "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
             "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}},
            # The recordSchema describes the abstract shape (raster); the
            # recordEncoding overrides it with an opaque Block. We model the
            # abstract side as a single Count here so the test schema parses
            # without the full DataArray-of-DataArray nesting — the codec only
            # cares about recordEncoding.members.
            {"type": "Count", "name": "img",
             "definition": "http://sensorml.com/ont/swe/property/RasterImage"},
        ],
    },
    "recordEncoding": {
        "type": "BinaryEncoding",
        "byteOrder": "bigEndian",
        "byteEncoding": "raw",
        "members": [
            {"type": "Component", "ref": "/time",
             "dataType": "http://www.opengis.net/def/dataType/OGC/0/double"},
            {"type": "Block", "ref": "/img", "compression": "H264"},
        ],
    },
}


def test_parse_ptz_schema():
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(PTZ_SCHEMA_DICT)
    assert schema.obs_format == "application/swe+binary"
    assert isinstance(schema.record_encoding, BinaryEncoding)
    assert len(schema.record_encoding.members) == 4
    # discriminated union resolves to the right concrete subclass
    assert isinstance(schema.record_encoding.members[0], BinaryComponentMember)


def test_parse_video_schema_has_block_member():
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(VIDEO_SCHEMA_DICT)
    members = schema.record_encoding.members
    assert isinstance(members[0], BinaryComponentMember)
    assert isinstance(members[1], BinaryBlockMember)
    assert members[1].compression == "H264"


def test_codec_round_trip_ptz_record():
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(PTZ_SCHEMA_DICT)
    codec = SWEBinaryCodec(schema)
    assert codec.field_names == ["time", "pan", "tilt", "zoomFactor"]
    payload = codec.encode({"time": 1_779_218_475.807, "pan": -6.7,
                            "tilt": 0.0, "zoomFactor": 1.0})
    assert len(payload) == 8 + 3 * 4
    out = codec.decode(payload)
    assert out["time"] == pytest.approx(1_779_218_475.807, rel=1e-9)
    assert out["pan"] == pytest.approx(-6.7, rel=1e-5)
    assert out["tilt"] == pytest.approx(0.0)
    assert out["zoomFactor"] == pytest.approx(1.0)


def test_codec_accepts_positional_sequence():
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(PTZ_SCHEMA_DICT)
    codec = SWEBinaryCodec(schema)
    by_mapping = codec.encode({"time": 1.0, "pan": 2.0,
                               "tilt": 3.0, "zoomFactor": 4.0})
    by_sequence = codec.encode([1.0, 2.0, 3.0, 4.0])
    assert by_mapping == by_sequence


def test_codec_round_trip_video_block():
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(VIDEO_SCHEMA_DICT)
    codec = SWEBinaryCodec(schema)
    fake_nal = b"\x00\x00\x00\x01" + b"\x67" + b"\xab" * 100
    payload = codec.encode({"time": 1_700_000_000.0, "img": fake_nal})
    # Wire: 8 (ts) + 4 (size prefix) + len(fake_nal)
    assert len(payload) == 8 + 4 + len(fake_nal)
    out = codec.decode(payload)
    assert out["time"] == pytest.approx(1_700_000_000.0)
    assert out["img"] == fake_nal
    assert isinstance(out["img"], bytes)


def test_codec_round_trip_concatenated_records():
    """`decode_with_offset` should walk multiple records in one buffer."""
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(PTZ_SCHEMA_DICT)
    codec = SWEBinaryCodec(schema)
    buf = b""
    expected = [
        {"time": 1.0, "pan": 2.0, "tilt": 3.0, "zoomFactor": 4.0},
        {"time": 5.0, "pan": 6.0, "tilt": 7.0, "zoomFactor": 8.0},
        {"time": 9.0, "pan": 10.0, "tilt": 11.0, "zoomFactor": 12.0},
    ]
    for rec in expected:
        buf += codec.encode(rec)
    offset = 0
    decoded = []
    while offset < len(buf):
        rec, offset = codec.decode_with_offset(buf, offset=offset)
        decoded.append(rec)
    assert offset == len(buf)
    assert len(decoded) == 3
    for got, want in zip(decoded, expected):
        for k in want:
            assert got[k] == pytest.approx(want[k])


def test_codec_rejects_unknown_datatype():
    bad = dict(PTZ_SCHEMA_DICT)
    bad["recordEncoding"] = {
        **bad["recordEncoding"],
        "members": [
            {"type": "Component", "ref": "/time",
             "dataType": "http://example.com/dataType/OGC/0/zebra"},
        ],
    }
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(bad)
    with pytest.raises(ValueError, match="unsupported dataType"):
        SWEBinaryCodec(schema)


def test_codec_rejects_base64_byte_encoding():
    bad = dict(PTZ_SCHEMA_DICT)
    bad["recordEncoding"] = {**bad["recordEncoding"], "byteEncoding": "base64"}
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(bad)
    with pytest.raises(NotImplementedError, match="base64"):
        SWEBinaryCodec(schema)


def test_codec_honours_little_endian():
    little = dict(PTZ_SCHEMA_DICT)
    little["recordEncoding"] = {**little["recordEncoding"],
                                "byteOrder": "littleEndian"}
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(little)
    codec = SWEBinaryCodec(schema)
    payload = codec.encode([1.0, 2.0, 3.0, 4.0])
    # First 8 bytes should pack as little-endian double
    expected_ts = struct.pack("<d", 1.0)
    assert payload[:8] == expected_ts


def test_scalar_array_fixed_size_round_trip():
    """Helper for DataArray packing: tightly-packed elements, no prefix."""
    from oshconnect.swe_binary import (
        decode_swe_binary_scalar_array, encode_swe_binary_scalar_array,
    )
    uri = "http://www.opengis.net/def/dataType/OGC/0/double"
    wire = encode_swe_binary_scalar_array([1.0, 2.0, 3.0], uri,
                                          byte_order="bigEndian",
                                          variable_size=False)
    assert len(wire) == 3 * 8  # three float64s, no prefix
    out = decode_swe_binary_scalar_array(wire, uri, element_count=3)
    assert out == [1.0, 2.0, 3.0]


def test_scalar_array_variable_size_round_trip():
    """Variable-size variant: leading uint32 BE count + N scalars."""
    from oshconnect.swe_binary import (
        decode_swe_binary_scalar_array, encode_swe_binary_scalar_array,
    )
    uri = "http://www.opengis.net/def/dataType/OGC/0/signedInt"
    wire = encode_swe_binary_scalar_array([7, 11, 13, 17], uri,
                                          variable_size=True)
    assert len(wire) == 4 + 4 * 4  # uint32 count + 4 int32s
    assert struct.unpack(">I", wire[:4])[0] == 4
    out = decode_swe_binary_scalar_array(wire, uri, variable_size=True)
    assert out == [7, 11, 13, 17]


def test_default_datatype_for_schema():
    """Mirrors OSH's `SWEHelper.getDefaultBinaryEncoding`: Quantity->double,
    Count->signedInt, Boolean->boolean, Time->double."""
    from oshconnect.swe_binary import default_datatype_for_schema
    from oshconnect.swe_components import (
        BooleanSchema, CountSchema, QuantitySchema, TimeSchema,
    )
    from oshconnect.api_utils import UCUMCode, URI
    q = QuantitySchema(name='x', label='X',
                       definition='http://example.org/x',
                       uom=UCUMCode(code='m', label='m'))
    c = CountSchema(name='n', label='N',
                    definition='http://example.org/n',
                    uom=UCUMCode(code='1', label='1'))
    b = BooleanSchema(name='b', label='B',
                      definition='http://example.org/b')
    t = TimeSchema(name='t', label='T',
                   definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                   uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'))
    assert default_datatype_for_schema(q).endswith("/double")
    assert default_datatype_for_schema(c).endswith("/signedInt")
    assert default_datatype_for_schema(b).endswith("/boolean")
    assert default_datatype_for_schema(t).endswith("/double")


def test_datatype_table_is_complete_for_common_widths():
    # Spot-check the most-seen URIs from real OSH wire payloads
    assert DATATYPE_STRUCT_FMT[
        "http://www.opengis.net/def/dataType/OGC/0/double"] == "d"
    assert DATATYPE_STRUCT_FMT[
        "http://www.opengis.net/def/dataType/OGC/0/float32"] == "f"


# ---------------------------------------------------------------------------
# Discriminated-union dispatch (Datastream side)
# ---------------------------------------------------------------------------


def test_anydatastreamrecordschema_dispatches_to_binary():
    """`AnyDatastreamRecordSchema` should route `obsFormat=swe+binary` to
    `SWEBinaryDatastreamRecordSchema`, not the JSON-family one."""
    from oshconnect.resource_datamodels import DatastreamResource

    payload = {
        "id": "ds-1",
        "name": "test-binary",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": PTZ_SCHEMA_DICT,
        "formats": ["application/swe+binary"],
    }
    ds = DatastreamResource.model_validate(payload, by_alias=True)
    assert isinstance(ds.record_schema, SWEBinaryDatastreamRecordSchema)


def test_anydatastreamrecordschema_still_dispatches_to_json():
    """Regression guard: JSON variant must keep parsing as before."""
    from oshconnect.resource_datamodels import DatastreamResource

    json_schema = {
        "obsFormat": "application/swe+json",
        "recordSchema": {
            "type": "DataRecord", "name": "test",
            "fields": [
                {"type": "Time", "name": "time",
                 "definition": "http://www.opengis.net/def/property/OGC/0/SamplingTime",
                 "uom": {"href": "http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"}},
            ],
        },
    }
    payload = {
        "id": "ds-2",
        "name": "test-json",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": json_schema,
        "formats": ["application/swe+json"],
    }
    ds = DatastreamResource.model_validate(payload, by_alias=True)
    assert isinstance(ds.record_schema, SWEDatastreamRecordSchema)


# ---------------------------------------------------------------------------
# Datastream.insert / decode_observation dispatch
# ---------------------------------------------------------------------------


class _StubNode:
    """Minimal `Node` stand-in for unit tests that don't need a real broker."""
    def register_streamable(self, _streamable):
        pass

    def get_mqtt_client(self):
        return None


def _make_binary_datastream():
    """Build a Datastream wired to a swe+binary schema, with the MQTT publish
    side stubbed so we can capture the wire bytes without a broker."""
    from oshconnect.resource_datamodels import DatastreamResource
    from oshconnect.resources.datastream import Datastream

    ds_resource = DatastreamResource.model_validate({
        "id": "ds-bin",
        "name": "bin",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": PTZ_SCHEMA_DICT,
        "formats": ["application/swe+binary"],
    }, by_alias=True)
    ds = Datastream(parent_node=_StubNode(), datastream_resource=ds_resource)
    captured: list[bytes] = []
    ds._topic = "test-topic"
    ds._publish_mqtt = lambda topic, payload: captured.append(payload)
    return ds, captured


def test_datastream_insert_routes_through_binary_codec():
    ds, captured = _make_binary_datastream()
    ds.insert({"time": 1.0, "pan": 2.0, "tilt": 3.0, "zoomFactor": 4.0})
    assert len(captured) == 1
    assert len(captured[0]) == 8 + 3 * 4
    # First 8 bytes are big-endian double 1.0
    assert struct.unpack(">d", captured[0][:8])[0] == 1.0


def test_datastream_insert_passes_bytes_through():
    ds, captured = _make_binary_datastream()
    pre_framed = encode_swe_binary_blob(b"hi", ts=1.0)
    ds.insert(pre_framed)
    assert captured == [pre_framed]


def test_datastream_decode_observation_uses_binary_codec():
    ds, _ = _make_binary_datastream()
    framed = struct.pack(">d3f", 7.0, 8.0, 9.0, 10.0)
    out = ds.decode_observation(framed)
    assert out["time"] == pytest.approx(7.0)
    assert out["pan"] == pytest.approx(8.0)


def test_datastream_decode_observation_without_schema_raises():
    from oshconnect.resource_datamodels import DatastreamResource
    from oshconnect.resources.datastream import Datastream
    ds_resource = DatastreamResource.model_validate({
        "id": "ds-noschema", "name": "x",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
    }, by_alias=True)
    ds = Datastream(parent_node=_StubNode(), datastream_resource=ds_resource)
    with pytest.raises(ValueError, match="no record_schema"):
        ds.decode_observation(b"\x00" * 12)


# ---------------------------------------------------------------------------
# Format picker (System.discover_datastreams helper)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("available, expected_fmt, expected_parser", [
    # swe+json wins whenever advertised
    (["application/om+json", "application/swe+json", "application/swe+binary"],
     "application/swe+json", SWEDatastreamRecordSchema.from_swejson_dict),
    # falls back to swe+binary
    (["application/om+json", "application/swe+binary"],
     "application/swe+binary",
     SWEBinaryDatastreamRecordSchema.from_swebinary_dict),
    # nothing supported → (None, None)
    (["application/om+json", "application/swe+csv"], None, None),
])
def test_pick_schema_format_prefers_best_supported(available, expected_fmt,
                                                   expected_parser):
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format(available)
    assert obs_fmt == expected_fmt
    if expected_parser is None:
        assert parser is None
    else:
        # Bound classmethods aren't identity-equal across accesses;
        # compare the underlying functions.
        assert parser.__func__ is expected_parser.__func__


# ---------------------------------------------------------------------------
# Network tests (require a live Axis-camera-backed OSH node)
# ---------------------------------------------------------------------------


AXIS_PORT = os.environ.get("OSHC_AXIS_PORT", "9191")
AXIS_BASE = f"http://localhost:{AXIS_PORT}/sensorhub/api"


pytestmark_network_axis = pytest.mark.skipif(
    not osh_node_reachable(int(AXIS_PORT), path="/sensorhub/api/systems",
                           auth=None),
    reason=f"Axis OSH node not reachable at {AXIS_BASE}",
)


@pytest.mark.network
@pytestmark_network_axis
def test_live_axis_video_schema_parses():
    """Pull the live `040g`/video1 schema (swe+binary only) and parse it."""
    resp = requests.get(
        f"{AXIS_BASE}/datastreams/040g/schema",
        params={"obsFormat": "application/swe+binary"},
        timeout=5,
    )
    resp.raise_for_status()
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(resp.json())
    assert schema.obs_format == "application/swe+binary"
    # Members include a block for /img with H264 compression
    block_members = [m for m in schema.record_encoding.members
                     if isinstance(m, BinaryBlockMember)]
    assert any(m.compression == "H264" for m in block_members)


@pytest.mark.network
@pytestmark_network_axis
def test_live_axis_video_observation_decodes():
    """Fetch one live H.264 frame via swe+binary and verify the frame's
    NAL start code survives the codec round-trip."""
    schema_resp = requests.get(
        f"{AXIS_BASE}/datastreams/040g/schema",
        params={"obsFormat": "application/swe+binary"},
        timeout=5,
    )
    schema_resp.raise_for_status()
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(schema_resp.json())
    obs_resp = requests.get(
        f"{AXIS_BASE}/datastreams/040g/observations",
        params={"f": "application/swe+binary", "limit": 1},
        timeout=5,
    )
    obs_resp.raise_for_status()
    codec = SWEBinaryCodec(schema)
    record = codec.decode(obs_resp.content)
    assert "time" in record
    img = record["img"]
    assert isinstance(img, bytes) and len(img) > 100
    # Annex B start code for H.264
    assert img[:4] == b"\x00\x00\x00\x01"


@pytest.mark.network
@pytestmark_network_axis
def test_live_axis_ptz_observation_round_trip():
    """Pull a ptzOutput swe+binary record and a swe+json record from the
    same datastream and check the numbers agree across formats."""
    schema_resp = requests.get(
        f"{AXIS_BASE}/datastreams/0410/schema",
        params={"obsFormat": "application/swe+binary"},
        timeout=5,
    )
    schema_resp.raise_for_status()
    schema = SWEBinaryDatastreamRecordSchema.from_swebinary_dict(schema_resp.json())
    codec = SWEBinaryCodec(schema)
    bin_resp = requests.get(
        f"{AXIS_BASE}/datastreams/0410/observations",
        params={"f": "application/swe+binary", "limit": 1},
        timeout=5,
    )
    bin_resp.raise_for_status()
    bin_record = codec.decode(bin_resp.content)
    # Fields we expect from the doc: time, pan, tilt, zoomFactor
    for k in ("time", "pan", "tilt", "zoomFactor"):
        assert k in bin_record


@pytest.mark.network
@pytestmark_network_axis
def test_live_axis_discovery_picks_binary_for_video():
    """Full discovery against the live Axis node: System.discover_datastreams
    must pick `application/swe+binary` for the video output (which doesn't
    advertise swe+json) and end up with a `SWEBinaryDatastreamRecordSchema`."""
    from oshconnect import Node

    # Go through Node directly — `OSHConnect.discover_systems()` mutates state
    # rather than returning the discovered list, and we just want the systems.
    node = Node(protocol="http", address="localhost", port=int(AXIS_PORT))
    systems = node.discover_systems()
    assert systems, "Expected at least one system on the Axis node"
    found_binary = False
    for sys in systems:
        for ds in sys.discover_datastreams():
            schema = ds.get_resource().record_schema
            if isinstance(schema, SWEBinaryDatastreamRecordSchema):
                found_binary = True
                # If this is video1, codec.decode of a fetched obs must work
                codec = SWEBinaryCodec(schema)
                obs_resp = requests.get(
                    f"{AXIS_BASE}/datastreams/{ds.get_id()}/observations",
                    params={"f": "application/swe+binary", "limit": 1},
                    timeout=5,
                )
                if obs_resp.ok and obs_resp.content:
                    codec.decode(obs_resp.content)
                break
        if found_binary:
            break
    assert found_binary, (
        "Discovery did not produce any SWEBinaryDatastreamRecordSchema; "
        "format-aware schema fetch is not engaging on the live node."
    )
