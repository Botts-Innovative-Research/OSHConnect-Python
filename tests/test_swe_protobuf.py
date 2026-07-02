#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/6/8
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Tests for the ``application/swe+proto`` codec (descriptor-driven).

``application/swe+proto`` is the per-datastream descriptor Protobuf
encoding: a DataStream ships a serialized ``FileDescriptorSet`` describing
one ``Observation_<dsId>``-shaped message (envelope fields 1–5 + the SWE
record at 6+), and each observation is a serialized instance of it.

The codec builds the message class dynamically from that descriptor, so
these tests construct a descriptor inline (no generated BinaryEncodings
bindings needed) — they only require the ``protobuf`` runtime. The
descriptor deliberately uses ``google.protobuf.Timestamp`` for the time
fields so the dependency-seeding path is exercised, matching what a real
node descriptor needs.
"""
from __future__ import annotations

import base64
import shutil

import pytest

_HAS_PROTOC = shutil.which("protoc") is not None

protobuf = pytest.importorskip(
    "google.protobuf", reason="protobuf runtime not installed (pip install 'oshconnect[protobuf]')")

from google.protobuf import descriptor_pb2  # noqa: E402

from oshconnect import SWEProtobufCodec, SWEProtobufDatastreamRecordSchema  # noqa: E402


# ---------------------------------------------------------------------------
# Inline descriptor construction
# ---------------------------------------------------------------------------

_FDP = descriptor_pb2.FieldDescriptorProto
_OPTIONAL = _FDP.LABEL_OPTIONAL


def _add_field(msg, name, number, ftype, type_name=None):
    f = msg.field.add()
    f.name = name
    f.number = number
    f.label = _OPTIONAL
    f.type = ftype
    if type_name is not None:
        f.type_name = type_name


def _weather_descriptor_set(*, package="oshtest.weather",
                            message="WeatherObservation") -> bytes:
    """A per-datastream weather observation message, as a FileDescriptorSet.

    Envelope (1–5): id, datastream_id, foi_id, phenomenon_time,
    result_time (the two times are google.protobuf.Timestamp). Result
    (6+): air_temperature/relative_humidity (float), samples (int32),
    clear_sky (bool), note (string).
    """
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "oshtest/weather.proto"
    fdp.package = package
    fdp.syntax = "proto3"
    fdp.dependency.append("google/protobuf/timestamp.proto")

    m = fdp.message_type.add()
    m.name = message
    _add_field(m, "id", 1, _FDP.TYPE_STRING)
    _add_field(m, "datastream_id", 2, _FDP.TYPE_STRING)
    _add_field(m, "foi_id", 3, _FDP.TYPE_STRING)
    _add_field(m, "phenomenon_time", 4, _FDP.TYPE_MESSAGE, ".google.protobuf.Timestamp")
    _add_field(m, "result_time", 5, _FDP.TYPE_MESSAGE, ".google.protobuf.Timestamp")
    _add_field(m, "air_temperature", 6, _FDP.TYPE_FLOAT)
    _add_field(m, "relative_humidity", 7, _FDP.TYPE_FLOAT)
    _add_field(m, "samples", 8, _FDP.TYPE_INT32)
    _add_field(m, "clear_sky", 9, _FDP.TYPE_BOOL)
    _add_field(m, "note", 10, _FDP.TYPE_STRING)

    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fdp)
    return fds.SerializeToString()


def _weather_schema() -> SWEProtobufDatastreamRecordSchema:
    return SWEProtobufDatastreamRecordSchema(
        file_descriptor_set=_weather_descriptor_set(),
        message_type="oshtest.weather.WeatherObservation",
    )


# Result values shared by several round-trip tests. floats are exact in
# binary32 so equality holds after the float64→float32→float64 trip.
_RESULT = {
    "air_temperature": 23.5,
    "relative_humidity": 60.0,
    "samples": 42,
    "clear_sky": True,
    "note": "sunny",
}


# ---------------------------------------------------------------------------
# Schema model
# ---------------------------------------------------------------------------


def test_schema_carries_protobuf_encoding_marker():
    from oshconnect import ProtobufEncoding
    schema = _weather_schema()
    assert isinstance(schema.record_encoding, ProtobufEncoding)
    assert schema.obs_format == "application/swe+proto"
    assert schema.message_type == "oshtest.weather.WeatherObservation"


def test_schema_base64_round_trips_through_json_dict():
    """The descriptor must survive a JSON dump/parse as base64 — that's how
    it travels in the CS API schema document and the discriminated union."""
    schema = _weather_schema()
    doc = schema.to_sweproto_dict()
    assert doc["obsFormat"] == "application/swe+proto"
    # fileDescriptorSet is base64 text in the JSON form, decoding back to
    # the original bytes.
    assert base64.b64decode(doc["fileDescriptorSet"]) == schema.file_descriptor_set
    parsed = SWEProtobufDatastreamRecordSchema.from_sweproto_dict(doc)
    assert parsed.file_descriptor_set == schema.file_descriptor_set
    assert parsed.message_type == schema.message_type


def test_schema_dispatches_via_any_datastream_record_schema():
    """A DatastreamResource carrying a swe+proto schema doc must route the
    literal obsFormat to SWEProtobufDatastreamRecordSchema, base64 and all."""
    from oshconnect.resource_datamodels import DatastreamResource

    payload = {
        "id": "ds-proto",
        "name": "proto-stream",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": _weather_schema().to_sweproto_dict(),
        "formats": ["application/swe+proto"],
    }
    ds = DatastreamResource.model_validate(payload, by_alias=True)
    assert isinstance(ds.record_schema, SWEProtobufDatastreamRecordSchema)
    # And the descriptor survived intact through the union round-trip.
    SWEProtobufCodec(ds.record_schema)  # builds without error


# ---------------------------------------------------------------------------
# Codec round-trips
# ---------------------------------------------------------------------------


def test_decode_returns_only_result_fields():
    """decode() yields the result record (6+) keyed by name — matching the
    swe+binary codec — and never leaks envelope metadata into it."""
    codec = SWEProtobufCodec(_weather_schema())
    assert set(codec.result_field_names) == set(_RESULT)
    wire = codec.encode(_RESULT, envelope={
        "datastream_id": "weather42",
        "result_time": "2026-01-01T00:00:00Z",
    })
    out = codec.decode(wire)
    # 23.5 and 60.0 are exact in binary32, so equality holds post round-trip.
    assert out == _RESULT
    for env_key in ("id", "datastream_id", "foi_id", "phenomenon_time", "result_time"):
        assert env_key not in out


def test_decode_with_envelope_recovers_metadata():
    codec = SWEProtobufCodec(_weather_schema())
    wire = codec.encode(_RESULT, envelope={
        "id": "obs-1",
        "datastream_id": "weather42",
        "foi_id": "foi-9",
        "phenomenon_time": "2026-01-01T00:00:00Z",
        "result_time": "2026-01-01T00:00:01Z",
    })
    full = codec.decode_with_envelope(wire)
    assert full["result"] == _RESULT
    assert full["id"] == "obs-1"
    assert full["datastream@id"] == "weather42"
    assert full["foi@id"] == "foi-9"
    # Timestamps come back as ISO 8601 (RFC 3339) strings.
    assert full["phenomenonTime"] == "2026-01-01T00:00:00Z"
    assert full["resultTime"] == "2026-01-01T00:00:01Z"


def test_encode_ignores_envelope_keys_in_result_dict():
    """An envelope key accidentally present in the result dict is skipped,
    not mis-encoded as a result field."""
    codec = SWEProtobufCodec(_weather_schema())
    polluted = dict(_RESULT, datastream_id="should-be-ignored")
    wire = codec.encode(polluted)
    # datastream_id was ignored (it's an envelope field, unset here).
    assert codec.decode_with_envelope(wire)["datastream@id"] == ""


def test_encode_accepts_epoch_and_timeinstant_times():
    from oshconnect.timemanagement import TimeInstant
    codec = SWEProtobufCodec(_weather_schema())
    wire = codec.encode(_RESULT, envelope={
        "phenomenon_time": 1_767_225_600,  # 2026-01-01T00:00:00Z
        "result_time": TimeInstant.from_string("2026-01-01T00:00:00Z"),
    })
    full = codec.decode_with_envelope(wire)
    assert full["phenomenonTime"] == "2026-01-01T00:00:00Z"
    assert full["resultTime"].startswith("2026-01-01T00:00:00")


def test_unknown_result_field_raises_keyerror():
    codec = SWEProtobufCodec(_weather_schema())
    with pytest.raises(KeyError, match="not in message"):
        codec.encode({"nonexistent": 1.0})


def test_message_type_auto_detected_when_single():
    """message_type may be omitted when the descriptor set has one message."""
    codec = SWEProtobufCodec(
        file_descriptor_set=_weather_descriptor_set(), message_type=None)
    assert "air_temperature" in codec.result_field_names


def test_wrap_file_descriptor_proto_round_trips():
    """A bare FileDescriptorProto must be wrapped before use; the helper
    does that and the wrapped form decodes identically."""
    from oshconnect.swe_protobuf import wrap_file_descriptor_proto

    # _weather_descriptor_set is a one-file Set; pull its single FDP back out
    # to simulate a node that delivered a bare descriptor.
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(_weather_descriptor_set())
    fdp_bytes = fds.file[0].SerializeToString()

    wrapped = wrap_file_descriptor_proto(fdp_bytes)
    codec = SWEProtobufCodec(
        file_descriptor_set=wrapped,
        message_type="oshtest.weather.WeatherObservation")
    assert codec.decode(codec.encode(_RESULT)) == _RESULT


def test_bare_descriptor_proto_without_wrap_raises():
    """Passing a bare FileDescriptorProto (unwrapped) fails loudly rather
    than silently mis-parsing."""
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(_weather_descriptor_set())
    fdp_bytes = fds.file[0].SerializeToString()
    with pytest.raises(Exception):  # DecodeError or ValueError — never silent
        SWEProtobufCodec(file_descriptor_set=fdp_bytes,
                         message_type="oshtest.weather.WeatherObservation")


def test_multi_file_set_resolves_non_google_dependency():
    """The realistic shape: a FileDescriptorSet where one file imports
    another (non-google) file — like a per-datastream descriptor importing
    swe_options. Exercises the topological add (set is ordered
    dependent-first) and cross-file type resolution that single-file tests
    never hit."""
    # File A — defines a type referenced by B.
    fa = descriptor_pb2.FileDescriptorProto()
    fa.name = "oshtest/units.proto"
    fa.package = "oshtest"
    fa.syntax = "proto3"
    ua = fa.message_type.add()
    ua.name = "Unit"
    _add_field(ua, "code", 1, _FDP.TYPE_STRING)

    # File B — imports A and references oshtest.Unit in a result field.
    fb = descriptor_pb2.FileDescriptorProto()
    fb.name = "oshtest/obs2.proto"
    fb.package = "oshtest"
    fb.syntax = "proto3"
    fb.dependency.append("oshtest/units.proto")
    mb = fb.message_type.add()
    mb.name = "Obs2"
    _add_field(mb, "datastream_id", 2, _FDP.TYPE_STRING)
    _add_field(mb, "air_temperature", 6, _FDP.TYPE_FLOAT)
    _add_field(mb, "unit", 7, _FDP.TYPE_MESSAGE, ".oshtest.Unit")

    # Dependent (B) before dependency (A) — the loader must reorder.
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fb)
    fds.file.append(fa)

    # Constructs without error → topological add + non-google resolution work.
    codec = SWEProtobufCodec(
        file_descriptor_set=fds.SerializeToString(), message_type="oshtest.Obs2")
    assert "air_temperature" in codec.result_field_names
    assert "unit" in codec.result_field_names
    # The nested cross-file message decodes as a nested dict (default here,
    # since only the scalar was set on encode).
    out = codec.decode(codec.encode({"air_temperature": 1.5}))
    assert out == {"air_temperature": 1.5, "unit": {"code": ""}}


def test_missing_non_google_dependency_raises():
    """A descriptor importing a non-google file that isn't in the set must
    fail loudly — that's the FileDescriptorSet completeness contract."""
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = "oshtest/needs_dep.proto"
    fdp.package = "oshtest"
    fdp.syntax = "proto3"
    fdp.dependency.append("oshtest/missing.proto")
    m = fdp.message_type.add()
    m.name = "Thing"
    _add_field(m, "x", 1, _FDP.TYPE_FLOAT)
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fdp)
    with pytest.raises(ImportError, match="missing dependencies"):
        SWEProtobufCodec(file_descriptor_set=fds.SerializeToString(),
                         message_type="oshtest.Thing")


# ---------------------------------------------------------------------------
# Wiring through Datastream and the format picker
# ---------------------------------------------------------------------------


def test_datastream_insert_routes_through_protobuf_codec():
    """Datastream.insert(...) → _encode_for_wire picks the proto codec and
    supplies datastream_id/result_time as envelope; decode_observation
    returns just the result record."""
    from oshconnect.resource_datamodels import DatastreamResource
    from oshconnect.resources.datastream import Datastream

    class _StubNode:
        def register_streamable(self, _s): pass
        def get_mqtt_client(self): return None
        def get_comm_client(self): return None

    payload = {
        "id": "weather42",
        "name": "proto",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "schema": _weather_schema().to_sweproto_dict(),
        "formats": ["application/swe+proto"],
    }
    ds_resource = DatastreamResource.model_validate(payload, by_alias=True)
    ds = Datastream(parent_node=_StubNode(), datastream_resource=ds_resource)
    captured: list[bytes] = []
    ds._topic = "t"
    ds._publish_mqtt = lambda topic, p: captured.append(p)

    ds.insert(_RESULT)
    assert len(captured) == 1
    # decode_observation returns the result record (envelope stripped).
    assert ds.decode_observation(captured[0]) == _RESULT


# ---------------------------------------------------------------------------
# Schema generation — translating a SWE record into a swe+proto descriptor
# ---------------------------------------------------------------------------


def _swe_record():
    """A scalar SWE DataRecord covering every translatable component type."""
    from oshconnect.api_utils import UCUMCode, URI
    from oshconnect.swe_components import (
        BooleanSchema, CategorySchema, CountSchema, DataRecordSchema,
        QuantitySchema, TextSchema, TimeSchema,
    )
    return DataRecordSchema(
        name="weather", label="Weather", definition="http://example.org/weather",
        fields=[
            TimeSchema(name="time", label="Time",
                       definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                       uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian")),
            QuantitySchema(name="temp", label="Temp", definition="http://example.org/temp",
                           uom=UCUMCode(code="Cel", label="Celsius")),
            CountSchema(name="samples", label="Samples", definition="http://example.org/n",
                        uom=UCUMCode(code="1", label="count")),
            BooleanSchema(name="clear_sky", label="Clear", definition="http://example.org/clear"),
            CategorySchema(name="state", label="State", definition="http://example.org/state"),
            TextSchema(name="note", label="Note", definition="http://example.org/note"),
        ],
    )


def test_from_record_schema_generates_round_trippable_descriptor():
    """A swe+proto schema generated from a SWE DataRecord must encode and
    decode through the codec — proving the create-side descriptor matches
    what the codec reads."""
    schema = SWEProtobufDatastreamRecordSchema.from_record_schema(
        _swe_record(), message_name="Observation_ds42")
    assert schema.message_type == "oshconnect.sweproto.Observation_ds42"
    codec = SWEProtobufCodec(schema)
    assert codec.result_field_names == [
        "time", "temp", "samples", "clear_sky", "state", "note"]
    value = {
        "time": "2026-01-01T00:00:00Z",
        "temp": 23.5,
        "samples": 42,
        "clear_sky": True,
        "state": "on",
        "note": "sunny",
    }
    assert codec.decode(codec.encode(value)) == value


def test_from_other_schema_translates_swe_binary_schema():
    """Translating from a SWE+binary datastream schema reuses its semantic
    record_schema and yields a working swe+proto schema."""
    from oshconnect import BinaryEncoding, SWEBinaryDatastreamRecordSchema

    binary = SWEBinaryDatastreamRecordSchema(
        record_schema=_swe_record(),
        record_encoding=BinaryEncoding(members=[]),
    )
    proto = SWEProtobufDatastreamRecordSchema.from_other_schema(binary)
    codec = SWEProtobufCodec(proto)
    assert "temp" in codec.result_field_names
    out = codec.decode(codec.encode({
        "time": "2026-01-01T00:00:00Z", "temp": 1.0, "samples": 1,
        "clear_sky": False, "state": "off", "note": "x",
    }))
    assert out["temp"] == 1.0 and out["state"] == "off"


def test_nested_record_generates_and_round_trips():
    """Nested DataRecords are first-class: the generator emits a nested
    message and the codec recurses into a nested dict, end to end."""
    from oshconnect.api_utils import UCUMCode, URI
    from oshconnect.swe_components import (
        DataRecordSchema, QuantitySchema, TimeSchema,
    )
    inner = DataRecordSchema(
        name="location", label="Location", definition="http://example.org/loc",
        fields=[
            QuantitySchema(name="lat", label="Lat", definition="http://example.org/lat",
                           uom=UCUMCode(code="deg", label="deg")),
            QuantitySchema(name="lon", label="Lon", definition="http://example.org/lon",
                           uom=UCUMCode(code="deg", label="deg")),
        ],
    )
    outer = DataRecordSchema(
        name="obs", label="Obs", definition="http://example.org/obs",
        fields=[
            TimeSchema(name="time", label="Time",
                       definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                       uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian")),
            QuantitySchema(name="temp", label="Temp", definition="http://example.org/temp",
                           uom=UCUMCode(code="Cel", label="C")),
            inner,
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(outer))
    assert codec.result_field_names == ["time", "temp", "location"]
    value = {
        "time": "2026-01-01T00:00:00Z",
        "temp": 21.0,
        "location": {"lat": 38.9, "lon": -77.0},
    }
    assert codec.decode(codec.encode(value)) == value


def test_deeply_nested_records_round_trip():
    """Recursion is arbitrary-depth — a record inside a record inside a record."""
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import DataRecordSchema, QuantitySchema

    def q(name):
        return QuantitySchema(name=name, label=name,
                              definition=f"http://example.org/{name}",
                              uom=UCUMCode(code="m", label="m"))

    level3 = DataRecordSchema(name="c", definition="http://example.org/c", fields=[q("z")])
    level2 = DataRecordSchema(name="b", definition="http://example.org/b", fields=[q("y"), level3])
    level1 = DataRecordSchema(name="a", definition="http://example.org/a", fields=[q("x"), level2])
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(level1))
    value = {"x": 1.0, "b": {"y": 2.0, "c": {"z": 3.0}}}
    assert codec.decode(codec.encode(value)) == value


def test_vector_generates_and_round_trips():
    """Vectors become a nested message (Vec<N>); encode accepts a sequence,
    decode returns a dict keyed by coordinate name."""
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        DataRecordSchema, QuantitySchema, VectorSchema,
    )
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            VectorSchema(
                name="pos", label="Position", definition="http://example.org/pos",
                reference_frame="http://example.org/frame",
                coordinates=[
                    QuantitySchema(name="x", label="X", definition="http://example.org/x",
                                   uom=UCUMCode(code="m", label="m")),
                    QuantitySchema(name="y", label="Y", definition="http://example.org/y",
                                   uom=UCUMCode(code="m", label="m")),
                    QuantitySchema(name="z", label="Z", definition="http://example.org/z",
                                   uom=UCUMCode(code="m", label="m")),
                ],
            ),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    # Encode the vector as a sequence...
    wire = codec.encode({"pos": [1.0, 2.0, 3.0]})
    # ...decode returns it keyed by coordinate name.
    assert codec.decode(wire) == {"pos": {"x": 1.0, "y": 2.0, "z": 3.0}}
    # A mapping value encodes identically.
    assert codec.encode({"pos": {"x": 1.0, "y": 2.0, "z": 3.0}}) == wire


_OGC_DT = "http://www.opengis.net/def/dataType/OGC/0/"


def _field_type(codec, name):
    """The proto wire type of a top-level field on the codec's message."""
    return codec._descriptor.fields_by_name[name].type


def test_datatype_drives_leaf_proto_type():
    """A component's OGC dataType selects the proto wire type (float32 →
    float, signedLong → int64), matching the node's getDataType — not a
    hardcoded double/int32."""
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        CountSchema, DataRecordSchema, QuantitySchema,
    )
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            QuantitySchema(name="temp", label="T", definition="http://example.org/t",
                           uom=UCUMCode(code="Cel", label="C")),
            CountSchema(name="ticks", label="N", definition="http://example.org/n",
                        uom=UCUMCode(code="1", label="c")),
        ],
    )
    schema = SWEProtobufDatastreamRecordSchema.from_record_schema(
        rec, datatype_by_path={
            "/temp": _OGC_DT + "float32",
            "/ticks": _OGC_DT + "signedLong",
        })
    codec = SWEProtobufCodec(schema)
    assert _field_type(codec, "temp") == FieldDescriptor.TYPE_FLOAT
    assert _field_type(codec, "ticks") == FieldDescriptor.TYPE_INT64
    # float32-exact value round-trips.
    assert codec.decode(codec.encode({"temp": 1.5, "ticks": 9_000_000_000})) == {
        "temp": 1.5, "ticks": 9_000_000_000}


def test_default_leaf_types_match_node_defaults():
    """With no dataType, Quantity → double and Count → int32 — the node's
    own defaults."""
    from google.protobuf.descriptor import FieldDescriptor
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(_swe_record()))
    assert _field_type(codec, "temp") == FieldDescriptor.TYPE_DOUBLE
    assert _field_type(codec, "samples") == FieldDescriptor.TYPE_INT32


def test_numeric_time_maps_to_double_iso_time_to_timestamp():
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect.api_utils import UCUMCode, URI
    from oshconnect.swe_components import DataRecordSchema, TimeSchema
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            TimeSchema(name="iso_t", label="iso",
                       definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                       uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian")),
            TimeSchema(name="epoch_t", label="epoch",
                       definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                       uom=UCUMCode(code="s", label="seconds")),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    # ISO time → Timestamp (message), numeric time → double.
    assert _field_type(codec, "iso_t") == FieldDescriptor.TYPE_MESSAGE
    assert _field_type(codec, "epoch_t") == FieldDescriptor.TYPE_DOUBLE
    out = codec.decode(codec.encode({"iso_t": "2026-01-01T00:00:00Z", "epoch_t": 1767225600.0}))
    assert out["iso_t"] == "2026-01-01T00:00:00Z"
    assert out["epoch_t"] == 1767225600.0


def test_from_other_schema_mines_binary_member_datatypes():
    """Translating from SWE+Binary uses the encoding members' dataTypes —
    including for nested paths like /pos/x — so float32 stays float32."""
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect import (
        BinaryComponentMember, BinaryEncoding, SWEBinaryDatastreamRecordSchema,
    )
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        DataRecordSchema, QuantitySchema, VectorSchema,
    )
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            QuantitySchema(name="temp", label="T", definition="http://example.org/t",
                           uom=UCUMCode(code="Cel", label="C")),
            VectorSchema(
                name="pos", label="P", definition="http://example.org/p",
                reference_frame="http://example.org/f",
                coordinates=[
                    QuantitySchema(name="x", label="X", definition="http://example.org/x",
                                   uom=UCUMCode(code="m", label="m")),
                    QuantitySchema(name="y", label="Y", definition="http://example.org/y",
                                   uom=UCUMCode(code="m", label="m")),
                ]),
        ],
    )
    binary = SWEBinaryDatastreamRecordSchema(
        record_schema=rec,
        record_encoding=BinaryEncoding(members=[
            BinaryComponentMember(ref="/temp", dataType=_OGC_DT + "float32"),
            BinaryComponentMember(ref="/pos/x", dataType=_OGC_DT + "float32"),
            BinaryComponentMember(ref="/pos/y", dataType=_OGC_DT + "float32"),
        ]),
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_other_schema(binary))
    assert _field_type(codec, "temp") == FieldDescriptor.TYPE_FLOAT
    # nested vector coordinate types resolved via /pos/x, /pos/y
    pos_desc = codec._descriptor.fields_by_name["pos"].message_type
    assert pos_desc.fields_by_name["x"].type == FieldDescriptor.TYPE_FLOAT
    assert codec.decode(codec.encode({"temp": 1.5, "pos": [2.5, 3.5]})) == {
        "temp": 1.5, "pos": {"x": 2.5, "y": 3.5}}


def test_constrained_category_generates_enum_and_round_trips():
    """A Category with an AllowedTokens constraint becomes a proto enum;
    encode accepts the token string, decode returns it."""
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect.swe_components import CategorySchema, DataRecordSchema

    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            CategorySchema(name="sky", label="Sky", definition="http://example.org/sky",
                           constraint={"values": ["CLEAR", "CLOUDY", "RAIN"]}),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    assert _field_type(codec, "sky") == FieldDescriptor.TYPE_ENUM
    # round-trips on the token string, not the integer ordinal
    assert codec.decode(codec.encode({"sky": "CLOUDY"})) == {"sky": "CLOUDY"}
    assert codec.decode(codec.encode({"sky": "RAIN"})) == {"sky": "RAIN"}


def test_unconstrained_category_stays_string():
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect.swe_components import CategorySchema, DataRecordSchema
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            CategorySchema(name="label", label="L", definition="http://example.org/l"),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    assert _field_type(codec, "label") == FieldDescriptor.TYPE_STRING
    assert codec.decode(codec.encode({"label": "anything"})) == {"label": "anything"}


def test_enum_rejects_unknown_token():
    from oshconnect.swe_components import CategorySchema, DataRecordSchema
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            CategorySchema(name="sky", definition="http://example.org/sky",
                           constraint={"values": ["CLEAR", "CLOUDY"]}),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    with pytest.raises(KeyError, match="not an allowed token"):
        codec.encode({"sky": "SNOW"})


def test_enum_inside_nested_record_round_trips():
    """An enum nested inside a record is scoped to that message and still
    round-trips — exercises the per-message enum nesting."""
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        CategorySchema, DataRecordSchema, QuantitySchema,
    )
    inner = DataRecordSchema(
        name="status", definition="http://example.org/status",
        fields=[
            QuantitySchema(name="battery", label="B", definition="http://example.org/b",
                           uom=UCUMCode(code="%", label="pct")),
            CategorySchema(name="mode", definition="http://example.org/mode",
                           constraint={"values": ["IDLE", "ACTIVE"]}),
        ],
    )
    outer = DataRecordSchema(
        name="obs", definition="http://example.org/obs",
        fields=[
            CategorySchema(name="sky", definition="http://example.org/sky",
                           constraint={"values": ["CLEAR", "RAIN"]}),
            inner,
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(outer))
    value = {"sky": "RAIN", "status": {"battery": 88, "mode": "ACTIVE"}}
    assert codec.decode(codec.encode(value)) == value


def test_enum_rejects_non_identifier_token():
    """Tokens must be valid proto enum identifiers (as the node requires)."""
    from oshconnect.swe_components import CategorySchema, DataRecordSchema
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            CategorySchema(name="sky", definition="http://example.org/sky",
                           constraint={"values": ["clear sky", "rain"]}),
        ],
    )
    with pytest.raises(ValueError, match="valid proto enum identifier"):
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec)


def test_data_array_of_scalars_generates_wrapper_and_round_trips():
    """A DataArray<scalar> becomes the node's `Array<N> { repeated <elt> = 1 }`
    wrapper, round-tripping as {array: {element: [...]}}."""
    from google.protobuf.descriptor import FieldDescriptor
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        DataArraySchema, DataRecordSchema, QuantitySchema,
    )
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            DataArraySchema(
                name="samples", label="Samples", definition="http://example.org/s",
                element_count={"value": 3},
                element_type=QuantitySchema(
                    name="reading", label="R", definition="http://example.org/x",
                    uom=UCUMCode(code="m", label="m")),
            ),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    # `samples` is a singular message field (the Array wrapper); the repeated
    # element lives inside it.
    samples_field = codec._descriptor.fields_by_name["samples"]
    assert samples_field.type == FieldDescriptor.TYPE_MESSAGE
    reading_field = samples_field.message_type.fields_by_name["reading"]
    assert reading_field.is_repeated
    value = {"samples": {"reading": [1.0, 2.0, 3.0]}}
    assert codec.decode(codec.encode(value)) == value


def test_data_array_of_records_round_trips():
    """Array of records exercises repeated + nested-message together."""
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        DataArraySchema, DataRecordSchema, QuantitySchema,
    )
    point = DataRecordSchema(
        name="point", definition="http://example.org/pt",
        fields=[
            QuantitySchema(name="lat", label="la", definition="http://example.org/lat",
                           uom=UCUMCode(code="deg", label="d")),
            QuantitySchema(name="lon", label="lo", definition="http://example.org/lon",
                           uom=UCUMCode(code="deg", label="d")),
        ],
    )
    rec = DataRecordSchema(
        name="r", definition="http://example.org/r",
        fields=[
            DataArraySchema(
                name="track", label="Track", definition="http://example.org/track",
                element_count={"value": 2}, element_type=point),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    value = {"track": {"point": [
        {"lat": 1.0, "lon": 2.0}, {"lat": 3.0, "lon": 4.0}]}}
    assert codec.decode(codec.encode(value)) == value


# ---------------------------------------------------------------------------
# .proto source translation (render + recompile)
# ---------------------------------------------------------------------------


def _sky_temp_schema(message_name="Obs_ds1"):
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        CategorySchema, DataRecordSchema, QuantitySchema,
    )
    rec = DataRecordSchema(
        name="obs", definition="http://example.org/o",
        fields=[
            QuantitySchema(name="temp", label="T", definition="http://example.org/t",
                           uom=UCUMCode(code="Cel", label="C")),
            CategorySchema(name="sky", definition="http://example.org/sky",
                           constraint={"values": ["CLEAR", "RAIN"]}),
        ],
    )
    return SWEProtobufDatastreamRecordSchema.from_record_schema(
        rec, message_name=message_name)


def test_to_proto_source_renders_editable_text():
    """to_proto_source renders the carried descriptor as .proto text — no
    protoc needed, and faithful to the descriptor (envelope, scalars, enum)."""
    text = _sky_temp_schema().to_proto_source()
    assert 'syntax = "proto3";' in text
    assert "package oshconnect.sweproto;" in text
    assert 'import "google/protobuf/timestamp.proto";' in text
    assert "message Obs_ds1 {" in text
    # envelope occupies 1–5, result fields start at 6
    assert "double temp = 6;" in text
    assert "enum Enum_sky {" in text
    assert "CLEAR = 0;" in text
    assert "sky = 7;" in text


@pytest.mark.skipif(not _HAS_PROTOC, reason="protoc not installed")
def test_proto_source_round_trips_via_protoc():
    """to_proto_source → from_proto_source (protoc) → codec still round-trips."""
    text = _sky_temp_schema().to_proto_source()
    schema = SWEProtobufDatastreamRecordSchema.from_proto_source(text)
    assert schema.message_type == "oshconnect.sweproto.Obs_ds1"
    codec = SWEProtobufCodec(schema)
    assert codec.decode(codec.encode({"temp": 1.5, "sky": "RAIN"})) == {
        "temp": 1.5, "sky": "RAIN"}


@pytest.mark.skipif(not _HAS_PROTOC, reason="protoc not installed")
def test_modified_proto_source_takes_effect():
    """The point of the .proto translation: hand-edit the text and the change
    flows through. Add a field, recompile, confirm it's live."""
    text = _sky_temp_schema().to_proto_source()
    edited = text.replace(
        "  double temp = 6;",
        "  double temp = 6;\n  float humidity = 8;")
    schema = SWEProtobufDatastreamRecordSchema.from_proto_source(edited)
    codec = SWEProtobufCodec(schema)
    assert "humidity" in codec.result_field_names
    assert codec.decode(codec.encode({"temp": 1.0, "humidity": 2.5, "sky": "CLEAR"})) == {
        "temp": 1.0, "humidity": 2.5, "sky": "CLEAR"}


def test_from_record_schema_rejects_unsupported_composite():
    """Composites without a wire mapping yet (DataChoice, etc.) still fail
    loudly rather than emit a wrong descriptor."""
    from oshconnect.api_utils import UCUMCode
    from oshconnect.swe_components import (
        DataChoiceSchema, DataRecordSchema, QuantitySchema,
    )
    rec = DataRecordSchema(
        name="r", fields=[
            DataChoiceSchema(
                name="choice", label="Choice", definition="http://example.org/c",
                items=[
                    QuantitySchema(name="a", label="A", definition="http://example.org/a",
                                   uom=UCUMCode(code="m", label="m")),
                ],
            ),
        ],
    )
    with pytest.raises(NotImplementedError):
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec)


def test_generated_field_name_is_sanitized():
    from oshconnect.swe_components import DataRecordSchema, QuantitySchema
    from oshconnect.api_utils import UCUMCode
    # SWE NameTokens allow hyphens (^[A-Za-z][A-Za-z0-9_\\-]*$) — invalid in
    # proto field identifiers, so they sanitize to underscores.
    rec = DataRecordSchema(
        name="r", fields=[
            QuantitySchema(name="air-temp-2m", label="T",
                           definition="http://example.org/t",
                           uom=UCUMCode(code="Cel", label="C")),
        ],
    )
    codec = SWEProtobufCodec(
        SWEProtobufDatastreamRecordSchema.from_record_schema(rec))
    assert codec.result_field_names == ["air_temp_2m"]


def test_pick_schema_format_picks_protobuf_when_present():
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/om+json", "application/swe+proto",
    ])
    assert obs_fmt == "application/swe+proto"
    assert parser.__func__ is SWEProtobufDatastreamRecordSchema.from_sweproto_dict.__func__


def test_pick_schema_format_prefers_swe_json_over_proto():
    from oshconnect import SWEDatastreamRecordSchema
    from oshconnect.resources.system import System
    obs_fmt, parser = System._pick_datastream_schema_format([
        "application/swe+json", "application/swe+proto",
    ])
    assert obs_fmt == "application/swe+json"
    assert parser.__func__ is SWEDatastreamRecordSchema.from_swejson_dict.__func__
