#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""SWE Common encoding models.

`Encoding` is the base; concrete subclasses (`JSONEncoding`, `BinaryEncoding`)
describe **how** a `recordSchema` is serialized on the wire. They do not
describe the **shape** of the record itself — that's in `swe_components`.

`BinaryEncoding.members` carry one entry per scalar/block in the record,
referencing a component by JSON-pointer-style path (e.g. ``/time``,
``/img``). Each member is either a `BinaryComponentMember` (a fixed-width
scalar with an OGC data-type URI) or a `BinaryBlockMember` (a
size-prefixed opaque payload, optionally identifying a compression
codec like ``H264`` or ``JPEG``). See ``src/oshconnect/swe_binary.py``
for the runtime codec that consumes these models.
"""

from __future__ import annotations

from typing import Annotated, List, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class Encoding(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(None)
    type: str = Field(...)
    vector_as_arrays: bool = Field(False, alias='vectorAsArrays')


class JSONEncoding(Encoding):
    # Kept loosely typed as `str` (matching `Encoding.type`) for backwards
    # compatibility — older callers may instantiate with non-canonical
    # values (e.g. `"json"`). Tighten with a Literal pin only when this
    # class is added to a discriminated union.
    type: str = "JSONEncoding"


# -----------------------------------------------------------------------------
# SWE BinaryEncoding (CS API Part 2 §16.2.3 / SWE Common 3 §6.4 BinaryEncoding)
# -----------------------------------------------------------------------------
#
# Wire model for `application/swe+binary`. The Python-side codec lives in
# `oshconnect.swe_binary`; this module only provides the parse/dump models.


class BinaryComponentMember(BaseModel):
    """A fixed-width scalar member of a `BinaryEncoding`.

    Maps an OGC data-type URI to a struct format character at codec time:
    ``http://www.opengis.net/def/dataType/OGC/0/double`` → ``d``,
    ``float32`` → ``f``, ``uint32`` → ``I``, etc. See
    ``oshconnect.swe_binary.DATATYPE_STRUCT_FMT`` for the full table.

    The ``ref`` is a JSON-pointer-style path into the record schema
    (e.g. ``/time`` or ``/pan``) and identifies which scalar field this
    member encodes. Members appear in `BinaryEncoding.members` in
    serialization order — the codec walks them in that order to encode or
    decode a record.
    """
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["Component"] = "Component"
    ref: str = Field(..., description="Path to the referenced scalar (e.g. '/time').")
    data_type: str = Field(..., alias='dataType',
                           description="OGC data-type URI (e.g. .../dataType/OGC/0/float32).")


class BinaryBlockMember(BaseModel):
    """A size-prefixed opaque block member of a `BinaryEncoding`.

    On the wire the codec writes a 4-byte big-endian ``uint32`` length
    followed by ``length`` raw payload bytes. The payload is **opaque**:
    the codec does not interpret it. If ``compression`` is set (e.g.
    ``H264``, ``JPEG``) it is metadata for downstream consumers — the
    SDK does not demux or decode the codec's frames. Callers receive
    the raw bytes and are responsible for any further decoding.

    See ``docs/AXIS_CAMERA_FORMATS.md`` (in the OGC code-sprint demo
    repo) for an end-to-end example of an H.264 video datastream.
    """
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["Block"] = "Block"
    ref: str = Field(..., description="Path to the referenced block field (e.g. '/img').")
    compression: str = Field(None,
                             description="Optional codec hint, e.g. 'H264', 'JPEG'. Opaque to the SDK.")
    byte_length: int = Field(None, alias='byteLength',
                             description="Optional fixed byte length (rare; spec allows it).")
    padding_bytes_after: int = Field(None, alias='paddingBytes-after')
    padding_bytes_before: int = Field(None, alias='paddingBytes-before')


# Discriminated union — pydantic dispatches on the literal `type` field
# (``"Component"`` vs ``"Block"``). Add other member types here (currently
# none are commonly seen on OSH wire payloads).
AnyBinaryMember = Annotated[
    Union[BinaryComponentMember, BinaryBlockMember],
    Field(discriminator='type'),
]


class ProtobufEncoding(Encoding):
    """SWE-side Encoding marker for ``application/swe+proto``.

    Carries no member list — the wire layout is fully described by the
    per-datastream Protobuf descriptor that the
    `SWEProtobufDatastreamRecordSchema` carries (a serialized
    ``google.protobuf.FileDescriptorSet``). The Python-side codec lives in
    ``oshconnect.swe_protobuf``.

    Why no `members`: unlike SWE BinaryEncoding (which has to declare a wire
    layout for opaque-bytes payloads), the Protobuf encoding's wire shape is
    a tag-length-value stream fully defined by the delivered descriptor.
    There is nothing to declare at the SDK level beyond "use the protobuf
    codec."
    """
    type: Literal["ProtobufEncoding"] = "ProtobufEncoding"


class FlatBuffersEncoding(Encoding):
    """SWE-side Encoding marker for ``application/swe+flatbuffers``.

    Mirrors `ProtobufEncoding`. The wire layout is described by the
    SWE Common 3 FlatBuffers schema (a generated ``sweCommon3_generated``
    module produced from the BinaryEncodings project).

    .. warning::

        FlatBuffers Python codegen does not currently support
        vectors-of-unions, which the SWE Common 3 BinaryEncoding
        schema uses for ``[BinaryMember] (union { BinaryComponent,
        BinaryBlock })``. Until ``flatc --python`` adds this support, the
        FlatBuffers codec raises `NotImplementedError`; the encoding
        declaration is preserved so the rest of the SDK can already
        round-trip schemas that name it. See
        ``docs/osh_spec_deviations.md`` (flatc-python-vector-of-union).
    """
    type: Literal["FlatBuffersEncoding"] = "FlatBuffersEncoding"


class BinaryEncoding(Encoding):
    """SWE BinaryEncoding — describes the wire layout of a binary record.

    The `members` list mirrors the scalar/block fields of the parent
    `recordSchema` in serialization order. ``byte_order`` defaults to
    ``bigEndian`` (the form OSH emits and the only form the bundled
    codec writes); ``byte_encoding`` defaults to ``raw`` (no base64).

    The bundled codec in ``oshconnect.swe_binary`` honours ``byte_order``
    when packing fixed-width scalars; ``byte_encoding`` other than
    ``raw`` is parsed but not currently emitted by the encoder (decoder
    raises if it sees ``base64`` — open a ticket if you need it).
    """
    type: Literal["BinaryEncoding"] = "BinaryEncoding"
    byte_order: Literal["bigEndian", "littleEndian"] = Field(
        "bigEndian", alias='byteOrder')
    byte_encoding: Literal["raw", "base64"] = Field(
        "raw", alias='byteEncoding')
    members: List[AnyBinaryMember] = Field(default_factory=list)
