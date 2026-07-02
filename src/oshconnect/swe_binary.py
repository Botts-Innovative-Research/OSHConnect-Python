#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Runtime codec for the SWE Common BinaryEncoding wire format.

Two complementary entry points:

* **Low-level helpers** (`encode_swe_binary_blob`, `encode_swe_binary_record`,
  `decode_swe_binary_blob`, `decode_swe_binary_record`) — small, dependency-free
  functions for the two shapes that dominate in practice:

  1. **Variable-size block**: ``[ts: 8B BE double][size: 4B BE uint32][N bytes]``
     — the form Axis-style camera datastreams use to ship one H.264 NAL unit
     per observation. Payload is opaque to the SDK.
  2. **Fixed-width record**: ``[ts: 8B BE double][f32, f32, ...]`` — the form
     PTZ-style scalar datastreams use. All fields are fixed-width; the parser
     walks the schema in declared order.

* **Schema-driven codec** (`SWEBinaryCodec`) — given a parsed
  `SWEBinaryDatastreamRecordSchema`, walks the `record_encoding.members` list
  in order, building a struct format string and (for block members) handling
  the size-prefixed framing. Supports mixed records: any combination of
  `BinaryComponentMember` (fixed-width scalar) and `BinaryBlockMember`
  (size-prefixed opaque bytes), in any declared order.

Block payloads are **opaque**: the codec strips/writes the 4-byte size prefix
but does NOT demux or transcode the payload bytes. A H.264 NAL unit goes in,
a H.264 NAL unit comes out. Per the SWE Common spec the `compression` field on
`BinaryBlockMember` is metadata for downstream consumers, not a directive
this codec acts on.

References:
- CS API Part 2 §16.2.3 (BinaryEncoding)
- SWE Common 3 §6.4 (BinaryEncoding)
- docs/AXIS_CAMERA_FORMATS.md in the OGC code-sprint demo repo
"""

from __future__ import annotations

import struct
import time
from typing import Any, Dict, Mapping, Sequence, Tuple, Union

from .encoding import BinaryBlockMember, BinaryComponentMember, BinaryEncoding
from .schema_datamodels import SWEBinaryDatastreamRecordSchema

# OGC data-type URI → `struct` format character.
# Big-/little-endian is set on the format string prefix (see `_endian_prefix`),
# not here — these are the size+sign characters only.
#
# Sources: CS API Part 2 §16.2.3 cross-referenced with SWE Common 3 §6.4.
# Add additional URIs here as they appear on real wire payloads; raising on
# unknown is preferable to silently guessing.
DATATYPE_STRUCT_FMT: Dict[str, str] = {
    "http://www.opengis.net/def/dataType/OGC/0/double": "d",
    "http://www.opengis.net/def/dataType/OGC/0/float64": "d",
    "http://www.opengis.net/def/dataType/OGC/0/float32": "f",
    "http://www.opengis.net/def/dataType/OGC/0/signedByte": "b",
    "http://www.opengis.net/def/dataType/OGC/0/signedShort": "h",
    "http://www.opengis.net/def/dataType/OGC/0/signedInt": "i",
    "http://www.opengis.net/def/dataType/OGC/0/signedLong": "q",
    "http://www.opengis.net/def/dataType/OGC/0/unsignedByte": "B",
    "http://www.opengis.net/def/dataType/OGC/0/unsignedShort": "H",
    "http://www.opengis.net/def/dataType/OGC/0/unsignedInt": "I",
    "http://www.opengis.net/def/dataType/OGC/0/unsignedLong": "Q",
    "http://www.opengis.net/def/dataType/OGC/0/boolean": "?",
}


# Default OGC dataType URI per SWE Common scalar component class. Mirrors
# OSH's `SWEHelper.getDefaultBinaryEncoding()` (lib-ogc/swe-common-core,
# line ~530): when a BinaryEncoding isn't explicitly declared, OSH walks
# scalars and assigns the canonical wire type per component kind. The
# resulting BinaryEncoding.members list is what `BinaryDataWriter` then
# uses to pack/unpack bytes.
#
# Time defaults to `double` (epoch seconds in scientific contexts) — ISO 8601
# strings can't go in a fixed-width slot. Callers who want sub-second
# precision past the float64 limit should declare an explicit dataType.
DEFAULT_DATATYPE_URI_FOR_SCALAR: Dict[str, str] = {
    "QuantitySchema": "http://www.opengis.net/def/dataType/OGC/0/double",
    "CountSchema":    "http://www.opengis.net/def/dataType/OGC/0/signedInt",
    "BooleanSchema":  "http://www.opengis.net/def/dataType/OGC/0/boolean",
    "TimeSchema":     "http://www.opengis.net/def/dataType/OGC/0/double",
}


def _endian_prefix(byte_order: str) -> str:
    """Map SWE `byteOrder` to a `struct` prefix.

    `struct` defaults to native byte order with native alignment when no
    prefix is given; we always emit ``>`` or ``<`` to lock both the byte
    order and standard sizes (no padding).
    """
    if byte_order == "bigEndian":
        return ">"
    if byte_order == "littleEndian":
        return "<"
    raise ValueError(f"Unsupported byteOrder: {byte_order!r}")


# -----------------------------------------------------------------------------
# Low-level helpers (no schema required)
# -----------------------------------------------------------------------------


def encode_swe_binary_blob(payload: bytes,
                           ts: float | None = None) -> bytes:
    """Encode one variable-size-block SWE binary record.

    Wire form: ``[8-byte BE double ts][4-byte BE uint32 size][N bytes payload]``.

    Use for video/image/opaque-codec datastreams whose schema declares a
    single `Block` member (compression = H264, JPEG, etc.). The payload is
    written verbatim; no codec interpretation.

    :param payload: Raw bytes to ship (e.g. one H.264 NAL unit).
    :param ts: Unix epoch seconds for the observation timestamp; defaults
        to ``time.time()`` at call time.
    :returns: ``12 + len(payload)`` bytes ready to publish.
    """
    t = ts if ts is not None else time.time()
    return struct.pack(">dI", t, len(payload)) + payload


def decode_swe_binary_blob(buf: bytes) -> Tuple[float, bytes]:
    """Decode one variable-size-block SWE binary record.

    Inverse of `encode_swe_binary_blob`. The trailing payload bytes are
    returned opaquely — the caller is responsible for any codec-specific
    decoding (H.264 NAL framing, JPEG marker parsing, etc.).

    :param buf: Bytes for exactly one record. Must be at least 12 bytes
        (header) long, and at least ``12 + size`` bytes total.
    :returns: ``(ts, payload)``.
    :raises ValueError: if `buf` is shorter than the declared record.
    """
    if len(buf) < 12:
        raise ValueError(
            f"SWE binary blob too short: got {len(buf)} bytes, need at least 12.")
    ts, size = struct.unpack(">dI", buf[:12])
    if len(buf) < 12 + size:
        raise ValueError(
            f"SWE binary blob truncated: declared size {size}, "
            f"have {len(buf) - 12} payload bytes.")
    return ts, bytes(buf[12:12 + size])


def encode_swe_binary_record(ts: float, *values: float,
                             fmt: str = "f") -> bytes:
    """Encode one fixed-width SWE binary record (`[ts][f32, f32, ...]`).

    The ts column is always a big-endian 8-byte double. The remaining
    columns share a single `struct` format character via `fmt` (default
    ``"f"`` = float32). For mixed-column records use `SWEBinaryCodec`.

    :param ts: Unix epoch seconds.
    :param values: Fixed-width scalar values in serialization order.
    :param fmt: Single `struct` format char (e.g. ``"f"``, ``"d"``,
        ``"i"``). Applied to every value.
    :returns: ``8 + len(values) * struct.calcsize(fmt)`` bytes.
    """
    return struct.pack(f">d{len(values)}{fmt}", ts, *values)


def decode_swe_binary_record(buf: bytes,
                             n_values: int,
                             fmt: str = "f") -> Tuple[float, ...]:
    """Decode one fixed-width SWE binary record.

    Inverse of `encode_swe_binary_record`.

    :param buf: Bytes for exactly one record.
    :param n_values: Number of trailing scalar columns.
    :param fmt: Single `struct` format char shared by all trailing scalars.
    :returns: ``(ts, *values)``.
    """
    full = f">d{n_values}{fmt}"
    expected = struct.calcsize(full)
    if len(buf) < expected:
        raise ValueError(
            f"SWE binary record too short: got {len(buf)} bytes, "
            f"need {expected} for fmt {full!r}.")
    return struct.unpack(full, buf[:expected])


# -----------------------------------------------------------------------------
# Schema-driven codec
# -----------------------------------------------------------------------------


def _member_key(ref: str) -> str:
    """Extract the field name a `ref` resolves to.

    For SWE Common binary encodings the wire emits refs like ``/time`` or
    ``/img``; we treat the last path segment as the dict key for encode/
    decode round-trips. Nested refs (e.g. ``/loc/lat``) are uncommon in
    practice and fall back to the last segment too — open a ticket if
    a real schema needs hierarchy preserved.
    """
    if not ref:
        raise ValueError("BinaryEncoding member has empty ref.")
    return ref.rstrip("/").split("/")[-1]


class SWEBinaryCodec:
    """Schema-driven encoder/decoder for `application/swe+binary` records.

    Constructed from a parsed `SWEBinaryDatastreamRecordSchema` (or its
    inner `BinaryEncoding`). At construction time the codec compiles each
    `Component` member into a `struct` format character; at encode/decode
    time it walks `members` in order, packing fixed-width columns and
    framing blocks with the 4-byte size prefix.

    Two methods:

    * :meth:`encode(values)` — values may be a `dict` keyed by field name
      (the ``ref`` last segment) or a `Sequence` in declared member order.
      Block members expect `bytes` (or `bytearray`/`memoryview`) values.
    * :meth:`decode(buf)` — returns a dict keyed by field name. Block
      values come back as `bytes`.

    The codec does not interpret block payloads; H.264 / JPEG / Protobuf /
    etc. pass through verbatim.
    """

    def __init__(
        self,
        schema: Union[SWEBinaryDatastreamRecordSchema, BinaryEncoding],
    ):
        if isinstance(schema, SWEBinaryDatastreamRecordSchema):
            encoding = schema.record_encoding
        elif isinstance(schema, BinaryEncoding):
            encoding = schema
        else:
            raise TypeError(
                "SWEBinaryCodec expects an SWEBinaryDatastreamRecordSchema "
                f"or BinaryEncoding, got {type(schema).__name__}.")

        if encoding.byte_encoding != "raw":
            # base64 is in-spec but rarely seen on OSH wire payloads.
            # Refuse loudly instead of silently mis-encoding.
            raise NotImplementedError(
                f"byteEncoding={encoding.byte_encoding!r} not supported; "
                "only 'raw' is implemented. Open a ticket if you need base64."
            )
        self._endian = _endian_prefix(encoding.byte_order)
        self._members = list(encoding.members)
        # Per-member compiled state: list of (kind, key, struct_fmt_or_None)
        # kind ∈ {"component", "block"}.
        self._compiled: list[tuple[str, str, str | None]] = []
        for i, m in enumerate(self._members):
            key = _member_key(m.ref)
            if isinstance(m, BinaryComponentMember):
                fmt_char = DATATYPE_STRUCT_FMT.get(m.data_type)
                if fmt_char is None:
                    raise ValueError(
                        f"BinaryEncoding.members[{i}]: unsupported dataType "
                        f"{m.data_type!r}. Add it to "
                        "oshconnect.swe_binary.DATATYPE_STRUCT_FMT.")
                self._compiled.append(("component", key, fmt_char))
            elif isinstance(m, BinaryBlockMember):
                self._compiled.append(("block", key, None))
            else:
                raise TypeError(
                    f"BinaryEncoding.members[{i}]: unsupported member type "
                    f"{type(m).__name__}.")

    @property
    def field_names(self) -> list[str]:
        """Field names in declared member order. Useful for `Sequence`
        callers that want to build a positional tuple."""
        return [key for _, key, _ in self._compiled]

    def encode(self, values: Union[Mapping[str, Any], Sequence[Any]]) -> bytes:
        """Encode one record. Returns the wire bytes.

        :param values: A mapping keyed by member name OR a positional
            sequence in declared member order. Component values must be
            numeric (or bool for the ``boolean`` data type); block values
            must be `bytes`-like.
        """
        if isinstance(values, Mapping):
            ordered = [values[key] for _, key, _ in self._compiled]
        else:
            ordered = list(values)
            if len(ordered) != len(self._compiled):
                raise ValueError(
                    f"SWEBinaryCodec.encode: expected {len(self._compiled)} "
                    f"values, got {len(ordered)}.")
        out = bytearray()
        for (kind, _, fmt_char), val in zip(self._compiled, ordered):
            if kind == "component":
                out += struct.pack(f"{self._endian}{fmt_char}", val)
            else:  # block
                if not isinstance(val, (bytes, bytearray, memoryview)):
                    raise TypeError(
                        f"Block member expects bytes-like payload, got "
                        f"{type(val).__name__}.")
                payload = bytes(val)
                # 4-byte BE uint32 size prefix is implicit in SWE
                # BinaryEncoding for Block members — see Axis demo doc.
                out += struct.pack(f"{self._endian}I", len(payload))
                out += payload
        return bytes(out)

    def decode(self, buf: bytes) -> Dict[str, Any]:
        """Decode one record. Returns a dict keyed by field name.

        Trailing bytes after the declared record are ignored (callers
        that want to demux a concatenated stream should slice on the
        consumed length — exposed via :meth:`decode_with_offset`).
        """
        result, _ = self.decode_with_offset(buf, offset=0)
        return result

    def decode_with_offset(self, buf: bytes, offset: int = 0
                           ) -> Tuple[Dict[str, Any], int]:
        """Decode one record starting at `offset`. Returns ``(dict, new_offset)``
        so callers can walk a concatenated stream of records."""
        out: Dict[str, Any] = {}
        i = offset
        for kind, key, fmt_char in self._compiled:
            if kind == "component":
                full_fmt = f"{self._endian}{fmt_char}"
                size = struct.calcsize(full_fmt)
                if i + size > len(buf):
                    raise ValueError(
                        f"SWEBinaryCodec.decode: ran out of bytes while "
                        f"reading component {key!r} (need {size}, "
                        f"have {len(buf) - i}).")
                (value,) = struct.unpack(full_fmt, buf[i:i + size])
                out[key] = value
                i += size
            else:  # block
                size_fmt = f"{self._endian}I"
                if i + 4 > len(buf):
                    raise ValueError(
                        f"SWEBinaryCodec.decode: ran out of bytes while "
                        f"reading block size prefix for {key!r}.")
                (size,) = struct.unpack(size_fmt, buf[i:i + 4])
                i += 4
                if i + size > len(buf):
                    raise ValueError(
                        f"SWEBinaryCodec.decode: block {key!r} truncated "
                        f"(declared {size}, have {len(buf) - i}).")
                out[key] = bytes(buf[i:i + size])
                i += size
        return out, i


# ---------------------------------------------------------------------------
# DataArray helpers — pack/unpack arrays of scalar values
# ---------------------------------------------------------------------------
#
# Ported from OSH core's `BinaryDataWriter` / `BinaryDataParser` behavior
# (see lib-ogc/swe-common-core in github.com/opensensorhub/osh-core):
#
# * Elements are packed tightly back-to-back per the declared dataType. No
#   padding or alignment between elements.
# * Variable-size arrays carry a single uint32 count *before* the elements;
#   fixed-size arrays carry just the elements.
# * Both layouts use the same big/little-endian convention as scalars.
#
# Scope: arrays of one scalar dataType. Arrays of records/vectors are
# legal SWE Common 3 and are supported by OSH, but require walking a
# member-list tree per element — left as a follow-up; the path is clear
# from the structure here.


def default_datatype_for_schema(schema) -> str:
    """Return the OGC dataType URI OSH would assign by default to a SWE scalar.

    Mirrors `SWEHelper.getDefaultBinaryEncoding()` — when an array's
    `element_type` is a scalar without an explicit BinaryEncoding member,
    OSH picks ``float64`` for Quantity/Time, ``signedInt`` for Count, and
    ``boolean`` for Boolean. Other component kinds (Text, Category) have
    no fixed-width wire type and raise.
    """
    cls_name = type(schema).__name__
    uri = DEFAULT_DATATYPE_URI_FOR_SCALAR.get(cls_name)
    if uri is None:
        raise TypeError(
            f"default_datatype_for_schema: no canonical OGC dataType URI "
            f"for {cls_name}. Supported scalar kinds: "
            f"{sorted(DEFAULT_DATATYPE_URI_FOR_SCALAR)}. For variable-width "
            "kinds (Text, Category) use SWE+JSON or carry the bytes via a "
            "BinaryBlock member instead.")
    return uri


def encode_swe_binary_scalar_array(
    values,
    data_type_uri: str,
    *,
    byte_order: str = "bigEndian",
    variable_size: bool = False,
) -> bytes:
    """Pack a list of scalars into SWE BinaryEncoding bytes.

    Wire layout (matches OSH ``BinaryDataWriter``):

    * ``variable_size=True``  -> ``[uint32 N (BE)][N scalars]``
    * ``variable_size=False`` -> just ``[N scalars]`` (caller knows N from
      the schema's ``element_count.value``)

    All elements share one dataType URI. For mixed-type arrays (rare in
    SWE Common 3) the caller is responsible for assembling the buffer
    member-by-member.

    :param values: Sequence of Python values. Numeric for float/int
        types, bool for the boolean type.
    :param data_type_uri: A key in `DATATYPE_STRUCT_FMT`.
    :param byte_order: ``"bigEndian"`` (default; OSH default) or
        ``"littleEndian"``.
    :param variable_size: Prepend a uint32 count if True.
    """
    fmt_char = DATATYPE_STRUCT_FMT.get(data_type_uri)
    if fmt_char is None:
        raise ValueError(
            f"encode_swe_binary_scalar_array: unsupported dataType "
            f"{data_type_uri!r}. Add it to DATATYPE_STRUCT_FMT.")
    endian = _endian_prefix(byte_order)
    body = struct.pack(f"{endian}{len(values)}{fmt_char}", *values)
    if variable_size:
        return struct.pack(f"{endian}I", len(values)) + body
    return body


def decode_swe_binary_scalar_array(
    buf: bytes,
    data_type_uri: str,
    *,
    byte_order: str = "bigEndian",
    variable_size: bool = False,
    element_count: int | None = None,
) -> list:
    """Inverse of `encode_swe_binary_scalar_array`.

    :param buf: Bytes for exactly one array record (no trailing data).
    :param data_type_uri: Same URI used at encode time.
    :param byte_order: Same byte_order used at encode time.
    :param variable_size: If True, read the leading uint32 count off the
        buffer. If False, ``element_count`` must be provided (the schema
        carries it via `element_count.value`).
    :param element_count: Required when ``variable_size=False``.
    """
    fmt_char = DATATYPE_STRUCT_FMT.get(data_type_uri)
    if fmt_char is None:
        raise ValueError(
            f"decode_swe_binary_scalar_array: unsupported dataType "
            f"{data_type_uri!r}.")
    endian = _endian_prefix(byte_order)
    offset = 0
    if variable_size:
        if len(buf) < 4:
            raise ValueError("Array buffer truncated before count prefix.")
        (n,) = struct.unpack(f"{endian}I", buf[:4])
        offset = 4
    else:
        if element_count is None:
            raise ValueError(
                "Fixed-size array decode requires element_count to be known "
                "from the schema (got None).")
        n = element_count
    full_fmt = f"{endian}{n}{fmt_char}"
    expected = struct.calcsize(full_fmt)
    if len(buf) - offset < expected:
        raise ValueError(
            f"Array buffer too short: need {expected} bytes for {n} elements, "
            f"have {len(buf) - offset}.")
    out = list(struct.unpack(full_fmt, buf[offset:offset + expected]))
    # struct's `?` returns native bool already; numeric URIs stay numeric.
    return out
