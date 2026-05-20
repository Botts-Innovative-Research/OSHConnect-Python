#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Runtime codec for the ``application/swe+proto`` wire format.

Wire model
----------
A single observation is a Protobuf-serialized ``DataRecord`` message from
the SWE Common 3 schemas in
https://github.com/tipatterson-dev/BinaryEncodings. The codec walks the
SWE-side record schema (a pydantic ``AnyComponent`` tree) and, for each
field, populates the matching variant of the protobuf
``AnyComponent`` oneof on the wire — for example, a SWE
``QuantitySchema`` field becomes a ``Quantity`` submessage; a
``TimeSchema`` field becomes a ``Time`` submessage; nested
``DataRecord``/``Vector``/``DataChoice``/``DataArray`` are recursive.

Why a runtime codec instead of using ``google.protobuf.json_format``:
the SWE-side dict uses field *names* as keys and the values are bare
scalars (e.g. ``{"pan": -6.7}``), but on the wire each scalar lives
inside a typed protobuf submessage with extra structure (e.g.
``Quantity.value.number``). The runtime codec is the smallest piece
that knows both shapes.

Bindings dependency
-------------------
The generated Python protobuf bindings are not bundled — install them
with the ``[protobuf]`` extra and produce them from the BinaryEncodings
repo:

.. code-block:: bash

   pip install "oshconnect[protobuf]"
   git clone https://github.com/tipatterson-dev/BinaryEncodings
   cd BinaryEncodings && make protobuf PROTO_LANG=python
   export PYTHONPATH="$PWD/gen/protobuf:$PYTHONPATH"

The codec imports ``sweCommon3_pb2`` (and ``basic_types_pb2``,
``scalar_components_pb2``, ``encodings_pb2``) lazily so that
OSHConnect installs without the extra still work — the missing-import
error only fires when a swe+proto datastream is actually used.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Union

from .schema_datamodels import SWEProtobufDatastreamRecordSchema
from .swe_binary import (
    decode_swe_binary_scalar_array, default_datatype_for_schema,
    encode_swe_binary_scalar_array,
)
from .swe_components import (
    AnyComponentSchema, BooleanSchema, CategorySchema, CountSchema,
    DataArraySchema, DataChoiceSchema, DataRecordSchema, QuantitySchema,
    TextSchema, TimeSchema, VectorSchema,
)


# Lazy-imported holders. Each entry is None until `_load_pb_modules` runs.
_pb: Any = None       # sweCommon3_pb2
_bt: Any = None       # basic_types_pb2
_sc: Any = None       # scalar_components_pb2


_INSTALL_HINT = (
    "Generated SWE Common 3 Protobuf bindings not found. Install with:\n"
    "  pip install 'oshconnect[protobuf]'\n"
    "Then generate the bindings from the BinaryEncodings project:\n"
    "  git clone https://github.com/tipatterson-dev/BinaryEncodings\n"
    "  cd BinaryEncodings && make protobuf PROTO_LANG=python\n"
    "  export PYTHONPATH=\"$PWD/gen/protobuf:$PYTHONPATH\""
)


def _load_pb_modules() -> None:
    """Import the generated protobuf modules on first use.

    Separate function so the import error message can include the
    install/generation hint instead of a bare ``ModuleNotFoundError``.
    """
    global _pb, _bt, _sc
    if _pb is not None:
        return
    try:
        import sweCommon3_pb2 as pb
        import basic_types_pb2 as bt
        import scalar_components_pb2 as sc
    except ImportError as exc:
        raise ImportError(f"{_INSTALL_HINT}\nOriginal error: {exc}") from exc
    _pb, _bt, _sc = pb, bt, sc


# Map a SWE Common component class to the (`AnyComponent` oneof field name,
# encode_func, decode_func) triple. Populated lazily in `_dispatch_table`
# because the protobuf modules aren't imported at import time.
_DISPATCH_TABLE: Dict[type, tuple] = {}


def _dispatch_table() -> Dict[type, tuple]:
    if _DISPATCH_TABLE:
        return _DISPATCH_TABLE
    _load_pb_modules()
    _DISPATCH_TABLE.update({
        BooleanSchema: ("boolean_component", _encode_boolean, _decode_boolean),
        CountSchema: ("count_component", _encode_count, _decode_count),
        QuantitySchema: ("quantity_component", _encode_quantity, _decode_quantity),
        TimeSchema: ("time_component", _encode_time, _decode_time),
        CategorySchema: ("category_component", _encode_category, _decode_category),
        TextSchema: ("text_component", _encode_text, _decode_text),
        DataRecordSchema: ("data_record", _encode_data_record, _decode_data_record),
        VectorSchema: ("vector", _encode_vector, _decode_vector),
        DataChoiceSchema: ("data_choice", _encode_data_choice, _decode_data_choice),
        # DataArray uses the EncodedValues.inline_data path: pack the
        # element values as SWE BinaryEncoding bytes (per the OSH
        # reference impl in BinaryDataWriter.java) and stuff them in
        # values.inline_data. Decode reads element_count + inline_data
        # and reverses. Supports arrays of scalars (Quantity, Count,
        # Boolean, Time); arrays of records/vectors raise.
        DataArraySchema: ("data_array", _encode_data_array, _decode_data_array),
    })
    return _DISPATCH_TABLE


# ---------------------------------------------------------------------------
# Scalar encoders / decoders. Each fills the leaf `value` slot on a freshly
# created protobuf submessage and returns it; decoders take a submessage and
# return the Python value.
# ---------------------------------------------------------------------------


def _encode_boolean(_schema: BooleanSchema, value: Any):
    msg = _sc.Boolean()
    msg.value = bool(value)
    return msg


def _decode_boolean(msg) -> bool:
    return bool(msg.value)


def _encode_count(_schema: CountSchema, value: Any):
    msg = _sc.Count()
    msg.value = int(value)
    return msg


def _decode_count(msg) -> int:
    return int(msg.value)


def _encode_quantity(_schema: QuantitySchema, value: Any):
    msg = _sc.Quantity()
    msg.value.number = float(value)
    return msg


def _decode_quantity(msg) -> Union[float, str]:
    """Decode a `Quantity` value.

    The encoder only writes ``NumberOrSpecial.number``, so messages this
    SDK produced always come back as `float`. The `special` branch
    (returning a `SpecialValue` enum name like ``"NA_N"``/``"POS_INFINITY"``
    as a string) is kept so the codec can also parse messages from other
    SWE Common 3 implementations that *do* emit the special variants —
    drop the branch when that interop requirement goes away.
    """
    if msg.value.WhichOneof("kind") == "number":
        return msg.value.number
    return _bt.SpecialValue.Name(msg.value.special)


def _encode_time(_schema: TimeSchema, value: Any):
    msg = _sc.Time()
    if isinstance(value, str):
        msg.value.date_time = value
    elif isinstance(value, (int, float)):
        msg.value.number = float(value)
    else:
        raise TypeError(
            f"Time value must be ISO 8601 string or numeric epoch seconds, "
            f"got {type(value).__name__}")
    return msg


def _decode_time(msg) -> Union[str, float]:
    kind = msg.value.WhichOneof("kind")
    if kind == "date_time":
        return msg.value.date_time
    if kind == "number":
        return msg.value.number
    return _bt.SpecialValue.Name(msg.value.special)


def _encode_category(_schema: CategorySchema, value: Any):
    msg = _sc.Category()
    msg.value = str(value)
    return msg


def _decode_category(msg) -> str:
    return msg.value


def _encode_text(_schema: TextSchema, value: Any):
    msg = _sc.Text()
    msg.value = str(value)
    return msg


def _decode_text(msg) -> str:
    return msg.value


# ---------------------------------------------------------------------------
# Composite encoders / decoders. Recurse via `_dispatch_table`.
# ---------------------------------------------------------------------------


def _set_component_value(target_any_component, schema: AnyComponentSchema, value: Any) -> None:
    """Populate one `AnyComponent` oneof in-place given a SWE schema + value."""
    table = _dispatch_table()
    for schema_cls, (oneof_field, encoder, _) in table.items():
        if isinstance(schema, schema_cls):
            sub_msg = encoder(schema, value)
            getattr(target_any_component, oneof_field).CopyFrom(sub_msg)
            return
    raise TypeError(
        f"swe_protobuf: unsupported component type {type(schema).__name__} "
        f"({schema.__class__.__module__}). Supported: "
        f"{sorted(s.__name__ for s in table)}")


def _get_component_value(any_component, schema: AnyComponentSchema) -> Any:
    """Extract the Python value from an `AnyComponent` oneof using its SWE schema."""
    table = _dispatch_table()
    oneof_set = any_component.WhichOneof("component")
    if oneof_set is None:
        raise ValueError("AnyComponent message is empty (no oneof variant set).")
    for _, (oneof_field, _, decoder) in table.items():
        if oneof_field == oneof_set:
            return decoder(getattr(any_component, oneof_field))
    raise TypeError(
        f"swe_protobuf: protobuf carried oneof variant {oneof_set!r} but "
        f"no decoder is registered for it.")


def _encode_data_record(schema: DataRecordSchema, value: Mapping[str, Any]):
    """Build a protobuf `DataRecord` from a `{name: value}` mapping.

    Field order follows ``schema.fields`` so the wire bytes are deterministic.
    Each value is encoded into the matching protobuf submessage by recursive
    dispatch — nested DataRecords therefore work transparently.
    """
    if not isinstance(value, Mapping):
        raise TypeError(
            f"DataRecord requires a mapping value, got {type(value).__name__}")
    msg = _pb.DataRecord()
    for field_schema in schema.fields:
        if field_schema.name not in value:
            raise KeyError(
                f"DataRecord field {field_schema.name!r} missing from value mapping. "
                f"Provided keys: {list(value.keys())}")
        named = msg.fields.add()
        named.name = field_schema.name
        _set_component_value(named.component.inline, field_schema, value[field_schema.name])
    return msg


def _decode_data_record(msg) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for named in msg.fields:
        # Re-decoding requires the SWE schema — see SWEProtobufCodec.decode
        # for the dispatcher that hands the schema back in. The schema-less
        # path is only used for *nested* records where the parent's
        # `_decode_*` already pairs each child with its schema. Here we look
        # up via the inline component's oneof.
        out[named.name] = _decode_any_component(named.component.inline)
    return out


def _decode_any_component(any_component) -> Any:
    """Schema-less decode of an AnyComponent — used for nested records where
    the parent codec walks both trees in lockstep (see _decode_data_record).
    """
    table = _dispatch_table()
    oneof = any_component.WhichOneof("component")
    if oneof is None:
        return None
    for _, (oneof_field, _, decoder) in table.items():
        if oneof_field == oneof:
            sub = getattr(any_component, oneof_field)
            return decoder(sub)
    raise TypeError(f"Unknown AnyComponent oneof variant {oneof!r}.")


# `Vector.coordinates[i].coordinate` is a narrower `CoordinateComponent`
# oneof — not the full `AnyComponent`. Per SWE Common 3, only Count /
# Quantity / Time are valid vector coordinate types, so we dispatch on a
# small lookup rather than reusing `_set_component_value`.
_COORDINATE_ONEOF_MAP: Dict[type, tuple] = {}


def _coordinate_oneof_map() -> Dict[type, tuple]:
    if _COORDINATE_ONEOF_MAP:
        return _COORDINATE_ONEOF_MAP
    _load_pb_modules()
    _COORDINATE_ONEOF_MAP.update({
        QuantitySchema: ("quantity", _encode_quantity, _decode_quantity),
        CountSchema: ("count", _encode_count, _decode_count),
        TimeSchema: ("time", _encode_time, _decode_time),
    })
    return _COORDINATE_ONEOF_MAP


def _encode_vector(schema: VectorSchema, value: Any):
    """Build a protobuf `Vector` from a sequence (one entry per coordinate)."""
    if not isinstance(value, (list, tuple)):
        raise TypeError(
            f"Vector requires a list/tuple value, got {type(value).__name__}")
    if len(value) != len(schema.coordinates):
        raise ValueError(
            f"Vector expects {len(schema.coordinates)} coordinates, got {len(value)}.")
    msg = _pb.Vector()
    coord_map = _coordinate_oneof_map()
    for coord_schema, v in zip(schema.coordinates, value):
        named = msg.coordinates.add()
        named.name = coord_schema.name
        entry = next((e for cls, e in coord_map.items()
                      if isinstance(coord_schema, cls)), None)
        if entry is None:
            raise TypeError(
                f"Vector.coordinates: unsupported coordinate type "
                f"{type(coord_schema).__name__}; only Quantity, Count, "
                f"and Time are valid per SWE Common 3.")
        oneof_field, encoder, _ = entry
        sub_msg = encoder(coord_schema, v)
        getattr(named.coordinate, oneof_field).CopyFrom(sub_msg)
    return msg


def _decode_vector(msg) -> list:
    """Decode a `Vector` into a list — schema-less variant used only when the
    parent codec has no schema to pair with. Otherwise see
    `_schema_aware_decode`.
    """
    coord_map = _coordinate_oneof_map()
    out = []
    for named in msg.coordinates:
        oneof = named.coordinate.WhichOneof("component")
        for _, (oneof_field, _, decoder) in coord_map.items():
            if oneof_field == oneof:
                out.append(decoder(getattr(named.coordinate, oneof_field)))
                break
    return out


def _encode_data_choice(schema: DataChoiceSchema, value: Any):
    """Build a `DataChoice` from a ``(item_name, value)`` tuple or
    ``{item_name: value}`` single-key mapping. The choice value (the
    discriminator) goes into ``choice_value``."""
    if isinstance(value, Mapping):
        if len(value) != 1:
            raise ValueError(
                f"DataChoice mapping must have exactly one key (the selected item), "
                f"got {len(value)}: {list(value.keys())}")
        item_name, item_value = next(iter(value.items()))
    elif isinstance(value, tuple) and len(value) == 2:
        item_name, item_value = value
    else:
        raise TypeError(
            "DataChoice value must be a single-key mapping or (name, value) tuple, "
            f"got {type(value).__name__}")
    msg = _pb.DataChoice()
    # Find the item schema by name
    item_schemas = getattr(schema, "items", None) or []
    chosen = next((it for it in item_schemas if getattr(it, "name", None) == item_name), None)
    if chosen is None:
        raise KeyError(
            f"DataChoice item {item_name!r} not found in schema. Available: "
            f"{[it.name for it in item_schemas]}")
    msg.choice_value.value = item_name
    named = msg.items.add()
    named.name = item_name
    _set_component_value(named.component.inline, chosen, item_value)
    return msg


def _decode_data_choice(msg) -> dict:
    if not msg.items:
        return {}
    # Use the discriminator if present, else fall back to the only item.
    chosen_name = msg.choice_value.value or msg.items[0].name
    chosen = next((it for it in msg.items if it.name == chosen_name), msg.items[0])
    return {chosen.name: _decode_any_component(chosen.component.inline)}


# Mapping of SWE byteOrder string -> protobuf ByteOrder enum value. Set on
# first use because the enum lives in the lazy-imported encodings module.
def _pb_byte_order(byte_order: str):
    import encodings_pb2 as enc
    return {
        "bigEndian": enc.ByteOrder.BYTE_ORDER_BIG_ENDIAN,
        "littleEndian": enc.ByteOrder.BYTE_ORDER_LITTLE_ENDIAN,
    }[byte_order]


def _encode_data_array(schema: DataArraySchema, value: Any):
    """Build a protobuf `DataArray` from a list of element values.

    Ported from OSH's `BinaryDataWriter`: pack element values as SWE
    BinaryEncoding bytes and stuff them in `values.inline_data`. The
    accompanying `encoding` field carries the wire spec (byte order,
    raw vs base64, the members list with one Component per element-type
    scalar). `element_count.inline.value` carries the array length so
    decoders don't have to inspect inline_data.

    Currently supports arrays of **one scalar type** — Quantity, Count,
    Boolean, Time. Arrays of records/vectors are legal SWE Common 3
    (and OSH supports them) but require walking a per-element member
    tree; see the follow-up note in `_dispatch_table()`.
    """
    import encodings_pb2 as enc
    if not isinstance(value, (list, tuple)):
        raise TypeError(
            f"DataArray requires a list/tuple, got {type(value).__name__}")
    element_schema = schema.element_type
    try:
        data_type_uri = default_datatype_for_schema(element_schema)
    except TypeError as exc:
        raise TypeError(
            f"DataArray.element_type {type(element_schema).__name__} is not "
            "a supported scalar; arrays of records/vectors are not yet "
            "implemented (only scalar element types — Quantity / Count / "
            "Boolean / Time)."
        ) from exc

    msg = _pb.DataArray()
    msg.element_count.inline.value = len(value)
    # Represent the element-type as a single NamedComponent — descriptive
    # only; the actual values are packed into inline_data below.
    elem_named = msg.element_type
    elem_named.name = getattr(element_schema, "name", "element")
    _set_component_value(elem_named.component.inline, element_schema, value[0] if value else 0)

    # Declare the wire spec used to pack inline_data.
    msg.encoding.binary_encoding.byte_order = _pb_byte_order("bigEndian")
    msg.encoding.binary_encoding.byte_encoding = enc.ByteEncodingMethod.BYTE_ENCODING_METHOD_RAW
    member = msg.encoding.binary_encoding.members.add()
    member.component.ref = f"/{elem_named.name}"
    member.component.data_type = data_type_uri

    # Pack and stuff. No size prefix in inline_data itself — element_count
    # carries N at the protobuf level, mirroring OSH's fixed-size layout.
    msg.values.inline_data = encode_swe_binary_scalar_array(
        list(value), data_type_uri, byte_order="bigEndian", variable_size=False)
    return msg


def _decode_data_array(msg) -> list:
    """Inverse of `_encode_data_array`.

    Drives off the protobuf message's own `element_count` + `encoding`
    + `values.inline_data` — *not* the SWE-side schema — so messages
    produced by other SWE Common 3 implementations decode the same as
    ones produced by this codec.
    """
    n = msg.element_count.inline.value or 0
    if n == 0:
        return []
    members = list(msg.encoding.binary_encoding.members)
    if not members:
        raise ValueError(
            "DataArray.encoding.binary_encoding.members is empty; cannot "
            "decode inline_data without knowing the element wire type.")
    # Scalar-only path: expect exactly one Component member.
    first = members[0]
    if first.WhichOneof("member") != "component":
        raise NotImplementedError(
            "DataArray decode: only scalar element types are supported; "
            f"first member is {first.WhichOneof('member')!r}.")
    data_type_uri = first.component.data_type
    # Map protobuf ByteOrder enum back to the SWE string.
    import encodings_pb2 as enc
    bo = msg.encoding.binary_encoding.byte_order
    byte_order = ("bigEndian"
                  if bo == enc.ByteOrder.BYTE_ORDER_BIG_ENDIAN
                  else "littleEndian")
    return decode_swe_binary_scalar_array(
        msg.values.inline_data, data_type_uri,
        byte_order=byte_order, variable_size=False, element_count=n)


# ---------------------------------------------------------------------------
# Public codec class
# ---------------------------------------------------------------------------


class SWEProtobufCodec:
    """Schema-driven encoder/decoder for ``application/swe+proto``.

    Construct from a parsed `SWEProtobufDatastreamRecordSchema` (or directly
    from a SWE Common `AnyComponent` schema tree); call :meth:`encode` /
    :meth:`decode` to round-trip records.

    Supported component types: ``Boolean``, ``Count``, ``Quantity``,
    ``Time``, ``Category``, ``Text``, ``DataRecord`` (incl. nested),
    ``Vector``, ``DataChoice``, and ``DataArray`` (of scalar element
    types — Quantity, Count, Boolean, Time). ``Matrix``, ``Geometry``,
    and the ``*Range`` variants — plus arrays of records/vectors — are
    not yet implemented; encoding such a record raises ``TypeError``.

    DataArray wire format mirrors OSH's `BinaryDataWriter` reference
    implementation (lib-ogc/swe-common-core): element values are packed
    tightly back-to-back as SWE BinaryEncoding bytes (see
    ``oshconnect.swe_binary.encode_swe_binary_scalar_array``) and
    placed in ``values.inline_data``. The accompanying
    ``encoding.binary_encoding`` carries the dataType URI used to pack
    them, so the wire is self-describing.
    """

    def __init__(
        self,
        schema: Union[SWEProtobufDatastreamRecordSchema, AnyComponentSchema],
    ):
        _load_pb_modules()
        if isinstance(schema, SWEProtobufDatastreamRecordSchema):
            self._root_schema = schema.record_schema
        elif isinstance(schema, AnyComponentSchema):
            self._root_schema = schema
        else:
            raise TypeError(
                "SWEProtobufCodec expects an SWEProtobufDatastreamRecordSchema "
                f"or AnyComponent schema, got {type(schema).__name__}.")

    def encode(self, value: Any) -> bytes:
        """Encode a single observation. ``value`` is whatever the root schema
        expects — a mapping for DataRecord, a sequence for Vector / DataArray,
        a scalar for a scalar-rooted schema."""
        table = _dispatch_table()
        # Find the encoder for the root schema
        for schema_cls, (_, encoder, _) in table.items():
            if isinstance(self._root_schema, schema_cls):
                msg = encoder(self._root_schema, value)
                return msg.SerializeToString()
        raise TypeError(
            f"swe_protobuf: cannot encode root schema of type "
            f"{type(self._root_schema).__name__}; only DataRecord / Vector / "
            f"DataChoice / DataArray and scalar types are currently wired up.")

    def decode(self, buf: bytes) -> Any:
        """Decode bytes back into a Python value. Inverse of :meth:`encode`."""
        table = _dispatch_table()
        # Determine the wire-side message type from the root schema, parse
        # the bytes into it, then dispatch the schema-aware decoder.
        for schema_cls, (_, _, decoder) in table.items():
            if isinstance(self._root_schema, schema_cls):
                msg_cls = _pb_message_for_schema(schema_cls)
                msg = msg_cls()
                msg.ParseFromString(buf)
                return _schema_aware_decode(self._root_schema, msg)
        raise TypeError(
            f"swe_protobuf: cannot decode root schema of type "
            f"{type(self._root_schema).__name__}.")


def _pb_message_for_schema(schema_cls: type) -> type:
    """Map a SWE schema class to its top-level protobuf message class."""
    return {
        BooleanSchema: _sc.Boolean,
        CountSchema: _sc.Count,
        QuantitySchema: _sc.Quantity,
        TimeSchema: _sc.Time,
        CategorySchema: _sc.Category,
        TextSchema: _sc.Text,
        DataRecordSchema: _pb.DataRecord,
        VectorSchema: _pb.Vector,
        DataChoiceSchema: _pb.DataChoice,
        DataArraySchema: _pb.DataArray,
    }[schema_cls]


def _schema_aware_decode(schema: AnyComponentSchema, msg) -> Any:
    """Decode a protobuf submessage using the matching SWE schema.

    Pairs with `_schema_aware_encode` so nested records keep their field
    *names* (the schema-less decode loses them once you're past one layer).
    """
    if isinstance(schema, DataRecordSchema):
        out: Dict[str, Any] = {}
        # Pair each named protobuf field with the schema field of the same
        # name (don't trust positional alignment in case the encoder ever
        # reorders).
        by_name = {nf.name: nf for nf in msg.fields}
        for field_schema in schema.fields:
            named = by_name.get(field_schema.name)
            if named is None:
                continue
            out[field_schema.name] = _schema_aware_decode(
                field_schema,
                getattr(named.component.inline,
                        _dispatch_table()[type(field_schema)][0]),
            )
        return out
    table = _dispatch_table()
    for schema_cls, (_, _, decoder) in table.items():
        if isinstance(schema, schema_cls) and schema_cls not in (
                DataRecordSchema, VectorSchema, DataChoiceSchema, DataArraySchema):
            return decoder(msg)
    if isinstance(schema, VectorSchema):
        # Coordinate dispatch is on CoordinateComponent (a narrower oneof
        # than AnyComponent), so look up via _coordinate_oneof_map.
        coord_map = _coordinate_oneof_map()
        out = []
        for coord_schema, named in zip(schema.coordinates, msg.coordinates):
            entry = next((e for cls, e in coord_map.items()
                          if isinstance(coord_schema, cls)), None)
            if entry is None:
                raise TypeError(
                    f"Vector.coordinates carries unsupported type "
                    f"{type(coord_schema).__name__}.")
            oneof_field, _, decoder = entry
            out.append(decoder(getattr(named.coordinate, oneof_field)))
        return out
    if isinstance(schema, DataChoiceSchema):
        return _decode_data_choice(msg)
    if isinstance(schema, DataArraySchema):
        return _decode_data_array(msg)
    raise TypeError(
        f"_schema_aware_decode: unsupported schema type {type(schema).__name__}.")
