#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/6/8
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Runtime codec for the ``application/swe+proto`` wire format.

Wire model
----------
``application/swe+proto`` is the **per-datastream descriptor** Protobuf
encoding. Each DataStream carries a pre-compiled Protobuf schema — a
``google.protobuf.FileDescriptorProto`` (delivered as a
``FileDescriptorSet`` so its imports travel with it) describing a single
per-datastream observation message of the shape:

.. code-block:: protobuf

   message Observation_<dsId> {
     // envelope (1–5) — observation metadata, not result data
     string id            = 1;
     string datastream_id = 2;
     string foi_id        = 3;
     google.protobuf.Timestamp phenomenon_time = 4;
     google.protobuf.Timestamp result_time     = 5;
     // result data (6+) — the SWE Common DataRecord, one field per component
     float air_temperature = 6;
     float relative_humidity = 7;
     ...
   }

An observation on the wire is a serialized instance of that message.
Receivers register the descriptor in a ``DescriptorPool`` and build the
message class dynamically — no ``protoc`` and no generated bindings.

This replaces the earlier self-describing SWE Common 3 ``DataRecord``
codec: that wire form (every value wrapped in a typed SWE submessage) is
**not** what ``application/swe+proto`` means anymore. See
``docs/osh_spec_deviations.md`` (swe-proto-descriptor-format).

Result vs. envelope split
-------------------------
The fields **6+** are the SWE Common record — the same dict shape the
sibling ``application/swe+binary`` codec round-trips and the same thing
that lands in ``ObservationResource.result``. So :meth:`encode` /
:meth:`decode` operate on the **result record** keyed by field name; the
envelope fields (id / datastream_id / foi_id / the two timestamps) are
observation metadata supplied separately by the producing
``Datastream`` (on encode) and recoverable via
:meth:`decode_with_envelope` (on decode). The result dict is never
flattened together with the envelope, so a node that exposes both a
binary and a proto datastream yields the same ``result`` dict from
either.

Bindings dependency
-------------------
Only the ``protobuf`` runtime is required (install via the ``[protobuf]``
extra). Unlike the previous codec, **no generated BinaryEncodings
modules are needed** — the per-datastream message is built dynamically
from the delivered descriptor. ``protobuf`` is imported lazily so
OSHConnect installs without the extra still work; the missing-import
error only fires when a swe+proto datastream is actually used.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Tuple

from .timemanagement import TimeInstant


# Envelope fields (per-datastream message field numbers 1–5). These are
# observation metadata, not result data — kept out of the result record
# dict so the codec's value shape matches the swe+binary codec and
# ``ObservationResource.result``.
ENVELOPE_FIELD_NAMES: Tuple[str, ...] = (
    "id", "datastream_id", "foi_id", "phenomenon_time", "result_time",
)

# Keys used by :meth:`decode_with_envelope` for the metadata block — the
# two timestamps use the CS API JSON spellings so callers can feed them
# straight into ``ObservationResource``/``ObservationOMJSONInline``.
_ENVELOPE_OUT_KEYS = {
    "id": "id",
    "datastream_id": "datastream@id",
    "foi_id": "foi@id",
    "phenomenon_time": "phenomenonTime",
    "result_time": "resultTime",
}

_TIMESTAMP_FULL_NAME = "google.protobuf.Timestamp"

_INSTALL_HINT = (
    "The 'protobuf' runtime is required for application/swe+proto. "
    "Install it with:\n  pip install 'oshconnect[protobuf]'"
)


def _import_protobuf():
    """Import the protobuf runtime modules, with an install hint on failure."""
    try:
        from google.protobuf import (  # noqa: F401
            descriptor_pb2, descriptor_pool, message_factory,
        )
    except ImportError as exc:  # pragma: no cover - exercised via install hint
        raise ImportError(f"{_INSTALL_HINT}\nOriginal error: {exc}") from exc
    return descriptor_pb2, descriptor_pool, message_factory


def wrap_file_descriptor_proto(fdp_bytes: bytes) -> bytes:
    """Wrap a serialized ``FileDescriptorProto`` in a one-file
    ``FileDescriptorSet``.

    Convenience for callers holding a bare ``FileDescriptorProto`` (e.g.
    a single ``.proto`` with no non-google imports). A descriptor with
    non-google dependencies must instead ship a full ``FileDescriptorSet``
    that carries them — otherwise the import won't resolve.
    """
    descriptor_pb2, _, _ = _import_protobuf()
    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.ParseFromString(fdp_bytes)
    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fdp)
    return fds.SerializeToString()


def _coerce_descriptor_set(raw: bytes):
    """Parse ``raw`` into a non-empty ``FileDescriptorSet``.

    The delivery contract is a serialized ``FileDescriptorSet`` (it can
    carry transitive dependencies). A bare ``FileDescriptorProto`` is
    *not* auto-detected — at the wire level it's ambiguous with a Set, so
    decoding one as the other yields silent garbage. Wrap a single
    descriptor with :func:`wrap_file_descriptor_proto` first.
    """
    descriptor_pb2, _, _ = _import_protobuf()
    fds = descriptor_pb2.FileDescriptorSet()
    fds.ParseFromString(raw)
    if not fds.file:
        raise ValueError(
            "application/swe+proto: expected a serialized FileDescriptorSet "
            "but parsed zero files. If you have a bare FileDescriptorProto, "
            "wrap it with oshconnect.swe_protobuf.wrap_file_descriptor_proto().")
    return fds


def _build_message_class(fds_bytes: bytes, message_type: Optional[str]):
    """Build a dynamic message class from a serialized ``FileDescriptorSet``.

    Seeds any ``google/protobuf/*`` imports from the default pool (so
    well-known types like ``Timestamp`` resolve without the caller
    shipping them), adds the provided files in dependency order, then
    resolves ``message_type`` (or the sole message if the set has exactly
    one and no name was given).

    :raises ImportError: if a non-google dependency is missing from the set.
    :raises KeyError: if ``message_type`` is absent / ambiguous.
    """
    import importlib

    descriptor_pb2, descriptor_pool, message_factory = _import_protobuf()
    fds = _coerce_descriptor_set(fds_bytes)
    pool = descriptor_pool.DescriptorPool()

    available: set = set()
    provided = {f.name: f for f in fds.file}

    def seed_google(dep: str) -> None:
        if dep in available or dep in provided:
            return
        if not dep.startswith("google/protobuf/"):
            return
        # Well-known types load lazily — importing their generated module
        # registers the file and gives us its descriptor to copy into our
        # private pool (the default pool's FindFileByName 404s until then).
        stem = dep.rsplit("/", 1)[-1][:-len(".proto")]
        mod = importlib.import_module(f"google.protobuf.{stem}_pb2")
        proto = descriptor_pb2.FileDescriptorProto()
        mod.DESCRIPTOR.CopyToProto(proto)
        for sub in proto.dependency:
            seed_google(sub)
        try:
            pool.Add(proto)
        except TypeError:  # pragma: no cover - already present
            pass
        available.add(dep)

    for f in fds.file:
        for dep in f.dependency:
            seed_google(dep)

    # Topologically add the provided files: only add a file once all of
    # its dependencies are already in the pool. Tolerant of any ordering
    # in the delivered set.
    remaining = list(fds.file)
    progressed = True
    while remaining and progressed:
        progressed = False
        for f in list(remaining):
            if all(dep in available for dep in f.dependency):
                pool.Add(f)
                available.add(f.name)
                remaining.remove(f)
                progressed = True
    if remaining:
        missing = sorted({
            dep for f in remaining for dep in f.dependency if dep not in available
        })
        raise ImportError(
            "application/swe+proto: descriptor set is missing dependencies "
            f"{missing}. Deliver a FileDescriptorSet that includes all "
            "transitive imports (e.g. protoc --include_imports "
            "--descriptor_set_out).")

    if not message_type:
        message_names = [
            f"{f.package + '.' if f.package else ''}{m.name}"
            for f in fds.file for m in f.message_type
        ]
        if len(message_names) != 1:
            raise KeyError(
                "application/swe+proto: descriptor set carries "
                f"{len(message_names)} message types {message_names}; a "
                "message_type must be specified to disambiguate.")
        message_type = message_names[0]

    descriptor = pool.FindMessageTypeByName(message_type)
    return descriptor, message_factory.GetMessageClass(descriptor)


def _set_timestamp(ts_field, value: Any) -> None:
    """Populate a ``google.protobuf.Timestamp`` submessage from a Python time.

    Accepts an ISO 8601 string, epoch seconds (int/float), a ``datetime``,
    or a `TimeInstant`.
    """
    if isinstance(value, TimeInstant):
        value = value.get_iso_time()
    if isinstance(value, str):
        ts_field.FromJsonString(value)
    elif isinstance(value, bool):
        raise TypeError("Timestamp value cannot be a bool.")
    elif isinstance(value, (int, float)):
        secs = int(value)
        ts_field.seconds = secs
        ts_field.nanos = int(round((value - secs) * 1_000_000_000))
    elif hasattr(value, "year") and hasattr(value, "month"):  # datetime-like
        ts_field.FromDatetime(value)
    else:
        raise TypeError(
            "Timestamp value must be an ISO 8601 string, epoch seconds, "
            f"datetime, or TimeInstant; got {type(value).__name__}.")


def _is_timestamp(field_descriptor) -> bool:
    msg_type = field_descriptor.message_type
    return msg_type is not None and msg_type.full_name == _TIMESTAMP_FULL_NAME


# ---------------------------------------------------------------------------
# Schema generation — translate a SWE Common record into a per-datastream
# observation descriptor (the inverse of OSH's ProtoSchemaWriter).
# ---------------------------------------------------------------------------


def _proto_field_name(name: str) -> str:
    """Sanitize a SWE component name into a valid proto3 field identifier.

    Non-identifier characters become ``_``; a leading digit is prefixed
    with ``_``. Names that are already valid identifiers (the common case
    — ``temp``, ``samples``, ``clear_sky``) pass through unchanged.
    """
    if not name:
        raise ValueError("Cannot generate a proto field from an unnamed component.")
    sanitized = re.sub(r"[^0-9A-Za-z_]", "_", name)
    if sanitized[0].isdigit():
        sanitized = "_" + sanitized
    return sanitized


_OGC_DATATYPE_BASE = "http://www.opengis.net/def/dataType/OGC/0/"

# OGC SWE dataType URI → proto field type, mirroring the node's
# ProtoSchemaWriter.getDataType (FLOAT→float, DOUBLE→double, signed/unsigned
# int widths, signedByte→sint32). The descriptor must declare the same wire
# type the producer encodes, or the bytes won't interoperate — so a float32
# quantity becomes ``float``, not ``double``.
_DATATYPE_URI_TO_PROTO: Dict[str, int] = {}


def _datatype_uri_to_proto(uri: str) -> int:
    descriptor_pb2, _, _ = _import_protobuf()
    if not _DATATYPE_URI_TO_PROTO:
        T = descriptor_pb2.FieldDescriptorProto
        b = _OGC_DATATYPE_BASE
        _DATATYPE_URI_TO_PROTO.update({
            b + "double": T.TYPE_DOUBLE,
            b + "float64": T.TYPE_DOUBLE,
            b + "float32": T.TYPE_FLOAT,
            b + "signedByte": T.TYPE_SINT32,
            b + "unsignedByte": T.TYPE_UINT32,
            b + "signedShort": T.TYPE_INT32,
            b + "unsignedShort": T.TYPE_UINT32,
            b + "signedInt": T.TYPE_INT32,
            b + "unsignedInt": T.TYPE_UINT32,
            b + "signedLong": T.TYPE_INT64,
            b + "unsignedLong": T.TYPE_UINT64,
        })
    if uri not in _DATATYPE_URI_TO_PROTO:
        raise NotImplementedError(
            f"swe+proto schema generation: dataType {uri!r} has no proto "
            f"mapping. Known: {sorted(_DATATYPE_URI_TO_PROTO)}")
    return _DATATYPE_URI_TO_PROTO[uri]


def _is_iso_time(component) -> bool:
    """A Time is ISO (→ Timestamp) when its uom is the ISO-8601 calendar
    reference; otherwise it's a numeric epoch (→ double), matching the
    node's ``Time.isIsoTime()``."""
    uom = getattr(component, "uom", None)
    href = getattr(uom, "href", None) if uom is not None else None
    return bool(href and "ISO-8601" in str(href))


_PROTO_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_proto_ident(name: str) -> bool:
    return bool(_PROTO_IDENT_RE.match(name))


def _allowed_tokens(component) -> Optional[List[str]]:
    """Return a Category's ``AllowedTokens`` value list, or ``None``.

    A ``Category`` constrained to a fixed token set becomes a proto enum
    (matching the node's ``writeEnum``); an unconstrained one stays a
    string. The constraint is loosely typed (``Any``) — accept either an
    ``AllowedTokens``-shaped dict (``{"values": [...]}``) or an object
    exposing ``values``. A constraint without a non-empty value list
    (e.g. pattern-only) returns ``None`` and the component stays a string.
    """
    from .swe_components import CategorySchema
    if not isinstance(component, CategorySchema):
        return None
    constraint = getattr(component, "constraint", None)
    if not constraint:
        return None
    if isinstance(constraint, Mapping):
        values = constraint.get("values")
    else:
        values = getattr(constraint, "values", None)
    if isinstance(values, (list, tuple)) and len(values) > 0:
        return [str(v) for v in values]
    return None


def _scalar_proto_type(component, data_type: Optional[str] = None) -> Tuple[int, Optional[str]]:
    """Map a SWE scalar component to a ``(proto field type, type_name)`` pair.

    Faithful to the node's ``ProtoSchemaWriter.getDataType``: the numeric
    wire type follows the component's OGC ``dataType`` when one is known
    (``data_type`` arg, or a ``data_type``/``dataType`` attribute on the
    component) — so float32 → ``float``, signedLong → ``int64``, etc. When
    no dataType is known the node *defaults* apply (Quantity → double,
    Count → int32), which match the node's own defaults. Time → ``Timestamp``
    when ISO (wire-identical to the node's ``Time{seconds,nanos}``) else
    ``double``; Category/Text → string; Boolean → bool.

    Nested records and vectors are handled by the caller
    (``build_observation_descriptor_set``) before reaching here, so this
    only sees leaf components.

    :raises NotImplementedError: for components with no scalar mapping —
        DataChoice, ranges, geometry, matrices — or an unmapped dataType.
    """
    descriptor_pb2, _, _ = _import_protobuf()
    from .swe_components import (
        BooleanSchema, CategorySchema, CountSchema, QuantitySchema,
        TextSchema, TimeSchema,
    )
    T = descriptor_pb2.FieldDescriptorProto
    dt = data_type or getattr(component, "data_type", None) or getattr(component, "dataType", None)

    if isinstance(component, BooleanSchema):
        return T.TYPE_BOOL, None
    if isinstance(component, (QuantitySchema, CountSchema)):
        if dt:
            return _datatype_uri_to_proto(dt), None
        # No declared dataType → node defaults (Quantity DOUBLE, Count INT).
        return (T.TYPE_DOUBLE if isinstance(component, QuantitySchema)
                else T.TYPE_INT32), None
    if isinstance(component, TimeSchema):
        if dt:
            return _datatype_uri_to_proto(dt), None
        if _is_iso_time(component):
            return T.TYPE_MESSAGE, ".google.protobuf.Timestamp"
        return T.TYPE_DOUBLE, None
    if isinstance(component, (CategorySchema, TextSchema)):
        # A constraint-free Category/Text maps to string (matching the node).
        # A Category *with* an AllowedTokens constraint is handled earlier in
        # build_observation_descriptor_set (emitted as a proto enum), so it
        # never reaches here.
        return T.TYPE_STRING, None
    raise NotImplementedError(
        f"swe+proto schema generation: component "
        f"{type(component).__name__} ({getattr(component, 'name', '?')!r}) is "
        "not a supported scalar. DataArray/DataChoice, ranges, geometry, "
        "and matrices are not yet translatable to a per-datastream "
        "observation message.")


def build_observation_descriptor_set(
    record,
    *,
    message_name: str = "Observation",
    package: str = "oshconnect.sweproto",
    datatype_by_path: Optional[Mapping[str, str]] = None,
) -> Tuple[bytes, str]:
    """Build a per-datastream swe+proto observation descriptor from a SWE record.

    Produces the inverse of OSH's ``ProtoSchemaWriter``: a serialized
    ``FileDescriptorSet`` for a message with the fixed envelope at fields
    1–5 (``id``, ``datastream_id``, ``foi_id``, ``phenomenon_time``,
    ``result_time``) and the record's components mapped to result fields
    6+ in declaration order. Nested records and vectors become nested
    ``Rec<N>`` / ``Vec<N>`` message types (inner fields numbered from 1),
    recursed to arbitrary depth.

    :param record: a SWE ``DataRecordSchema`` (the ``record_schema`` that
        the SWE+JSON / SWE+Binary datastream schemas carry).
    :param message_name: the generated message name (e.g.
        ``"Observation_<dsId>"``).
    :param package: the proto package for the generated message.
    :param datatype_by_path: optional ``{json_pointer_ref: dataType_uri}``
        map giving the OGC dataType of leaf components by their record path
        (e.g. ``{"/temp": ".../float32", "/pos/x": ".../float32"}``) — the
        same refs a SWE ``BinaryEncoding`` uses. Lets float32 / int-width
        components map to the matching proto wire type instead of the
        defaults. ``SWEProtobufDatastreamRecordSchema.from_other_schema``
        builds this automatically from a SWE+Binary source.
    :returns: ``(file_descriptor_set_bytes, fully_qualified_message_type)``.
    :raises TypeError: if ``record`` is not a ``DataRecordSchema``.
    :raises NotImplementedError: for components not yet translatable
        (DataChoice, ranges, geometry, matrices).
    """
    descriptor_pb2, _, _ = _import_protobuf()
    from .swe_components import DataArraySchema, DataRecordSchema, VectorSchema
    datatypes = dict(datatype_by_path or {})

    if not isinstance(record, DataRecordSchema):
        raise TypeError(
            "swe+proto schema generation requires a DataRecordSchema root; "
            f"got {type(record).__name__}. Wrap scalars/vectors in a "
            "DataRecord first.")

    T = descriptor_pb2.FieldDescriptorProto

    fdp = descriptor_pb2.FileDescriptorProto()
    fdp.name = f"{package.replace('.', '/')}/{message_name}.proto"
    fdp.package = package
    fdp.syntax = "proto3"
    fdp.dependency.append("google/protobuf/timestamp.proto")

    # Nested records / vectors become their own message types in the file,
    # referenced by a message-typed field. Names are cosmetic (the wire is
    # field-number driven), but we mirror the node's `Rec<N>` / `Vec<N>`
    # convention; a counter guarantees uniqueness across nesting levels.
    nested_counter = [0]

    def add_field(msg_proto, name, number, ftype, type_name=None, repeated=False):
        f = msg_proto.field.add()
        f.name = name
        f.number = number
        f.label = T.LABEL_REPEATED if repeated else T.LABEL_OPTIONAL
        f.type = ftype
        if type_name is not None:
            f.type_name = type_name

    def add_component_field(msg_proto, component, number, path, repeated=False):
        """Add one field for ``component`` to ``msg_proto``, creating any
        nested message / enum types it needs. ``repeated`` is set for
        DataArray elements."""
        fname = _proto_field_name(component.name)
        if isinstance(component, DataRecordSchema):
            nested = make_nested("Rec", component.fields, path)
            add_field(msg_proto, fname, number, T.TYPE_MESSAGE,
                      f".{package}.{nested}", repeated)
        elif isinstance(component, VectorSchema):
            nested = make_nested("Vec", component.coordinates, path)
            add_field(msg_proto, fname, number, T.TYPE_MESSAGE,
                      f".{package}.{nested}", repeated)
        elif isinstance(component, DataArraySchema):
            nested = make_array(component.element_type, path)
            add_field(msg_proto, fname, number, T.TYPE_MESSAGE,
                      f".{package}.{nested}", repeated)
        elif _allowed_tokens(component):
            # Constrained Category → proto enum (matching node writeEnum).
            enum_name = make_enum(msg_proto, fname, _allowed_tokens(component))
            add_field(msg_proto, fname, number, T.TYPE_ENUM,
                      f".{package}.{msg_proto.name}.{enum_name}", repeated)
        else:
            ftype, type_name = _scalar_proto_type(
                component, data_type=datatypes.get(path))
            add_field(msg_proto, fname, number, ftype, type_name, repeated)

    def populate(msg_proto, components, start_number, used, path):
        number = start_number
        for component in components:
            fname = _proto_field_name(component.name)
            if fname in used:
                raise ValueError(
                    f"swe+proto schema generation: component name "
                    f"{component.name!r} sanitizes to {fname!r}, which "
                    "collides with a sibling field. Rename to avoid the clash.")
            add_component_field(msg_proto, component, number,
                                f"{path}/{component.name}")
            used.add(fname)
            number += 1

    def make_nested(prefix, components, path):
        """Create a nested message type (inner fields numbered from 1) and
        return its name."""
        nested_counter[0] += 1
        nested_msg = fdp.message_type.add()
        nested_msg.name = f"{prefix}{nested_counter[0]}"
        populate(nested_msg, components, 1, set(), path)
        return nested_msg.name

    def make_array(element_type, path):
        """Create an ``Array<N>`` wrapper message holding one repeated field
        for the element (matching the node's ``writeArraySchema``) and return
        its name. The element may itself be a scalar, record, vector, or
        constrained category."""
        nested_counter[0] += 1
        array_msg = fdp.message_type.add()
        array_msg.name = f"Array{nested_counter[0]}"
        add_component_field(array_msg, element_type, 1,
                            f"{path}/{element_type.name}", repeated=True)
        return array_msg.name

    def make_enum(msg_proto, field_name, tokens):
        """Create an enum type (nested in the containing message, values
        numbered from 0 — matching the node's ``writeEnum``) and return its
        name. Tokens are used verbatim as enum value identifiers, as the
        node does; a token that isn't a valid identifier is an error."""
        enum_proto = msg_proto.enum_type.add()
        enum_name = f"Enum_{field_name}"
        enum_proto.name = enum_name
        for i, token in enumerate(tokens):
            if not _is_proto_ident(token):
                raise ValueError(
                    f"swe+proto schema generation: Category token {token!r} is "
                    "not a valid proto enum identifier "
                    "([A-Za-z_][A-Za-z0-9_]*); the node requires enumerable "
                    "tokens to be valid identifiers.")
            value = enum_proto.value.add()
            value.name = token
            value.number = i
        return enum_name

    top = fdp.message_type.add()
    top.name = message_name
    add_field(top, "id", 1, T.TYPE_STRING)
    add_field(top, "datastream_id", 2, T.TYPE_STRING)
    add_field(top, "foi_id", 3, T.TYPE_STRING)
    add_field(top, "phenomenon_time", 4, T.TYPE_MESSAGE, ".google.protobuf.Timestamp")
    add_field(top, "result_time", 5, T.TYPE_MESSAGE, ".google.protobuf.Timestamp")
    populate(top, record.fields, 6, set(ENVELOPE_FIELD_NAMES), "")

    fds = descriptor_pb2.FileDescriptorSet()
    fds.file.append(fdp)
    return fds.SerializeToString(), f"{package}.{message_name}"


# ---------------------------------------------------------------------------
# .proto source rendering — turn a descriptor into editable text. The text
# is a faithful rendering of the descriptor (not a second generator), so the
# two never drift; edit it and recompile with `from_proto_source`.
# ---------------------------------------------------------------------------


def _proto_type_keyword(field_type) -> Optional[str]:
    descriptor_pb2, _, _ = _import_protobuf()
    T = descriptor_pb2.FieldDescriptorProto
    return {
        T.TYPE_DOUBLE: "double", T.TYPE_FLOAT: "float",
        T.TYPE_INT64: "int64", T.TYPE_UINT64: "uint64",
        T.TYPE_INT32: "int32", T.TYPE_UINT32: "uint32",
        T.TYPE_FIXED64: "fixed64", T.TYPE_FIXED32: "fixed32",
        T.TYPE_SFIXED64: "sfixed64", T.TYPE_SFIXED32: "sfixed32",
        T.TYPE_SINT64: "sint64", T.TYPE_SINT32: "sint32",
        T.TYPE_BOOL: "bool", T.TYPE_STRING: "string", T.TYPE_BYTES: "bytes",
    }.get(field_type)


def _render_enum(enum_proto, indent: int) -> List[str]:
    pad = "  " * indent
    out = [f"{pad}enum {enum_proto.name} {{"]
    for value in enum_proto.value:
        out.append(f"{pad}  {value.name} = {value.number};")
    out.append(f"{pad}}}")
    return out


def _render_message(msg_proto, indent: int) -> List[str]:
    descriptor_pb2, _, _ = _import_protobuf()
    T = descriptor_pb2.FieldDescriptorProto
    pad = "  " * indent
    out = [f"{pad}message {msg_proto.name} {{"]
    for enum_proto in msg_proto.enum_type:
        out.extend(_render_enum(enum_proto, indent + 1))
    for nested in msg_proto.nested_type:
        out.extend(_render_message(nested, indent + 1))
    fpad = "  " * (indent + 1)
    for field in msg_proto.field:
        label = "repeated " if field.label == T.LABEL_REPEATED else ""
        if field.type in (T.TYPE_MESSAGE, T.TYPE_ENUM):
            type_str = field.type_name  # fully-qualified, leading-dot form
        else:
            type_str = _proto_type_keyword(field.type)
        out.append(f"{fpad}{label}{type_str} {field.name} = {field.number};")
    out.append(f"{pad}}}")
    return out


def render_proto_source(file_descriptor_set: bytes,
                        message_type: Optional[str] = None) -> str:
    """Render a ``FileDescriptorSet`` to ``.proto`` source text.

    Renders the file that defines ``message_type`` (or the first
    non-google file if not given) — the per-datastream schema, not its
    imported well-known types. The output is editable and recompilable
    with :meth:`SWEProtobufDatastreamRecordSchema.from_proto_source`.
    """
    fds = _coerce_descriptor_set(file_descriptor_set)
    target = None
    if message_type:
        pkg, _, name = message_type.rpartition(".")
        for f in fds.file:
            if f.package == pkg and any(m.name == name for m in f.message_type):
                target = f
                break
    if target is None:
        target = next((f for f in fds.file
                       if not f.name.startswith("google/protobuf/")), None)
    if target is None:
        raise ValueError("render_proto_source: no renderable file in the set.")

    lines = ['syntax = "proto3";', ""]
    if target.package:
        lines += [f"package {target.package};", ""]
    for dep in target.dependency:
        lines.append(f'import "{dep}";')
    if target.dependency:
        lines.append("")
    for msg_proto in target.message_type:
        lines += _render_message(msg_proto, 0)
        lines.append("")
    return "\n".join(lines).rstrip("\n") + "\n"


def compile_proto_source(proto_text: str, *, protoc: str = "protoc") -> bytes:
    """Compile ``.proto`` source text into a serialized ``FileDescriptorSet``.

    Shells out to ``protoc`` (required — raises if it isn't on PATH). The
    well-known type imports (``google/protobuf/*``) resolve from protoc's
    bundled includes; the codec seeds them, so they are not embedded.
    """
    import os
    import shutil
    import subprocess
    import tempfile

    if shutil.which(protoc) is None and not os.path.isfile(protoc):
        raise RuntimeError(
            f"protoc not found ({protoc!r}). Install the Protocol Buffers "
            "compiler (or pass protoc=<path>) to compile .proto source. The "
            "binary-descriptor path (from_record_schema) needs no protoc.")
    with tempfile.TemporaryDirectory() as work:
        proto_path = os.path.join(work, "schema.proto")
        out_path = os.path.join(work, "schema.fds")
        with open(proto_path, "w", encoding="utf-8") as fh:
            fh.write(proto_text)
        result = subprocess.run(
            [protoc, f"--proto_path={work}",
             f"--descriptor_set_out={out_path}", proto_path],
            capture_output=True, text=True)
        if result.returncode != 0:
            raise ValueError(
                f"protoc failed to compile the .proto source:\n{result.stderr}")
        with open(out_path, "rb") as fh:
            return fh.read()


def primary_message_type(file_descriptor_set: bytes) -> Optional[str]:
    """Fully-qualified name of the first message in the first non-google file
    — the per-datastream observation message (the root)."""
    fds = _coerce_descriptor_set(file_descriptor_set)
    for f in fds.file:
        if f.name.startswith("google/protobuf/") or not f.message_type:
            continue
        name = f.message_type[0].name
        return f"{f.package}.{name}" if f.package else name
    return None


class SWEProtobufCodec:
    """Descriptor-driven encoder/decoder for ``application/swe+proto``.

    Construct from a parsed ``SWEProtobufDatastreamRecordSchema`` (which
    carries the serialized ``FileDescriptorSet`` and the fully-qualified
    per-datastream message type), or directly from
    ``file_descriptor_set`` bytes + ``message_type``.

    :meth:`encode` / :meth:`decode` operate on the **result record** — a
    ``{field_name: value}`` mapping over the per-datastream message's
    result fields (numbers 6+). The envelope fields (``id``,
    ``datastream_id``, ``foi_id``, ``phenomenon_time``, ``result_time``)
    are observation metadata: pass them to :meth:`encode` via
    ``envelope=`` and read them back with :meth:`decode_with_envelope`.

    Supported result-field types: protobuf scalars (numbers, bool,
    string, bytes), ``google.protobuf.Timestamp`` (decoded to an ISO 8601
    string), **enums** (a constrained ``Category`` — encode accepts the
    token string, decode returns it), **nested messages** (nested records
    and vectors recurse into nested dicts; a vector may be given as a
    sequence on encode and comes back as a dict keyed by coordinate name),
    and **repeated** fields. A ``DataArray`` is the node's
    ``Array<N> { repeated <elt> = 1 }`` wrapper, so it round-trips as
    ``{array_name: {element_name: [...]}}``; the element may itself be a
    scalar, record, vector, or constrained category.
    """

    def __init__(
        self,
        schema: Any = None,
        *,
        file_descriptor_set: Optional[bytes] = None,
        message_type: Optional[str] = None,
    ):
        if schema is not None:
            file_descriptor_set = getattr(schema, "file_descriptor_set", None)
            message_type = getattr(schema, "message_type", None)
        if not file_descriptor_set:
            raise ValueError(
                "SWEProtobufCodec requires a FileDescriptorSet — pass a "
                "SWEProtobufDatastreamRecordSchema or file_descriptor_set bytes.")
        self._descriptor, self._message_cls = _build_message_class(
            file_descriptor_set, message_type)
        # Result fields = everything that isn't an envelope field, in
        # field-number order (descriptor.fields is number-ordered).
        self._result_fields = [
            f for f in self._descriptor.fields
            if f.name not in ENVELOPE_FIELD_NAMES
        ]
        self._fields_by_name = {f.name: f for f in self._descriptor.fields}

    @property
    def result_field_names(self) -> List[str]:
        """Names of the per-datastream message's result fields (6+)."""
        return [f.name for f in self._result_fields]

    # -- encode ------------------------------------------------------------

    def encode(self, record: Mapping[str, Any], *,
               envelope: Mapping[str, Any] = None) -> bytes:
        """Encode one observation's result record into wire bytes.

        :param record: ``{field_name: value}`` over the result fields
            (6+). Keys that name envelope fields are ignored here — pass
            those via ``envelope`` instead.
        :param envelope: optional ``{field_name: value}`` for the
            envelope fields (``id``, ``datastream_id``, ``foi_id``,
            ``phenomenon_time``, ``result_time``). Timestamp fields accept
            ISO strings, epoch seconds, ``datetime``, or `TimeInstant`.
        :raises KeyError: if ``record`` names a field absent from the schema.
        """
        if not isinstance(record, Mapping):
            raise TypeError(
                f"swe+proto encode expects a mapping result record, got "
                f"{type(record).__name__}.")
        msg = self._message_cls()
        for name, value in record.items():
            if name in ENVELOPE_FIELD_NAMES:
                continue
            field = self._fields_by_name.get(name)
            if field is None:
                raise KeyError(
                    f"swe+proto: result field {name!r} not in message "
                    f"{self._descriptor.full_name!r}. Known result fields: "
                    f"{self.result_field_names}")
            self._set_field(msg, field, value)
        if envelope:
            for name, value in envelope.items():
                if value is None:
                    continue
                field = self._fields_by_name.get(name)
                if field is None:
                    continue  # envelope key the descriptor doesn't carry
                self._set_field(msg, field, value)
        return msg.SerializeToString()

    def _set_field(self, msg, field, value: Any) -> None:
        if field.is_repeated:
            # DataArray — the node wraps it as `Array<N> { repeated <elt> = 1 }`,
            # so the repeated field lives one message-level down; handle every
            # element kind (scalar / message / enum / timestamp).
            self._set_repeated(msg, field, value)
            return
        if _is_timestamp(field):
            _set_timestamp(getattr(msg, field.name), value)
            return
        if field.message_type is not None:
            # Nested record / vector — recurse into the submessage. The
            # node emits these as `Rec<N>` / `Vec<N>` messages; the wire is
            # driven entirely by field structure, so we walk the descriptor.
            self._set_message(getattr(msg, field.name), field.message_type, value)
            return
        if field.enum_type is not None:
            # Constrained Category — accept the token string and map it to the
            # enum number via the descriptor (an int passes through).
            setattr(msg, field.name, self._enum_number(field, value))
            return
        setattr(msg, field.name, value)

    def _enum_number(self, field, value) -> int:
        if isinstance(value, str):
            enum_value = field.enum_type.values_by_name.get(value)
            if enum_value is None:
                raise KeyError(
                    f"swe+proto: {value!r} is not an allowed token for enum "
                    f"field {field.name!r}. Allowed: "
                    f"{list(field.enum_type.values_by_name)}")
            return enum_value.number
        return int(value)

    def _set_repeated(self, msg, field, values) -> None:
        if not isinstance(values, (list, tuple)):
            raise TypeError(
                f"swe+proto: repeated field {field.name!r} requires a "
                f"list/tuple, got {type(values).__name__}.")
        container = getattr(msg, field.name)
        for item in values:
            if _is_timestamp(field):
                _set_timestamp(container.add(), item)
            elif field.message_type is not None:
                self._set_message(container.add(), field.message_type, item)
            elif field.enum_type is not None:
                container.append(self._enum_number(field, item))
            else:
                container.append(item)

    def _set_message(self, submsg, descriptor, value: Any) -> None:
        """Populate a nested message from a mapping (by field name) or a
        sequence (positionally — e.g. a Vector given as ``[x, y, z]``)."""
        if isinstance(value, Mapping):
            by_name = {f.name: f for f in descriptor.fields}
            for key, sub_value in value.items():
                sub_field = by_name.get(key)
                if sub_field is None:
                    raise KeyError(
                        f"swe+proto: field {key!r} not in nested message "
                        f"{descriptor.full_name!r}. Known fields: {list(by_name)}")
                self._set_field(submsg, sub_field, sub_value)
        elif isinstance(value, (list, tuple)):
            fields = list(descriptor.fields)
            if len(value) != len(fields):
                raise ValueError(
                    f"swe+proto: nested message {descriptor.full_name!r} has "
                    f"{len(fields)} fields but {len(value)} values were given.")
            for sub_field, sub_value in zip(fields, value):
                self._set_field(submsg, sub_field, sub_value)
        else:
            raise TypeError(
                f"swe+proto: nested message {descriptor.full_name!r} requires a "
                f"mapping or sequence value, got {type(value).__name__}.")

    # -- decode ------------------------------------------------------------

    def decode(self, buf: bytes) -> Dict[str, Any]:
        """Decode wire bytes into the result record dict (fields 6+).

        Mirrors ``SWEBinaryCodec.decode`` — returns only the result
        fields, keyed by name, so the dict drops straight into
        ``ObservationResource.result``. Use :meth:`decode_with_envelope`
        to also recover the observation metadata.
        """
        msg = self._message_cls()
        msg.ParseFromString(buf)
        return {f.name: self._get_field(msg, f) for f in self._result_fields}

    def decode_with_envelope(self, buf: bytes) -> Dict[str, Any]:
        """Decode wire bytes into ``{"result": {...}, <metadata>}``.

        The metadata keys use CS API JSON spellings (``datastream@id``,
        ``phenomenonTime``, ``resultTime``, ``foi@id``) and timestamps
        come back as ISO 8601 strings, so the block feeds directly into
        ``ObservationResource`` / ``ObservationOMJSONInline``.
        """
        msg = self._message_cls()
        msg.ParseFromString(buf)
        out: Dict[str, Any] = {
            "result": {f.name: self._get_field(msg, f) for f in self._result_fields}
        }
        for name in ENVELOPE_FIELD_NAMES:
            field = self._fields_by_name.get(name)
            if field is None:
                continue
            out[_ENVELOPE_OUT_KEYS[name]] = self._get_field(msg, field)
        return out

    def _get_field(self, msg, field) -> Any:
        if field.is_repeated:
            return self._get_repeated(msg, field)
        if _is_timestamp(field):
            return getattr(msg, field.name).ToJsonString()
        if field.message_type is not None:
            # Nested record / vector — recurse into the submessage and
            # return a nested dict keyed by field name.
            sub = getattr(msg, field.name)
            return {f.name: self._get_field(sub, f) for f in field.message_type.fields}
        if field.enum_type is not None:
            # Constrained Category — return the token string (matching the
            # sibling SWE codecs), falling back to the raw int for an
            # unknown value (proto3 enums are open).
            return self._enum_name(field, getattr(msg, field.name))
        return getattr(msg, field.name)

    def _enum_name(self, field, number):
        enum_value = field.enum_type.values_by_number.get(number)
        return enum_value.name if enum_value is not None else number

    def _get_repeated(self, msg, field) -> list:
        container = getattr(msg, field.name)
        if _is_timestamp(field):
            return [ts.ToJsonString() for ts in container]
        if field.message_type is not None:
            return [{f.name: self._get_field(item, f)
                     for f in field.message_type.fields} for item in container]
        if field.enum_type is not None:
            return [self._enum_name(field, n) for n in container]
        return list(container)
