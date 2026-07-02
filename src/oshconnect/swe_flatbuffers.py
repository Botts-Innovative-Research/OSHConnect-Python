#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Runtime codec for the ``application/swe+flatbuffers`` wire format.

OpenSensorHub encodes ``swe+flatbuffers`` observations as **schemaless
FlexBuffers**, not schema-compiled FlatBuffers. Each observation is a
length-prefixed frame: a 4-byte big-endian length followed by one
FlexBuffers document (see ``FlexFraming`` in
``sensorhub-service-consys-flatbuffers`` — "Over NATS each proactive
observation is published as one message whose payload is one such
frame"). The FlexBuffers root is located from the *end* of the buffer,
which is why concatenated bare documents would be unsplittable and the
length prefix is required.

FlexBuffers is self-describing, so decoding needs no compiled bindings —
``flatbuffers.flexbuffers.Loads`` returns a plain ``dict`` keyed by the
SWE field names (``phenomenon_time``, ``result_time``, ``result`` …).
This sidesteps the ``flatc --python`` "vectors of unions" limitation
(``docs/osh_spec_deviations.md`` → ``flatc-python-vector-of-union``),
which only blocks *schema-compiled* FlatBuffers bindings — a path OSH's
wire format does not use.

Requires the optional ``flatbuffers`` dependency
(``pip install oshconnect[flatbuffers]``); the import is deferred so the
rest of the SDK can register and round-trip schemas naming this format
without it installed.
"""

from __future__ import annotations

import struct
from typing import Any, Union

from .schema_datamodels import SWEFlatBuffersDatastreamRecordSchema
from .swe_components import AnyComponentSchema

# 4-byte big-endian frame length prefix, matching the Java ``FlexFraming``.
_FRAME_LEN = struct.Struct(">I")


def _flexbuffers():
    """Import ``flatbuffers.flexbuffers`` lazily with a helpful error."""
    try:
        from flatbuffers import flexbuffers
    except ImportError as exc:  # pragma: no cover - exercised only without extra
        raise ImportError(
            "The swe+flatbuffers codec requires the optional 'flatbuffers' "
            "dependency. Install it with `pip install oshconnect[flatbuffers]`."
        ) from exc
    return flexbuffers


class SWEFlatBuffersCodec:
    """FlexBuffers codec for ``application/swe+flatbuffers`` observations.

    :meth:`encode` produces a length-prefixed FlexBuffers frame from a
    mapping; :meth:`decode` reverses it, returning a ``dict`` keyed by SWE
    field name. FlexBuffers is schemaless, so the ``schema`` is retained
    only for API symmetry with the other codecs and is not required to
    decode.
    """

    def __init__(
        self,
        schema: Union[SWEFlatBuffersDatastreamRecordSchema, AnyComponentSchema],
    ):
        if not isinstance(schema, (SWEFlatBuffersDatastreamRecordSchema, AnyComponentSchema)):
            raise TypeError(
                "SWEFlatBuffersCodec expects an "
                "SWEFlatBuffersDatastreamRecordSchema or AnyComponent schema, "
                f"got {type(schema).__name__}.")
        self._schema = schema

    def encode(self, value: Any) -> bytes:
        """Encode *value* (a mapping/record) as a length-prefixed FlexBuffers frame."""
        doc = _flexbuffers().Dumps(value)
        return _FRAME_LEN.pack(len(doc)) + doc

    def decode(self, buf: bytes) -> Any:
        """Decode one FlexBuffers observation frame into a ``dict``.

        Accepts either a length-prefixed frame (as published over NATS/MQTT)
        or a bare FlexBuffers document; the 4-byte prefix is stripped when
        present.
        """
        return _flexbuffers().Loads(self._strip_frame(buf))

    @staticmethod
    def _strip_frame(buf: bytes) -> bytes:
        """Strip the 4-byte length prefix when the frame length matches."""
        data = bytes(buf)
        if len(data) >= 4:
            declared = _FRAME_LEN.unpack(data[:4])[0]
            if declared == len(data) - 4:
                return data[4:]
        return data
