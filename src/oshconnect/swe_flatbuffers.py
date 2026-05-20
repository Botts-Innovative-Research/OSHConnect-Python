#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/19
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Runtime codec for the ``application/swe+flatbuffers`` wire format.

**Status: blocked on an upstream FlatBuffers compiler limitation.**

The SWE Common 3 FlatBuffers schemas (in the BinaryEncodings project)
declare ``BinaryEncoding.members`` as ``[BinaryMember]`` where
``BinaryMember`` is a union of ``BinaryComponent`` and ``BinaryBlock``.
``flatc --python`` rejects this with::

    error: Vectors of unions are not yet supported in at least one of
    the specified programming languages.

Until ``flatc`` adds Python support for vector-of-union, we cannot
generate the SWE Common 3 Python bindings for FlatBuffers, and this
codec cannot do anything useful at runtime. The
`SWEFlatBuffersCodec` class is provided as a placeholder so the rest
of the SDK can already register, parse, and round-trip schemas that
name ``application/swe+flatbuffers`` — only the encode/decode
endpoints raise.

See ``docs/osh_spec_deviations.md`` (``flatc-python-vector-of-union``)
and track upstream progress at https://github.com/google/flatbuffers.
"""

from __future__ import annotations

from typing import Any, Union

from .schema_datamodels import SWEFlatBuffersDatastreamRecordSchema
from .swe_components import AnyComponentSchema


_BLOCKED_MESSAGE = (
    "SWEFlatBuffersCodec is currently blocked on a `flatc --python` "
    "limitation: vectors of unions are not yet supported, and the SWE "
    "Common 3 BinaryEncoding schema uses one. The schema class is "
    "kept registered so the SDK can round-trip schemas naming this "
    "format, but encode/decode cannot be implemented until the "
    "FlatBuffers compiler grows the missing feature. See "
    "docs/osh_spec_deviations.md (flatc-python-vector-of-union)."
)


class SWEFlatBuffersCodec:
    """Placeholder for the FlatBuffers SWE codec.

    Constructed normally so callers don't have to special-case schema
    registration — but :meth:`encode` and :meth:`decode` raise
    ``NotImplementedError`` until the upstream toolchain limitation is
    lifted.
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

    def encode(self, _value: Any) -> bytes:
        raise NotImplementedError(_BLOCKED_MESSAGE)

    def decode(self, _buf: bytes) -> Any:
        raise NotImplementedError(_BLOCKED_MESSAGE)
