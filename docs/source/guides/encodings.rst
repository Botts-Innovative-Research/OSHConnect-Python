Binary and Compact Encodings
============================

Datastreams whose payloads do not fit a JSON envelope: SWE+Binary
(e.g. H.264 video), swe+proto (Protocol Buffers), and
swe+flatbuffers (FlexBuffers).

Working with SWE+Binary Datastreams
-----------------------------------
Some datastreams ship payloads that don't fit a JSON envelope — H.264 video
frames, JPEG snapshots, dense fixed-width records. For these the OGC CS API
defines ``application/swe+binary``: each observation is a packed byte
sequence whose layout is described by the datastream's ``recordEncoding``
(a SWE Common ``BinaryEncoding``).

OSHConnect parses these schemas automatically. When you call
``System.discover_datastreams()``, the SDK picks the schema obsFormat from
each datastream's advertised ``formats``:

* ``application/swe+json`` if available (parsed as
  ``SWEDatastreamRecordSchema``)
* otherwise ``application/swe+binary`` (parsed as
  ``SWEBinaryDatastreamRecordSchema``)

Decoding observations
~~~~~~~~~~~~~~~~~~~~~

For an existing binary datastream, ``Datastream.decode_observation(raw)``
returns a dict keyed by field name. Block members (e.g. an H.264 frame)
come back as ``bytes`` — the SDK does not demux video codecs.

.. code-block:: python

   # Assume `ds` is a Datastream whose schema is application/swe+binary,
   # e.g. an Axis camera's `video1` output.
   raw = bytes(ds._inbound_deque.popleft())   # one MQTT message
   record = ds.decode_observation(raw)
   ts = record['time']                        # float — Unix epoch s
   nal = record['img']                        # bytes — opaque H.264 NAL unit

Publishing binary observations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For a binary datastream, ``Datastream.insert(...)`` dispatches through
``SWEBinaryCodec``, so you pass a dict keyed by field name (or a positional
sequence in declared member order) and the SDK packs it for you:

.. code-block:: python

   # Pan/tilt record (fixed-width: [ts: double][f32][f32][f32])
   ds.insert({'time': time.time(),
              'pan': -6.7, 'tilt': 0.0, 'zoomFactor': 1.0})

   # Video frame (variable-size block: [ts: double][size: uint32][N bytes])
   nal_bytes = grab_h264_nal_unit()           # your codec, opaque to OSHConnect
   ds.insert({'time': time.time(), 'img': nal_bytes})

You can also bypass the codec entirely by passing pre-encoded ``bytes`` —
useful when another component has already framed the record:

.. code-block:: python

   from oshconnect.swe_binary import encode_swe_binary_blob
   pre_framed = encode_swe_binary_blob(nal_bytes)
   ds.insert(pre_framed)                       # passes through unchanged

Building a binary datastream from scratch
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When registering a new binary datastream against an OSH node, build the
schema with ``SWEBinaryDatastreamRecordSchema`` and a ``BinaryEncoding``
whose ``members`` list maps each record field to a wire shape:

.. code-block:: python

   from oshconnect import DataRecordSchema, TimeSchema, QuantitySchema
   from oshconnect.api_utils import URI, UCUMCode
   from oshconnect.encoding import (
       BinaryComponentMember, BinaryEncoding,
   )
   from oshconnect.schema_datamodels import SWEBinaryDatastreamRecordSchema

   record = DataRecordSchema(
       name='ptz', label='PTZ Snapshot',
       definition='http://example.org/ptz',
       fields=[
           TimeSchema(name='time', label='Timestamp',
                      definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                      uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
           QuantitySchema(name='pan', label='Pan',
                          definition='http://example.org/pan',
                          uom=UCUMCode(code='deg', label='degrees')),
       ],
   )
   encoding = BinaryEncoding(
       byte_order='bigEndian', byte_encoding='raw',
       members=[
           BinaryComponentMember(
               ref='/time',
               data_type='http://www.opengis.net/def/dataType/OGC/0/double'),
           BinaryComponentMember(
               ref='/pan',
               data_type='http://www.opengis.net/def/dataType/OGC/0/float32'),
       ],
   )
   schema = SWEBinaryDatastreamRecordSchema(
       obs_format='application/swe+binary',
       record_schema=record,
       record_encoding=encoding,
   )

Block payloads (H.264, JPEG, etc.) are declared with
``BinaryBlockMember``; the ``compression`` attribute is metadata for
downstream consumers and is **not** acted on by the codec.


Working with SWE+Protobuf and SWE+FlatBuffers Datastreams
---------------------------------------------------------
``application/swe+proto`` ships observations as Protocol Buffers
messages encoded against a **per-datastream descriptor**. Each
DataStream carries a pre-compiled Protobuf schema — a serialized
``google.protobuf.FileDescriptorSet`` describing one
``Observation_<dsId>``-shaped message — and every observation is a
serialized instance of that message. ``application/swe+flatbuffers`` is
the FlatBuffers analogue.

The per-datastream message has a fixed envelope at fields 1–5
(``id``, ``datastream_id``, ``foi_id``, ``phenomenon_time``,
``result_time`` — the two times are ``google.protobuf.Timestamp``)
followed by the SWE Common record at fields 6+, one flat field per
component. SWE semantics (definition, label, unit) travel as field
options inside the descriptor.

Why a separate encoding family from SWE+Binary?

* **SWE+Binary** is a packed wire format for known-shape records (declared
  per-field by `BinaryEncoding.members`). It's compact and demands no
  schema-side runtime; it's also rigid — fields must be fixed-width or
  size-prefixed blocks.
* **SWE+Protobuf** is a self-describing tag-length-value stream whose
  layout comes from the delivered descriptor. The receiver registers the
  ``FileDescriptorSet`` in a ``DescriptorPool`` and builds the message
  class dynamically — no ``protoc`` and no code-generated bindings.

Install requirements
~~~~~~~~~~~~~~~~~~~~

Install the optional extra — only the ``protobuf`` runtime is needed
(the per-datastream message is built dynamically from the descriptor, so
no generated SWE Common bindings are required):

.. code-block:: bash

   pip install "oshconnect[protobuf]"

The codec imports the ``protobuf`` runtime lazily on first use and
raises a descriptive ``ImportError`` (including the install hint) if the
extra is not installed.

Encoding and decoding observations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Datastream.insert(...)`` and ``decode_observation(...)`` dispatch on
the schema's ``obs_format`` exactly as they do for SWE+Binary. The
schema carries the descriptor; during discovery it is fetched from
``/datastreams/{id}/schema`` and parsed into a
`SWEProtobufDatastreamRecordSchema`:

.. code-block:: python

   from oshconnect import SWEProtobufDatastreamRecordSchema, SWEProtobufCodec

   # `fds_bytes` is a serialized google.protobuf.FileDescriptorSet for the
   # per-datastream observation message (delivered by the OSH node, or
   # compiled from a .proto with `protoc --include_imports
   # --descriptor_set_out`).
   schema = SWEProtobufDatastreamRecordSchema(
       file_descriptor_set=fds_bytes,
       message_type="org.example.WeatherObservation",  # optional if the set has one message
   )
   ds_resource.record_schema = schema   # attach to a DatastreamResource
   # Now `Datastream.insert({...})` packs the result record via
   # SWEProtobufCodec (stamping datastream_id + result_time into the
   # envelope) and `Datastream.decode_observation(raw)` returns the
   # result record dict.

``insert`` / ``decode_observation`` operate on the **result record**
(fields 6+), keyed by field name — the same dict shape the SWE+Binary
codec uses and what lands in ``ObservationResource.result``. The
envelope metadata (ids and the two timestamps) is supplied by the
``Datastream`` on encode and recoverable on decode via
``SWEProtobufCodec.decode_with_envelope(raw)``.

Supported result-field types: Protobuf scalars (numbers, ``bool``,
``string``, ``bytes``), ``google.protobuf.Timestamp`` (decoded to an ISO
8601 string), and **nested records and vectors** — these recurse into
nested dicts (``{"location": {"lat": …, "lon": …}}``); a vector may be
given as a sequence on encode and comes back as a dict keyed by
coordinate name. ``DataArray`` (repeated) fields are not yet supported
and raise ``NotImplementedError``.

Generating a swe+proto schema (create side)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When *producing* a datastream you usually have the record structure in
SWE Common already (the ``record_schema`` your SWE+JSON or SWE+Binary
datastream carries). Translate it into a swe+proto schema — the
descriptor is generated for you (the inverse of OSH's
``ProtoSchemaWriter``: envelope fields 1–5 plus the record's components
mapped to flat result fields 6+):

.. code-block:: python

   from oshconnect import SWEProtobufDatastreamRecordSchema

   # From a SWE Common DataRecord directly...
   proto_schema = SWEProtobufDatastreamRecordSchema.from_record_schema(
       record, message_name=f"Observation_{ds_id}")

   # ...or from another datastream schema you already hold (its semantic
   # record_schema is reused, so SWE+JSON / SWE+CSV / SWE+binary all map
   # to the same descriptor):
   proto_schema = SWEProtobufDatastreamRecordSchema.from_other_schema(swe_binary_schema)

Editing the schema as ``.proto`` text
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The generated schema is a binary descriptor, but you can translate it to
editable ``.proto`` **source text** — inspect it, hand-tweak it (rename
or add fields, change types, add annotations), then compile it back:

.. code-block:: python

   schema = SWEProtobufDatastreamRecordSchema.from_record_schema(record)

   # Render the carried descriptor as .proto source (no protoc needed).
   # Works for node-delivered schemas too — it renders whatever descriptor
   # the schema holds.
   text = schema.to_proto_source()

   # ... edit `text` as needed ...

   # Recompile the edited source back into a schema (requires protoc on PATH;
   # the from_record_schema path itself needs no protoc).
   edited = SWEProtobufDatastreamRecordSchema.from_proto_source(text)

``to_proto_source`` is a faithful rendering of the descriptor (not a
second generator), so the text and the binary descriptor never drift.
``from_proto_source`` shells out to ``protoc`` and defaults the message
type to the first message in the compiled file.

Component → field type mapping: Quantity → ``double``, Count →
``int32``, Boolean → ``bool``, Time → ``google.protobuf.Timestamp``
(ISO) or ``double`` (numeric), Text → ``string``. A **Category** maps to
``string`` when unconstrained, or to a proto **enum** (``Enum_<field>``,
tokens numbered from 0 — matching OSH's convention) when it carries an
``AllowedTokens`` constraint; encode accepts the token string and decode
returns it. The component's OGC ``dataType`` selects the numeric width
(float32 → ``float``, signedLong → ``int64``, …) when known. **Nested
records and vectors** become nested message types (``Rec<N>`` /
``Vec<N>``, inner fields numbered from 1), recursed to arbitrary depth. A
**DataArray** becomes the node's ``Array<N> { repeated <elt> = 1 }``
wrapper (the element may be a scalar, record, vector, or constrained
category), round-tripping as ``{array_name: {element_name: [...]}}``.
Component names that aren't valid proto identifiers (e.g. SWE NameToken
hyphens) are sanitized to underscores; enum tokens must already be valid
identifiers (as the node requires). The remaining composites
(``DataChoice``, ranges, geometry, matrices) are not yet translatable and
raise ``NotImplementedError``.

.. note::

   The JSON envelope that delivers the descriptor over the schema
   endpoint is a contract still being finalized with the OSH node side;
   OSHConnect currently assumes
   ``{"obsFormat", "messageType", "fileDescriptorSet": <base64>}``.

FlatBuffers status
~~~~~~~~~~~~~~~~~~

``application/swe+flatbuffers`` is fully supported and wired through
the same machinery: `SWEFlatBuffersDatastreamRecordSchema` parses the
schema, the format picker advertises the obsFormat, and
``Datastream.insert`` / ``decode_observation`` route to
``SWEFlatBuffersCodec``. On the wire, OSH encodes each observation as
a length-prefixed **schemaless FlexBuffers** document (a 4-byte
big-endian length followed by one FlexBuffers buffer) — not
schema-compiled FlatBuffers — so decoding needs no generated bindings
and results come back as plain dicts keyed by the SWE field names.
The codec requires the optional ``flatbuffers`` extra
(``pip install "oshconnect[flatbuffers]"``).
