OSHConnect-Python Tutorial
==========================
OSHConnect-Python is a library for interacting with OpenSensorHub using OGC API Connected Systems.
This tutorial walks through the most common workflows.

Installation
------------
Install using ``uv`` (recommended):

.. code-block:: bash

   uv add git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git

Or with ``pip``:

.. code-block:: bash

   pip install git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git

All public classes and utilities can be imported directly from ``oshconnect``:

.. code-block:: python

   from oshconnect import OSHConnect, Node, System, Datastream, ControlStream
   from oshconnect import TimePeriod, TimeInstant, TemporalModes
   from oshconnect import DataRecordSchema, QuantitySchema, TimeSchema, TextSchema
   from oshconnect import ObservationFormat, DefaultEventTypes


Creating an OSHConnect Instance
--------------------------------
The main entry point is the ``OSHConnect`` class:

.. code-block:: python

   from oshconnect import OSHConnect, TemporalModes

   app = OSHConnect(name='MyApp')


Adding a Node
-------------
A ``Node`` represents a connection to a single OSH server.
The ``OSHConnect`` instance can manage multiple nodes simultaneously.

.. code-block:: python

   from oshconnect import OSHConnect, Node

   app = OSHConnect(name='MyApp')
   node = Node(protocol='http', address='localhost', port=8585,
               username='test', password='test')
   app.add_node(node)

To connect a node with MQTT support for streaming:

.. code-block:: python

   node = Node(protocol='http', address='localhost', port=8585,
               username='test', password='test',
               enable_mqtt=True, mqtt_port=1883)
   app.add_node(node)


Authentication
--------------
OSHConnect speaks **HTTP Basic Auth** to OGC CS API servers. There is no
bearer-token, OAuth, or API-key flow — the underlying ``requests``
library carries credentials as a ``(username, password)`` tuple.

For a secured server, pass ``username`` and ``password`` to ``Node``:

.. code-block:: python

   node = Node(protocol='https', address='sensors.example.org', port=443,
               username='alice', password='s3cret')

Every HTTP call the node makes — discovery, resource creation, schema
fetches — automatically carries those credentials. Internally, the node
constructs an ``APIHelper`` that holds the credentials and reads them
back via ``get_helper_auth()`` on each request. The same credentials
also flow into the MQTT client when ``enable_mqtt=True``.

For an unsecured server (e.g., a local OSH dev instance), simply omit
``username`` and ``password``:

.. code-block:: python

   node = Node(protocol='http', address='localhost', port=8585)

If the server has been secured but you forget to provide credentials,
each request will return ``401 Unauthorized`` from the server — no
exception is raised by the library; inspect the response status.

Lower-level usage (free helpers)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For one-off scripts or when you don't want a full ``Node`` /
``OSHConnect`` setup, the module-level helpers in
``oshconnect.api_helpers`` mirror each CS API endpoint and accept an
optional ``auth`` tuple plus optional ``headers`` dict. Every helper
returns a ``requests.Response`` object:

.. code-block:: python

   from oshconnect.api_helpers import list_all_systems, create_new_systems

   resp = list_all_systems(
       'http://sensors.example.org/sensorhub',
       auth=('alice', 's3cret'),
   )
   resp.raise_for_status()
   systems = resp.json()['items']

   created = create_new_systems(
       'http://sensors.example.org/sensorhub',
       request_body={'name': 'Sensor #1', 'uid': 'urn:test:sensor:1'},
       auth=('alice', 's3cret'),
       headers={'Content-Type': 'application/sml+json'},
   )
   new_id = created.headers['Location'].rsplit('/', 1)[-1]

Omit ``auth`` to call an unsecured endpoint. For application code,
prefer the ``Node`` / ``APIHelper`` path so credentials are configured
once at the node boundary instead of threaded through every call site.


Discovery
---------

Discover all systems available on all registered nodes:

.. code-block:: python

   app.discover_systems()

Discover all datastreams across all discovered systems:

.. code-block:: python

   app.discover_datastreams()

Each discovered ``Datastream`` arrives with its SWE+JSON record schema
already cached on ``ds._underlying_resource.record_schema`` — discovery
makes a follow-up ``GET /datastreams/{id}/schema`` per stream so callers
that build observations don't need a second round trip.

Discover control streams the same way, per system:

.. code-block:: python

   for system in node.get_systems():
       control_streams = system.discover_controlstreams()
       for cs in control_streams:
           print(cs.get_id(), cs._underlying_resource.input_name)

Discovered control streams arrive with their command schema cached on
``cs._underlying_resource.command_schema`` (a ``JSONCommandSchema`` —
OSH normalizes responses to the JSON envelope). Reach the inner SWE
Common component via ``cs._underlying_resource.command_schema.params_schema``;
its ``items`` (for ``DataChoice``) or ``fields`` (for ``DataRecord``)
list the parameters the stream accepts.


Streaming Observations (MQTT)
------------------------------
Once a node is configured with MQTT and datastreams are discovered, start receiving
observations by initializing and starting each datastream:

.. code-block:: python

   from oshconnect import StreamableModes

   for ds in app.get_datastreams():
       ds.set_connection_mode(StreamableModes.PULL)
       ds.initialize()
       ds.start()

Incoming messages are appended to each datastream's inbound deque:

.. code-block:: python

   import time

   time.sleep(2)  # allow messages to arrive
   for ds in app.get_datastreams():
       while ds.get_inbound_deque():
           msg = ds.get_inbound_deque().popleft()
           print(msg)

Resource Event Subscriptions
-----------------------------
Subscribe to resource lifecycle events (create/update/delete) using
``subscribe_events()``. These arrive as CloudEvents v1.0 JSON payloads:

.. code-block:: python

   def on_event(client, userdata, msg):
       print(f"Event on {msg.topic}: {msg.payload}")

   for ds in app.get_datastreams():
       topic = ds.subscribe_events(callback=on_event)
       print(f"Subscribed to event topic: {topic}")


Inserting a New System
-----------------------

.. code-block:: python

   from oshconnect import OSHConnect, Node

   app = OSHConnect(name='MyApp')
   node = Node(protocol='http', address='localhost', port=8585,
               username='admin', password='admin')
   app.add_node(node)

   new_system = app.create_and_insert_system(
       system_opts={
           'name': 'Test System',
           'description': 'A test system',
           'uid': 'urn:system:test:001',
       },
       target_node=node
   )


Inserting a New Datastream
--------------------------
Build a schema using SWE Common component classes, then attach it to a system:

.. code-block:: python

   from oshconnect import DataRecordSchema, TimeSchema, QuantitySchema, TextSchema
   from oshconnect.api_utils import URI, UCUMCode

   datarecord = DataRecordSchema(
       label='Example Record',
       description='Example datastream record',
       definition='http://example.org/records/example',
       fields=[]
   )

   # TimeSchema must be the first field for OSH
   datarecord.fields.append(
       TimeSchema(label='Timestamp', definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                  name='timestamp', uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'))
   )
   datarecord.fields.append(
       QuantitySchema(name='distance', label='Distance', definition='http://example.org/Distance',
                      uom=UCUMCode(code='m', label='meters'))
   )
   datarecord.fields.append(
       TextSchema(name='label', label='Label', definition='http://example.org/Label')
   )

   datastream = new_system.add_insert_datastream(datarecord)

.. note::

   A ``TimeSchema`` must be the first field in the ``DataRecordSchema`` when targeting OpenSensorHub.


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
messages serialized against the SWE Common 3 schemas in the
`BinaryEncodings project <https://github.com/tipatterson-dev/BinaryEncodings>`_.
``application/swe+flatbuffers`` is the FlatBuffers analogue.

Why a separate encoding family from SWE+Binary?

* **SWE+Binary** is a packed wire format for known-shape records (declared
  per-field by `BinaryEncoding.members`). It's compact and demands no
  schema-side runtime; it's also rigid — fields must be fixed-width or
  size-prefixed blocks.
* **SWE+Protobuf** is self-describing tag-length-value bytes interpreted
  through a code-generated schema (the ``sweCommon3_pb2`` module). It
  handles nested records, choice variants, variable-length lists, and
  field evolution naturally. The trade-off is the runtime dependency on
  the generated bindings and slightly larger wire size for trivial records.

Install requirements
~~~~~~~~~~~~~~~~~~~~

Install the optional extra and generate the bindings from BinaryEncodings:

.. code-block:: bash

   pip install "oshconnect[protobuf]"
   git clone https://github.com/tipatterson-dev/BinaryEncodings
   cd BinaryEncodings && make protobuf PROTO_LANG=python
   export PYTHONPATH="$PWD/gen/protobuf:$PYTHONPATH"

The bindings are looked up via the standard Python import path — the
codec imports ``sweCommon3_pb2`` lazily on first use and raises a
descriptive ``ImportError`` (including the install hint) if they're
not available.

Encoding and decoding observations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Datastream.insert(...)`` and ``decode_observation(...)`` dispatch on
the schema's ``obs_format`` exactly as they do for SWE+Binary:

.. code-block:: python

   from oshconnect import (
       DataRecordSchema, TimeSchema, QuantitySchema, CountSchema,
       BooleanSchema, TextSchema,
       SWEProtobufDatastreamRecordSchema,
   )
   from oshconnect.api_utils import URI, UCUMCode

   record = DataRecordSchema(
       name='weather', label='Weather',
       definition='http://example.org/weather',
       fields=[
           TimeSchema(name='time', label='Time',
                      definition='http://www.opengis.net/def/property/OGC/0/SamplingTime',
                      uom=URI(href='http://www.opengis.net/def/uom/ISO-8601/0/Gregorian')),
           QuantitySchema(name='temp', label='Temperature',
                          definition='http://example.org/temp',
                          uom=UCUMCode(code='Cel', label='Celsius')),
       ],
   )
   schema = SWEProtobufDatastreamRecordSchema(record_schema=record)
   ds_resource.record_schema = schema   # attach to a DatastreamResource
   # Now `Datastream.insert({...})` packs values via SWEProtobufCodec
   # and `Datastream.decode_observation(raw)` reverses it.

Supported SWE Common 3 component types: ``Boolean``, ``Count``,
``Quantity``, ``Time``, ``Category``, ``Text``, ``DataRecord``
(including nested), ``Vector``, ``DataChoice``, and ``DataArray``
of scalar element types (Quantity, Count, Boolean, Time).

DataArray wire format mirrors the OpenSensorHub reference
implementation (``BinaryDataWriter`` in
``lib-ogc/swe-common-core``): element values are packed tightly
back-to-back as SWE BinaryEncoding bytes (via
``oshconnect.swe_binary.encode_swe_binary_scalar_array``) and stuffed
in ``EncodedValues.inline_data``; the accompanying
``encoding.binary_encoding`` carries the dataType URI so the wire is
self-describing. Decoders can therefore read messages produced by any
SWE Common 3 implementation without needing the Python-side schema.

``Matrix``, ``Geometry``, the ``*Range`` variants, and arrays of
records/vectors are not yet wired through the codec — using them
raises ``TypeError`` so the gap is explicit; extension is
straightforward via the dispatch table in ``oshconnect.swe_protobuf``.

FlatBuffers status
~~~~~~~~~~~~~~~~~~

``application/swe+flatbuffers`` is wired through the same machinery
(`SWEFlatBuffersDatastreamRecordSchema` parses cleanly, the format
picker advertises the obsFormat, and ``Datastream.insert`` /
``decode_observation`` route to ``SWEFlatBuffersCodec``), but the codec
itself raises ``NotImplementedError`` until the FlatBuffers compiler
adds Python support for vectors of unions. See
``docs/osh_spec_deviations.md`` (``flatc-python-vector-of-union``).


Inserting a New Control Stream
------------------------------
A control stream is the input counterpart to a datastream — it accepts
commands and emits status reports. Build a ``DataRecordSchema``
describing the command structure, then attach it to a system via
``System.add_and_insert_control_stream(...)``:

.. code-block:: python

   from oshconnect import DataRecordSchema, BooleanSchema, CountSchema

   command_record = DataRecordSchema(
       name='counterControl',
       label='Counter Control',
       description='Commands to control the counter behavior',
       fields=[
           BooleanSchema(name='setCountDown', label='Set Count Down',
                         definition='http://sensorml.com/ont/swe/property/SetCountDown'),
           CountSchema(name='setStep', label='Set Step',
                       definition='http://sensorml.com/ont/swe/property/SetStep'),
       ],
   )

   control_stream = new_system.add_and_insert_control_stream(command_record)

The default wire form is ``application/json`` —
``commandFormat: "application/json"`` with a ``parametersSchema`` block
(no ``encoding``). It matches what OSH echoes back from
``GET /controlstreams/{id}/schema?f=json``, which is the form
``discover_controlstreams`` parses, so cross-node sync round-trips
without any format conversion. It also sidesteps the SWE+JSON
``encoding``-omission deviation documented in
``docs/osh_spec_deviations.md`` §1.

For the spec-canonical SWE+JSON form (``recordSchema`` plus a
``JSONEncoding`` block), pass ``command_format='application/swe+json'``:

.. code-block:: python

   control_stream = new_system.add_and_insert_control_stream(
       command_record,
       command_format='application/swe+json',
   )

For full control over the resource body — for example, when copying a
control stream from one node to another and you already have a
``ControlStreamResource`` in hand — use ``add_insert_controlstream(...)``
instead. It takes a fully-built resource and POSTs it as-is. Build the
embedded ``command_schema`` as a ``JSONCommandSchema`` for the
recommended JSON form:

.. code-block:: python

   from oshconnect.resource_datamodels import ControlStreamResource
   from oshconnect.schema_datamodels import JSONCommandSchema

   resource = ControlStreamResource(
       name='Counter Control',
       input_name='counterControl',
       command_schema=JSONCommandSchema(
           command_format='application/json',
           params_schema=command_record,
       ),
   )
   control_stream = new_system.add_insert_controlstream(resource)

After insert, the returned ``ControlStream`` carries the server-assigned
ID (``control_stream.get_id()``) and is appended to ``new_system.control_channels``.


Sending Commands
----------------
A control stream is the input side of a system. Once you have one — either
freshly inserted or reconstructed from ``System.discover_controlstreams()`` —
there are two ways to deliver a command:

**Over MQTT (preferred for real-time control).** Initialize the stream's
MQTT client, then publish to the command topic:

.. code-block:: python

   from oshconnect import StreamableModes

   control_stream.set_connection_mode(StreamableModes.BIDIRECTIONAL)
   control_stream.initialize()
   control_stream.start()

   control_stream.publish_command({
       'params': {'setStep': 5},
   })

``publish_command(payload)`` is sugar for ``publish(payload, topic='command')``;
it routes to the CS API Part 3 ``:commands`` topic for this stream
(``…/controlstreams/{id}/commands``). The payload shape is whatever the
control stream's command schema accepts — a dict matching the field names
under ``params``, or a SWE+JSON envelope if the stream uses the SWE form.

**Over HTTP (stateless, one-shot).** POST a command directly to the
``/controlstreams/{id}/commands`` endpoint via the node's
``APIHelper``:

.. code-block:: python

   from oshconnect.csapi4py.constants import APIResourceTypes
   from oshconnect.schema_datamodels import CommandJSON

   command = CommandJSON(params={'setStep': 5})
   api = node.get_api_helper()
   resp = api.create_resource(
       APIResourceTypes.COMMAND,
       command.to_csapi_dict(),
       parent_res_id=control_stream.get_id(),
       req_headers={'Content-Type': 'application/json'},
   )
   resp.raise_for_status()
   command_id = resp.headers['Location'].rsplit('/', 1)[-1]

The server responds with ``201 Created`` and a ``Location`` header pointing
at the newly-created command resource (``/commands/{id}``); poll its
``/status`` sub-resource (or subscribe to the MQTT status topic — next
section) to see whether the system accepted and executed it.

Subscribing to Command Status
-----------------------------
Control streams emit two MQTT topics: ``:commands`` (input) and ``:status``
(output, where the system reports execution results). Subscribe to status
updates:

.. code-block:: python

   def on_status(client, userdata, msg):
       print(f"Status on {msg.topic}: {msg.payload}")

   control_stream.subscribe(topic='status', callback=on_status)

Inbound status reports are also pushed onto an internal deque — drain it
exactly like a datastream's inbound queue:

.. code-block:: python

   while control_stream.get_status_deque_inbound():
       status = control_stream.get_status_deque_inbound().popleft()
       print(status)


Inserting an Observation
------------------------
Once a datastream is registered, send observation data using ``insert_observation_dict()``:

.. code-block:: python

   from oshconnect import TimeInstant

   datastream.insert_observation_dict({
       'resultTime': TimeInstant.now_as_time_instant().get_iso_time(),
       'phenomenonTime': TimeInstant.now_as_time_instant().get_iso_time(),
       'result': {
           'timestamp': TimeInstant.now_as_time_instant().epoch_time,
           'distance': 1.0,
           'label': 'example observation',
       }
   })

.. note::

   The keys in ``result`` correspond to the ``name`` fields of each schema component.
   ``resultTime`` and ``phenomenonTime`` are required by OpenSensorHub.


Saving and Loading Configuration
---------------------------------
The OSHConnect state (nodes, systems, datastreams) can be persisted to a JSON file:

.. code-block:: python

   app.save_config()          # saves to a default file
   app = OSHConnect.load_config('my_config.json')