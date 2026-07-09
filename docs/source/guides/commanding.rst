Commanding Systems
==================

Tasking a system: create a control stream (the input counterpart to
a datastream), send commands to it, and track their execution status.
The examples build on a ``System`` created as shown in
:doc:`publishing`.

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
without any format conversion. It also sidesteps a known OSH server
quirk where the ``encoding`` block is omitted from SWE+JSON
control-stream schemas.

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
Each control stream exposes two MQTT topics: ``/commands:data/<format>``
(input — the operator publishes here) and ``/status:data/json`` (output —
the system reports execution results here). See the format-token table
in :doc:`consuming`. Subscribe to status updates:

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
