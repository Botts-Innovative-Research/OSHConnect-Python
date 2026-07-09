Consuming Sensor Data
=====================

Everything on the read side: finding systems and datastreams on a
node, streaming observations in real time, and subscribing to
resource lifecycle events.

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


MQTT Topic Conventions
----------------------
OSHConnect speaks the CS API Part 3 pub/sub conventions, including the
optional **format subtopic** that selects the wire format for each
Resource Data Topic. A subscription path looks like::

   {mqtt_root}/datastreams/{ds_id}/observations:data/<format-token>
   {mqtt_root}/controlstreams/{cs_id}/commands:data/<format-token>
   {mqtt_root}/controlstreams/{cs_id}/status:data/json

The trailing ``<format-token>`` is the hyphen-substituted MIME subtype
(``+`` is reserved as an MQTT wildcard and is disallowed in Kafka topic
names, so the server uses ``-`` instead):

============================  ======================
Content-type                  Topic token
============================  ======================
``application/json``          ``json``
``application/swe+json``      ``swe-json``
``application/swe+binary``    ``swe-binary``
``application/swe+csv``       ``swe-csv``
``application/om+json``       ``om-json``
``application/sml+json``      ``sml-json``
============================  ======================

The Python client picks the right token for you. ``Datastream.init_mqtt``
reads the discovered ``record_schema.obs_format`` (e.g.
``application/swe+binary`` for video datastreams) and appends
``/swe-binary`` to the data topic. ``ControlStream.init_mqtt`` does the
same with ``command_schema.command_format``, and the status topic is
always suffixed with ``/json`` since status payloads are JSON by
convention. If you build a topic manually via
``APIHelper.get_mqtt_topic`` you can pass ``format=...`` explicitly; an
unknown MIME type raises ``ValueError`` from
``oshconnect.csapi4py.mqtt.mqtt_topic_format_token`` so the client never
sends a token the server can't parse.

Older servers that don't recognise the format subtopic still accept the
bare ``:data`` form — that's what ``init_mqtt`` produces when a
datastream has no fetched schema (the server's default format applies).


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
