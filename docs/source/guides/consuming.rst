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
Resource Data Topic. Datastream and control stream topics are nested under
their system. A subscription path looks like::

   {mqtt_root}/systems/{sys_id}/datastreams/{ds_id}/observations:data/<format-token>
   {mqtt_root}/systems/{sys_id}/controlstreams/{cs_id}/commands:data/<format-token>
   {mqtt_root}/systems/{sys_id}/controlstreams/{cs_id}/commands/{cmd_id}/status:data

``{mqtt_root}`` is the server's ``nodeId`` (default ``api``); set it with
``Node(mqtt_topic_root=...)``. The system id comes from discovery: streams
found through ``System.discover_datastreams`` / ``discover_controlstreams``
already know their parent. A stream built by hand needs
``set_parent_resource_id(system_id)`` before ``initialize()``, otherwise
topic construction raises ``ValueError``.

Servers that use other topic forms are selected with
``Node(mqtt_topic_scheme=...)`` — see :doc:`connecting`.

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
``application/sml+xml``       ``sml-xml``
``application/geo+json``      ``geo-json``
``application/swe+xml``       ``swe-xml``
============================  ======================

Any other type, such as a custom format a node lists in a stream's
``formats``, gets its token by the same rule: drop the parameters and the
leading ``application/``, replace ``+`` with ``-``, lowercase
(``application/x-custom+json`` → ``x-custom-json``). A type whose name would
still contain ``/`` (``text/csv``) has no subtopic.

The Python client picks the right token for you. ``Datastream.init_mqtt``
reads the discovered ``record_schema.obs_format`` (e.g.
``application/swe+binary`` for video datastreams) and appends
``/swe-binary`` to the data topic. ``ControlStream.init_mqtt`` does the
same with ``command_schema.command_format``. The status topic has no
format subtopic (status is JSON). If you build a topic manually via
``APIHelper.get_mqtt_topic`` you can pass ``format=...`` explicitly; an
MIME type with no valid token raises ``ValueError`` from
``oshconnect.csapi4py.mqtt.mqtt_topic_format_token``. The server rejects a
token for a format the stream does not offer.

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
