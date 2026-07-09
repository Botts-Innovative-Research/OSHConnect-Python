Connecting to a Node
====================

How to construct the ``OSHConnect`` entry point, attach ``Node``
connections (including the MQTT and NATS streaming transports), and
authenticate against secured servers.

Creating an OSHConnect Instance
--------------------------------
All public classes and utilities can be imported directly from
``oshconnect``:

.. code-block:: python

   from oshconnect import OSHConnect, Node, System, Datastream, ControlStream
   from oshconnect import TimePeriod, TimeInstant, TemporalModes
   from oshconnect import DataRecordSchema, QuantitySchema, TimeSchema, TextSchema
   from oshconnect import ObservationFormat, DefaultEventTypes

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

To talk to an **older OSH server** that predates the CS API Part 3 topic
scheme, enable legacy topics. This reverts MQTT topic construction to the
pre-Part-3 form — a leading slash and no ``:data`` suffix or format
subtopic (e.g. ``/api/datastreams/{id}/observations`` instead of
``api/datastreams/{id}/observations:data/<token>``):

.. code-block:: python

   node = Node(protocol='http', address='localhost', port=8585,
               username='test', password='test',
               enable_mqtt=True, mqtt_legacy_topics=True)
   app.add_node(node)

Legacy mode affects MQTT topics only; NATS subjects are unaffected.

To stream over **NATS.io** instead (the corporate-bus transport, served by
OSH's ``sensorhub-service-consys-nats`` binding), enable it the same way —
``enable_nats`` mirrors ``enable_mqtt``:

.. code-block:: python

   node = Node(protocol='http', address='localhost', port=8585,
               username='test', password='test',
               enable_nats=True, nats_port=4222)      # or nats_token='...'
   app.add_node(node)

The two transports are drop-in twins: `Datastream` / `ControlStream` drive
whichever one the node was configured with (NATS takes precedence if both
are enabled). The only wire difference is the subject namespace — NATS data
subjects are dot-delimited and *nested under systems*
(``api.systems.{sysId}.datastreams.{dsId}.observations:data.<token>``),
which the client builds for you. Only PROACTIVE-mode plain publish/subscribe
is supported; the optional flow-control channel and JetStream are out of
scope.


Authentication
--------------
OSHConnect currently speaks **HTTP Basic Auth** to OGC CS API servers.
There is no bearer-token, OAuth, or API-key flow yet — the underlying
``requests`` library carries credentials as a ``(username, password)``
tuple.

.. note::

   Work is planned for more secure authentication options — OpenID
   Connect (OIDC) / OAuth2 bearer-token flows, matching OpenSensorHub's
   own OAuth security module, and API keys are under consideration.
   Until those land, always pair Basic Auth with ``protocol='https'``
   on anything other than a local dev node, since Basic credentials are
   only base64-encoded, not encrypted.

For a secured server, pass ``username`` and ``password`` to ``Node``:

.. code-block:: python

   node = Node(protocol='https', address='sensors.example.org', port=443,
               username='alice', password='s3cret')

Every HTTP call the node makes — discovery, resource creation, schema
fetches — automatically carries those credentials. Internally, the node
constructs an ``APIHelper`` that holds the credentials and reads them
back via ``get_helper_auth()`` on each request. The same credentials
also flow into the MQTT client when ``enable_mqtt=True`` (and, for NATS,
into user/password auth when ``enable_nats=True``; pass ``nats_token`` for
token auth instead).

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
