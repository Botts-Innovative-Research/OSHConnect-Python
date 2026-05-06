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