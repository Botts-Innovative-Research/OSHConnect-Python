Publishing Sensor Data
======================

The write side: register a system, define a datastream schema, and
push observations into the node — the workflow for building a sensor
driver or data ingester in Python.

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
