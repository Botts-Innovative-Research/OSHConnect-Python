OSH Connect Tutorial
====================
OSH Connect for Python is a straightforward library for interacting with OpenSensorHub using OGC API Connected Systems.
This tutorial will help guide you through a few simple examples to get you started with OSH Connect.

OSH Connect Installation
--------------------------
OSH Connect can be installed using `pip`. To install the latest version of OSH Connect, run the following command:

.. code-block:: bash

   pip install git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git

Or, if you prefer `poetry`:

.. code-block:: bash

   poetry add git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git

Creating an instance of OSHConnect
---------------------------------------
The intended method of interacting with OpenSensorHub is through the `OSHConnect` class.
To this you must first create an instance of `OSHConnect`:

.. code-block:: python

   from oshconnect.oshconnectapi import OSHConnect, TemporalModes

   connect_app = OSHConnect(name='OSHConnect', playback_mode=TemporalModes.REAL_TIME)

.. tip::

    The `name` parameter is optional, but can be useful for debugging purposes.
    The playback mode determines how the data is retrieved from OpenSensorHub.

The next step is to add a `Node` to the `OSHConnect` instance. A `Node` is a representation of a server that you want to connect to.
The  OSHConnect instance can support multiple Nodes at once.

Adding a Node to an OSHConnect instance
-----------------------------------------
.. code-block:: python

    from oshconnect.oshconnectapi import OSHConnect, TemporalModes
    from oshconnect.osh_connect_datamodels import Node

    connect_app = OSHConnect(name='OSHConnect', playback_mode=TemporalModes.REAL_TIME)
    node = Node(protocol='http', address="localhost", port=8585, username="test", password="test")
    connect_app.add_node(node)

System Discovery
-----------------------------------------
Once you have added a Node to the OSHConnect instance, you can discover the systems that are available on that Node.
This is done by calling the `discover_systems()` method on the OSHConnect instance.

.. code-block:: python

    connect_app.discover_systems()

Datastream Discovery
-----------------------------------------
Once you have discovered the systems that are available on a Node, you can discover the datastreams that are available to those
systems. This is done by calling the `discover_datastreams` method on the OSHConnect instance.

.. code-block:: python

    connect_app.discover_datastreams()

Playing back data
-----------------------------------------
Once you have discovered the datastreams that are available on a Node, you can play back the data from those datastreams.
This is done by calling the `playback_streams` method on the OSHConnect instance.

.. code-block:: python

    connect_app.playback_streams()

Accessing data
-----------------------------------------
To access the data retrieved from the datastreams, you need to access the messages available to the OSHConnect instance.
Calling the `get_messages` method on the OSHConnect instance will return a list of `MessageWrapper` objects that contain individual
observations.

.. code-block:: python

    messages = connect_app.get_messages()

    for message in messages:
        print(message)

    # or, to access the individual observations
    for message in messages:
        for observation in message.observations:
            do_something_with(observation)


Resource Insertion
=========================================
Other use cases of the OSH Connect library may involve inserting new resources into OpenSensorHub or another Connected Systems API server.

Adding and Inserting a New System
-----------------------------------------
The first major step in a common workflow is to add a new system to the OSH Connect instance.
There are a couple of ways to do this, but the recommended method is as follows:

.. note::

    The `insert_system` method requires a `Node` object to be passed in as the second argument.
    Creating one is covered in an earlier section.

.. code-block:: python

    from oshconnect.osh_connect_datamodels import System

    new_system = app.insert_system(
        System(name="Test System", description="Test System Description", label="Test System",
               urn="urn:system:test"), node)

Adding and Inserting a New Datastream
-----------------------------------------
Once you have a `System` object, you can add a new datastream to it. This is one of the more complex operations
in the library as the schema is very flexible by design. Luckily, the schemas are validated by the underlying data
models, so you can be sure that your datastream is valid before inserting it.

.. caution::

    Some implementations of the Connected Systems API may require additional fields to be filled in.
    OSH Connect is primarily focused on the OpenSensorHub implementation, but does not some of the fields that
    are required by and OpenSensorHub node.

In this example, we will add a new datastream to the `new_system` object that we created in the previous example.
You'll note the creation of a `DataRecordSchema` object, in OSH's implementation, a DataRecord is the root of all
datastream schemas.

.. code-block:: python

    from oshconnect.osh_connect_datamodels import Datastream

    datarecord_schema = DataRecordSchema(label='Example Data Record', description='Example Data Record Description',
                                         definition='www.test.org/records/example-datarecord', fields=[])
    time_schema = TimeSchema(label="Timestamp", definition="http://test.com/Time", name="timestamp",
                             uom=URI(href="http://test.com/TimeUOM"))
    continuous_value_field = QuantitySchema(name='continuous-value-distance', label='Continuous Value Distance',
                                            description='Continuous Value Description',
                                            definition='www.test.org/fields/continuous-value',
                                            uom=UCUMCode(code='m', label='meters'))
    example_text_field = TextSchema(name='example-text-field', label='Example Text Field', definition='www.test.org/fields/example-text-field')
    # add the fields to the datarecord schema, these can also be added added to the datarecord when it is created
    datarecord_schema.fields.append(time_schema)   # TimeSchema is required to be the first field in the datarecord for OSH
    datarecord_schema.fields.append(continuous_value_field)
    datarecord_schema.fields.append(example_text_field)
    # Add the datastream to the system
    datastream = new_system.add_insert_datastream(datarecord_schema)

.. note::

    A TimeSchema is required to be the first field in the DataRecordSchema for OSH.

Inserting an Observation into and OpenSensorHub Node
-----------------------------------------------------
Upon successfully adding a new datastream to a system, it is now possible to send observation data to the node.

.. code-block:: python

    datastream.insert_observation_dict({
        "resultTime": TimeInstant.now_as_time_instant().get_iso_time(),     # resultTime is required for OSH
        "phenomenonTime": TimeInstant.now_as_time_instant().get_iso_time(), # phenomenonTime is required for OSH
        "result": {
            "timestamp": TimeInstant.now_as_time_instant().epoch_time,
            "continuous-value-distance": 1.0,
            "example-text-field": "Here is some text"
        }
    })

.. note::

        The `resultTime` and `phenomenonTime` fields are required for OSH.
        The `result` field is representative of the schemas included in the DataRecordSchema's fields.
        You'll notice that they are referred to by their `name` field in the schema as it is the "machine" name
        of the output.