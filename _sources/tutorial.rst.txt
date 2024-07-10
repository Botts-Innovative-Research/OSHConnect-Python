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
