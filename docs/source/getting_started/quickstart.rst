Quickstart
==========

From install to live observations in about thirty lines. This assumes a
running OSH node with the Connected Systems API and MQTT services enabled
— see :doc:`prerequisites` if you don't have one yet.

Install the current alpha (details and uv equivalents in
:doc:`installation`):

.. code-block:: bash

   pip install --pre oshconnect

Connect, discover, and stream:

.. code-block:: python

   import time

   from oshconnect import OSHConnect, Node, StreamableModes

   # one app, one node — adjust host/port/credentials to your OSH server
   app = OSHConnect(name='QuickstartApp')
   node = Node(protocol='http', address='localhost', port=8585,
               username='admin', password='admin',
               enable_mqtt=True, mqtt_port=1883)
   app.add_node(node)

   # find every system and datastream the node serves
   app.discover_systems()
   app.discover_datastreams()

   # start real-time streaming on each datastream
   for ds in app.get_datastreams():
       ds.set_connection_mode(StreamableModes.PULL)
       ds.initialize()
       ds.start()

   # observations accumulate on each datastream's inbound deque
   time.sleep(2)
   for ds in app.get_datastreams():
       while ds.get_inbound_deque():
           print(ds.get_inbound_deque().popleft())

That's the whole read path: one ``OSHConnect`` app, one ``Node`` with the
MQTT transport enabled, discovery, then a deque of live observations per
datastream.

Where to go next
----------------

- :doc:`/guides/connecting` — nodes, transports (MQTT / NATS), and
  authentication in depth
- :doc:`/guides/consuming` — discovery, streaming, topic conventions, and
  event subscriptions
- :doc:`/guides/publishing` — register your own systems and datastreams
  and publish observations
- :doc:`/guides/commanding` — control streams, commands, and status
- :doc:`/architecture/index` — how the pieces fit together
