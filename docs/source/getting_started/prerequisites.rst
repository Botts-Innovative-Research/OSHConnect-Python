Prerequisites
=============

OSHConnect-Python is a *client*. It talks to a running **OpenSensorHub (OSH)
node** (or any OGC API – Connected Systems server) — it does not start or host
one for you. Before using this library you need:

- **A running OSH node** with the **Connected Systems API** service enabled.
  This is what exposes the HTTP endpoints (Parts 1, 2, and 3) that discovery
  and resource creation use.
- **For real-time streaming:** the **Connected Systems API – MQTT** service
  module enabled on that node, with an MQTT broker reachable (OSH's default
  broker port is ``1883``). Streaming over NATS instead requires the
  Connected Systems API – NATS service and a reachable NATS server (default
  port ``4222``). Client-side, install the matching extra —
  ``oshconnect[mqtt]`` / ``oshconnect[nats]`` (see the *Optional
  features* table in :doc:`installation`).
- **Credentials** for the node, if it has security enabled (OSHConnect uses
  HTTP Basic Auth).

Installing, configuring, and enabling those services on an OSH node is outside
the scope of this library. See the official
`OpenSensorHub documentation <https://docs.opensensorhub.org/>`_ — in
particular the node setup guides and the
`OSHConnect getting-started guide
<https://docs.opensensorhub.org/docs/osh-connect/getting-started/usage>`_.

What you need from your OSH node
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
To point OSHConnect-Python at your node, gather the following from its
configuration and pass them to ``Node`` (see :doc:`/guides/connecting`):

.. list-table::
   :header-rows: 1
   :widths: 24 22 54

   * - What to find on the node
     - ``Node`` parameter
     - Notes
   * - Protocol
     - ``protocol``
     - ``'http'`` or ``'https'``.
   * - Host / IP
     - ``address``
     - Hostname or IP serving the node, e.g. ``'localhost'``.
   * - HTTP port
     - ``port``
     - The port the Connected Systems API is served on (e.g. ``8181``).
   * - Servlet root
     - ``server_root``
     - Context path; OSH default ``'sensorhub'``. The CS API base URL is
       ``{protocol}://{address}:{port}/{server_root}/{api_root}/``.
   * - API root
     - ``api_root``
     - CS API path segment; default ``'api'``.
   * - Username / password
     - ``username`` / ``password``
     - Only if the node enforces authentication.
   * - MQTT broker port
     - ``mqtt_port`` (with ``enable_mqtt=True``)
     - The node's MQTT broker port for real-time streaming; default ``1883``.
   * - NATS server port / token
     - ``nats_port`` / ``nats_token`` (with ``enable_nats=True``)
     - Only for NATS streaming; default port ``4222``.

For example, a node whose Connected Systems API answers at
``http://localhost:8585/sensorhub/api/`` with an MQTT broker on ``1883``
maps to:

.. code-block:: python

   node = Node(protocol='http', address='localhost', port=8585,
               server_root='sensorhub', api_root='api',
               enable_mqtt=True, mqtt_port=1883)
