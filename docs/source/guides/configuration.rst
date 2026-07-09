Saving and Loading Configuration
================================

The OSHConnect state (nodes, systems, datastreams) can be persisted to a JSON file:

.. code-block:: python

   app.save_config()          # saves to a default file
   app = OSHConnect.load_config('my_config.json')
