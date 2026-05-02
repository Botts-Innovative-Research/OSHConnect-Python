OSHConnect-Python
=================

OSHConnect-Python is the Python member of the OSHConnect family of application
libraries. It provides a simple, straightforward way to interact with
OpenSensorHub (or any other OGC API – Connected Systems server).

It supports Parts 1, 2, and 3 (Pub/Sub) of the OGC Connected Systems API,
including:

- System, Datastream, and ControlStream discovery and management
- Real-time MQTT streaming using CS API Part 3 ``:data`` topic conventions
- Resource event topic subscriptions (CloudEvents lifecycle notifications)
- Batch retrieval and archival stream playback
- Configuration persistence (JSON save / load)
- SWE Common schema builders for defining datastream and command schemas
- OGC standard-format serialization (SML+JSON, OM+JSON, SWE+JSON, GeoJSON)

All major classes and utilities are importable directly from ``oshconnect``.
Lower-level CS API utilities are available from ``oshconnect.csapi4py``.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   architecture
   tutorial
   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
