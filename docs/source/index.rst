Welcome to OSHConnect-Python's documentation!
=============================================

OSHConnect-Python
=================
OSHConnect-Python is the Python version of the OSHConnect family of application libraries intended to provide a
simple and straightforward way to interact with OpenSensorHub (or another CS API server) by way of
OGC API - Connected Systems.

It supports Parts 1, 2, and 3 (Pub/Sub) of the OGC Connected Systems API, including:

- System, Datastream, and ControlStream discovery and management
- Real-time MQTT streaming with CS API Part 3 ``:data`` topic conventions
- Resource event topic subscriptions (CloudEvents lifecycle notifications)
- Batch retrieval and archival stream playback
- Configuration persistence (JSON save/load)
- SWE Common schema builders for defining datastream and command schemas

All major classes and utilities are importable directly from ``oshconnect``.
Lower-level CS API utilities are available from ``oshconnect.csapi4py``.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   tutorial
   api



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`