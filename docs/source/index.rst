OSHConnect-Python
=================

OSHConnect-Python is the Python member of the OSHConnect family of application
libraries. It provides a simple, straightforward way to interact with
OpenSensorHub (or any other OGC API – Connected Systems server).

What do you want to do?
-----------------------

- **Read live sensor data** — start with the
  :doc:`getting_started/quickstart`, then :doc:`guides/consuming`.
- **Feed data into an OSH node** (build a sensor driver or ingester in
  Python) — :doc:`guides/publishing`.
- **Send commands to devices** — :doc:`guides/commanding`.
- **Work with binary or compact wire formats** (video, swe+proto,
  swe+flatbuffers) — :doc:`guides/encodings`.
- **Understand the object model** — :doc:`architecture/index`.
- **Look up a class or function** — :doc:`api`.

Capabilities
------------

OSHConnect-Python supports Parts 1, 2, and 3 (Pub/Sub) of the OGC Connected
Systems API, including:

- System, Datastream, and ControlStream discovery and management
- Real-time MQTT streaming using CS API Part 3 ``:data/<format>`` topic conventions
  (``swe-binary``, ``swe-json``, ``json``, …)
- Resource event topic subscriptions (CloudEvents lifecycle notifications)
- Batch retrieval and archival stream playback
- Configuration persistence (JSON save / load)
- SWE Common schema builders for defining datastream and command schemas
- OGC standard-format serialization (SML+JSON, OM+JSON, SWE+JSON, GeoJSON)

All major classes and utilities are importable directly from ``oshconnect``.
Lower-level CS API utilities are available from ``oshconnect.csapi4py``.

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   getting_started/installation
   getting_started/prerequisites
   getting_started/quickstart

.. toctree::
   :maxdepth: 1
   :caption: Guides

   guides/connecting
   guides/consuming
   guides/publishing
   guides/commanding
   guides/encodings
   guides/configuration

.. toctree::
   :maxdepth: 1
   :caption: Concepts

   architecture/index

.. toctree::
   :maxdepth: 1
   :caption: Reference

   api


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
