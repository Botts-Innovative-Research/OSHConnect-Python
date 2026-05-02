API Reference
=============

All public symbols are re-exported from the top-level package and can be imported directly::

    from oshconnect import OSHConnect, Node, Datastream, TimePeriod, ObservationFormat, ...

Lower-level CS API utilities are available from the ``oshconnect.csapi4py`` subpackage::

    from oshconnect.csapi4py import APIResourceTypes, MQTTCommClient, ConnectedSystemsRequestBuilder, ...

---

Core Application
----------------

.. automodule:: oshconnect.oshconnectapi
    :members:
    :undoc-members:
    :show-inheritance:

---

Streamable Resources
--------------------
These are the primary objects for interacting with systems, datastreams, and control streams on an OSH node.
Includes ``Node``, ``System``, ``Datastream``, ``ControlStream``, and supporting enums.

.. automodule:: oshconnect.streamableresource
    :members:
    :undoc-members:
    :show-inheritance:

---

Resource Data Models
--------------------
Pydantic models that represent CS API resources returned from or sent to an OSH server.

.. automodule:: oshconnect.resource_datamodels
    :members:
    :undoc-members:
    :show-inheritance:

---

SWE Schema Components
---------------------
Builder classes for constructing datastream and command schemas using SWE Common data types.

.. automodule:: oshconnect.swe_components
    :members:
    :undoc-members:
    :show-inheritance:

.. automodule:: oshconnect.schema_datamodels
    :members:
    :undoc-members:
    :show-inheritance:

---

Event System
------------
Pub/sub event bus for in-process notifications. Implement ``IEventListener`` to receive events.

The names below are re-exported from ``oshconnect.events.core``,
``oshconnect.events.handler``, etc.; ``:no-index:`` keeps Sphinx from
reporting them as duplicate object descriptions.

.. automodule:: oshconnect.eventbus
    :members:
    :undoc-members:
    :show-inheritance:
    :no-index:

---

Time Management
---------------

.. automodule:: oshconnect.timemanagement
    :members:
    :undoc-members:
    :show-inheritance:

---

CS API Integration (``csapi4py``)
----------------------------------

Constants and Enums
~~~~~~~~~~~~~~~~~~~

.. automodule:: oshconnect.csapi4py.constants
    :members:
    :undoc-members:
    :show-inheritance:

Request Builder
~~~~~~~~~~~~~~~

.. automodule:: oshconnect.csapi4py.con_sys_api
    :members:
    :undoc-members:
    :show-inheritance:

API Helper
~~~~~~~~~~

.. automodule:: oshconnect.csapi4py.default_api_helpers
    :members:
    :undoc-members:
    :show-inheritance:

MQTT Client
~~~~~~~~~~~

.. automodule:: oshconnect.csapi4py.mqtt
    :members:
    :undoc-members:
    :show-inheritance: