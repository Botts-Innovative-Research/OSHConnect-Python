API Reference
=============

OSHConnect
----------


OSHConnect Utilities and Helpers
--------------------------------
.. automodule:: oshconnect.oshconnectapi
    :members:
    :undoc-members:
    :show-inheritance:

OSH Connect Data Models
-----------------------
These are the second highest level pieces in the hierarchy of the library and the utilities needed to help almost
everything else in the app function.

.. automodule:: oshconnect.osh_connect_datamodels
    :members:
    :undoc-members:
    :show-inheritance:


DataSources and Messaging
-------------------------
Due to their extreme importance in the library, the data sources are listed separately along with the classes that help
manage them and their data.

.. automodule:: oshconnect.datasource
    :members:
    :undoc-members:
    :show-inheritance:

Time Management
---------------
Currently **WIP** but this module will contain the classes and functions that help manage the current time and other
playback features of groups of datasources/datafeeds

.. automodule:: oshconnect.timemanagement
    :members:
    :undoc-members:
    :show-inheritance:

Styling
-------
**WIP** This module contains the classes and functions that help manage the styling and visualization recommendations that
the library provides.

Datastore
---------
**WIP** This module is for managing the state of the app. The configurations files are intended to be interchgangale
among all language versions of the OSHConnect ecosystem.

Core Data Models
----------------
Theses data models are not often intended to be used directly by the user, but are used by the library to help manage
validation of data that flows to and from the API.

.. automodule:: oshconnect.core_datamodels
    :members:
    :undoc-members:
    :show-inheritance:



Helpers
~~~~~~~
