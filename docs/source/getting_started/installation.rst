Installation
============

OSHConnect is published to `PyPI <https://pypi.org/project/oshconnect/>`_, but
so far **only as alpha pre-releases** (e.g. ``0.5.1a22``) — there is no stable
release yet. Because of that, ``pip``/``uv`` skip these versions by default,
so you must opt into pre-releases.

Install using ``uv`` (recommended):

.. code-block:: bash

   uv add "oshconnect==0.5.1a22"        # exact pin auto-allows the pre-release
   # or, to allow future alphas:
   uv add "oshconnect>=0.5.1a0" --prerelease=allow

Or with ``pip``:

.. code-block:: bash

   pip install "oshconnect==0.5.1a22"   # exact pin auto-allows the pre-release
   # or, latest alpha — but note pip's --pre is global, so it also allows
   # pre-releases of every dependency, not just oshconnect:
   pip install --pre oshconnect

To track unreleased work, install straight from Git instead (no pre-release
flag needed):

.. code-block:: bash

   uv add "git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git"
   pip install "git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git"

Optional features
~~~~~~~~~~~~~~~~~
The base install is deliberately small: HTTP discovery, resource CRUD, and
the pydantic model layer. Streaming transports and binary encodings are
opt-in extras:

.. list-table::
   :header-rows: 1
   :widths: 22 30 48

   * - Extra
     - Installs
     - Enables
   * - ``[mqtt]``
     - ``paho-mqtt``
     - Real-time MQTT streaming (``enable_mqtt=True``)
   * - ``[nats]``
     - ``nats-py``
     - NATS.io streaming (``enable_nats=True``)
   * - ``[streaming]``
     - both of the above
     - Both transports
   * - ``[protobuf]``
     - ``protobuf``
     - ``application/swe+proto`` encoding
   * - ``[flatbuffers]``
     - ``flatbuffers``
     - ``application/swe+flatbuffers`` encoding
   * - ``[tinydb]``
     - ``tinydb``
     - TinyDB-backed persistence
   * - ``[all]``
     - all of the above
     - Everything except the ``av`` video demo extra

**Extras combine** — list several in one comma-separated bracket (no spaces);
they are additive, so ``[mqtt,protobuf]`` installs both. This works
identically from PyPI, from the Git URL, and in a uv project.

From PyPI (remember the pre-release opt-in from the top of this page):

.. code-block:: bash

   pip install --pre "oshconnect[mqtt,protobuf]"        # MQTT + swe+proto
   pip install --pre "oshconnect[streaming]"            # MQTT + NATS
   pip install --pre "oshconnect[all]"                  # everything

   uv add "oshconnect[mqtt,nats]==0.5.1a22"             # exact pin auto-allows the alpha
   uv add "oshconnect[streaming]>=0.5.1a0" --prerelease=allow

From Git (for unreleased work), use the PEP 508 "name-with-extras @ URL" form:

.. code-block:: bash

   pip install "oshconnect[mqtt,protobuf] @ git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git"

   uv add "oshconnect[mqtt,nats] @ git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git"
   # equivalently, the Git URL plus repeated --extra flags:
   uv add "git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git" --extra mqtt --extra nats

In a uv project you can also hand-edit ``pyproject.toml`` — the extras go in
the dependency string (from PyPI or from Git):

.. code-block:: toml

   [project]
   dependencies = [
       "oshconnect[mqtt,nats]>=0.5.1a0",   # from PyPI (needs the pre-release opt-in)
       # or from Git:
       # "oshconnect[mqtt,nats] @ git+https://github.com/Botts-Innovative-Research/OSHConnect-Python.git",
   ]

Using a transport without its extra raises a ``RuntimeError`` naming the
extra to install.
