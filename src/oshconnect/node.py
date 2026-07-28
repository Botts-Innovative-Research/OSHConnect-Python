#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""`Node` — one client connection to an OpenSensorHub server.

A `Node` owns the `APIHelper` that builds and executes HTTP requests, an
optional `MQTTCommClient`, and the list of `System` objects discovered
from or inserted into that server. This module also houses the small
session / endpoint helpers that travel with a node:

- `Endpoints` — default URL path segments for the server's REST APIs.
- `Utilities` — module-level helper namespace (currently just
  base64-encoded Basic-Auth construction).
- `OSHClientSession` — per-node client session owning its registered
  streamables' lifecycle.
- `SessionManager` — top-level registry of `OSHClientSession` instances.

`Node.discover_systems` and `Node.from_storage_dict` reach back into the
`System` wrapper at runtime; those imports are deferred to method bodies
to avoid an import cycle with `oshconnect.resources.system`.
"""
from __future__ import annotations

import asyncio
import base64
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from .csapi4py.constants import APIResourceTypes
from .csapi4py.default_api_helpers import APIHelper
from .csapi4py.mqtt import MQTTCommClient
from .csapi4py.nats import NatsCommClient
from .resource_datamodels import SystemResource

if TYPE_CHECKING:
    from .resources.base import StreamableResource
    from .resources.system import System


@dataclass(kw_only=True)
class Endpoints:
    """Default URL path segments for an OSH server's REST APIs."""
    root: str = "sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


class Utilities:
    """Module-level helper namespace; intentionally just static methods."""

    @staticmethod
    def convert_auth_to_base64(username: str, password: str) -> str:
        """Return ``username:password`` Base64-encoded for HTTP Basic Auth."""
        return base64.b64encode(f"{username}:{password}".encode()).decode()


class OSHClientSession:
    """One client session against a Node, owning its registered streamables.

    Created by `SessionManager.register_session` and used by `Node` to manage
    the lifecycle (start/stop) of every `StreamableResource` attached to that
    node. Holds the streamables in a dict keyed by streamable ID.

    :param base_url: Base URL of the OSH server (passed by Node, not used
        directly by this class today).
    :param verify_ssl: Whether to verify TLS certificates. Default True.
    """
    verify_ssl = True
    _streamables: dict[str, 'StreamableResource'] = None

    def __init__(self, base_url, *args, verify_ssl=True, **kwargs):
        # super().__init__(base_url, *args, **kwargs)
        self.verify_ssl = verify_ssl
        self._streamables = {}

    def connect_streamables(self):
        """Call ``start()`` on every registered streamable."""
        for streamable in self._streamables.values():
            streamable.start()

    def close_streamables(self):
        """Call ``stop()`` on every registered streamable."""
        for streamable in self._streamables.values():
            streamable.stop()

    def register_streamable(self, streamable: StreamableResource):
        """Track a streamable so its lifecycle is driven by this session."""
        if self._streamables is None:
            self._streamables = {}
        self._streamables[streamable.get_streamable_id_str()] = streamable


class SessionManager:
    """Top-level registry for `OSHClientSession` instances, one per Node.

    The application owns one `SessionManager`; passing it to ``Node(...)``
    causes the node to call `register_session` and bind itself to a fresh
    `OSHClientSession`. `start_session_streams` / `start_all_streams` are
    convenience entry points for booting streams on a single node or all
    nodes at once.

    :param session_tokens: Optional dict of session tokens keyed by ID
        (reserved for future auth schemes; currently unused).
    """
    _session_tokens = None
    sessions: dict[str, OSHClientSession] = None

    def __init__(self, session_tokens: dict[str, str] = None):
        self._session_tokens = session_tokens
        self.sessions = {}

    def register_session(self, session_id, session: OSHClientSession) -> OSHClientSession:
        """Store ``session`` under ``session_id`` and return it."""
        self.sessions[session_id] = session
        return session

    def unregister_session(self, session_id):
        """Remove the session and call ``close()`` on it."""
        session = self.sessions.pop(session_id)
        session.close()

    def get_session(self, session_id) -> OSHClientSession | None:
        """Return the session for ``session_id`` or ``None`` if unknown."""
        return self.sessions.get(session_id, None)

    def start_session_streams(self, session_id):
        """Start every streamable on the session identified by ``session_id``.

        :raises ValueError: if no session is registered for that ID.
        """
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"No session found for ID {session_id}")
        session.connect_streamables()

    def start_all_streams(self):
        """Start every streamable across every registered session."""
        for session in self.sessions.values():
            session.connect_streamables()


@dataclass(kw_only=True)
class Node:
    """One connection to a single OSH server.

    A `Node` is the unit of "where to talk to". It owns the `APIHelper` that
    builds and executes HTTP requests, an optional `MQTTCommClient` for
    Pub/Sub, and the list of `System` objects discovered from or inserted
    into that server. Most user code creates a `Node` and then either calls
    `discover_systems()` or attaches user-built systems via `add_system()`.

    :param protocol: ``"http"`` or ``"https"``.
    :param address: Hostname or IP (no scheme).
    :param port: HTTP port the server is listening on.
    :param username: Optional Basic-Auth username.
    :param password: Optional Basic-Auth password.
    :param server_root: First path segment of the server URL (default
        ``"sensorhub"``).
    :param api_root: Second path segment under ``server_root``
        (default ``"api"``).
    :param mqtt_topic_root: Override for the MQTT topic root if it diverges
        from the HTTP api root (CS API Part 3 § A.1).
    :param session_manager: Optional `SessionManager`; if given the node
        registers itself and gets a fresh `OSHClientSession`.
    :param enable_mqtt: If True, connects an MQTT client to ``address``.
    :param mqtt_port: MQTT broker port. Default 1883.
    """
    _id: str
    protocol: str
    address: str
    port: int
    server_root: str = 'sensorhub'
    endpoints: Endpoints
    is_secure: bool
    _basic_auth: bytes
    _api_helper: APIHelper
    _systems: list[System] = field(default_factory=list)
    _client_session: OSHClientSession
    _mqtt_client: MQTTCommClient
    _mqtt_port: int = 1883
    _nats_client: NatsCommClient
    _nats_port: int = 4222

    def __init__(self, protocol: str, address: str, port: int, username: str = None, password: str = None,
                 server_root: str = 'sensorhub', api_root: str = 'api', mqtt_topic_root: str = None,
                 session_manager: SessionManager = None, enable_mqtt: bool = False, mqtt_port: int = 1883,
                 enable_nats: bool = False, nats_host: str = None, nats_port: int = 4222,
                 nats_token: str = None, mqtt_legacy_topics: bool = False):
        self._id = f'node-{uuid.uuid4()}'
        self.protocol = protocol
        self.address = address
        self.server_root = server_root
        self.port = port
        self.is_secure = username is not None and password is not None
        if self.is_secure:
            self.add_basicauth(username, password)
        self.endpoints = Endpoints()
        self._api_helper = APIHelper(
            server_url=self.address, protocol=self.protocol, port=self.port,
            server_root=self.server_root, api_root=api_root, mqtt_topic_root=mqtt_topic_root,
            username=username, password=password, legacy_topics=mqtt_legacy_topics,
        )
        if self.is_secure:
            self._api_helper.user_auth = True
        self._systems = []
        # Default to no client session; populated by `register_with_session_manager`.
        self._client_session = None
        if session_manager is not None:
            session_task = self.register_with_session_manager(session_manager)
            asyncio.gather(session_task)

        if enable_mqtt:
            self._mqtt_port = mqtt_port
            self._mqtt_client = MQTTCommClient(url=self.address, port=self._mqtt_port, username=username,
                                               password=password, client_id_suffix=uuid.uuid4().hex, )
            self._mqtt_client.connect()
            self._mqtt_client.start()

        if enable_nats:
            self._nats_port = nats_port
            # The NATS broker may live on a different host than the HTTP API
            # (a common split-host deployment). Default to the API host for
            # backwards compatibility; ``nats_host`` may be a bare hostname or
            # a full ``nats://host:port`` URL (NatsCommClient handles both).
            nats_url = nats_host if nats_host is not None else self.address
            self._nats_client = NatsCommClient(url=nats_url, port=self._nats_port, username=username,
                                               password=password, token=nats_token,
                                               client_id_suffix=uuid.uuid4().hex)
            self._nats_client.connect()
            self._nats_client.start()

    def get_id(self) -> str:
        """Return the locally-generated node ID (``node-<uuid4>``)."""
        return self._id

    def get_address(self) -> str:
        """Return the configured server hostname/IP."""
        return self.address

    def get_port(self) -> int:
        """Return the configured server port."""
        return self.port

    def get_api_endpoint(self) -> str:
        """Return the fully-qualified CS API root URL for this node."""
        return self._api_helper.get_api_root_url()

    def add_basicauth(self, username: str, password: str):
        """Attach Basic-Auth credentials and mark the node as secure."""
        if not self.is_secure:
            self.is_secure = True
        self._basic_auth = base64.b64encode(f"{username}:{password}".encode('utf-8'))

    def get_decoded_auth(self) -> str:
        """Return the Base64 Basic-Auth header value as a UTF-8 string."""
        return self._basic_auth.decode('utf-8')

    # def get_basicauth(self):
    #     return BasicAuth(self._api_helper.username, self._api_helper.password)

    def get_mqtt_client(self) -> MQTTCommClient:
        """Return the connected `MQTTCommClient` or ``None`` if MQTT was
        not enabled at construction (``enable_mqtt=True``)."""
        return getattr(self, '_mqtt_client', None)

    def get_nats_client(self) -> NatsCommClient:
        """Return the connected `NatsCommClient` or ``None`` if NATS was
        not enabled at construction (``enable_nats=True``)."""
        return getattr(self, '_nats_client', None)

    def get_comm_client(self):
        """Return the active pub/sub transport client for this node.

        Prefers NATS when it was enabled, otherwise falls back to MQTT (and
        ``None`` if neither was enabled). `StreamableResource` reads this so
        it can drive whichever transport the node was configured with.
        """
        nats_client = self.get_nats_client()
        if nats_client is not None:
            return nats_client
        return self.get_mqtt_client()

    def discover_systems(self) -> list[System] | None:
        """GET ``/systems?f=application/sml+json`` and create a `System` for
        each entry.

        We pin SML+JSON because the GeoJSON listing variant (OSH's default
        when no format is specified) is a summary that drops SensorML
        detail — ``identifiers``, ``classifiers``, ``keywords``,
        ``characteristics``, ``definition``, ``typeOf``, ``configuration``,
        ``contacts``, ``documentation``, ``inputs``/``outputs``/``parameters``,
        ``modes``, ``method``, ``featuresOfInterest``. SML+JSON delivers
        all of those, which cross-node sync and any caller round-tripping
        ``_underlying_resource`` need.

        ``Accept: application/sml+json`` is ignored by the OSH listing
        endpoint (still returns GeoJSON), so the format is selected via
        the ``?f=`` query parameter — the OGC API standard format
        selector. ``SystemResource.model_validate`` parses both shapes,
        so the wrapper still copes if a server returns GeoJSON anyway.

        The new systems are appended to this node's internal list and also
        returned for convenience.

        :return: List of newly-created `System` objects, or ``None`` if
            the HTTP request failed.
        """
        # Deferred runtime import: System -> StreamableResource -> Node would
        # otherwise close a cycle when this module is first loaded.
        from .resources.system import System
        result = self._api_helper.get_resource(
            APIResourceTypes.SYSTEM,
            params={'f': 'application/sml+json'},
        )
        if result.ok:
            new_systems = []
            system_objs = result.json()['items']
            for system_json in system_objs:
                system = SystemResource.model_validate(system_json, by_alias=True)
                # Route through the canonical factory so the parsed
                # `SystemResource` is bound to the wrapper via
                # `set_system_resource(...)`. The previous manual
                # `System(label=..., name=..., urn=..., resource_id=...)`
                # call dropped the parsed resource on the floor —
                # any caller reaching for `_underlying_resource`
                # (deep-copy round-trip, cross-node sync, geometry,
                # validTime, properties) saw only a thin shell.
                sys_obj = System.from_resource(system, parent_node=self)
                self._systems.append(sys_obj)
                new_systems.append(sys_obj)
            return new_systems
        else:
            return None

    def get_api_helper(self) -> APIHelper:
        """Return the `APIHelper` this node uses for HTTP calls."""
        return self._api_helper

    # System Management

    def add_system(self, system: System, insert_resource: bool = False) -> System:
        """Attach a system to this node.

        When ``insert_resource=True``, the system is first POSTed to the
        server via ``system.insert_self()`` (which populates its
        server-assigned resource id), then attached locally — so the
        system enters this node's collection already carrying its real
        id. With ``insert_resource=False`` the system is attached
        in-memory only; useful when reconstructing state from a
        datastore or staging a system before a deferred POST.

        A failed POST propagates out of ``insert_self()`` and the system
        is *not* attached — better a loud failure here than a system that
        looks attached but has no server-side id, which previously only
        surfaced later as an ``AttributeError`` from the first child
        resource call.

        :param system: ``System`` object to attach.
        :param insert_resource: Whether to POST the system to the
            server before attaching it locally.
        :return: The same ``System`` (now parented to this node and
            tracked in ``self.systems()``).
        :raises Exception: if ``insert_resource=True`` and the server
            rejects the POST.
        """
        if insert_resource:
            system.insert_self()
        system.set_parent_node(self)
        self._systems.append(system)
        return system

    def systems(self) -> list[System]:
        """Return the list of `System` objects currently attached to this node."""
        return self._systems

    def register_with_session_manager(self, session_manager: SessionManager):
        """
        Registers this node with the provided session manager, creating a new client session.
        :param session_manager: SessionManager instance
        """
        self._client_session = session_manager.register_session(self._id, OSHClientSession(
            base_url=self._api_helper.get_base_url()))

    def register_streamable(self, streamable: StreamableResource):
        """Register a streamable with this node's session so its lifecycle
        is driven by `OSHClientSession.connect_streamables` /
        `close_streamables`.

        Soft no-op when no `SessionManager` was attached at construction;
        the caller can still drive the streamable manually via
        `initialize()` / `start()` / `stop()`.
        """
        if self._client_session is None:
            return
        self._client_session.register_streamable(streamable)

    def get_session(self) -> OSHClientSession:
        """Return the `OSHClientSession` bound to this node."""
        return self._client_session

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe dict snapshot of this node — connection
        params, attached systems / streamables, and any locally-tracked
        state — for OSHConnect's persistence layer (see
        `OSHConnect.save_config`, `oshconnect.datastores.sqlite_store`).

        Not a CS API server-shaped payload; the dict format is OSHConnect's
        own. For a CS API-shaped representation, use the underlying
        pydantic resource model's ``model_dump(by_alias=True)``.
        """
        data = {
            "_id": self._id,
            "protocol": self.protocol,
            "address": self.address,
            "port": self.port,
            "server_root": self.server_root,
            "api_root": getattr(self._api_helper, "api_root", "api"),
            "mqtt_topic_root": getattr(self._api_helper, "mqtt_topic_root", None),
            "is_secure": self.is_secure,
            "username": getattr(self._api_helper, "username", None),
            "password": getattr(self._api_helper, "password", None),
            "_systems": [system.to_storage_dict() for system in self._systems] if self._systems is not None else None,
        }
        data["name"] = getattr(self, "name", None)
        data["label"] = getattr(self, "label", None)
        data["urn"] = getattr(self, "urn", None)
        data["description"] = getattr(self, "description", None)
        datastreams = getattr(self, "datastreams", None)
        if datastreams is not None:
            data["datastreams"] = [ds.to_storage_dict() for ds in datastreams]
        else:
            data["datastreams"] = None
        control_channels = getattr(self, "control_channels", None)
        if control_channels is not None:
            data["control_channels"] = [cc.to_storage_dict() for cc in control_channels]
        else:
            data["control_channels"] = None
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True, mode='json')
            elif hasattr(underlying, 'to_dict'):
                data["underlying_resource"] = underlying.to_dict()
            else:
                data["underlying_resource"] = str(underlying)
        else:
            data["underlying_resource"] = None
        # Remove any 'resource' key if present
        data.pop("resource", None)
        return data

    @classmethod
    def from_storage_dict(cls, data: dict, session_manager: 'SessionManager' = None) -> 'Node':
        """Build a `Node` from a dict produced by `to_storage_dict`
        (i.e., from OSHConnect's persistence layer, not from a CS API
        server response).

        Expects connection params (``protocol``, ``address``, ``port``,
        optional ``username``/``password``/``server_root``/``api_root``/
        ``mqtt_topic_root``), an ``_id``, and a ``_systems`` list.

        :param data: Source dict.
        :param session_manager: Optional `SessionManager` to register the
            rebuilt node with — required if any child `StreamableResource`
            in ``_systems`` was originally registered.
        """
        # Deferred runtime import: System -> StreamableResource -> Node would
        # otherwise close a cycle when this module is first loaded.
        from .resources.system import System
        node = cls(
            protocol=data["protocol"], address=data["address"], port=data["port"],
            username=data.get("username"), password=data.get("password"),
            server_root=data.get("server_root", "sensorhub"),
            api_root=data.get("api_root", "api"),
            mqtt_topic_root=data.get("mqtt_topic_root"),
        )
        node._id = data["_id"]
        node.is_secure = data.get("is_secure", False)
        # Register with the session manager before rehydrating child resources,
        # because StreamableResource.__init__ calls node.register_streamable().
        if session_manager is not None:
            node.register_with_session_manager(session_manager)
        node._systems = [System.from_storage_dict(sys, node) for sys in data.get("_systems", [])] if data.get(
            "_systems") is not None else []
        return node
