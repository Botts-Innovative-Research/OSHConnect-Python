#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/29
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

"""
Streamable resource hierarchy: the user-facing primitives for talking to an
OpenSensorHub server.

Object model
------------

::

    Node                # connection to one OSH server
    ├── APIHelper       # builds and executes HTTP requests
    └── System[]        # discovered or user-created sensor systems
        ├── Datastream[]      # output channels (observations)
        └── ControlStream[]   # input channels (commands + status)

`Node`, `System`, `Datastream`, and `ControlStream` are the types most user
code touches. `StreamableResource` is the abstract base that powers MQTT
streaming, WebSocket connections, and inbound/outbound message queues for
all three concrete subclasses.

Conventions
-----------

- Construction → `initialize()` (sets up MQTT subscriptions and the WS URL)
  → `start()` (opens the streaming loop). `stop()` tears down.
- Inbound MQTT messages land in `_inbound_deque`; outbound payloads queued
  via `publish()` / `insert_data()` flow through `_outbound_deque`.
- Resource creation (`add_insert_datastream`, `add_and_insert_control_stream`,
  `insert_self`) goes through the parent `Node`'s `APIHelper` and a
  `Location` header on the response is parsed to capture the new server-side
  ID.
- `StreamableModes`: `PUSH` = we publish, `PULL` = we subscribe,
  `BIDIRECTIONAL` = both. Defaults to `PUSH` on construction.
"""
from __future__ import annotations

import asyncio
import base64
import datetime
import json
import logging
import traceback
import uuid
import warnings
from abc import ABC
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import TypeVar, Generic, Union
from uuid import UUID, uuid4

from pydantic.alias_generators import to_camel

from .csapi4py.constants import APIResourceTypes
from .csapi4py.constants import ContentTypes
from .csapi4py.default_api_helpers import APIHelper
from .csapi4py.mqtt import MQTTCommClient
from .events import EventHandler, DefaultEventTypes
from .events.builder import EventBuilder
from .resource_datamodels import ControlStreamResource
from .resource_datamodels import DatastreamResource, ObservationResource
from .resource_datamodels import SystemResource
from .encoding import JSONEncoding
from .schema_datamodels import JSONCommandSchema, SWEDatastreamRecordSchema, SWEJSONCommandSchema
from .swe_components import DataRecordSchema
from .timemanagement import TimeInstant, TimePeriod, TimeUtils


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

    def __init__(self, protocol: str, address: str, port: int, username: str = None, password: str = None,
                 server_root: str = 'sensorhub', api_root: str = 'api', mqtt_topic_root: str = None,
                 session_manager: SessionManager = None, enable_mqtt: bool = False, mqtt_port: int = 1883):
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
            username=username, password=password,
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

    def discover_systems(self) -> list[System] | None:
        """GET ``/systems`` and create a `System` for each entry.

        The new systems are appended to this node's internal list and also
        returned for convenience.

        :return: List of newly-created `System` objects, or ``None`` if
            the HTTP request failed.
        """
        result = self._api_helper.retrieve_resource(APIResourceTypes.SYSTEM, req_headers={})
        if result.ok:
            new_systems = []
            system_objs = result.json()['items']
            print(system_objs)
            for system_json in system_objs:
                print(system_json)
                system = SystemResource.model_validate(system_json, by_alias=True)
                sys_obj = System(label=system.properties['name'],
                                 name=to_camel(system.properties['name'].replace(" ", "_")),
                                 urn=system.properties['uid'], parent_node=self, resource_id=system.system_id)

                self._systems.append(sys_obj)
                new_systems.append(sys_obj)
            return new_systems
        else:
            return None

    def add_new_system(self, system: System):
        """Attach a system to this node without inserting it server-side.

        Use `add_system(system, insert_resource=True)` if you also want to
        POST it to the server.
        """
        system.set_parent_node(self)
        self._systems.append(system)

    def get_api_helper(self) -> APIHelper:
        """Return the `APIHelper` this node uses for HTTP calls."""
        return self._api_helper

    # System Management

    def add_system(self, system: System, insert_resource: bool = False):
        """
        Add a system to the target node.
        :param system: System object
        :param insert_resource: Whether to insert the system into the target node's server, default is False
        :return:
        """
        if insert_resource:
            system.insert_self()
        self.add_new_system(system)
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


class Status(Enum):
    """Lifecycle states a `StreamableResource` transitions through:
    ``STOPPED → INITIALIZING → INITIALIZED → STARTING → STARTED → STOPPING → STOPPED``."""
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    STOPPED = "stopped"


class StreamableModes(Enum):
    """Direction(s) in which a streamable resource exchanges messages.

    - ``PUSH``: this client publishes outbound messages only.
    - ``PULL``: this client subscribes to inbound messages only.
    - ``BIDIRECTIONAL``: both publish and subscribe.
    """
    PUSH = "push"
    PULL = "pull"
    BIDIRECTIONAL = "bidirectional"


T = TypeVar('T', SystemResource, DatastreamResource, ControlStreamResource)


class StreamableResource(Generic[T], ABC):
    """Abstract base for `System`, `Datastream`, and `ControlStream`.

    Encapsulates the streaming machinery shared by all three: MQTT subscribe/
    publish, optional WebSocket I/O, inbound and outbound message deques,
    and lifecycle (`initialize` → `start` → `stop`). Subclasses set
    ``_underlying_resource`` (a `SystemResource` / `DatastreamResource` /
    `ControlStreamResource` pydantic model) and override `init_mqtt` to
    derive the appropriate topic.

    :param node: The parent `Node` this resource lives under.
    :param connection_mode: One of `StreamableModes`. Default ``PUSH``.
    """
    _id: UUID
    _resource_id: str
    # _canonical_link: str
    _topic: str
    _status: str = Status.STOPPED.value
    ws_url: str
    _message_handler = None
    _parent_node: Node
    _underlying_resource: T
    _process: Process
    _msg_reader_queue: asyncio.Queue[Union[str, bytes, float, int]]
    _msg_writer_queue: asyncio.Queue[Union[str, bytes, float, int]]
    _inbound_deque: deque
    _outbound_deque: deque
    _mqtt_client: MQTTCommClient
    _parent_resource_id: str
    _connection_mode: StreamableModes = StreamableModes.PUSH.value

    def __init__(self, node: Node, connection_mode: StreamableModes = StreamableModes.PUSH.value):
        self._id = uuid4()
        self._parent_node = node
        self._parent_node.register_streamable(self)
        self._mqtt_client = self._parent_node.get_mqtt_client()
        self._connection_mode = connection_mode
        self._inbound_deque = deque()
        self._outbound_deque = deque()
        self._parent_resource_id = None

    def get_streamable_id(self) -> UUID:
        """Return the local UUID assigned at construction (not the server-side ID)."""
        return self._id

    def get_streamable_id_str(self) -> str:
        """Return the local UUID as a hex string."""
        return self._id.hex

    def initialize(self):
        """Build the WebSocket URL, allocate I/O queues, and configure MQTT.

        Must be called before `start`. Inspects ``_underlying_resource`` to
        determine the right resource type and constructs the WS URL via
        the parent node's `APIHelper`.

        :raises ValueError: if ``_underlying_resource`` is not set or is
            not one of System / Datastream / ControlStream.
        """
        resource_type = None
        if isinstance(self._underlying_resource, SystemResource):
            resource_type = APIResourceTypes.SYSTEM
        elif isinstance(self._underlying_resource, DatastreamResource):
            resource_type = APIResourceTypes.DATASTREAM
        elif isinstance(self._underlying_resource, ControlStreamResource):
            resource_type = APIResourceTypes.CONTROL_CHANNEL
        if resource_type is None:
            raise ValueError(
                "Underlying resource must be set to either SystemResource or DatastreamResource before initialization.")
        # This needs to be implemented separately for each subclass
        res_id = getattr(self._underlying_resource, "ds_id", None) or getattr(self._underlying_resource, "cs_id", None)
        self.ws_url = self._parent_node.get_api_helper().construct_url(resource_type=resource_type,
                                                                       subresource_type=APIResourceTypes.OBSERVATION,
                                                                       resource_id=res_id, subresource_id=None)
        self._msg_reader_queue = asyncio.Queue()
        self._msg_writer_queue = asyncio.Queue()
        self.init_mqtt()
        self._status = Status.INITIALIZED.value

    def start(self):
        """Subclasses override to also kick off MQTT subscribe / async write
        tasks. Logs and returns silently if `initialize` hasn't been called.
        """
        if self._status != Status.INITIALIZED.value:
            logging.warning(f"Streamable resource {self._id} not initialized. Call initialize() first.")
            return
        self._status = Status.STARTING.value
        self._status = Status.STARTED.value

    async def stream(self):
        """Open a WebSocket to ``ws_url`` and run read/write loops in parallel.

        Used as an alternative to MQTT for resources that prefer WS streaming.
        Reads incoming frames into the message handler and drains
        ``_msg_writer_queue`` to the socket.
        """
        session = self._parent_node.get_session()

        try:
            async with session.ws_connect(self.ws_url, auth=self._parent_node.get_basicauth()) as ws:
                logging.info(f"Streamable resource {self._id} started.")
                read_task = asyncio.create_task(self._read_from_ws(ws))
                write_task = asyncio.create_task(self._write_to_ws(ws))
                await asyncio.gather(read_task, write_task)
        except Exception as e:
            logging.error(f"Error in streamable resource {self._id}: {e}")
            logging.error(traceback.format_exc())

    def init_mqtt(self):
        """Wire the MQTT subscribe-acknowledged callback if a client exists.

        Subclasses override to additionally derive their resource-specific
        topic into ``self._topic`` (see `Datastream.init_mqtt` /
        `ControlStream.init_mqtt`).
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return

        self._mqtt_client.set_on_subscribe(self._default_on_subscribe)

        # self.get_mqtt_topic()

    def _default_on_subscribe(self, client, userdata, mid, granted_qos, properties):
        logging.debug("OSH Subscribed: mid=%s granted_qos=%s", mid, granted_qos)

    def get_mqtt_topic(self, subresource: APIResourceTypes | None = None, data_topic: bool = True):
        """
        Retrieves the MQTT topic for this streamable resource based on its underlying resource type. By default,
        returns a Resource Data Topic (`:data` suffix per CS API Part 3).
        :param subresource: Optional subresource type to get the topic for, defaults to None
        :param data_topic: If True (default), produces a Resource Data Topic with ':data' suffix. Set False for
        Resource Event Topics.
        """
        resource_type = None
        parent_res_type = None
        parent_id = None

        if isinstance(self._underlying_resource, ControlStreamResource):
            parent_res_type = APIResourceTypes.CONTROL_CHANNEL
            parent_id = self._resource_id

            match subresource:
                case APIResourceTypes.COMMAND:
                    resource_type = APIResourceTypes.COMMAND
                case APIResourceTypes.STATUS:
                    resource_type = APIResourceTypes.STATUS

        elif isinstance(self._underlying_resource, DatastreamResource):
            parent_res_type = APIResourceTypes.DATASTREAM
            resource_type = APIResourceTypes.OBSERVATION
            parent_id = self._resource_id

        elif isinstance(self._underlying_resource, SystemResource):
            match subresource:
                case APIResourceTypes.DATASTREAM:
                    resource_type = APIResourceTypes.DATASTREAM
                    parent_res_type = APIResourceTypes.SYSTEM
                    parent_id = self._resource_id
                case APIResourceTypes.CONTROL_CHANNEL:
                    resource_type = APIResourceTypes.CONTROL_CHANNEL
                    parent_res_type = APIResourceTypes.SYSTEM
                    parent_id = self._resource_id
                case None:
                    resource_type = APIResourceTypes.SYSTEM
                    parent_res_type = None
                    parent_id = None
                case _:
                    raise ValueError(f"Unsupported subresource type {subresource} for SystemResource.")

        topic = self._parent_node.get_api_helper().get_mqtt_topic(subresource_type=resource_type, resource_id=parent_id,
                                                                  resource_type=parent_res_type, data_topic=data_topic)
        return topic

    def get_event_topic(self) -> str:
        """
        Returns the Resource Event Topic for this streamable resource per CS API Part 3. Event topics point to the
        resource itself (no ':data' suffix) and are used to receive CloudEvents lifecycle notifications
        (create/update/delete) published by the server.

        For Datastream/ControlStream, includes the parent system path when a parent resource ID is available.
        """
        mqtt_root = self._parent_node.get_api_helper().get_mqtt_root()

        if isinstance(self._underlying_resource, DatastreamResource):
            if self._parent_resource_id:
                return f'{mqtt_root}/systems/{self._parent_resource_id}/datastreams/{self._resource_id}'
            return f'{mqtt_root}/datastreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, ControlStreamResource):
            if self._parent_resource_id:
                return f'{mqtt_root}/systems/{self._parent_resource_id}/controlstreams/{self._resource_id}'
            return f'{mqtt_root}/controlstreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, SystemResource):
            return f'{mqtt_root}/systems/{self._resource_id}'

        raise ValueError(f"Cannot determine event topic for resource type {type(self._underlying_resource)}")

    def subscribe_events(self, callback=None, qos: int = 0) -> str:
        """
        Subscribes to the Resource Event Topic for this streamable resource. Event messages are CloudEvents v1.0
        JSON payloads published by the server when the resource is created, updated, or deleted.

        :param callback: Optional message callback. If None, uses the default handler (appends to inbound deque).
        :param qos: MQTT Quality of Service level, default 0.
        :return: The event topic string that was subscribed to.
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return ""
        event_topic = self.get_event_topic()
        cb = callback if callback is not None else self._mqtt_sub_callback
        self._mqtt_client.subscribe(event_topic, qos=qos, msg_callback=cb)
        return event_topic

    async def _read_from_ws(self, ws):
        async for msg in ws:
            self._message_handler(ws, msg)

    async def _write_to_ws(self, ws):
        while self._status is Status.STARTED.value:
            try:
                msg = self._msg_writer_queue.get_nowait()
                await ws.send_bytes(msg)
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.05)

    def stop(self):
        """Tear down the streaming process and mark the resource ``STOPPED``.

        Note: currently calls ``Process.terminate()``; cleaner shutdown
        (graceful drain, auth state preservation) is a known follow-up.
        """
        # It would be nicer to join() here once we have cleaner shutdown logic in place to avoid corrupting processes
        # that are writing to streams or that need to manage authentication state
        self._status = "stopping"
        self._process.terminate()
        self._status = "stopped"

    def set_parent_node(self, node: Node):
        """Attach this resource to the given `Node`."""
        self._parent_node = node

    def get_parent_node(self) -> Node:
        """Return the `Node` this resource is attached to."""
        return self._parent_node

    def set_parent_resource_id(self, res_id: str):
        """Set the server-side ID of the parent resource (e.g. the parent
        System for a Datastream / ControlStream)."""
        self._parent_resource_id = res_id

    def get_parent_resource_id(self) -> str:
        """Return the server-side ID of the parent resource, if set."""
        return self._parent_resource_id

    def set_connection_mode(self, connection_mode: StreamableModes):
        """Switch direction (PUSH / PULL / BIDIRECTIONAL)."""
        self._connection_mode = connection_mode

    def poll(self):
        """Poll for new data. Hook for subclass implementations; no-op here."""
        pass

    def fetch(self, time_period: TimePeriod):
        """Fetch data over a `TimePeriod`. Hook for subclass implementations; no-op here."""
        pass

    def get_msg_reader_queue(self) -> Queue:
        """
        Returns the message queue for this streamable resource. In cases where a custom message handler is used this is
        not guaranteed to return anything or provided a queue with data.
        :return: Queue object
        """
        return self._msg_reader_queue

    def get_msg_writer_queue(self) -> Queue:
        """
        Returns the message queue for writing messages to this streamable resource.
        :return: Queue object
        """
        return self._msg_writer_queue

    def get_underlying_resource(self) -> T:
        """Return the pydantic resource model (System/Datastream/ControlStream)
        that backs this streamable."""
        return self._underlying_resource

    def get_internal_id(self) -> UUID:
        """Return the local UUID. Alias for `get_streamable_id`."""
        return self._id

    def insert_data(self, data: dict):
        """ Naively inserts data into the message writer queue to be sent over the WebSocket connection.
            No Checks are performed to ensure the data is valid for the underlying resource.
            :param data: Data to be sent, typically bytes or str
        """
        print(f"Inserting data into message writer queue: {data}")
        data_bytes = json.dumps(data).encode("utf-8") if isinstance(data, dict) else data
        self._msg_writer_queue.put_nowait(data_bytes)

    def subscribe_mqtt(self, topic: str, qos: int = 0):
        """Subscribe to an arbitrary MQTT ``topic`` using the default callback
        (appends incoming payloads to ``_inbound_deque``).

        :param topic: MQTT topic string. The caller is responsible for any
            topic-prefix conventions (CS API Part 3 ``:data`` etc.).
        :param qos: MQTT QoS level. Default 0.
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return
        self._mqtt_client.subscribe(topic, qos=qos, msg_callback=self._mqtt_sub_callback)

    def _publish_mqtt(self, topic, payload):
        if self._mqtt_client is None:
            logging.warning("No MQTT client configured for streamable resource %s.", self._id)
            return
        logging.debug("Publishing to MQTT topic %s", topic)
        self._mqtt_client.publish(topic, payload, qos=0)

    async def _write_to_mqtt(self):
        while self._status == Status.STARTED.value:
            try:
                msg = self._outbound_deque.popleft()
                logging.debug("Publishing outbound message from %s", self._id)
                self._publish_mqtt(self._topic, msg)
            except IndexError:
                await asyncio.sleep(0.05)
            except Exception as e:
                logging.error("Error in Write To MQTT %s: %s\n%s", self._id, e, traceback.format_exc())
        if self._status == Status.STOPPED.value:
            logging.debug("MQTT write task stopping: resource %s stopped", self._id)

    def publish(self, payload, topic: str = None):
        """
        Publishes data to the MQTT topic associated with this streamable resource.
        :param payload: Data to be published, subclass should determine specifically allowed types
        :param topic: Specific implementation determines the topic from the provided string, if None the default topic is used
        """
        self._publish_mqtt(self._topic, payload)

    def subscribe(self, topic=None, callback=None, qos=0):
        """
        Subscribes to the MQTT topic associated with this streamable resource.
        :param topic: Specific implementation determines the topic from the provided string, if None the default topic is used
        :param callback: Optional callback function to handle incoming messages, if None the default handler is used
        :param qos: Quality of Service level for the subscription, default is 0
        """
        t = None

        if topic is None:
            t = self._topic
        else:
            raise ValueError("Invalid topic provided, must be None to use default topic.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def _mqtt_sub_callback(self, client, userdata, msg):
        logging.debug("Received MQTT message on topic %s (%s bytes)", msg.topic, len(msg.payload))
        # Appends to right of deque
        self._inbound_deque.append(msg.payload)
        self._emit_inbound_event(msg)

    def _emit_inbound_event(self, msg):
        """Hook for subclasses to publish EventHandler events on incoming MQTT messages."""
        pass

    def get_inbound_deque(self) -> deque:
        """Return the deque that receives inbound MQTT message payloads."""
        return self._inbound_deque

    def get_outbound_deque(self) -> deque:
        """Return the deque feeding outbound MQTT publishes."""
        return self._outbound_deque

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of the streamable's identity and
        connection state, for OSHConnect's persistence layer. Subclasses
        extend this with their own fields and the dumped underlying
        resource. Safely handles missing / None attributes.

        Not a CS API server-shaped payload.
        """
        topic = getattr(self, "_topic", None)
        status = getattr(self, "_status", None)
        parent_resource_id = getattr(self, "_parent_resource_id", None)
        connection_mode = getattr(self, "_connection_mode", None)
        resource_id = getattr(self, "_resource_id", None)
        if isinstance(connection_mode, Enum):
            connection_mode = connection_mode.value

        return {
            "id": str(getattr(self, "_id", None)),
            "resource_id": resource_id,
            # "canonical_link": getattr(self, "_canonical_link", None),
            "topic": topic,
            "status": status,
            "parent_resource_id": parent_resource_id,
            "connection_mode": connection_mode,
        }

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'StreamableResource':
        """Rebuild common attributes from a `to_storage_dict` payload.
        Subclasses override and call ``super()`` to wire in their own
        fields and the underlying resource.
        """
        obj = cls(node=node)
        obj._id = uuid.UUID(data["id"])
        obj._resource_id = data.get("resource_id")
        # obj._canonical_link = data.get("canonical_link")
        obj._topic = data.get("topic")
        obj._status = data.get("status")
        obj._parent_resource_id = data.get("parent_resource_id")
        obj._connection_mode = StreamableModes(data.get("connection_mode", StreamableModes.PUSH.value)),
        return obj


class System(StreamableResource[SystemResource]):
    """A sensor system on an OSH server: a logical grouping of one or more
    `Datastream` outputs and `ControlStream` inputs sharing a single URN.

    Construct directly to define a new system, or build one from a parsed
    `SystemResource` via `from_system_resource`. Use `discover_datastreams` /
    `discover_controlstreams` to populate child resources from the server,
    or `add_insert_datastream` / `add_and_insert_control_stream` to create
    new ones server-side.
    """
    name: str
    label: str
    datastreams: list[Datastream]
    control_channels: list[ControlStream]
    description: str
    urn: str
    _parent_node: Node

    def __init__(self, name: str, label: str, urn: str, parent_node: Node, **kwargs):
        """
        :param name: The machine-accessible name of the system
        :param label: The human-readable label of the system
        :param urn: The URN of the system, typically formed as such: 'urn:general_identifier:specific_identifier:more_specific_identifier'
        :param kwargs:
            - 'description': A description of the system
        """
        super().__init__(node=parent_node)
        self.name = name
        self.label = label
        self.datastreams = []
        self.control_channels = []
        self.urn = urn
        if kwargs.get('resource_id'):
            self._resource_id = kwargs['resource_id']
        if kwargs.get('description'):
            self.description = kwargs['description']

        self._underlying_resource = self.to_system_resource()

    def discover_datastreams(self) -> list[Datastream]:
        """GET ``/systems/{id}/datastreams`` and instantiate `Datastream`
        objects for every entry. New datastreams are appended to
        ``self.datastreams`` and also returned.

        For each discovered datastream we additionally fetch the SWE+JSON
        record schema (``GET /datastreams/{id}/schema?obsFormat=application/swe+json``)
        and cache it on ``_underlying_resource.record_schema``. The CS API
        listing endpoint omits the inner schema, so without this step every
        discovered datastream would be missing the schema callers need for
        observation construction or cross-node sync. A failure on a single
        datastream's schema fetch is downgraded to a warning so it doesn't
        poison the whole call.
        """
        api = self._parent_node.get_api_helper()
        res = api.get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                               APIResourceTypes.DATASTREAM)
        datastream_json = res.json()['items']
        datastreams = []

        for ds in datastream_json:
            datastream_objs = DatastreamResource.model_validate(ds, by_alias=True)
            new_ds = Datastream(self._parent_node, datastream_objs)
            try:
                schema_resp = api.get_resource(
                    APIResourceTypes.DATASTREAM, datastream_objs.ds_id,
                    APIResourceTypes.SCHEMA,
                    params={'obsFormat': 'application/swe+json'},
                )
                schema_resp.raise_for_status()
                new_ds._underlying_resource.record_schema = (
                    SWEDatastreamRecordSchema.from_swejson_dict(schema_resp.json())
                )
            except Exception as e:
                warnings.warn(
                    f"Failed to fetch SWE+JSON schema for datastream "
                    f"{datastream_objs.ds_id}: {e}",
                    stacklevel=2,
                )
            datastreams.append(new_ds)

            if not [ds.get_underlying_resource() != datastream_objs for ds in self.datastreams]:
                self.datastreams.append(new_ds)

        return datastreams

    def discover_controlstreams(self) -> list[ControlStream]:
        """GET ``/systems/{id}/controlstreams`` and instantiate `ControlStream`
        objects for every entry. New control streams are appended to
        ``self.control_channels`` and also returned.

        For each discovered control stream we additionally fetch the
        command schema (``GET /controlstreams/{id}/schema?f=json``,
        which OSH returns as ``application/json`` with a
        ``parametersSchema`` SWE Common component) and cache it on
        ``_underlying_resource.command_schema`` as a `JSONCommandSchema`.
        ``f=json`` is the OGC API standard format-selector and pins the
        response shape to the JSON variant — without it the server
        default could change. The CS API listing endpoint omits the
        inner schema, so without this step every discovered control
        stream would be missing the schema callers need for command
        construction or cross-node sync. A failure on a single control
        stream's schema fetch is downgraded to a warning so it doesn't
        poison the whole call.
        """
        api = self._parent_node.get_api_helper()
        res = api.get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                               APIResourceTypes.CONTROL_CHANNEL)
        controlstream_json = res.json()['items']
        controlstreams = []

        for cs_json in controlstream_json:
            controlstream_objs = ControlStreamResource.model_validate(cs_json)
            new_cs = ControlStream(self._parent_node, controlstream_objs)
            try:
                schema_resp = api.get_resource(
                    APIResourceTypes.CONTROL_CHANNEL, controlstream_objs.cs_id,
                    APIResourceTypes.SCHEMA,
                    params={'f': 'json'},
                )
                schema_resp.raise_for_status()
                new_cs._underlying_resource.command_schema = (
                    JSONCommandSchema.from_json_dict(schema_resp.json())
                )
            except Exception as e:
                warnings.warn(
                    f"Failed to fetch command schema for control stream "
                    f"{controlstream_objs.cs_id}: {e}",
                    stacklevel=2,
                )
            controlstreams.append(new_cs)

            if not [cs.get_underlying_resource() != controlstream_objs for cs in self.control_channels]:
                self.control_channels.append(new_cs)

        return controlstreams

    @classmethod
    def _construct_from_resource(cls, system_resource: SystemResource, parent_node: Node) -> "System":
        """Build a `System` from a parsed `SystemResource`. Internal helper
        shared by `from_csapi_dict` / `from_smljson_dict` / `from_geojson_dict`
        and the deprecated `from_system_resource`.
        """
        # exclude_none avoids triggering TimePeriod.ser_model on None-valued
        # optional time fields (it does `str(self.start)` unconditionally).
        other_props = system_resource.model_dump(exclude_none=True)
        # GeoJSON form carries name/uid under properties; SML form has
        # label/uid directly on the resource.
        if other_props.get('properties'):
            props = other_props['properties']
            new_system = cls(name=props.get('name'), label=props.get('name'), urn=props.get('uid'),
                             resource_id=system_resource.system_id, parent_node=parent_node)
        else:
            new_system = cls(name=system_resource.label, label=system_resource.label, urn=system_resource.uid,
                             resource_id=system_resource.system_id, parent_node=parent_node)

        new_system.set_system_resource(system_resource)
        return new_system

    @classmethod
    def from_resource(cls, system_resource: SystemResource, parent_node: Node) -> "System":
        """Build a `System` from an already-parsed `SystemResource`.

        Mirror of `Datastream.__init__(parent_node=, datastream_resource=)`
        and `ControlStream.__init__(node=, controlstream_resource=)` —
        provides the same "I have a parsed pydantic resource model in
        memory and want a wrapper attached to a node" entry point for
        Systems, whose constructor takes individual fields rather than a
        full resource model.

        Handles both wire shapes that round-trip through `SystemResource`:
        the GeoJSON form (with a ``properties`` block carrying
        ``name``/``uid``) and the SML form (``label``/``uid`` directly on
        the resource). Source of the resource doesn't matter — built
        locally, validated from `from_smljson_dict` / `from_geojson_dict`
        / `from_csapi_dict`, returned by some other library, etc.

        :param system_resource: A populated `SystemResource` instance.
        :param parent_node: The `Node` the new `System` will attach to.
        :return: A `System` wrapper bound to ``parent_node`` with
            ``_underlying_resource`` set to ``system_resource``.
        """
        return cls._construct_from_resource(system_resource, parent_node)

    @staticmethod
    def from_system_resource(system_resource: SystemResource, parent_node: Node) -> System:
        """Build a `System` from an already-parsed `SystemResource`.

        .. deprecated:: 0.5.1
            Use :meth:`System.from_resource` instead — same behavior, more
            consistent name with other wrappers' resource-taking factories.

        Handles both shapes the OSH server emits: the GeoJSON form (with a
        ``properties`` block carrying ``name``/``uid``) and the SML form
        (``label``/``uid`` directly on the resource).
        """
        warnings.warn("System.from_system_resource is deprecated; use System.from_resource instead "
                      "(then dump it to a dict if you need wire JSON).", DeprecationWarning, stacklevel=2, )
        return System._construct_from_resource(system_resource, parent_node)

    def to_system_resource(self) -> SystemResource:
        """Render this `System` as a `SystemResource` pydantic model
        suitable for POSTing to the server. Wrapper-specific: assembles
        attached datastreams into the resource's ``outputs`` list.
        """
        resource = SystemResource(uid=self.urn, label=self.name, feature_type='PhysicalSystem')
        if self.datastreams:
            resource.outputs = [ds.get_underlying_resource() for ds in self.datastreams]
        return resource

    def set_system_resource(self, sys_resource: SystemResource):
        """Replace the underlying `SystemResource` model."""
        self._underlying_resource = sys_resource

    def get_system_resource(self) -> SystemResource:
        """Return the underlying `SystemResource` model."""
        return self._underlying_resource

    def add_insert_datastream(self, datastream_schema: DatastreamResource):
        """Adds a datastream to the system while also inserting it into the
        system's parent node via HTTP POST.

        :param datastream_schema: DataRecordSchema to be used to define the
            datastream. Must carry a ``name`` matching NameToken
            (``^[A-Za-z][A-Za-z0-9_\\-]*$``); SWE Common 3 wraps
            DataStream.elementType in SoftNamedProperty, so the root
            component requires a name.
        :return:
        """
        print(f'Adding datastream: {datastream_schema.model_dump_json(exclude_none=True, by_alias=True)}')
        # Make the request to add the datastream
        # if successful, add the datastream to the system
        # datastream_record_schema = SWEDatastreamRecordSchema(record_schema=datastream_schema,
        #                                                      obs_format='application/swe+json', encoding=JSONEncoding())
        # datastream_resource = DatastreamResource(ds_id="default", name=datastream_schema.name,
        #                                          output_name=datastream_schema.name,
        #                                          record_schema=datastream_record_schema,
        #                                          valid_time=TimePeriod(start=TimeInstant.now_as_time_instant(),
        #                                                                end=TimeInstant(utc_time=TimeUtils.to_utc_time(
        #                                                                    "2026-12-31T00:00:00Z"))))

        api = self._parent_node.get_api_helper()
        # print(f'Attempting to create datastream: {datastream_resource.model_dump(by_alias=True, exclude_none=True)}')
        res = api.create_resource(APIResourceTypes.DATASTREAM,
                                  datastream_schema.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={'Content-Type': ContentTypes.JSON.value},
                                  parent_res_id=self._resource_id)

        if res.ok:
            datastream_id = res.headers['Location'].split('/')[-1]
            print(f'Resource Location: {datastream_id}')
            datastream_schema.ds_id = datastream_id
        else:
            raise Exception(
                f'Failed to create datastream {datastream_schema.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_ds = Datastream(self._parent_node, datastream_schema)
        new_ds.set_parent_resource_id(self._underlying_resource.system_id)
        self.datastreams.append(new_ds)
        return new_ds

    def add_insert_controlstream(self, controlstream_resource: ControlStreamResource) -> ControlStream:
        """Adds a control stream to the system while also inserting it into
        the system's parent node via HTTP POST.

        Mirrors `add_insert_datastream`: caller assembles the full
        `ControlStreamResource` (including the embedded `command_schema`)
        and this method posts it to ``/systems/{id}/controlstreams``,
        captures the new resource ID from the ``Location`` header, and
        returns a wrapped `ControlStream`.

        For the embedded `command_schema`, prefer
        `JSONCommandSchema` (`commandFormat: application/json` with a
        ``parametersSchema``). It matches what OSH returns from
        ``GET /controlstreams/{id}/schema?f=json`` (the form
        ``discover_controlstreams`` parses), keeps round-trip sync
        symmetric, and avoids the SWE+JSON ``encoding``-omission
        deviation documented in ``docs/osh_spec_deviations.md`` §1.
        `SWEJSONCommandSchema` (``application/swe+json`` with
        ``recordSchema`` plus ``encoding``) is also accepted for
        spec-strict scenarios.

        :param controlstream_resource: A fully-built
            `ControlStreamResource` carrying ``name``, ``input_name``,
            and ``command_schema``.
        :return: ControlStream object added to the system.
        """
        api = self._parent_node.get_api_helper()
        res = api.create_resource(
            APIResourceTypes.CONTROL_CHANNEL,
            controlstream_resource.model_dump_json(by_alias=True, exclude_none=True),
            req_headers={'Content-Type': ContentTypes.JSON.value},
            parent_res_id=self._resource_id,
        )

        if res.ok:
            cs_id = res.headers['Location'].split('/')[-1]
            controlstream_resource.cs_id = cs_id
        else:
            raise Exception(
                f'Failed to create control stream {controlstream_resource.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_cs = ControlStream(node=self._parent_node, controlstream_resource=controlstream_resource)
        new_cs.set_parent_resource_id(self._underlying_resource.system_id)
        self.control_channels.append(new_cs)
        return new_cs

    def add_and_insert_control_stream(self, control_stream_record_schema: DataRecordSchema, input_name: str = None,
                                      valid_time: TimePeriod = None,
                                      command_format: str = "application/json") -> ControlStream:
        """Accepts a DataRecordSchema and creates a ControlStreamResource
        with the matching command-schema variant, then POSTs it to the
        parent node.

        Per CS API Part 2 §16.x, command schemas come in two wire forms:

        - ``application/json`` → `JSONCommandSchema` carrying
          `parametersSchema` (the SWE Common component); no `encoding`.
          **This is the default.** It matches what OSH returns from
          ``GET /controlstreams/{id}/schema?f=json`` (the form
          ``discover_controlstreams`` parses), keeps round-trip sync
          symmetric, and avoids the SWE+JSON ``encoding``-omission
          deviation documented in ``docs/osh_spec_deviations.md`` §1.
        - ``application/swe+json`` → `SWEJSONCommandSchema` carrying
          `recordSchema` (the SWE Common component) and `encoding`
          (`JSONEncoding`). Spec-canonical; pass
          ``command_format='application/swe+json'`` to opt in.

        :param control_stream_record_schema: DataRecordSchema to wrap.
            Must carry a ``name`` matching NameToken
            (``^[A-Za-z][A-Za-z0-9_\\-]*$``); the schema is the root
            named component required by both command-schema variants.
        :param input_name: Name of the input. If None, the schema label
            is lowercased and whitespace-stripped.
        :param valid_time: Optional `TimePeriod`; defaults to
            ``[now, now + 1 year]``.
        :param command_format: ``"application/json"`` (default) or
            ``"application/swe+json"``. Anything else raises
            ``ValueError``.
        :return: ControlStream object added to the system.
        """
        input_name_checked = input_name if input_name is not None else control_stream_record_schema.label.lower().replace(
            ' ', '')

        now = datetime.datetime.now()
        future_time = now.replace(year=now.year + 1)
        future_str = future_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        valid_time_checked = valid_time if valid_time else TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                      end=TimeInstant(
                                                                          utc_time=TimeUtils.to_utc_time(future_str)))

        if command_format == "application/swe+json":
            command_schema = SWEJSONCommandSchema(
                command_format="application/swe+json",
                record_schema=control_stream_record_schema,
                encoding=JSONEncoding(),
            )
        elif command_format == "application/json":
            command_schema = JSONCommandSchema(
                command_format="application/json",
                params_schema=control_stream_record_schema,
            )
        else:
            raise ValueError(
                f"Unsupported command_format: {command_format!r}. "
                f"Expected 'application/swe+json' or 'application/json'."
            )

        control_stream_resource = ControlStreamResource(name=control_stream_record_schema.label,
                                                        input_name=input_name_checked, command_schema=command_schema,
                                                        validTime=valid_time_checked)
        api = self._parent_node.get_api_helper()
        res = api.create_resource(APIResourceTypes.CONTROL_CHANNEL,
                                  control_stream_resource.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={'Content-Type': 'application/json'}, parent_res_id=self._resource_id)

        if res.ok:
            control_channel_id = res.headers['Location'].split('/')[-1]
            control_stream_resource.cs_id = control_channel_id
        else:
            raise Exception(
                f'Failed to create control stream {control_stream_resource.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_cs = ControlStream(node=self._parent_node, controlstream_resource=control_stream_resource)
        new_cs.set_parent_resource_id(self._underlying_resource.system_id)
        self.control_channels.append(new_cs)
        return new_cs

    def insert_self(self):
        """POST this system to the server (Content-Type
        ``application/sml+json``) and capture the new resource ID from
        the ``Location`` response header.
        """
        res = self._parent_node.get_api_helper().create_resource(
            APIResourceTypes.SYSTEM,
            self.to_system_resource().model_dump_json(by_alias=True, exclude_none=True),
            req_headers={'Content-Type': 'application/sml+json'})

        if res.ok:
            location = res.headers['Location']
            sys_id = location.split('/')[-1]
            self._resource_id = sys_id
            print(f'Created system: {self._resource_id}')

    def retrieve_resource(self):
        """GET ``/systems/{id}`` and refresh the underlying `SystemResource`.
        Returns ``None`` either way (kept for API symmetry).
        """
        if self._resource_id is None:
            return None
        res = self._parent_node.get_api_helper().retrieve_resource(res_type=APIResourceTypes.SYSTEM,
                                                                   res_id=self._resource_id)
        if res.ok:
            system_json = res.json()
            print(system_json)
            system_resource = SystemResource.model_validate(system_json)
            print(f'System Resource: {system_resource}')
            self._underlying_resource = system_resource
            return None

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this system, its child datastreams /
        control streams, and the dumped underlying `SystemResource`, for
        OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API system shape.
        """
        data = super().to_storage_dict()
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
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'System':
        """Build a `System` from a dict produced by `to_storage_dict`.

        Expects ``name``, ``label``, ``urn``, optional ``description`` /
        ``resource_id``, and optional ``datastreams`` / ``control_channels``
        / ``underlying_resource`` blocks. The embedded
        ``underlying_resource`` is parsed via `SystemResource.model_validate`,
        so that nested block can also be a CS API server response body.

        :param data: Source dict.
        :param node: Parent `Node` the rebuilt system attaches to.
        """
        obj = cls(
            name=data["name"], label=data["label"], urn=data["urn"], parent_node=node,
            description=data.get("description"), resource_id=data.get("resource_id"))
        obj._id = uuid.UUID(data["id"])
        obj.datastreams = [Datastream.from_storage_dict(ds, node) for ds in data.get("datastreams", [])]
        obj.control_channels = [ControlStream.from_storage_dict(cc, node) for cc in data.get("control_channels", [])]
        underlying = data.get("underlying_resource")
        obj._underlying_resource = SystemResource.model_validate(underlying) if underlying else None
        return obj


class Datastream(StreamableResource[DatastreamResource]):
    """An output channel of a `System`: produces observations.

    Created from a parsed `DatastreamResource` (typically returned by
    `System.discover_datastreams`) or built locally and inserted via
    `System.add_insert_datastream`. Subscribes to its observation MQTT
    topic when started.

    :param parent_node: The `Node` this datastream lives under.
    :param datastream_resource: The pydantic `DatastreamResource` model.
    """
    should_poll: bool

    def __init__(self, parent_node: Node = None, datastream_resource: DatastreamResource = None):
        super().__init__(node=parent_node)
        self._underlying_resource = datastream_resource
        self._resource_id = datastream_resource.ds_id

    def get_id(self) -> str:
        """Return the server-side datastream ID."""
        return self._underlying_resource.ds_id

    @staticmethod
    def from_resource(ds_resource: DatastreamResource, parent_node: Node) -> 'Datastream':
        """Build a `Datastream` from an already-parsed `DatastreamResource`.

        .. deprecated:: 0.5.1
            Use the constructor directly instead:
            ``Datastream(parent_node=node, datastream_resource=ds_resource)``.
            For raw JSON, parse first via ``DatastreamResource.from_csapi_dict(data)``.
        """
        warnings.warn(
            "Datastream.from_resource is deprecated; pass datastream_resource directly "
            "to the constructor: Datastream(parent_node=node, datastream_resource=res). "
            "For raw JSON, parse via DatastreamResource.from_csapi_dict(data) first.",
            DeprecationWarning, stacklevel=2,
        )
        return Datastream(parent_node=parent_node, datastream_resource=ds_resource)

    def set_resource(self, resource: DatastreamResource):
        """Replace the underlying `DatastreamResource` model."""
        self._underlying_resource = resource

    def get_resource(self) -> DatastreamResource:
        """Return the underlying `DatastreamResource` model."""
        return self._underlying_resource

    def create_observation(self, obs_data: dict) -> ObservationResource:
        """Build an `ObservationResource` from a result dict, validating
        against this datastream's record schema if one is set.

        Does NOT insert the observation server-side — pair with
        `insert_observation_dict` if you want to POST it.
        """
        obs = ObservationResource(result=obs_data, result_time=TimeInstant.now_as_time_instant())
        # Validate against the schema
        if self._underlying_resource.record_schema is not None:
            obs.validate_against_schema(self._underlying_resource.record_schema)
        return obs

    def insert_observation_dict(self, obs_data: dict):
        """POST an observation dict to ``/datastreams/{id}/observations``.

        :raises Exception: if the server returns a non-OK response.
        """
        res = self._parent_node.get_api_helper().create_resource(APIResourceTypes.OBSERVATION, obs_data,
                                                                 parent_res_id=self._resource_id,
                                                                 req_headers={'Content-Type': 'application/json'})
        if res.ok:
            obs_id = res.headers['Location'].split('/')[-1]
            print(f'Inserted observation: {obs_id}')
            return id
        else:
            raise Exception(f'Failed to insert observation: {res.text}')

    def start(self):
        """Start the datastream. PULL/BIDIRECTIONAL subscribes to the
        observation topic; PUSH spawns the async MQTT write loop. Requires
        an active asyncio event loop for PUSH mode.
        """
        super().start()
        if self._mqtt_client is not None:
            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
                self._mqtt_client.subscribe(self._topic, msg_callback=self._mqtt_sub_callback)
            else:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._write_to_mqtt())
                except RuntimeError:
                    logging.warning("No running event loop — MQTT write task for %s not started. "
                                    "Call start() from within an async context.", self._id)
                except Exception as e:
                    logging.error("Error starting MQTT write task for %s: %s\n%s", self._id, e, traceback.format_exc())

    def init_mqtt(self):
        """Set ``self._topic`` to the datastream's observation data topic
        (CS API Part 3 ``:data`` suffix)."""
        super().init_mqtt()
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)

    def _emit_inbound_event(self, msg):
        evt = (EventBuilder().with_type(DefaultEventTypes.NEW_OBSERVATION).with_topic(msg.topic).with_data(
            msg.payload).with_producer(self).build())
        EventHandler().publish(evt)

    def _queue_push(self, msg):
        print(f'Pushing message to reader queue: {msg}')
        self._msg_writer_queue.put_nowait(msg)
        print(f'Queue size is now: {self._msg_writer_queue.qsize()}')

    def _queue_pop(self):
        return self._msg_reader_queue.get_nowait()

    def insert(self, data: dict):
        """Encode ``data`` as JSON and publish it to this datastream's
        observation MQTT topic. Bypasses the outbound deque."""
        # self._queue_push(data)
        encoded = json.dumps(data).encode('utf-8')
        self._publish_mqtt(self._topic, encoded)

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this datastream — local identity,
        connection state, polling flag, and the dumped underlying
        `DatastreamResource` — for OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API datastream shape.
        """
        data = super().to_storage_dict()
        data["should_poll"] = getattr(self, "should_poll", None)
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

        return data

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'Datastream':
        """Build a `Datastream` from a dict produced by `to_storage_dict`.
        The embedded ``underlying_resource`` is parsed via
        `DatastreamResource.model_validate`, so that nested block can also
        be a CS API server response body for the datastream.
        """
        ds_resource = DatastreamResource.model_validate(data["underlying_resource"]) if data.get(
            "underlying_resource") else None
        obj = cls(parent_node=node, datastream_resource=ds_resource)
        obj._id = uuid.UUID(data["id"])
        obj.should_poll = data.get("should_poll", False)
        return obj

    def subscribe(self, topic=None, callback=None, qos=0):
        """Subscribe to this datastream's observation MQTT topic.

        :param topic: ``None`` or ``"observation"`` — both resolve to the
            datastream's data topic. Any other string raises.
        :param callback: Override the default callback (which appends
            payloads to ``_inbound_deque``).
        :param qos: MQTT QoS level. Default 0.
        :raises ValueError: if ``topic`` is anything other than None /
            ``"observation"``.
        """
        t = None

        if topic is None or topic == APIResourceTypes.OBSERVATION.value:
            t = self._topic
        # elif topic == APIResourceTypes.STATUS.value:
        #     t = self._status_topic
        else:
            raise ValueError(f"Invalid topic provided {topic}, must be None or 'observation'.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)


class ControlStream(StreamableResource[ControlStreamResource]):
    """An input channel of a `System`: accepts commands and emits status.

    Unlike `Datastream`, a control stream has TWO MQTT topics — one for
    commands (``self._topic``) and one for status updates
    (``self._status_topic``) — and two pairs of inbound/outbound deques to
    match. Construct from a parsed `ControlStreamResource` (typically from
    `System.discover_controlstreams`) or build locally and insert via
    `System.add_and_insert_control_stream`.

    :param node: The `Node` this control stream lives under.
    :param controlstream_resource: The pydantic `ControlStreamResource`
        model that backs this stream.
    """
    _status_topic: str
    _inbound_status_deque: deque
    _outbound_status_deque: deque

    def __init__(self, node: Node = None, controlstream_resource: ControlStreamResource = None):
        super().__init__(node=node)
        self._underlying_resource = controlstream_resource
        self._inbound_status_deque = deque()
        self._outbound_status_deque = deque()
        self._resource_id = controlstream_resource.cs_id
        # Always make sure this is set after the resource ids are set
        self._status_topic = self.get_mqtt_status_topic()

    def add_underlying_resource(self, resource: ControlStreamResource):
        """Replace the underlying `ControlStreamResource` model."""
        self._underlying_resource = resource

    def get_id(self) -> str:
        """Return the server-side control-stream ID."""
        return self._underlying_resource.cs_id

    def init_mqtt(self):
        """Set ``self._topic`` to the control stream's command data topic."""
        super().init_mqtt()
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)

    def get_mqtt_status_topic(self) -> str:
        """Return the MQTT topic for command status updates (``:status``)."""
        return self.get_mqtt_topic(subresource=APIResourceTypes.STATUS, data_topic=True)

    def _emit_inbound_event(self, msg):
        evt_type = (DefaultEventTypes.NEW_COMMAND if msg.topic == self._topic else DefaultEventTypes.NEW_COMMAND_STATUS)
        evt = (
            EventBuilder().with_type(evt_type).with_topic(msg.topic).with_data(msg.payload).with_producer(self).build())
        EventHandler().publish(evt)

    def start(self):
        """Start the control stream. PULL/BIDIRECTIONAL subscribes to the
        command topic; PUSH spawns the async MQTT write loop. Requires
        an active asyncio event loop for PUSH mode.
        """
        super().start()
        if self._mqtt_client is not None:
            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
                # Subs to command topic by default
                self._mqtt_client.subscribe(self._topic, msg_callback=self._mqtt_sub_callback)
            else:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._write_to_mqtt())
                except RuntimeError:
                    logging.warning("No running event loop — MQTT write task for %s not started. "
                                    "Call start() from within an async context.", self._id)
                except Exception as e:
                    logging.error("Error starting MQTT write task for %s: %s\n%s", self._id, e, traceback.format_exc())

    def get_inbound_deque(self) -> deque:
        """Return the deque receiving inbound command payloads."""
        return self._inbound_deque

    def get_outbound_deque(self) -> deque:
        """Return the deque feeding outbound command publishes."""
        return self._outbound_deque

    def get_status_deque_inbound(self) -> deque:
        """Return the deque receiving inbound status updates."""
        return self._inbound_status_deque

    def get_status_deque_outbound(self) -> deque:
        """Return the deque feeding outbound status publishes."""
        return self._outbound_status_deque

    def publish_command(self, payload):
        """Publish ``payload`` to the command MQTT topic. Convenience wrapper for ``publish(payload, 'command')``."""
        self.publish(payload, topic=APIResourceTypes.COMMAND.value)

    def publish_status(self, payload):
        """Publish ``payload`` to the status MQTT topic. Convenience wrapper for ``publish(payload, 'status')``."""
        self.publish(payload, topic=APIResourceTypes.STATUS.value)

    def publish(self, payload, topic: str = 'command'):
        """
        Publishes data to the MQTT topic associated with this control stream resource.
        :param payload: Data to be published, subclass should determine specifically allowed types
        :param topic: Specific implementation determines the topic from the provided string
        """

        if topic == APIResourceTypes.COMMAND.value:
            self._publish_mqtt(self._topic, payload)
        elif topic == APIResourceTypes.STATUS.value:
            self._publish_mqtt(self._status_topic, payload)
        else:
            raise ValueError(f"Unsupported topic type {topic} for ControlStream publish().")

    def subscribe(self, topic=None, callback=None, qos=0):
        """
        Subscribes to the MQTT topic associated with this control stream resource.
        :param topic: Specific implementation determines the topic from the provided string
        :param callback: Optional callback function to handle incoming messages, if None the default handler is used
        :param qos: Quality of Service level for the subscription, default is 0
        """

        t = None

        if topic is None or topic == APIResourceTypes.COMMAND.value:
            t = self._topic
        elif topic == APIResourceTypes.STATUS.value:
            t = self._status_topic
        else:
            raise ValueError(f"Invalid topic provided {topic}, must be None or one of 'command' or 'status'.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this control stream — local
        identity, connection state, status topic, and the dumped underlying
        `ControlStreamResource` — for OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API control-stream
        shape.
        """
        data = super().to_storage_dict()
        data["status_topic"] = getattr(self, "_status_topic", None)
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

        return data

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'ControlStream':
        """Build a `ControlStream` from a dict produced by `to_storage_dict`.
        The embedded ``underlying_resource`` is parsed via
        `ControlStreamResource.model_validate`, so that nested block can
        also be a CS API server response body for the control stream.
        """
        cs_resource = ControlStreamResource.model_validate(data["underlying_resource"]) if data.get(
            "underlying_resource") else None
        obj = cls(node=node, controlstream_resource=cs_resource)
        obj._id = uuid.UUID(data["id"])
        obj._status_topic = data.get("status_topic")
        return obj
