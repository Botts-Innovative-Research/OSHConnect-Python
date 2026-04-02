#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/29
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import asyncio
import base64
import datetime
import json
import logging
import traceback
import uuid
from abc import ABC
from argparse import ArgumentError
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import TypeVar, Generic, Union
from uuid import UUID, uuid4
from collections import deque

from pydantic.v1.utils import to_lower_camel

from .csapi4py.constants import ContentTypes
from .schema_datamodels import JSONCommandSchema
from .csapi4py.mqtt import MQTTCommClient
from .csapi4py.constants import APIResourceTypes, ObservationFormat
from .csapi4py.default_api_helpers import APIHelper
from .encoding import JSONEncoding
from .resource_datamodels import ControlStreamResource
from .resource_datamodels import DatastreamResource, ObservationResource
from .resource_datamodels import SystemResource
from .schema_datamodels import SWEDatastreamRecordSchema
from .swe_components import DataRecordSchema
from .timemanagement import TimeInstant, TimePeriod, TimeUtils


@dataclass(kw_only=True)
class Endpoints:
    root: str = "sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


class Utilities:

    @staticmethod
    def convert_auth_to_base64(username: str, password: str) -> str:
        return base64.b64encode(f"{username}:{password}".encode()).decode()


class OSHClientSession:
    verify_ssl = True
    _streamables: dict[str, 'StreamableResource'] = None

    def __init__(self, base_url, *args, verify_ssl=True, **kwargs):
        # super().__init__(base_url, *args, **kwargs)
        self.verify_ssl = verify_ssl
        self._streamables = {}

    def connect_streamables(self):
        for streamable in self._streamables.values():
            streamable.start()

    def close_streamables(self):
        for streamable in self._streamables.values():
            streamable.stop()

    def register_streamable(self, streamable: StreamableResource):
        if self._streamables is None:
            self._streamables = {}
        self._streamables[streamable.get_streamable_id_str()] = streamable


class SessionManager:
    _session_tokens = None
    sessions: dict[str, OSHClientSession] = None

    def __init__(self, session_tokens: dict[str, str] = None):
        self._session_tokens = session_tokens
        self.sessions = {}

    def register_session(self, session_id, session: OSHClientSession) -> OSHClientSession:
        self.sessions[session_id] = session
        return session

    def unregister_session(self, session_id):
        session = self.sessions.pop(session_id)
        session.close()

    def get_session(self, session_id):
        return self.sessions.get(session_id, None)

    def start_session_streams(self, session_id):
        session = self.get_session(session_id)
        if session is None:
            raise ValueError(f"No session found for ID {session_id}")
        session.connect_streamables()

    def start_all_streams(self):
        for session in self.sessions.values():
            session.connect_streamables()


@dataclass(kw_only=True)
class Node:
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

    def __init__(self, protocol: str, address: str, port: int,
                 username: str = None, password: str = None, server_root: str = 'sensorhub',
                 session_manager: SessionManager = None,
                 **kwargs):
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
            server_url=self.address,
            protocol=self.protocol,
            port=self.port,
            server_root=self.server_root,
            api_root='api', username=username,
            password=password)
        if self.is_secure:
            self._api_helper.user_auth = True
        self._systems = []
        if session_manager is not None:
            session_task = self.register_with_session_manager(session_manager)
            asyncio.gather(session_task)

        if kwargs.get('enable_mqtt'):
            if kwargs.get('mqtt_port') is not None:
                self._mqtt_port = kwargs.get('mqtt_port')
            self._mqtt_client = MQTTCommClient(url=self.address, port=self._mqtt_port,
                                               client_id_suffix=uuid.uuid4().hex, )
            self._mqtt_client.connect()
            self._mqtt_client.start()

    def get_id(self):
        return self._id

    def get_address(self):
        return self.address

    def get_port(self):
        return self.port

    def get_api_endpoint(self):
        return self._api_helper.get_api_root_url()

    def add_basicauth(self, username: str, password: str):
        if not self.is_secure:
            self.is_secure = True
        self._basic_auth = base64.b64encode(
            f"{username}:{password}".encode('utf-8'))

    def get_decoded_auth(self):
        return self._basic_auth.decode('utf-8')

    # def get_basicauth(self):
    #     return BasicAuth(self._api_helper.username, self._api_helper.password)

    def get_mqtt_client(self) -> MQTTCommClient:
        return self._mqtt_client

    def discover_systems(self):
        result = self._api_helper.retrieve_resource(APIResourceTypes.SYSTEM,
                                                    req_headers={})
        if result.ok:
            new_systems = []
            system_objs = result.json()['items']
            print(system_objs)
            for system_json in system_objs:
                print(system_json)
                system = SystemResource.model_validate(system_json, by_alias=True)
                sys_obj = System(label=system.properties['name'],
                                 name=to_lower_camel(system.properties['name'].replace(" ", "_")),
                                 urn=system.properties['uid'], parent_node=self, resource_id=system.system_id)

                self._systems.append(sys_obj)
                new_systems.append(sys_obj)
            return new_systems
        else:
            return None

    def add_new_system(self, system: System):
        system.set_parent_node(self)
        self._systems.append(system)

    def get_api_helper(self) -> APIHelper:
        return self._api_helper

    # System Management

    def add_system(self, system: System, insert_resource: bool = False):
        """
        Add a system to the target node.
        :param system: System object
        :param target_node: Node object
        :param insert_resource: Whether to insert the system into the target node's server, default is False
        :return:
        """
        if insert_resource:
            system.insert_self()
        self.add_new_system(system)
        self._systems.append(system)
        return system

    def systems(self) -> list[System]:
        return self._systems

    def register_with_session_manager(self, session_manager: SessionManager):
        """
        Registers this node with the provided session manager, creating a new client session.
        :param session_manager: SessionManager instance
        """
        self._client_session = session_manager.register_session(self._id, OSHClientSession(
            base_url=self._api_helper.get_base_url()))

    def register_streamable(self, streamable: StreamableResource):
        if self._client_session is None:
            raise ValueError("Node is not registered with a SessionManager.")
        self._client_session.register_streamable(streamable)

    def get_session(self) -> OSHClientSession:
        return self._client_session

    def serialize(self) -> dict:
        data = {
            "_id": self._id,
            "protocol": self.protocol,
            "address": self.address,
            "port": self.port,
            "server_root": self.server_root,
            "is_secure": self.is_secure,
            "username": getattr(self._api_helper, "username", None),
            "password": getattr(self._api_helper, "password", None),
            "_systems": [system.serialize() for system in self._systems] if self._systems is not None else None,
        }
        data["name"] = getattr(self, "name", None)
        data["label"] = getattr(self, "label", None)
        data["urn"] = getattr(self, "urn", None)
        data["description"] = getattr(self, "description", None)
        datastreams = getattr(self, "datastreams", None)
        if datastreams is not None:
            data["datastreams"] = [ds.serialize() for ds in datastreams]
        else:
            data["datastreams"] = None
        control_channels = getattr(self, "control_channels", None)
        if control_channels is not None:
            data["control_channels"] = [cc.serialize() for cc in control_channels]
        else:
            data["control_channels"] = None
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True)
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
    def deserialize(cls, data: dict, session_manager: 'SessionManager' = None) -> 'Node':
        node = cls(
            protocol=data["protocol"],
            address=data["address"],
            port=data["port"],
            username=data.get("username"),
            password=data.get("password"),
            server_root=data.get("server_root", "sensorhub"),
            session_manager=session_manager
        )
        node._id = data["_id"]
        node.is_secure = data.get("is_secure", False)
        node._systems = [System.deserialize(sys, node) for sys in data.get("_systems", [])] if data.get(
            "_systems") is not None else []
        return node


class Status(Enum):
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    STOPPED = "stopped"


class StreamableModes(Enum):
    PUSH = "push"
    PULL = "pull"
    BIDIRECTIONAL = "bidirectional"


T = TypeVar('T', SystemResource, DatastreamResource, ControlStreamResource)


class StreamableResource(Generic[T], ABC):
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

    def get_streamable_id(self) -> UUID:
        return self._id

    def get_streamable_id_str(self) -> str:
        return self._id.hex

    def initialize(self):
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
                                                                       resource_id=res_id,
                                                                       subresource_id=None)
        self._msg_reader_queue = asyncio.Queue()
        self._msg_writer_queue = asyncio.Queue()
        self.init_mqtt()
        self._status = Status.INITIALIZED.value

    def start(self):
        if self._status != Status.INITIALIZED.value:
            logging.warning(f"Streamable resource {self._id} not initialized. Call initialize() first.")
            return
        self._status = Status.STARTING.value
        self._status = Status.STARTED.value

    async def stream(self):
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
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return

        self._mqtt_client.set_on_subscribe(self._default_on_subscribe)

        # self.get_mqtt_topic()

    def _default_on_subscribe(self, client, userdata, mid, granted_qos, properties):
        print("OSH Subscribed: " + str(mid) + " " + str(granted_qos))

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

        topic = self._parent_node.get_api_helper().get_mqtt_topic(subresource_type=resource_type,
                                                                  resource_id=parent_id,
                                                                  resource_type=parent_res_type,
                                                                  data_topic=data_topic)
        return topic

    def get_event_topic(self) -> str:
        """
        Returns the Resource Event Topic for this streamable resource per CS API Part 3. Event topics point to the
        resource itself (no ':data' suffix) and are used to receive CloudEvents lifecycle notifications
        (create/update/delete) published by the server.

        For Datastream/ControlStream, includes the parent system path when a parent resource ID is available.
        """
        api_root = self._parent_node.get_api_helper().api_root

        if isinstance(self._underlying_resource, DatastreamResource):
            if self._parent_resource_id:
                return f'/{api_root}/systems/{self._parent_resource_id}/datastreams/{self._resource_id}'
            return f'/{api_root}/datastreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, ControlStreamResource):
            if self._parent_resource_id:
                return f'/{api_root}/systems/{self._parent_resource_id}/controlstreams/{self._resource_id}'
            return f'/{api_root}/controlstreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, SystemResource):
            return f'/{api_root}/systems/{self._resource_id}'

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
        # It would be nicer to join() here once we have cleaner shutdown logic in place to avoid corrupting processes
        # that are writing to streams or that need to manage authentication state
        self._status = "stopping"
        self._process.terminate()
        self._status = "stopped"

    def set_parent_node(self, node: Node):
        self._parent_node = node

    def get_parent_node(self) -> Node:
        return self._parent_node

    def set_parent_resource_id(self, res_id: str):
        self._parent_resource_id = res_id

    def get_parent_resource_id(self) -> str:
        return self._parent_resource_id

    def set_connection_mode(self, connection_mode: StreamableModes):
        self._connection_mode = connection_mode

    def poll(self):
        pass

    def fetch(self, time_period: TimePeriod):
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
        return self._underlying_resource

    def get_internal_id(self) -> UUID:
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
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return
        self._mqtt_client.subscribe(topic, qos=qos, msg_callback=self._mqtt_sub_callback)

    def _publish_mqtt(self, topic, payload):
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return
        print(f'Publishing to MQTT topic {topic}: {payload}')
        self._mqtt_client.publish(topic, payload, qos=0)

    async def _write_to_mqtt(self):
        while self._status is Status.STARTED.value:
            try:
                msg = self._outbound_deque.popleft()
                print(f"Popped message: {msg}, attempting to publish...")
                self._publish_mqtt(self._topic, msg)
            except IndexError:
                await asyncio.sleep(0.05)
            except Exception as e:
                print(f"Error in Write To MQTT {self._id}: {e}")
                print(traceback.format_exc())
        if self._status is Status.STOPPED.value:
            print("MQTT write task stopping as streamable resource is stopped.")

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
            raise ArgumentError("Invalid topic provided, must be None to use default topic.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def _mqtt_sub_callback(self, client, userdata, msg):
        print(f"Received MQTT message on topic {msg.topic}: {msg.payload}")
        # Appends to right of deque
        self._inbound_deque.append(msg.payload)

    def get_inbound_deque(self):
        return self._inbound_deque

    def get_outbound_deque(self):
        return self._outbound_deque

    def serialize(self) -> dict:
        """Serializes common attributes of StreamableResource, safely handling missing/None attributes."""
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
    def deserialize(cls, data: dict, node: 'Node') -> 'StreamableResource':
        """Deserializes common attributes. Subclasses should override and call super()."""
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
        res = self._parent_node.get_api_helper().get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                                                              APIResourceTypes.DATASTREAM)
        datastream_json = res.json()['items']
        datastreams = []

        for ds in datastream_json:
            datastream_objs = DatastreamResource.model_validate(ds, by_alias=True)
            new_ds = Datastream(self._parent_node, datastream_objs)
            datastreams.append(new_ds)

            if not [ds.get_underlying_resource() != datastream_objs for ds in self.datastreams]:
                self.datastreams.append(new_ds)

        return datastreams

    def discover_controlstreams(self) -> list[ControlStream]:
        res = self._parent_node.get_api_helper().get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                                                              APIResourceTypes.CONTROL_CHANNEL)
        controlstream_json = res.json()['items']
        controlstreams = []

        for cs_json in controlstream_json:
            controlstream_objs = ControlStreamResource.model_validate(cs_json)
            new_cs = ControlStream(self._parent_node, controlstream_objs)
            controlstreams.append(new_cs)

            if not [cs.get_underlying_resource() != controlstream_objs for cs in self.control_channels]:
                self.control_channels.append(new_cs)

        return controlstreams

    @staticmethod
    def from_system_resource(system_resource: SystemResource, parent_node: Node) -> System:
        other_props = system_resource.model_dump()
        print(f'Props of SystemResource: {other_props}')

        # case 1: has properties a la geojson
        if 'properties' in other_props:
            new_system = System(name=other_props['properties']['name'],
                                label=other_props['properties']['name'],
                                urn=other_props['properties']['uid'],
                                resource_id=system_resource.system_id, parent_node=parent_node)
        else:
            new_system = System(name=system_resource.name,
                                label=system_resource.label, urn=system_resource.urn,
                                resource_id=system_resource.system_id, parent_node=parent_node)

        new_system.set_system_resource(system_resource)
        return new_system

    def to_system_resource(self) -> SystemResource:
        resource = SystemResource(uid=self.urn, label=self.name, feature_type='PhysicalSystem')

        if len(self.datastreams) > 0:
            resource.outputs = [ds.get_underlying_resource() for ds in self.datastreams]

        # if len(self.control_channels) > 0:
        #     resource.inputs = [cc.to_resource() for cc in self.control_channels]
        return resource

    def set_system_resource(self, sys_resource: SystemResource):
        self._underlying_resource = sys_resource

    def get_system_resource(self) -> SystemResource:
        return self._underlying_resource

    def add_insert_datastream(self, datarecord_schema: DataRecordSchema):
        """
        Adds a datastream to the system while also inserting it into the system's parent node via HTTP POST.
        :param datarecord_schema: DataRecordSchema to be used to define the datastream
        :return:
        """
        print(f'Adding datastream: {datarecord_schema.model_dump_json(exclude_none=True, by_alias=True)}')
        # Make the request to add the datastream
        # if successful, add the datastream to the system
        datastream_schema = SWEDatastreamRecordSchema(record_schema=datarecord_schema,
                                                      obs_format='application/swe+json',
                                                      encoding=JSONEncoding())
        datastream_resource = DatastreamResource(ds_id="default", name=datarecord_schema.label,
                                                 output_name=datarecord_schema.label,
                                                 record_schema=datastream_schema,
                                                 valid_time=TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                       end=TimeInstant(utc_time=TimeUtils.to_utc_time(
                                                                           "2026-12-31T00:00:00Z"))))

        api = self._parent_node.get_api_helper()
        print(
            f'Attempting to create datastream: {datastream_resource.model_dump(by_alias=True, exclude_none=True)}')
        res = api.create_resource(APIResourceTypes.DATASTREAM,
                                  datastream_resource.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={
                                      'Content-Type': ContentTypes.JSON.value
                                  }, parent_res_id=self._resource_id)

        if res.ok:
            datastream_id = res.headers['Location'].split('/')[-1]
            print(f'Resource Location: {datastream_id}')
            datastream_resource.ds_id = datastream_id
        else:
            raise Exception(f'Failed to create datastream: {datastream_resource.name}')

        new_ds = Datastream(self._parent_node, datastream_resource)
        new_ds.set_parent_resource_id(self._underlying_resource.system_id)
        self.datastreams.append(new_ds)
        return new_ds

    def add_and_insert_control_stream(self, control_stream_record_schema: DataRecordSchema, input_name: str = None,
                                      valid_time: TimePeriod = None) -> ControlStream:
        """
        Accepts a DataRecordSchema and creates a JSON encoded schema structure ControlStreamResource, which is inserted
        into the parent system via the host node.
        :param control_stream_record_schema: DataRecordSchema to be used for the control stream
        :param input_name: Name of the input, if None the label of the schema is converted to lower and stripped of whitespace
        :return: ControlStream object added to the system
        """
        input_name_checked = input_name if input_name is not None else control_stream_record_schema.label.lower().replace(
            ' ', '')

        now = datetime.datetime.now()
        future_time = now.replace(year=now.year + 1)
        future_str = future_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        valid_time_checked = valid_time if valid_time else TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                      end=TimeInstant(
                                                                          utc_time=TimeUtils.to_utc_time(future_str)))

        command_schema = JSONCommandSchema(command_format=ObservationFormat.SWE_JSON.value,
                                           params_schema=control_stream_record_schema)
        control_stream_resource = ControlStreamResource(name=control_stream_record_schema.label,
                                                        input_name=input_name_checked,
                                                        command_schema=command_schema,
                                                        validTime=valid_time_checked)
        api = self._parent_node.get_api_helper()
        res = api.create_resource(APIResourceTypes.CONTROL_CHANNEL,
                                  control_stream_resource.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={
                                      'Content-Type': 'application/json'
                                  }, parent_res_id=self._resource_id)

        if res.ok:
            control_channel_id = res.headers['Location'].split('/')[-1]
            print(f'Control Stream Resource Location: {control_channel_id}')
            control_stream_resource.cs_id = control_channel_id
        else:
            raise Exception(f'Failed to create control stream: {control_stream_resource.name}')

        new_cs = ControlStream(node=self._parent_node, controlstream_resource=control_stream_resource)
        new_cs.set_parent_resource_id(self._underlying_resource.system_id)
        self.control_channels.append(new_cs)
        return new_cs

    def insert_self(self):
        res = self._parent_node.get_api_helper().create_resource(
            APIResourceTypes.SYSTEM, self.to_system_resource().model_dump_json(by_alias=True, exclude_none=True),
            req_headers={
                'Content-Type': 'application/sml+json'
            })

        if res.ok:
            location = res.headers['Location']
            sys_id = location.split('/')[-1]
            self._resource_id = sys_id
            print(f'Created system: {self._resource_id}')

    def retrieve_resource(self):
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

    def serialize(self) -> dict:
        data = super().serialize()
        data["name"] = getattr(self, "name", None)
        data["label"] = getattr(self, "label", None)
        data["urn"] = getattr(self, "urn", None)
        data["description"] = getattr(self, "description", None)
        datastreams = getattr(self, "datastreams", None)
        if datastreams is not None:
            data["datastreams"] = [ds.serialize() for ds in datastreams]
        else:
            data["datastreams"] = None
        control_channels = getattr(self, "control_channels", None)
        if control_channels is not None:
            data["control_channels"] = [cc.serialize() for cc in control_channels]
        else:
            data["control_channels"] = None
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True)
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
    def deserialize(cls, data: dict, node: 'Node') -> 'System':
        obj = cls(
            name=data["name"],
            label=data["label"],
            urn=data["urn"],
            parent_node=node,
            description=data.get("description"),
            resource_id=data.get("resource_id")
        )
        obj._id = uuid.UUID(data["id"])
        obj.datastreams = [Datastream.deserialize(ds, node) for ds in data.get("datastreams", [])]
        obj.control_channels = [ControlStream.deserialize(cc, node) for cc in data.get("control_channels", [])]
        obj._underlying_resource = SystemResource.model_validate(data.get("_underlying_resource"))
        return obj


class Datastream(StreamableResource[DatastreamResource]):
    should_poll: bool

    def __init__(self, parent_node: Node = None, datastream_resource: DatastreamResource = None):
        super().__init__(node=parent_node)
        self._underlying_resource = datastream_resource
        self._resource_id = datastream_resource.ds_id

    def get_id(self):
        return self._underlying_resource.ds_id

    @staticmethod
    def from_resource(ds_resource: DatastreamResource, parent_node: Node):
        new_ds = Datastream(parent_node=parent_node, datastream_resource=ds_resource)
        return new_ds

    def set_resource(self, resource: DatastreamResource):
        self._underlying_resource = resource

    def get_resource(self) -> DatastreamResource:
        return self._underlying_resource

    def create_observation(self, obs_data: dict):
        obs = ObservationResource(result=obs_data, result_time=TimeInstant.now_as_time_instant())
        # Validate against the schema
        if self._underlying_resource.record_schema is not None:
            obs.validate_against_schema(self._underlying_resource.record_schema)
        return obs

    def insert_observation_dict(self, obs_data: dict):
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
        super().start()
        if self._mqtt_client is not None:
            # self._mqtt_client.connect()

            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
                self._mqtt_client.subscribe(self._topic, msg_callback=self._mqtt_sub_callback)
            else:
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(self._write_to_mqtt())
                except Exception as e:
                    # TODO: Use logging instead of print
                    print(traceback.format_exc())
                    print(f"Error starting MQTT write task: {e}")

            # self._mqtt_client.start()

    def init_mqtt(self):
        super().init_mqtt()
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)

    def _queue_push(self, msg):
        print(f'Pushing message to reader queue: {msg}')
        self._msg_writer_queue.put_nowait(msg)
        print(f'Queue size is now: {self._msg_writer_queue.qsize()}')

    def _queue_pop(self):
        return self._msg_reader_queue.get_nowait()

    def insert(self, data: dict):
        # self._queue_push(data)
        encoded = json.dumps(data).encode('utf-8')
        self._publish_mqtt(self._topic, encoded)

    def serialize(self) -> dict:
        data = super().serialize()
        data["should_poll"] = getattr(self, "should_poll", None)
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True)
            elif hasattr(underlying, 'to_dict'):
                data["underlying_resource"] = underlying.to_dict()
            else:
                data["underlying_resource"] = str(underlying)
        else:
            data["underlying_resource"] = None

        return data

    @classmethod
    def deserialize(cls, data: dict, node: 'Node') -> 'Datastream':
        ds_resource = DatastreamResource.model_validate(data["resource"]) if data.get("resource") else None
        obj = cls(parent_node=node, datastream_resource=ds_resource)
        obj._id = uuid.UUID(data["id"])
        obj.should_poll = data.get("should_poll", False)
        obj._underlying_resource = DatastreamResource.model_validate(data["_underlying_resource"])
        return obj

    def subscribe(self, topic=None, callback=None, qos=0):
        t = None

        if topic is None or topic == APIResourceTypes.OBSERVATION.value:
            t = self._topic
        # elif topic == APIResourceTypes.STATUS.value:
        #     t = self._status_topic
        else:
            raise ArgumentError(f"Invalid topic provided {topic}, must be None or 'observation'.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)


class ControlStream(StreamableResource[ControlStreamResource]):
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
        self._underlying_resource = resource

    def init_mqtt(self):
        super().init_mqtt()
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)

    def get_mqtt_status_topic(self):
        return self.get_mqtt_topic(subresource=APIResourceTypes.STATUS, data_topic=True)

    def start(self):
        super().start()
        if self._mqtt_client is not None:
            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
                # Subs to command topic by default
                self._mqtt_client.subscribe(self._topic, msg_callback=self._mqtt_sub_callback)
            else:
                try:
                    loop = asyncio.get_event_loop()
                    loop.create_task(self._write_to_mqtt())
                except Exception as e:
                    print(traceback.format_exc())
                    print(f"Error starting MQTT write task: {e}")

    def get_inbound_deque(self):
        return self._inbound_deque

    def get_outbound_deque(self):
        return self._outbound_deque

    def get_status_deque_inbound(self):
        return self._inbound_status_deque

    def get_status_deque_outbound(self):
        return self._outbound_status_deque

    def publish_command(self, payload):
        self.publish(payload, topic=APIResourceTypes.COMMAND.value)

    def publish_status(self, payload):
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
            raise ArgumentError(f"Invalid topic provided {topic}, must be None or one of 'command' or 'status'.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def serialize(self) -> dict:
        data = super().serialize()
        data["status_topic"] = getattr(self, "_status_topic", None)
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True)
            elif hasattr(underlying, 'to_dict'):
                data["underlying_resource"] = underlying.to_dict()
            else:
                data["underlying_resource"] = str(underlying)
        else:
            data["underlying_resource"] = None

        return data

    @classmethod
    def deserialize(cls, data: dict, node: 'Node') -> 'ControlStream':
        cs_resource = ControlStreamResource.model_validate(data["resource"]) if data.get("resource") else None
        obj = cls(node=node, controlstream_resource=cs_resource)
        obj._id = uuid.UUID(data["id"])
        obj._status_topic = data.get("status_topic")
        obj._underlying_resource = ControlStreamResource.model_validate(data["underlying_resource"])
        return obj
