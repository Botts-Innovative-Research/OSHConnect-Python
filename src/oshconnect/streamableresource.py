#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/29
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import asyncio
import base64
import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import TypeVar, Generic, Union
from uuid import UUID, uuid4

from aiohttp import ClientSession, BasicAuth
from aiohttp import WSMsgType, ClientWebSocketResponse

from .csapi4py.constants import APIResourceTypes
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


class OSHClientSession(ClientSession):
    verify_ssl = True
    _streamables: dict[str, 'StreamableResource'] = None

    def __init__(self, base_url, *args, verify_ssl=True, **kwargs):
        super().__init__(base_url, *args, **kwargs)
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
    endpoints: Endpoints
    is_secure: bool
    _basic_auth: bytes = None
    _api_helper: APIHelper
    _systems: list[System] = field(default_factory=list)
    _client_session: OSHClientSession = None

    def __init__(self, protocol: str, address: str, port: int,
                 username: str = None, password: str = None, session_manager: SessionManager = None,
                 **kwargs: dict):
        self._id = f'node-{uuid.uuid4()}'
        self.protocol = protocol
        self.address = address
        self.port = port
        self.is_secure = username is not None and password is not None
        if self.is_secure:
            self.add_basicauth(username, password)
        self.endpoints = Endpoints()
        self._api_helper = APIHelper(
            server_url=self.address,
            protocol=self.protocol,
            port=self.port,
            api_root=self.endpoints.connected_systems, username=username,
            password=password)
        if self.is_secure:
            self._api_helper.user_auth = True
        self._systems = []
        if session_manager is not None:
            self.register_with_session_manager(session_manager)

    def get_id(self):
        return self._id

    def get_address(self):
        return self.address

    def get_port(self):
        return self.port

    def get_api_endpoint(self):
        # return f"http{'s' if self.is_secure else ''}://{self.address}:{self.port}/{self.endpoints.connected_systems}"
        return self._api_helper.get_api_root_url()

    def add_basicauth(self, username: str, password: str):
        if not self.is_secure:
            self.is_secure = True
        self._basic_auth = base64.b64encode(
            f"{username}:{password}".encode('utf-8'))

    def get_decoded_auth(self):
        return self._basic_auth.decode('utf-8')

    def discover_systems(self):
        result = self._api_helper.retrieve_resource(APIResourceTypes.SYSTEM,
                                                    req_headers={})
        if result.ok:
            new_systems = []
            system_objs = result.json()['items']
            print(system_objs)
            for system_json in system_objs:
                print(system_json)
                system = SystemResource.model_validate(system_json)
                sys_obj = System.from_system_resource(system, self)
                sys_obj.set_parent_node(self)
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

    def add_system(self, system: System, target_node: Node, insert_resource: bool = False):
        """
        Add a system to the target node.
        :param system: System object
        :param target_node: Node object
        :param insert_resource: Whether to insert the system into the target node's server, default is False
        :return:
        """
        if insert_resource:
            system.insert_self()
        target_node.add_new_system(system)
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


class Status(Enum):
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    STOPPED = "stopped"


T = TypeVar('T', SystemResource, DatastreamResource)


class StreamableResource(Generic[T]):
    _id: UUID = None
    _resource_id: str = None
    _canonical_link: str = None
    _topic: str = None
    _status: str = Status.STOPPED.value
    ws_url: str = None
    _client_websocket: ClientWebSocketResponse = None
    _message_handler = None
    _parent_node: Node = None
    _underlying_resource: T = None
    _process: Process = None
    _msg_queue: asyncio.Queue[Union[str, bytes, float, int]] = None

    def __init__(self, node: Node):
        self._id = uuid4()
        self._message_handler = self._default_message_handler_fn
        self._parent_node = node
        self._parent_node.register_streamable(self)

    def get_streamable_id(self) -> UUID:
        return self._id

    def get_streamable_id_str(self) -> str:
        return self._id.hex

    def initialize(self):
        # self._process = Process(target=self.stream, args=())
        resource_type = None
        if isinstance(self._underlying_resource, SystemResource):
            resource_type = APIResourceTypes.SYSTEM
        elif isinstance(self._underlying_resource, DatastreamResource):
            resource_type = APIResourceTypes.DATASTREAM
        if resource_type is None:
            raise ValueError("Underlying resource must be set to either SystemResource or DatastreamResource before initialization.")
        # This needs to be implemented separately for each subclass
        self.ws_url = self._parent_node.get_api_helper().construct_url(parent_type=resource_type, res_type=APIResourceTypes.OBSERVATION, parent_res_id=self._underlying_resource.ds_id, res_id=None)
        self._msg_queue = asyncio.Queue()
        self._status = Status.INITIALIZED.value

    def start(self):
        if self._status != Status.INITIALIZED.value:
            logging.warning(f"Streamable resource {self._id} not initialized. Call initialize() first.")
            return
        self._status = Status.STARTING.value
        # self._process.start()
        self._status = Status.STARTED.value

        asyncio.gather(self.stream())

    async def stream(self):
        if self._msg_queue is None:
            self._msg_queue = asyncio.Queue()

        session = self._parent_node.get_session()

        #  TODO: handle auth properly and not right here...
        auth = BasicAuth("admin", "admin")

        async with session.ws_connect(self.ws_url, auth=auth) as ws:
            logging.info(f"Streamable resource {self._id} started.")
            async for msg in ws:
                self._message_handler(ws, msg)

    def stop(self):
        # It would be nicer to join() here once we have cleaner shutdown logic in place to avoid corrupting processes
        # that are writing to streams or that need to manage authentication state
        self._status = "stopping"
        self._process.terminate()
        self._status = "stopped"

    def _default_message_handler_fn(self, ws, msg):
        if msg.type == WSMsgType.TEXT:
            print(f"Received text message: {msg.data}")
            self._msg_queue.put(msg.data)
        elif msg.type == WSMsgType.BINARY:
            print(f"Received binary message: {msg.data}")
            self._msg_queue.put(msg.data)
        elif msg.type == WSMsgType.CLOSE:
            print("WebSocket closed")
        elif msg.type == WSMsgType.ERROR:
            print(f"WebSocket error: {ws.exception()}")

    def set_parent_node(self, node: Node):
        self._parent_node = node

    def get_parent_node(self) -> Node:
        return self._parent_node

    def poll(self):
        pass

    def fetch(self, time_period: TimePeriod):
        pass

    def get_msg_queue(self) -> Queue:
        """
        Returns the message queue for this streamable resource. In cases where a custom message handler is used this is
        not guaranteed to return anything or provided a queue with data.
        :return: Queue object
        """
        return self._msg_queue

    def get_underlying_resource(self) -> T:
        return self._underlying_resource


class System(StreamableResource[SystemResource]):
    resources_id: str
    name: str
    label: str
    datastreams: list[Datastream]
    control_channels: list[ControlChannel]
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
            self.resource_id = kwargs['resource_id']
        if kwargs.get('description'):
            self.description = kwargs['description']

        self._underlying_resource = self.to_system_resource()
        # self.underlying_resource = self._sys_resource

    def discover_datastreams(self) -> list[DatastreamResource]:
        res = self._parent_node.get_api_helper().retrieve_resource(
            APIResourceTypes.DATASTREAM, req_headers={})
        datastream_json = res.json()['items']
        ds_resources = []

        for ds in datastream_json:
            datastream_objs = DatastreamResource.model_validate(ds)
            ds_resources.append(datastream_objs)

        return ds_resources

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

    def add_insert_datastream(self, datastream: DataRecordSchema):
        """
        Adds a datastream to the system while also inserting it into the system's parent node via HTTP POST.
        :param datastream: DataRecordSchema to be used to define the datastream
        :return:
        """
        print(f'Adding datastream: {datastream.model_dump_json(exclude_none=True, by_alias=True)}')
        # Make the request to add the datastream
        # if successful, add the datastream to the system
        datastream_schema = SWEDatastreamRecordSchema(record_schema=datastream, obs_format='application/swe+json',
                                                      encoding=JSONEncoding())
        datastream_resource = DatastreamResource(ds_id="default", name=datastream.label, output_name=datastream.label,
                                                 record_schema=datastream_schema,
                                                 valid_time=TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                       end=TimeInstant(utc_time=TimeUtils.to_utc_time(
                                                                           "2026-12-31T00:00:00Z"))))

        api = self._parent_node.get_api_helper()
        print(
            f'Attempting to create datastream: {datastream_resource.model_dump_json(by_alias=True, exclude_none=True)}')
        print(
            f'Attempting to create datastream: {datastream_resource.model_dump(by_alias=True, exclude_none=True)}')
        res = api.create_resource(APIResourceTypes.DATASTREAM,
                                  datastream_resource.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={
                                      'Content-Type': 'application/json'
                                  }, parent_res_id=self.resource_id)

        if res.ok:
            datastream_id = res.headers['Location'].split('/')[-1]
            print(f'Resource Location: {datastream_id}')
            datastream_resource.ds_id = datastream_id
        else:
            raise Exception(f'Failed to create datastream: {datastream_resource.name}')

        self.datastreams.append(datastream_resource)
        return Datastream(datastream_id, self._parent_node, datastream_resource)

    def insert_self(self):
        res = self._parent_node.get_api_helper().create_resource(
            APIResourceTypes.SYSTEM, self.to_system_resource().model_dump_json(by_alias=True, exclude_none=True),
            req_headers={
                'Content-Type': 'application/sml+json'
            })

        if res.ok:
            location = res.headers['Location']
            sys_id = location.split('/')[-1]
            self.resource_id = sys_id
            print(f'Created system: {self.resource_id}')

    def retrieve_resource(self):
        if self.resource_id is None:
            return None
        res = self._parent_node.get_api_helper().retrieve_resource(res_type=APIResourceTypes.SYSTEM,
                                                                   res_id=self.resource_id)
        if res.ok:
            system_json = res.json()
            print(system_json)
            system_resource = SystemResource.model_validate(system_json)
            print(f'System Resource: {system_resource}')
            self._underlying_resource = system_resource
            return None


class Datastream(StreamableResource[DatastreamResource]):
    should_poll: bool
    resource_id: str
    # _datastream_resource: DatastreamResource
    _parent_node: Node

    def __init__(self, id: str = None, parent_node: Node = None, datastream_resource: DatastreamResource = None):
        super().__init__(node=parent_node)
        self.resource_id = id
        self._parent_node = parent_node
        # self._datastream_resource = datastream_resource
        self._underlying_resource = datastream_resource

    def get_id(self):
        return self._underlying_resource.ds_id

    def insert_observation(self, observation: Observation):
        pass

    @staticmethod
    def from_resource(ds_resource: DatastreamResource, parent_node: Node):
        new_ds = Datastream(id=ds_resource.ds_id, parent_node=parent_node, datastream_resource=ds_resource)
        return new_ds

    def set_resource(self, resource: DatastreamResource):
        self._underlying_resource = resource

    def get_resource(self) -> DatastreamResource:
        return self._underlying_resource

    def observation_template(self) -> Observation:
        pass

    def create_observation(self, obs_data: dict):
        obs = ObservationResource(result=obs_data, result_time=TimeInstant.now_as_time_instant())
        # Validate against the schema
        if self._underlying_resource.record_schema is not None:
            obs.validate_against_schema(self._underlying_resource.record_schema)
        return obs

    def insert_observation_dict(self, obs_data: dict):
        res = self._parent_node.get_api_helper().create_resource(APIResourceTypes.OBSERVATION, obs_data,
                                                                 parent_res_id=self.resource_id,
                                                                 req_headers={'Content-Type': 'application/json'})
        if res.ok:
            obs_id = res.headers['Location'].split('/')[-1]
            print(f'Inserted observation: {obs_id}')
            return id
        else:
            raise Exception(f'Failed to insert observation: {res.text}')

    # def initialize(self):


    # def create_from_record_schema(record_schema: DataRecordSchema, parent_system: System):
    #     new_ds = Datastream(name=record_schema.label, record_schema=record_schema)
    #     new_ds._datastream_resource = DatastreamResource(ds_id=uuid.uuid4(), name=new_ds.name)
    #     parent_system.datastreams.append(new_ds)
    #     return new_ds


class ControlChannel:
    _cc_resource: ControlStreamResource
    resource_id: str
    _parent_node: Node

    def __init__(self):
        pass


class Observation:
    _observation_resource: ObservationResource

    def __init__(self, observation_res: ObservationResource):
        self._observation_resource = observation_res

    def to_resource(self) -> ObservationResource:
        return self._observation_resource


class Output:
    name: str
    field_map: dict
