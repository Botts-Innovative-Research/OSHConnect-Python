#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass, field

from conSys4Py import APIResourceTypes, DataRecordSchema
from conSys4Py.core.default_api_helpers import APIHelper
from conSys4Py.datamodels.datastreams import SWEDatastreamSchema
from conSys4Py.datamodels.encoding import JSONEncoding

from .core_datamodels import DatastreamResource, ObservationResource, SystemResource
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
    _system_ids: list[uuid] = field(default_factory=list)

    def __init__(self, protocol: str, address: str, port: int,
                 username: str = None, password: str = None,
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
            server_url=f'{self.protocol}://{self.address}:{self.port}',
            api_root=self.endpoints.connected_systems, username=username,
            password=password)
        if self.is_secure:
            self._api_helper.user_auth = True
        self._system_ids = []

    def get_id(self):
        return self._id

    def get_address(self):
        return self.address

    def get_port(self):
        return self.port

    def get_api_endpoint(self):
        return f"http{'s' if self.is_secure else ''}://{self.address}:{self.port}/{self.endpoints.connected_systems}"

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
                sys_obj = System.from_system_resource(system)
                sys_obj.update_parent_node(self)
                self._system_ids.append(sys_obj.uid)
                new_systems.append(sys_obj)
            return new_systems
        else:
            return None

    def add_new_system(self, system: System):
        system.update_parent_node(self)
        self._system_ids.append(system.uid)

    def get_api_helper(self) -> APIHelper:
        return self._api_helper


class System:
    uid: uuid.UUID
    resource_id: str
    name: str
    label: str
    # datastreams: list[Datastream]
    datastreams: list[DatastreamResource]
    control_channels: list[ControlChannel]
    description: str
    urn: str
    _parent_node: Node
    _sys_resource: SystemResource

    def __init__(self, name: str, label: str, urn: str, **kwargs):
        """
        :param name: The machine-accessible name of the system
        :param label: The human-readable label of the system
        :param urn: The URN of the system, typically formed as such: 'urn:general_identifier:specific_identifier:more_specific_identifier'
        :param kwargs:
            - 'description': A description of the system
        """
        self.uid = uuid.uuid4()
        self.name = name
        self.label = label
        self.datastreams = []
        self.control_channels = []
        self.urn = urn
        if kwargs.get('resource_id'):
            self.resource_id = kwargs['resource_id']
        if kwargs.get('description'):
            self.description = kwargs['description']

    def update_parent_node(self, node: Node):
        self._parent_node = node

    def get_parent_node(self) -> Node:
        return self._parent_node

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
    def from_system_resource(system_resource: SystemResource):
        other_props = system_resource.model_dump()
        print(f'Props of SystemResource: {other_props}')

        # case 1: has properties a la geojson
        if 'properties' in other_props:
            new_system = System(name=other_props['properties']['name'],
                                label=other_props['properties']['name'])
        else:
            new_system = System(name=system_resource.name,
                                label=system_resource.label, urn=system_resource.urn, resource_id=system_resource.system_id)
        return new_system

    def to_system_resource(self) -> SystemResource:
        resource = SystemResource(uid=self.urn, label=self.name, feature_type='PhysicalSystem')

        if len(self.datastreams) > 0:
            resource.outputs = [ds.to_resource() for ds in self.datastreams]

        # if len(self.control_channels) > 0:
        #     resource.inputs = [cc.to_resource() for cc in self.control_channels]
        return resource

    def add_insert_datastream(self, datastream: DataRecordSchema):
        """
        Adds a datastream to the system while also inserting it into the system's parent node via HTTP POST.
        :param datastream: DataRecordSchema to be used to define the datastream
        :return:
        """
        print(f'Adding datastream: {datastream.model_dump_json(exclude_none=True, by_alias=True)}')
        # Make the request to add the datastream
        # if successful, add the datastream to the system
        datastream_schema = SWEDatastreamSchema(record_schema=datastream, obs_format='application/swe+json', encoding=JSONEncoding())
        datastream_resource = DatastreamResource(ds_id="default", name=datastream.label, output_name=datastream.label, record_schema=datastream_schema,
                                                 valid_time=TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                       end=TimeInstant(utc_time=TimeUtils.to_utc_time("2026-12-31T00:00:00Z"))))
        # datasource = DataStream(name=datastream.label, datastream=datastream_resource, parent_system=self._sys_resource)

        api = self._parent_node.get_api_helper()
        print(f'Attempting to create datastream: {datastream_resource.model_dump_json(by_alias=True, exclude_none=True)}')
        print(
            f'Attempting to create datastream: {datastream_resource.model_dump(by_alias=True, exclude_none=True)}')
        res = api.create_resource(APIResourceTypes.DATASTREAM, datastream_resource.model_dump_json(by_alias=True, exclude_none=True),
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

    def insert_self(self):
        res = self._parent_node.get_api_helper().create_resource(
            APIResourceTypes.SYSTEM, self.to_system_resource().model_dump_json(by_alias=True, exclude_none=True), req_headers={
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
        res = self._parent_node.get_api_helper().retrieve_resource(res_type=APIResourceTypes.SYSTEM, res_id=self.resource_id)
        if res.ok:
            system_json = res.json()
            print(system_json)
            system_resource = SystemResource.model_validate(system_json)
            print(f'System Resource: {system_resource}')
            self._sys_resource = system_resource


# class Datastream:
#     should_poll: bool
#     _datastream_resource: DatastreamResource
#
#     def __init__(self, datastream_resource: DatastreamResource):
#         pass
#
#     def get_id(self):
#         return self._datastream_resource.ds_id
#
#     def insert_observation(self, observation: Observation):
#         pass
#
#     def to_resource(self) -> DatastreamResource:
#         # if self._datastream_resource is None:
#         #     self._datastream_resource = DatastreamResource(
#         #         ds_id=uuid.uuid4(), name=self.name,
#         #         valid_time=self.validTimeRange)
#         return self._datastream_resource
#
#     # def create_from_record_schema(record_schema: DataRecordSchema, parent_system: System):
#     #     new_ds = Datastream(name=record_schema.label, record_schema=record_schema)
#     #     new_ds._datastream_resource = DatastreamResource(ds_id=uuid.uuid4(), name=new_ds.name)
#     #     parent_system.datastreams.append(new_ds)
#     #     return new_ds


class ControlChannel:
    # _cc_resource: ControlStream

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
