#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/13
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import base64
import uuid
from dataclasses import dataclass, field

from conSys4Py import APIResourceTypes
from conSys4Py.core.default_api_helpers import APIHelper

from external_models.object_models import System as SystemResource
from oshconnect import Endpoints


@dataclass(kw_only=True)
class Node:
    _id: str
    address: str
    port: int
    endpoints: Endpoints
    is_secure: bool
    _basic_auth: bytes = None
    _api_helper: APIHelper
    _system_ids: list[uuid] = field(default_factory=list)

    def __init__(self, address: str, port: int, username: str = None, password: str = None, **kwargs: dict):
        self._id = f'node-{uuid.uuid4()}'
        self.address = address
        self.port = port
        self.is_secure = username is not None and password is not None
        if self.is_secure:
            self.add_basicauth(username, password)
        self.endpoints = Endpoints()
        self._api_helper = APIHelper(server_url=f'{self.address}:{self.port}',
                                     api_root=self.endpoints.connected_systems, username=username, password=password)
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
        self._basic_auth = base64.b64encode(f"{username}:{password}".encode('utf-8'))

    def get_decoded_auth(self):
        return self._basic_auth.decode('utf-8')

    def discover_systems(self):
        result = self._api_helper.retrieve_resource(APIResourceTypes.SYSTEM, req_headers={})
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
        self._system_ids.append(system.uid)


class System:
    uid: uuid.UUID
    name: str
    label: str
    datastreams: list[Datastream]
    control_channels: list[ControlChannel]
    description: str
    _parent_node: Node

    def __init__(self, name: str, label: str, **kwargs):
        self.uid = uuid.uuid4()
        self.name = name
        self.label = label
        self.datastreams = []
        self.control_channels = []

    def update_parent_node(self, node: Node):
        self._parent_node = node

    @staticmethod
    def from_system_resource(system_resource: SystemResource):
        other_props = system_resource.dict()
        print(f'Props of SystemResource: {other_props}')

        # case 1: has properties a la geojson
        if 'properties' in other_props:
            new_system = System(name=other_props['properties']['name'], label=other_props['properties']['name'])
        else:
            new_system = System(name=system_resource.name, label=system_resource.label)
        return new_system


class Datastream:

    def __init__(self):
        pass


class ControlChannel:

    def __init__(self):
        pass
