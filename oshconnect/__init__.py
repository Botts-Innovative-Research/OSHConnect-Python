#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import base64
import uuid
from abc import ABC
from dataclasses import dataclass
from enum import Enum
import swecommondm as swe_common
from conSys4Py import APIResourceTypes
from conSys4Py.core.default_api_helpers import APIHelper


@dataclass(kw_only=True)
class Endpoints:
    root: str = "sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


@dataclass(kw_only=True)
class Node:
    _id: str
    address: str
    port: int
    endpoints: Endpoints
    is_secure: bool
    _basic_auth: bytes = None
    _api_helper: APIHelper

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
        res = self._api_helper.retrieve_resource(APIResourceTypes.SYSTEM, req_headers={})
        if res.ok:
            return res.json()
        else:
            return None


class TemporalModes(Enum):
    REAL_TIME = 0
    ARCHIVE = 1
    BATCH = 2
    RT_SYNC = 3
    ARCHIVE_SYNC = 4
