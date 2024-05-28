
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
import oshdatacore as swe_common


@dataclass(kw_only=True)
class Endpoints:
    root: str = "/sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


@dataclass(kw_only=True)
class Node(ABC):
    _id: str
    address: str
    port: int
    endpoints: Endpoints
    is_secure: bool
    _basic_auth: bytes

    def __init__(self, address: str, port: int, is_secure: bool):
        self._id = f'node-{uuid.uuid4()}'
        self.address = address
        self.port = port
        self.is_secure = is_secure
        self.endpoints = Endpoints()

    def get_id(self):
        return self._id

    def get_address(self):
        return self.address

    def get_port(self):
        return self.port

    def get_api_endpoint(self):
        return f"http{'s' if self.is_secure else ''}://{self.address}:{self.port}/{self.endpoints.connected_systems}"

    def add_basicauth(self, username: str, password: str):
        self._basic_auth = base64.b64encode(f"{username}:{password}".encode('utf-8'))

    def get_decoded_auth(self):
        return self._basic_auth.decode('utf-8')


class TemporalModes(Enum):
    REAL_TIME = 0
    ARCHIVE = 1
    BATCH = 2
    RT_SYNC = 3
    ARCHIVE_SYNC = 4


