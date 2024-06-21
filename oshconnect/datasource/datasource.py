#  Copyright (c) 2024 Ian Patterson
#
#  Author: Ian Patterson <ian@botts-inc.com>
#
#  Contact Email: ian@botts-inc.com
import asyncio
from uuid import uuid4

import websockets

from external_models.object_models import DatastreamResource
from oshconnect import Utilities
from oshconnect.datamodels.datamodels import System
from oshconnect.datasource import Mode


class DataSource:
    """
    DataSource: represents the active connection of a datastream object
    """

    def __init__(self, name: str, mode: str, properties: dict, datastream: DatastreamResource, parent_system: System):
        self._status = None
        self._id = f'datasource-{uuid4()}'
        self.name = name
        self.mode = mode
        self.properties = properties
        self._datastream = datastream
        self._websocket = None
        self._parent_system = parent_system
        self._url = None
        if mode == "websocket":
            self._url = (f'ws://{self._parent_system.get_parent_node().get_address()}:'
                         f'{self._parent_system.get_parent_node().get_port()}'
                         f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                         f'/observations?f=application%2Fjson')
        self._auth = None
        self._extra_headers = None
        if self._parent_system.get_parent_node().is_secure:
            self._auth = self._parent_system.get_parent_node().get_decoded_auth()
            self._extra_headers = {'Authorization': f'Basic {self._auth}'}

    def get_id(self) -> str:
        return self._id

    def get_name(self):
        pass

    def create_process(self):
        pass

    def terminate_process(self):
        pass

    # Might not be necessary
    def subscribe(self):
        pass

    def update_properties(self, properties: dict):
        # TODO: need to stop in progress sub-processes and restart
        self.properties = properties

    def initialize(self):
        pass

    async def connect(self):
        if self.mode == "websocket":
            self._websocket = await websockets.connect(self._url, extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket


def disconnect(self):
    pass


def reset(self):
    pass


def get_status(self):
    return self.status


class DataSourceHandler:
    datasource_map: dict[str, DataSource]

    def __init__(self):
        self.datasource_map = {}

    def add_datasource(self, datasource: DataSource):
        self.datasource_map[datasource.get_id()] = datasource

    def initialize_ds(self, datasource_id: str, properties: dict):
        ds = self.datasource_map.get(datasource_id)
        ds.initialize()

    def initialize_all(self):
        # list comp is faster than for loop
        [ds.initialize() for ds in self.datasource_map.values()]

    async def connect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        await ds.connect()

    async def connect_all(self):
        results = await asyncio.gather(*(ds.connect() for ds in self.datasource_map.values()))
        return results

    def disconnect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        ds.disconnect()

    def disconnect_all(self):
        [ds.disconnect() for ds in self.datasource_map.values()]
