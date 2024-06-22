#  Copyright (c) 2024 Ian Patterson
#
#  Author: Ian Patterson <ian@botts-inc.com>
#
#  Contact Email: ian@botts-inc.com
from __future__ import annotations
import asyncio
import json
from uuid import uuid4

import websockets
from conSys4Py.datamodels.observations import ObservationOMJSONInline

from external_models import TimePeriod
from external_models.object_models import DatastreamResource
from oshconnect import Utilities
from oshconnect.datamodels.datamodels import System
from oshconnect.datasource import Mode
from oshconnect import TemporalModes


class DataSource:
    """
    DataSource: represents the active connection of a datastream object
    """

    def __init__(self, name: str, datastream: DatastreamResource, parent_system: System):
        self._status = None
        self._id = f'datasource-{uuid4()}'
        self.name = name
        self._datastream = datastream
        self._websocket = None
        self._parent_system = parent_system
        self._playback_mode = None
        self._url = None
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

    def set_mode(self, mode: TemporalModes):
        self._playback_mode = mode
        self.generate_url()

    def initialize(self):
        if self._websocket.is_open():
            self._websocket.close()
        self._websocket = None
        self._status = "initialized"

    async def connect(self):
        if self._playback_mode == TemporalModes.REAL_TIME:
            self._websocket = await websockets.connect(self._url, extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket
        elif self._playback_mode == TemporalModes.ARCHIVE:
            self._status = "connected"
            return "Playback mode is not yet implemented."
        elif self._playback_mode == TemporalModes.BATCH:
            self._status = "connected"
            return "Live-batch mode is not yet implemented."

    def disconnect(self):
        self._websocket.close()

    def reset(self):
        self._websocket = None
        self._status = "initialized"

    def get_status(self):
        return self._status

    def get_ws_client(self):
        return self._websocket

    def is_within_timeperiod(self, timeperiod: TimePeriod):
        return timeperiod.does_timeperiod_overlap(self._datastream.valid_time)

    def generate_url(self):
        if self._playback_mode == TemporalModes.REAL_TIME:
            self._url = (f'ws://{self._parent_system.get_parent_node().get_address()}:'
                         f'{self._parent_system.get_parent_node().get_port()}'
                         f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                         f'/observations?f=application%2Fjson')
        elif self._playback_mode == TemporalModes.ARCHIVE:
            self._url = (f'ws://{self._parent_system.get_parent_node().get_address()}:'
                         f'{self._parent_system.get_parent_node().get_port()}'
                         f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                         f'/observations?f=application%2Fjson&resultTime={self._datastream.valid_time.start}/'
                         f'{self._datastream.valid_time.end}')
        else:
            raise ValueError("Playback mode not set. Cannot generate URL for DataSource.")


class DataSourceHandler:
    datasource_map: dict[str, DataSource]
    _message_list: MessageHandler
    _playback_mode: TemporalModes

    def __init__(self, playback_mode: TemporalModes = TemporalModes.REAL_TIME):
        self.datasource_map = {}
        self._message_list = MessageHandler()
        self._playback_mode = playback_mode

    def set_playback_mode(self, mode: TemporalModes):
        self._playback_mode = mode

    def add_datasource(self, datasource: DataSource):
        datasource.set_mode(self._playback_mode)
        self.datasource_map[datasource.get_id()] = datasource

    def remove_datasource(self, datasource_id: str):
        return self.datasource_map.pop(datasource_id)

    def initialize_ds(self, datasource_id: str, properties: dict):
        ds = self.datasource_map.get(datasource_id)
        ds.initialize()

    def initialize_all(self):
        # list comp is faster than for loop
        [ds.initialize() for ds in self.datasource_map.values()]

    def set_ds_mode(self):
        var = (ds.set_mode(self._playback_mode) for ds in self.datasource_map.values())

    async def connect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        await ds.connect()

    async def connect_all(self, timeperiod: TimePeriod):
        """
        Connects all datasources, optionally within a provided TimePeriod
        :param timeperiod:
        :return:
        """
        # search for datasources that fall within the timeperiod
        if timeperiod is not None:
            ds_matches = [ds for ds in self.datasource_map.values() if ds.is_within_timeperiod(timeperiod)]
        else:
            ds_matches = self.datasource_map.values()

        [(ds, await ds.connect()) for ds in ds_matches]
        for ds in ds_matches:
            task = asyncio.create_task(self._handle_datastream_client(ds))
            # return task

    def disconnect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        ds.disconnect()

    def disconnect_all(self):
        [ds.disconnect() for ds in self.datasource_map.values()]

    async def _handle_datastream_client(self, datasource: DataSource):
        try:
            async for msg in datasource.get_ws_client():
                msg_dict = json.loads(msg.decode('utf-8'))
                obs = ObservationOMJSONInline.model_validate(msg_dict)
                msg_wrapper = MessageWrapper(datasource=datasource, message=obs)
                self._message_list.add_message(msg_wrapper)

        except Exception as e:
            print(f"An error occurred while reading from websocket: {e}")


class MessageHandler:
    _message_list: list[MessageWrapper]

    def __init__(self):
        self._message_list = []

    def add_message(self, message: MessageWrapper):
        self._message_list.append(message)
        print(self._message_list)

    def get_messages(self):
        return self._message_list

    def clear_messages(self):
        self._message_list.clear()

    def sort_messages(self):
        # copy the list
        sorted_list = self._message_list.copy()
        sorted_list.sort(key=lambda x: x.resultTime)
        return sorted_list


class MessageWrapper:
    """
    Combines a DataSource and a Message into a single object for easier access
    """

    def __init__(self, datasource: DataSource, message: ObservationOMJSONInline):
        self._message = message
        self._datasource = datasource

    def get_message(self):
        return self._message

    def get_message_as_dict(self):
        return self._message.dict()

    def __repr__(self):
        return f"{self._datasource}, {self._message}"
