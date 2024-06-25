#  Copyright (c) 2024 Ian Patterson
#
#  Author: Ian Patterson <ian@botts-inc.com>
#
#  Contact Email: ian@botts-inc.com
from __future__ import annotations

import asyncio
import json
from uuid import uuid4

import requests
import websockets
from conSys4Py import APIResourceTypes
from conSys4Py.datamodels.observations import ObservationOMJSONInline

from external_models import TimePeriod
from external_models.object_models import DatastreamResource
from oshconnect import TemporalModes
from oshconnect.datamodels.datamodels import System


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
            self._websocket = await websockets.connect(self._url, extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket
        elif self._playback_mode == TemporalModes.BATCH:
            self._websocket = await websockets.connect(self._url, extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket

    def disconnect(self):
        self._websocket.close()

    def reset(self):
        self._websocket = None
        self._status = "initialized"

    def get_status(self):
        return self._status

    def get_parent_system(self):
        return self._parent_system

    def get_ws_client(self):
        return self._websocket

    def is_within_timeperiod(self, timeperiod: TimePeriod):
        return timeperiod.does_timeperiod_overlap(self._datastream.valid_time)

    def generate_url(self):
        # TODO: need to specify secure vs insecure protocols
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
        elif self._playback_mode == TemporalModes.BATCH:
            # TODO: need to allow for batch counts selection through DS Handler or TimeManager
            self._url = (f'wss://{self._parent_system.get_parent_node().get_address()}:'
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

        if self._playback_mode == TemporalModes.REAL_TIME:
            [(ds, await ds.connect()) for ds in ds_matches]
            for ds in ds_matches:
                task = asyncio.create_task(self._handle_datastream_client(ds))
        elif self._playback_mode == TemporalModes.ARCHIVE:
            pass
        elif self._playback_mode == TemporalModes.BATCH:
            for ds in ds_matches:
                task = asyncio.create_task(self.handle_http_batching(ds))

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

    async def handle_http_batching(self, datasource: DataSource, offset: int = None, query_params: dict = None,
                                   next_link: str = None):
        # access api_helper
        api_helper = datasource.get_parent_system().get_parent_node().get_api_helper()
        # needs to create a new call to make a request to the server if there is a link to a next page
        resp = None
        if next_link is None:
            resp = api_helper.retrieve_resource(APIResourceTypes.OBSERVATION,
                                                parent_res_id=datasource._datastream.ds_id,
                                                req_headers={'Content-Type': 'application/json'})
        elif next_link is not None:
            resp = requests.get(next_link, auth=(datasource._parent_system.get_parent_node()._api_helper.username,
                                                 datasource._parent_system.get_parent_node()._api_helper.password))
        results = resp.json()
        if 'links' in results:
            for link in results['links']:
                if link['rel'] == 'next':
                    new_offset = link['href'].split('=')[-1]
                    asyncio.create_task(self.handle_http_batching(datasource, next_link=link['href']))

        # print(results)
        for obs in results['items']:
            obs_obj = ObservationOMJSONInline.model_validate(obs)
            msg_wrapper = MessageWrapper(datasource=datasource, message=obs_obj)
            self._message_list.add_message(msg_wrapper)
        return resp.json()


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
