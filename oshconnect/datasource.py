#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
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
from conSys4Py.datamodels.swe_components import DataRecordSchema

from .core_datamodels import DatastreamResource, SystemResource, TimePeriod
from .osh_connect_datamodels import TemporalModes


# from swecommondm.component_implementations import DataRecord


class DataSource:
    """
    DataSource: represents the active connection of a datastream object.
    This class may later be used to connect to a control channel as well. It will almost certainly be used
    for Control Stream status monitoring.

    Attributes:
        name: Human readable name of the DataSource
        _datastream: DatastreamResource object
        _parent_system: System object that the DataSource is associated with.
    """
    name: str = None
    _id: str = None
    _datastream: DatastreamResource = None
    _parent_system: SystemResource = None
    _playback_mode: TemporalModes = None
    _url: str = None
    _auth: str = None
    _websocket: websockets.WebSocketClientProtocol = None
    _extra_headers: dict = None
    _result_schema: DataRecordSchema = None

    def __init__(self, name: str, datastream: DatastreamResource,
                 parent_system: SystemResource):
        """
        :param name: Human-readable name of the DataSource
        :param datastream: DatastreamResource object
        :param parent_system: System object that the DataSource is associated with.
        """
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
            # get result schema

            # t_url = f'http://{self._parent_system.get_parent_node().get_address()}:{self._parent_system.get_parent_node().get_port()}'
            #
            # res = conSys4Py.part_2.datastreams.retrieve_datastream_schema(t_url,
            #                                                               datastream_id=self._datastream.ds_id,
            #                                                               api_root=self._parent_system.get_parent_node()._api_helper.api_root,
            #                                                               headers=self._extra_headers)
            # print(res.json())

    def get_id(self) -> str:
        """
        Get the ID of the DataSource

        :return: str UID of the DataSource
        """
        return self._id

    def get_name(self):
        """
        Get the name of the DataSource

        :return: str name of the DataSource
        """
        return self.name

    def create_process(self):
        """
        **Unimplemented**

        Create a process for the DataSource

        :return:
        """
        pass

    def terminate_process(self):
        """
        **Unimplemented**
        """
        pass

    # Might not be necessary
    def subscribe(self):
        """
        **Unimplemented**

        :return:
        """
        pass

    def set_mode(self, mode: TemporalModes):
        """
        Sets the playback mode of the DataSource and regenerates the URL accordingly

        :param mode: TemporalModes

        :return:
        """
        self._playback_mode = mode
        self.generate_url()

    def initialize(self):
        """
        Initializes the DataSource object, resetting the status and closing any open connections if necessary.

        :return:
        """
        if self._websocket.is_open():
            self._websocket.close()
        self._websocket = None
        self._status = "initialized"

    async def connect(self) -> websockets.WebSocketClientProtocol or None:
        """
        Attempts to connect to the DataSource's websocket, or HTTP endpoint if in BATCH mode.
        :return: The websocket connection if in REAL_TIME or ARCHIVE mode, ``None`` if in BATCH mode.
        """
        if self._playback_mode == TemporalModes.REAL_TIME:
            self._websocket = await websockets.connect(self._url,
                                                       extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket
        elif self._playback_mode == TemporalModes.ARCHIVE:
            self._websocket = await websockets.connect(self._url,
                                                       extra_headers=self._extra_headers)
            self._status = "connected"
            return self._websocket
        elif self._playback_mode == TemporalModes.BATCH:
            self._websocket = await websockets.connect(self._url,
                                                       extra_headers=self._extra_headers)
            self._status = "connected"
            return None

    def disconnect(self):
        """
        Closes the websocket connection, *should* also stop any future http requests if in BATCH mode. This feature
        is *WIP*.

        :return:
        """
        self._websocket.close()

    def reset(self):
        """
        Resets the DataSource object, closing any open connections and resetting the status. Currently has the same
        effect as ``initialize()``.

        :return:
        """
        if self._websocket.is_open():
            self._websocket.close()
        self._websocket = None
        self._status = "initialized"

    def get_status(self):
        """
        Get the status code of the DataSource

        :return:
        """
        return self._status

    def get_parent_system(self) -> SystemResource:
        """
        Retrieve the DataSource's parent System

        :return: The parent System object of the DataSource
        """
        return self._parent_system

    def get_ws_client(self):
        """
        Get the websocket client object

        :return:
        """
        return self._websocket

    def is_within_timeperiod(self, timeperiod: TimePeriod) -> bool:
        """
        Checks if the DataSource's Datastream is within the provided TimePeriod

        :param timeperiod: TimePeriod object
        :return: ``True`` if the Datastream is within the TimePeriod, ``False`` otherwise
        """
        return timeperiod.does_timeperiod_overlap(self._datastream.valid_time)

    def generate_url(self):
        """
        Generates the URL for the DataSource based on the playback mode. This url is used for accessing the datastream
        on the OSH server.

        :return:
        """
        # TODO: need to specify secure vs insecure protocols
        if self._playback_mode == TemporalModes.REAL_TIME:
            self._url = (
                f'ws://{self._parent_system.get_parent_node().get_address()}:'
                f'{self._parent_system.get_parent_node().get_port()}'
                f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                f'/observations?f=application%2Fjson')
        elif self._playback_mode == TemporalModes.ARCHIVE:
            self._url = (
                f'ws://{self._parent_system.get_parent_node().get_address()}:'
                f'{self._parent_system.get_parent_node().get_port()}'
                f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                f'/observations?f=application%2Fjson&resultTime={self._datastream.valid_time.start}/'
                f'{self._datastream.valid_time.end}')
        elif self._playback_mode == TemporalModes.BATCH:
            # TODO: need to allow for batch counts selection through DS Handler or TimeManager
            self._url = (
                f'wss://{self._parent_system.get_parent_node().get_address()}:'
                f'{self._parent_system.get_parent_node().get_port()}'
                f'/sensorhub/api/datastreams/{self._datastream.ds_id}'
                f'/observations?f=application%2Fjson&resultTime={self._datastream.valid_time.start}/'
                f'{self._datastream.valid_time.end}')
        else:
            raise ValueError(
                "Playback mode not set. Cannot generate URL for DataSource.")


class DataSourceHandler:
    """
    Manages a collection of DataSource objects, allowing for easy access and control of multiple datastreams. As well
    as providing them access to a message handler for processing incoming data.
    """
    datasource_map: dict[str, DataSource]
    _message_list: MessageHandler
    _playback_mode: TemporalModes

    def __init__(self, playback_mode: TemporalModes = TemporalModes.REAL_TIME):
        self.datasource_map = {}
        self._message_list = MessageHandler()
        self._playback_mode = playback_mode

    def set_playback_mode(self, mode: TemporalModes):
        """
        Sets the playback mode for the DataSourceHandler and all of its DataSources

        :param mode: TemporalModes

        :return:
        """
        self._playback_mode = mode

    def add_datasource(self, datasource: DataSource):
        """
        Adds a DataSource object to the DataSourceHandler

        :param datasource: DataSource

        :return:
        """
        datasource.set_mode(self._playback_mode)
        self.datasource_map[datasource.get_id()] = datasource

    def remove_datasource(self, datasource_id: str) -> DataSource:
        """
        Removes a DataSource object from the DataSourceHandler

        :param datasource_id: str uid of the DataSource

        :return: the removed DataSource object
        """
        return self.datasource_map.pop(datasource_id)

    def initialize_ds(self, datasource_id: str):
        """
        Initializes a DataSource object by calling its initialize method

        :param datasource_id:

        :return:
        """
        ds = self.datasource_map.get(datasource_id)
        ds.initialize()

    def initialize_all(self):
        """
        Initializes all DataSource objects in the DataSourceHandler

        :return:
        """
        [ds.initialize() for ds in self.datasource_map.values()]

    def set_ds_mode(self):
        """
        Sets the playback mode for all DataSource objects in the DataSourceHandler, uses the playback mode of the
        DataSourceHandler
        :return:
        """
        (ds.set_mode(self._playback_mode) for ds in self.datasource_map.values())

    async def connect_ds(self, datasource_id: str):
        """
        Connects a DataSource object by calling its connect method

        :param datasource_id:

        :return:
        """
        ds = self.datasource_map.get(datasource_id)
        await ds.connect()

    async def connect_all(self, timeperiod: TimePeriod):
        """
        Connects all datasources, optionally within a provided TimePeriod
        :param timeperiod: TimePeriod object
        :return:
        """
        # search for datasources that fall within the timeperiod
        if timeperiod is not None:
            ds_matches = [ds for ds in self.datasource_map.values() if
                          ds.is_within_timeperiod(timeperiod)]
        else:
            ds_matches = self.datasource_map.values()

        if self._playback_mode == TemporalModes.REAL_TIME:
            [(ds, await ds.connect()) for ds in ds_matches]
            for ds in ds_matches:
                asyncio.create_task(self._handle_datastream_client(ds))
        elif self._playback_mode == TemporalModes.ARCHIVE:
            pass
        elif self._playback_mode == TemporalModes.BATCH:
            for ds in ds_matches:
                asyncio.create_task(self.handle_http_batching(ds))

    def disconnect_ds(self, datasource_id: str):
        """
        Disconnects a DataSource object by calling its disconnect method
        :param datasource_id:
        :return:
        """
        ds = self.datasource_map.get(datasource_id)
        ds.disconnect()

    def disconnect_all(self):
        """
        Disconnects all DataSource objects in the DataSourceHandler
        :return:
        """
        [ds.disconnect() for ds in self.datasource_map.values()]

    async def _handle_datastream_client(self, datasource: DataSource):
        """
        Handles the websocket client for a DataSource object, passes Observations to the MessageHandler in the
        form of MessageWrapper objects

        :param datasource:

        :return:
        """
        try:
            async for msg in datasource.get_ws_client():
                msg_dict = json.loads(msg.decode('utf-8'))
                obs = ObservationOMJSONInline.model_validate(msg_dict)
                msg_wrapper = MessageWrapper(datasource=datasource,
                                             message=obs)
                self._message_list.add_message(msg_wrapper)

        except Exception as e:
            print(f"An error occurred while reading from websocket: {e}")

    async def handle_http_batching(self, datasource: DataSource,
                                   offset: int = None,
                                   query_params: dict = None,
                                   next_link: str = None) -> dict:
        """
        Handles the batching of HTTP requests for a DataSource object, passes Observations to the MessageHandler

        :param datasource:
        :param offset:
        :param query_params:
        :param next_link:

        :return: dict of the response from the server
        """
        # access api_helper
        api_helper = datasource.get_parent_system().get_parent_node().get_api_helper()
        # needs to create a new call to make a request to the server if there is a link to a next page
        resp = None
        if next_link is None:
            resp = api_helper.retrieve_resource(APIResourceTypes.OBSERVATION,
                                                parent_res_id=datasource._datastream.ds_id,
                                                req_headers={
                                                    'Content-Type': 'application/json'})
        elif next_link is not None:
            resp = requests.get(next_link, auth=(
                datasource._parent_system.get_parent_node()._api_helper.username,
                datasource._parent_system.get_parent_node()._api_helper.password))
        results = resp.json()
        if 'links' in results:
            for link in results['links']:
                if link['rel'] == 'next':
                    # new_offset = link['href'].split('=')[-1]
                    asyncio.create_task(self.handle_http_batching(datasource, next_link=link['href']))

        # print(results)
        for obs in results['items']:
            obs_obj = ObservationOMJSONInline.model_validate(obs)
            msg_wrapper = MessageWrapper(datasource=datasource,
                                         message=obs_obj)
            self._message_list.add_message(msg_wrapper)
        return resp.json()

    def get_message_handler(self) -> MessageHandler:
        """
        Get the MessageHandler object from the DataSourceHandler

        :return: MessageHandler object
        """
        return self._message_list

    def get_messages(self) -> list[MessageWrapper]:
        """
        Get the list of MessageWrapper objects from the MessageHandler

        :return: List of MessageWrapper objects
        """
        return self._message_list.get_messages()


class MessageHandler:
    """
    Manages a list of MessageWrapper objects, allowing for easy access and control of multiple messages. Works in
    conjunction with the TimeManager to sort messages by their resultTime.
    """
    _message_list: list[MessageWrapper]

    def __init__(self):
        self._message_list = []

    def add_message(self, message: MessageWrapper):
        """
        Adds a MessageWrapper object to the MessageHandler

        :param message:

        :return:
        """
        self._message_list.append(message)
        # print(self._message_list)

    def get_messages(self) -> list[MessageWrapper]:
        """
        Get the list of MessageWrapper objects

        :return: List of MessageWrapper objects
        """
        return self._message_list

    def clear_messages(self):
        """
        Empties the list of MessageWrapper objects

        :return:
        """
        self._message_list.clear()

    def sort_messages(self) -> list[MessageWrapper]:
        """
        Sorts the list of MessageWrapper objects by their resultTime

        :return: the sorted List of MessageWrapper objects
        """
        # copy the list
        sorted_list = self._message_list.copy()
        sorted_list.sort(key=lambda x: x.resultTime)
        return sorted_list


class MessageWrapper:
    """
    Combines a DataSource and a Message into a single object for easier access
    """

    def __init__(self, datasource: DataSource,
                 message: ObservationOMJSONInline):
        self._message = message
        self._datasource = datasource

    def get_message(self) -> ObservationOMJSONInline:
        """
        Get the observation data from the MessageWrapper

        :return: ObservationOMJSONInline that is easily serializable
        """
        return self._message

    def get_message_as_dict(self) -> dict:
        """
        Get the observation data from the MessageWrapper as a dictionary

        :return: dict of the observation result data
        """
        return self._message.model_dump()

    def __repr__(self):
        return f"{self._datasource}, {self._message}"
