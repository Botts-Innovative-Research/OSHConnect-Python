#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================

from conSys4Py.core.default_api_helpers import APIHelper

from .core_datamodels import TimePeriod
from .datasource import DataSource, DataSourceHandler, MessageWrapper
from .datastore import DataStore
from .osh_connect_datamodels import Node, System, TemporalModes
from .styling import Styling
from .timemanagement import TimeManagement


class OSHConnect:
    _name: str = None
    datastore: DataStore = None
    styling: Styling = None
    timestream: TimeManagement = None
    _nodes: list[Node] = []
    _systems: list[System] = []
    _cs_api_builder: APIHelper = None
    _datasource_handler: DataSourceHandler = None
    _datafeeds: list[DataSource] = []
    _datataskers: list[DataStore] = []
    _datagroups: list = []
    _tasks: list = []
    _playback_mode: TemporalModes = TemporalModes.REAL_TIME

    def __init__(self, name: str, **kwargs):
        """
        :param name: name of the OSHConnect instance, in the event that
        :param kwargs:
            - 'playback_mode': TemporalModes
        """
        self._name = name
        if 'nodes' in kwargs:
            self._nodes = kwargs['nodes']
            self._playback_mode = kwargs['playback_mode']
            self._datasource_handler.set_playback_mode(self._playback_mode)
        self._datasource_handler = DataSourceHandler()
        if 'playback_mode' in kwargs:
            self._playback_mode = kwargs['playback_mode']
            self._datasource_handler.set_playback_mode(self._playback_mode)

    def get_name(self):
        """
        Get the name of the OSHConnect instance.
        :return:
        """
        return self._name

    def add_node(self, node: Node):
        """
        Add a node to the OSHConnect instance.
        :param node: Node object
        :return:
        """
        self._nodes.append(node)

    def remove_node(self, node_id: str):
        """
        Remove a node from the OSHConnect instance.
        :param node_id:
        :return:
        """
        # TODO: should disconnect datastreams and delete them and all systems at the same time.
        # list of nodes in our node list that do not have the id of the node we want to remove
        self._nodes = [node for node in self._nodes if
                       node.get_id() != node_id]

    def save_config(self, config: dict):
        pass

    def load_config(self, config: dict):
        pass

    def share_config(self, config: dict):
        pass

    def update_config(self, config: dict):
        pass

    def delete_config(self, config: dict):
        pass

    def configure_nodes(self, nodes: list):
        pass

    def filter_nodes(self, nodes: list):
        pass

    def task_system(self, task: dict):
        pass

    def select_temporal_mode(self, mode: str):
        """
        Select the temporal mode for the system. Real-time, archive, batch, as well as synchronization settings.
        :param mode:
        :return:
        """
        pass

    async def playback_streams(self, stream_ids: list = None):
        """
        Begins playback of the datastreams that have been connected to the app. The method of playback is determined
        by the temporal mode that has been set.
        :param stream_ids:
        :return:
        """
        if stream_ids is None:
            await self._datasource_handler.connect_all(
                self.timestream.get_time_range())
        else:
            for stream_id in stream_ids:
                await self._datasource_handler.connect_ds(stream_id)

    def visualize_streams(self, streams: list):
        pass

    # Second Level Use Cases
    def get_visualization_recommendations(self, streams: list):
        pass

    def discover_datastreams(self):
        """
        Discover datastreams of the current systems of the OSHConnect instance and create objects for them that are
        stored in the DataSourceHandler.
        :return:
        """
        # NOTE: This will need to check to prevent dupes in the future
        for system in self._systems:
            res_datastreams = system.discover_datastreams()
            # create DataSource(s)
            new_datasource = [
                DataSource(name=ds.name, datastream=ds, parent_system=system)
                for ds in
                res_datastreams]
            self._datafeeds.extend(new_datasource)
            list(map(self._datasource_handler.add_datasource, new_datasource))

    def discover_systems(self, nodes: list[str] = None):
        """
        Discover systems from the nodes that have been added to the OSHConnect instance. They are associated with the
        nodes that they are discovered from so access to them flows through there.
        :param nodes:
        :return:
        """
        search_nodes = self._nodes
        if nodes is not None:
            search_nodes = [node for node in search_nodes if
                            node.get_id() in nodes]

        for node in search_nodes:
            res_systems = node.discover_systems()
            self._systems.extend(res_systems)

    def discover_controlstreams(self, streams: list):
        pass

    def authenticate_user(self, user: dict):
        pass

    def synchronize_streams(self, systems: list):
        pass

    def set_playback_mode(self, mode: TemporalModes):
        self._datasource_handler.set_playback_mode(mode)

    def set_timeperiod(self, start_time: str, end_time: str):
        """
        Sets the time range (TimePeriod) for the OSHConnect instance. This is used to bookend the playback of the
        datastreams.
        :param start_time: ISO8601 formatted string or one of (now or latest)
        :param end_time:  ISO8601 formatted string or one of (now or latest)
        :return:
        """
        tp = TimePeriod(start=start_time, end=end_time)
        self.timestream = TimeManagement(time_range=tp)

    def get_message_list(self) -> list[MessageWrapper]:
        """
        Get the list of messages that have been received by the OSHConnect instance.
        :return: list of MessageWrapper objects
        """
        return self._datasource_handler.get_messages()
