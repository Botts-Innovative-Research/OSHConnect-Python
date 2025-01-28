#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================
import logging
import shelve

from consys4py.core.default_api_helpers import APIHelper

from .core_datamodels import DatastreamResource, TimePeriod
from .datasource import DataStream, DataStreamHandler, MessageWrapper
from .datastore import DataStore
from .osh_connect_datamodels import Node, System
from .styling import Styling
from .timemanagement import TemporalModes, TimeManagement


class OSHConnect:
    _name: str = None
    datastore: DataStore = None
    styling: Styling = None
    timestream: TimeManagement = None
    _nodes: list[Node] = []
    _systems: list[System] = []
    _cs_api_builder: APIHelper = None
    _datasource_handler: DataStreamHandler = None
    _datafeeds: list[DataStream] = []
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
        self._datasource_handler = DataStreamHandler()
        if 'playback_mode' in kwargs:
            self._playback_mode = kwargs['playback_mode']
            self._datasource_handler.set_playback_mode(self._playback_mode)

        logging.info(f"OSHConnect instance {name} created")

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
        logging.info(f"Saving configuration for {self._name}")
        with shelve.open(f"{self._name}_config") as db:
            db['app_config'] = self
            db.close()

    @classmethod
    def load_config(cls, file_name: str) -> 'OSHConnect':
        with shelve.open(file_name, 'r') as db:
            app = db['app_config']
            db.close()
            return app

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
                DataStream(name=ds.name, datastream=ds, parent_system=system)
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

    def insert_system(self, system: System, target_node: Node):
        """
        Create a system on the target node.
        :param system: System object
        :param target_node: Node object, must be within the OSHConnect instance
        :return: the created system
        """
        if target_node in self._nodes:
            self.add_system(system, target_node, insert_resource=True)
            return system

    def insert_datastream(self, datastream: DatastreamResource, system: str | System) -> str:
        """
        Insert a datastream into the OSHConnect instance.
        :param datastream: DataSource object
        :param system: System object or system id
        :return:
        """
        sys_obj: System
        if isinstance(system, str):
            sys_obj = self.find_system(system)
            if sys_obj is None:
                raise ValueError(f"System with id {system} not found")
        else:
            sys_obj = system

        sys_obj.add_insert_datastream(datastream)

        self._datafeeds.append(datastream)

    def find_system(self, system_id: str) -> System | None:
        """
        Find a system in the OSHConnect instance.
        :param system_id:
        :return: the found system or None if not found
        """
        for system in self._systems:
            if system.uid == system_id:
                return system
        return None

    # System Management
    def add_system(self, system: System, target_node: Node, insert_resource: bool = False):
        """
        Add a system to the target node.
        :param system: System object
        :param target_node: Node object,  must be within the OSHConnect instance
        :param insert_resource: Whether to insert the system into the target node's server, default is False
        :return:
        """
        if target_node in self._nodes:
            target_node.add_new_system(system)
            if insert_resource:
                system.insert_self()
            self._systems.append(system)
            return

    def create_and_insert_system(self, system_opts: dict, target_node: Node):
        """
        Create a system on the target node.
        :param system_opts: System object parameters
        :param target_node: Node object, must be within the OSHConnect instance
        :return: the created system
        """
        if target_node in self._nodes:
            new_system = System(**system_opts)
            self.add_system(new_system, target_node, insert_resource=True)
            return new_system

    def remove_system(self, system_id: str):
        pass
