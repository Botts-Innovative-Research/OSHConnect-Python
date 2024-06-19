#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================

from conSys4Py.core.default_api_helpers import APIHelper

from oshconnect.datamodels.datamodels import Node, System
from oshconnect.datasource.datasource import DataSource
from oshconnect.datastore.datastore import DataStore
from oshconnect.styling.styling import Styling
from oshconnect.timemanagement.timemanagement import TimeManagement


class OSHConnect:
    _name: str = None
    datasource: DataSource = None
    datastore: DataStore = None
    styling: Styling = None
    timestream: TimeManagement = None
    _nodes: list[Node] = []
    _systems: list[System] = []
    _cs_api_builder: APIHelper = None
    _datafeeds: list[DataSource] = []
    _datataskers: list[DataStore] = []
    _datagroups: list = []

    def __init__(self, name: str, **kwargs):
        self._name = name
        if 'nodes' in kwargs:
            self._nodes = kwargs['nodes']

    def get_name(self):
        return self._name

    def add_node(self, node: Node):
        self._nodes.append(node)

    def remove_node(self, node_id: str):
        # list of nodes in our node list that do not have the id of the node we want to remove
        self._nodes = [node for node in self._nodes if node.get_id() != node_id]

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

    def playback_streams(self, streams: list):
        pass

    def visualize_streams(self, streams: list):
        pass

    # Second Level Use Cases
    def get_visualization_recommendations(self, streams: list):
        pass

    def discover_datastreams(self, streams: list):
        pass

    def discover_systems(self, nodes: list[str] = None):
        search_nodes = self._nodes
        if nodes is not None:
            search_nodes = [node for node in search_nodes if node.get_id() in nodes]

        for node in search_nodes:
            res_systems = node.discover_systems()
            self._systems.extend(res_systems)

    def discover_controlstreams(self, streams: list):
        pass

    def authenticate_user(self, user: dict):
        pass

    def synchronize_streams(self, systems: list):
        pass
