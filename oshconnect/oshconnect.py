#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================

from conSys4Py.con_sys_api import ConnectedSystemsRequestBuilder, ConnectedSystemAPIRequest

from oshconnect import Node
from oshconnect.datasource.datasource import DataSource
from oshconnect.styling.styling import Styling
from oshconnect.timemanagement.timemanagement import TimeManagement
from oshconnect.datastore.datastore import DataStore


class OSHConnect:
    datasource: DataSource
    datastore: DataStoreAPI
    styling: StylingAPI
    timestream: TimeManagement
    _nodes: list[Node]
    _cs_api_builder: ConnectedSystemsRequestBuilder

    def __init__(self, **kwargs):
        if 'nodes' in kwargs:
            self._nodes = kwargs['nodes']

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

    def discover_systems(self):
        sys_list = systems.list_all_systems()

    def discover_controlstreams(self, streams: list):
        pass

    def authenticate_user(self, user: dict):
        pass

    def synchronize_streams(self, systems: list):
        pass
