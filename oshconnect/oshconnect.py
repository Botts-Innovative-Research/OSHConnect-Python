#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================

class OSHConnect:
    datasource: DataSourceAPI
    datastore: DataStoreAPI
    styling: StylingAPI
    timestream: TimeStreamAPI

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

    def discover_systems(self, systems: list):
        pass

    def discover_controlstreams(self, streams: list):
        pass

    def authenticate_user(self, user: dict):
        pass

    def synchronize_streams(self, systems: list):
        pass
