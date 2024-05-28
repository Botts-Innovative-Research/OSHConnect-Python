#  Copyright (c) 2024 Ian Patterson
#
#  Author: Ian Patterson <ian@botts-inc.com>
#
#  Contact Email: ian@botts-inc.com

from uuid import uuid4

from oshconnect.datasource import Mode


class DataSource:

    def __init__(self, name: str, mode: Mode, properties: dict):
        self._id = f'datasource-{uuid4()}'
        self.name = name
        self.mode = mode
        self.properties = properties

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

    def connect(self):
        pass

    def disconnect(self):
        pass

    def reset(self):
        pass

    def get_status(self):
        return self.status


class DatasourceHandler:
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

    def connect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        ds.connect()

    def connect_all(self):
        [ds.connect() for ds in self.datasource_map.values()]

    def disconnect_ds(self, datasource_id: str):
        ds = self.datasource_map.get(datasource_id)
        ds.disconnect()

    def disconnect_all(self):
        [ds.disconnect() for ds in self.datasource_map.values()]
