
#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from abc import ABC, abstractmethod


# Might not be necessary due to differences in this implementation die to actual multiprocessing
class DataSourceHandler(ABC):
    def __init__(self):
        self.context = None
        self.topic = None
        self.broadcast_channel = None
        self.values = []
        self.version = 0
        self.properties = {
            "batchsize": 1
        }
        self._initialized = False
        self.datasource_id = None

    @abstractmethod
    def create_context(self, properties: dict):
        pass

    async def init(self, datasource_id: str, properties: dict, topics: list[str]):
        self.datasource_id = datasource_id
        self.properties.update(properties)
        self.topic = self.set_topics(topics)
        # Context doesn't really have to exist in python
        self.context = self.create_context(properties)
        self.context.on_change_status = self.on_change_status()
        self.context.handle_data = self.handle_data()
        await self.context.init(self.properties)
        self._initialized = True

    # TODO: topics may not be necessary
    def set_topics(self, topics):
        _topic = topics.data
        if self.topic == _topic:
            return
        return

    def on_change_status(self):
        pass

    def handle_data(self):
        pass

    def flush(self):
        pass

    def connect(self):
        pass

    def disconnect(self):
        pass

    def is_initialized(self):
        return self._initialized

    def is_connected(self):
        pass
