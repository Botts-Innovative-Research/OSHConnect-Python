#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import websockets

from oshconnect.core_datamodels import TimePeriod
from oshconnect.osh_connect_datamodels import Node
from oshconnect.oshconnectapi import OSHConnect
from oshconnect.timemanagement import TimeInstant


class TestOSHConnect:
    TEST_PORT = 8282

    def test_time_period(self):
        tp = TimePeriod(start="2024-06-18T15:46:32Z", end="2024-06-18T20:00:00Z")
        assert tp is not None
        tps = tp.start
        tpe = tp.end
        assert isinstance(tps, TimeInstant)
        assert isinstance(tpe, TimeInstant)
        assert tps.epoch_time == TimeInstant.from_string("2024-06-18T15:46:32Z").epoch_time
        assert tpe.epoch_time == TimeInstant.from_string("2024-06-18T20:00:00Z").epoch_time

        tp = TimePeriod(start="now", end="2025-06-18T20:00:00Z")
        assert tp is not None
        assert tp.start == "now"
        assert tp.end.epoch_time == TimeInstant.from_string("2025-06-18T20:00:00Z").epoch_time

        tp = TimePeriod(start="2024-06-18T20:00:00Z", end="now")
        assert tp is not None
        assert tp.start.epoch_time == TimeInstant.from_string("2024-06-18T20:00:00Z").epoch_time
        assert tp.end == "now"

        # tp = TimePeriod(start="now", end="now")

    def test_oshconnect_create(self):
        app = OSHConnect(name="Test OSH Connect")
        assert app is not None
        assert app.get_name() == "Test OSH Connect"

    def test_oshconnect_add_node(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="http://localhost", port=self.TEST_PORT, protocol="http", username="admin", password="admin")
        # node.add_basicauth("admin", "admin")
        app.add_node(node)
        assert len(app._nodes) == 1
        assert app._nodes[0] == node

    def test_find_systems(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="localhost", port=self.TEST_PORT, username="admin", password="admin", protocol="http")
        # node.add_basicauth("admin", "admin")
        app.add_node(node)
        app.discover_systems()
        print(f'Found systems: {app._systems}')
        # assert len(systems) == 1
        # assert systems[0] == node.get_api_endpoint()

    def test_oshconnect_find_datastreams(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="localhost", port=self.TEST_PORT, username="admin", password="admin", protocol="http")
        app.add_node(node)
        app.discover_systems()

        app.discover_datastreams()
        assert len(app._datafeeds) > 0

    async def test_obs_ws_stream(self):
        ds_url = ("ws://localhost:8585/sensorhub/api/datastreams/e07n5sbjqvalm/observations?f=application%2Fjson"
                  "&resultTime=latest/2025-06-18T15:46:32Z")

        # stream = requests.get(ds_url, stream=True, auth=('admin', 'admin'))
        async with websockets.connect(ds_url, extra_headers={'Authorization': 'Basic YWRtaW46YWRtaW4='}) as stream:
            async for message in stream:
                print(message)
