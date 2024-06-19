#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import pytest

from oshconnect.oshconnect import OSHConnect
from oshconnect.datamodels.datamodels import Node, System


class TestOSHConnect:

    def test_oshconnect_create(self):
        app = OSHConnect(name="Test OSH Connect")
        assert app is not None
        assert app.get_name() == "Test OSH Connect"

    def test_oshconnect_add_node(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="http://localhost", port=8585)
        # node.add_basicauth("admin", "admin")
        app.add_node(node)
        assert len(app._nodes) == 1
        assert app._nodes[0] == node

    def test_find_systems(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="http://localhost", port=8585, username="admin", password="admin")
        # node.add_basicauth("admin", "admin")
        app.add_node(node)
        app.discover_systems()
        print(f'Found systems: {app._systems}')
        # assert len(systems) == 1
        # assert systems[0] == node.get_api_endpoint()

    def test_oshconnect_find_datastreams(self):
        app = OSHConnect(name="Test OSH Connect")
        node = Node(address="http://localhost", port=8585, username="admin", password="admin")
        app.add_node(node)
        app.discover_systems()

        app.discover_datastreams()
        assert len(app._datafeeds) > 0

