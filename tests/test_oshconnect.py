"""OSHConnect application object: construction, node attachment, live discovery.

Tests marked `@pytest.mark.network` require a live OSH server at localhost:8282
(e.g. FakeWeatherDriver). Skip in CI; see `.github/workflows/tests.yaml`.
"""
import pytest

from oshconnect import Node, OSHConnect

TEST_PORT = 8282


def test_oshconnect_constructs_with_name():
    app = OSHConnect(name="Test OSH Connect")
    assert app.get_name() == "Test OSH Connect"


def test_oshconnect_add_node_appends_to_nodes_list():
    app = OSHConnect(name="Test OSH Connect")
    node = Node(address="http://localhost", port=TEST_PORT, protocol="http",
                username="admin", password="admin")
    app.add_node(node)
    assert len(app._nodes) == 1
    assert app._nodes[0] is node


# ---------------------------------------------------------------------------
# Live-server tests (network-marked)
# ---------------------------------------------------------------------------

@pytest.mark.network
def test_discover_systems_against_live_node():
    app = OSHConnect(name="Test OSH Connect")
    node = Node(address="localhost", port=TEST_PORT, username="admin",
                password="admin", protocol="http")
    app.add_node(node)
    app.discover_systems()
    print(f'Found systems: {app._systems}')


@pytest.mark.network
def test_discover_datastreams_against_live_node():
    app = OSHConnect(name="Test OSH Connect")
    node = Node(address="localhost", port=TEST_PORT, username="admin",
                password="admin", protocol="http")
    app.add_node(node)
    app.discover_systems()
    app.discover_datastreams()
    assert len(app._datastreams) > 0


@pytest.mark.network
def test_discover_then_get_datastreams_returns_list():
    app = OSHConnect("Test App")
    node = Node(address="localhost", port=TEST_PORT, username="admin",
                password="admin", protocol="http")
    app.add_node(node)
    app.discover_systems()
    app.discover_datastreams()
    datastreams = app.get_datastreams()
    print(datastreams)