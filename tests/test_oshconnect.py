"""OSHConnect application object: construction, node attachment, live discovery.

Tests marked `@pytest.mark.network` require a live OSH server at localhost:8282
(e.g. FakeWeatherDriver). Skip in CI; see `.github/workflows/tests.yaml`.
"""
import pytest

from oshconnect import Node, OSHConnect
from tests.helpers import osh_node_reachable

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

@pytest.fixture
def live_app() -> OSHConnect:
    """An OSHConnect wired to the live node — skips (instead of hanging)
    when no server answers on :8282."""
    if not osh_node_reachable(TEST_PORT):
        pytest.skip(f"OSH node not reachable at localhost:{TEST_PORT}")
    app = OSHConnect(name="Test OSH Connect")
    app.add_node(Node(address="localhost", port=TEST_PORT, username="admin",
                      password="admin", protocol="http"))
    return app


@pytest.mark.network
def test_discover_systems_against_live_node(live_app):
    live_app.discover_systems()
    print(f'Found systems: {live_app._systems}')


@pytest.mark.network
def test_discover_datastreams_against_live_node(live_app):
    live_app.discover_systems()
    live_app.discover_datastreams()
    assert len(live_app._datastreams) > 0


@pytest.mark.network
def test_discover_then_get_datastreams_returns_list(live_app):
    live_app.discover_systems()
    live_app.discover_datastreams()
    datastreams = live_app.get_datastreams()
    print(datastreams)
