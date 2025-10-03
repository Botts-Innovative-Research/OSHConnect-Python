from src.oshconnect import OSHConnect, Node


def test_streamble_observations():
    app = OSHConnect("Test App")
    node = Node(address="localhost", port=8282, username="admin", password="admin", protocol="http")
    app.add_node(node)
    app.discover_systems()
    app.discover_datastreams()

    datastreams = app.get_datastreams()
    print(datastreams)