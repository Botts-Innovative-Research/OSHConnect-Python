"""Node and APIHelper basics: URL construction and (de)serialization."""
from oshconnect import Node
from oshconnect.csapi4py import APIHelper


def test_apihelper_url_generation():
    helper = APIHelper(server_url='localhost', port=8282, protocol='http',
                       username='admin', password='admin')

    assert helper.get_api_root_url() == "http://localhost:8282/sensorhub/api"
    assert helper.get_api_root_url(socket=True) == "ws://localhost:8282/sensorhub/api"

    helper.set_protocol('https')
    assert helper.get_api_root_url() == "https://localhost:8282/sensorhub/api"
    assert helper.get_api_root_url(socket=True) == "wss://localhost:8282/sensorhub/api"


def test_node_password_round_trips_through_storage_dict():
    node = Node(protocol='http', address='localhost', port=8080,
                username='user', password='pass')
    stored = node.to_storage_dict()
    assert stored['password'] == 'pass'
    rehydrated = Node.from_storage_dict(stored)
    assert rehydrated._api_helper.password == 'pass'