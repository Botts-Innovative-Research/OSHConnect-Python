from oshconnect.csapi4py import APIHelper


def test_url_generation():
    helper = APIHelper(server_url='localhost', port=8282, protocol='http', username='admin', password='admin')
    expected_url = "http://localhost:8282/sensorhub/api"
    url = helper.get_api_root_url()
    assert url == expected_url
    expected_url = "ws://localhost:8282/sensorhub/api"
    url = helper.get_api_root_url(socket=True)
    assert url == expected_url
    helper.set_protocol('https')
    expected_url = "https://localhost:8282/sensorhub/api"
    url = helper.get_api_root_url()
    assert url == expected_url
    expected_url = "wss://localhost:8282/sensorhub/api"
    url = helper.get_api_root_url(socket=True)
    assert url == expected_url
