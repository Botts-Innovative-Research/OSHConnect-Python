"""
Tests verifying that each resource type produces the correct MQTT topic strings
per the CS API Part 3 pub/sub conventions.

Topic format (Resource Data Topic):
  /{api_root}/{resource_type}/{resource_id}/{subresource_type}:data

Event topic format (Resource Event Topic):
  /{api_root}/{resource_type}/{resource_id}
  /{api_root}/{parent_type}/{parent_id}/{resource_type}/{resource_id}  (with parent)
"""
import pytest
from unittest.mock import MagicMock

from src.oshconnect.csapi4py.constants import APIResourceTypes
from src.oshconnect.csapi4py.default_api_helpers import APIHelper
from src.oshconnect.resource_datamodels import DatastreamResource, ControlStreamResource, SystemResource
from src.oshconnect.streamableresource import Datastream, ControlStream, System

DS_ID = "ds_test_001"
CS_ID = "cs_test_001"
SYS_ID = "sys_test_001"
PARENT_SYS_ID = "sys_parent_001"


def make_mock_node(api_root="api", mqtt_topic_root=None):
    """Returns a mock Node backed by a real APIHelper so topic construction is exercised."""
    api_helper = APIHelper(
        server_url="localhost",
        port=8282,
        protocol="http",
        server_root="sensorhub",
        api_root=api_root,
        mqtt_topic_root=mqtt_topic_root,
    )
    node = MagicMock()
    node.get_api_helper.return_value = api_helper
    node.get_mqtt_client.return_value = None
    return node


def make_datastream(node=None):
    if node is None:
        node = make_mock_node()
    ds_resource = DatastreamResource.model_validate({
        "id": DS_ID,
        "name": "Test Datastream",
        "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
    })
    return Datastream(parent_node=node, datastream_resource=ds_resource)


def make_controlstream(node=None):
    if node is None:
        node = make_mock_node()
    cs_resource = ControlStreamResource.model_validate({
        "id": CS_ID,
        "name": "Test ControlStream",
    })
    return ControlStream(node=node, controlstream_resource=cs_resource)


def make_system(node=None):
    if node is None:
        node = make_mock_node()
    sys = System(name="test_system", label="Test System", urn="urn:test:system", parent_node=node,
                 resource_id=SYS_ID)
    return sys


class TestDatastreamTopics:
    def test_observation_data_topic(self):
        ds = make_datastream()
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        assert topic == f"api/datastreams/{DS_ID}/observations:data"

    def test_event_topic_no_parent(self):
        ds = make_datastream()
        topic = ds.get_event_topic()
        assert topic == f"api/datastreams/{DS_ID}"

    def test_event_topic_with_parent_system(self):
        ds = make_datastream()
        ds.set_parent_resource_id(PARENT_SYS_ID)
        topic = ds.get_event_topic()
        assert topic == f"api/systems/{PARENT_SYS_ID}/datastreams/{DS_ID}"

    def test_init_mqtt_sets_correct_topic(self):
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        ds = make_datastream(node)
        ds.init_mqtt()

        assert ds._topic == f"api/datastreams/{DS_ID}/observations:data"


class TestControlStreamTopics:
    def test_command_data_topic(self):
        cs = make_controlstream()
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)
        assert topic == f"api/controlstreams/{CS_ID}/commands:data"

    def test_status_data_topic(self):
        cs = make_controlstream()
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.STATUS, data_topic=True)
        assert topic == f"api/controlstreams/{CS_ID}/status:data"

    def test_status_topic_set_on_init(self):
        """_status_topic is assigned in __init__ before any explicit init_mqtt call."""
        cs = make_controlstream()
        assert cs._status_topic == f"api/controlstreams/{CS_ID}/status:data"

    def test_init_mqtt_sets_command_topic(self):
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        cs = make_controlstream(node)
        cs.init_mqtt()

        assert cs._topic == f"api/controlstreams/{CS_ID}/commands:data"

    def test_event_topic_no_parent(self):
        cs = make_controlstream()
        topic = cs.get_event_topic()
        assert topic == f"api/controlstreams/{CS_ID}"

    def test_event_topic_with_parent_system(self):
        cs = make_controlstream()
        cs.set_parent_resource_id(PARENT_SYS_ID)
        topic = cs.get_event_topic()
        assert topic == f"api/systems/{PARENT_SYS_ID}/controlstreams/{CS_ID}"

    def test_publish_routes_command_to_command_topic(self):
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        cs = make_controlstream(node)
        cs.init_mqtt()
        cs.publish("payload", topic=APIResourceTypes.COMMAND.value)

        mock_mqtt.publish.assert_called_once_with(
            f"api/controlstreams/{CS_ID}/commands:data", "payload", qos=0
        )

    def test_publish_routes_status_to_status_topic(self):
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        cs = make_controlstream(node)
        cs.init_mqtt()
        cs.publish("payload", topic=APIResourceTypes.STATUS.value)

        mock_mqtt.publish.assert_called_once_with(
            f"api/controlstreams/{CS_ID}/status:data", "payload", qos=0
        )


class TestSystemTopics:
    def test_system_data_topic(self):
        sys = make_system()
        topic = sys.get_mqtt_topic(subresource=None, data_topic=True)
        assert topic == "api/systems:data"

    def test_system_event_topic(self):
        sys = make_system()
        topic = sys.get_event_topic()
        assert topic == f"api/systems/{SYS_ID}"

    def test_system_datastream_subresource_topic(self):
        sys = make_system()
        topic = sys.get_mqtt_topic(subresource=APIResourceTypes.DATASTREAM, data_topic=True)
        assert topic == f"api/systems/{SYS_ID}/datastreams:data"

    def test_system_controlstream_subresource_topic(self):
        sys = make_system()
        topic = sys.get_mqtt_topic(subresource=APIResourceTypes.CONTROL_CHANNEL, data_topic=True)
        assert topic == f"api/systems/{SYS_ID}/controlstreams:data"


class TestCustomApiRoot:
    """Verify that a non-default api_root (with no separate mqtt_topic_root) propagates into all topic strings."""

    CUSTOM_ROOT = "connected-systems"

    def make_node(self):
        return make_mock_node(api_root=self.CUSTOM_ROOT)

    def test_datastream_data_topic(self):
        ds = make_datastream(self.make_node())
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        assert topic == f"{self.CUSTOM_ROOT}/datastreams/{DS_ID}/observations:data"

    def test_datastream_event_topic(self):
        ds = make_datastream(self.make_node())
        topic = ds.get_event_topic()
        assert topic == f"{self.CUSTOM_ROOT}/datastreams/{DS_ID}"

    def test_controlstream_command_topic(self):
        cs = make_controlstream(self.make_node())
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)
        assert topic == f"{self.CUSTOM_ROOT}/controlstreams/{CS_ID}/commands:data"

    def test_controlstream_status_topic(self):
        cs = make_controlstream(self.make_node())
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.STATUS, data_topic=True)
        assert topic == f"{self.CUSTOM_ROOT}/controlstreams/{CS_ID}/status:data"

    def test_system_event_topic(self):
        sys = make_system(self.make_node())
        topic = sys.get_event_topic()
        assert topic == f"{self.CUSTOM_ROOT}/systems/{SYS_ID}"


class TestIndependentMqttTopicRoot:
    """
    Verify that mqtt_topic_root overrides api_root for MQTT topics while leaving
    the HTTP api_root untouched.
    """

    HTTP_ROOT = "api"
    MQTT_ROOT = "sensorhub/mqtt"

    def make_node(self):
        return make_mock_node(api_root=self.HTTP_ROOT, mqtt_topic_root=self.MQTT_ROOT)

    def test_mqtt_root_used_for_datastream_data_topic(self):
        ds = make_datastream(self.make_node())
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        assert topic == f"{self.MQTT_ROOT}/datastreams/{DS_ID}/observations:data"

    def test_mqtt_root_used_for_datastream_event_topic(self):
        ds = make_datastream(self.make_node())
        topic = ds.get_event_topic()
        assert topic == f"{self.MQTT_ROOT}/datastreams/{DS_ID}"

    def test_mqtt_root_used_for_controlstream_command_topic(self):
        cs = make_controlstream(self.make_node())
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)
        assert topic == f"{self.MQTT_ROOT}/controlstreams/{CS_ID}/commands:data"

    def test_mqtt_root_used_for_controlstream_status_topic(self):
        cs = make_controlstream(self.make_node())
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.STATUS, data_topic=True)
        assert topic == f"{self.MQTT_ROOT}/controlstreams/{CS_ID}/status:data"

    def test_mqtt_root_used_for_system_event_topic(self):
        sys = make_system(self.make_node())
        topic = sys.get_event_topic()
        assert topic == f"{self.MQTT_ROOT}/systems/{SYS_ID}"

    def test_http_api_root_unaffected(self):
        """api_root must not change when mqtt_topic_root is set independently."""
        node = self.make_node()
        assert node.get_api_helper().api_root == self.HTTP_ROOT
        assert node.get_api_helper().get_mqtt_root() == self.MQTT_ROOT
