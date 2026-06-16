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

from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.csapi4py.default_api_helpers import APIHelper
from tests import helpers

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


# Thin wrappers over the shared factories in ``tests.helpers`` that fill in
# this module's mock node and topic-id constants.

def make_datastream(node=None):
    return helpers.make_datastream(node or make_mock_node(), ds_id=DS_ID)


def make_controlstream(node=None):
    return helpers.make_controlstream(node or make_mock_node(), cs_id=CS_ID)


def make_system(node=None):
    return helpers.make_system(node or make_mock_node(), resource_id=SYS_ID)


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
        """_status_topic is assigned in __init__ before any explicit
        init_mqtt call. Status payloads are always JSON, so the topic
        carries the ``/json`` format subtopic."""
        cs = make_controlstream()
        assert cs._status_topic == f"api/controlstreams/{CS_ID}/status:data/json"

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
            f"api/controlstreams/{CS_ID}/status:data/json", "payload", qos=0
        )

    def test_publish_default_topic_routes_to_command_topic(self):
        """Regression: prior to the topic-default fix, calling
        ``cs.publish(payload)`` with no topic argument used the lowercase
        default ``'command'`` which never matched
        ``APIResourceTypes.COMMAND.value`` (``'Command'``) and raised
        ``ValueError`` instead of publishing. The default must canonicalize
        on the enum value."""
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        cs = make_controlstream(node)
        cs.init_mqtt()
        cs.publish("payload")  # no topic argument — must hit the command path

        mock_mqtt.publish.assert_called_once_with(
            f"api/controlstreams/{CS_ID}/commands:data", "payload", qos=0
        )

    def test_publish_unknown_topic_error_names_canonical_values(self):
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        cs = make_controlstream(node)
        cs.init_mqtt()
        with pytest.raises(ValueError) as excinfo:
            cs.publish("payload", topic="command")  # lowercase — invalid
        msg = str(excinfo.value)
        assert "'Command'" in msg and "'Status'" in msg

    def test_subscribe_default_topic_routes_to_command_topic(self):
        node = make_mock_node()
        mock_mqtt = MagicMock()
        node.get_mqtt_client.return_value = mock_mqtt

        cs = make_controlstream(node)
        cs.init_mqtt()
        cs.subscribe()  # topic=None default

        mock_mqtt.subscribe.assert_called_once()
        args, kwargs = mock_mqtt.subscribe.call_args
        assert args[0] == f"api/controlstreams/{CS_ID}/commands:data"

    def test_subscribe_unknown_topic_error_names_canonical_values(self):
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        cs = make_controlstream(node)
        cs.init_mqtt()
        with pytest.raises(ValueError) as excinfo:
            cs.subscribe(topic="command")  # lowercase — invalid
        msg = str(excinfo.value)
        assert "'Command'" in msg and "'Status'" in msg and "None" in msg


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


class TestDataTopicFormatSubtopic:
    """CS API Part 3 §Resource Data Messages Content Negotiation — the
    optional ``:data/<token>`` subtopic selects the wire format. Mirrors
    the Java reference ``ConSysTopicValidator.FORMAT_SUBTOPICS``."""

    @pytest.mark.parametrize("content_type,token", [
        ("application/json",       "json"),
        ("application/swe+json",   "swe-json"),
        ("application/swe+binary", "swe-binary"),
        ("application/swe+csv",    "swe-csv"),
        ("application/swe+proto",  "swe-proto"),
        ("application/om+json",    "om-json"),
        ("application/sml+json",   "sml-json"),
    ])
    def test_format_token_mapping(self, content_type, token):
        from oshconnect.csapi4py.mqtt import mqtt_topic_format_token
        assert mqtt_topic_format_token(content_type) == token

    def test_unknown_format_raises_value_error(self):
        from oshconnect.csapi4py.mqtt import mqtt_topic_format_token
        with pytest.raises(ValueError, match="No MQTT topic-format token"):
            mqtt_topic_format_token("application/swe+protobuf")

    def test_get_mqtt_topic_omits_format_when_none(self):
        """``format=None`` (default) emits bare ``:data`` so the server's
        default format applies. Preserves prior behavior for any callers
        that don't know the wire format."""
        helper = make_mock_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id=DS_ID,
            data_topic=True,
        )
        assert topic == f"api/datastreams/{DS_ID}/observations:data"

    def test_get_mqtt_topic_appends_format_when_provided(self):
        helper = make_mock_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id=DS_ID,
            data_topic=True,
            format="application/swe+binary",
        )
        assert topic == f"api/datastreams/{DS_ID}/observations:data/swe-binary"

    def test_get_mqtt_topic_raises_for_unknown_format(self):
        helper = make_mock_node().get_api_helper()
        with pytest.raises(ValueError, match="No MQTT topic-format token"):
            helper.get_mqtt_topic(
                resource_type=APIResourceTypes.DATASTREAM,
                subresource_type=APIResourceTypes.OBSERVATION,
                resource_id=DS_ID,
                data_topic=True,
                format="application/swe+protobuf",
            )

    def test_get_mqtt_topic_ignores_format_on_event_topic(self):
        """Event topics (no ``:data`` suffix) never carry a format
        subtopic — the format param is silently ignored."""
        helper = make_mock_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.SYSTEM,
            subresource_type=APIResourceTypes.DATASTREAM,
            resource_id=SYS_ID,
            data_topic=False,
            format="application/swe+binary",
        )
        assert topic == f"api/systems/{SYS_ID}/datastreams"

    def test_datastream_init_mqtt_with_swe_binary_schema_appends_token(self):
        """When a Datastream carries a swe+binary record_schema,
        init_mqtt() builds a topic with the matching format subtopic."""
        from oshconnect.schema_datamodels import SWEBinaryDatastreamRecordSchema
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        ds = make_datastream(node)
        ds._underlying_resource.record_schema = (
            SWEBinaryDatastreamRecordSchema.model_construct(
                obs_format="application/swe+binary",
            )
        )
        ds.init_mqtt()
        assert ds._topic == f"api/datastreams/{DS_ID}/observations:data/swe-binary"

    def test_datastream_init_mqtt_with_swe_json_schema_appends_token(self):
        from oshconnect.schema_datamodels import SWEDatastreamRecordSchema
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        ds = make_datastream(node)
        ds._underlying_resource.record_schema = (
            SWEDatastreamRecordSchema.model_construct(
                obs_format="application/swe+json",
            )
        )
        ds.init_mqtt()
        assert ds._topic == f"api/datastreams/{DS_ID}/observations:data/swe-json"

    def test_datastream_init_mqtt_without_schema_stays_bare(self):
        """No record_schema → no known format → bare ``:data`` topic so
        the server's default applies."""
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        ds = make_datastream(node)
        assert ds._underlying_resource.record_schema is None
        ds.init_mqtt()
        assert ds._topic == f"api/datastreams/{DS_ID}/observations:data"

    def test_controlstream_init_mqtt_with_swe_json_schema_appends_token(self):
        from oshconnect.schema_datamodels import SWEJSONCommandSchema
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        cs = make_controlstream(node)
        cs._underlying_resource.command_schema = (
            SWEJSONCommandSchema.model_construct(
                command_format="application/swe+json",
            )
        )
        cs.init_mqtt()
        assert cs._topic == f"api/controlstreams/{CS_ID}/commands:data/swe-json"

    def test_controlstream_init_mqtt_with_json_command_schema_appends_token(self):
        from oshconnect.schema_datamodels import JSONCommandSchema
        node = make_mock_node()
        node.get_mqtt_client.return_value = MagicMock()
        cs = make_controlstream(node)
        cs._underlying_resource.command_schema = (
            JSONCommandSchema.model_construct(
                command_format="application/json",
            )
        )
        cs.init_mqtt()
        assert cs._topic == f"api/controlstreams/{CS_ID}/commands:data/json"

    def test_controlstream_status_topic_always_uses_json_token(self):
        """Status payloads are always JSON regardless of the command
        format, so the status topic is always suffixed with ``/json``."""
        cs = make_controlstream()
        assert cs._status_topic == f"api/controlstreams/{CS_ID}/status:data/json"

    def test_custom_mqtt_topic_root_preserved_with_format(self):
        """Format subtopic stacks correctly when a custom mqtt_topic_root
        is in play — the suffix is appended after ``:data``, not after
        the topic root."""
        node = make_mock_node(api_root="api", mqtt_topic_root="osh/mqtt")
        helper = node.get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id=DS_ID,
            data_topic=True,
            format="application/swe+binary",
        )
        assert topic == f"osh/mqtt/datastreams/{DS_ID}/observations:data/swe-binary"
