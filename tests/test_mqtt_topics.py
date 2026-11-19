"""
Tests verifying that each resource type produces the correct MQTT topic strings
for each topic scheme.

``part3`` (default), CS API Part 3 nested under the system path:
  {root}/systems/{sysId}/datastreams/{dsId}/observations:data[/{format}]
  {root}/systems/{sysId}/controlstreams/{csId}/commands:data[/{format}]
  {root}/systems/{sysId}/controlstreams/{csId}/commands/{cmdId}/status:data
  {root}/systems/{sysId}/datastreams/{dsId}                (Resource Event)

``part3-flat``, the same without the system path:
  {root}/datastreams/{dsId}/observations:data[/{format}]

``legacy``, pre-Part-3:
  /{root}/datastreams/{dsId}/observations
"""
import pytest
from unittest.mock import MagicMock

from oshconnect import ConfigurationError, Datastream, Node, System
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.csapi4py.default_api_helpers import APIHelper
from oshconnect.resource_datamodels import DatastreamResource
from tests import helpers

DS_ID = "ds_test_001"
CS_ID = "cs_test_001"
SYS_ID = "sys_test_001"
PARENT_SYS_ID = "sys_parent_001"


def make_mock_node(api_root="api", mqtt_topic_root=None, legacy_topics=False,
                   topic_scheme="part3-flat"):
    """Returns a mock Node backed by a real APIHelper so topic construction is exercised.

    Defaults to the ``part3-flat`` scheme: the classes below pin the topics
    that scheme (and ``legacy``) must keep producing. The default ``part3``
    scheme is covered by ``TestPart3NestedTopics``.
    """
    if legacy_topics:
        topic_scheme = None
    api_helper = APIHelper(
        server_url="localhost",
        port=8282,
        protocol="http",
        server_root="sensorhub",
        api_root=api_root,
        mqtt_topic_root=mqtt_topic_root,
        legacy_topics=legacy_topics,
        topic_scheme=topic_scheme,
    )
    node = MagicMock()
    node.get_api_helper.return_value = api_helper
    node.get_mqtt_client.return_value = None
    # StreamableResource sources its transport via get_comm_client(); alias it
    # to get_mqtt_client so per-test `get_mqtt_client.return_value = ...` wiring
    # continues to drive the resource's client (these tests exercise MQTT).
    node.get_comm_client = node.get_mqtt_client
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
            mqtt_topic_format_token("text/csv")

    @pytest.mark.parametrize("content_type,token", [
        ("application/geo+json", "geo-json"),
        ("application/sml+xml", "sml-xml"),
        ("application/swe+xml", "swe-xml"),
        # custom formats follow the Part 3 normalization rule
        ("application/x-custom+json", "x-custom-json"),
        ("application/X-Custom+JSON; version=2", "x-custom-json"),
        ("application/swe+protobuf", "swe-protobuf"),
        ("application/vnd.acme.thing+json", "vnd.acme.thing-json"),
    ])
    def test_format_token_follows_part3_rule(self, content_type, token):
        from oshconnect.csapi4py.mqtt import mqtt_topic_format_token
        assert mqtt_topic_format_token(content_type) == token

    @pytest.mark.parametrize("content_type", [
        "text/csv", "image/png", "", None, "application/a#b",
    ])
    def test_format_without_a_valid_token_raises(self, content_type):
        from oshconnect.csapi4py.mqtt import mqtt_topic_format_token
        with pytest.raises(ValueError, match="No MQTT topic-format token"):
            mqtt_topic_format_token(content_type)

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

    def test_get_mqtt_topic_raises_for_format_without_token(self):
        helper = make_mock_node().get_api_helper()
        with pytest.raises(ValueError, match="No MQTT topic-format token"):
            helper.get_mqtt_topic(
                resource_type=APIResourceTypes.DATASTREAM,
                subresource_type=APIResourceTypes.OBSERVATION,
                resource_id=DS_ID,
                data_topic=True,
                format="text/csv",
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


class TestLegacyTopics:
    """Pre-Part-3 ("legacy") topic form: leading slash, no ``:data`` suffix,
    no format subtopic — for backwards compatibility with older OSH servers."""

    def test_datastream_observation_legacy_topic(self):
        ds = make_datastream(make_mock_node(legacy_topics=True))
        # Format is ignored in legacy mode.
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION,
                                  data_topic=True, format="application/swe+binary")
        assert topic == f"/api/datastreams/{DS_ID}/observations"

    def test_controlstream_command_legacy_topic(self):
        cs = make_controlstream(make_mock_node(legacy_topics=True))
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, data_topic=True)
        assert topic == f"/api/controlstreams/{CS_ID}/commands"

    def test_controlstream_status_legacy_topic(self):
        cs = make_controlstream(make_mock_node(legacy_topics=True))
        topic = cs.get_mqtt_topic(subresource=APIResourceTypes.STATUS,
                                  data_topic=True, format="application/json")
        assert topic == f"/api/controlstreams/{CS_ID}/status"

    def test_system_datastreams_collection_legacy_topic(self):
        sysres = make_system(make_mock_node(legacy_topics=True))
        topic = sysres.get_mqtt_topic(subresource=APIResourceTypes.DATASTREAM)
        assert topic == f"/api/systems/{SYS_ID}/datastreams"

    def test_init_mqtt_sets_legacy_topics(self):
        node = make_mock_node(legacy_topics=True)
        node.get_mqtt_client.return_value = MagicMock()

        ds = make_datastream(node)
        ds.init_mqtt()
        assert ds._topic == f"/api/datastreams/{DS_ID}/observations"
        # Subscribe topic mirrors publish topic in legacy MQTT (no wildcard).
        assert ds._subscribe_topic == f"/api/datastreams/{DS_ID}/observations"

    def test_controlstream_init_mqtt_sets_legacy_topics(self):
        node = make_mock_node(legacy_topics=True)
        node.get_mqtt_client.return_value = MagicMock()

        cs = make_controlstream(node)
        cs.init_mqtt()
        assert cs._topic == f"/api/controlstreams/{CS_ID}/commands"
        assert cs._status_topic == f"/api/controlstreams/{CS_ID}/status"

    def test_flat_scheme_is_not_legacy(self):
        ds = make_datastream(make_mock_node())
        topic = ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, data_topic=True)
        assert topic == f"api/datastreams/{DS_ID}/observations:data"

    def test_per_call_legacy_override(self):
        """Callers can force legacy per-call whatever the helper's scheme."""
        node = make_mock_node()  # part3-flat
        helper = node.get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id=DS_ID, data_topic=True, legacy=True,
        )
        assert topic == f"/api/datastreams/{DS_ID}/observations"


# ---------------------------------------------------------------------------
# Default ``part3`` scheme: topics nested under the system path.
#
# The expected strings are the ones the server accepts or rejects in
# osh-addons ``sensorhub-service-csapi-mqtt`` (``TestTopicMapper``), so a
# mismatch here means the client and the server disagree.
# ---------------------------------------------------------------------------

def part3_node(mqtt_topic_root="node1"):
    return make_mock_node(mqtt_topic_root=mqtt_topic_root, topic_scheme="part3")


def part3_datastream(system_id="134", node=None):
    ds = helpers.make_datastream(node or part3_node(), ds_id="12")
    if system_id is not None:
        ds.set_parent_resource_id(system_id)
    return ds


def part3_controlstream(system_id="134", node=None):
    cs = helpers.make_controlstream(node or part3_node(), cs_id="3")
    if system_id is not None:
        cs.set_parent_resource_id(system_id)
    return cs


class TestTopicScheme:
    def test_default_scheme_is_part3(self):
        assert APIHelper().get_topic_scheme() == "part3"

    def test_node_default_scheme_is_part3(self):
        node = Node(protocol="http", address="localhost", port=8282)
        assert node.get_api_helper().get_topic_scheme() == "part3"

    def test_legacy_flag_selects_legacy_scheme(self):
        node = Node(protocol="http", address="localhost", port=8282, mqtt_legacy_topics=True)
        assert node.get_api_helper().get_topic_scheme() == "legacy"

    @pytest.mark.parametrize("scheme", ["part3", "part3-flat", "legacy"])
    def test_node_accepts_each_scheme(self, scheme):
        node = Node(protocol="http", address="localhost", port=8282, mqtt_topic_scheme=scheme)
        assert node.get_api_helper().get_topic_scheme() == scheme

    def test_unknown_scheme_raises(self):
        with pytest.raises(ConfigurationError, match="Unknown MQTT topic scheme"):
            Node(protocol="http", address="localhost", port=8282, mqtt_topic_scheme="flat")

    def test_legacy_flag_conflicting_with_scheme_raises(self):
        with pytest.raises(ConfigurationError, match="conflicts"):
            Node(protocol="http", address="localhost", port=8282,
                 mqtt_legacy_topics=True, mqtt_topic_scheme="part3")

    def test_legacy_flag_with_legacy_scheme_is_allowed(self):
        node = Node(protocol="http", address="localhost", port=8282,
                    mqtt_legacy_topics=True, mqtt_topic_scheme="legacy")
        assert node.get_api_helper().get_topic_scheme() == "legacy"

    @pytest.mark.parametrize("scheme", ["part3", "part3-flat", "legacy"])
    def test_scheme_survives_storage_round_trip(self, scheme):
        node = Node(protocol="http", address="localhost", port=8282, mqtt_topic_scheme=scheme)
        stored = node.to_storage_dict()
        assert stored["mqtt_topic_scheme"] == scheme
        assert Node.from_storage_dict(stored).get_api_helper().get_topic_scheme() == scheme

    def test_config_without_scheme_loads_as_part3(self):
        stored = Node(protocol="http", address="localhost", port=8282).to_storage_dict()
        del stored["mqtt_topic_scheme"]
        assert Node.from_storage_dict(stored).get_api_helper().get_topic_scheme() == "part3"


class TestPart3NestedTopics:
    def test_observation_data_topic(self):
        ds = part3_datastream()
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                == "node1/systems/134/datastreams/12/observations:data")

    def test_observation_data_topic_with_format(self):
        ds = part3_datastream()
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION, format="application/swe+csv")
                == "node1/systems/134/datastreams/12/observations:data/swe-csv")

    def test_datastream_parent_from_system_id_field(self):
        """A datastream read from the server carries ``system@id``."""
        res = DatastreamResource.model_validate({
            "id": "12", "name": "d", "system@id": "134",
            "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]})
        ds = Datastream(parent_node=part3_node(), datastream_resource=res)
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                == "node1/systems/134/datastreams/12/observations:data")

    def test_datastream_system_id_field_wins_over_parent_id(self):
        res = DatastreamResource.model_validate({
            "id": "12", "name": "d", "system@id": "134",
            "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]})
        ds = Datastream(parent_node=part3_node(), datastream_resource=res)
        ds.set_parent_resource_id("other")
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                == "node1/systems/134/datastreams/12/observations:data")

        # data and event topics resolve the system the same way
        assert ds.get_event_topic() == "node1/systems/134/datastreams/12"

    def test_command_data_topic(self):
        cs = part3_controlstream()
        assert (cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND)
                == "node1/systems/134/controlstreams/3/commands:data")

    def test_command_status_topic_is_per_command(self):
        cs = part3_controlstream()
        assert (cs.get_mqtt_status_topic("99")
                == "node1/systems/134/controlstreams/3/commands/99/status:data")

    def test_subsystem_path(self):
        helper = part3_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM, subresource_type=APIResourceTypes.OBSERVATION,
            resource_id="12", system_path=["134", "7", "8"])
        assert topic == "node1/systems/134/subsystems/7/subsystems/8/datastreams/12/observations:data"

    def test_subsystem_path_for_command_status(self):
        helper = part3_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.CONTROL_CHANNEL, subresource_type=APIResourceTypes.STATUS,
            resource_id="3", subresource_id="99", system_path=["134", "7"])
        assert topic == "node1/systems/134/subsystems/7/controlstreams/3/commands/99/status:data"

    def test_system_path_accepts_a_single_id(self):
        helper = part3_node().get_api_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM, subresource_type=APIResourceTypes.OBSERVATION,
            resource_id="2", system_path="1", format="application/swe+json")
        assert topic == "node1/systems/1/datastreams/2/observations:data/swe-json"

    def test_system_collection_topics(self):
        sysres = helpers.make_system(part3_node(), resource_id="134")
        assert sysres.get_mqtt_topic() == "node1/systems:data"
        assert (sysres.get_mqtt_topic(subresource=APIResourceTypes.DATASTREAM)
                == "node1/systems/134/datastreams:data")
        assert (sysres.get_mqtt_topic(subresource=APIResourceTypes.CONTROL_CHANNEL)
                == "node1/systems/134/controlstreams:data")

    def test_event_topics(self):
        assert part3_datastream().get_event_topic() == "node1/systems/134/datastreams/12"
        assert part3_controlstream().get_event_topic() == "node1/systems/134/controlstreams/3"
        assert helpers.make_system(part3_node(), resource_id="134").get_event_topic() == "node1/systems/134"

    def test_default_root_is_api(self):
        ds = part3_datastream(node=part3_node(mqtt_topic_root=None))
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                == "api/systems/134/datastreams/12/observations:data")

    # -- forms the server rejects are never built ---------------------------

    def test_datastream_without_parent_raises(self):
        """The server rejects ``node1/datastreams/2/observations:data``."""
        ds = part3_datastream(system_id=None)
        with pytest.raises(ValueError, match="parent system id"):
            ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)

    def test_controlstream_without_parent_raises(self):
        cs = part3_controlstream(system_id=None)
        with pytest.raises(ValueError, match="parent system id"):
            cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND)

    def test_event_topic_without_parent_raises(self):
        """The server rejects ``node1/datastreams/2``."""
        with pytest.raises(ValueError, match="parent system id"):
            part3_datastream(system_id=None).get_event_topic()
        with pytest.raises(ValueError, match="parent system id"):
            part3_controlstream(system_id=None).get_event_topic()

    def test_status_topic_without_command_id_raises(self):
        with pytest.raises(ValueError, match="per command"):
            part3_controlstream().get_mqtt_status_topic()

    def test_unknown_format_raises(self):
        with pytest.raises(ValueError, match="No MQTT topic-format token"):
            part3_datastream().get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION,
                                              format="text/csv")

    def test_custom_format_subtopic(self):
        ds = part3_datastream()
        assert (ds.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION,
                                  format="application/x-custom+json")
                == "node1/systems/134/datastreams/12/observations:data/x-custom-json")

    def test_command_format_subtopic(self):
        cs = part3_controlstream()
        assert (cs.get_mqtt_topic(subresource=APIResourceTypes.COMMAND, format="application/swe+json")
                == "node1/systems/134/controlstreams/3/commands:data/swe-json")

    def test_init_mqtt_without_parent_raises_when_a_client_exists(self):
        node = part3_node()
        node.get_mqtt_client.return_value = MagicMock()
        with pytest.raises(ValueError, match="parent system id"):
            part3_datastream(system_id=None, node=node).init_mqtt()
        with pytest.raises(ValueError, match="parent system id"):
            part3_controlstream(system_id=None, node=node).init_mqtt()

    def test_init_mqtt_without_parent_or_client_does_not_raise(self):
        """HTTP-only use: no pub/sub client, so the topics are never needed."""
        ds = part3_datastream(system_id=None)
        ds.init_mqtt()
        assert ds._topic is None and ds._subscribe_topic is None
        cs = part3_controlstream(system_id=None)
        cs.init_mqtt()
        assert cs._topic is None and cs._status_topic is None

    def test_constructing_without_parent_does_not_raise(self):
        """The parent id may be set after construction; only topic building needs it."""
        cs = part3_controlstream(system_id=None)
        assert cs._status_topic is None


class TestPart3ResourceWiring:
    def test_datastream_init_mqtt_sets_nested_topics(self):
        ds = part3_datastream()
        ds.init_mqtt()
        assert ds._topic == "node1/systems/134/datastreams/12/observations:data"
        assert ds._subscribe_topic == "node1/systems/134/datastreams/12/observations:data"

    def test_controlstream_init_mqtt_sets_command_topic_only(self):
        cs = part3_controlstream()
        cs.init_mqtt()
        assert cs._topic == "node1/systems/134/controlstreams/3/commands:data"
        assert cs._subscribe_topic == "node1/systems/134/controlstreams/3/commands:data"
        assert cs._status_topic is None
        assert cs._status_subscribe_topic is None

    def test_publish_command_uses_nested_topic(self):
        node = part3_node()
        client = MagicMock()
        node.get_mqtt_client.return_value = client
        cs = part3_controlstream(node=node)
        cs.init_mqtt()
        cs.publish_command("payload")
        client.publish.assert_called_once_with(
            "node1/systems/134/controlstreams/3/commands:data", "payload", qos=0)

    def test_publish_status_raises_and_sends_nothing(self):
        node = part3_node()
        client = MagicMock()
        node.get_mqtt_client.return_value = client
        cs = part3_controlstream(node=node)
        cs.init_mqtt()
        with pytest.raises(ValueError, match="subscribe-only"):
            cs.publish_status("payload")
        client.publish.assert_not_called()

    def test_subscribe_status_needs_command_id(self):
        node = part3_node()
        client = MagicMock()
        node.get_mqtt_client.return_value = client
        cs = part3_controlstream(node=node)
        cs.init_mqtt()
        with pytest.raises(ValueError, match="per command"):
            cs.subscribe(topic=APIResourceTypes.STATUS.value)
        client.subscribe.assert_not_called()

    def test_subscribe_status_with_command_id(self):
        node = part3_node()
        client = MagicMock()
        node.get_mqtt_client.return_value = client
        cs = part3_controlstream(node=node)
        cs.init_mqtt()
        cs.subscribe(topic=APIResourceTypes.STATUS.value, command_id="99")
        assert (client.subscribe.call_args.args[0]
                == "node1/systems/134/controlstreams/3/commands/99/status:data")

    def test_reloaded_streams_get_the_parent_system_id(self):
        """Configs saved before the parent id was recorded still build nested topics."""
        node = part3_node()
        system = helpers.make_system(node, resource_id="134")
        system.datastreams = [part3_datastream(system_id=None, node=node)]
        system.control_channels = [part3_controlstream(system_id=None, node=node)]
        stored = system.to_storage_dict()
        assert stored["datastreams"][0]["parent_resource_id"] is None

        reloaded = System.from_storage_dict(stored, node)
        assert (reloaded.datastreams[0].get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION)
                == "node1/systems/134/datastreams/12/observations:data")
        assert (reloaded.control_channels[0].get_mqtt_topic(subresource=APIResourceTypes.COMMAND)
                == "node1/systems/134/controlstreams/3/commands:data")
