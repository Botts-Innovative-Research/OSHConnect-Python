#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Unit tests for the NATS transport subject conventions.

Mirrors ``test_mqtt_topics.py`` but for the NATS binding, whose data
subjects are *nested under systems* and dot-delimited — see
``ConSysApiNatsConnector.getResourceUri`` in the reference server
(``sensorhub-service-consys-nats``). No live NATS server is required:
these exercise pure subject construction plus the transport-dispatch
wiring on `StreamableResource`.
"""
from unittest.mock import MagicMock

import pytest

# The TestTransportDispatch fixture constructs a real NatsCommClient, which
# needs the optional [nats] extra. Subject-string tests are pure, but keep
# the module runnable only where the dev env installed all extras.
pytest.importorskip("nats", reason="requires the oshconnect[nats] extra")

from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.csapi4py.default_api_helpers import APIHelper
from oshconnect.csapi4py.nats import (
    NatsCommClient,
    nats_content_type_from_subject,
    nats_subject_from_topic,
)
from oshconnect.resource_datamodels import (
    ControlStreamResource,
    DatastreamResource,
)
from oshconnect.resources.controlstream import ControlStream
from oshconnect.resources.datastream import Datastream
from oshconnect.resources.system import System

SYS_ID = "sys-1"
DS_ID = "ds-1"
CS_ID = "cs-1"


def make_mock_node(api_root="api", mqtt_topic_root=None, comm_client=None):
    """Mock Node with a real APIHelper so ``get_mqtt_root`` is exercised.

    ``comm_client`` is returned from ``get_comm_client`` — pass a
    `NatsCommClient` to make resources report ``uses_nats() is True``.
    """
    api_helper = APIHelper(server_url="localhost", port=8282, protocol="http",
                           server_root="sensorhub", api_root=api_root,
                           mqtt_topic_root=mqtt_topic_root)
    node = MagicMock()
    node.get_api_helper.return_value = api_helper
    node.get_mqtt_client.return_value = None
    node.get_comm_client.return_value = comm_client
    return node


def make_ds(node, ds_id=DS_ID, system_id=SYS_ID):
    payload = {
        "id": ds_id, "name": "d",
        "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
    }
    if system_id is not None:
        payload["system@id"] = system_id
    res = DatastreamResource.model_validate(payload)
    return Datastream(parent_node=node, datastream_resource=res)


def make_cs(node, cs_id=CS_ID, system_id=SYS_ID):
    res = ControlStreamResource.model_validate({"id": cs_id, "name": "c"})
    cs = ControlStream(node=node, controlstream_resource=res)
    if system_id is not None:
        cs.set_parent_resource_id(system_id)
    return cs


# ---------------------------------------------------------------------------
# nats_subject_from_topic — delimiter/wildcard translation
# ---------------------------------------------------------------------------

class TestSubjectTranslation:
    def test_slash_becomes_dot(self):
        assert (nats_subject_from_topic("api/datastreams/ds1/observations:data/swe-proto")
                == "api.datastreams.ds1.observations:data.swe-proto")

    def test_single_and_multi_level_wildcards(self):
        assert (nats_subject_from_topic("api/datastreams/+/observations/#")
                == "api.datastreams.*.observations.>")

    def test_already_nats_form_passthrough(self):
        subject = "api.systems.sys-1.datastreams.ds-1.observations:data.swe-json"
        assert nats_subject_from_topic(subject) == subject


class TestContentTypeFromSubject:
    def test_flatbuffers_token(self):
        assert (nats_content_type_from_subject(
            "api.systems.s.datastreams.d.observations:data.swe-flatbuffers")
            == "application/swe+flatbuffers")

    def test_swe_json_token(self):
        assert (nats_content_type_from_subject(
            "api.systems.s.datastreams.d.observations:data.swe-json")
            == "application/swe+json")

    def test_bare_data_returns_none(self):
        assert nats_content_type_from_subject(
            "api.systems.s.datastreams.d.observations:data") is None

    def test_unknown_token_returns_none(self):
        assert nats_content_type_from_subject(
            "api.systems.s.datastreams.d.observations:data.bogus") is None

    def test_no_data_suffix_returns_none(self):
        assert nats_content_type_from_subject("api.systems.s") is None


# ---------------------------------------------------------------------------
# get_nats_subject — nested-under-systems data subjects
# ---------------------------------------------------------------------------

class TestDatastreamSubject:
    def test_observation_subject_is_nested_under_system(self):
        ds = make_ds(make_mock_node())
        assert (ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION)
                == "api.systems.sys-1.datastreams.ds-1.observations:data")

    def test_format_token_appended(self):
        ds = make_ds(make_mock_node())
        assert (ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION,
                                    format="application/swe+proto")
                == "api.systems.sys-1.datastreams.ds-1.observations:data.swe-proto")

    def test_event_subject_has_no_data_suffix(self):
        ds = make_ds(make_mock_node())
        assert (ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION,
                                    data_topic=False)
                == "api.systems.sys-1.datastreams.ds-1.observations")

    def test_parent_resource_id_fallback_when_system_id_absent(self):
        ds = make_ds(make_mock_node(), system_id=None)
        ds.set_parent_resource_id("sys-fallback")
        assert (ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION)
                == "api.systems.sys-fallback.datastreams.ds-1.observations:data")

    def test_missing_system_id_raises(self):
        ds = make_ds(make_mock_node(), system_id=None)
        with pytest.raises(ValueError, match="parent system id"):
            ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION)

    def test_custom_api_root_becomes_subject_prefix(self):
        ds = make_ds(make_mock_node(api_root="v2"))
        assert ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION).startswith(
            "v2.systems.sys-1.datastreams.ds-1.observations")

    def test_mqtt_topic_root_overrides_prefix(self):
        ds = make_ds(make_mock_node(mqtt_topic_root="bus"))
        assert ds.get_nats_subject(subresource=APIResourceTypes.OBSERVATION).startswith(
            "bus.systems.sys-1.datastreams.ds-1.observations")


class TestControlStreamSubject:
    def test_command_subject_nested_under_system(self):
        cs = make_cs(make_mock_node())
        assert (cs.get_nats_subject(subresource=APIResourceTypes.COMMAND)
                == "api.systems.sys-1.controlstreams.cs-1.commands:data")

    def test_status_subject_nested_under_system(self):
        cs = make_cs(make_mock_node())
        assert (cs.get_nats_subject(subresource=APIResourceTypes.STATUS,
                                    format="application/json")
                == "api.systems.sys-1.controlstreams.cs-1.status:data.json")


class TestSystemSubject:
    def test_system_event_subject(self):
        node = make_mock_node()
        sysres = System(label="s", urn="urn:x", parent_node=node, resource_id=SYS_ID)
        assert sysres.get_nats_subject(data_topic=False) == "api.systems.sys-1"

    def test_system_datastreams_collection_subject(self):
        node = make_mock_node()
        sysres = System(label="s", urn="urn:x", parent_node=node, resource_id=SYS_ID)
        assert (sysres.get_nats_subject(subresource=APIResourceTypes.DATASTREAM)
                == "api.systems.sys-1.datastreams:data")


# ---------------------------------------------------------------------------
# Transport dispatch — get_stream_topic / init_mqtt pick the right form
# ---------------------------------------------------------------------------

@pytest.fixture
def nats_client():
    """A NatsCommClient that never connects; loop thread is stopped after use."""
    client = NatsCommClient(url="localhost", port=4222, client_id_suffix="test")
    yield client
    client.stop()


class TestTransportDispatch:
    def test_uses_nats_true_with_nats_client(self, nats_client):
        ds = make_ds(make_mock_node(comm_client=nats_client))
        assert ds.uses_nats() is True

    def test_uses_nats_false_without_client(self):
        ds = make_ds(make_mock_node(comm_client=None))
        assert ds.uses_nats() is False

    def test_get_stream_topic_nested_for_nats(self, nats_client):
        ds = make_ds(make_mock_node(comm_client=nats_client))
        assert (ds.get_stream_topic(subresource=APIResourceTypes.OBSERVATION)
                == "api.systems.sys-1.datastreams.ds-1.observations:data")

    def test_get_stream_topic_flat_for_mqtt(self):
        ds = make_ds(make_mock_node(comm_client=None))
        # Flat MQTT topic: datastream at top level, slash-delimited.
        assert (ds.get_stream_topic(subresource=APIResourceTypes.OBSERVATION)
                == "api/datastreams/ds-1/observations:data")

    def test_nats_subscribe_topic_is_format_wildcard(self, nats_client):
        ds = make_ds(make_mock_node(comm_client=nats_client))
        assert (ds.get_subscribe_topic(subresource=APIResourceTypes.OBSERVATION)
                == "api.systems.sys-1.datastreams.ds-1.observations:data.*")

    def test_mqtt_subscribe_topic_is_exact_format(self):
        ds = make_ds(make_mock_node(comm_client=None))
        assert (ds.get_subscribe_topic(subresource=APIResourceTypes.OBSERVATION,
                                       format="application/swe+json")
                == "api/datastreams/ds-1/observations:data/swe-json")

    def test_datastream_init_mqtt_sets_nested_topic(self, nats_client):
        ds = make_ds(make_mock_node(comm_client=nats_client))
        ds.init_mqtt()
        # Publish topic is bare :data (no schema/format); subscribe is wildcard.
        assert ds._topic == "api.systems.sys-1.datastreams.ds-1.observations:data"
        assert ds._subscribe_topic == "api.systems.sys-1.datastreams.ds-1.observations:data.*"

    def test_controlstream_init_mqtt_sets_nested_topics(self, nats_client):
        cs = make_cs(make_mock_node(comm_client=nats_client))
        cs.init_mqtt()
        assert cs._topic == "api.systems.sys-1.controlstreams.cs-1.commands:data"
        assert cs._status_topic == "api.systems.sys-1.controlstreams.cs-1.status:data.json"
