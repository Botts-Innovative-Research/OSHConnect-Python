"""Unit tests for ``oshconnect.csapi4py.default_api_helpers``.

Covers the two module-level helpers (``determine_parent_type``,
``resource_type_to_endpoint``) and every public method on the
``APIHelper`` dataclass. HTTP methods are exercised with
``monkeypatch`` against ``requests.{get,post,put,delete}`` (same
pattern as ``tests/test_controlstream_insert_schema.py``) so the
constructed URL, body, headers, and auth tuple can be inspected
without standing up a server.

The ``update_resource`` and ``delete_resource`` tests specifically
pin the resource ID into the URL — regression lock-in for the bug
where those methods were dropping ``res_id`` on the floor.
"""
from __future__ import annotations

import pytest

from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.csapi4py.default_api_helpers import (
    APIHelper,
    determine_parent_type,
    resource_type_to_endpoint,
)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

class TestDetermineParentType:
    """``determine_parent_type`` is a static mapping; lock every branch."""

    @pytest.mark.parametrize("res_type, expected_parent", [
        (APIResourceTypes.SYSTEM, APIResourceTypes.SYSTEM),
        (APIResourceTypes.CONTROL_CHANNEL, APIResourceTypes.SYSTEM),
        (APIResourceTypes.DATASTREAM, APIResourceTypes.SYSTEM),
        (APIResourceTypes.SYSTEM_EVENT, APIResourceTypes.SYSTEM),
        (APIResourceTypes.SAMPLING_FEATURE, APIResourceTypes.SYSTEM),
        (APIResourceTypes.COMMAND, APIResourceTypes.CONTROL_CHANNEL),
        (APIResourceTypes.OBSERVATION, APIResourceTypes.DATASTREAM),
    ])
    def test_known_parent_mappings(self, res_type, expected_parent):
        assert determine_parent_type(res_type) is expected_parent

    @pytest.mark.parametrize("res_type", [
        APIResourceTypes.COLLECTION,
        APIResourceTypes.PROCEDURE,
        APIResourceTypes.PROPERTY,
        APIResourceTypes.SYSTEM_HISTORY,
        APIResourceTypes.DEPLOYMENT,
        APIResourceTypes.STATUS,    # falls into default branch
        APIResourceTypes.SCHEMA,    # falls into default branch
    ])
    def test_top_level_or_default_returns_none(self, res_type):
        assert determine_parent_type(res_type) is None


class TestResourceTypeToEndpoint:
    """``resource_type_to_endpoint`` is also a static mapping; lock every branch."""

    @pytest.mark.parametrize("res_type, expected", [
        (APIResourceTypes.SYSTEM, "systems"),
        (APIResourceTypes.COLLECTION, "collections"),
        (APIResourceTypes.CONTROL_CHANNEL, "controlstreams"),
        (APIResourceTypes.COMMAND, "commands"),
        (APIResourceTypes.DATASTREAM, "datastreams"),
        (APIResourceTypes.OBSERVATION, "observations"),
        (APIResourceTypes.SYSTEM_EVENT, "systemEvents"),
        (APIResourceTypes.SAMPLING_FEATURE, "samplingFeatures"),
        (APIResourceTypes.PROCEDURE, "procedures"),
        (APIResourceTypes.PROPERTY, "properties"),
        (APIResourceTypes.SYSTEM_HISTORY, "history"),
        (APIResourceTypes.DEPLOYMENT, "deployments"),
        (APIResourceTypes.STATUS, "status"),
        (APIResourceTypes.SCHEMA, "schema"),
    ])
    def test_known_endpoint_mappings(self, res_type, expected):
        assert resource_type_to_endpoint(res_type) == expected

    def test_collection_parent_overrides_to_items(self):
        """When the parent type is COLLECTION, the endpoint becomes
        ``items`` regardless of the inner ``res_type``."""
        assert resource_type_to_endpoint(
            APIResourceTypes.SYSTEM, parent_type=APIResourceTypes.COLLECTION,
        ) == "items"

    def test_unknown_type_raises(self):
        """The default branch raises ``ValueError`` for an unmapped type.
        ``None`` falls through every match arm and trips the default."""
        with pytest.raises(ValueError, match="Invalid resource type"):
            resource_type_to_endpoint(None)


# ---------------------------------------------------------------------------
# APIHelper utility methods (no HTTP)
# ---------------------------------------------------------------------------

def _make_helper(**overrides) -> APIHelper:
    defaults = dict(
        server_url="localhost",
        port=8282,
        protocol="http",
        server_root="sensorhub",
        api_root="api",
        mqtt_topic_root=None,
        username=None,
        password=None,
        user_auth=False,
    )
    defaults.update(overrides)
    return APIHelper(**defaults)


class TestAPIHelperBaseURLs:
    def test_get_base_url_http_with_port(self):
        helper = _make_helper(protocol="http", port=8282)
        assert helper.get_base_url() == "http://localhost:8282"

    def test_get_base_url_https_with_port(self):
        helper = _make_helper(protocol="https", port=8443)
        assert helper.get_base_url() == "https://localhost:8443"

    def test_get_base_url_no_port(self):
        helper = _make_helper(protocol="https", port=None)
        assert helper.get_base_url() == "https://localhost"

    def test_get_base_url_socket_upgrades_http_to_ws(self):
        helper = _make_helper(protocol="http", port=8282)
        assert helper.get_base_url(socket=True) == "ws://localhost:8282"

    def test_get_base_url_socket_upgrades_https_to_wss(self):
        helper = _make_helper(protocol="https", port=8443)
        assert helper.get_base_url(socket=True) == "wss://localhost:8443"

    def test_get_api_root_url_composes_full_path(self):
        helper = _make_helper(server_root="sensorhub", api_root="api")
        assert helper.get_api_root_url() == "http://localhost:8282/sensorhub/api"

    def test_get_api_root_url_socket_variant(self):
        helper = _make_helper(protocol="https", port=8443)
        assert (
            helper.get_api_root_url(socket=True)
            == "wss://localhost:8443/sensorhub/api"
        )


class TestAPIHelperAuth:
    def test_get_helper_auth_when_unauthenticated(self):
        helper = _make_helper(user_auth=False)
        assert helper.get_helper_auth() is None

    def test_get_helper_auth_returns_credential_tuple(self):
        helper = _make_helper(username="admin", password="secret", user_auth=True)
        assert helper.get_helper_auth() == ("admin", "secret")


class TestAPIHelperProtocol:
    @pytest.mark.parametrize("protocol", ["http", "https", "ws", "wss"])
    def test_set_protocol_accepts_valid(self, protocol):
        helper = _make_helper()
        helper.set_protocol(protocol)
        assert helper.protocol == protocol

    def test_set_protocol_rejects_invalid(self):
        helper = _make_helper()
        with pytest.raises(ValueError):
            helper.set_protocol("ftp")


class TestAPIHelperMQTTRoot:
    def test_falls_back_to_api_root_when_unset(self):
        helper = _make_helper(api_root="api", mqtt_topic_root=None)
        assert helper.get_mqtt_root() == "api"

    def test_uses_explicit_mqtt_topic_root_when_set(self):
        helper = _make_helper(api_root="api", mqtt_topic_root="osh/mqtt")
        assert helper.get_mqtt_root() == "osh/mqtt"


class TestConstructURL:
    """``construct_url`` is the low-level URL builder. Cover its four shapes."""

    def test_top_level_resource_no_id(self):
        helper = _make_helper()
        url = helper.construct_url(
            resource_type=None, subresource_id=None,
            subresource_type=APIResourceTypes.SYSTEM, resource_id=None,
        )
        assert url == "http://localhost:8282/sensorhub/api/systems"

    def test_top_level_resource_with_id(self):
        helper = _make_helper()
        url = helper.construct_url(
            resource_type=None, subresource_id="sys-1",
            subresource_type=APIResourceTypes.SYSTEM, resource_id=None,
        )
        assert url == "http://localhost:8282/sensorhub/api/systems/sys-1"

    def test_subresource_collection(self):
        helper = _make_helper()
        url = helper.construct_url(
            resource_type=APIResourceTypes.SYSTEM, subresource_id=None,
            subresource_type=APIResourceTypes.DATASTREAM, resource_id="sys-1",
        )
        assert (
            url == "http://localhost:8282/sensorhub/api/systems/sys-1/datastreams"
        )

    def test_subresource_with_id(self):
        helper = _make_helper()
        url = helper.construct_url(
            resource_type=APIResourceTypes.SYSTEM, subresource_id="ds-1",
            subresource_type=APIResourceTypes.DATASTREAM, resource_id="sys-1",
        )
        assert (
            url
            == "http://localhost:8282/sensorhub/api/systems/sys-1/datastreams/ds-1"
        )

    def test_for_socket_uses_ws_scheme(self):
        helper = _make_helper(protocol="http")
        url = helper.construct_url(
            resource_type=None, subresource_id=None,
            subresource_type=APIResourceTypes.SYSTEM, resource_id=None,
            for_socket=True,
        )
        assert url.startswith("ws://localhost:8282")


class TestResourceURLResolver:
    def test_none_subresource_type_raises(self):
        helper = _make_helper()
        with pytest.raises(ValueError, match="valid APIResourceType"):
            helper.resource_url_resolver(subresource_type=None)

    def test_collection_as_subresource_of_collection_raises(self):
        helper = _make_helper()
        with pytest.raises(ValueError, match="not sub-resources of other collections"):
            helper.resource_url_resolver(
                subresource_type=APIResourceTypes.COLLECTION,
                from_collection=True,
            )

    def test_top_level_resolves_to_collection_endpoint(self):
        helper = _make_helper()
        url = helper.resource_url_resolver(
            subresource_type=APIResourceTypes.SYSTEM,
        )
        assert url.endswith("/systems")

    def test_subresource_resolves_with_parent_id(self):
        helper = _make_helper()
        url = helper.resource_url_resolver(
            subresource_type=APIResourceTypes.DATASTREAM,
            subresource_id="ds-1",
            resource_id="sys-1",
        )
        assert url.endswith("/systems/sys-1/datastreams/ds-1")

    def test_collection_membership_uses_items_endpoint(self):
        """When ``from_collection=True`` and a parent ID is provided,
        the parent endpoint becomes ``collections/<id>`` and the
        sub-resource endpoint becomes ``items``."""
        helper = _make_helper()
        url = helper.resource_url_resolver(
            subresource_type=APIResourceTypes.SYSTEM,
            resource_id="col-1",
            from_collection=True,
        )
        assert url.endswith("/collections/col-1/items")


class TestGetMQTTTopic:
    def test_data_topic_for_datastream_observations(self):
        helper = _make_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id="ds-1",
            data_topic=True,
        )
        assert topic == "api/datastreams/ds-1/observations:data"

    def test_event_topic_omits_data_suffix(self):
        helper = _make_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.SYSTEM,
            subresource_type=APIResourceTypes.DATASTREAM,
            resource_id="sys-1",
            data_topic=False,
        )
        assert topic == "api/systems/sys-1/datastreams"

    def test_topic_uses_mqtt_topic_root_when_set(self):
        helper = _make_helper(mqtt_topic_root="osh/mqtt")
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id="ds-1",
            data_topic=True,
        )
        assert topic.startswith("osh/mqtt/")

    def test_topic_with_subresource_id_appends_after_data_suffix(self):
        helper = _make_helper()
        topic = helper.get_mqtt_topic(
            resource_type=APIResourceTypes.DATASTREAM,
            subresource_type=APIResourceTypes.OBSERVATION,
            resource_id="ds-1",
            subresource_id="obs-1",
            data_topic=True,
        )
        assert topic == "api/datastreams/ds-1/observations:data/obs-1"


# ---------------------------------------------------------------------------
# APIHelper HTTP methods (monkeypatch requests.{verb})
# ---------------------------------------------------------------------------

class _MockResponse:
    status_code = 200
    ok = True
    text = ""
    headers = {"Location": "http://localhost:8282/sensorhub/api/systems/new-id"}


def _capture(into: dict):
    """Returns a callable usable for monkeypatching ``requests.<verb>``;
    captures every kwarg the wrapper passes through and returns a
    successful response."""
    def _f(url, params=None, headers=None, auth=None, data=None, json=None, **kwargs):
        into["url"] = str(url)
        into["params"] = params
        into["headers"] = headers
        into["auth"] = auth
        into["data"] = data
        into["json"] = json
        return _MockResponse()
    return _f


class TestCreateResource:
    def test_top_level_post_url_and_body(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        helper = _make_helper(username="u", password="p", user_auth=True)
        helper.create_resource(APIResourceTypes.SYSTEM, '{"name": "x"}')
        assert captured["url"] == "http://localhost:8282/sensorhub/api/systems"
        assert captured["data"] == '{"name": "x"}'
        assert captured["auth"] == ("u", "p")

    def test_subresource_post_threads_parent_id(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        helper = _make_helper()
        helper.create_resource(
            APIResourceTypes.DATASTREAM, '{"name": "x"}',
            parent_res_id="sys-1",
        )
        assert (
            captured["url"]
            == "http://localhost:8282/sensorhub/api/systems/sys-1/datastreams"
        )

    def test_url_endpoint_override(self, monkeypatch):
        """When url_endpoint is supplied, the URL is built off the full
        API root (protocol + port + server_root + api_root) — not just
        ``server_url/api_root`` (which would drop the scheme)."""
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        helper = _make_helper()
        helper.create_resource(
            APIResourceTypes.SYSTEM, '{}', url_endpoint="custom/path",
        )
        assert (
            captured["url"]
            == "http://localhost:8282/sensorhub/api/custom/path"
        )


class TestRetrieveResource:
    def test_retrieve_with_id(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        helper = _make_helper()
        helper.retrieve_resource(APIResourceTypes.SYSTEM, res_id="sys-1")
        assert (
            captured["url"]
            == "http://localhost:8282/sensorhub/api/systems/sys-1"
        )

    def test_retrieve_collection_when_id_omitted(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        helper = _make_helper()
        helper.retrieve_resource(APIResourceTypes.SYSTEM)
        assert captured["url"].endswith("/systems")


class TestGetResource:
    def test_resource_type_only(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        helper = _make_helper()
        helper.get_resource(APIResourceTypes.SYSTEM)
        assert captured["url"].endswith("/systems")

    def test_resource_with_id_and_subresource(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        helper = _make_helper()
        helper.get_resource(
            APIResourceTypes.DATASTREAM,
            resource_id="ds-1",
            subresource_type=APIResourceTypes.SCHEMA,
        )
        assert captured["url"].endswith("/datastreams/ds-1/schema")

    def test_get_resource_threads_query_params(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        helper = _make_helper()
        helper.get_resource(
            APIResourceTypes.CONTROL_CHANNEL,
            resource_id="cs-1",
            subresource_type=APIResourceTypes.SCHEMA,
            params={"f": "json"},
        )
        assert captured["params"] == {"f": "json"}


class TestUpdateResource:
    """Regression lock-in: the URL must include ``res_id`` (was None pre-fix)."""

    def test_top_level_put_includes_res_id(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.put",
            _capture(captured),
        )
        helper = _make_helper()
        helper.update_resource(
            APIResourceTypes.SYSTEM, "sys-1", '{"name": "renamed"}',
        )
        assert (
            captured["url"]
            == "http://localhost:8282/sensorhub/api/systems/sys-1"
        ), "PUT URL must include the resource id; pre-fix it was /systems"
        assert captured["data"] == '{"name": "renamed"}'

    def test_subresource_put_includes_both_ids(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.put",
            _capture(captured),
        )
        helper = _make_helper()
        helper.update_resource(
            APIResourceTypes.DATASTREAM, "ds-1", "{}",
            parent_res_id="sys-1",
        )
        assert captured["url"].endswith("/systems/sys-1/datastreams/ds-1")


class TestDeleteResource:
    """Regression lock-in: the URL must include ``res_id`` (was None pre-fix)."""

    def test_top_level_delete_includes_res_id(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        helper = _make_helper()
        helper.delete_resource(APIResourceTypes.SYSTEM, "sys-1")
        assert (
            captured["url"]
            == "http://localhost:8282/sensorhub/api/systems/sys-1"
        ), "DELETE URL must include the resource id; pre-fix it was /systems"

    def test_subresource_delete_includes_both_ids(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        helper = _make_helper()
        helper.delete_resource(
            APIResourceTypes.DATASTREAM, "ds-1", parent_res_id="sys-1",
        )
        assert captured["url"].endswith("/systems/sys-1/datastreams/ds-1")

    def test_delete_threads_auth_when_user_auth_enabled(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        helper = _make_helper(username="admin", password="s3cret", user_auth=True)
        helper.delete_resource(APIResourceTypes.SYSTEM, "sys-1")
        assert captured["auth"] == ("admin", "s3cret")
