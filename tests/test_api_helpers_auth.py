"""Auth and request-routing tests for the free helpers in
``oshconnect.api_helpers`` and the ``ConnectedSystemsRequestBuilder``.

The helpers all funnel through ``ConnectedSystemAPIRequest.make_request``
into ``oshconnect.csapi4py.request_wrappers``. Tests monkeypatch the
underlying ``requests.<verb>`` calls and capture the kwargs to verify
that ``auth`` and ``headers`` flow through as a tuple, not a leaked
``(None, None)`` placeholder.
"""
from __future__ import annotations

from oshconnect import api_helpers
from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder


class _MockResponse:
    status_code = 200

    def raise_for_status(self):
        pass

    def json(self):
        return {}


def _capture(into: dict):
    def _f(url, params=None, headers=None, auth=None, **kwargs):
        into["url"] = str(url)
        into["params"] = params
        into["headers"] = headers
        into["auth"] = auth
        return _MockResponse()
    return _f


def test_with_basic_auth_no_op_when_none():
    builder = ConnectedSystemsRequestBuilder()
    builder.with_basic_auth(None)
    assert builder.api_request.auth is None


def test_with_basic_auth_sets_tuple():
    builder = ConnectedSystemsRequestBuilder()
    builder.with_basic_auth(("alice", "pw"))
    assert builder.api_request.auth == ("alice", "pw")


def test_with_auth_legacy_no_leaks_none_pair():
    """``with_auth(None, None)`` should not leak as Basic Auth."""
    builder = ConnectedSystemsRequestBuilder()
    builder.with_auth(None, None)
    assert builder.api_request.auth is None


def test_with_auth_legacy_sets_tuple_when_supplied():
    builder = ConnectedSystemsRequestBuilder()
    builder.with_auth("u", "p")
    assert builder.api_request.auth == ("u", "p")


def test_retrieve_datastream_schema_plumbs_auth(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _capture(captured),
    )
    api_helpers.retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "ds-id",
        auth=("alice", "pw"),
        obs_format="application/swe+json",
    )
    assert captured["auth"] == ("alice", "pw")
    assert captured["params"] == {"obsFormat": "application/swe+json"}


def test_retrieve_datastream_schema_omits_auth_when_none(monkeypatch):
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _capture(captured),
    )
    api_helpers.retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "ds-id",
    )
    assert captured["auth"] is None


def test_retrieve_system_by_id_returns_response_not_dict(monkeypatch):
    """Formerly bypassed ``make_request()`` and returned ``resp.json()``;
    after standardization it returns the ``Response`` object like every
    other helper."""
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _capture(captured),
    )
    resp = api_helpers.retrieve_system_by_id(
        "http://localhost:8282/sensorhub", "sys-id",
        auth=("u", "p"),
    )
    assert isinstance(resp, _MockResponse)
    assert captured["auth"] == ("u", "p")


def test_create_new_systems_uses_auth_tuple(monkeypatch):
    """Sanity check the migrated signature: ``auth=`` tuple flows through
    POST as Basic Auth."""
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.post", _capture(captured),
    )
    api_helpers.create_new_systems(
        "http://localhost:8282/sensorhub",
        request_body={"name": "x"},
        auth=("u", "p"),
    )
    assert captured["auth"] == ("u", "p")


def test_list_all_systems_in_collection_returns_response(monkeypatch):
    """One of the formerly-raw-``requests`` helpers — confirms it now
    routes through ``make_request()`` and returns a ``Response``."""
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", _capture(captured),
    )
    resp = api_helpers.list_all_systems_in_collection(
        "http://localhost:8282/sensorhub", "col-id",
        auth=("u", "p"),
    )
    assert isinstance(resp, _MockResponse)
    assert captured["auth"] == ("u", "p")
