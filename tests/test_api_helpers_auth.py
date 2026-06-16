"""Auth and request-routing tests for the free helpers in
``oshconnect.api_helpers``.

The helpers all funnel through ``ConnectedSystemAPIRequest.make_request``
into ``oshconnect.csapi4py.request_wrappers``. Tests intercept the
underlying ``requests.<verb>`` calls with ``tests.helpers.capture_request``
to verify that ``auth`` flows through as a tuple, not a leaked
``(None, None)`` placeholder.

Builder-level auth behaviour (``with_auth`` / ``with_basic_auth`` and the
(None, None) carve-out) is covered in
``tests/test_con_sys_api.py::TestBuilderAuth``.
"""
from __future__ import annotations

from oshconnect import api_helpers
from tests.helpers import MockResponse, capture_request


def test_retrieve_datastream_schema_plumbs_auth(monkeypatch):
    captured = capture_request(monkeypatch, "get")
    api_helpers.retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "ds-id",
        auth=("alice", "pw"),
        obs_format="application/swe+json",
    )
    assert captured["auth"] == ("alice", "pw")
    assert captured["params"] == {"obsFormat": "application/swe+json"}


def test_retrieve_datastream_schema_omits_auth_when_none(monkeypatch):
    captured = capture_request(monkeypatch, "get")
    api_helpers.retrieve_datastream_schema(
        "http://localhost:8282/sensorhub", "ds-id",
    )
    assert captured["auth"] is None


def test_retrieve_system_by_id_returns_response_not_dict(monkeypatch):
    """Formerly bypassed ``make_request()`` and returned ``resp.json()``;
    after standardization it returns the ``Response`` object like every
    other helper."""
    captured = capture_request(monkeypatch, "get")
    resp = api_helpers.retrieve_system_by_id(
        "http://localhost:8282/sensorhub", "sys-id",
        auth=("u", "p"),
    )
    assert isinstance(resp, MockResponse)
    assert captured["auth"] == ("u", "p")


def test_create_new_systems_uses_auth_tuple(monkeypatch):
    """Sanity check the migrated signature: ``auth=`` tuple flows through
    POST as Basic Auth."""
    captured = capture_request(monkeypatch, "post")
    api_helpers.create_new_systems(
        "http://localhost:8282/sensorhub",
        request_body={"name": "x"},
        auth=("u", "p"),
    )
    assert captured["auth"] == ("u", "p")


def test_list_all_systems_in_collection_returns_response(monkeypatch):
    """One of the formerly-raw-``requests`` helpers — confirms it now
    routes through ``make_request()`` and returns a ``Response``."""
    captured = capture_request(monkeypatch, "get")
    resp = api_helpers.list_all_systems_in_collection(
        "http://localhost:8282/sensorhub", "col-id",
        auth=("u", "p"),
    )
    assert isinstance(resp, MockResponse)
    assert captured["auth"] == ("u", "p")
