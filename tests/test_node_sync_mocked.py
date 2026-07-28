"""Mocked coverage for the cross-node sync insert path — GitHub issue #45.

`tests/test_node_to_node_sync.py` exercises this flow properly, but every
test in it is `@pytest.mark.network` and CI runs `-m "not network"`. That
left `System.insert_self()`'s server-facing path — and the helper that
calls it during sync — with no CI coverage at all, which matters now that
#42 made a failed POST raise rather than return quietly.

These tests import the real `_ensure_dest_system` helper from that module
(the network markers are per-test, so importing is safe) and drive it
against mocked HTTP, so a regression in the raise/no-raise contract fails
in CI instead of waiting for someone to run the live suite.
"""
from __future__ import annotations

import pytest

from oshconnect import Node
from oshconnect.exceptions import ResourceInsertError
from tests.helpers import MockResponse, capture_request
from tests.test_node_to_node_sync import _ensure_dest_system


@pytest.fixture
def node() -> Node:
    return Node(protocol="http", address="localhost", port=8282)


def test_ensure_dest_system_reuses_an_existing_system(node, monkeypatch):
    """When the destination already has a system, sync must adopt it and
    report `created_by_us=False` so cleanup doesn't delete a system the
    test didn't create."""
    capture_request(monkeypatch, "get", response=MockResponse(
        payload={"items": [{
            "type": "PhysicalSystem",
            "id": "existing-sys-1",
            "uniqueId": "urn:test:existing:1",
            "label": "Already There",
        }]},
        status=200,
    ))

    system, created = _ensure_dest_system(node)

    assert created is False
    assert system._resource_id == "existing-sys-1"


def test_ensure_dest_system_creates_one_when_none_exist(node, monkeypatch):
    """Empty destination → build a System locally and POST it, picking the
    new id off the Location header."""
    capture_request(monkeypatch, "get", response=MockResponse(
        payload={"items": []}, status=200))
    captured = capture_request(monkeypatch, "post", response=MockResponse(
        status=201,
        headers={"Location":
                 "http://localhost:8282/sensorhub/api/systems/new-sys-9"},
    ))

    system, created = _ensure_dest_system(node)

    assert created is True
    assert system._resource_id == "new-sys-9"
    assert captured["called"] is True


def test_ensure_dest_system_propagates_a_failed_insert(node, monkeypatch):
    """The regression this file exists for: a rejected POST during sync
    must surface as a typed error, not leave a system with no server-side
    id for a later call to trip over. See GitHub issues #42 and #45."""
    capture_request(monkeypatch, "get", response=MockResponse(
        payload={"items": []}, status=200))
    capture_request(monkeypatch, "post", response=MockResponse(
        payload={"error": "no space left on device"}, status=500))

    with pytest.raises(ResourceInsertError) as excinfo:
        _ensure_dest_system(node)

    assert excinfo.value.status_code == 500
    assert "no space left" in excinfo.value.response_text
