"""Schema-variant tests for ``System.add_and_insert_control_stream``.

The CS API offers two command-schema wire forms:

- ``application/swe+json`` → SWE Common ``recordSchema`` plus a
  ``JSONEncoding`` block.
- ``application/json`` → SWE Common ``parametersSchema``; no encoding.

The previous implementation mixed them — emitting
``commandFormat: "application/swe+json"`` alongside ``parametersSchema``,
which violates both. These tests pin the expected on-the-wire shape per
``command_format`` so the bug can't regress.
"""
from __future__ import annotations

import json

import pytest

from oshconnect import Node, System
from oshconnect.api_utils import URI, UCUMCode
from oshconnect.swe_components import DataRecordSchema, QuantitySchema, TimeSchema


class _MockResponse:
    status_code = 201
    ok = True
    text = ""
    headers = {"Location": "http://localhost:8585/sensorhub/api/controlstreams/cs-new"}


def _capture_post(into: dict):
    def _f(url, params=None, headers=None, auth=None, data=None, json=None, **kwargs):
        into["url"] = str(url)
        into["headers"] = headers
        into["data"] = data
        into["json"] = json
        return _MockResponse()
    return _f


def _record_schema() -> DataRecordSchema:
    return DataRecordSchema(
        name="counterControl",
        label="Counter Control",
        definition="http://example.org/CounterControl",
        fields=[
            TimeSchema(
                name="timestamp",
                label="Timestamp",
                definition="http://www.opengis.net/def/property/OGC/0/SamplingTime",
                uom=URI(href="http://www.opengis.net/def/uom/ISO-8601/0/Gregorian"),
            ),
            QuantitySchema(
                name="setStep",
                label="Set Step",
                definition="http://example.org/SetStep",
                uom=UCUMCode(code="1", label="step"),
            ),
        ],
    )


@pytest.fixture
def system(monkeypatch) -> System:
    """A System wired to a Node, with the system already 'inserted' so
    `_resource_id` is populated for the controlstream POST."""
    node = Node(protocol="http", address="localhost", port=8585)
    sys = System(
        label="Test System", urn="urn:test:sys:1",
        parent_node=node, resource_id="sys-1",
    )
    return sys


def _captured_body_json(captured: dict) -> dict:
    """``request_wrappers.post_request`` chooses ``data=`` for str bodies and
    ``json=`` for dicts. The control-stream path dumps to a JSON string, so
    the body lands in ``data``."""
    body = captured.get("data")
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8")
    assert body is not None, f"no body captured: {captured}"
    return json.loads(body)


def test_json_default_emits_parametersschema_no_encoding(system, monkeypatch):
    """Default ``command_format='application/json'`` must produce the JSON
    wire form: ``commandFormat: application/json`` plus ``parametersSchema``.
    NOT ``recordSchema`` and NOT ``encoding``."""
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.post", _capture_post(captured),
    )

    system.add_and_insert_control_stream(_record_schema())

    body = _captured_body_json(captured)
    schema = body["schema"]
    assert schema["commandFormat"] == "application/json"
    assert "parametersSchema" in schema, "JSON form must carry parametersSchema"
    assert "recordSchema" not in schema, (
        "JSON form must NOT carry recordSchema (that's the SWE+JSON form)"
    )
    assert "encoding" not in schema, (
        "JSON form has no encoding block — that's SWE+JSON only"
    )


def test_swejson_emits_recordschema_and_encoding(system, monkeypatch):
    """`command_format='application/swe+json'` must produce the
    spec-canonical wire form: ``commandFormat: application/swe+json`` plus
    ``recordSchema`` plus ``encoding`` (JSONEncoding). NOT ``parametersSchema``."""
    captured: dict = {}
    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.post", _capture_post(captured),
    )

    system.add_and_insert_control_stream(
        _record_schema(), command_format="application/swe+json",
    )

    body = _captured_body_json(captured)
    schema = body["schema"]
    assert schema["commandFormat"] == "application/swe+json"
    assert "recordSchema" in schema, "SWE+JSON form must carry recordSchema"
    assert "parametersSchema" not in schema, (
        "SWE+JSON form must NOT carry parametersSchema (that's the JSON form)"
    )
    assert schema["encoding"]["type"] == "JSONEncoding"


def test_unsupported_command_format_raises(system):
    """Anything other than the two supported formats is a programming
    error — fail loudly rather than silently emit malformed JSON."""
    with pytest.raises(ValueError, match="Unsupported command_format"):
        system.add_and_insert_control_stream(
            _record_schema(), command_format="application/xml",
        )
