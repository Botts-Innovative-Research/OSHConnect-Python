"""Discovery-path tests.

Two cohorts:

1. ``DatastreamResource``-only: round-trip the listing JSON shape we
   actually get from OSH and assert the model captures the fields the
   listing returns (incl. the previously-broken ``phenomenonTime``
   alias).
2. ``System.discover_datastreams`` end-to-end: monkeypatch the listing
   endpoint and the per-datastream ``/schema`` endpoint, then assert
   the eager-fetch contract — every discovered ``Datastream`` carries
   its SWE+JSON schema on ``_underlying_resource.record_schema``, and
   a single failing schema fetch downgrades to a warning instead of
   poisoning the whole call.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from oshconnect import Node, System
from oshconnect.resource_datamodels import DatastreamResource
from oshconnect.schema_datamodels import SWEDatastreamRecordSchema
from oshconnect.timemanagement import TimePeriod

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# DatastreamResource model fixes
# ---------------------------------------------------------------------------

def test_datastream_resource_phenomenon_time_alias():
    """The CS API listing returns ``phenomenonTime`` (not
    ``phenomenonTimeInterval``). Pre-fix, the alias mismatch left
    ``phenomenon_time`` silently None on every discovered datastream."""
    raw = {
        "id": "ds-x",
        "name": "weather",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "phenomenonTime": ["2026-04-01T00:00:00Z", "2026-04-05T00:00:00Z"],
    }
    ds = DatastreamResource.model_validate(raw, by_alias=True)
    assert ds.phenomenon_time is not None
    assert isinstance(ds.phenomenon_time, TimePeriod)


def test_datastream_resource_captures_listing_fields():
    """``formats``, ``observedProperties``, and ``system@id`` are present
    in the listing response — discovery should preserve them on the
    parsed resource so callers can branch on supported formats etc."""
    raw = {
        "id": "038s1ic7k460",
        "name": "Weather - weather",
        "outputName": "weather",
        "system@id": "03ie1mkrr9r0",
        "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
        "formats": ["application/om+json", "application/swe+json",
                    "application/swe+csv"],
        "observedProperties": [
            {"definition": "http://mmisw.org/ont/cf/parameter/air_temperature",
             "label": "Air Temperature"},
        ],
    }
    ds = DatastreamResource.model_validate(raw, by_alias=True)
    assert ds.formats == ["application/om+json", "application/swe+json",
                          "application/swe+csv"]
    assert ds.system_id == "03ie1mkrr9r0"
    assert len(ds.observed_properties) == 1
    assert ds.observed_properties[0]["label"] == "Air Temperature"


# ---------------------------------------------------------------------------
# Eager schema fetch in System.discover_datastreams
# ---------------------------------------------------------------------------

@pytest.fixture
def node() -> Node:
    return Node(protocol="http", address="localhost", port=8282)


def _listing_payload(*ds_ids: str) -> dict:
    """Listing-endpoint response shape (only the keys discovery actually
    parses)."""
    return {
        "items": [
            {
                "id": ds_id,
                "name": f"weather-{ds_id}",
                "outputName": "weather",
                "system@id": "sys-1",
                "validTime": ["2026-01-01T00:00:00Z", "2099-01-01T00:00:00Z"],
                "phenomenonTime": ["2026-04-01T00:00:00Z",
                                   "2026-04-05T00:00:00Z"],
                "formats": ["application/swe+json"],
                "observedProperties": [],
            }
            for ds_id in ds_ids
        ]
    }


class _MockResponse:
    def __init__(self, payload: dict, status: int = 200):
        self._payload = payload
        self.status_code = status
        self.ok = 200 <= status < 300
        self.headers = {}
        self.text = json.dumps(payload)

    def raise_for_status(self):
        if not self.ok:
            from requests import HTTPError
            raise HTTPError(f"{self.status_code} for url")

    def json(self):
        return self._payload


def _install_dispatching_get(monkeypatch, listing_payload, schema_handler):
    """Patch ``requests.get`` at both modules discovery touches:
       - ``oshconnect.csapi4py.request_wrappers.requests.get`` → listing
       - ``oshconnect.streamableresource.requests.get`` → /schema

    ``schema_handler(ds_id) -> _MockResponse`` is invoked per-datastream
    so a single test can vary failure modes per ds_id.
    """
    def mock_get(url, params=None, headers=None, auth=None, **kwargs):
        url_str = str(url)
        if "/datastreams/" in url_str and url_str.endswith("/schema"):
            ds_id = url_str.rsplit("/", 2)[-2]
            return schema_handler(ds_id)
        # Fallback: the system-scoped listing
        return _MockResponse(listing_payload)

    monkeypatch.setattr(
        "oshconnect.csapi4py.request_wrappers.requests.get", mock_get,
    )
    monkeypatch.setattr(
        "oshconnect.streamableresource.requests.get", mock_get,
    )


def test_discover_datastreams_populates_record_schema(node, monkeypatch):
    """After discovery, every Datastream's underlying resource carries
    its SWE+JSON schema. Without this, callers downstream would get
    ``record_schema=None`` and silently fail."""
    swe_schema = json.loads(
        (FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text()
    )

    _install_dispatching_get(
        monkeypatch,
        listing_payload=_listing_payload("ds-1"),
        schema_handler=lambda ds_id: _MockResponse(swe_schema),
    )

    sys = System(name="s", label="S", urn="urn:test:s",
                 parent_node=node, resource_id="sys-1")
    discovered = sys.discover_datastreams()

    assert len(discovered) == 1
    populated = discovered[0]._underlying_resource.record_schema
    assert isinstance(populated, SWEDatastreamRecordSchema)
    assert populated.obs_format == "application/swe+json"
    assert populated.record_schema.name == "weather"
    assert {f.name for f in populated.record_schema.fields} == {
        "time", "temperature", "pressure", "windSpeed", "windDirection",
    }


def test_discover_datastreams_continues_on_schema_fetch_failure(node, monkeypatch):
    """A single failing /schema call must not poison the entire discovery
    run. The failing datastream gets ``record_schema=None`` plus a
    warning; subsequent datastreams' schemas still populate."""
    swe_schema = json.loads(
        (FIXTURES_DIR / "fake_weather_schema_swejson.json").read_text()
    )

    def schema_handler(ds_id):
        if ds_id == "ds-broken":
            return _MockResponse({"error": "boom"}, status=500)
        return _MockResponse(swe_schema)

    _install_dispatching_get(
        monkeypatch,
        listing_payload=_listing_payload("ds-broken", "ds-ok"),
        schema_handler=schema_handler,
    )

    sys = System(name="s", label="S", urn="urn:test:s",
                 parent_node=node, resource_id="sys-1")

    with pytest.warns(UserWarning, match="Failed to fetch SWE\\+JSON schema"):
        discovered = sys.discover_datastreams()

    assert len(discovered) == 2
    by_id = {d._underlying_resource.ds_id: d for d in discovered}
    assert by_id["ds-broken"]._underlying_resource.record_schema is None
    assert isinstance(
        by_id["ds-ok"]._underlying_resource.record_schema,
        SWEDatastreamRecordSchema,
    )