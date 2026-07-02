"""Shared test helpers.

Centralizes the three patterns that used to be copy-pasted across the
suite:

- :class:`MockResponse` + :func:`capture_request` — intercept the
  ``requests.<verb>`` calls that every CS API code path funnels through
  (``oshconnect.csapi4py.request_wrappers``) and record the kwargs.
- ``make_system`` / ``make_datastream`` / ``make_controlstream`` —
  build minimal streamable resources wired to a node.
- :func:`osh_node_reachable` — reachability probe used to skip
  network-marked tests instead of hanging against a dead server.
"""
from __future__ import annotations

import json

import requests

from oshconnect import ControlStream, Datastream, System
from oshconnect.resource_datamodels import (
    ControlStreamResource,
    DatastreamResource,
)


# ---------------------------------------------------------------------------
# HTTP mocking
# ---------------------------------------------------------------------------

class MockResponse:
    """Stand-in for ``requests.Response`` — a superset of every per-file
    variant it replaced (``status_code``, ``ok``, ``text``, ``headers``,
    ``json()``, ``raise_for_status()``)."""

    def __init__(self, payload: dict | None = None, status: int = 200,
                 headers: dict | None = None):
        self._payload = payload if payload is not None else {}
        self.status_code = status
        self.ok = 200 <= status < 300
        self.text = json.dumps(self._payload) if payload is not None else ""
        self.headers = headers or {}

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"{self.status_code} for url")

    def json(self):
        return self._payload


def capture_request(monkeypatch, verb: str,
                    response: MockResponse | None = None) -> dict:
    """Patch ``oshconnect.csapi4py.request_wrappers.requests.<verb>`` —
    the single point all CS API HTTP calls funnel through — and return a
    dict that fills with the kwargs of the (last) intercepted call.

    Usage::

        captured = capture_request(monkeypatch, "post",
                                   response=MockResponse(status=201))
        ...trigger code under test...
        assert captured["url"].endswith("/systems")

    Captured keys: ``called``, ``url`` (str), ``params``, ``headers``,
    ``auth``, ``data``, ``json``.
    """
    captured: dict = {}
    resp = response if response is not None else MockResponse()

    def _f(url, params=None, headers=None, auth=None, data=None, json=None,
           **kwargs):
        captured.update(called=True, url=str(url), params=params,
                        headers=headers, auth=auth, data=data, json=json)
        return resp

    monkeypatch.setattr(
        f"oshconnect.csapi4py.request_wrappers.requests.{verb}", _f)
    return captured


# ---------------------------------------------------------------------------
# Resource factories
# ---------------------------------------------------------------------------

def make_system(node, label: str = "Test System",
                urn: str = "urn:test:system",
                resource_id: str = "sys-test-1") -> System:
    return System(label=label, urn=urn, parent_node=node,
                  resource_id=resource_id)


def make_datastream(node, ds_id: str = "ds-test-1",
                    name: str = "Test Datastream") -> Datastream:
    ds_resource = DatastreamResource.model_validate({
        "id": ds_id,
        "name": name,
        "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
    })
    return Datastream(parent_node=node, datastream_resource=ds_resource)


def make_controlstream(node, cs_id: str = "cs-test-1",
                       name: str = "Test ControlStream") -> ControlStream:
    cs_resource = ControlStreamResource.model_validate({
        "id": cs_id,
        "name": name,
    })
    return ControlStream(node=node, controlstream_resource=cs_resource)


# ---------------------------------------------------------------------------
# Network reachability (for ``-m network`` tests)
# ---------------------------------------------------------------------------

def osh_node_reachable(port: int = 8282, *, host: str = "localhost",
                       path: str = "/sensorhub/api/",
                       auth: tuple | None = ("admin", "admin"),
                       timeout: float = 2.0) -> bool:
    """True if an OSH node answers with a status in [200, 400)."""
    try:
        resp = requests.get(f"http://{host}:{port}{path}",
                            auth=auth, timeout=timeout)
        return 200 <= resp.status_code < 400
    except (requests.RequestException, OSError):
        return False
