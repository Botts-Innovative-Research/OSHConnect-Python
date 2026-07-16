#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Unit tests for NATS wiring on `Node` and `NatsCommClient` teardown.

No live NATS server is required:

* the `Node` tests patch ``NatsCommClient`` to capture the URL it is
  constructed with (proving the split-host ``nats_host`` routing), and
* the teardown tests inject a fake nats-py connection so ``stop()`` /
  ``__close`` can be exercised without a network.
"""
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("nats", reason="requires the oshconnect[nats] extra")

from oshconnect.csapi4py.nats import NatsCommClient  # noqa: E402
from oshconnect.node import Node  # noqa: E402


# ---------------------------------------------------------------------------
# Node -> NatsCommClient URL routing (split-host support)
# ---------------------------------------------------------------------------

class TestNodeNatsHost:
    def _make_node(self, **kwargs):
        with patch("oshconnect.node.NatsCommClient") as mock_cls:
            Node(protocol="http", address="api.example", port=8282,
                 enable_nats=True, nats_token="tok", **kwargs)
            return mock_cls

    def test_defaults_to_api_host(self):
        mock_cls = self._make_node()
        assert mock_cls.call_args.kwargs["url"] == "api.example"

    def test_nats_host_overrides_api_host(self):
        mock_cls = self._make_node(nats_host="nats.example")
        assert mock_cls.call_args.kwargs["url"] == "nats.example"

    def test_nats_host_accepts_full_url(self):
        mock_cls = self._make_node(nats_host="nats://nats.example:4333")
        assert mock_cls.call_args.kwargs["url"] == "nats://nats.example:4333"

    def test_token_and_port_forwarded(self):
        mock_cls = self._make_node(nats_host="nats.example", nats_port=4300)
        kwargs = mock_cls.call_args.kwargs
        assert kwargs["port"] == 4300
        assert kwargs["token"] == "tok"

    def test_client_connect_and_start_called(self):
        with patch("oshconnect.node.NatsCommClient") as mock_cls:
            Node(protocol="http", address="api.example", port=8282,
                 enable_nats=True, nats_host="nats.example")
            inst = mock_cls.return_value
            inst.connect.assert_called_once()
            inst.start.assert_called_once()


# ---------------------------------------------------------------------------
# NatsCommClient teardown — no orphaned tasks, no drain hang
# ---------------------------------------------------------------------------

class _FakeSub:
    def __init__(self):
        self.unsubscribed = False

    async def unsubscribe(self):
        self.unsubscribed = True


class _FakeNats:
    """Minimal async stand-in for a connected nats-py client."""

    def __init__(self, drain_error=None):
        self.calls = []
        self._drain_error = drain_error

    @property
    def is_connected(self):
        return "close" not in self.calls

    async def drain(self):
        self.calls.append("drain")
        if self._drain_error is not None:
            raise self._drain_error

    async def close(self):
        self.calls.append("close")


def _inject(client, fake, subs=None):
    """Wire a fake connection into the name-mangled slots stop() touches."""
    setattr(client, "_NatsCommClient__nc", fake)
    setattr(client, "_NatsCommClient__is_connected", True)
    if subs:
        getattr(client, "_NatsCommClient__subs").update(subs)


class TestNatsCommClientTeardown:
    def test_stop_unsubscribes_then_drains(self):
        client = NatsCommClient(url="localhost", port=4222, client_id_suffix="t")
        fake = _FakeNats()
        sub = _FakeSub()
        _inject(client, fake, {"subj": sub})
        client.stop()
        assert sub.unsubscribed is True          # unsubscribe happened
        assert fake.calls[0] == "drain"          # and before/leading to drain
        assert client.is_connected() is False

    def test_stop_forces_close_when_drain_fails(self):
        client = NatsCommClient(url="localhost", port=4222, client_id_suffix="t")
        fake = _FakeNats(drain_error=RuntimeError("boom"))
        _inject(client, fake)
        client.stop()
        # drain raised, so close() must be forced to cancel nats-py loop tasks
        assert fake.calls == ["drain", "close"]

    def test_stop_without_connection_is_noop(self):
        # Mirrors the never-connected fixture path used elsewhere.
        client = NatsCommClient(url="localhost", port=4222, client_id_suffix="t")
        client.stop()  # must not raise
        assert client.is_connected() is False
