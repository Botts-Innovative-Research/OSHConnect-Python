#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""`ControlStream` — an input channel of a `System` that accepts commands.

Concrete `StreamableResource` subclass with two MQTT topics
(``self._topic`` for commands, ``self._status_topic`` for status updates)
and two pairs of inbound/outbound deques to match.
"""
from __future__ import annotations

import asyncio
import logging
import traceback
import uuid
from collections import deque
from typing import TYPE_CHECKING

from ..csapi4py.constants import APIResourceTypes
from ..events import DefaultEventTypes, EventHandler
from ..events.builder import EventBuilder
from ..resource_datamodels import ControlStreamResource
from .base import StreamableModes, StreamableResource

if TYPE_CHECKING:
    from ..node import Node


class ControlStream(StreamableResource[ControlStreamResource]):
    """An input channel of a `System`: accepts commands and emits status.

    Unlike `Datastream`, a control stream has TWO MQTT topics — one for
    commands (``self._topic``) and one for status updates
    (``self._status_topic``) — and two pairs of inbound/outbound deques to
    match. Construct from a parsed `ControlStreamResource` (typically from
    `System.discover_controlstreams`) or build locally and insert via
    `System.add_and_insert_control_stream`.

    :param node: The `Node` this control stream lives under.
    :param controlstream_resource: The pydantic `ControlStreamResource`
        model that backs this stream.
    """
    _status_topic: str
    _inbound_status_deque: deque
    _outbound_status_deque: deque

    def __init__(self, node: Node = None, controlstream_resource: ControlStreamResource = None):
        super().__init__(node=node)
        self._underlying_resource = controlstream_resource
        self._inbound_status_deque = deque()
        self._outbound_status_deque = deque()
        self._resource_id = controlstream_resource.cs_id
        # Always make sure this is set after the resource ids are set
        self._status_topic = self.get_mqtt_status_topic()

    def add_underlying_resource(self, resource: ControlStreamResource):
        """Replace the underlying `ControlStreamResource` model."""
        self._underlying_resource = resource

    def get_id(self) -> str:
        """Return the server-side control-stream ID."""
        return self._underlying_resource.cs_id

    def init_mqtt(self):
        """Set ``self._topic`` to the control stream's command data topic.
        When this control stream has a ``command_schema`` the topic is
        suffixed with the matching format subtopic (e.g.
        ``…/commands:data/swe-json``); otherwise a bare ``:data`` topic is
        used and the server's default format applies."""
        super().init_mqtt()
        schema = getattr(self._underlying_resource, "command_schema", None)
        cmd_format = getattr(schema, "command_format", None) if schema is not None else None
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.COMMAND,
                                          data_topic=True, format=cmd_format)

    def get_mqtt_status_topic(self) -> str:
        """Return the MQTT topic for command status updates. Status payloads
        are always ``application/json``, so the topic is suffixed with the
        ``json`` format subtopic (``…/status:data/json``)."""
        return self.get_mqtt_topic(subresource=APIResourceTypes.STATUS,
                                   data_topic=True, format="application/json")

    def _emit_inbound_event(self, msg):
        evt_type = (DefaultEventTypes.NEW_COMMAND if msg.topic == self._topic else DefaultEventTypes.NEW_COMMAND_STATUS)
        evt = (
            EventBuilder().with_type(evt_type).with_topic(msg.topic).with_data(msg.payload).with_producer(self).build())
        EventHandler().publish(evt)

    def start(self):
        """Start the control stream. PULL/BIDIRECTIONAL subscribes to the
        command topic; PUSH spawns the async MQTT write loop. Requires
        an active asyncio event loop for PUSH mode.
        """
        super().start()
        if self._mqtt_client is not None:
            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
                # Subs to command topic by default
                self._mqtt_client.subscribe(self._topic, msg_callback=self._mqtt_sub_callback)
            else:
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(self._write_to_mqtt())
                except RuntimeError:
                    logging.warning("No running event loop — MQTT write task for %s not started. "
                                    "Call start() from within an async context.", self._id)
                except Exception as e:
                    logging.error("Error starting MQTT write task for %s: %s\n%s", self._id, e, traceback.format_exc())

    def get_inbound_deque(self) -> deque:
        """Return the deque receiving inbound command payloads."""
        return self._inbound_deque

    def get_outbound_deque(self) -> deque:
        """Return the deque feeding outbound command publishes."""
        return self._outbound_deque

    def get_status_deque_inbound(self) -> deque:
        """Return the deque receiving inbound status updates."""
        return self._inbound_status_deque

    def get_status_deque_outbound(self) -> deque:
        """Return the deque feeding outbound status publishes."""
        return self._outbound_status_deque

    def publish_command(self, payload):
        """Publish ``payload`` to the command MQTT topic. Convenience wrapper
        for ``publish(payload, APIResourceTypes.COMMAND.value)``."""
        self.publish(payload, topic=APIResourceTypes.COMMAND.value)

    def publish_status(self, payload):
        """Publish ``payload`` to the status MQTT topic. Convenience wrapper
        for ``publish(payload, APIResourceTypes.STATUS.value)``."""
        self.publish(payload, topic=APIResourceTypes.STATUS.value)

    def publish(self, payload, topic: str = APIResourceTypes.COMMAND.value):
        """
        Publishes data to the MQTT topic associated with this control stream resource.

        :param payload: Data to be published; subclass determines specifically allowed types.
        :param topic: One of ``APIResourceTypes.COMMAND.value`` (``"Command"``,
            the default) or ``APIResourceTypes.STATUS.value`` (``"Status"``).
            Pass the enum value rather than a lowercase shorthand — the
            comparison is case-sensitive against the canonical CS API
            resource-type strings.
        """

        if topic == APIResourceTypes.COMMAND.value:
            self._publish_mqtt(self._topic, payload)
        elif topic == APIResourceTypes.STATUS.value:
            self._publish_mqtt(self._status_topic, payload)
        else:
            raise ValueError(
                f"Unsupported topic {topic!r} for ControlStream publish(); "
                f"expected {APIResourceTypes.COMMAND.value!r} or "
                f"{APIResourceTypes.STATUS.value!r}."
            )

    def subscribe(self, topic=None, callback=None, qos=0):
        """
        Subscribes to the MQTT topic associated with this control stream resource.

        :param topic: ``None`` (defaults to the command topic),
            ``APIResourceTypes.COMMAND.value`` (``"Command"``), or
            ``APIResourceTypes.STATUS.value`` (``"Status"``). Comparison is
            case-sensitive against the canonical CS API resource-type strings.
        :param callback: Optional callback function to handle incoming messages, if None the default handler is used.
        :param qos: Quality of Service level for the subscription, default is 0.
        """

        t = None

        if topic is None or topic == APIResourceTypes.COMMAND.value:
            t = self._topic
        elif topic == APIResourceTypes.STATUS.value:
            t = self._status_topic
        else:
            raise ValueError(
                f"Invalid topic {topic!r}; must be None, "
                f"{APIResourceTypes.COMMAND.value!r}, or "
                f"{APIResourceTypes.STATUS.value!r}."
            )

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this control stream — local
        identity, connection state, status topic, and the dumped underlying
        `ControlStreamResource` — for OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API control-stream
        shape.
        """
        data = super().to_storage_dict()
        data["status_topic"] = getattr(self, "_status_topic", None)
        underlying = getattr(self, "_underlying_resource", None)
        if underlying is not None:
            dump = getattr(underlying, 'model_dump', None)
            if callable(dump):
                data["underlying_resource"] = underlying.model_dump(by_alias=True, exclude_none=True, mode='json')
            elif hasattr(underlying, 'to_dict'):
                data["underlying_resource"] = underlying.to_dict()
            else:
                data["underlying_resource"] = str(underlying)
        else:
            data["underlying_resource"] = None

        return data

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'ControlStream':
        """Build a `ControlStream` from a dict produced by `to_storage_dict`.
        The embedded ``underlying_resource`` is parsed via
        `ControlStreamResource.model_validate`, so that nested block can
        also be a CS API server response body for the control stream.
        """
        cs_resource = ControlStreamResource.model_validate(data["underlying_resource"]) if data.get(
            "underlying_resource") else None
        obj = cls(node=node, controlstream_resource=cs_resource)
        obj._id = uuid.UUID(data["id"])
        obj._status_topic = data.get("status_topic")
        return obj
