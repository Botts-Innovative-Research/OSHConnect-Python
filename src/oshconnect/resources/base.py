#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Abstract `StreamableResource` base and the lifecycle / direction enums.

This module is the shared streaming-machinery layer used by every concrete
resource wrapper (`System`, `Datastream`, `ControlStream`). It defines:

- `SchemaFetchWarning` — surfaced from discovery when an individual
  per-resource schema fetch fails.
- `Status` / `StreamableModes` — lifecycle and direction enums.
- `StreamableResource` — the ABC every concrete wrapper extends; owns
  MQTT subscribe/publish, optional WebSocket I/O, inbound/outbound
  deques, and the ``initialize → start → stop`` lifecycle.

The concrete subclasses live in sibling modules
(`oshconnect.resources.system`, `oshconnect.resources.datastream`,
`oshconnect.resources.controlstream`) and the parent `Node` lives in
`oshconnect.node`. Public imports continue to resolve through
`oshconnect.streamableresource` (a re-export shim) and the package-level
`oshconnect.__init__`.
"""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid
from abc import ABC
from collections import deque
from enum import Enum
from multiprocessing import Process
from multiprocessing.queues import Queue
from typing import TYPE_CHECKING, Generic, TypeVar, Union
from uuid import UUID, uuid4

from ..csapi4py.constants import APIResourceTypes
from ..csapi4py.default_api_helpers import resource_type_to_endpoint
from ..csapi4py.mqtt import MQTTCommClient, mqtt_topic_format_token
from ..csapi4py.nats import NatsCommClient
from ..exceptions import MissingLocationHeaderError, ResourceInsertError
from ..resource_datamodels import ControlStreamResource
from ..resource_datamodels import DatastreamResource
from ..resource_datamodels import SystemResource
from ..timemanagement import TimePeriod

if TYPE_CHECKING:
    from ..node import Node


def new_resource_id_from_response(res, *, resource_type: str,
                                  resource_label: str = None) -> str:
    """Extract a server-assigned resource id from a create-POST response.

    Every CS API create call follows the same contract — POST the body,
    read the new id off the ``Location`` header — and every one of them
    can fail the same two ways. Centralised here so all five call sites
    (`System.insert_self`, `System.add_insert_datastream`,
    `System.add_insert_controlstream`,
    `System.add_and_insert_control_stream`,
    `Datastream.insert_observation_dict`) report failures identically.

    :param res: The `requests.Response` from the create request.
    :param resource_type: Human-readable resource name for the message,
        e.g. ``'system'`` or ``'observation'``.
    :param resource_label: The specific resource's caller-facing name,
        included in the message when available.
    :return: The new resource's server-assigned id.
    :raises ResourceInsertError: if the response is not OK.
    :raises MissingLocationHeaderError: if the response is OK but carries
        no ``Location`` header, so the id cannot be recovered.
    """
    named = f' {resource_label!r}' if resource_label is not None else ''
    if not res.ok:
        raise ResourceInsertError(
            f'Failed to create {resource_type}{named}: '
            f'HTTP {res.status_code} — {res.text}',
            status_code=res.status_code, response_text=res.text,
            resource_type=resource_type, resource_label=resource_label,
        )
    location = res.headers.get('Location')
    if location is None:
        raise MissingLocationHeaderError(
            f'Created {resource_type}{named} (HTTP {res.status_code}) but the '
            f'response carried no Location header, so its server-assigned id '
            f'cannot be determined. The resource was most likely created — '
            f're-discover rather than re-POSTing, to avoid a duplicate.',
            status_code=res.status_code, response_text=res.text,
            resource_type=resource_type, resource_label=resource_label,
        )
    return location.split('/')[-1]


class SchemaFetchWarning(UserWarning):
    """A datastream/control-stream schema fetch or parse failed during
    `Node.discover_systems` / `System.discover_datastreams` /
    `System.discover_controlstreams`.

    Discovery deliberately does not raise on per-resource schema failures —
    one broken schema would otherwise poison the entire listing. The
    matching wrapper is still appended (with `record_schema` / `command_schema`
    left as ``None``), but the original exception is surfaced both here
    (via ``warnings.warn``) and in the root logger at ERROR level (with a
    full traceback via ``exc_info=True``). Filter or capture this category
    if you want to react programmatically.
    """


class Status(Enum):
    """Lifecycle states a `StreamableResource` transitions through:
    ``STOPPED → INITIALIZING → INITIALIZED → STARTING → STARTED → STOPPING → STOPPED``."""
    INITIALIZING = "initializing"
    INITIALIZED = "initialized"
    STARTING = "starting"
    STARTED = "started"
    STOPPING = "stopping"
    STOPPED = "stopped"


class StreamableModes(Enum):
    """Direction(s) in which a streamable resource exchanges messages.

    - ``PUSH``: this client publishes outbound messages only.
    - ``PULL``: this client subscribes to inbound messages only.
    - ``BIDIRECTIONAL``: both publish and subscribe.
    """
    PUSH = "push"
    PULL = "pull"
    BIDIRECTIONAL = "bidirectional"


T = TypeVar('T', SystemResource, DatastreamResource, ControlStreamResource)


class StreamableResource(Generic[T], ABC):
    """Abstract base for `System`, `Datastream`, and `ControlStream`.

    Encapsulates the streaming machinery shared by all three: MQTT subscribe/
    publish, optional WebSocket I/O, inbound and outbound message deques,
    and lifecycle (`initialize` → `start` → `stop`). Subclasses set
    ``_underlying_resource`` (a `SystemResource` / `DatastreamResource` /
    `ControlStreamResource` pydantic model) and override `init_mqtt` to
    derive the appropriate topic.

    :param node: The parent `Node` this resource lives under.
    :param connection_mode: One of `StreamableModes`. Default ``PUSH``.
    """
    _id: UUID
    _resource_id: str | None
    # _canonical_link: str
    _topic: str
    _status: str = Status.STOPPED.value
    ws_url: str
    _message_handler = None
    _parent_node: Node
    _underlying_resource: T
    _process: Process
    _msg_reader_queue: asyncio.Queue[Union[str, bytes, float, int]]
    _msg_writer_queue: asyncio.Queue[Union[str, bytes, float, int]]
    _inbound_deque: deque
    _outbound_deque: deque
    _mqtt_client: MQTTCommClient
    _subscribe_topic: str
    _parent_resource_id: str
    _connection_mode: StreamableModes = StreamableModes.PUSH.value

    def __init__(self, node: Node, connection_mode: StreamableModes = StreamableModes.PUSH.value):
        self._id = uuid4()
        self._parent_node = node
        self._parent_node.register_streamable(self)
        self._mqtt_client = self._parent_node.get_comm_client()
        self._connection_mode = connection_mode
        self._inbound_deque = deque()
        self._outbound_deque = deque()
        self._subscribe_topic = None
        self._parent_resource_id = None
        # Always present, even before the resource exists server-side, so
        # pre-insert access is a clean `is None` check rather than an
        # AttributeError far from the failed POST that caused it.
        # Subclasses overwrite this when they know the server-assigned id.
        self._resource_id = None

    def get_streamable_id(self) -> UUID:
        """Return the local UUID assigned at construction (not the server-side ID)."""
        return self._id

    def get_streamable_id_str(self) -> str:
        """Return the local UUID as a hex string."""
        return self._id.hex

    def initialize(self):
        """Build the WebSocket URL, allocate I/O queues, and configure MQTT.

        Must be called before `start`. Inspects ``_underlying_resource`` to
        determine the right resource type and constructs the WS URL via
        the parent node's `APIHelper`.

        :raises ValueError: if ``_underlying_resource`` is not set or is
            not one of System / Datastream / ControlStream.
        """
        resource_type = None
        if isinstance(self._underlying_resource, SystemResource):
            resource_type = APIResourceTypes.SYSTEM
        elif isinstance(self._underlying_resource, DatastreamResource):
            resource_type = APIResourceTypes.DATASTREAM
        elif isinstance(self._underlying_resource, ControlStreamResource):
            resource_type = APIResourceTypes.CONTROL_CHANNEL
        if resource_type is None:
            raise ValueError(
                "Underlying resource must be set to either SystemResource or DatastreamResource before initialization.")
        # This needs to be implemented separately for each subclass
        res_id = getattr(self._underlying_resource, "ds_id", None) or getattr(self._underlying_resource, "cs_id", None)
        self.ws_url = self._parent_node.get_api_helper().construct_url(resource_type=resource_type,
                                                                       subresource_type=APIResourceTypes.OBSERVATION,
                                                                       resource_id=res_id, subresource_id=None)
        self._msg_reader_queue = asyncio.Queue()
        self._msg_writer_queue = asyncio.Queue()
        self.init_mqtt()
        self._status = Status.INITIALIZED.value

    def start(self):
        """Subclasses override to also kick off MQTT subscribe / async write
        tasks. Logs and returns silently if `initialize` hasn't been called.
        """
        if self._status != Status.INITIALIZED.value:
            logging.warning(f"Streamable resource {self._id} not initialized. Call initialize() first.")
            return
        self._status = Status.STARTING.value
        self._status = Status.STARTED.value

    async def stream(self):
        """Open a WebSocket to ``ws_url`` and run read/write loops in parallel.

        Used as an alternative to MQTT for resources that prefer WS streaming.
        Reads incoming frames into the message handler and drains
        ``_msg_writer_queue`` to the socket.
        """
        session = self._parent_node.get_session()

        try:
            async with session.ws_connect(self.ws_url, auth=self._parent_node.get_basicauth()) as ws:
                logging.info(f"Streamable resource {self._id} started.")
                read_task = asyncio.create_task(self._read_from_ws(ws))
                write_task = asyncio.create_task(self._write_to_ws(ws))
                await asyncio.gather(read_task, write_task)
        except Exception as e:
            logging.error(f"Error in streamable resource {self._id}: {e}")
            logging.error(traceback.format_exc())

    def init_mqtt(self):
        """Wire the MQTT subscribe-acknowledged callback if a client exists.

        Subclasses override to additionally derive their resource-specific
        topic into ``self._topic`` (see `Datastream.init_mqtt` /
        `ControlStream.init_mqtt`).
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return

        self._mqtt_client.set_on_subscribe(self._default_on_subscribe)

        # self.get_mqtt_topic()

    def _default_on_subscribe(self, client, userdata, mid, granted_qos, properties):
        logging.debug("OSH Subscribed: mid=%s granted_qos=%s", mid, granted_qos)

    def get_mqtt_topic(self, subresource: APIResourceTypes | None = None, data_topic: bool = True,
                       format: str | None = None):
        """
        Retrieves the MQTT topic for this streamable resource based on its underlying resource type. By default,
        returns a Resource Data Topic (`:data` suffix per CS API Part 3).
        :param subresource: Optional subresource type to get the topic for, defaults to None
        :param data_topic: If True (default), produces a Resource Data Topic with ':data' suffix. Set False for
        Resource Event Topics.
        :param format: Optional MIME content-type for the ``:data/<token>`` format subtopic. ``None`` (default) emits
        bare ``:data`` so the server's default format applies. Ignored when ``data_topic=False``.
        """
        resource_type = None
        parent_res_type = None
        parent_id = None

        if isinstance(self._underlying_resource, ControlStreamResource):
            parent_res_type = APIResourceTypes.CONTROL_CHANNEL
            parent_id = self._resource_id

            match subresource:
                case APIResourceTypes.COMMAND:
                    resource_type = APIResourceTypes.COMMAND
                case APIResourceTypes.STATUS:
                    resource_type = APIResourceTypes.STATUS

        elif isinstance(self._underlying_resource, DatastreamResource):
            parent_res_type = APIResourceTypes.DATASTREAM
            resource_type = APIResourceTypes.OBSERVATION
            parent_id = self._resource_id

        elif isinstance(self._underlying_resource, SystemResource):
            match subresource:
                case APIResourceTypes.DATASTREAM:
                    resource_type = APIResourceTypes.DATASTREAM
                    parent_res_type = APIResourceTypes.SYSTEM
                    parent_id = self._resource_id
                case APIResourceTypes.CONTROL_CHANNEL:
                    resource_type = APIResourceTypes.CONTROL_CHANNEL
                    parent_res_type = APIResourceTypes.SYSTEM
                    parent_id = self._resource_id
                case None:
                    resource_type = APIResourceTypes.SYSTEM
                    parent_res_type = None
                    parent_id = None
                case _:
                    raise ValueError(f"Unsupported subresource type {subresource} for SystemResource.")

        topic = self._parent_node.get_api_helper().get_mqtt_topic(subresource_type=resource_type, resource_id=parent_id,
                                                                  resource_type=parent_res_type, data_topic=data_topic,
                                                                  format=format)
        return topic

    def uses_nats(self) -> bool:
        """True when this resource's active transport is NATS.

        Drives the flat-vs-nested topic choice: NATS data subjects nest under
        ``systems`` (and need the parent system id) where the MQTT topics are
        flat. See `get_nats_subject`.
        """
        return isinstance(self._mqtt_client, NatsCommClient)

    def get_stream_topic(self, subresource: APIResourceTypes | None = None, data_topic: bool = True,
                         format: str | None = None) -> str:
        """Return the data topic/subject for the *active* transport.

        Dispatches to `get_nats_subject` (nested-under-systems, dot-delimited)
        when NATS is active, else `get_mqtt_topic` (flat, slash-delimited).
        Subclasses call this from ``init_mqtt`` so they stay transport-agnostic.
        """
        if self.uses_nats():
            return self.get_nats_subject(subresource=subresource, data_topic=data_topic, format=format)
        return self.get_mqtt_topic(subresource=subresource, data_topic=data_topic, format=format)

    def get_subscribe_topic(self, subresource: APIResourceTypes | None = None,
                            format: str | None = None) -> str:
        """Return the topic/subject to *subscribe* to for inbound data.

        For MQTT this is the exact per-format topic (the broker negotiates the
        format per subscription). For NATS it is a format-wildcard subject
        (``…:data.*``): in PROACTIVE mode the server publishes each stream on a
        single server-chosen format regardless of the subscriber, so we accept
        whatever it emits and read the concrete format back from each delivered
        subject via
        :func:`~oshconnect.csapi4py.nats.nats_content_type_from_subject`.
        """
        if self.uses_nats():
            return self.get_nats_subject(subresource=subresource, data_topic=True, format_wildcard=True)
        return self.get_mqtt_topic(subresource=subresource, data_topic=True, format=format)

    def get_nats_subject(self, subresource: APIResourceTypes | None = None, data_topic: bool = True,
                         format: str | None = None, format_wildcard: bool = False) -> str:
        """Build the CS API Part 3 NATS data subject for this resource.

        Unlike the flat MQTT topics, NATS data subjects are the canonical
        *nested-under-systems* resource path, dot-delimited, with a
        ``:data[.<token>]`` suffix — e.g.
        ``api.systems.{sysId}.datastreams.{dsId}.observations:data.swe-proto``.
        This mirrors ``ConSysApiNatsConnector.getResourceUri`` on the server,
        which reverses a subject back into ``/systems/.../observations`` by
        stripping the ``:data`` suffix and replacing ``.`` with ``/``.

        When ``format_wildcard`` is set, the format subtopic is a NATS
        single-token wildcard (``…:data.*``) instead of a concrete token.
        This is used to *subscribe* in PROACTIVE mode, where the server
        publishes on one server-chosen format subtopic regardless of the
        datastream's own obs format — the actual format is then read back
        from each delivered subject's trailing token (see
        :func:`~oshconnect.csapi4py.nats.nats_content_type_from_subject`).

        The parent system id required for the nesting is read from the
        datastream's ``system_id`` (``system@id``) or — for control streams
        and locally-created datastreams — from ``_parent_resource_id``.

        :raises ValueError: if the underlying resource type is unsupported or
            the parent system id cannot be resolved.
        """
        parts = [self._parent_node.get_api_helper().get_mqtt_root()]
        collection_type = None

        if isinstance(self._underlying_resource, DatastreamResource):
            sys_id = getattr(self._underlying_resource, "system_id", None) or self._parent_resource_id
            parts += [resource_type_to_endpoint(APIResourceTypes.SYSTEM), sys_id,
                      resource_type_to_endpoint(APIResourceTypes.DATASTREAM), self._resource_id]
            collection_type = APIResourceTypes.OBSERVATION
        elif isinstance(self._underlying_resource, ControlStreamResource):
            sys_id = self._parent_resource_id
            parts += [resource_type_to_endpoint(APIResourceTypes.SYSTEM), sys_id,
                      resource_type_to_endpoint(APIResourceTypes.CONTROL_CHANNEL), self._resource_id]
            # Command status subjects follow the same nesting convention as
            # command subjects. The reference server publishes observation and
            # command data subjects explicitly; the status data subject is
            # built by analogy — see docs/osh_spec_deviations.md#nats-status-subject.
            collection_type = (APIResourceTypes.STATUS if subresource is APIResourceTypes.STATUS
                               else APIResourceTypes.COMMAND)
        elif isinstance(self._underlying_resource, SystemResource):
            parts += [resource_type_to_endpoint(APIResourceTypes.SYSTEM), self._resource_id]
            match subresource:
                case APIResourceTypes.DATASTREAM:
                    collection_type = APIResourceTypes.DATASTREAM
                case APIResourceTypes.CONTROL_CHANNEL:
                    collection_type = APIResourceTypes.CONTROL_CHANNEL
                case None:
                    collection_type = None
                case _:
                    raise ValueError(f"Unsupported subresource type {subresource} for SystemResource.")
        else:
            raise ValueError("Underlying resource must be a System, Datastream, or ControlStream resource.")

        if any(p is None for p in parts):
            raise ValueError(
                "Cannot build NATS subject: parent system id is unresolved. Ensure the resource "
                "was discovered with its 'system@id' or had set_parent_resource_id() called.")

        if collection_type is not None:
            parts.append(resource_type_to_endpoint(collection_type))

        subject = ".".join(str(p) for p in parts)
        if data_topic:
            subject += ":data"
            if format_wildcard:
                subject += ".*"
            elif format is not None:
                subject += f".{mqtt_topic_format_token(format)}"
        return subject

    def get_event_topic(self) -> str:
        """
        Returns the Resource Event Topic for this streamable resource per CS API Part 3. Event topics point to the
        resource itself (no ':data' suffix) and are used to receive CloudEvents lifecycle notifications
        (create/update/delete) published by the server.

        For Datastream/ControlStream, includes the parent system path when a parent resource ID is available.
        """
        mqtt_root = self._parent_node.get_api_helper().get_mqtt_root()

        if isinstance(self._underlying_resource, DatastreamResource):
            # Prefer the nested-under-system path (required by the NATS event
            # subjects, and valid for MQTT too). Fall back to the datastream's
            # own ``system@id`` when no parent id was assigned locally.
            sys_id = self._parent_resource_id or getattr(self._underlying_resource, "system_id", None)
            if sys_id:
                return f'{mqtt_root}/systems/{sys_id}/datastreams/{self._resource_id}'
            return f'{mqtt_root}/datastreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, ControlStreamResource):
            if self._parent_resource_id:
                return f'{mqtt_root}/systems/{self._parent_resource_id}/controlstreams/{self._resource_id}'
            return f'{mqtt_root}/controlstreams/{self._resource_id}'

        elif isinstance(self._underlying_resource, SystemResource):
            return f'{mqtt_root}/systems/{self._resource_id}'

        raise ValueError(f"Cannot determine event topic for resource type {type(self._underlying_resource)}")

    def subscribe_events(self, callback=None, qos: int = 0) -> str:
        """
        Subscribes to the Resource Event Topic for this streamable resource. Event messages are CloudEvents v1.0
        JSON payloads published by the server when the resource is created, updated, or deleted.

        :param callback: Optional message callback. If None, uses the default handler (appends to inbound deque).
        :param qos: MQTT Quality of Service level, default 0.
        :return: The event topic string that was subscribed to.
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return ""
        event_topic = self.get_event_topic()
        cb = callback if callback is not None else self._mqtt_sub_callback
        self._mqtt_client.subscribe(event_topic, qos=qos, msg_callback=cb)
        return event_topic

    async def _read_from_ws(self, ws):
        async for msg in ws:
            self._message_handler(ws, msg)

    async def _write_to_ws(self, ws):
        while self._status is Status.STARTED.value:
            try:
                msg = self._msg_writer_queue.get_nowait()
                await ws.send_bytes(msg)
            except asyncio.QueueEmpty:
                await asyncio.sleep(0.05)

    def stop(self):
        """Tear down the streaming process and mark the resource ``STOPPED``.

        Note: currently calls ``Process.terminate()``; cleaner shutdown
        (graceful drain, auth state preservation) is a known follow-up.
        """
        # It would be nicer to join() here once we have cleaner shutdown logic in place to avoid corrupting processes
        # that are writing to streams or that need to manage authentication state
        self._status = "stopping"
        self._process.terminate()
        self._status = "stopped"

    def set_parent_node(self, node: Node):
        """Attach this resource to the given `Node`."""
        self._parent_node = node

    def get_parent_node(self) -> Node:
        """Return the `Node` this resource is attached to."""
        return self._parent_node

    def set_parent_resource_id(self, res_id: str):
        """Set the server-side ID of the parent resource (e.g. the parent
        System for a Datastream / ControlStream)."""
        self._parent_resource_id = res_id

    def get_parent_resource_id(self) -> str:
        """Return the server-side ID of the parent resource, if set."""
        return self._parent_resource_id

    def set_connection_mode(self, connection_mode: StreamableModes):
        """Switch direction (PUSH / PULL / BIDIRECTIONAL)."""
        self._connection_mode = connection_mode

    def poll(self):
        """Poll for new data. Hook for subclass implementations; no-op here."""
        pass

    def fetch(self, time_period: TimePeriod):
        """Fetch data over a `TimePeriod`. Hook for subclass implementations; no-op here."""
        pass

    def get_msg_reader_queue(self) -> Queue:
        """
        Returns the message queue for this streamable resource. In cases where a custom message handler is used this is
        not guaranteed to return anything or provided a queue with data.
        :return: Queue object
        """
        return self._msg_reader_queue

    def get_msg_writer_queue(self) -> Queue:
        """
        Returns the message queue for writing messages to this streamable resource.
        :return: Queue object
        """
        return self._msg_writer_queue

    def get_underlying_resource(self) -> T:
        """Return the pydantic resource model (System/Datastream/ControlStream)
        that backs this streamable."""
        return self._underlying_resource

    def get_internal_id(self) -> UUID:
        """Return the local UUID. Alias for `get_streamable_id`."""
        return self._id

    def insert_data(self, data):
        """ Inserts data into the message writer queue to be sent over the WebSocket / MQTT connection.
            Encoding is delegated to `_encode_for_wire`, which subclasses can override to honour
            their datastream's wire format (e.g. `Datastream` routes through `SWEBinaryCodec` when
            its schema is `application/swe+binary`). No semantic validation is performed.
            :param data: Data to be sent (dict, sequence, or bytes-like).
        """
        self._msg_writer_queue.put_nowait(self._encode_for_wire(data))

    def _encode_for_wire(self, data) -> bytes:
        """Default wire encoding: pass `bytes`-likes through, else `json.dumps`.

        Subclasses with format-specific codecs (see `Datastream._encode_for_wire`)
        override this. Single hook so changing the encoding policy on one path
        does not silently leave the other path producing stale bytes.
        """
        if isinstance(data, (bytes, bytearray, memoryview)):
            return bytes(data)
        return json.dumps(data).encode("utf-8")

    def subscribe_mqtt(self, topic: str, qos: int = 0):
        """Subscribe to an arbitrary MQTT ``topic`` using the default callback
        (appends incoming payloads to ``_inbound_deque``).

        :param topic: MQTT topic string. The caller is responsible for any
            topic-prefix conventions (CS API Part 3 ``:data`` etc.).
        :param qos: MQTT QoS level. Default 0.
        """
        if self._mqtt_client is None:
            logging.warning(f"No MQTT client configured for streamable resource {self._id}.")
            return
        self._mqtt_client.subscribe(topic, qos=qos, msg_callback=self._mqtt_sub_callback)

    def _publish_mqtt(self, topic, payload):
        if self._mqtt_client is None:
            logging.warning("No MQTT client configured for streamable resource %s.", self._id)
            return
        logging.debug("Publishing to MQTT topic %s", topic)
        self._mqtt_client.publish(topic, payload, qos=0)

    async def _write_to_mqtt(self):
        while self._status == Status.STARTED.value:
            try:
                msg = self._outbound_deque.popleft()
                logging.debug("Publishing outbound message from %s", self._id)
                self._publish_mqtt(self._topic, msg)
            except IndexError:
                await asyncio.sleep(0.05)
            except Exception as e:
                logging.error("Error in Write To MQTT %s: %s\n%s", self._id, e, traceback.format_exc())
        if self._status == Status.STOPPED.value:
            logging.debug("MQTT write task stopping: resource %s stopped", self._id)

    def publish(self, payload, topic: str = None):
        """
        Publishes data to the MQTT topic associated with this streamable resource.
        :param payload: Data to be published, subclass should determine specifically allowed types
        :param topic: Specific implementation determines the topic from the provided string, if None the default topic is used
        """
        self._publish_mqtt(self._topic, payload)

    def subscribe(self, topic=None, callback=None, qos=0):
        """
        Subscribes to the MQTT topic associated with this streamable resource.
        :param topic: Specific implementation determines the topic from the provided string, if None the default topic is used
        :param callback: Optional callback function to handle incoming messages, if None the default handler is used
        :param qos: Quality of Service level for the subscription, default is 0
        """
        t = None

        if topic is None:
            t = self._topic
        else:
            raise ValueError("Invalid topic provided, must be None to use default topic.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)

    def _mqtt_sub_callback(self, client, userdata, msg):
        logging.debug("Received MQTT message on topic %s (%s bytes)", msg.topic, len(msg.payload))
        # Appends to right of deque
        self._inbound_deque.append(msg.payload)
        self._emit_inbound_event(msg)

    def _emit_inbound_event(self, msg):
        """Hook for subclasses to publish EventHandler events on incoming MQTT messages."""
        pass

    def get_inbound_deque(self) -> deque:
        """Return the deque that receives inbound MQTT message payloads."""
        return self._inbound_deque

    def get_outbound_deque(self) -> deque:
        """Return the deque feeding outbound MQTT publishes."""
        return self._outbound_deque

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of the streamable's identity and
        connection state, for OSHConnect's persistence layer. Subclasses
        extend this with their own fields and the dumped underlying
        resource. Safely handles missing / None attributes.

        Not a CS API server-shaped payload.
        """
        topic = getattr(self, "_topic", None)
        status = getattr(self, "_status", None)
        parent_resource_id = getattr(self, "_parent_resource_id", None)
        connection_mode = getattr(self, "_connection_mode", None)
        resource_id = getattr(self, "_resource_id", None)
        if isinstance(connection_mode, Enum):
            connection_mode = connection_mode.value

        return {
            "id": str(getattr(self, "_id", None)),
            "resource_id": resource_id,
            # "canonical_link": getattr(self, "_canonical_link", None),
            "topic": topic,
            "status": status,
            "parent_resource_id": parent_resource_id,
            "connection_mode": connection_mode,
        }

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'StreamableResource':
        """Rebuild common attributes from a `to_storage_dict` payload.
        Subclasses override and call ``super()`` to wire in their own
        fields and the underlying resource.
        """
        obj = cls(node=node)
        obj._id = uuid.UUID(data["id"])
        obj._resource_id = data.get("resource_id")
        # obj._canonical_link = data.get("canonical_link")
        obj._topic = data.get("topic")
        obj._status = data.get("status")
        obj._parent_resource_id = data.get("parent_resource_id")
        obj._connection_mode = StreamableModes(data.get("connection_mode", StreamableModes.PUSH.value)),
        return obj
