#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""`Datastream` — an output channel of a `System` that produces observations.

Concrete `StreamableResource` subclass. Each datastream owns its observation
MQTT topic (CS API Part 3 ``:data``) and bridges between the user's
``insert(...)`` / ``insert_observation_dict(...)`` calls and the OSH server.
"""
from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid
import warnings
from typing import TYPE_CHECKING

from ..csapi4py.constants import APIResourceTypes
from ..events import DefaultEventTypes, EventHandler
from ..events.builder import EventBuilder
from ..resource_datamodels import DatastreamResource, ObservationResource
from ..schema_datamodels import (
    SWEBinaryDatastreamRecordSchema,
    SWEFlatBuffersDatastreamRecordSchema,
    SWEProtobufDatastreamRecordSchema,
)
from ..swe_binary import SWEBinaryCodec
from ..timemanagement import TimeInstant
from .base import StreamableModes, StreamableResource

if TYPE_CHECKING:
    from ..node import Node


class Datastream(StreamableResource[DatastreamResource]):
    """An output channel of a `System`: produces observations.

    Created from a parsed `DatastreamResource` (typically returned by
    `System.discover_datastreams`) or built locally and inserted via
    `System.add_insert_datastream`. Subscribes to its observation MQTT
    topic when started.

    :param parent_node: The `Node` this datastream lives under.
    :param datastream_resource: The pydantic `DatastreamResource` model.
    """
    should_poll: bool

    def __init__(self, parent_node: Node = None, datastream_resource: DatastreamResource = None):
        super().__init__(node=parent_node)
        self._underlying_resource = datastream_resource
        self._resource_id = datastream_resource.ds_id

    def get_id(self) -> str:
        """Return the server-side datastream ID."""
        return self._underlying_resource.ds_id

    @staticmethod
    def from_resource(ds_resource: DatastreamResource, parent_node: Node) -> 'Datastream':
        """Build a `Datastream` from an already-parsed `DatastreamResource`.

        .. deprecated:: 0.5.1
            Use the constructor directly instead:
            ``Datastream(parent_node=node, datastream_resource=ds_resource)``.
            For raw JSON, parse first via ``DatastreamResource.from_csapi_dict(data)``.
        """
        warnings.warn(
            "Datastream.from_resource is deprecated; pass datastream_resource directly "
            "to the constructor: Datastream(parent_node=node, datastream_resource=res). "
            "For raw JSON, parse via DatastreamResource.from_csapi_dict(data) first.",
            DeprecationWarning, stacklevel=2,
        )
        return Datastream(parent_node=parent_node, datastream_resource=ds_resource)

    def set_resource(self, resource: DatastreamResource):
        """Replace the underlying `DatastreamResource` model."""
        self._underlying_resource = resource

    def get_resource(self) -> DatastreamResource:
        """Return the underlying `DatastreamResource` model."""
        return self._underlying_resource

    def create_observation(self, obs_data: dict) -> ObservationResource:
        """Build an `ObservationResource` from a result dict, validating
        against this datastream's record schema if one is set.

        Does NOT insert the observation server-side — pair with
        `insert_observation_dict` if you want to POST it.
        """
        obs = ObservationResource(result=obs_data, result_time=TimeInstant.now_as_time_instant())
        # Validate against the schema
        if self._underlying_resource.record_schema is not None:
            obs.validate_against_schema(self._underlying_resource.record_schema)
        return obs

    def insert_observation_dict(self, obs_data: dict):
        """POST an observation dict to ``/datastreams/{id}/observations``.

        :raises Exception: if the server returns a non-OK response.
        """
        res = self._parent_node.get_api_helper().create_resource(APIResourceTypes.OBSERVATION, obs_data,
                                                                 parent_res_id=self._resource_id,
                                                                 req_headers={'Content-Type': 'application/json'})
        if res.ok:
            obs_id = res.headers['Location'].split('/')[-1]
            return obs_id
        else:
            raise Exception(f'Failed to insert observation: {res.text}')

    def start(self):
        """Start the datastream. PULL/BIDIRECTIONAL subscribes to the
        observation topic; PUSH spawns the async MQTT write loop. Requires
        an active asyncio event loop for PUSH mode.
        """
        super().start()
        if self._mqtt_client is not None:
            if self._connection_mode is StreamableModes.PULL or self._connection_mode is StreamableModes.BIDIRECTIONAL:
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

    def init_mqtt(self):
        """Set ``self._topic`` to the datastream's observation data topic
        (CS API Part 3 ``:data`` suffix). When this datastream has a
        ``record_schema`` the topic is suffixed with the matching format
        subtopic (e.g. ``…/observations:data/swe-binary``); otherwise a
        bare ``:data`` topic is used and the server's default format
        applies."""
        super().init_mqtt()
        schema = getattr(self._underlying_resource, "record_schema", None)
        obs_format = getattr(schema, "obs_format", None) if schema is not None else None
        self._topic = self.get_mqtt_topic(subresource=APIResourceTypes.OBSERVATION,
                                          data_topic=True, format=obs_format)

    def _emit_inbound_event(self, msg):
        evt = (EventBuilder().with_type(DefaultEventTypes.NEW_OBSERVATION).with_topic(msg.topic).with_data(
            msg.payload).with_producer(self).build())
        EventHandler().publish(evt)

    def _queue_push(self, msg):
        self._msg_writer_queue.put_nowait(msg)

    def _queue_pop(self):
        return self._msg_reader_queue.get_nowait()

    def insert(self, data):
        """Encode ``data`` and publish it to this datastream's observation
        MQTT topic. Bypasses the outbound deque.

        Encoding is chosen from the datastream's record schema:

        * ``application/swe+binary`` → uses `SWEBinaryCodec` to pack a
          dict (or `Sequence` in declared member order) into the binary
          wire form. Raw ``bytes``/``bytearray``/``memoryview`` payloads
          are passed through verbatim — useful when the caller has
          already framed a record (e.g. a pre-encoded H.264 NAL unit
          with the standard ``[ts][size][bytes]`` blob framing from
          ``oshconnect.swe_binary.encode_swe_binary_blob``).
        * everything else (incl. ``application/swe+json``,
          ``application/om+json``) → ``json.dumps`` of a dict.
        """
        encoded = self._encode_for_wire(data)
        self._publish_mqtt(self._topic, encoded)

    def _encode_for_wire(self, data) -> bytes:
        """Encode ``data`` for publish over this datastream's wire format.

        Single source of truth used by both `insert` (MQTT bypass) and
        ``base.StreamableResource.insert_data`` (deque-routed) via the
        ``_streamable_encode_payload`` hook on `StreamableResource`.
        Keeping the dispatch here means changing the encoding policy
        does not require touching both call sites.
        """
        # Already-encoded bytes pass through. Lets callers ship a
        # pre-framed binary blob (or a hand-built JSON dict) without
        # going through the codec.
        if isinstance(data, (bytes, bytearray, memoryview)):
            return bytes(data)
        schema = getattr(self._underlying_resource, "record_schema", None)
        if isinstance(schema, SWEBinaryDatastreamRecordSchema):
            return SWEBinaryCodec(schema).encode(data)
        if isinstance(schema, SWEProtobufDatastreamRecordSchema):
            from ..swe_protobuf import SWEProtobufCodec  # lazy: optional dep
            return SWEProtobufCodec(schema).encode(data)
        if isinstance(schema, SWEFlatBuffersDatastreamRecordSchema):
            from ..swe_flatbuffers import SWEFlatBuffersCodec  # lazy: stub
            return SWEFlatBuffersCodec(schema).encode(data)
        # JSON-family fallback (om+json, swe+json, swe+csv-handed-a-dict).
        return json.dumps(data).encode("utf-8")

    def decode_observation(self, raw: bytes) -> dict:
        """Decode one observation off the wire using this datastream's schema.

        For ``application/swe+binary`` datastreams: walks the record
        encoding's members and returns a dict keyed by field name. Block
        members come back as ``bytes`` (opaque — the codec does not
        demux H.264 / JPEG / etc.).

        For JSON-family datastreams: returns ``json.loads(raw)``.

        :raises ValueError: if no schema has been fetched.
        """
        schema = getattr(self._underlying_resource, "record_schema", None)
        if schema is None:
            raise ValueError(
                "Cannot decode observation: no record_schema on this "
                "datastream. Call System.discover_datastreams() first, "
                "or set record_schema manually.")
        if isinstance(schema, SWEBinaryDatastreamRecordSchema):
            return SWEBinaryCodec(schema).decode(raw)
        if isinstance(schema, SWEProtobufDatastreamRecordSchema):
            from ..swe_protobuf import SWEProtobufCodec  # lazy: optional dep
            return SWEProtobufCodec(schema).decode(raw)
        if isinstance(schema, SWEFlatBuffersDatastreamRecordSchema):
            from ..swe_flatbuffers import SWEFlatBuffersCodec  # lazy: stub
            return SWEFlatBuffersCodec(schema).decode(raw)
        return json.loads(raw)

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this datastream — local identity,
        connection state, polling flag, and the dumped underlying
        `DatastreamResource` — for OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API datastream shape.
        """
        data = super().to_storage_dict()
        data["should_poll"] = getattr(self, "should_poll", None)
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
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'Datastream':
        """Build a `Datastream` from a dict produced by `to_storage_dict`.
        The embedded ``underlying_resource`` is parsed via
        `DatastreamResource.model_validate`, so that nested block can also
        be a CS API server response body for the datastream.
        """
        ds_resource = DatastreamResource.model_validate(data["underlying_resource"]) if data.get(
            "underlying_resource") else None
        obj = cls(parent_node=node, datastream_resource=ds_resource)
        obj._id = uuid.UUID(data["id"])
        obj.should_poll = data.get("should_poll", False)
        return obj

    def subscribe(self, topic=None, callback=None, qos=0):
        """Subscribe to this datastream's observation MQTT topic.

        :param topic: ``None`` or ``"observation"`` — both resolve to the
            datastream's data topic. Any other string raises.
        :param callback: Override the default callback (which appends
            payloads to ``_inbound_deque``).
        :param qos: MQTT QoS level. Default 0.
        :raises ValueError: if ``topic`` is anything other than None /
            ``"observation"``.
        """
        t = None

        if topic is None or topic == APIResourceTypes.OBSERVATION.value:
            t = self._topic
        # elif topic == APIResourceTypes.STATUS.value:
        #     t = self._status_topic
        else:
            raise ValueError(f"Invalid topic provided {topic}, must be None or 'observation'.")

        if callback is None:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=self._mqtt_sub_callback)
        else:
            self._mqtt_client.subscribe(t, qos=qos, msg_callback=callback)
