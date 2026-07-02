#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/5/18
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""`System` — a sensor system on an OSH server.

Concrete `StreamableResource` subclass. Logical grouping of one or more
`Datastream` outputs and `ControlStream` inputs sharing a single URN.
Exposes discovery and creation flows for both child resource types.
"""
from __future__ import annotations

import datetime
import logging
import uuid
import warnings
from typing import TYPE_CHECKING

from ..csapi4py.constants import APIResourceTypes, ContentTypes
from ..encoding import JSONEncoding
from ..resource_datamodels import ControlStreamResource, DatastreamResource, SystemResource
from ..schema_datamodels import (
    JSONCommandSchema, SWEBinaryDatastreamRecordSchema,
    SWEDatastreamRecordSchema, SWEFlatBuffersDatastreamRecordSchema,
    SWEJSONCommandSchema, SWEProtobufDatastreamRecordSchema,
)
from ..swe_components import DataRecordSchema
from ..timemanagement import TimeInstant, TimePeriod, TimeUtils
from .base import SchemaFetchWarning, StreamableResource
from .controlstream import ControlStream
from .datastream import Datastream

if TYPE_CHECKING:
    from ..node import Node


class System(StreamableResource[SystemResource]):
    """A sensor system on an OSH server: a logical grouping of one or more
    `Datastream` outputs and `ControlStream` inputs sharing a single URN.

    Construct directly to define a new system, or build one from a parsed
    `SystemResource` via `from_system_resource`. Use `discover_datastreams` /
    `discover_controlstreams` to populate child resources from the server,
    or `add_insert_datastream` / `add_and_insert_control_stream` to create
    new ones server-side.
    """
    label: str
    datastreams: list[Datastream]
    control_channels: list[ControlStream]
    description: str
    urn: str
    _parent_node: Node

    def __init__(self, label: str = None, urn: str = None, parent_node: Node = None, **kwargs):
        """
        :param label: The display string for the system. Maps to SML's
            ``label`` and GeoJSON's ``properties.name`` on the wire —
            the OGC CS API only carries one display string per system.
        :param urn: The URN of the system, typically formed as such:
            ``'urn:general_identifier:specific_identifier:…'``.
        :param parent_node: The `Node` this system attaches to.
        :param kwargs:
            - 'description': A description of the system
            - 'resource_id': The server-assigned ID once known
            - 'name': Deprecated alias for ``label``. Emits
              ``DeprecationWarning``; if ``label`` is also supplied,
              ``name`` is ignored. Will be removed in a future release.
        """
        super().__init__(node=parent_node)

        # Back-compat: `name` was a separate constructor parameter that
        # always carried the same value as `label` because the wire only
        # has one display string. Route deprecated callers to `label`.
        if 'name' in kwargs:
            import warnings
            warnings.warn(
                "`System(name=...)` is deprecated; use `label=` instead. "
                "The wire-format only carries one display string per "
                "system and `name` was always populated from the same "
                "source as `label`.",
                DeprecationWarning, stacklevel=2,
            )
            legacy_name = kwargs.pop('name')
            if label is None:
                label = legacy_name

        self.label = label
        self.datastreams = []
        self.control_channels = []
        self.urn = urn
        if kwargs.get('resource_id'):
            self._resource_id = kwargs['resource_id']
        if kwargs.get('description'):
            self.description = kwargs['description']

        self._underlying_resource = self.to_system_resource()

    @property
    def name(self) -> str:
        """Deprecated alias for `label`. Will be removed in a future release.

        SWE Common 3 / OGC CS API only carry one display string per system
        (SML's ``label``, GeoJSON's ``properties.name``). The wrapper's
        prior `name` field was always set to the same value as `label`.
        Use `self.label` directly going forward.
        """
        import warnings
        warnings.warn(
            "`System.name` is deprecated; use `.label` instead.",
            DeprecationWarning, stacklevel=2,
        )
        return self.label

    @name.setter
    def name(self, value: str) -> None:
        import warnings
        warnings.warn(
            "Setting `System.name` is deprecated; set `.label` instead.",
            DeprecationWarning, stacklevel=2,
        )
        self.label = value

    @staticmethod
    def _pick_datastream_schema_format(formats: list[str]):
        """Choose an ``obsFormat`` for the schema fetch, plus the parser
        that knows how to validate the response.

        Preference order: SWE+JSON (textual, easiest to inspect) →
        SWE+binary (the only choice for video/blob datastreams that
        don't advertise SWE+JSON, e.g. Axis cameras' ``video1``). Returns
        ``(None, None)`` if neither is advertised, so the caller can
        skip the fetch with a warning instead of crashing.
        """
        if formats is None:
            return None, None
        if "application/swe+json" in formats:
            return ("application/swe+json",
                    SWEDatastreamRecordSchema.from_swejson_dict)
        if "application/swe+proto" in formats:
            return ("application/swe+proto",
                    SWEProtobufDatastreamRecordSchema.from_sweproto_dict)
        if "application/swe+flatbuffers" in formats:
            return ("application/swe+flatbuffers",
                    SWEFlatBuffersDatastreamRecordSchema.from_sweflatbuffers_dict)
        if "application/swe+binary" in formats:
            return ("application/swe+binary",
                    SWEBinaryDatastreamRecordSchema.from_swebinary_dict)
        return None, None

    def discover_datastreams(self) -> list[Datastream]:
        """GET ``/systems/{id}/datastreams`` and instantiate `Datastream`
        objects for every entry. New datastreams are appended to
        ``self.datastreams`` and also returned.

        For each discovered datastream we additionally fetch its record
        schema (``GET /datastreams/{id}/schema?obsFormat=…``) and cache it
        on ``_underlying_resource.record_schema``. The schema variant is
        chosen from the datastream's advertised ``formats`` list:
        ``application/swe+json`` is preferred when available (parsed as
        `SWEDatastreamRecordSchema`); otherwise ``application/swe+binary``
        is used (parsed as `SWEBinaryDatastreamRecordSchema`). Datastreams
        like Axis camera ``video1`` outputs advertise *only* the binary
        variant — without this fallback every video datastream would land
        without a schema. The CS API listing endpoint omits the inner
        schema, so without this step every discovered datastream would be
        missing the schema callers need for observation construction or
        cross-node sync. A failure on a single datastream's schema fetch
        is downgraded to a warning so it doesn't poison the whole call.
        """
        api = self._parent_node.get_api_helper()
        res = api.get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                               APIResourceTypes.DATASTREAM)
        datastream_json = res.json()['items']
        datastreams = []

        for ds in datastream_json:
            datastream_objs = DatastreamResource.model_validate(ds, by_alias=True)
            new_ds = Datastream(self._parent_node, datastream_objs)
            obs_format, parser = self._pick_datastream_schema_format(
                datastream_objs.formats)
            if obs_format is None:
                msg = (
                    f"Datastream {datastream_objs.ds_id} advertises no "
                    f"supported schema format (have: {datastream_objs.formats}); "
                    "skipping schema fetch."
                )
                logging.warning(msg)
                warnings.warn(msg, SchemaFetchWarning, stacklevel=2)
            else:
                try:
                    schema_resp = api.get_resource(
                        APIResourceTypes.DATASTREAM, datastream_objs.ds_id,
                        APIResourceTypes.SCHEMA,
                        params={'obsFormat': obs_format},
                    )
                    schema_resp.raise_for_status()
                    new_ds._underlying_resource.record_schema = parser(schema_resp.json())
                except Exception as e:
                    msg = (
                        f"Failed to fetch {obs_format} schema for datastream "
                        f"{datastream_objs.ds_id}: {type(e).__name__}: {e}"
                    )
                    logging.error(msg, exc_info=True)
                    warnings.warn(msg, SchemaFetchWarning, stacklevel=2)
            datastreams.append(new_ds)

            # Dedup by server-side id so a re-discovery refreshes nothing
            # but also duplicates nothing. (A previous expression here
            # evaluated truthiness of a list comprehension, which only
            # ever admitted the first datastream.)
            if all(ds.get_id() != datastream_objs.ds_id for ds in self.datastreams):
                self.datastreams.append(new_ds)

        return datastreams

    def discover_controlstreams(self) -> list[ControlStream]:
        """GET ``/systems/{id}/controlstreams`` and instantiate `ControlStream`
        objects for every entry. New control streams are appended to
        ``self.control_channels`` and also returned.

        For each discovered control stream we additionally fetch the
        command schema (``GET /controlstreams/{id}/schema?f=json``,
        which OSH returns as ``application/json`` with a
        ``parametersSchema`` SWE Common component) and cache it on
        ``_underlying_resource.command_schema`` as a `JSONCommandSchema`.
        ``f=json`` is the OGC API standard format-selector and pins the
        response shape to the JSON variant — without it the server
        default could change. The CS API listing endpoint omits the
        inner schema, so without this step every discovered control
        stream would be missing the schema callers need for command
        construction or cross-node sync. A failure on a single control
        stream's schema fetch is downgraded to a warning so it doesn't
        poison the whole call.
        """
        api = self._parent_node.get_api_helper()
        res = api.get_resource(APIResourceTypes.SYSTEM, self._resource_id,
                               APIResourceTypes.CONTROL_CHANNEL)
        controlstream_json = res.json()['items']
        controlstreams = []

        for cs_json in controlstream_json:
            controlstream_objs = ControlStreamResource.model_validate(cs_json)
            new_cs = ControlStream(self._parent_node, controlstream_objs)
            try:
                schema_resp = api.get_resource(
                    APIResourceTypes.CONTROL_CHANNEL, controlstream_objs.cs_id,
                    APIResourceTypes.SCHEMA,
                    params={'f': 'json'},
                )
                schema_resp.raise_for_status()
                new_cs._underlying_resource.command_schema = (
                    JSONCommandSchema.from_json_dict(schema_resp.json())
                )
            except Exception as e:
                msg = (
                    f"Failed to fetch command schema for control stream "
                    f"{controlstream_objs.cs_id}: {type(e).__name__}: {e}"
                )
                logging.error(msg, exc_info=True)
                warnings.warn(msg, SchemaFetchWarning, stacklevel=2)
            controlstreams.append(new_cs)

            # Same id-based dedup as discover_datastreams (the previous
            # list-truthiness expression only ever admitted the first one).
            if all(cs.get_id() != controlstream_objs.cs_id for cs in self.control_channels):
                self.control_channels.append(new_cs)

        return controlstreams

    @classmethod
    def _construct_from_resource(cls, system_resource: SystemResource, parent_node: Node) -> "System":
        """Build a `System` from a parsed `SystemResource`. Internal helper
        shared by `from_csapi_dict` / `from_smljson_dict` / `from_geojson_dict`
        and the deprecated `from_system_resource`.
        """
        # exclude_none avoids triggering TimePeriod.ser_model on None-valued
        # optional time fields (it does `str(self.start)` unconditionally).
        other_props = system_resource.model_dump(exclude_none=True)
        # GeoJSON form carries `properties.name`/`properties.uid`; SML form
        # has `label`/`uid` directly on the resource. Both wire shapes
        # carry exactly one display string, mapped to `System.label`.
        if other_props.get('properties'):
            props = other_props['properties']
            new_system = cls(label=props.get('name'), urn=props.get('uid'),
                             resource_id=system_resource.system_id, parent_node=parent_node)
        else:
            new_system = cls(label=system_resource.label, urn=system_resource.uid,
                             resource_id=system_resource.system_id, parent_node=parent_node)

        new_system.set_system_resource(system_resource)
        return new_system

    @classmethod
    def from_resource(cls, system_resource: SystemResource, parent_node: Node) -> "System":
        """Build a `System` from an already-parsed `SystemResource`.

        Mirror of `Datastream.__init__(parent_node=, datastream_resource=)`
        and `ControlStream.__init__(node=, controlstream_resource=)` —
        provides the same "I have a parsed pydantic resource model in
        memory and want a wrapper attached to a node" entry point for
        Systems, whose constructor takes individual fields rather than a
        full resource model.

        Handles both wire shapes that round-trip through `SystemResource`:
        the GeoJSON form (with a ``properties`` block carrying
        ``name``/``uid``) and the SML form (``label``/``uid`` directly on
        the resource). Source of the resource doesn't matter — built
        locally, validated from `from_smljson_dict` / `from_geojson_dict`
        / `from_csapi_dict`, returned by some other library, etc.

        :param system_resource: A populated `SystemResource` instance.
        :param parent_node: The `Node` the new `System` will attach to.
        :return: A `System` wrapper bound to ``parent_node`` with
            ``_underlying_resource`` set to ``system_resource``.
        """
        return cls._construct_from_resource(system_resource, parent_node)

    @staticmethod
    def from_system_resource(system_resource: SystemResource, parent_node: Node) -> System:
        """Build a `System` from an already-parsed `SystemResource`.

        .. deprecated:: 0.5.1
            Use :meth:`System.from_resource` instead — same behavior, more
            consistent name with other wrappers' resource-taking factories.

        Handles both shapes the OSH server emits: the GeoJSON form (with a
        ``properties`` block carrying ``name``/``uid``) and the SML form
        (``label``/``uid`` directly on the resource).
        """
        warnings.warn("System.from_system_resource is deprecated; use System.from_resource instead "
                      "(then dump it to a dict if you need wire JSON).", DeprecationWarning, stacklevel=2, )
        return System._construct_from_resource(system_resource, parent_node)

    def to_system_resource(self) -> SystemResource:
        """Render this `System` as a `SystemResource` pydantic model
        suitable for POSTing to the server.

        When this wrapper already carries an ``_underlying_resource``
        (e.g. populated by ``from_csapi_dict``, ``set_system_resource``,
        or a prior ``retrieve_resource`` call), all of its fields are
        preserved into a deep copy — so cross-node sync, partial
        updates, and re-POSTs round-trip everything the source carried,
        not just ``uniqueId`` / ``label`` / a hardcoded
        ``PhysicalSystem`` type. Currently-attached datastreams are
        always reflected into ``outputs`` so newly-added children come
        along.

        When no underlying resource is present (i.e. during this
        wrapper's own ``__init__``), a thin shell is built from
        wrapper attrs and the SML type defaults to ``PhysicalSystem``.
        """
        underlying = getattr(self, '_underlying_resource', None)
        if underlying is not None:
            resource = underlying.model_copy(deep=True)
            # Pick up any wrapper-side updates the user made directly
            # on the System (the wrapper doesn't proxy these into the
            # resource on assignment).
            if self.urn and not resource.uid:
                resource.uid = self.urn
            if self.label and not resource.label:
                resource.label = self.label
        else:
            resource = SystemResource(uid=self.urn, label=self.label,
                                      feature_type='PhysicalSystem')
        if self.datastreams:
            resource.outputs = [ds.get_underlying_resource() for ds in self.datastreams]
        return resource

    def set_system_resource(self, sys_resource: SystemResource):
        """Replace the underlying `SystemResource` model."""
        self._underlying_resource = sys_resource

    def get_system_resource(self) -> SystemResource:
        """Return the underlying `SystemResource` model."""
        return self._underlying_resource

    def add_insert_datastream(self, datastream_schema: DatastreamResource):
        """Adds a datastream to the system while also inserting it into the
        system's parent node via HTTP POST.

        :param datastream_schema: DataRecordSchema to be used to define the
            datastream. Must carry a ``name`` matching NameToken
            (``^[A-Za-z][A-Za-z0-9_\\-]*$``); SWE Common 3 wraps
            DataStream.elementType in SoftNamedProperty, so the root
            component requires a name.
        :return:
        """
        api = self._parent_node.get_api_helper()
        res = api.create_resource(APIResourceTypes.DATASTREAM,
                                  datastream_schema.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={'Content-Type': ContentTypes.JSON.value},
                                  parent_res_id=self._resource_id)

        if res.ok:
            datastream_id = res.headers['Location'].split('/')[-1]
            datastream_schema.ds_id = datastream_id
        else:
            raise Exception(
                f'Failed to create datastream {datastream_schema.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_ds = Datastream(self._parent_node, datastream_schema)
        new_ds.set_parent_resource_id(self._underlying_resource.system_id)
        self.datastreams.append(new_ds)
        return new_ds

    def add_insert_controlstream(self, controlstream_resource: ControlStreamResource) -> ControlStream:
        """Adds a control stream to the system while also inserting it into
        the system's parent node via HTTP POST.

        Mirrors `add_insert_datastream`: caller assembles the full
        `ControlStreamResource` (including the embedded `command_schema`)
        and this method posts it to ``/systems/{id}/controlstreams``,
        captures the new resource ID from the ``Location`` header, and
        returns a wrapped `ControlStream`.

        For the embedded `command_schema`, prefer
        `JSONCommandSchema` (`commandFormat: application/json` with a
        ``parametersSchema``). It matches what OSH returns from
        ``GET /controlstreams/{id}/schema?f=json`` (the form
        ``discover_controlstreams`` parses), keeps round-trip sync
        symmetric, and avoids the SWE+JSON ``encoding``-omission
        deviation documented in ``docs/osh_spec_deviations.md`` §1.
        `SWEJSONCommandSchema` (``application/swe+json`` with
        ``recordSchema`` plus ``encoding``) is also accepted for
        spec-strict scenarios.

        :param controlstream_resource: A fully-built
            `ControlStreamResource` carrying ``name``, ``input_name``,
            and ``command_schema``.
        :return: ControlStream object added to the system.
        """
        api = self._parent_node.get_api_helper()
        res = api.create_resource(
            APIResourceTypes.CONTROL_CHANNEL,
            controlstream_resource.model_dump_json(by_alias=True, exclude_none=True),
            req_headers={'Content-Type': ContentTypes.JSON.value},
            parent_res_id=self._resource_id,
        )

        if res.ok:
            cs_id = res.headers['Location'].split('/')[-1]
            controlstream_resource.cs_id = cs_id
        else:
            raise Exception(
                f'Failed to create control stream {controlstream_resource.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_cs = ControlStream(node=self._parent_node, controlstream_resource=controlstream_resource)
        new_cs.set_parent_resource_id(self._underlying_resource.system_id)
        self.control_channels.append(new_cs)
        return new_cs

    def add_and_insert_control_stream(self, control_stream_record_schema: DataRecordSchema, input_name: str = None,
                                      valid_time: TimePeriod = None,
                                      command_format: str = "application/json") -> ControlStream:
        """Accepts a DataRecordSchema and creates a ControlStreamResource
        with the matching command-schema variant, then POSTs it to the
        parent node.

        Per CS API Part 2 §16.x, command schemas come in two wire forms:

        - ``application/json`` → `JSONCommandSchema` carrying
          `parametersSchema` (the SWE Common component); no `encoding`.
          **This is the default.** It matches what OSH returns from
          ``GET /controlstreams/{id}/schema?f=json`` (the form
          ``discover_controlstreams`` parses), keeps round-trip sync
          symmetric, and avoids the SWE+JSON ``encoding``-omission
          deviation documented in ``docs/osh_spec_deviations.md`` §1.
        - ``application/swe+json`` → `SWEJSONCommandSchema` carrying
          `recordSchema` (the SWE Common component) and `encoding`
          (`JSONEncoding`). Spec-canonical; pass
          ``command_format='application/swe+json'`` to opt in.

        :param control_stream_record_schema: DataRecordSchema to wrap.
            Must carry a ``name`` matching NameToken
            (``^[A-Za-z][A-Za-z0-9_\\-]*$``); the schema is the root
            named component required by both command-schema variants.
        :param input_name: Name of the input. If None, the schema label
            is lowercased and whitespace-stripped.
        :param valid_time: Optional `TimePeriod`; defaults to
            ``[now, now + 1 year]``.
        :param command_format: ``"application/json"`` (default) or
            ``"application/swe+json"``. Anything else raises
            ``ValueError``.
        :return: ControlStream object added to the system.
        """
        input_name_checked = input_name if input_name is not None else control_stream_record_schema.label.lower().replace(
            ' ', '')

        now = datetime.datetime.now()
        future_time = now.replace(year=now.year + 1)
        future_str = future_time.strftime("%Y-%m-%dT%H:%M:%SZ")

        valid_time_checked = valid_time if valid_time else TimePeriod(start=TimeInstant.now_as_time_instant(),
                                                                      end=TimeInstant(
                                                                          utc_time=TimeUtils.to_utc_time(future_str)))

        if command_format == "application/swe+json":
            command_schema = SWEJSONCommandSchema(
                command_format="application/swe+json",
                record_schema=control_stream_record_schema,
                encoding=JSONEncoding(),
            )
        elif command_format == "application/json":
            command_schema = JSONCommandSchema(
                command_format="application/json",
                params_schema=control_stream_record_schema,
            )
        else:
            raise ValueError(
                f"Unsupported command_format: {command_format!r}. "
                f"Expected 'application/swe+json' or 'application/json'."
            )

        control_stream_resource = ControlStreamResource(name=control_stream_record_schema.label,
                                                        input_name=input_name_checked, command_schema=command_schema,
                                                        validTime=valid_time_checked)
        api = self._parent_node.get_api_helper()
        res = api.create_resource(APIResourceTypes.CONTROL_CHANNEL,
                                  control_stream_resource.model_dump_json(by_alias=True, exclude_none=True),
                                  req_headers={'Content-Type': 'application/json'}, parent_res_id=self._resource_id)

        if res.ok:
            control_channel_id = res.headers['Location'].split('/')[-1]
            control_stream_resource.cs_id = control_channel_id
        else:
            raise Exception(
                f'Failed to create control stream {control_stream_resource.name!r}: '
                f'HTTP {res.status_code} — {res.text}'
            )

        new_cs = ControlStream(node=self._parent_node, controlstream_resource=control_stream_resource)
        new_cs.set_parent_resource_id(self._underlying_resource.system_id)
        self.control_channels.append(new_cs)
        return new_cs

    def insert_self(self):
        """POST this system to the server (Content-Type
        ``application/sml+json``) and capture the new resource ID from
        the ``Location`` response header.

        Server-assigned fields (``id``, ``links``) are stripped from
        the body before POST so a re-POSTed (e.g. cross-node-synced)
        system doesn't leak the source server's identifier or links to
        the destination — the destination assigns its own.
        """
        body_resource = self.to_system_resource().model_copy(deep=True)
        body_resource.system_id = None
        body_resource.links = None
        res = self._parent_node.get_api_helper().create_resource(
            APIResourceTypes.SYSTEM,
            body_resource.model_dump_json(by_alias=True, exclude_none=True),
            req_headers={'Content-Type': 'application/sml+json'})

        if res.ok:
            location = res.headers['Location']
            sys_id = location.split('/')[-1]
            self._resource_id = sys_id
            if self._underlying_resource is not None:
                self._underlying_resource.system_id = sys_id

    def retrieve_resource(self):
        """GET ``/systems/{id}`` and refresh the underlying `SystemResource`.
        Returns ``None`` either way (kept for API symmetry).
        """
        if self._resource_id is None:
            return None
        res = self._parent_node.get_api_helper().retrieve_resource(res_type=APIResourceTypes.SYSTEM,
                                                                   res_id=self._resource_id)
        if res.ok:
            system_json = res.json()
            system_resource = SystemResource.model_validate(system_json)
            self._underlying_resource = system_resource
            return None

    def to_storage_dict(self) -> dict:
        """Return a JSON-safe snapshot of this system, its child datastreams /
        control streams, and the dumped underlying `SystemResource`, for
        OSHConnect's persistence layer.

        Not a CS API server-shaped payload — the ``underlying_resource``
        block is the only piece that matches the CS API system shape.
        """
        data = super().to_storage_dict()
        data["label"] = getattr(self, "label", None)
        data["urn"] = getattr(self, "urn", None)
        data["description"] = getattr(self, "description", None)
        datastreams = getattr(self, "datastreams", None)
        if datastreams is not None:
            data["datastreams"] = [ds.to_storage_dict() for ds in datastreams]
        else:
            data["datastreams"] = None
        control_channels = getattr(self, "control_channels", None)
        if control_channels is not None:
            data["control_channels"] = [cc.to_storage_dict() for cc in control_channels]
        else:
            data["control_channels"] = None
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
        # Remove any 'resource' key if present
        data.pop("resource", None)
        return data

    @classmethod
    def from_storage_dict(cls, data: dict, node: 'Node') -> 'System':
        """Build a `System` from a dict produced by `to_storage_dict`.

        Expects ``label``, ``urn``, optional ``description`` /
        ``resource_id``, and optional ``datastreams`` / ``control_channels``
        / ``underlying_resource`` blocks. The embedded
        ``underlying_resource`` is parsed via `SystemResource.model_validate`,
        so that nested block can also be a CS API server response body.

        For backwards compatibility, ``data["name"]`` is accepted as a
        legacy alias for ``label`` if ``label`` is missing — older
        snapshots written before the `name`/`label` consolidation
        still load.

        :param data: Source dict.
        :param node: Parent `Node` the rebuilt system attaches to.
        """
        label = data.get("label") or data.get("name")
        obj = cls(
            label=label, urn=data["urn"], parent_node=node,
            description=data.get("description"), resource_id=data.get("resource_id"))
        obj._id = uuid.UUID(data["id"])
        obj.datastreams = [Datastream.from_storage_dict(ds, node) for ds in data.get("datastreams", [])]
        obj.control_channels = [ControlStream.from_storage_dict(cc, node) for cc in data.get("control_channels", [])]
        underlying = data.get("underlying_resource")
        obj._underlying_resource = SystemResource.model_validate(underlying) if underlying else None
        return obj
