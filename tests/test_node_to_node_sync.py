"""Cross-node sync integration tests.

Each test fetches a datastream's SWE+JSON schema from a source OSH node and
uses it to create a fresh datastream on a destination node, verifying the
end-to-end conversion path. Both servers must be running locally; the
tests are tagged ``@pytest.mark.network`` and skipped by default in CI
(see ``.github/workflows/tests.yaml``).

Default endpoints:
    SRC_PORT  = 8282    (provides datastreams to fetch from)
    DEST_PORT = 8382    (receives newly-created datastreams)

Override per-run with ``OSHC_SRC_PORT`` / ``OSHC_DEST_PORT`` env vars.
"""
from __future__ import annotations

import os
import uuid

import pytest
import requests

from oshconnect import Node, System
from oshconnect.csapi4py.constants import APIResourceTypes
from oshconnect.encoding import JSONEncoding
from oshconnect.resource_datamodels import ControlStreamResource, DatastreamResource
from oshconnect.schema_datamodels import (
    CommandJSON,
    SWEDatastreamRecordSchema,
    SWEJSONCommandSchema,
)
from oshconnect.timemanagement import TimeInstant, TimePeriod, TimeUtils

SRC_PORT = int(os.environ.get("OSHC_SRC_PORT", "8282"))
DEST_PORT = int(os.environ.get("OSHC_DEST_PORT", "8382"))
NODE_TIMEOUT = 2.0


def _node_reachable(port: int) -> bool:
    """True if HTTP root responds with anything in [200, 400)."""
    try:
        r = requests.get(
            f"http://localhost:{port}/sensorhub/api/",
            timeout=NODE_TIMEOUT,
            auth=("admin", "admin"),
        )
        return 200 <= r.status_code < 400
    except (requests.RequestException, OSError):
        return False


def _make_node(port: int) -> Node:
    return Node(
        protocol="http", address="localhost", port=port,
        username="admin", password="admin",
    )


@pytest.fixture
def src_node():
    if not _node_reachable(SRC_PORT):
        pytest.skip(f"src OSH node not reachable at localhost:{SRC_PORT}")
    return _make_node(SRC_PORT)


@pytest.fixture
def dest_node():
    if not _node_reachable(DEST_PORT):
        pytest.skip(f"dest OSH node not reachable at localhost:{DEST_PORT}")
    return _make_node(DEST_PORT)


def _first_datastream_with_schema(node: Node):
    """Walk this node's systems and return the first datastream that has
    something fetch-able. Returns ``None`` if no datastream exists."""
    systems = node.discover_systems() or []
    for sys in systems:
        datastreams = sys.discover_datastreams()
        if datastreams:
            return datastreams[0]
    return None


def _ensure_dest_system(node: Node) -> tuple[System, bool]:
    """Find or create a system on the destination node to attach new
    datastreams to. Returns ``(system, created_by_us)`` so cleanup can
    decide whether to tear the system down."""
    systems = node.discover_systems()
    if systems:
        return systems[0], False
    sys = System(
        label="Sync Target System",
        urn=f"urn:test:cross-node-sync:{uuid.uuid4().hex[:8]}",
        parent_node=node,
    )
    sys.insert_self()
    return sys, True


def _delete_resource(node: Node, path: str) -> None:
    """Best-effort DELETE against ``<protocol>://<addr>:<port>/sensorhub/api/<path>``.
    Suppresses errors so cleanup never masks a real test failure."""
    url = f"{node.protocol}://{node.address}:{node.port}/sensorhub/api/{path}"
    try:
        requests.delete(url, auth=("admin", "admin"), timeout=NODE_TIMEOUT)
    except (requests.RequestException, OSError):
        pass


@pytest.mark.network
def test_swejson_schema_round_trips_src_to_dest(src_node, dest_node):
    """Pull the first datastream's SWE+JSON schema from the source node
    via the eager-fetch cache populated by ``discover_datastreams``, use
    its ``recordSchema`` (the inner SWE Common DataRecord) to create a
    new datastream on the destination, then verify by re-discovering on
    dest and comparing the cached schema."""
    src_ds = _first_datastream_with_schema(src_node)
    if src_ds is None:
        pytest.skip(f"no datastreams found on any system at :{SRC_PORT}")

    # Eager-fetch contract: discover_datastreams populates the SWE+JSON
    # schema on the underlying resource. Without this, every workflow
    # that needs the schema (cross-node sync, observation building, etc.)
    # silently breaks.
    cached = src_ds._underlying_resource.record_schema
    assert cached is not None, (
        "discover_datastreams should populate _underlying_resource.record_schema"
    )
    assert isinstance(cached, SWEDatastreamRecordSchema)
    src_record = cached.record_schema
    assert src_record.name, "source schema's recordSchema has no name"

    # Ensure a system on the destination to attach to.
    dest_sys, created_dest_sys = _ensure_dest_system(dest_node)
    dest_sys_id = dest_sys._resource_id  # System has no public id getter
    new_id = None

    try:
        # `System.add_insert_datastream` takes a fully-built
        # `DatastreamResource` (caller assembles the SWE+JSON envelope,
        # output_name, validTime). We wrap the source's inner record
        # schema and POST to dest's `/systems/{id}/datastreams`.
        dest_resource = DatastreamResource(
            ds_id="default",
            name=src_record.name,
            output_name=src_record.name,
            record_schema=SWEDatastreamRecordSchema(
                record_schema=src_record,
                obs_format="application/swe+json",
            ),
            valid_time=TimePeriod(
                start=TimeInstant.now_as_time_instant(),
                end=TimeInstant(
                    utc_time=TimeUtils.to_utc_time("2026-12-31T00:00:00Z")
                ),
            ),
        )
        new_ds = dest_sys.add_insert_datastream(dest_resource)
        assert new_ds is not None, "add_insert_datastream returned None"

        new_id = new_ds.get_id()
        assert new_id and new_id != "default", (
            f"expected a real server-assigned datastream id from dest's "
            f"Location header; got {new_id!r}"
        )

        # Round-trip verify: re-discover on dest and confirm the schema
        # we POSTed comes back with the same structure.
        dest_streams = dest_sys.discover_datastreams()
        dest_match = next((d for d in dest_streams if d.get_id() == new_id), None)
        assert dest_match is not None, (
            f"newly-created datastream {new_id!r} not found in "
            f"discover_datastreams() on dest"
        )
        dest_cached = dest_match._underlying_resource.record_schema
        assert isinstance(dest_cached, SWEDatastreamRecordSchema)
        dest_record = dest_cached.record_schema
        assert dest_record.name == src_record.name, (
            f"recordSchema.name didn't round-trip: "
            f"src={src_record.name!r}, dest={dest_record.name!r}"
        )

        src_fields = {f.name for f in src_record.fields}
        dest_fields = {f.name for f in dest_record.fields}
        assert src_fields == dest_fields, (
            f"field names differ across sync: "
            f"src={src_fields}, dest={dest_fields}"
        )

        print(
            f"Synced datastream {src_ds.get_id()} from :{SRC_PORT} → "
            f"datastream {new_id} on :{DEST_PORT} "
            f"(fields: {sorted(src_fields)})"
        )
    finally:
        # Best-effort teardown: drop the datastream we created, then the
        # system if we created it. Runs on success and failure so the
        # dest node doesn't accumulate test residue across runs.
        if new_id:
            _delete_resource(dest_node, f"datastreams/{new_id}")
        if created_dest_sys and dest_sys_id:
            _delete_resource(dest_node, f"systems/{dest_sys_id}")


def _first_controlstream_with_schema(node: Node):
    """Walk this node's systems and return the first control stream that
    has a populated command schema. Returns ``None`` if none exists."""
    systems = node.discover_systems() or []
    for sys in systems:
        controlstreams = sys.discover_controlstreams()
        for cs in controlstreams:
            if cs._underlying_resource.command_schema is not None:
                return cs
    return None


@pytest.mark.network
def test_command_schema_round_trips_src_to_dest(src_node, dest_node):
    """Fetch the first control stream's command schema from the source
    node, use its ``parametersSchema`` (the inner SWE Common component —
    a `DataChoice` for the controllable counter) to create a new control
    stream on the destination, then verify by reading the new schema
    back and comparing structure.

    Mirrors `test_swejson_schema_round_trips_src_to_dest` but for
    `/controlstreams`. The CS API returns command schemas as
    ``application/json`` envelopes carrying a ``parametersSchema`` SWE
    component; we wrap it in a fresh `JSONCommandSchema` for the dest
    POST.
    """
    src_cs = _first_controlstream_with_schema(src_node)
    if src_cs is None:
        pytest.skip(f"no control streams with schemas found on any system at :{SRC_PORT}")

    # Eager-fetch contract: discover_controlstreams should already have
    # populated the command schema on the underlying resource.
    cached = src_cs._underlying_resource.command_schema
    assert cached is not None, (
        "discover_controlstreams should populate _underlying_resource.command_schema"
    )
    assert isinstance(cached, JSONCommandSchema)
    src_params = cached.params_schema
    assert src_params.name, "source command schema's parametersSchema has no name"

    # Ensure a system on the destination to attach to.
    dest_sys, created_dest_sys = _ensure_dest_system(dest_node)
    dest_sys_id = dest_sys._resource_id
    new_id = None

    try:
        # Wrap the source's parametersSchema in a fresh JSONCommandSchema
        # and POST to dest's `/systems/{id}/controlstreams`.
        src_input_name = src_cs._underlying_resource.input_name or src_params.name
        dest_resource = ControlStreamResource(
            cs_id="default",
            name=src_cs._underlying_resource.name,
            input_name=src_input_name,
            command_schema=JSONCommandSchema(
                command_format="application/json",
                params_schema=src_params,
            ),
            valid_time=TimePeriod(
                start=TimeInstant.now_as_time_instant(),
                end=TimeInstant(
                    utc_time=TimeUtils.to_utc_time("2026-12-31T00:00:00Z")
                ),
            ),
        )
        new_cs = dest_sys.add_insert_controlstream(dest_resource)
        assert new_cs is not None, "add_insert_controlstream returned None"

        new_id = new_cs.get_id()
        assert new_id and new_id != "default", (
            f"expected a real server-assigned control-stream id from dest's "
            f"Location header; got {new_id!r}"
        )

        # Round-trip verify: re-discover on dest and confirm the schema
        # we POSTed comes back with the same structure.
        dest_streams = dest_sys.discover_controlstreams()
        dest_match = next((cs for cs in dest_streams if cs.get_id() == new_id), None)
        assert dest_match is not None, (
            f"newly-created control stream {new_id!r} not found in "
            f"discover_controlstreams() on dest"
        )
        dest_cmd_schema = dest_match._underlying_resource.command_schema
        assert isinstance(dest_cmd_schema, JSONCommandSchema)
        dest_params = dest_cmd_schema.params_schema
        assert dest_params.name == src_params.name, (
            f"parametersSchema.name didn't round-trip: "
            f"src={src_params.name!r}, dest={dest_params.name!r}"
        )

        def _child_names(component):
            # DataChoice has `items`, DataRecord has `fields`. Either is
            # a list of named SWE components.
            for attr in ("items", "fields"):
                children = getattr(component, attr, None)
                if children:
                    return {c.name for c in children}
            return set()

        src_children = _child_names(src_params)
        dest_children = _child_names(dest_params)
        assert src_children == dest_children, (
            f"command schema child names differ across sync: "
            f"src={src_children}, dest={dest_children}"
        )

        print(
            f"Synced control stream {src_cs.get_id()} from :{SRC_PORT} → "
            f"control stream {new_id} on :{DEST_PORT} "
            f"(child fields: {sorted(src_children)})"
        )
    finally:
        if new_id:
            _delete_resource(dest_node, f"controlstreams/{new_id}")
        if created_dest_sys and dest_sys_id:
            _delete_resource(dest_node, f"systems/{dest_sys_id}")


def _build_command_payload(cmd_schema: JSONCommandSchema) -> dict:
    """Build a sensible command payload for the given parsed command
    schema. Picks the first scalar item with a known type. Used to
    exercise the send-command code path without hard-coding a sensor's
    parameter names."""
    params = cmd_schema.params_schema
    # DataChoice has `items`, DataRecord has `fields`. Walk whichever is
    # populated and pick the first scalar with a defaulted value we can
    # generate.
    children = getattr(params, "items", None) or getattr(params, "fields", None) or []
    for child in children:
        ctype = getattr(child, "type", None)
        if ctype == "Boolean":
            return {child.name: False}
        if ctype in ("Count", "Quantity"):
            return {child.name: 1}
        if ctype in ("Text", "Category"):
            return {child.name: "x"}
    raise pytest.skip(
        f"command schema {params.name!r} has no scalar item we know how to "
        f"populate (children types: {[getattr(c, 'type', '?') for c in children]})"
    )


@pytest.mark.network
def test_send_command_after_sync_src_to_dest(src_node, dest_node):
    """Two-leg test of the command-send path:

    1. POST a command against the SOURCE node's existing control stream
       (where a real driver is registered — for the controllable counter
       sample sensor, this exercises actual command execution).
    2. Sync the same control stream's schema to DEST and POST the same
       command body to the freshly-inserted copy. Dest may not have a
       driver behind the inserted control stream (OSH typically rejects
       commands without one); we tolerate that with a clear log line so
       the test still proves the source path works end-to-end.

    Either way, the test verifies our `CommandJSON` model serializes to
    the wire shape OSH accepts (``parameters`` field, not ``params``).
    """
    src_cs = _first_controlstream_with_schema(src_node)
    if src_cs is None:
        pytest.skip(f"no control streams with schemas found on any system at :{SRC_PORT}")

    cached = src_cs._underlying_resource.command_schema
    assert cached is not None, "expected discover_controlstreams to cache command_schema"
    payload = _build_command_payload(cached)
    print(f"Command payload chosen for schema {cached.params_schema.name!r}: {payload}")

    # --- Leg 1: send to the source's real control stream --------------
    src_api = src_node.get_api_helper()
    src_command = CommandJSON(params=payload)
    src_resp = src_api.create_resource(
        APIResourceTypes.COMMAND,
        src_command.to_csapi_dict(),
        parent_res_id=src_cs.get_id(),
        req_headers={'Content-Type': 'application/json'},
    )
    # CS API Part 2 allows 200 (sync), 201 (created), or 202 (async accepted).
    assert src_resp.status_code in (200, 201, 202), (
        f"source command POST returned {src_resp.status_code}: {src_resp.text[:300]}"
    )
    print(
        f"Source command accepted: HTTP {src_resp.status_code} "
        f"(body[:200]={src_resp.text[:200]!r})"
    )

    # --- Leg 2: sync schema to dest, then send to the new control stream
    dest_sys, created_dest_sys = _ensure_dest_system(dest_node)
    dest_sys_id = dest_sys._resource_id
    new_id = None

    try:
        src_input_name = src_cs._underlying_resource.input_name or cached.params_schema.name
        dest_resource = ControlStreamResource(
            cs_id="default",
            name=src_cs._underlying_resource.name,
            input_name=src_input_name,
            command_schema=JSONCommandSchema(
                command_format="application/json",
                params_schema=cached.params_schema,
            ),
            valid_time=TimePeriod(
                start=TimeInstant.now_as_time_instant(),
                end=TimeInstant(
                    utc_time=TimeUtils.to_utc_time("2026-12-31T00:00:00Z")
                ),
            ),
        )
        new_cs = dest_sys.add_insert_controlstream(dest_resource)
        new_id = new_cs.get_id()
        assert new_id and new_id != "default"

        dest_api = dest_node.get_api_helper()
        dest_command = CommandJSON(params=payload)
        dest_resp = dest_api.create_resource(
            APIResourceTypes.COMMAND,
            dest_command.to_csapi_dict(),
            parent_res_id=new_id,
            req_headers={'Content-Type': 'application/json'},
        )
        # CS API Part 2 allows 200 (sync), 201 (created), or 202 (async).
        # On a freshly-syncd dest with no driver behind the control
        # stream, OSH typically returns 202 (queued) rather than 200
        # (executed) — that's still success.
        assert dest_resp.status_code in (200, 201, 202), (
            f"dest command POST on control stream {new_id} returned "
            f"{dest_resp.status_code}: {dest_resp.text[:300]}"
        )
        print(
            f"Dest command accepted: HTTP {dest_resp.status_code} "
            f"on control stream {new_id} "
            f"(body[:200]={dest_resp.text[:200]!r})"
        )
    finally:
        if new_id:
            _delete_resource(dest_node, f"controlstreams/{new_id}")
        if created_dest_sys and dest_sys_id:
            _delete_resource(dest_node, f"systems/{dest_sys_id}")
