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
from oshconnect.resource_datamodels import DatastreamResource
from oshconnect.schema_datamodels import SWEDatastreamRecordSchema
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
        name="SyncTarget",
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
    """Fetch the first datastream's SWE+JSON schema from the source node,
    use its ``recordSchema`` (the inner SWE Common DataRecord) to create a
    new datastream on the destination, then verify by fetching the new
    schema back and comparing structure."""
    src_ds = _first_datastream_with_schema(src_node)
    if src_ds is None:
        pytest.skip(f"no datastreams found on any system at :{SRC_PORT}")

    # Eager-fetch contract: discover_datastreams should already have
    # populated the SWE+JSON schema on the underlying resource. Without
    # this, every workflow that needs the schema (cross-node sync,
    # observation building, etc.) silently breaks.
    cached = src_ds._underlying_resource.record_schema
    assert cached is not None, (
        "discover_datastreams should populate _underlying_resource.record_schema"
    )
    assert isinstance(cached, SWEDatastreamRecordSchema)

    # The explicit fetch path is still supported and exercised here too.
    src_schema = src_ds.fetch_swejson_schema()
    src_record = src_schema.record_schema
    assert src_record.name, "source schema's recordSchema has no name"

    # Ensure a system on the destination to attach to.
    dest_sys, created_dest_sys = _ensure_dest_system(dest_node)
    dest_sys_id = dest_sys._resource_id  # System has no public id getter
    new_id = None

    try:
        # `System.add_insert_datastream` now takes a fully-built
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

        # Round-trip verify: fetch the new schema from dest and confirm
        # the field structure matches the source.
        dest_schema = new_ds.fetch_swejson_schema()
        dest_record = dest_schema.record_schema
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
