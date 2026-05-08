#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   ==============================================================================

"""Tests for the DataStore layer (SQLiteDataStore) — no live OSH server required.

All tests use SQLiteDataStore(":memory:") so there is no file I/O.
"""

import pytest

from src.oshconnect import OSHConnect
from src.oshconnect.datastores import SQLiteDataStore
from src.oshconnect.resource_datamodels import (
    ControlStreamResource,
    DatastreamResource,
)
from src.oshconnect.streamableresource import (
    ControlStream,
    Datastream,
    Node,
    SessionManager,
    System,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_node(sm: SessionManager = None) -> Node:
    """Create a real Node registered with *sm* (or a fresh SessionManager)."""
    if sm is None:
        sm = SessionManager()
    node = Node(
        protocol="http",
        address="localhost",
        port=8282,
        username="admin",
        password="admin",
    )
    node.register_with_session_manager(sm)
    return node


def make_system(node: Node) -> System:
    return System(
        label="Test System",
        urn="urn:test:sensors:sys1",
        parent_node=node,
        resource_id="sys001",
    )


def make_datastream(node: Node) -> Datastream:
    ds_resource = DatastreamResource.model_validate({
        "id": "ds001",
        "name": "Test Datastream",
        "validTime": ["2024-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
    })
    return Datastream(parent_node=node, datastream_resource=ds_resource)


def make_controlstream(node: Node) -> ControlStream:
    cs_resource = ControlStreamResource.model_validate({
        "id": "cs001",
        "name": "Test ControlStream",
    })
    return ControlStream(node=node, controlstream_resource=cs_resource)


# ---------------------------------------------------------------------------
# Node round-trip
# ---------------------------------------------------------------------------

class TestNodeRoundTrip:
    def test_save_and_load_node(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        original_id = node.get_id()

        store.save_node(node)
        loaded = store.load_node(original_id, session_manager=sm)

        assert loaded is not None
        assert loaded.get_id() == original_id
        assert loaded.address == node.address
        assert loaded.port == node.port

    def test_load_missing_node_returns_none(self):
        store = SQLiteDataStore(":memory:")
        assert store.load_node("nonexistent-id") is None

    def test_load_all_nodes(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node1 = make_node(sm)
        node2 = make_node(sm)
        store.save_node(node1)
        store.save_node(node2)

        nodes = store.load_all_nodes(session_manager=sm)
        assert len(nodes) == 2
        ids = {n.get_id() for n in nodes}
        assert node1.get_id() in ids
        assert node2.get_id() in ids

    def test_delete_node(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        store.save_node(node)
        store.delete_node(node.get_id())
        assert store.load_node(node.get_id()) is None

    def test_upsert_overwrites_existing_node(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        store.save_node(node)
        store.save_node(node)  # second save should not raise
        nodes = store.load_all_nodes(session_manager=sm)
        assert len(nodes) == 1


# ---------------------------------------------------------------------------
# System CRUD
# ---------------------------------------------------------------------------

class TestSystemCRUD:
    def test_save_and_load_system(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        system = make_system(node)
        system_id = str(system.get_internal_id())

        store.save_system(system, node)
        loaded = store.load_system(system_id, node)

        assert loaded is not None
        assert loaded.label == system.label
        assert loaded.urn == system.urn

    def test_load_missing_system_returns_none(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        assert store.load_system("missing-id", node) is None

    def test_load_systems_for_node(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        sys1 = make_system(node)
        sys2 = System(
            label="System Two",
            urn="urn:test:sensors:sys2",
            parent_node=node,
            resource_id="sys002",
        )
        store.save_system(sys1, node)
        store.save_system(sys2, node)

        systems = store.load_systems_for_node(node.get_id(), node)
        assert len(systems) == 2
        labels = {s.label for s in systems}
        assert "Test System" in labels
        assert "System Two" in labels

    def test_delete_system(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        system = make_system(node)
        system_id = str(system.get_internal_id())
        store.save_system(system, node)
        store.delete_system(system_id)
        assert store.load_system(system_id, node) is None


# ---------------------------------------------------------------------------
# Datastream CRUD
# ---------------------------------------------------------------------------

class TestDatastreamCRUD:
    def test_save_and_load_datastream(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        ds = make_datastream(node)
        ds_id = str(ds.get_internal_id())

        store.save_datastream(ds, node)
        loaded = store.load_datastream(ds_id, node)

        assert loaded is not None
        assert loaded.get_id() == ds.get_id()

    def test_load_missing_datastream_returns_none(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        assert store.load_datastream("missing-id", node) is None

    def test_delete_datastream(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        ds = make_datastream(node)
        ds_id = str(ds.get_internal_id())
        store.save_datastream(ds, node)
        store.delete_datastream(ds_id)
        assert store.load_datastream(ds_id, node) is None


# ---------------------------------------------------------------------------
# ControlStream CRUD
# ---------------------------------------------------------------------------

class TestControlStreamCRUD:
    def test_save_and_load_controlstream(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        cs = make_controlstream(node)
        cs_id = str(cs.get_internal_id())

        store.save_controlstream(cs, node)
        loaded = store.load_controlstream(cs_id, node)

        assert loaded is not None

    def test_delete_controlstream(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        cs = make_controlstream(node)
        cs_id = str(cs.get_internal_id())
        store.save_controlstream(cs, node)
        store.delete_controlstream(cs_id)
        assert store.load_controlstream(cs_id, node) is None


# ---------------------------------------------------------------------------
# Bulk save_all / load_all
# ---------------------------------------------------------------------------

class TestBulkOperations:
    def test_save_all_and_load_all(self):
        store = SQLiteDataStore(":memory:")
        sm = SessionManager()
        node = make_node(sm)
        system = make_system(node)
        node.add_system(system)

        store.save_all([node])
        nodes = store.load_all(session_manager=sm)

        assert len(nodes) == 1
        loaded_node = nodes[0]
        assert loaded_node.get_id() == node.get_id()
        assert len(loaded_node.systems()) == 1
        assert loaded_node.systems()[0].label == system.label

    def test_save_all_empty_node_list(self):
        store = SQLiteDataStore(":memory:")
        store.save_all([])
        assert store.load_all() == []

    def test_load_all_empty_store(self):
        store = SQLiteDataStore(":memory:")
        assert store.load_all() == []


# ---------------------------------------------------------------------------
# OSHConnect integration
# ---------------------------------------------------------------------------

class TestOSHConnectIntegration:
    def test_save_to_store_and_load_from_store(self):
        store = SQLiteDataStore(":memory:")
        app = OSHConnect(name="test-app", datastore=store)

        node = Node(
            protocol="http",
            address="localhost",
            port=8282,
            username="admin",
            password="admin",
        )
        app.add_node(node)
        system = make_system(node)
        app.add_system_to_node(system, node)

        app.save_to_store()

        # Restore into a fresh OSHConnect instance using the same in-memory store
        app2 = OSHConnect(name="test-app-restored", datastore=store)
        app2.load_from_store()

        assert len(app2._nodes) == 1
        assert len(app2._systems) == 1
        assert app2._systems[0].label == system.label

    def test_save_to_store_no_datastore_raises(self):
        app = OSHConnect(name="no-store-app")
        with pytest.raises(RuntimeError):
            app.save_to_store()

    def test_load_from_store_no_datastore_raises(self):
        app = OSHConnect(name="no-store-app")
        with pytest.raises(RuntimeError):
            app.load_from_store()

    def test_multiple_instances_do_not_share_node_list(self):
        """Regression: class-level mutable defaults used to share _nodes across instances."""
        app1 = OSHConnect(name="app1")
        app2 = OSHConnect(name="app2")
        node = Node(protocol="http", address="localhost", port=8282)
        app1.add_node(node)
        assert len(app1._nodes) == 1
        assert len(app2._nodes) == 0
