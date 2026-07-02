#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Optional

from ..datastore import DataStore
from ..streamableresource import (
    ControlStream,
    Datastream,
    Node,
    SessionManager,
    System,
)


class SQLiteDataStore(DataStore):
    """SQLite-backed DataStore implementation using Python's stdlib ``sqlite3``.

    Pass ``db_path=":memory:"`` for in-process testing with no file I/O.

    Schema notes
    ------------
    Each resource type is stored as a single JSON blob (the output of its
    ``to_storage_dict()`` method) alongside a primary-key string ID and any
    foreign-key columns needed for filtered lookups. Using blobs means new
    Pydantic fields do not require schema migrations.

    *Bulk operations* (``save_all`` / ``load_all``) work at the Node level:
    ``save_all`` persists every resource separately for individual lookups;
    ``load_all`` reconstructs the full hierarchy from the *nodes* table only
    (``Node.from_storage_dict`` handles the embedded systems/streams), avoiding
    duplication.
    """

    def __init__(self, db_path: str | Path = "oshconnect.db") -> None:
        self._db_path = Path(db_path) if db_path != ":memory:" else db_path
        self._conn: sqlite3.Connection = sqlite3.connect(
            str(self._db_path), check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        self._create_tables()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_tables(self) -> None:
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS nodes (
                id   TEXT PRIMARY KEY,
                data TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS systems (
                id      TEXT PRIMARY KEY,
                node_id TEXT NOT NULL,
                data    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS datastreams (
                id        TEXT PRIMARY KEY,
                system_id TEXT,
                node_id   TEXT NOT NULL,
                data      TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS controlstreams (
                id        TEXT PRIMARY KEY,
                system_id TEXT,
                node_id   TEXT NOT NULL,
                data      TEXT NOT NULL
            );
        """)
        self._conn.commit()

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        return self._conn.execute(sql, params)

    # ------------------------------------------------------------------
    # Node
    # ------------------------------------------------------------------

    def save_node(self, node: Node) -> None:
        data = json.dumps(node.to_storage_dict())
        self._execute(
            "INSERT OR REPLACE INTO nodes (id, data) VALUES (?, ?)",
            (node.get_id(), data),
        )
        self._conn.commit()

    def load_node(
        self, node_id: str, session_manager: Optional[SessionManager] = None
    ) -> Optional[Node]:
        row = self._execute(
            "SELECT data FROM nodes WHERE id = ?", (node_id,)
        ).fetchone()
        if row is None:
            return None
        return Node.from_storage_dict(json.loads(row["data"]), session_manager=session_manager)

    def load_all_nodes(
        self, session_manager: Optional[SessionManager] = None
    ) -> list[Node]:
        rows = self._execute("SELECT data FROM nodes").fetchall()
        return [
            Node.from_storage_dict(json.loads(r["data"]), session_manager=session_manager)
            for r in rows
        ]

    def delete_node(self, node_id: str) -> None:
        self._execute("DELETE FROM nodes WHERE id = ?", (node_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # System
    # ------------------------------------------------------------------

    def save_system(self, system: System, node: Node) -> None:
        system_id = str(system.get_internal_id())
        data = json.dumps(system.to_storage_dict())
        self._execute(
            "INSERT OR REPLACE INTO systems (id, node_id, data) VALUES (?, ?, ?)",
            (system_id, node.get_id(), data),
        )
        self._conn.commit()

    def load_system(self, system_id: str, node: Node) -> Optional[System]:
        row = self._execute(
            "SELECT data FROM systems WHERE id = ?", (system_id,)
        ).fetchone()
        if row is None:
            return None
        return System.from_storage_dict(json.loads(row["data"]), node)

    def load_systems_for_node(self, node_id: str, node: Node) -> list[System]:
        rows = self._execute(
            "SELECT data FROM systems WHERE node_id = ?", (node_id,)
        ).fetchall()
        return [System.from_storage_dict(json.loads(r["data"]), node) for r in rows]

    def delete_system(self, system_id: str) -> None:
        self._execute("DELETE FROM systems WHERE id = ?", (system_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Datastream
    # ------------------------------------------------------------------

    def save_datastream(self, datastream: Datastream, node: Node) -> None:
        ds_id = str(datastream.get_internal_id())
        system_id = datastream.get_parent_resource_id()
        data = json.dumps(datastream.to_storage_dict())
        self._execute(
            "INSERT OR REPLACE INTO datastreams (id, system_id, node_id, data) VALUES (?, ?, ?, ?)",
            (ds_id, system_id, node.get_id(), data),
        )
        self._conn.commit()

    def load_datastream(self, datastream_id: str, node: Node) -> Optional[Datastream]:
        row = self._execute(
            "SELECT data FROM datastreams WHERE id = ?", (datastream_id,)
        ).fetchone()
        if row is None:
            return None
        return Datastream.from_storage_dict(json.loads(row["data"]), node)

    def load_datastreams_for_system(self, system_id: str, node: Node) -> list[Datastream]:
        rows = self._execute(
            "SELECT data FROM datastreams WHERE system_id = ?", (system_id,)
        ).fetchall()
        return [Datastream.from_storage_dict(json.loads(r["data"]), node) for r in rows]

    def delete_datastream(self, datastream_id: str) -> None:
        self._execute("DELETE FROM datastreams WHERE id = ?", (datastream_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # ControlStream
    # ------------------------------------------------------------------

    def save_controlstream(self, controlstream: ControlStream, node: Node) -> None:
        cs_id = str(controlstream.get_internal_id())
        system_id = controlstream.get_parent_resource_id()
        data = json.dumps(controlstream.to_storage_dict())
        self._execute(
            "INSERT OR REPLACE INTO controlstreams (id, system_id, node_id, data) VALUES (?, ?, ?, ?)",
            (cs_id, system_id, node.get_id(), data),
        )
        self._conn.commit()

    def load_controlstream(self, controlstream_id: str, node: Node) -> Optional[ControlStream]:
        row = self._execute(
            "SELECT data FROM controlstreams WHERE id = ?", (controlstream_id,)
        ).fetchone()
        if row is None:
            return None
        return ControlStream.from_storage_dict(json.loads(row["data"]), node)

    def load_controlstreams_for_system(self, system_id: str, node: Node) -> list[ControlStream]:
        rows = self._execute(
            "SELECT data FROM controlstreams WHERE system_id = ?", (system_id,)
        ).fetchall()
        return [ControlStream.from_storage_dict(json.loads(r["data"]), node) for r in rows]

    def delete_controlstream(self, controlstream_id: str) -> None:
        self._execute("DELETE FROM controlstreams WHERE id = ?", (controlstream_id,))
        self._conn.commit()

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def save_all(self, nodes: list[Node]) -> None:
        """Walk the full Node graph and persist every resource individually."""
        for node in nodes:
            self.save_node(node)
            for system in node.systems():
                self.save_system(system, node)
                for ds in system.datastreams:
                    self.save_datastream(ds, node)
                for cs in system.control_channels:
                    self.save_controlstream(cs, node)

    def load_all(
        self, session_manager: Optional[SessionManager] = None
    ) -> list[Node]:
        """Reconstruct the full resource graph from the nodes table.

        ``Node.from_storage_dict`` handles the embedded systems/datastreams/
        controlstreams hierarchy, so only the *nodes* table is used here.
        The individual resource tables (systems, datastreams, controlstreams)
        exist for targeted single-resource lookups and are not consulted here
        to avoid double-instantiation.
        """
        return self.load_all_nodes(session_manager=session_manager)

    def clear(self) -> None:
        """Delete all persisted resources from every table."""
        self._conn.executescript("""
            DELETE FROM controlstreams;
            DELETE FROM datastreams;
            DELETE FROM systems;
            DELETE FROM nodes;
        """)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()
