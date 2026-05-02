#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .streamableresource import Node, System, Datastream, ControlStream, SessionManager


class DataStore(ABC):
    """Abstract interface for persisting OSHConnect resource graphs.

    Implementations must provide CRUD operations for Node, System, Datastream,
    and ControlStream objects. Observations are out of scope.

    The ``load_all`` / ``load_node`` / ``load_all_nodes`` methods accept an
    optional *session_manager* so that deserialized Nodes can register a client
    session — required because ``StreamableResource.__init__`` calls
    ``node.register_streamable()``, which needs an active session.
    """

    # ------------------------------------------------------------------
    # Node
    # ------------------------------------------------------------------

    @abstractmethod
    def save_node(self, node: Node) -> None:
        """Persist a Node (upsert semantics)."""
        ...

    @abstractmethod
    def load_node(self, node_id: str, session_manager: SessionManager = None) -> Optional[Node]:
        """Load a single Node by its string ID. Returns None if not found."""
        ...

    @abstractmethod
    def load_all_nodes(self, session_manager: SessionManager = None) -> list[Node]:
        """Load all stored Nodes."""
        ...

    @abstractmethod
    def delete_node(self, node_id: str) -> None:
        """Delete a Node row. Does NOT cascade-delete child resources."""
        ...

    # ------------------------------------------------------------------
    # System
    # ------------------------------------------------------------------

    @abstractmethod
    def save_system(self, system: System, node: Node) -> None:
        """Persist a System under the given Node (upsert)."""
        ...

    @abstractmethod
    def load_system(self, system_id: str, node: Node) -> Optional[System]:
        """Load a single System by its internal UUID string. Returns None if not found."""
        ...

    @abstractmethod
    def load_systems_for_node(self, node_id: str, node: Node) -> list[System]:
        """Load all Systems stored under *node_id*."""
        ...

    @abstractmethod
    def delete_system(self, system_id: str) -> None:
        """Delete a System row."""
        ...

    # ------------------------------------------------------------------
    # Datastream
    # ------------------------------------------------------------------

    @abstractmethod
    def save_datastream(self, datastream: Datastream, node: Node) -> None:
        """Persist a Datastream (upsert)."""
        ...

    @abstractmethod
    def load_datastream(self, datastream_id: str, node: Node) -> Optional[Datastream]:
        """Load a single Datastream by its internal UUID string."""
        ...

    @abstractmethod
    def load_datastreams_for_system(self, system_id: str, node: Node) -> list[Datastream]:
        """Load all Datastreams whose *parent_resource_id* matches *system_id*."""
        ...

    @abstractmethod
    def delete_datastream(self, datastream_id: str) -> None:
        """Delete a Datastream row."""
        ...

    # ------------------------------------------------------------------
    # ControlStream
    # ------------------------------------------------------------------

    @abstractmethod
    def save_controlstream(self, controlstream: ControlStream, node: Node) -> None:
        """Persist a ControlStream (upsert)."""
        ...

    @abstractmethod
    def load_controlstream(self, controlstream_id: str, node: Node) -> Optional[ControlStream]:
        """Load a single ControlStream by its internal UUID string."""
        ...

    @abstractmethod
    def load_controlstreams_for_system(self, system_id: str, node: Node) -> list[ControlStream]:
        """Load all ControlStreams whose *parent_resource_id* matches *system_id*."""
        ...

    @abstractmethod
    def delete_controlstream(self, controlstream_id: str) -> None:
        """Delete a ControlStream row."""
        ...

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    @abstractmethod
    def save_all(self, nodes: list[Node]) -> None:
        """Persist an entire Node graph (nodes + their systems + streams)."""
        ...

    @abstractmethod
    def load_all(self, session_manager: SessionManager = None) -> list[Node]:
        """Reconstruct the full graph from storage, returning top-level Nodes.

        Pass *session_manager* so reconstructed Nodes can register a client
        session — required for their child resources to initialise correctly.
        """
        ...

    @abstractmethod
    def close(self) -> None:
        """Release any held resources (file handles, connections)."""
        ...
