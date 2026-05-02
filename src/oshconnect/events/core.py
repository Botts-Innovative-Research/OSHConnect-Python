#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict


class DefaultEventTypes(Enum):
    ADD_NODE: str = "add_node"
    REMOVE_NODE: str = "remove_node"
    ADD_SYSTEM: str = "add_system"
    REMOVE_SYSTEM: str = "remove_system"
    ADD_DATASTREAM: str = "add_datastream"
    REMOVE_DATASTREAM: str = "remove_datastream"
    ADD_CONTROLSTREAM: str = "add_controlstream"
    REMOVE_CONTROLSTREAM: str = "remove_controlstream"
    NEW_OBSERVATION: str = "new_observation"
    NEW_COMMAND: str = "new_command"
    NEW_COMMAND_STATUS: str = "new_command_status"


class AtomicEventTypes(Enum):
    """
    Defines atomic event types for local resource operations.

    Attributes:
        CREATE (str): Creating a resource within OSHConnect (local, in-app).
        POST (str): Posting a resource to an external server.
        GET (str): Retrieving a resource from an external server.
        MODIFY (str): Modifying a resource within OSHConnect (local, in-app).
        UPDATE (str): Updating a resource on an external server.
        REMOVE (str): Removing a resource within OSHConnect (local, in-app).
        DELETE (str): Deleting a resource from an external server.
    """
    CREATE: str = "create"
    POST: str = "post"
    GET: str = "get"
    MODIFY: str = "modify"
    UPDATE: str = "update"
    REMOVE: str = "remove"
    DELETE: str = "delete"


class Event(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    timestamp: datetime.datetime
    type: DefaultEventTypes
    topic: str
    data: Any
    producer: Any

    @classmethod
    def blank_event(cls) -> Event:
        return cls(
            timestamp=datetime.datetime.now(),
            type=DefaultEventTypes.NEW_OBSERVATION,
            topic="",
            data=None,
            producer=None
        )
