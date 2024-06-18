#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/13
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import uuid

from oshconnect import Node
from external_models.object_models import System as SystemResource


class System:
    uid: uuid.UUID
    name: str
    label: str
    datastreams: list[Datastream]
    control_channels: list[ControlChannel]
    description: str
    _parent_node: Node

    def __init__(self, name: str, label: str, **kwargs):
        self.uid = uuid.uuid4()
        self.name = name
        self.label = label
        self.datastreams = []
        self.control_channels = []

    def update_parent_node(self, node: Node):
        self._parent_node = node

    @staticmethod
    def from_system_resource(system_resource: SystemResource):
        other_props = SystemResource.__dict__
        return System(name=system_resource.name, label=system_resource.label)


class Datastream:

    def __init__(self):
        pass


class ControlChannel:

    def __init__(self):
        pass
