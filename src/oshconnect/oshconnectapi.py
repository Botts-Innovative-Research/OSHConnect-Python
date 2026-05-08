#  ==============================================================================
#  Copyright (c) 2024. Botts Innovative Research, Inc.
#  Date:  2024/5/15
#  Author:  Ian Patterson
#  Contact email:  ian@botts-inc.com
#   ==============================================================================
import logging
import json
from typing import Callable
from uuid import UUID

from .events import EventHandler, DefaultEventTypes, CallbackListener
from .events.builder import EventBuilder
from .csapi4py.default_api_helpers import APIHelper
from .datastore import DataStore
from .resource_datamodels import DatastreamResource
from .streamableresource import Node, System, SessionManager, Datastream, ControlStream
from .styling import Styling
from .timemanagement import TemporalModes, TimeManagement, TimePeriod


class OSHConnect:
    _name: str
    datastore: DataStore
    styling: Styling
    timestream: TimeManagement
    _nodes: list[Node]
    _systems: list[System]
    _cs_api_builder: APIHelper
    _datastreams: list[Datastream]
    _controlstreams: list[ControlStream]
    _datagroups: list
    _tasks: list
    _playback_mode: TemporalModes
    _session_manager: SessionManager
    _event_bus: EventHandler

    def __init__(self, name: str, datastore: DataStore = None, **kwargs):
        """
        :param name: name of the OSHConnect instance
        :param datastore: optional DataStore backend for persisting the resource graph
        :param kwargs:
        """
        self._name = name
        self.datastore = datastore
        self.styling = None
        self.timestream = None
        self._nodes = []
        self._systems = []
        self._cs_api_builder = None
        self._datastreams = []
        self._controlstreams = []
        self._datagroups = []
        self._tasks = []
        self._playback_mode = TemporalModes.REAL_TIME
        logging.info(f"OSHConnect instance {name} created")
        self._session_manager = SessionManager()
        self._event_bus = EventHandler()

    def get_name(self):
        """
        Get the name of the OSHConnect instance.
        :return:
        """
        return self._name

    def add_node(self, node: Node):
        """
        Add a node to the OSHConnect instance.
        :param node: Node object
        :return:
        """
        node.register_with_session_manager(self._session_manager)
        self._nodes.append(node)
        self._event_bus.publish(
            EventBuilder().with_type(DefaultEventTypes.ADD_NODE)
            .with_topic(EventBuilder.create_topic(DefaultEventTypes.ADD_NODE, node.get_id()))
            .with_data(node).with_producer(self).build()
        )

    def remove_node(self, node_id: str):
        """
        Remove a node from the OSHConnect instance.
        :param node_id:
        :return:
        """
        # TODO: should disconnect datastreams and delete them and all systems at the same time.
        # list of nodes in our node list that do not have the id of the node we want to remove
        self._nodes = [node for node in self._nodes if
                       node.get_id() != node_id]
        self._event_bus.publish(
            EventBuilder().with_type(DefaultEventTypes.REMOVE_NODE)
            .with_topic(EventBuilder.create_topic(DefaultEventTypes.REMOVE_NODE, node_id))
            .with_data(node_id).with_producer(self).build()
        )

    def save_config(self):
        logging.info(f"Saving configuration for {self._name}")

        data = {}
        for node in self._nodes:
            node_dict = node.to_storage_dict()
            data.update({node.get_id(): node_dict})

        # write to JSON file
        file_path = f"{self._name}_config.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump({"app_config": data}, f, ensure_ascii=False, indent=2)

    @classmethod
    def load_config(cls, file_name: str) -> 'OSHConnect':
        """Load configuration data from a JSON file and return the stored config dict.
        Note: Despite the return type hint, this returns the configuration dictionary.
        """
        with open(file_name, 'r', encoding='utf-8') as f:
            obj = json.load(f)
            return obj.get('app_config', obj)

    def save_to_store(self) -> None:
        """Persist the full node graph to the configured datastore.

        :raises RuntimeError: if no datastore has been configured.
        """
        if self.datastore is None:
            raise RuntimeError(
                "No datastore configured. Pass a DataStore instance to OSHConnect()."
            )
        self.datastore.save_all(self._nodes)

    def load_from_store(self) -> None:
        """Restore the node graph from the configured datastore into this instance.

        Reconstructed Nodes are registered with this instance's SessionManager so
        their child resources (Systems, Datastreams, ControlStreams) can initialise
        correctly. Calling this method appends to any already-loaded nodes.

        :raises RuntimeError: if no datastore has been configured.
        """
        if self.datastore is None:
            raise RuntimeError(
                "No datastore configured. Pass a DataStore instance to OSHConnect()."
            )
        nodes = self.datastore.load_all(session_manager=self._session_manager)
        for node in nodes:
            self._nodes.append(node)
            self._systems.extend(node.systems())

    def share_config(self, config: dict):
        pass

    def update_config(self, config: dict):
        pass

    def delete_config(self, config: dict):
        pass

    def configure_nodes(self, nodes: list):
        pass

    def filter_nodes(self, nodes: list):
        pass

    def task_system(self, task: dict):
        pass

    def select_temporal_mode(self, mode: str):
        """
        Select the temporal mode for the system. Real-time, archive, batch, as well as synchronization settings.
        :param mode:
        :return:
        """
        pass

    def visualize_streams(self, streams: list):
        pass

    # Second Level Use Cases
    def get_visualization_recommendations(self, streams: list):
        pass

    def discover_systems(self, nodes: list[str] = None):
        """
        Discover systems from the nodes that have been added to the OSHConnect instance. They are associated with the
        nodes that they are discovered from so access to them flows through there.
        :param nodes:
        :return:
        """
        search_nodes = self._nodes
        if nodes is not None:
            search_nodes = [node for node in search_nodes if
                            node.get_id() in nodes]

        for node in search_nodes:
            res_systems = node.discover_systems()
            self._systems.extend(res_systems)
            for system in res_systems:
                self._event_bus.publish(
                    EventBuilder().with_type(DefaultEventTypes.ADD_SYSTEM)
                    .with_topic(EventBuilder.create_topic(DefaultEventTypes.ADD_SYSTEM,
                                                          getattr(system, '_resource_id', None)))
                    .with_data(system).with_producer(self).build()
                )

    def discover_datastreams(self):
        for system in self._systems:
            datastreams = system.discover_datastreams()
            self._datastreams.extend(datastreams)
            for ds in datastreams:
                self._event_bus.publish(
                    EventBuilder().with_type(DefaultEventTypes.ADD_DATASTREAM)
                    .with_topic(EventBuilder.create_topic(DefaultEventTypes.ADD_DATASTREAM,
                                                          getattr(ds, '_resource_id', None)))
                    .with_data(ds).with_producer(self).build()
                )

    def discover_controlstreams(self, streams: list):
        for system in self._systems:
            controlstreams = system.discover_controlstreams()
            self._controlstreams.extend(controlstreams)
            for cs in controlstreams:
                self._event_bus.publish(
                    EventBuilder().with_type(DefaultEventTypes.ADD_CONTROLSTREAM)
                    .with_topic(EventBuilder.create_topic(DefaultEventTypes.ADD_CONTROLSTREAM,
                                                          getattr(cs, '_resource_id', None)))
                    .with_data(cs).with_producer(self).build()
                )

    def authenticate_user(self, user: dict):
        pass

    def synchronize_streams(self, systems: list):
        pass

    def set_playback_mode(self, mode: TemporalModes):
        self._datasource_handler.set_playback_mode(mode)

    def set_timeperiod(self, start_time: str, end_time: str):
        """
        Sets the time range (TimePeriod) for the OSHConnect instance. This is used to bookend the playback of the
        datastreams.
        :param start_time: ISO8601 formatted string or one of (now or latest)
        :param end_time:  ISO8601 formatted string or one of (now or latest)
        :return:
        """
        tp = TimePeriod(start=start_time, end=end_time)
        self.timestream = TimeManagement(time_range=tp)

    # def get_message_list(self) -> list[MessageWrapper]:
    #     """
    #     Get the list of messages that have been received by the OSHConnect instance.
    #     :return: list of MessageWrapper objects
    #     """
    #     return self._datasource_handler.get_messages()

    def _insert_system(self, system: System, target_node: Node):
        """
        Create a system on the target node.
        :param system: System object
        :param target_node: Node object, must be within the OSHConnect instance
        :return: the created system
        """
        if target_node in self._nodes:
            self.add_system_to_node(system, target_node, insert_resource=True)
            return system

    def add_datastream(self, datastream: DatastreamResource, system: str | System) -> str:
        """
        Adds a datastream into the OSHConnect instance.
        :param datastream: DataSource object
        :param system: System object or system id
        :return:
        """
        sys_obj: System
        if isinstance(system, str):
            sys_obj = self.find_system(system)
            if sys_obj is None:
                raise ValueError(f"System with id {system} not found")
        else:
            sys_obj = system

        sys_obj.add_insert_datastream(datastream)

        self._datastreams.append(datastream)

    def find_system(self, system_id: str) -> System | None:
        """
        Find a system in the OSHConnect instance.
        :param system_id:
        :return: the found system or None if not found
        """
        for system in self._systems:
            if system.uid == system_id:
                return system
        return None

    # System Management
    def add_system_to_node(self, system: System, target_node: Node, insert_resource: bool = False):
        """
        Add a system to the target node.
        :param system: System object
        :param target_node: Node object,  must be within the OSHConnect instance
        :param insert_resource: Whether to insert the system into the target node's server, default is False
        :return:
        """
        if target_node in self._nodes:
            target_node.add_system(system, insert_resource=insert_resource)
            self._systems.append(system)
            return

    def create_and_insert_system(self, system_opts: dict, target_node: Node):
        """
        Create a system on the target node.
        :param system_opts: System object parameters
        :param target_node: Node object, must be within the OSHConnect instance
        :return: the created system
        """
        if target_node in self._nodes:
            new_system = System(**system_opts)
            self.add_system_to_node(new_system, target_node, insert_resource=True)
            return new_system

    def remove_system(self, system_id: str):
        pass

    # DataStream Helpers
    def get_datastreams(self) -> list[Datastream]:
        return self._datastreams

    def get_datastream_ids(self) -> list[UUID]:
        return [ds.get_internal_id() for ds in self._datastreams]

    def connect_session_streams(self, session_id: str):
        """
        Connects all datastreams that are associated with the given session ID.
        :param session_id:
        :return:
        """
        self._session_manager.start_session_streams(session_id)

    def get_resource_group(self, resource_ids: list[UUID]) -> tuple[list[System], list[Datastream]]:
        """
        Get a group of resources by their IDs. Can be any mix of systems, datastreams, and controlstreams.
        :param resource_ids: list of resource IDs (internal UUID)
        """
        systems = [system for system in self._systems if system.get_internal_id() in resource_ids]
        datastreams = [ds for ds in self._datastreams if ds.get_internal_id() in resource_ids]
        return systems, datastreams

    def initialize_resource_groups(self, resource_ids: list = None):
        """
        Initializes the datastreams that are specified.
        """
        systems, datastreams = self.get_resource_group(resource_ids)

        if systems:
            for system in systems:
                system.initialize()
        if datastreams:
            for ds in datastreams:
                ds.initialize()

    def start_datastreams(self, dsid_list: list = None):
        """
        Starts the datastreams that are specified.
        """
        datastreams = self.get_resource_group(dsid_list)[1]
        for ds in datastreams:
            ds.start()

    def start_systems(self, sysid_list: list = None):
        """
        Starts the systems that are specified.
        """
        systems = self.get_resource_group(sysid_list)[0]
        for system in systems:
            system.start()

    # ------------------------------------------------------------------
    # Event subscription convenience methods
    # ------------------------------------------------------------------

    def on_observation(self, callback: Callable, datastream_id: str = None) -> CallbackListener:
        """
        Subscribe to incoming observation events.

        :param callback: ``fn(event: Event)`` called for each matching event.
        :param datastream_id: When provided, only events from that datastream are
            delivered (matched via the datastream's MQTT data topic). When omitted,
            all observation events are delivered.
        :returns: ``CallbackListener`` — pass to ``event_bus.unregister_listener()`` to cancel.
        """
        topic_filter = []
        if datastream_id is not None:
            ds = next((ds for ds in self._datastreams if ds.get_id() == datastream_id), None)
            if ds is not None and getattr(ds, '_topic', None):
                topic_filter = [ds._topic]
        return self._event_bus.subscribe(callback, types=[DefaultEventTypes.NEW_OBSERVATION],
                                         topics=topic_filter)

    def on_system_added(self, callback: Callable) -> CallbackListener:
        """
        Subscribe to system-discovered / system-added events.

        :param callback: ``fn(event: Event)`` where ``event.data`` is the ``System``.
        :returns: ``CallbackListener`` for later removal.
        """
        return self._event_bus.subscribe(callback, types=[DefaultEventTypes.ADD_SYSTEM])

    def on_command(self, callback: Callable, controlstream_id: str = None) -> CallbackListener:
        """
        Subscribe to incoming command events.

        :param callback: ``fn(event: Event)`` called for each matching event.
        :param controlstream_id: When provided, only events from that control stream are
            delivered. When omitted, all command events are delivered.
        :returns: ``CallbackListener`` for later removal.
        """
        topic_filter = []
        if controlstream_id is not None:
            cs = next((cs for cs in self._controlstreams
                       if getattr(cs, '_resource_id', None) == controlstream_id), None)
            if cs is not None and getattr(cs, '_topic', None):
                topic_filter = [cs._topic]
        return self._event_bus.subscribe(callback, types=[DefaultEventTypes.NEW_COMMAND],
                                         topics=topic_filter)

    @property
    def event_bus(self) -> EventHandler:
        """Direct access to the EventHandler for advanced subscriptions."""
        return self._event_bus
