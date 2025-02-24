#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/7/1
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import websockets
from consys4py.comm.mqtt import MQTTCommClient
from consys4py.datamodels.commands import CommandJSON
from consys4py.datamodels.control_streams import ControlStreamJSONSchema

from oshconnect.osh_connect_datamodels import System


class ControlSchema:
    schema: dict = None


class ControlStream:
    name: str = None
    _parent_system: System = None
    _strategy: str = "mqtt"
    _resource_endpoint = None
    # _auth: str = None
    _websocket: websockets.WebSocketServerProtocol = None
    _schema: ControlStreamJSONSchema = None
    _mqtt_client: MQTTCommClient = None
    _id: str = None

    def __init__(self, parent_system: System, resource_endpoint: str, name=None, strategy="mqtt"):
        self._parent_system = parent_system
        self.name = name
        self._strategy = strategy
        self._resource_endpoint = resource_endpoint

    def set_schema(self, schema: ControlStreamJSONSchema):
        self._schema = schema

    def connect(self):
        pass

    def subscribe(self):
        if self._strategy == "mqtt" and self._mqtt_client is not None:
            self._mqtt_client.subscribe(f'{self._resource_endpoint}/commands')
        elif self._strategy == "mqtt" and self._mqtt_client is None:
            raise ValueError("No MQTT Client found.")
        elif self._strategy == "websocket":
            pass

    def publish_status(self, payload: CommandJSON):
        if self._strategy == "mqtt" and self._mqtt_client is not None:
            self._mqtt_client.publish(f'{self._resource_endpoint}/status', payload=payload, qos=1)
        elif self._strategy == "mqtt" and self._mqtt_client is None:
            raise ValueError("No MQTT Client found.")
        elif self._strategy == "websocket":
            pass

    def publish_command(self, payload: CommandJSON):
        if self._strategy == "mqtt" and self._mqtt_client is not None:
            self._mqtt_client.publish(f'{self._resource_endpoint}/status', payload=payload, qos=1)
        elif self._strategy == "mqtt" and self._mqtt_client is None:
            raise ValueError("No MQTT Client found.")
        elif self._strategy == "websocket":
            pass

    def disconnect(self):
        pass

    def unsubscribe(self):
        self._mqtt_client.unsubscribe(f'{self._resource_endpoint}/commands')

    def get_schema(self):
        return self._schema

    def set_id(self, id: str):
        self._id = id

    def get_id(self):
        return self._id

    def add_status_listener(self, listener: callable):
        """
        Adds a callback function which will be called when a status message is received via MQTT.
        :param listener: callback function that executes when the status message is received.
        :return: None
        """
        self._mqtt_client.subscribe(f'{self._resource_endpoint}/status', msg_callback=listener)

    def add_command_listener_and_callback(self, callback: callable):
        """
        Creates a listener for the command stream and adds a callback function which will be called when a command message is received.
        """
        self._mqtt_client.subscribe(f'{self._resource_endpoint}/commands', msg_callback=callback)

    def send_command(self, command: CommandJSON | Command):
        if isinstance(command, CommandJSON):
            self.publish_command(command)
        elif isinstance(command, Command):
            self.publish_status(command.record)


class Command:
    _id: str = None
    record: CommandJSON = None
    status: str = None
    _parent_stream: ControlStream = None

    def send(self):
        self._parent_stream.send_command(self)

    def update_status(self, status_val: str):
        self._parent_stream.publish_status(status_val)

