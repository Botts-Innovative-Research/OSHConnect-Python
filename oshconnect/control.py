#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/7/1
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
import websockets
from consys4py.comm.mqtt import MQTTCommClient
from consys4py.datamodels.commands import CommandJSON
from consys4py.datamodels.control_streams import ControlStreamJSONSchema

from oshconnect.osh_connect_datamodels import System


class ControlSchema:
    schema: dict = None


class ControlStream:
    name: str = None
    _parent_systems: System = None
    _strategy: str = "mqtt"
    _resource_endpoint = None
    # _auth: str = None
    _websocket: websockets.WebSocketServerProtocol = None
    _schema: ControlStreamJSONSchema = None
    _mqtt_client: MQTTCommClient = None

    def __init__(self, parent_system: System, resource_endpoint: str, name=None, strategy="mqtt"):
        self._parent_systems = parent_system
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

    def publish(self, payload: CommandJSON):
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


class Command:
    pass
