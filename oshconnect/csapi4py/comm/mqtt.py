import paho.mqtt.client as mqtt


class MQTTCommClient:
    def __init__(self, url, port=1883, username=None, password=None, path='mqtt', client_id="", transport='tcp'):
        """
    Wraps a paho mqtt client to provide a simple interface for interacting with the mqtt server that is customized
    for this library.

    :param url: url of the mqtt server
    :param port: port the mqtt server is communicating over, default is 1883 or whichever port the main node is
    using if in websocket mode
    :param username: used if node is requiring authentication to access this service
    :param password: used if node is requiring authentication to access this service
    :param path: used for setting the path when using websockets (usually sensorhub/mqtt by default)
    """
        self.__url = url
        self.__port = port
        self.__path = path
        self.__client_id = client_id
        self.__transport = transport

        self.__client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)

        if self.__transport == 'websockets':
            self.__client.ws_set_options(path=self.__path)

        if username is not None and password is not None:
            self.__client.username_pw_set(username, password)
            self.__client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLSv1_2)

        self.__client.on_connect = self.on_connect
        self.__client.on_subscribe = self.on_subscribe
        self.__client.on_message = self.on_message
        self.__client.on_publish = self.on_publish
        self.__client.on_log = self.on_log
        self.__client.on_disconnect = self.on_disconnect

        self.__is_connected = False

    @staticmethod
    def on_connect(client, userdata, flags, rc, properties):
        print(f'Connected with result code: {rc}')
        print(f'{properties}')

    @staticmethod
    def on_subscribe(client, userdata, mid, granted_qos, properties):
        print(f'Subscribed: {mid} {granted_qos}')

    @staticmethod
    def on_message(client, userdata, msg):
        print(f'{msg.payload.decode("utf-8")}')

    @staticmethod
    def on_publish(client, userdata, mid, info, properties):
        print(f'Published: {mid}')

    @staticmethod
    def on_log(client, userdata, level, buf):
        print(f'Log: {buf}')

    @staticmethod
    def on_disconnect(client, userdata, dc_flag, rc, properties):
        print(f'Client {client} disconnected: {dc_flag} {rc}')

    def connect(self, keepalive=60):
        # print(f'Connecting to {self.__url}:{self.__port}')
        self.__client.connect(self.__url, self.__port, keepalive=keepalive)

    def subscribe(self, topic, qos=0, msg_callback=None):
        """
        Subscribe to a topic, and optionally set a callback for when a message is received on that topic. To actually
        retrieve any information you must set a callback.

        :param topic: MQTT topic to subscribe to (example/topic)
        :param qos: quality of service, 0, 1, or 2
        :param msg_callback: callback with the form: callback(client, userdata, msg)
        :return:
        """
        self.__client.subscribe(topic, qos)
        if msg_callback is not None:
            self.__client.message_callback_add(topic, msg_callback)

    def publish(self, topic, payload=None, qos=0, retain=False):
        self.__client.publish(topic, payload, qos, retain=retain)

    def unsubscribe(self, topic):
        self.__client.unsubscribe(topic)

    def disconnect(self):
        self.__client.disconnect()

    def set_on_connect(self, on_connect):
        """
        Set the on_connect callback for the MQTT client.

        :param on_connect:
        :return:
        """
        self.__client.on_connect = on_connect

    def set_on_disconnect(self, on_disconnect):
        """
        Set the on_disconnect callback for the MQTT client.

        :param on_disconnect:
        :return:
        """
        self.__client.on_disconnect = on_disconnect

    def set_on_subscribe(self, on_subscribe):
        """
        Set the on_subscribe callback for the MQTT client.

        :param on_subscribe:
        :return:
        """
        self.__client.on_subscribe = on_subscribe

    def set_on_unsubscribe(self, on_unsubscribe):
        """
        Set the on_unsubscribe callback for the MQTT client.

        :param on_unsubscribe:
        :return:
        """
        self.__client.on_unsubscribe = on_unsubscribe

    def set_on_publish(self, on_publish):
        """
        Set the on_publish callback for the MQTT client.

        :param on_publish:
        :return:
        """
        self.__client.on_publish = on_publish

    def set_on_message(self, on_message):
        """
        Set the on_message callback for the MQTT client. It is recommended to set individual callbacks for each
        subscribed topic.

        :param on_message:
        :return:
        """
        self.__client.on_message = on_message

    def set_on_log(self, on_log):
        """
        Set the on_log callback for the MQTT client.

        :param on_log:
        :return:
        """
        self.__client.on_log = on_log

    def set_on_message_callback(self, sub, on_message_callback):
        """
        Set the on_message callback for a specific topic.
        :param sub:
        :param on_message_callback:
        :return:
        """
        self.__client.message_callback_add(sub, on_message_callback)

    def start(self):
        """
        Start the MQTT client in a separate thread. This is required for the client to be able to receive messages.

        :return:
        """
        self.__client.loop_start()

    def stop(self):
        """
        Stop the MQTT client.\

        :return:
        """
        self.__client.loop_stop()

    def __toggle_is_connected(self):
        self.__is_connected = not self.__is_connected

    def is_connected(self):
        return self.__is_connected

    @staticmethod
    def publish_single(self, topic, msg):
        self.__client.single(topic, msg, 0)

    @staticmethod
    def publish_multiple(self, topic, msgs):
        self.__client.multiple(msgs, )

    def tls_set(self):
        self.__client.tls_set()
