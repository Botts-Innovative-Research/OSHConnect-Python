import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


class MQTTCommClient:
    def __init__(self, url, port=1883, username=None, password=None, path='mqtt', client_id_suffix="",
                 transport='tcp', use_tls=False, reconnect_delay=5):
        """
    Wraps a paho mqtt client to provide a simple interface for interacting with the mqtt server that is customized
    for this library.

    :param url: url of the mqtt server
    :param port: port the mqtt server is communicating over, default is 1883 or whichever port the main node is
    using if in websocket mode
    :param username: used if node is requiring authentication to access this service
    :param password: used if node is requiring authentication to access this service
    :param path: used for setting the path when using websockets (usually sensorhub/mqtt by default)
    :param transport: 'tcp' (default) or 'websockets'
    :param use_tls: explicitly enable TLS; when False (default), credentials are sent without TLS
    :param reconnect_delay: seconds between automatic reconnect attempts on disconnect (0 disables)
    """
        self.__url = url
        self.__port = port
        self.__path = path
        self.__client_id = f'oscapy_mqtt-{client_id_suffix}'
        self.__transport = transport
        self.__reconnect_delay = reconnect_delay

        self.__client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.__client_id,
            transport=self.__transport,
        )

        if self.__transport == 'websockets':
            self.__client.ws_set_options(path=self.__path)

        if username is not None and password is not None:
            self.__client.username_pw_set(username, password)

        if use_tls:
            self.__client.tls_set(tls_version=mqtt.ssl.PROTOCOL_TLSv1_2)

        self.__client.on_connect = self._on_connect
        self.__client.on_subscribe = self._on_subscribe
        self.__client.on_message = self._on_message
        self.__client.on_publish = self._on_publish
        self.__client.on_log = self._on_log
        self.__client.on_disconnect = self._on_disconnect

        self.__is_connected = False

    def _on_connect(self, client, userdata, flags, rc, properties):
        if rc == mqtt.MQTT_ERR_SUCCESS:
            self.__is_connected = True
            logger.info('MQTT connected to %s:%s (rc=%s)', self.__url, self.__port, rc)
        else:
            self.__is_connected = False
            logger.error('MQTT connection failed: rc=%s (%s)', rc, mqtt.error_string(rc))

    def _on_subscribe(self, client, userdata, mid, granted_qos, properties):
        logger.debug('MQTT subscribed: mid=%s granted_qos=%s', mid, granted_qos)

    def _on_message(self, client, userdata, msg):
        logger.debug('MQTT message on %s: %s bytes', msg.topic, len(msg.payload))

    def _on_publish(self, client, userdata, mid, info, properties):
        logger.debug('MQTT published: mid=%s', mid)

    def _on_log(self, client, userdata, level, buf):
        logger.debug('MQTT paho: %s', buf)

    def _on_disconnect(self, client, userdata, dc_flag, rc, properties):
        self.__is_connected = False
        if rc == mqtt.MQTT_ERR_SUCCESS:
            logger.info('MQTT disconnected cleanly from %s:%s', self.__url, self.__port)
        else:
            logger.warning('MQTT unexpected disconnect from %s:%s rc=%s (%s) — will attempt reconnect',
                           self.__url, self.__port, rc, mqtt.error_string(rc))

    def connect(self, keepalive=60):
        logger.info('MQTT connecting to %s:%s', self.__url, self.__port)
        try:
            self.__client.connect(self.__url, self.__port, keepalive=keepalive)
            if self.__reconnect_delay > 0:
                self.__client.reconnect_delay_set(min_delay=1, max_delay=self.__reconnect_delay)
        except Exception as exc:
            logger.error('MQTT connect failed: %s', exc)
            raise

    def subscribe(self, topic, qos=0, msg_callback=None):
        """
        Subscribe to a topic, and optionally set a callback for when a message is received on that topic. To actually
        retrieve any information you must set a callback.

        :param topic: MQTT topic to subscribe to (example/topic)
        :param qos: quality of service, 0, 1, or 2
        :param msg_callback: callback with the form: callback(client, userdata, msg)
        :return:
        """
        if not self.__is_connected:
            logger.warning('MQTT subscribe called on %s while not connected — message will be queued by paho', topic)
        self.__client.subscribe(topic, qos)
        if msg_callback is not None:
            self.__client.message_callback_add(topic, msg_callback)
        logger.debug('MQTT subscribed to topic: %s (qos=%s)', topic, qos)

    def publish(self, topic, payload=None, qos=0, retain=False):
        if not self.__is_connected:
            logger.warning('MQTT publish called on %s while not connected — message may be lost', topic)
        result = self.__client.publish(topic, payload, qos, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error('MQTT publish error on %s: rc=%s (%s)', topic, result.rc, mqtt.error_string(result.rc))

    def unsubscribe(self, topic):
        self.__client.unsubscribe(topic)
        logger.debug('MQTT unsubscribed from topic: %s', topic)

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
        Start the MQTT client network loop in a background thread.

        :return:
        """
        self.__client.loop_start()

    def stop(self):
        """
        Stop the MQTT client network loop and disconnect cleanly.

        :return:
        """
        self.__client.loop_stop()

    def is_connected(self):
        return self.__is_connected

    def tls_set(self):
        self.__client.tls_set()
