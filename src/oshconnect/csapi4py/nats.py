#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""NATS.io transport for the CS API Part 3 Pub/Sub binding.

:class:`NatsCommClient` is a drop-in twin of
:class:`oshconnect.csapi4py.mqtt.MQTTCommClient`: it exposes the same
synchronous ``connect``/``start``/``subscribe``/``publish``/``stop``
interface (including the paho-style ``msg_callback(client, userdata, msg)``
signature, where ``msg.topic`` and ``msg.payload`` are read) so that
:class:`~oshconnect.resources.base.StreamableResource` can drive either
transport without change.

Internally the client owns an :mod:`asyncio` event loop running on a
background thread; the ``nats-py`` client is asyncio-native, so each
synchronous method marshals its work onto that loop via
:func:`asyncio.run_coroutine_threadsafe`. Callbacks fire on the loop
thread — the same "not the main thread" contract paho already imposes.

**Subject conventions.** The OSH NATS binding (``sensorhub-service-consys-nats``)
maps a subject to a REST resource path by stripping the ``{nodeId}`` prefix
and the ``:data`` suffix and replacing ``.`` with ``/`` (see
``ConSysApiNatsConnector.getResourceUri``). Data subjects are therefore the
canonical *nested-under-systems* resource path, dot-delimited, e.g.::

    api.systems.{sysId}.datastreams.{dsId}.observations:data.swe-proto

This differs from the client's flat MQTT topics — the nesting (and the
parent system id it requires) is built at the resource layer in
:meth:`StreamableResource.get_nats_subject`. This module only handles the
delimiter mapping (:func:`nats_subject_from_topic`) as a safety net for
callers that hand it an MQTT-style topic string.

Only PROACTIVE-mode plain publish/subscribe is implemented (matching the
server's default ``dataStreamingMode``); the optional ``_control.*``
request/reply flow-control channel and JetStream durable consumers are out
of scope for this transport twin.
"""
from __future__ import annotations

import asyncio
import logging
import threading

from .mqtt import MQTT_TOPIC_FORMAT_TOKENS

try:
    import nats as _nats
except ImportError:  # pragma: no cover - exercised only without the extra
    _nats = None

logger = logging.getLogger(__name__)

# Reverse of ``MQTT_TOPIC_FORMAT_TOKENS``: ``:data.<token>`` subtopic → MIME.
_TOKEN_TO_CONTENT_TYPE = {token: ct for ct, token in MQTT_TOPIC_FORMAT_TOKENS.items()}


def nats_content_type_from_subject(subject: str) -> str | None:
    """Return the MIME content-type named by a data subject's format subtopic.

    Parses the trailing ``:data.<token>`` of a NATS data subject and maps the
    token back to its MIME type (e.g. ``…observations:data.swe-flatbuffers`` →
    ``application/swe+flatbuffers``). Returns ``None`` for a bare ``:data``
    subject (server-default format), an unknown token, or a subject with no
    ``:data`` suffix. Used to decode messages received via a format-wildcard
    subscription, where the concrete format is only known per-message.
    """
    idx = subject.rfind(':data')
    if idx == -1:
        return None
    tail = subject[idx + len(':data'):]
    if not tail.startswith('.'):
        return None
    token = tail[1:]
    return _TOKEN_TO_CONTENT_TYPE.get(token)


def nats_subject_from_topic(topic: str) -> str:
    """Translate an MQTT-style topic into a NATS subject.

    NATS uses ``.`` as the token separator, ``*`` as the single-token
    wildcard and ``>`` as the multi-token (tail) wildcard, where MQTT uses
    ``/``, ``+`` and ``#`` respectively. A subject that is already in NATS
    form (no ``/``, ``+`` or ``#``) passes through unchanged, so this is
    safe to call on subjects built directly by
    :meth:`StreamableResource.get_nats_subject`.
    """
    return topic.replace('/', '.').replace('+', '*').replace('#', '>')


class _NatsMsgShim:
    """Adapt a ``nats.aio.msg.Msg`` to the paho-style object the resource
    callbacks expect.

    Exposes ``topic`` (the NATS subject, left dot-delimited so it compares
    equal to the dot-delimited ``self._topic`` the resource subscribed
    with) and ``payload`` (raw ``bytes``). ``headers`` carries the NATS
    message headers (e.g. ``CS-Origin``) for callers that need them.
    """

    __slots__ = ("topic", "payload", "headers")

    def __init__(self, subject: str, payload: bytes, headers=None):
        self.topic = subject
        self.payload = payload
        self.headers = headers


class NatsCommClient:
    """Synchronous, thread-backed NATS client mirroring ``MQTTCommClient``.

    :param url: NATS server host (or full ``nats://host:port`` URL).
    :param port: server port, default ``4222`` (ignored when ``url`` already
        carries a scheme/port).
    :param username: optional user for user/password auth.
    :param password: optional password for user/password auth.
    :param token: optional auth token (takes precedence over user/password,
        matching the Java ``ConSysApiNatsService`` precedence).
    :param client_id_suffix: appended to the connection name for traceability.
    :param connect_timeout: seconds to wait for the initial connect.
    :param max_reconnects: reconnect attempts (``-1`` = unlimited, matching
        the server default).
    :raises RuntimeError: if the ``nats-py`` package is not importable.
    """

    def __init__(self, url, port=4222, username=None, password=None, token=None,
                 client_id_suffix="", connect_timeout=5, max_reconnects=-1):
        if _nats is None:
            raise RuntimeError(
                "The NATS transport requires the 'nats-py' package. "
                "Install it with `pip install oshconnect[nats]` "
                "(or `oshconnect[streaming]` for MQTT + NATS)."
            )
        self.__url = url if "://" in str(url) else f"nats://{url}:{port}"
        self.__port = port
        self.__username = username
        self.__password = password
        self.__token = token
        self.__name = f"oscapy_nats-{client_id_suffix}"
        self.__connect_timeout = connect_timeout
        self.__max_reconnects = max_reconnects

        self.__nc = None
        self.__subs = {}
        self.__is_connected = False
        self.__closing = False

        # asyncio loop pinned to a dedicated background thread. nats-py binds
        # to the running loop, so every coroutine must be scheduled onto it.
        self.__loop = asyncio.new_event_loop()
        self.__thread = threading.Thread(
            target=self.__run_loop, name=self.__name, daemon=True)
        self.__thread.start()

        # Callback slots kept for MQTTCommClient interface parity. NATS has no
        # broker-side subscribe/publish acknowledgement callbacks, so most are
        # stored-and-ignored; on_connect/on_disconnect are invoked on the
        # corresponding lifecycle transitions.
        self.__on_connect = None
        self.__on_disconnect = None
        self.__on_subscribe = None
        self.__on_unsubscribe = None
        self.__on_publish = None
        self.__on_message = None
        self.__on_log = None

    # -- event-loop plumbing --------------------------------------------------

    def __run_loop(self):
        asyncio.set_event_loop(self.__loop)
        self.__loop.run_forever()

    def __call_sync(self, coro, timeout=None):
        """Schedule *coro* on the loop thread and block for its result."""
        future = asyncio.run_coroutine_threadsafe(coro, self.__loop)
        return future.result(timeout=timeout)

    # -- lifecycle ------------------------------------------------------------

    def connect(self, keepalive=60):
        """Open the connection to the NATS server (blocks until connected).

        ``keepalive`` is accepted for MQTTCommClient signature parity and is
        unused — nats-py manages ping/pong keepalive internally.
        """
        logger.info('NATS connecting to %s', self.__url)
        try:
            self.__call_sync(self.__connect(), timeout=self.__connect_timeout + 2)
        except Exception as exc:
            logger.error('NATS connect failed: %s', exc)
            raise

    async def __connect(self):
        opts = dict(
            servers=[self.__url],
            name=self.__name,
            connect_timeout=self.__connect_timeout,
            max_reconnect_attempts=self.__max_reconnects,
            allow_reconnect=self.__max_reconnects != 0,
            error_cb=self.__error_cb,
            disconnected_cb=self.__disconnected_cb,
            reconnected_cb=self.__reconnected_cb,
        )
        if self.__token:
            opts["token"] = self.__token
        elif self.__username is not None:
            opts["user"] = self.__username
            opts["password"] = self.__password if self.__password is not None else ""
        self.__nc = await _nats.connect(**opts)
        self.__is_connected = True
        logger.info('NATS connected to %s', self.__url)
        if self.__on_connect is not None:
            self.__on_connect(self, None, None, 0, None)

    async def __error_cb(self, exc):
        logger.error('NATS error: %s', exc)

    async def __disconnected_cb(self):
        self.__is_connected = False
        if self.__closing:
            logger.info('NATS disconnected from %s (shutting down)', self.__url)
            return
        logger.warning('NATS disconnected from %s — will attempt reconnect', self.__url)
        if self.__on_disconnect is not None:
            self.__on_disconnect(self, None, None, 1, None)

    async def __reconnected_cb(self):
        self.__is_connected = True
        logger.info('NATS reconnected to %s', self.__url)

    def start(self):
        """No-op for interface parity.

        The background event loop is already running (started in
        ``__init__``) and I/O is serviced by nats-py on that loop as soon as
        :meth:`connect` returns; there is no separate network loop to start
        as there is with paho's ``loop_start``.
        """
        return

    def stop(self):
        """Drain, close the connection, and stop the background loop cleanly."""
        self.__closing = True
        try:
            if self.__nc is not None:
                self.__call_sync(self.__close(), timeout=self.__connect_timeout + 5)
        except Exception as exc:
            logger.debug('NATS close error (ignored): %s', exc)
        finally:
            self.__loop.call_soon_threadsafe(self.__loop.stop)
            self.__thread.join(timeout=5)
            self.__is_connected = False

    async def __close(self):
        """Tear the connection down without orphaning nats-py's loop tasks.

        Unsubscribing first stops inbound delivery so the subsequent ``drain``
        has nothing left to flush and returns promptly (a wildcard subscription
        under load could otherwise outrun a bare ``drain`` and time out). If the
        bounded drain still fails, ``close`` is forced so nats-py's internal
        ``_read_loop``/``_ping_interval``/``_flusher`` tasks are cancelled —
        otherwise stopping our event loop leaves them pending and CPython prints
        a flurry of "Task was destroyed but it is pending" warnings on teardown.
        """
        try:
            for subject, sub in list(self.__subs.items()):
                try:
                    await sub.unsubscribe()
                except Exception as exc:  # pragma: no cover - best-effort cleanup
                    logger.debug('NATS unsubscribe error on %s (ignored): %s', subject, exc)
            self.__subs.clear()
            try:
                await asyncio.wait_for(self.__nc.drain(), timeout=5)
            except Exception as exc:
                logger.debug('NATS drain failed, forcing close: %s', exc)
                try:
                    await self.__nc.close()
                except Exception:  # pragma: no cover - best-effort cleanup
                    pass
        finally:
            self.__is_connected = False

    def disconnect(self):
        """Close the NATS connection but leave the background loop running."""
        self.__closing = True
        if self.__nc is not None:
            try:
                self.__call_sync(self.__close(), timeout=self.__connect_timeout + 5)
            except Exception as exc:
                logger.debug('NATS disconnect error (ignored): %s', exc)

    # -- pub/sub --------------------------------------------------------------

    def subscribe(self, topic, qos=0, msg_callback=None):
        """Subscribe to *topic* (an MQTT-style or native NATS subject).

        :param topic: subject to subscribe to; MQTT-style separators/wildcards
            are translated via :func:`nats_subject_from_topic`.
        :param qos: accepted for MQTTCommClient parity and ignored (NATS core
            has no QoS levels).
        :param msg_callback: ``callback(client, userdata, msg)`` invoked on the
            loop thread for each message, where ``msg`` exposes ``topic`` and
            ``payload``. Without it, messages are received but dropped.

        .. warning::
            The callback runs on this client's event-loop thread. Do **not**
            call :meth:`publish` (or any other blocking method here) from
            inside it — each of those blocks on ``run_coroutine_threadsafe``
            waiting for the same loop, which would deadlock. Hand work off to
            another thread / the application loop instead (the built-in
            resource callbacks already do this via the inbound deque).
        """
        subject = nats_subject_from_topic(topic)
        if not self.__is_connected:
            logger.warning('NATS subscribe called on %s while not connected', subject)
        self.__call_sync(self.__subscribe(subject, msg_callback), timeout=5)
        logger.debug('NATS subscribed to subject: %s', subject)

    async def __subscribe(self, subject, msg_callback):
        async def _handler(msg):
            shim = _NatsMsgShim(msg.subject, msg.data, getattr(msg, 'headers', None))
            try:
                if msg_callback is not None:
                    msg_callback(self, None, shim)
                if self.__on_message is not None:
                    self.__on_message(self, None, shim)
            except Exception:
                logger.exception('NATS message callback error on %s', msg.subject)

        sub = await self.__nc.subscribe(subject, cb=_handler)
        self.__subs[subject] = sub

    def publish(self, topic, payload=None, qos=0, retain=False):
        """Publish *payload* to *topic*.

        :param payload: ``bytes`` (or ``str``, encoded UTF-8); ``None`` sends
            an empty message.
        :param qos: accepted for parity and ignored.
        :param retain: accepted for parity and ignored (NATS core has no
            retained messages).
        """
        subject = nats_subject_from_topic(topic)
        if not self.__is_connected:
            logger.warning('NATS publish called on %s while not connected — message may be lost', subject)
        data = self.__coerce_payload(payload)
        try:
            self.__call_sync(self.__nc.publish(subject, data), timeout=5)
        except Exception as exc:
            logger.error('NATS publish error on %s: %s', subject, exc)

    @staticmethod
    def __coerce_payload(payload):
        if payload is None:
            return b''
        if isinstance(payload, str):
            return payload.encode('utf-8')
        return bytes(payload)

    def unsubscribe(self, topic):
        subject = nats_subject_from_topic(topic)
        sub = self.__subs.pop(subject, None)
        if sub is not None:
            try:
                self.__call_sync(sub.unsubscribe(), timeout=5)
            except Exception as exc:
                logger.debug('NATS unsubscribe error on %s (ignored): %s', subject, exc)
        logger.debug('NATS unsubscribed from subject: %s', subject)

    def is_connected(self):
        return self.__is_connected and self.__nc is not None and self.__nc.is_connected

    # -- callback setters (MQTTCommClient parity) -----------------------------

    def set_on_connect(self, on_connect):
        """Set the connect callback ``fn(client, userdata, flags, rc, props)``."""
        self.__on_connect = on_connect

    def set_on_disconnect(self, on_disconnect):
        """Set the disconnect callback ``fn(client, userdata, flag, rc, props)``."""
        self.__on_disconnect = on_disconnect

    def set_on_subscribe(self, on_subscribe):
        """Stored for parity; NATS has no subscribe-acknowledged callback."""
        self.__on_subscribe = on_subscribe

    def set_on_unsubscribe(self, on_unsubscribe):
        """Stored for parity; NATS has no unsubscribe-acknowledged callback."""
        self.__on_unsubscribe = on_unsubscribe

    def set_on_publish(self, on_publish):
        """Stored for parity; NATS core publishes are fire-and-forget."""
        self.__on_publish = on_publish

    def set_on_message(self, on_message):
        """Set a catch-all message callback invoked in addition to per-subject ones."""
        self.__on_message = on_message

    def set_on_log(self, on_log):
        """Stored for parity; nats-py logs via the standard logging module."""
        self.__on_log = on_log

    def set_on_message_callback(self, sub, on_message_callback):
        """Subscribe *sub* with a dedicated per-subject callback."""
        self.subscribe(sub, msg_callback=on_message_callback)
