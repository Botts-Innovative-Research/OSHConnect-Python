#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from pydantic import BaseModel, Field

from .con_sys_api import DeleteRequest, GetRequest, PostRequest, PutRequest
from .constants import APIResourceTypes, ContentTypes, APITerms
from .mqtt import MQTT_TOPIC_SCHEMES, TOPIC_SCHEME_LEGACY, TOPIC_SCHEME_PART3, mqtt_topic_format_token
from ..exceptions import ConfigurationError


# TODO: rework to make the first resource in the endpoint the primary key for URL construction, currently, the implementation is a bit on the confusing side with what is being generated and why.

def determine_parent_type(res_type: APIResourceTypes):
    match res_type:
        case APIResourceTypes.SYSTEM:
            return APIResourceTypes.SYSTEM
        case APIResourceTypes.COLLECTION:
            return None
        case APIResourceTypes.CONTROL_CHANNEL:
            return APIResourceTypes.SYSTEM
        case APIResourceTypes.COMMAND:
            return APIResourceTypes.CONTROL_CHANNEL
        case APIResourceTypes.DATASTREAM:
            return APIResourceTypes.SYSTEM
        case APIResourceTypes.OBSERVATION:
            return APIResourceTypes.DATASTREAM
        case APIResourceTypes.SYSTEM_EVENT:
            return APIResourceTypes.SYSTEM
        case APIResourceTypes.SAMPLING_FEATURE:
            return APIResourceTypes.SYSTEM
        case APIResourceTypes.PROCEDURE:
            return None
        case APIResourceTypes.PROPERTY:
            return None
        case APIResourceTypes.SYSTEM_HISTORY:
            return None
        case APIResourceTypes.DEPLOYMENT:
            return None
        case _:
            return None


def resource_type_to_endpoint(res_type: APIResourceTypes, parent_type: APIResourceTypes = None):
    if parent_type is APIResourceTypes.COLLECTION:
        return APITerms.ITEMS.value

    match res_type:
        case APIResourceTypes.SYSTEM:
            return APITerms.SYSTEMS.value
        case APIResourceTypes.COLLECTION:
            return APITerms.COLLECTIONS.value
        case APIResourceTypes.CONTROL_CHANNEL:
            return APITerms.CONTROL_STREAMS.value
        case APIResourceTypes.COMMAND:
            return APITerms.COMMANDS.value
        case APIResourceTypes.DATASTREAM:
            return APITerms.DATASTREAMS.value
        case APIResourceTypes.OBSERVATION:
            return APITerms.OBSERVATIONS.value
        case APIResourceTypes.SYSTEM_EVENT:
            return APITerms.SYSTEM_EVENTS.value
        case APIResourceTypes.SAMPLING_FEATURE:
            return APITerms.SAMPLING_FEATURES.value
        case APIResourceTypes.PROCEDURE:
            return APITerms.PROCEDURES.value
        case APIResourceTypes.PROPERTY:
            return APITerms.PROPERTIES.value
        case APIResourceTypes.SYSTEM_HISTORY:
            return APITerms.HISTORY.value
        case APIResourceTypes.DEPLOYMENT:
            return APITerms.DEPLOYMENTS.value
        case APIResourceTypes.STATUS:
            return APITerms.STATUS.value
        case APIResourceTypes.SCHEMA:
            return APITerms.SCHEMA.value
        case _:
            raise ValueError('Invalid resource type')


@dataclass
class APIHelper(ABC):
    server_url: str = None
    port: int = None
    protocol: str = "https"
    server_root: str = "sensorhub"
    api_root: str = "api"
    mqtt_topic_root: str = None
    username: str = None
    password: str = None
    user_auth: bool = False
    # When True, build the pre-Part-3 ("legacy") MQTT topic form: a leading
    # slash, no ``:data`` suffix, and no format subtopic — for talking to
    # older OSH servers that predate the CS API Part 3 topic scheme.
    # Kept for backwards compatibility; equivalent to ``topic_scheme="legacy"``.
    legacy_topics: bool = False
    # One of ``MQTT_TOPIC_SCHEMES``. ``None`` means ``"legacy"`` when
    # ``legacy_topics`` is set, else ``"part3"``.
    topic_scheme: str = None

    def __post_init__(self):
        self.get_topic_scheme()

    def get_topic_scheme(self) -> str:
        """Return the MQTT topic scheme in effect: ``"part3"`` (default),
        ``"part3-flat"`` or ``"legacy"``.

        :raises ConfigurationError: if ``topic_scheme`` is not a known scheme,
            or if it contradicts ``legacy_topics=True``.
        """
        if self.topic_scheme is None:
            return TOPIC_SCHEME_LEGACY if self.legacy_topics else TOPIC_SCHEME_PART3
        if self.topic_scheme not in MQTT_TOPIC_SCHEMES:
            raise ConfigurationError(
                f"Unknown MQTT topic scheme {self.topic_scheme!r}; expected one of {MQTT_TOPIC_SCHEMES}.")
        if self.legacy_topics and self.topic_scheme != TOPIC_SCHEME_LEGACY:
            raise ConfigurationError(
                f"legacy_topics=True conflicts with topic_scheme={self.topic_scheme!r}; "
                f"set only one of them.")
        return self.topic_scheme

    def get_mqtt_root(self) -> str:
        """
        Returns the root path segment used when building MQTT topic strings.
        Defaults to ``api_root`` when ``mqtt_topic_root`` has not been set explicitly,
        so existing callers that only configure ``api_root`` are unaffected.
        """
        return self.mqtt_topic_root if self.mqtt_topic_root is not None else self.api_root

    def create_resource(self, res_type: APIResourceTypes, json_data: any, parent_res_id: str = None,
                        from_collection: bool = False, url_endpoint: str = None, req_headers: dict = None):
        """
        Creates a resource of the given type with the given data, will attempt to create a sub-resource if parent_res_id
        is provided.
        :param req_headers:
        :param res_type:
        :param json_data:
        :param parent_res_id:
        :param from_collection:
        :param url_endpoint: If given, will override the default URL construction. Should contain the endpoint past the API root.
        :return:
        """

        if url_endpoint is None:
            url = self.resource_url_resolver(res_type, None, parent_res_id, from_collection)
        else:
            url = f'{self.get_api_root_url()}/{url_endpoint}'
        return PostRequest(url=url, body=json_data, headers=req_headers,
                           auth=self.get_helper_auth()).execute()

    def retrieve_resource(self, res_type: APIResourceTypes, res_id: str = None, parent_res_id: str = None,
                          from_collection: bool = False,
                          collection_id: str = None, url_endpoint: str = None, req_headers: dict = None):
        """
        Retrieves a resource or list of resources if no res_id is provided, will attempt to retrieve a sub-resource if
        parent_res_id is provided.
        :param req_headers:
        :param res_type:
        :param res_id:
        :param parent_res_id:
        :param from_collection:
        :param collection_id:
        :param url_endpoint: If given, will override the default URL construction. Should contain the endpoint past the API root.
        :return:
        """
        if url_endpoint is None:
            url = self.resource_url_resolver(res_type, res_id, parent_res_id, from_collection)
        else:
            url = f'{self.get_api_root_url()}/{url_endpoint}'
        return GetRequest(url=url, headers=req_headers,
                          auth=self.get_helper_auth()).execute()

    def get_resource(self, resource_type: APIResourceTypes, resource_id: str = None,
                     subresource_type: APIResourceTypes = None,
                     req_headers: dict = None,
                     params: dict = None):

        """
        Helper to get resources by type, specifically by id, and optionally a sub-resource collection of a specified resource.
        :param resource_type:
        :param resource_id:
        :param subresource_type:
        :param req_headers:
        :param params: Optional query-string parameters (e.g., ``{"obsFormat": "logical"}`` for schema variants).
        :return:
        """
        if req_headers is None:
            req_headers = {}
        base_api_url = self.get_api_root_url()
        resource_type_str = resource_type_to_endpoint(resource_type)
        res_id_str = f'/{resource_id}' if resource_id else ""
        sub_res_type_str = f'/{resource_type_to_endpoint(subresource_type)}' if subresource_type else ""
        complete_url = f'{base_api_url}/{resource_type_str}{res_id_str}{sub_res_type_str}'
        return GetRequest(url=complete_url, params=params, headers=req_headers,
                          auth=self.get_helper_auth()).execute()

    def update_resource(self, res_type: APIResourceTypes, res_id: str, json_data: any, parent_res_id: str = None,
                        from_collection: bool = False, url_endpoint: str = None, req_headers: dict = None):
        """
        Updates a resource of the given type by its id, if necessary, will attempt to update a sub-resource if
        parent_res_id is provided.
        :param req_headers:
        :param res_type:
        :param res_id:
        :param json_data:
        :param parent_res_id:
        :param from_collection:
        :param url_endpoint: If given, will override the default URL construction. Should contain the endpoint past the API root.
        :return:
        """
        if url_endpoint is None:
            url = self.resource_url_resolver(res_type, res_id, parent_res_id, from_collection)
        else:
            url = f'{self.get_api_root_url()}/{url_endpoint}'
        return PutRequest(url=url, body=json_data, headers=req_headers,
                          auth=self.get_helper_auth()).execute()

    def delete_resource(self, res_type: APIResourceTypes, res_id: str, parent_res_id: str = None,
                        from_collection: bool = False, url_endpoint: str = None, req_headers: dict = None):
        """
        Deletes a resource of the given type by its id, if necessary, will attempt to delete a sub-resource if
        parent_res_id is provided.
        :param req_headers:
        :param res_type:
        :param res_id:
        :param parent_res_id:
        :param from_collection:
        :param url_endpoint: If given, will override the default URL construction. Should contain the endpoint past the API root.
        :return:
        """
        if url_endpoint is None:
            url = self.resource_url_resolver(res_type, res_id, parent_res_id, from_collection)
        else:
            url = f'{self.get_api_root_url()}/{url_endpoint}'
        return DeleteRequest(url=url, headers=req_headers,
                             auth=self.get_helper_auth()).execute()

    # Helpers
    def resource_url_resolver(self, subresource_type: APIResourceTypes, subresource_id: str = None,
                              resource_id: str = None,
                              from_collection: bool = False):
        """
        Helper to generate a URL endpoint for a given resource type and id by matching the resource type to an
        appropriate parent endpoint and inserting the resource ids as necessary.
        :param subresource_type:
        :param subresource_id:
        :param resource_id:
        :param from_collection:
        :return:
        """
        if subresource_type is None:
            raise ValueError('Resource type must contain a valid APIResourceType')
        if subresource_type is APIResourceTypes.COLLECTION and from_collection:
            raise ValueError('Collections are not sub-resources of other collections')

        parent_type = None
        if resource_id and not from_collection:
            parent_type = determine_parent_type(subresource_type)
        elif resource_id and from_collection:
            parent_type = APIResourceTypes.COLLECTION

        return self.construct_url(parent_type, subresource_id, subresource_type, resource_id)

    def construct_url(self, resource_type: APIResourceTypes, subresource_id, subresource_type, resource_id,
                      for_socket: bool = False):
        """
        Constructs an API endpoint url from the given parameters
        :param resource_type:
        :param subresource_id:
        :param subresource_type:
        :param resource_id:
        :param for_socket: If true, will construct a WebSocket URL (ws:// or wss://) instead of HTTP/HTTPS.
        :return:
        """
        # TODO: Test for less common cases to ensure that the URL is being constructed correctly
        base_url = self.get_api_root_url(socket=for_socket)

        resource_endpoint = resource_type_to_endpoint(subresource_type, resource_type)
        url = f'{base_url}/{resource_endpoint}'

        if resource_type:
            parent_endpoint = resource_type_to_endpoint(resource_type)
            url = f'{base_url}/{parent_endpoint}/{resource_id}/{resource_endpoint}'

        if subresource_id:
            url = f'{url}/{subresource_id}'

        return url

    def get_helper_auth(self):
        if self.user_auth:
            return self.username, self.password
        return None

    def get_base_url(self, socket: bool = False):
        if socket:
            protocol = 'ws' if self.protocol == 'http' else 'wss'
            return f'{protocol}://{self.server_url}{f":{self.port}" if self.port else ""}'
        return f'{self.protocol}://{self.server_url}{f":{self.port}" if self.port else ""}'

    def get_api_root_url(self, socket: bool = False):
        """
        Returns the full API root URL including protocol, server address, port (if applicable), and API root path.
        :param socket: If true, will return a WebSocket URL (ws:// or wss://) instead of HTTP/HTTPS.
        :return:
        """
        return f'{self.get_base_url(socket=socket)}/{self.server_root}/{self.api_root}'

    def set_protocol(self, protocol: str):
        if protocol not in ['http', 'https', 'ws', 'wss']:
            raise ValueError('Protocol must be either "http" or "https"')
        self.protocol = protocol

    # TODO: add validity checking for resource type combinations
    def get_mqtt_topic(self, resource_type, subresource_type, resource_id: str, subresource_id: str = None,
                       data_topic: bool = True, format: str | None = None, legacy: bool | None = None,
                       system_path: str | list[str] | None = None):
        """
        Returns the MQTT topic for the resource type, does not check for validity of the resource type combination.

        With the default ``"part3"`` scheme, datastream and control stream topics are nested under the system
        path, e.g. ``api/systems/{sysId}/datastreams/{dsId}/observations:data``, and the command status topic is
        ``…/controlstreams/{csId}/commands/{cmdId}/status:data`` (``subresource_id`` is the command id).
        With ``"part3-flat"`` the system path is left out; with ``"legacy"`` the topic mirrors the HTTP path.
        :param resource_type: The API resource type of the resource that comes first in the URL, cannot be None
        :param subresource_type: The API resource type of the sub-resource that comes second in the URL, optional if there
        is no sub-resource.
        :param resource_id: The ID of the primary resource, can be none if the request is being made for all resources of
        the given type.
        :param subresource_id: The ID of the sub-resource, can be none if the request is being made for all sub-resources of
        the given type.
        :param data_topic: If True (default), appends ':data' to the subresource collection endpoint per CS API Part 3
        spec for Resource Data Topics. Set to False for Resource Event Topics (no suffix).
        :param format: Optional MIME content-type that selects the ``:data/<token>`` format subtopic per CS API Part 3
        §Resource Data Messages Content Negotiation. ``None`` (default) emits a bare ``:data`` topic so the server's
        default format applies. Ignored when ``data_topic=False``. Raises ``ValueError`` for unmapped MIME types — see
        :func:`oshconnect.csapi4py.mqtt.mqtt_topic_format_token`.
        :param legacy: Force the pre-Part-3 topic form (leading slash, no ``:data`` suffix, no format subtopic).
        ``None`` (default) uses this helper's ``legacy_topics`` setting; pass ``True``/``False`` to override per-call.
        In legacy mode ``data_topic`` and ``format`` are ignored. ``legacy=False`` on a helper whose scheme is
        ``"legacy"`` builds the ``"part3"`` form.
        :param system_path: Id of the parent system of the datastream or control stream, or the list of ids from
        the top-level system down to the owning subsystem. Required by the ``"part3"`` scheme when
        ``resource_type`` is a datastream or control stream; ignored by the other schemes.
        :raises ValueError: in the ``"part3"`` scheme, if ``system_path`` is missing for a datastream or control
        stream topic, or the command id is missing for a status topic.
        :return:
        """
        scheme = self.get_topic_scheme()
        if legacy is True:
            scheme = TOPIC_SCHEME_LEGACY
        elif legacy is False and scheme == TOPIC_SCHEME_LEGACY:
            scheme = TOPIC_SCHEME_PART3
        use_legacy = scheme == TOPIC_SCHEME_LEGACY
        if scheme == TOPIC_SCHEME_PART3:
            return self._part3_mqtt_topic(resource_type, subresource_type, resource_id, subresource_id,
                                          data_topic, format, system_path)
        subresource_endpoint = f'/{resource_type_to_endpoint(subresource_type)}'
        resource_endpoint = "" if resource_type is None else f'/{resource_type_to_endpoint(resource_type)}'
        resource_ident = "" if resource_id is None else f'/{resource_id}'
        subresource_ident = "" if subresource_id is None else f'/{subresource_id}'
        if use_legacy:
            # Pre-Part-3 form: leading slash, no ``:data``/format subtopic.
            return f'/{self.get_mqtt_root()}{resource_endpoint}{resource_ident}{subresource_endpoint}{subresource_ident}'
        data_suffix = ':data' if data_topic else ''
        if data_topic and format is not None:
            data_suffix = f'{data_suffix}/{mqtt_topic_format_token(format)}'
        topic_locator = f'{self.get_mqtt_root()}{resource_endpoint}{resource_ident}{subresource_endpoint}{data_suffix}{subresource_ident}'
        return topic_locator

    def _part3_mqtt_topic(self, resource_type, subresource_type, resource_id, subresource_id,
                          data_topic, format, system_path) -> str:
        """Build a CS API Part 3 topic: ``{root}/{systemPath}/…[:data[/{token}]]``."""
        parts = [self.get_mqtt_root()]
        if resource_type in (APIResourceTypes.DATASTREAM, APIResourceTypes.CONTROL_CHANNEL):
            path = [system_path] if isinstance(system_path, str) else list(system_path or [])
            if not path or not all(path):
                raise ValueError(
                    f"Cannot build the MQTT topic for {resource_type.value} {resource_id!r}: the parent system id "
                    f"is unknown. Part 3 topics are nested under 'systems/{{id}}'. Discover the resource through "
                    f"its System, or call set_parent_resource_id().")
            parts += [resource_type_to_endpoint(APIResourceTypes.SYSTEM), path[0]]
            for subsystem_id in path[1:]:
                parts += ['subsystems', subsystem_id]
        if resource_type is not None:
            parts.append(resource_type_to_endpoint(resource_type))
            if resource_id is not None:
                parts.append(resource_id)
        if resource_type is APIResourceTypes.CONTROL_CHANNEL and subresource_type is APIResourceTypes.STATUS:
            # Status is published per command: …/commands/{cmdId}/status
            if subresource_id is None:
                raise ValueError(
                    "Cannot build the command status topic: Part 3 status topics are per command "
                    "('…/commands/{cmdId}/status:data'). Pass the command id.")
            parts += [resource_type_to_endpoint(APIResourceTypes.COMMAND), subresource_id,
                      resource_type_to_endpoint(APIResourceTypes.STATUS)]
        else:
            parts.append(resource_type_to_endpoint(subresource_type))
            if subresource_id is not None:
                parts.append(subresource_id)
        topic = '/'.join(str(p) for p in parts)
        if data_topic:
            topic += ':data'
            if format is not None:
                topic += f'/{mqtt_topic_format_token(format)}'
        return topic


@dataclass(kw_only=True)
class ResponseParserHelper:
    default_object_reps: DefaultObjectRepresentations


class DefaultObjectRepresentations(BaseModel):
    """
    Intended to be used as a way to determine which formats should be used when serializing and deserializing objects.
    Should work in tandem with planned Serializer/Deserializer classes.
    """
    # Part 1
    collections: str = Field(ContentTypes.JSON.value)
    deployments: str = Field(ContentTypes.GEO_JSON.value)
    procedures: str = Field(ContentTypes.GEO_JSON.value)
    properties: str = Field(ContentTypes.SML_JSON.value)
    sampling_features: str = Field(ContentTypes.GEO_JSON.value)
    systems: str = Field(ContentTypes.GEO_JSON.value)
    # Part 2
    datastreams: str = Field(ContentTypes.JSON.value)
    observations: str = Field(ContentTypes.JSON.value)
    control_channels: str = Field(ContentTypes.JSON.value)
    commands: str = Field(ContentTypes.JSON.value)
    system_events: str = Field(ContentTypes.OM_JSON.value)
    system_history: str = Field(ContentTypes.GEO_JSON.value)
    # TODO: validate schemas for each resource to amke sure they are allowed per the spec
