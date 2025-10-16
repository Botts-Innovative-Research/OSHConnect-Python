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

from .con_sys_api import ConnectedSystemAPIRequest
from .constants import APIResourceTypes, ContentTypes, APITerms


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
    username: str = None
    password: str = None
    user_auth: bool = False

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
            url = f'{self.server_url}/{self.api_root}/{url_endpoint}'
        api_request = ConnectedSystemAPIRequest(url=url, request_method='POST', auth=self.get_helper_auth(),
                                                body=json_data, headers=req_headers)
        return api_request.make_request()

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
            url = f'{self.server_url}/{self.api_root}/{url_endpoint}'
        api_request = ConnectedSystemAPIRequest(url=url, request_method='GET', auth=self.get_helper_auth(),
                                                headers=req_headers)
        return api_request.make_request()

    def get_resource(self, resource_type: APIResourceTypes, resource_id: str = None,
                     subresource_type: APIResourceTypes = None,
                     req_headers: dict = None):

        """
        Helper to get resources by type, specifically by id, and optionally a sub-resource collection of a specified resource.
        :param resource_type:
        :param resource_id:
        :param subresource_type:
        :param req_headers:
        :return:
        """
        if req_headers is None:
            req_headers = {}
        base_api_url = self.get_api_root_url()
        resource_type_str = resource_type_to_endpoint(resource_type)
        res_id_str = f'/{resource_id}' if resource_id else ""
        sub_res_type_str = f'/{resource_type_to_endpoint(subresource_type)}' if subresource_type else ""
        complete_url = f'{base_api_url}/{resource_type_str}{res_id_str}{sub_res_type_str}'
        api_request = ConnectedSystemAPIRequest(url=complete_url, request_method='GET', auth=self.get_helper_auth(),
                                                headers=req_headers)
        return api_request.make_request()

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
            url = self.resource_url_resolver(res_type, None, parent_res_id, from_collection)
        else:
            url = f'{self.server_url}/{self.api_root}/{url_endpoint}'
        api_request = ConnectedSystemAPIRequest(url=url, request_method='PUT', auth=self.get_helper_auth(),
                                                body=json_data, headers=req_headers)
        return api_request.make_request()

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
            url = self.resource_url_resolver(res_type, None, parent_res_id, from_collection)
        else:
            url = f'{self.server_url}/{self.api_root}/{url_endpoint}'
        api_request = ConnectedSystemAPIRequest(url=url, request_method='DELETE', auth=self.get_helper_auth(),
                                                headers=req_headers)
        return api_request.make_request()

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
    def get_mqtt_topic(self, resource_type, subresource_type, resource_id: str, subresource_id: str = None):
        """
        Returns the MQTT topic for the resource type, does not check for validity of the resource type combination
        :param resource_type : The API resource type of the resource that comes first in the URL, cannot be None
        :param subresource_type: The API resource type of the sub-resource that comes second in the URL, optional if there
        is no sub-resource.
        :param resource_id: The ID of the primary resource, can be none if the request is being made for all resources of
        the given type.
        :param subresource_id: The ID of the sub-resource, can be none if the request is being made for all sub-resources of
        the given type.
        :return:
        """
        subresource_endpoint = f'/{resource_type_to_endpoint(subresource_type)}'
        resource_endpoint = "" if resource_type is None else f'/{resource_type_to_endpoint(resource_type)}'
        resource_ident = "" if resource_id is None else f'/{resource_id}'
        subresource_ident = "" if subresource_id is None else f'/{subresource_id}'
        topic_locator = f'/{self.api_root}{resource_endpoint}{resource_ident}{subresource_endpoint}{subresource_ident}'
        print(f'MQTT Topic: {topic_locator}')

        return topic_locator


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
