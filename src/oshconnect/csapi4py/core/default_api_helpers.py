from __future__ import annotations

from abc import ABC
from dataclasses import dataclass

from pydantic import BaseModel, Field

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemAPIRequest
from src.oshconnect.csapi4py.constants import APIResourceTypes, EncodingSchema, APITerms


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
        case _:
            raise ValueError('Invalid resource type')


@dataclass
class APIHelper(ABC):
    server_url: str = None
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
    def resource_url_resolver(self, res_type: APIResourceTypes, res_id: str = None, parent_res_id: str = None,
                              from_collection: bool = False):
        """
        Helper to generate a URL endpoint for a given resource type and id by matching the resource type to an
        appropriate parent endpoint and inserting the resource ids as necessary.
        :param res_type:
        :param res_id:
        :param parent_res_id:
        :param from_collection:
        :return:
        """
        if res_type is None:
            raise ValueError('Resource type must contain a valid APIResourceType')
        if res_type is APIResourceTypes.COLLECTION and from_collection:
            raise ValueError('Collections are not sub-resources of other collections')

        parent_type = None
        if parent_res_id and not from_collection:
            parent_type = determine_parent_type(res_type)
        elif parent_res_id and from_collection:
            parent_type = APIResourceTypes.COLLECTION

        return self.construct_url(parent_type, res_id, res_type, parent_res_id)

    def construct_url(self, parent_type, res_id, res_type, parent_res_id):
        """
        Constructs an API endpoint url from the given parameters
        :param parent_type:
        :param res_id:
        :param res_type:
        :param parent_res_id:
        :return:
        """
        # TODO: Test for less common cases to ensure that the URL is being constructed correctly
        base_url = f'{self.server_url}/{self.api_root}'
        resource_endpoint = resource_type_to_endpoint(res_type, parent_type)
        url = f'{base_url}/{resource_endpoint}'

        if parent_type:
            parent_endpoint = resource_type_to_endpoint(parent_type)
            url = f'{base_url}/{parent_endpoint}/{parent_res_id}/{resource_endpoint}'

        if res_id:
            url = f'{url}/{res_id}'

        return url

    def get_helper_auth(self):
        if self.user_auth:
            return self.username, self.password
        return None


@dataclass(kw_only=True)
class ResponseParserHelper:
    default_object_reps: DefaultObjectRepresentations


class DefaultObjectRepresentations(BaseModel):
    """
    Intended to be used as a way to determine which formats should be used when serializing and deserializing objects.
    Should work in tandem with planned Serializer/Deserializer classes.
    """
    # Part 1
    collections: str = Field(EncodingSchema.JSON.value)
    deployments: str = Field(EncodingSchema.GEO_JSON.value)
    procedures: str = Field(EncodingSchema.GEO_JSON.value)
    properties: str = Field(EncodingSchema.SML_JSON.value)
    sampling_features: str = Field(EncodingSchema.GEO_JSON.value)
    systems: str = Field(EncodingSchema.GEO_JSON.value)
    # Part 2
    datastreams: str = Field(EncodingSchema.JSON.value)
    observations: str = Field(EncodingSchema.JSON.value)
    control_channels: str = Field(EncodingSchema.JSON.value)
    commands: str = Field(EncodingSchema.JSON.value)
    system_events: str = Field(EncodingSchema.OM_JSON.value)
    system_history: str = Field(EncodingSchema.GEO_JSON.value)
    # TODO: validate schemas for each resource to amke sure they are allowed per the spec
