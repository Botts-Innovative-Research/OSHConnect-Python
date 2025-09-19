from pydantic import HttpUrl

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from src.oshconnect.csapi4py.constants import APITerms


def list_all_properties(server_addr: HttpUrl, api_root: str = APITerms.API.value):
    """
    List all properties
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .build_url_from_base()
                   .build())
    return api_request


def create_new_properties(server_addr: HttpUrl, request_body: dict, api_root: str = APITerms.API.value):
    """
    Create a new property as defined by the request body
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .build())
    return api_request


def retrieve_property_by_id(server_addr: HttpUrl, property_id: str, api_root: str = APITerms.API.value):
    """
    Retrieve a property by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .with_resource_id(property_id)
                   .build_url_from_base()
                   .build())
    return api_request


def update_property_by_id(server_addr: HttpUrl, property_id: str, request_body: dict,
                          api_root: str = APITerms.API.value):
    """
    Update a property by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .with_resource_id(property_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .build())
    return api_request


def delete_property_by_id(server_addr: HttpUrl, property_id: str, api_root: str = APITerms.API.value):
    """
    Delete a property by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .with_resource_id(property_id)
                   .build_url_from_base()
                   .build())
    return api_request
