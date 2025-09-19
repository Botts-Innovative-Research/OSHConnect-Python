from typing import Union

from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def list_all_procedures(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Lists all procedures in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROCEDURES.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def create_new_procedures(server_addr: HttpUrl, request_body: Union[str, dict], api_root: str = APITerms.API.value,
                          headers: dict = None):
    """
    Create a new procedure as defined by the request body
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROCEDURES.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())
    print(api_request)
    return api_request.make_request()


def retrieve_procedure_by_id(server_addr: HttpUrl, procedure_id: str, api_root: str = APITerms.API.value,
                             headers: dict = None):
    """
    Retrieve a procedure by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROCEDURES.value)
                   .with_resource_id(procedure_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_procedure_by_id(server_addr: HttpUrl, procedure_id: str, request_body: Union[str, dict],
                           api_root: str = APITerms.API.value, headers: dict = None):
    """
    Update a procedure by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROCEDURES.value)
                   .with_resource_id(procedure_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_procedure_by_id(server_addr: HttpUrl, procedure_id: str, api_root: str = APITerms.API.value,
                           headers: dict = None):
    """
    Delete a procedure by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROCEDURES.value)
                   .with_resource_id(procedure_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()
