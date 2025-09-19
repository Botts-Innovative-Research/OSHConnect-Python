from typing import Union

from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def list_all_control_streams(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Lists all control streams
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_control_streams_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                   headers=None):
    """
    Lists all control streams of a system
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.CONTROL_STREAMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_control_streams_to_system(server_addr: HttpUrl, system_id: str, request_body: Union[str, dict],
                                  api_root: str = APITerms.API.value, headers=None):
    """
    Adds a control stream to a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_control_stream_description_by_id(server_addr: HttpUrl, control_stream_id: str,
                                              api_root: str = APITerms.API.value, headers: dict = None):
    """
    Retrieves a control stream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_control_stream_description_by_id(server_addr: HttpUrl, control_stream_id: str,
                                            request_body: Union[str, dict],
                                            api_root: str = APITerms.API.value, headers: dict = None):
    """
    Updates a control stream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_control_stream_by_id(server_addr: HttpUrl, control_stream_id: str, api_root: str = APITerms.API.value,
                                headers=None):
    """
    Deletes a control stream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def retrieve_control_stream_schema_by_id(server_addr: HttpUrl, control_stream_id: str,
                                         api_root: str = APITerms.API.value, headers: dict = None):
    """
    Retrieves a control stream schema by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   .for_sub_resource_type(APITerms.SCHEMA.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_control_stream_schema_by_id(server_addr: HttpUrl, control_stream_id: str, request_body: Union[str, dict],
                                       api_root: str = APITerms.API.value, headers: dict = None):
    """
    Updates a control stream schema by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   # .for_sub_resource_type(APITerms.SCHEMA.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()
