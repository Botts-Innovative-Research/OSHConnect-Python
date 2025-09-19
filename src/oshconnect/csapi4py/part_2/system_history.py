from pydantic import HttpUrl

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from src.oshconnect.csapi4py.constants import APITerms


def list_system_history(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Lists all history versions of a system
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_resource_type(APITerms.HISTORY.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def retrieve_system_historical_description_by_id(server_addr: HttpUrl, system_id: str, history_id: str,
                                                 api_root: str = APITerms.API.value, headers: dict = None):
    """
    Retrieves a historical system description by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_resource_type(APITerms.HISTORY.value)
                   .with_secondary_resource_id(history_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_system_historical_description(server_addr: HttpUrl, system_id: str, history_rev_id: str, request_body: dict,
                                         api_root: str = APITerms.API.value, headers: dict = None):
    """
    Updates a historical system description by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_resource_type(APITerms.HISTORY.value)
                   .with_secondary_resource_id(history_rev_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_system_historical_description_by_id(server_addr: HttpUrl, system_id: str, history_rev_id: str,
                                               api_root: str = APITerms.API.value, headers: dict = None):
    """
    Deletes a historical system description by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_resource_type(APITerms.HISTORY.value)
                   .with_secondary_resource_id(history_rev_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()
