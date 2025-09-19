from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def list_system_events(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Lists all system events
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEM_EVENTS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_events_by_system_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                             headers: dict = None):
    """
    Lists all events of a system
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.EVENTS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_new_system_events(server_addr: HttpUrl, system_id: str, request_body: dict,
                          api_root: str = APITerms.API.value, headers: dict = None):
    """
    Adds a new system event to a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.EVENTS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str,
                                api_root: str = APITerms.API.value, headers: dict = None):
    """
    Retrieves a system event by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.EVENTS.value)
                   .with_secondary_resource_id(event_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str, request_body: dict,
                              api_root: str = APITerms.API.value, headers: dict = None):
    """
    Updates a system event by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.EVENTS.value)

                   .with_secondary_resource_id(event_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str, api_root: str = APITerms.API.value,
                              headers: dict = None):
    """
    Deletes a system event by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.EVENTS.value)
                   .with_secondary_resource_id(event_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()
