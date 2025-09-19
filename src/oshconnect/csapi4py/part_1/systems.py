from typing import Union

import requests
from pydantic import HttpUrl

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from src.oshconnect.csapi4py.constants import APITerms
from src.oshconnect.csapi4py.request_wrappers import post_request


def list_all_systems(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Lists all systems in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def create_new_systems(server_addr: HttpUrl, request_body: Union[str, dict], api_root: str = APITerms.API.value,
                       uname: str = None,
                       pword: str = None, headers: dict = None):
    """
    Create a new system as defined by the request body
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_auth(uname, pword)
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())
    print(api_request.url)
    # resp = requests.post(api_request.url, data=api_request.body, headers=api_request.headers, auth=(uname, pword))
    resp = post_request(api_request.url, api_request.body, api_request.headers, api_request.auth)
    print(f'Create new system response: {resp}')
    return resp


def list_all_systems_in_collection(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value):
    """
    NOTE: function may not be able to fully represent a request to the API at this time, as the test server lacks a few
    elements.
    Lists all systems in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .with_resource_id(collection_id)
                   # .for_sub_resource_type(APITerms.ITEMS.value)
                   .build_url_from_base()
                   .build())
    print(api_request.url)
    resp = requests.get(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()


def add_systems_to_collection(server_addr: HttpUrl, collection_id: str, uri_list: str,
                              api_root: str = APITerms.API.value):
    """
    Lists all systems in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .with_resource_id(collection_id)
                   .for_sub_resource_type(APITerms.ITEMS.value)
                   .with_request_body(uri_list)
                   .build_url_from_base()
                   .build())
    resp = requests.post(api_request.url, json=api_request.body, headers=api_request.headers)
    return resp.json()


def retrieve_system_by_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value):
    """
    Retrieves a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .build_url_from_base()
                   .build())
    resp = requests.get(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()


def update_system_description(server_addr: HttpUrl, system_id: str, request_body: str,
                              api_root: str = APITerms.API.value, headers: dict = None):
    """
    Updates a system's description by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .build())
    resp = requests.put(api_request.url, data=request_body, headers=api_request.headers)
    return resp


def delete_system_by_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value, headers: dict = None):
    """
    Deletes a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_system_components(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value):
    """
    Lists all components of a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.COMPONENTS.value)
                   .build_url_from_base()
                   .build())
    print(api_request.url)
    resp = requests.get(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()


def add_system_components(server_addr: HttpUrl, system_id: str, request_body: dict,
                          api_root: str = APITerms.API.value):
    """
    Adds components to a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.COMPONENTS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .build())
    resp = requests.post(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()


def list_deployments_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value):
    """
    Lists all deployments of a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.DEPLOYMENTS.value)
                   .build_url_from_base()

                   .build())
    resp = requests.get(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()


def list_sampling_features_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value):
    """
    Lists all sampling features of a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .build_url_from_base()
                   .build())
    print(api_request.url)
    resp = requests.get(api_request.url, params=api_request.body, headers=api_request.headers)
    return resp.json()
