from typing import Union

from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def list_all_sampling_features(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers=None):
    """
    Lists all sampling features in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_sampling_features_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                     headers=None):
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
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def create_new_sampling_features(server_addr: HttpUrl, system_id: str, request_body: Union[dict, str],
                                 api_root: str = APITerms.API.value, headers=None):
    """
    Create a new sampling feature as defined by the request body
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, api_root: str = APITerms.API.value,
                                    headers=None):
    """
    Retrieve a sampling feature by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .with_resource_id(sampling_feature_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, request_body: Union[dict, str],
                                  api_root: str = APITerms.API.value, headers=None):
    """
    Update a sampling feature by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .with_resource_id(sampling_feature_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, api_root: str = APITerms.API.value,
                                  headers=None):
    """
    Delete a sampling feature by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SAMPLING_FEATURES.value)
                   .with_resource_id(sampling_feature_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()
