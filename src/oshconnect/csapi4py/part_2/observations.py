from typing import Union

from pydantic import HttpUrl

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from src.oshconnect.csapi4py.constants import APITerms


def list_all_observations(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers=None):
    """
    Lists all observations
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.OBSERVATIONS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def list_observations_from_datastream(server_addr: HttpUrl, datastream_id: str, api_root: str = APITerms.API.value,
                                      headers=None):
    """
    Lists all observations of a datastream
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .for_sub_resource_type(APITerms.OBSERVATIONS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def add_observations_to_datastream(server_addr: HttpUrl, datastream_id: str, request_body: Union[str, dict],
                                   api_root: str = APITerms.API.value, headers=None):
    """
    Adds an observation to a datastream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .for_sub_resource_type(APITerms.OBSERVATIONS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_observation_by_id(server_addr: HttpUrl, observation_id: str, api_root: str = APITerms.API.value,
                               headers=None):
    """
    Retrieves an observation by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.OBSERVATIONS.value)
                   .with_resource_id(observation_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_observation_by_id(server_addr: HttpUrl, observation_id: str, request_body: Union[str, dict],
                             api_root: str = APITerms.API.value, headers=None):
    """
    Updates an observation by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.OBSERVATIONS.value)
                   .with_resource_id(observation_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_observation_by_id(server_addr: HttpUrl, observation_id: str, api_root: str = APITerms.API.value,
                             headers=None):
    """
    Deletes an observation by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.OBSERVATIONS.value)
                   .with_resource_id(observation_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()
