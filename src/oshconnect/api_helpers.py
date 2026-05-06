#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
from typing import Union

from pydantic import HttpUrl

from .csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from .csapi4py.constants import APITerms


def get_landing_page(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                     auth: tuple = None, headers: dict = None):
    """
    Returns the landing page of the API
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def get_conformance_info(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                         auth: tuple = None, headers: dict = None):
    """
    Returns the conformance information of the API
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONFORMANCE.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_all_collections(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                         auth: tuple = None, headers: dict = None):
    """
    List all collections
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def retrieve_collection_metadata(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value,
                                 auth: tuple = None, headers: dict = None):
    """
    Retrieve a collection by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .with_resource_id(collection_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_all_items_in_collection(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value,
                                 auth: tuple = None, headers: dict = None):
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
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def retrieve_collection_item_by_id(server_addr: HttpUrl, collection_id: str, item_id: str,
                                   api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
    """
    Retrieves a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .with_resource_id(collection_id)
                   .for_sub_resource_type(APITerms.ITEMS.value)
                   .with_resource_id(item_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_all_commands(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                      auth: tuple = None, headers: dict = None):
    """
    Lists all commands
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def list_commands_of_control_channel(server_addr: HttpUrl, control_channel_id: str, api_root: str = APITerms.API.value,
                                     auth: tuple = None, headers: dict = None):
    """
    Lists all commands of a control channel
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_channel_id)
                   .for_sub_resource_type(APITerms.COMMANDS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def send_commands_to_specific_control_stream(server_addr: HttpUrl, control_stream_id: str,
                                             request_body: Union[dict, str],
                                             api_root: str = APITerms.API.value,
                                             auth: tuple = None, headers: dict = None):
    """
    Sends a command to a control stream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONTROL_STREAMS.value)
                   .with_resource_id(control_stream_id)
                   .for_sub_resource_type(APITerms.COMMANDS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_command_by_id(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value,
                           auth: tuple = None, headers: dict = None):
    """
    Retrieves a command by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_command_description(server_addr: HttpUrl, command_id: str, request_body: Union[dict, str],
                               api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None):
    """
    Updates a command's description by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_command_by_id(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value,
                         auth: tuple = None, headers: dict = None):
    """
    Deletes a command by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def list_command_status_reports(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value,
                                auth: tuple = None, headers: dict = None):
    """
    Lists all status reports of a command by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .for_sub_resource_type(APITerms.STATUS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def add_command_status_reports(server_addr: HttpUrl, command_id: str, request_body: Union[dict, str],
                               api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None):
    """
    Adds a status report to a command by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .for_sub_resource_type(APITerms.STATUS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                         api_root: str = APITerms.API.value,
                                         auth: tuple = None, headers: dict = None):
    """
    Retrieves a status report of a command by its id and status report id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .for_sub_resource_type(APITerms.STATUS.value)
                   .with_secondary_resource_id(status_report_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                       request_body: Union[dict, str], api_root: str = APITerms.API.value,
                                       auth: tuple = None, headers: dict = None):
    """
    Updates a status report of a command by its id and status report id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .for_sub_resource_type(APITerms.STATUS.value)
                   .with_secondary_resource_id(status_report_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                       api_root: str = APITerms.API.value,
                                       auth: tuple = None, headers: dict = None):
    """
    Deletes a status report of a command by its id and status report id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COMMANDS.value)
                   .with_resource_id(command_id)
                   .for_sub_resource_type(APITerms.STATUS.value)
                   .with_secondary_resource_id(status_report_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def list_all_control_streams(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_control_streams_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_control_streams_to_system(server_addr: HttpUrl, system_id: str, request_body: Union[str, dict],
                                  api_root: str = APITerms.API.value,
                                  auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_control_stream_description_by_id(server_addr: HttpUrl, control_stream_id: str,
                                              api_root: str = APITerms.API.value,
                                              auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_control_stream_description_by_id(server_addr: HttpUrl, control_stream_id: str,
                                            request_body: Union[str, dict],
                                            api_root: str = APITerms.API.value,
                                            auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_control_stream_by_id(server_addr: HttpUrl, control_stream_id: str, api_root: str = APITerms.API.value,
                                auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def retrieve_control_stream_schema_by_id(server_addr: HttpUrl, control_stream_id: str,
                                         api_root: str = APITerms.API.value,
                                         auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_control_stream_schema_by_id(server_addr: HttpUrl, control_stream_id: str, request_body: Union[str, dict],
                                       api_root: str = APITerms.API.value,
                                       auth: tuple = None, headers: dict = None):
    """
    Updates a control stream schema by its id
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def list_all_datastreams(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                         auth: tuple = None, headers: dict = None):
    """
    Lists all datastreams
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def list_all_datastreams_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
    """
    Lists all datastreams of a system
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.DATASTREAMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_datastreams_to_system(server_addr: HttpUrl, system_id: str, request_body: Union[str, dict],
                              api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
    """
    Adds a datastream to a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.DATASTREAMS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_datastream_by_id(server_addr: HttpUrl, datastream_id: str, api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
    """
    Retrieves a datastream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_datastream_by_id(server_addr: HttpUrl, datastream_id: str, request_body: Union[str, dict],
                            api_root: str = APITerms.API.value,
                            auth: tuple = None, headers: dict = None):
    """
    Updates a datastream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_datastream_by_id(server_addr: HttpUrl, datastream_id: str, api_root: str = APITerms.API.value,
                            auth: tuple = None, headers: dict = None):
    """
    Deletes a datastream by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def retrieve_datastream_schema(server_addr: HttpUrl, datastream_id: str, api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None, obs_format: str = None):
    """
    Retrieves a datastream schema by its id.

    Hits ``GET /datastreams/{datastream_id}/schema``, optionally with
    ``?obsFormat={obs_format}`` to pick a specific schema variant. The
    CS API supports ``application/swe+json`` (default for typed
    record schemas) and ``application/om+json`` (observation-model
    form); OSH additionally supports ``logical`` (a JSON Schema
    document with ``x-ogc-*`` extension keywords — OSH-specific, not
    in the spec).

    Returns the raw HTTP response. Parse the body with the
    appropriate schema model from ``oshconnect.schema_datamodels``:
    ``SWEDatastreamRecordSchema.from_swejson_dict``,
    ``OMJSONDatastreamRecordSchema.from_omjson_dict``, or
    ``LogicalDatastreamRecordSchema.from_logical_dict``.
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .for_sub_resource_type(APITerms.SCHEMA.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    if obs_format is not None:
        api_request.params = {'obsFormat': obs_format}
    return api_request.make_request()


def update_datastream_schema(server_addr: HttpUrl, datastream_id: str, request_body: dict,
                             api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
    """
    Updates a datastream schema by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DATASTREAMS.value)
                   .with_resource_id(datastream_id)
                   .for_resource_type(APITerms.SCHEMA.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def list_all_deployments(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                         auth: tuple = None, headers: dict = None):
    """
    Lists all deployments in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def create_new_deployments(server_addr: HttpUrl, request_body: Union[str, dict], api_root: str = APITerms.API.value,
                           auth: tuple = None, headers: dict = None):
    """
    Create a new deployment as defined by the request body
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_deployment_by_id(server_addr: HttpUrl, deployment_id: str, api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
    """
    Retrieve a deployment by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_deployment_by_id(server_addr: HttpUrl, deployment_id: str, request_body: Union[str, dict],
                            api_root: str = APITerms.API.value,
                            auth: tuple = None, headers: dict = None):
    """
    Update a deployment by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_deployment_by_id(server_addr: HttpUrl, deployment_id: str, api_root: str = APITerms.API.value,
                            auth: tuple = None, headers: dict = None):
    """
    Delete a deployment by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_deployed_systems(server_addr: HttpUrl, deployment_id, api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
    """
    Lists all deployed systems in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .for_sub_resource_type(APITerms.SYSTEMS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_systems_to_deployment(server_addr: HttpUrl, deployment_id: str, uri_list: str,
                              api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
    """
    Lists all systems in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .for_sub_resource_type(APITerms.SYSTEMS.value)
                   .with_request_body(uri_list)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_deployed_system_by_id(server_addr: HttpUrl, deployment_id: str, system_id: str,
                                   api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
    """
    Retrieves a system by its id
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .for_sub_resource_type(APITerms.SYSTEMS.value)
                   .with_secondary_resource_id(system_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_deployed_system_by_id(server_addr: HttpUrl, deployment_id: str, system_id: str, request_body: dict,
                                 api_root: str = APITerms.API.value,
                                 auth: tuple = None, headers: dict = None):
    """
    Update a system by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .for_sub_resource_type(APITerms.SYSTEMS.value)
                   .with_secondary_resource_id(system_id)
                   .with_request_body(request_body)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_deployed_system_by_id(server_addr: HttpUrl, deployment_id: str, system_id: str,
                                 api_root: str = APITerms.API.value,
                                 auth: tuple = None, headers: dict = None):
    """
    Delete a system by its ID
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.DEPLOYMENTS.value)
                   .with_resource_id(deployment_id)
                   .for_sub_resource_type(APITerms.SYSTEMS.value)
                   .with_secondary_resource_id(system_id)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_deployments_of_specific_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                        auth: tuple = None, headers: dict = None):
    """
    Lists all deployments of a specific system in the server at the default API endpoint
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.SYSTEMS.value)
                   .with_resource_id(system_id)
                   .for_sub_resource_type(APITerms.DEPLOYMENTS.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_all_observations(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def list_observations_from_datastream(server_addr: HttpUrl, datastream_id: str, api_root: str = APITerms.API.value,
                                      auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def add_observations_to_datastream(server_addr: HttpUrl, datastream_id: str, request_body: Union[str, dict],
                                   api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_observation_by_id(server_addr: HttpUrl, observation_id: str, api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_observation_by_id(server_addr: HttpUrl, observation_id: str, request_body: Union[str, dict],
                             api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_observation_by_id(server_addr: HttpUrl, observation_id: str, api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def list_all_procedures(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                        auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def create_new_procedures(server_addr: HttpUrl, request_body: Union[str, dict], api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_procedure_by_id(server_addr: HttpUrl, procedure_id: str, api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_procedure_by_id(server_addr: HttpUrl, procedure_id: str, request_body: Union[str, dict],
                           api_root: str = APITerms.API.value,
                           auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_procedure_by_id(server_addr: HttpUrl, procedure_id: str, api_root: str = APITerms.API.value,
                           auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_all_properties(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                        auth: tuple = None, headers: dict = None):
    """
    List all properties
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.PROPERTIES.value)
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def create_new_properties(server_addr: HttpUrl, request_body: dict, api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_property_by_id(server_addr: HttpUrl, property_id: str, api_root: str = APITerms.API.value,
                            auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_property_by_id(server_addr: HttpUrl, property_id: str, request_body: dict,
                          api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_property_by_id(server_addr: HttpUrl, property_id: str, api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_all_sampling_features(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_sampling_features_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                                     auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def create_new_sampling_features(server_addr: HttpUrl, system_id: str, request_body: Union[dict, str],
                                 api_root: str = APITerms.API.value,
                                 auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, api_root: str = APITerms.API.value,
                                    auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, request_body: Union[dict, str],
                                  api_root: str = APITerms.API.value,
                                  auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_sampling_feature_by_id(server_addr: HttpUrl, sampling_feature_id: str, api_root: str = APITerms.API.value,
                                  auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_system_events(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                       auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def list_events_by_system_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                             auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_new_system_events(server_addr: HttpUrl, system_id: str, request_body: dict,
                          api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str,
                                api_root: str = APITerms.API.value,
                                auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str, request_body: dict,
                              api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_system_event_by_id(server_addr: HttpUrl, system_id: str, event_id: str, api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def list_system_history(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                        auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def retrieve_system_historical_description_by_id(server_addr: HttpUrl, system_id: str, history_id: str,
                                                 api_root: str = APITerms.API.value,
                                                 auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_system_historical_description(server_addr: HttpUrl, system_id: str, history_rev_id: str, request_body: dict,
                                         api_root: str = APITerms.API.value,
                                         auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_system_historical_description_by_id(server_addr: HttpUrl, system_id: str, history_rev_id: str,
                                               api_root: str = APITerms.API.value,
                                               auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_all_systems(server_addr: HttpUrl, api_root: str = APITerms.API.value,
                     auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def create_new_systems(server_addr: HttpUrl, request_body: Union[str, dict], api_root: str = APITerms.API.value,
                       auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def list_all_systems_in_collection(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value,
                                   auth: tuple = None, headers: dict = None):
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
                   .build_url_from_base()
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_systems_to_collection(server_addr: HttpUrl, collection_id: str, uri_list: str,
                              api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def retrieve_system_by_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def update_system_description(server_addr: HttpUrl, system_id: str, request_body: str,
                              api_root: str = APITerms.API.value,
                              auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('PUT')
                   .build())
    return api_request.make_request()


def delete_system_by_id(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                        auth: tuple = None, headers: dict = None):
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
                   .with_basic_auth(auth)
                   .with_request_method('DELETE')
                   .build())
    return api_request.make_request()


def list_system_components(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                           auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()


def add_system_components(server_addr: HttpUrl, system_id: str, request_body: dict,
                          api_root: str = APITerms.API.value,
                          auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('POST')
                   .build())
    return api_request.make_request()


def list_deployments_of_system(server_addr: HttpUrl, system_id: str, api_root: str = APITerms.API.value,
                               auth: tuple = None, headers: dict = None):
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
                   .with_headers(headers)
                   .with_basic_auth(auth)
                   .with_request_method('GET')
                   .build())
    return api_request.make_request()
