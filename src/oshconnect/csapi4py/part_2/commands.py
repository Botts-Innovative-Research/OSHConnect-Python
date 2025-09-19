from typing import Union

from pydantic import HttpUrl

from src.oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from src.oshconnect.csapi4py.constants import APITerms


def list_all_commands(server_addr: HttpUrl, api_root: str = APITerms.API.value, headers: dict = None):
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
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def list_commands_of_control_channel(server_addr: HttpUrl, control_channel_id: str, api_root: str = APITerms.API.value,
                                     headers=None):
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
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def send_commands_to_specific_control_stream(server_addr: HttpUrl, control_stream_id: str,
                                             request_body: Union[dict, str],
                                             api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_command_by_id(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_command_description(server_addr: HttpUrl, command_id: str, request_body: Union[dict, str],
                               api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_command_by_id(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()


def list_command_status_reports(server_addr: HttpUrl, command_id: str, api_root: str = APITerms.API.value,
                                headers=None):
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
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def add_command_status_reports(server_addr: HttpUrl, command_id: str, request_body: Union[dict, str],
                               api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('POST')
                   .build())

    return api_request.make_request()


def retrieve_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                         api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('GET')
                   .build())

    return api_request.make_request()


def update_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                       request_body: Union[dict, str], api_root: str = APITerms.API.value,
                                       headers=None):
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
                   .with_request_method('PUT')
                   .build())

    return api_request.make_request()


def delete_command_status_report_by_id(server_addr: HttpUrl, command_id: str, status_report_id: str,
                                       api_root: str = APITerms.API.value, headers=None):
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
                   .with_request_method('DELETE')
                   .build())

    return api_request.make_request()
