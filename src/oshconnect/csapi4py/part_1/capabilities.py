from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def get_landing_page(server_addr: HttpUrl, api_root: str = APITerms.API.value):
    """
    Returns the landing page of the API
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .build_url_from_base()
                   .build())
    return api_request


def get_conformance_info(server_addr: HttpUrl, api_root: str = APITerms.API.value):
    """
    Returns the conformance information of the API
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.CONFORMANCE.value)
                   .build_url_from_base()
                   .build())
    return api_request
