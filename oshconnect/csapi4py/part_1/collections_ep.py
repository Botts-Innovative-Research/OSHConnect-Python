from pydantic import HttpUrl

from oshconnect.csapi4py.con_sys_api import ConnectedSystemsRequestBuilder
from oshconnect.csapi4py.constants import APITerms


def list_all_collections(server_addr: HttpUrl, api_root: str = APITerms.API.value):
    """
    List all collections
    :return:
    """
    builder = ConnectedSystemsRequestBuilder()
    api_request = (builder.with_server_url(server_addr)
                   .with_api_root(api_root)
                   .for_resource_type(APITerms.COLLECTIONS.value)
                   .build_url_from_base()
                   .build())
    return api_request


def retrieve_collection_metadata(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value):
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
                   .build())
    return api_request


def list_all_items_in_collection(server_addr: HttpUrl, collection_id: str, api_root: str = APITerms.API.value):
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
                   .build())
    return api_request


def retrieve_collection_item_by_id(server_addr: HttpUrl, collection_id: str, item_id: str,
                                   api_root: str = APITerms.API.value):
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
                   .build())
    return api_request
