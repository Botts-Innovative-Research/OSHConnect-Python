from enum import Enum

import requests
# import websockets
from pydantic import BaseModel, Field

from .constants import APITerms


class Endpoint(BaseModel):
    api_root: str = APITerms.API.value
    base_resource: APITerms = Field(None)
    resource_id: str = Field(None)
    sub_component: APITerms = Field(None)
    secondary_resource_id: str = Field(None)

    def create_endpoint(self):
        # TODO: Handle insertion of  "/" in the right places
        # Create endpoints bases of api spec
        base_res_id = '' if self.base_resource is None else f'/{self.base_resource}'
        res_id = '' if self.resource_id is None else f'/{self.resource_id}'
        sub_comp = '' if self.sub_component is None else f'/{self.sub_component}'
        secondary_res_id = '' if self.secondary_resource_id is None else f'/{self.secondary_resource_id}'

        return f'{self.api_root}{base_res_id}{res_id}{sub_comp}{secondary_res_id}'


class SystemQueryParams(Enum):
    Keywords = 'q'
    """
    A comma-separated list of keywords to search for in the system name, description, and definition.
    """
    BBOX = 'bbox'
    """
    BBOX to fileter resources based on their location
    """
    LOCATION = 'location'
    """
    WKT geometry to filter resources based on their location or geometry
    """
    VALID_TIME = 'validTime'
    """
    ISO 8601 time interval to filter resources based on their valid time. When omitted, the implicit time is "now"
    except for "history" collection where no filtering is applied.
    """
    PARENT = 'parent'
    """
    Comma-separated list of parent system IDs or "*" to included nested resources at any level
    """
    SELECT = 'select'
    """
    Comma-separated list of properties to include or exclude from results (use "!" prefix to exclude)
    """
    FORMAT = 'format'
    """
    Mime type of the response format.
    """
    LIMIT = 'limit'
    """
    Maximum number of resources to return per page (max 1000)
    """
    OFFSET = 'offset'
    """
    Token specifying the page to return (usually the token provided in the previous call)
    """

    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


def handle_request(url, params=None, content_json=None, method='get', response_handler=None):
    """
    Handles a request to the API
    :param url: The URL to make the request to
    :param params: The parameters to send with the request
    :param content_json: The JSON to send with the request
    :param method: The method to use for the request
    :param response_handler:
    :return: The response from the API
    """

    r = None

    if method == 'get':
        r = requests.get(url, params=params)
    elif method == 'post':
        r = requests.post(url, params=params, json=content_json, headers={'Content-Type': 'application/json'})
    elif method == 'put':
        r = requests.put(url, params=params, json=content_json)
    elif method == 'delete':
        r = requests.delete(url, params=params)
    else:
        raise ValueError(f'Invalid method: {method}')

    if response_handler is not None:
        return response_handler(r)
    else:
        return r


# async def handle_ws(url, params=None, json_data=None, method='get', response_handler=None):
#     """
#     Handles a request to the API. Functionality is limited to receiving observations for now, but will be improved in
#     future versions.
#     :param url: The URL to make the request to
#     :param params: The parameters to send with the request
#     :param json_data: The JSON to send with the request
#     :param method: The method to use for the request
#     :param response_handler: callback function to handle the response msg
#     :return: The response from the API
#     """
#
#     async with websockets.connect(url) as ws:
#         while True:
#             msg = await ws.recv()
#             response_handler(msg)
