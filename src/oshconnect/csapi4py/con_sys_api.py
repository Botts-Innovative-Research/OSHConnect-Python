from typing import Union

from pydantic import BaseModel, HttpUrl, Field

from .endpoints import Endpoint
from .request_wrappers import post_request, put_request, get_request, delete_request


class ConnectedSystemAPIRequest(BaseModel):
    url: HttpUrl = Field(None)
    body: Union[dict, str] = Field(None)
    params: dict = Field(None)
    request_method: str = Field('GET')
    headers: dict = Field(None)
    auth: Union[tuple, None] = Field(None)

    def make_request(self):
        match self.request_method:
            case 'GET':
                return get_request(self.url, self.params, self.headers, self.auth)
            case 'POST':
                print(f'POST request: {self}')
                return post_request(self.url, self.body, self.headers, self.auth)
            case 'PUT':
                print(f'PUT request: {self}')
                return put_request(self.url, self.body, self.headers, self.auth)
            case 'DELETE':
                print(f'DELETE request: {self}')
                return delete_request(self.url, self.params, self.headers, self.auth)
            case _:
                raise ValueError('Invalid request method')


class ConnectedSystemsRequestBuilder(BaseModel):
    api_request: ConnectedSystemAPIRequest = Field(default_factory=ConnectedSystemAPIRequest)
    base_url: HttpUrl = None
    endpoint: Endpoint = Field(default_factory=Endpoint)

    def with_api_url(self, url: HttpUrl):
        self.api_request.url = url
        return self

    def with_server_url(self, server_url: HttpUrl):
        self.base_url = server_url
        return self

    def build_url_from_base(self):
        """
        Builds the full API endpoint URL from the base URL and the endpoint parameters that have been previously
        provided.
        """
        self.api_request.url = f'{self.base_url}/{self.endpoint.create_endpoint()}'
        return self

    def with_api_root(self, api_root: str):
        """
        Optional: Set the API root for the request. This is useful if you want to use a different API root than the
        default one (api).
        :param api_root:
        :return:
        """
        self.endpoint.api_root = api_root
        return self

    def for_resource_type(self, resource_type: str):
        self.endpoint.base_resource = resource_type
        return self

    def with_resource_id(self, resource_id: str):
        self.endpoint.resource_id = resource_id
        return self

    def for_sub_resource_type(self, sub_resource_type: str):
        self.endpoint.sub_component = sub_resource_type
        return self

    def with_secondary_resource_id(self, resource_id: str):
        self.endpoint.secondary_resource_id = resource_id
        return self

    def with_request_body(self, request_body: str):
        self.api_request.body = request_body
        return self

    def with_request_method(self, request_method: str):
        self.api_request.request_method = request_method
        return self

    def with_headers(self, headers: dict = None):
        # TODO: ensure headers can default if excluded
        self.api_request.headers = headers
        return self

    def with_auth(self, uname: str, pword: str):
        self.api_request.auth = (uname, pword)
        return self

    def build(self):
        # convert endpoint to HttpUrl
        return self.api_request

    def reset(self):
        self.api_request = ConnectedSystemAPIRequest()
        self.endpoint = Endpoint()
        return self
