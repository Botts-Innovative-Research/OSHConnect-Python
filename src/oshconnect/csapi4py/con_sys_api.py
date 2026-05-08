from typing import Optional, Union

from pydantic import BaseModel, HttpUrl, Field

from .endpoints import Endpoint
from .request_wrappers import post_request, put_request, get_request, delete_request


class APIRequest(BaseModel):
    """Base for per-verb request classes.

    Holds the fields every HTTP method shares: ``url`` (required),
    ``headers``, ``auth``. Subclasses (`GetRequest`, `PostRequest`,
    `PutRequest`, `DeleteRequest`) extend with verb-specific fields —
    ``params`` for GET/DELETE, ``body`` for POST/PUT — so the type
    system rejects incoherent shapes (e.g. a GET carrying a body) at
    construction time instead of silently sending them.

    Subclasses implement ``execute()`` to dispatch through the
    matching ``request_wrappers`` function.
    """
    url: HttpUrl = Field(...)
    headers: Union[dict, None] = Field(None)
    auth: Union[tuple, None] = Field(None)

    def execute(self):
        raise NotImplementedError("APIRequest subclasses must implement execute().")


class GetRequest(APIRequest):
    """GET — query parameters only; no body."""
    params: Union[dict, None] = Field(None)

    def execute(self):
        return get_request(self.url, self.params, self.headers, self.auth)


class PostRequest(APIRequest):
    """POST — body, optional. ``dict`` lands in ``json``, ``str`` in ``data``."""
    body: Union[dict, str, None] = Field(None)

    def execute(self):
        return post_request(self.url, self.body, self.headers, self.auth)


class PutRequest(APIRequest):
    """PUT — body, optional. Same body routing as POST."""
    body: Union[dict, str, None] = Field(None)

    def execute(self):
        return put_request(self.url, self.body, self.headers, self.auth)


class DeleteRequest(APIRequest):
    """DELETE — query parameters only. HTTP allows a body but the
    project's wrapper doesn't pass one, so we don't model it here."""
    params: Union[dict, None] = Field(None)

    def execute(self):
        return delete_request(self.url, self.params, self.headers, self.auth)


class ConnectedSystemAPIRequest(BaseModel):
    """Legacy single-class request shape used by the fluent
    ``ConnectedSystemsRequestBuilder`` and the free helper functions
    in ``oshconnect.api_helpers``. New code in ``APIHelper`` uses the
    per-verb subclasses above.
    """
    url: Union[HttpUrl, None] = Field(None)
    body: Union[dict, str, None] = Field(None)
    params: Union[dict, None] = Field(None)
    request_method: str = Field('GET')
    headers: Union[dict, None] = Field(None)
    auth: Union[tuple, None] = Field(None)

    def make_request(self):
        self._validate_for_send()
        match self.request_method:
            case 'GET':
                return get_request(self.url, self.params, self.headers, self.auth)
            case 'POST':
                return post_request(self.url, self.body, self.headers, self.auth)
            case 'PUT':
                return put_request(self.url, self.body, self.headers, self.auth)
            case 'DELETE':
                return delete_request(self.url, self.params, self.headers, self.auth)
            case _:
                raise ValueError(f'Invalid request method: {self.request_method!r}')

    def _validate_for_send(self):
        """Final coherence check before dispatch.

        ``url`` may be ``None`` during builder-style construction, but
        an unset URL at send time is a programming error. ``GET`` with
        a body is well-formed at the HTTP level but most servers ignore
        the body — we reject it so the caller doesn't silently send
        data that goes nowhere. ``POST``/``PUT`` bodies are optional;
        ``DELETE`` with a body is allowed by HTTP and accepted here.
        """
        if self.url is None:
            raise ValueError(
                "ConnectedSystemAPIRequest cannot be sent: 'url' is not set."
            )
        if self.request_method == 'GET' and self.body is not None:
            raise ValueError(
                "GET requests must not carry a body; pass query parameters "
                "via 'params' instead."
            )


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
        return self.with_basic_auth((uname, pword) if uname is not None or pword is not None else None)

    def with_basic_auth(self, auth: Optional[tuple]):
        """
        Set HTTP Basic Auth credentials as a (username, password) tuple. When ``auth`` is ``None``,
        leaves any previously set credentials untouched — no-ops cleanly so callers can pass an
        optional auth value through the fluent chain without an ``if`` branch.
        """
        if auth is not None:
            self.api_request.auth = auth
        return self

    def build(self):
        # convert endpoint to HttpUrl
        return self.api_request

    def reset(self):
        self.api_request = ConnectedSystemAPIRequest()
        self.endpoint = Endpoint()
        return self
