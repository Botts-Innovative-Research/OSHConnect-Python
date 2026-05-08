"""Unit tests for ``oshconnect.csapi4py.con_sys_api``.

Covers ``ConnectedSystemAPIRequest`` (construction + ``make_request``
dispatch) and ``ConnectedSystemsRequestBuilder`` (the fluent chain
used by the free helpers in ``api_helpers.py``). HTTP wrappers are
intercepted with ``monkeypatch.setattr`` against
``requests.{get,post,put,delete}`` so we exercise the dispatch
without standing up a server.

Auth-handling on the builder gets dedicated coverage because the
``with_auth`` ↔ ``with_basic_auth`` interplay has a non-obvious
(None, None) carve-out that prevents leaking empty credentials.
"""
from __future__ import annotations

import pytest

from oshconnect.csapi4py.con_sys_api import (
    APIRequest,
    ConnectedSystemAPIRequest,
    ConnectedSystemsRequestBuilder,
    DeleteRequest,
    GetRequest,
    PostRequest,
    PutRequest,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _MockResponse:
    status_code = 200
    ok = True
    text = ""
    headers = {}


def _capture(into: dict):
    """Returns a ``requests.<verb>``-shaped callable that records every
    kwarg the wrapper passes through."""
    def _f(url, params=None, headers=None, auth=None, data=None, json=None, **kwargs):
        into["called"] = True
        into["url"] = str(url)
        into["params"] = params
        into["headers"] = headers
        into["auth"] = auth
        into["data"] = data
        into["json"] = json
        return _MockResponse()
    return _f


# ---------------------------------------------------------------------------
# ConnectedSystemAPIRequest
# ---------------------------------------------------------------------------

class TestConnectedSystemAPIRequestConstruction:
    def test_default_method_is_get(self):
        req = ConnectedSystemAPIRequest()
        assert req.request_method == "GET"

    def test_all_optional_fields_accept_none(self):
        """All fields tolerate explicit ``None`` (regression guard for the
        pydantic ``dict = Field(None)`` annotation bug). Pre-fix, passing
        ``headers=None`` or ``params=None`` raised ``ValidationError``."""
        req = ConnectedSystemAPIRequest(
            url=None, body=None, params=None, headers=None, auth=None,
        )
        assert req.url is None
        assert req.body is None
        assert req.params is None
        assert req.headers is None
        assert req.auth is None

    def test_body_accepts_dict_or_str(self):
        as_dict = ConnectedSystemAPIRequest(body={"k": "v"})
        as_str = ConnectedSystemAPIRequest(body='{"k": "v"}')
        assert as_dict.body == {"k": "v"}
        assert as_str.body == '{"k": "v"}'

    def test_auth_accepts_tuple_or_none(self):
        with_creds = ConnectedSystemAPIRequest(auth=("u", "p"))
        without_creds = ConnectedSystemAPIRequest(auth=None)
        assert with_creds.auth == ("u", "p")
        assert without_creds.auth is None


class TestMakeRequestDispatch:
    """Each method routes to its matching ``requests.<verb>`` wrapper."""

    def test_get_routes_to_requests_get(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost:8282/sensorhub/api/systems",
            request_method="GET",
            params={"f": "json"},
            headers={"Accept": "application/json"},
            auth=("u", "p"),
        ).make_request()
        assert captured["called"] is True
        assert captured["url"] == "http://localhost:8282/sensorhub/api/systems"
        assert captured["params"] == {"f": "json"}
        assert captured["headers"] == {"Accept": "application/json"}
        assert captured["auth"] == ("u", "p")

    def test_post_routes_to_requests_post_with_body(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost:8282/sensorhub/api/systems",
            request_method="POST",
            body='{"name": "x"}',
            headers={"Content-Type": "application/json"},
        ).make_request()
        assert captured["called"] is True
        # str body lands in ``data``; dict body would land in ``json``.
        assert captured["data"] == '{"name": "x"}'
        assert captured["json"] is None

    def test_post_routes_dict_body_to_json(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost:8282/sensorhub/api/systems",
            request_method="POST",
            body={"name": "x"},
        ).make_request()
        assert captured["json"] == {"name": "x"}
        assert captured["data"] is None

    def test_put_routes_to_requests_put(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.put",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost:8282/sensorhub/api/systems/sys-1",
            request_method="PUT",
            body='{"name": "renamed"}',
        ).make_request()
        assert captured["called"] is True
        assert captured["data"] == '{"name": "renamed"}'

    def test_delete_routes_to_requests_delete(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost:8282/sensorhub/api/systems/sys-1",
            request_method="DELETE",
            auth=("u", "p"),
        ).make_request()
        assert captured["called"] is True
        assert captured["url"] == "http://localhost:8282/sensorhub/api/systems/sys-1"
        assert captured["auth"] == ("u", "p")

    def test_invalid_method_raises_value_error(self):
        req = ConnectedSystemAPIRequest(
            url="http://localhost/api/systems",
            request_method="PATCH",
        )
        with pytest.raises(ValueError, match="Invalid request method"):
            req.make_request()


class TestSendTimeValidation:
    """``make_request`` validates request coherence before dispatch.

    ``url`` may be ``None`` during builder-style construction, but the
    request must have a URL by send time. GET requests must not carry
    a body; POST/PUT bodies are optional; DELETE bodies are tolerated.
    """

    def test_send_without_url_raises(self):
        req = ConnectedSystemAPIRequest(request_method="GET")
        with pytest.raises(ValueError, match="'url' is not set"):
            req.make_request()

    def test_get_with_body_raises(self):
        req = ConnectedSystemAPIRequest(
            url="http://localhost/api/systems",
            request_method="GET",
            body={"oops": "bodies don't belong on GET"},
        )
        with pytest.raises(ValueError, match="GET requests must not carry a body"):
            req.make_request()

    def test_get_without_body_dispatches(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems",
            request_method="GET",
        ).make_request()
        assert captured["called"] is True

    def test_post_without_body_dispatches(self, monkeypatch):
        """Bodyless POST is permitted (e.g., trigger-style endpoints)."""
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems/sys-1/actions/reset",
            request_method="POST",
        ).make_request()
        assert captured["called"] is True
        assert captured["json"] is None
        assert captured["data"] is None

    def test_post_with_body_dispatches(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems",
            request_method="POST",
            body={"name": "x"},
        ).make_request()
        assert captured["json"] == {"name": "x"}

    def test_put_with_body_dispatches(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.put",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems/sys-1",
            request_method="PUT",
            body='{"name": "renamed"}',
        ).make_request()
        assert captured["data"] == '{"name": "renamed"}'

    def test_delete_without_body_dispatches(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems/sys-1",
            request_method="DELETE",
        ).make_request()
        assert captured["called"] is True

    def test_delete_with_body_is_tolerated(self, monkeypatch):
        """HTTP allows DELETE with a body (some APIs use it). We don't
        enforce against it — just ensure dispatch still happens."""
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        ConnectedSystemAPIRequest(
            url="http://localhost/api/systems/sys-1",
            request_method="DELETE",
            body={"reason": "cleanup"},
        ).make_request()
        assert captured["called"] is True


# ---------------------------------------------------------------------------
# ConnectedSystemsRequestBuilder
# ---------------------------------------------------------------------------

class TestBuilderFluentChain:
    """Every ``with_*`` method must return ``self`` for chaining."""

    @pytest.mark.parametrize("method, args", [
        ("with_api_url", ["http://localhost/api/systems"]),
        ("with_server_url", ["http://localhost:8282"]),
        ("with_api_root", ["api"]),
        ("for_resource_type", ["systems"]),
        ("with_resource_id", ["sys-1"]),
        ("for_sub_resource_type", ["datastreams"]),
        ("with_secondary_resource_id", ["ds-1"]),
        ("with_request_body", ['{"name": "x"}']),
        ("with_request_method", ["GET"]),
        ("with_headers", [{"Accept": "application/json"}]),
    ])
    def test_with_methods_return_self(self, method, args):
        builder = ConnectedSystemsRequestBuilder()
        result = getattr(builder, method)(*args)
        assert result is builder

    def test_chained_call_threads_state(self):
        """Smoke test: a representative chain produces the expected
        request shape."""
        req = (
            ConnectedSystemsRequestBuilder()
            .with_server_url("http://localhost:8282")
            .with_api_root("api")
            .for_resource_type("systems")
            .with_resource_id("sys-1")
            .build_url_from_base()
            .with_request_method("GET")
            .with_headers({"Accept": "application/json"})
            .with_basic_auth(("u", "p"))
            .build()
        )
        assert req.request_method == "GET"
        assert req.headers == {"Accept": "application/json"}
        assert req.auth == ("u", "p")
        assert "/systems/sys-1" in str(req.url)


class TestBuilderURLConstruction:
    def test_with_api_url_sets_url_directly(self):
        builder = ConnectedSystemsRequestBuilder()
        req = builder.with_api_url("http://example.com/api/x").build()
        assert str(req.url) == "http://example.com/api/x"

    def test_build_url_from_base_uses_endpoint(self):
        """``build_url_from_base`` composes ``base_url`` with whatever
        ``Endpoint.create_endpoint()`` returns."""
        req = (
            ConnectedSystemsRequestBuilder()
            .with_server_url("http://localhost:8282")
            .with_api_root("api")
            .for_resource_type("systems")
            .with_resource_id("sys-1")
            .build_url_from_base()
            .build()
        )
        assert str(req.url) == "http://localhost:8282/api/systems/sys-1"

    def test_build_url_threads_subcomponent_and_secondary_id(self):
        req = (
            ConnectedSystemsRequestBuilder()
            .with_server_url("http://localhost:8282")
            .for_resource_type("systems")
            .with_resource_id("sys-1")
            .for_sub_resource_type("datastreams")
            .with_secondary_resource_id("ds-1")
            .build_url_from_base()
            .build()
        )
        assert str(req.url).endswith("/systems/sys-1/datastreams/ds-1")


class TestBuilderAuth:
    """``with_auth`` and ``with_basic_auth`` have a non-obvious
    (None, None) carve-out that prevents leaking empty credentials."""

    def test_with_basic_auth_tuple_sets_auth(self):
        req = (
            ConnectedSystemsRequestBuilder()
            .with_basic_auth(("u", "p"))
            .build()
        )
        assert req.auth == ("u", "p")

    def test_with_basic_auth_none_is_noop(self):
        """A no-op when ``None`` is passed — does not overwrite anything
        previously set on the builder."""
        builder = ConnectedSystemsRequestBuilder()
        builder.with_basic_auth(("u", "p"))
        builder.with_basic_auth(None)
        assert builder.api_request.auth == ("u", "p")

    def test_with_auth_both_none_does_not_set_credentials(self):
        """Regression guard: ``with_auth(None, None)`` MUST NOT set
        ``("None", "None")`` or any tuple at all on the request."""
        req = (
            ConnectedSystemsRequestBuilder()
            .with_auth(None, None)
            .build()
        )
        assert req.auth is None

    def test_with_auth_real_credentials_sets_tuple(self):
        req = (
            ConnectedSystemsRequestBuilder()
            .with_auth("admin", "secret")
            .build()
        )
        assert req.auth == ("admin", "secret")

    def test_with_auth_partial_credentials_passes_through(self):
        """A single populated half *does* set a tuple — the carve-out is
        only for both being None. Documented behaviour, not a leak."""
        req = (
            ConnectedSystemsRequestBuilder()
            .with_auth("admin", None)
            .build()
        )
        assert req.auth == ("admin", None)


class TestBuilderBuildAndReset:
    def test_build_returns_api_request(self):
        builder = ConnectedSystemsRequestBuilder()
        builder.with_request_method("DELETE")
        req = builder.build()
        assert isinstance(req, ConnectedSystemAPIRequest)
        assert req.request_method == "DELETE"

    def test_reset_clears_state(self):
        builder = ConnectedSystemsRequestBuilder()
        builder.with_request_method("DELETE")
        builder.with_basic_auth(("u", "p"))
        builder.for_resource_type("systems")
        builder.reset()
        assert builder.api_request.request_method == "GET"  # back to default
        assert builder.api_request.auth is None
        # Endpoint state is reset too — re-building from base gives an
        # empty path under the api root.
        assert builder.endpoint.base_resource is None

    def test_reset_returns_self(self):
        builder = ConnectedSystemsRequestBuilder()
        assert builder.reset() is builder


# ---------------------------------------------------------------------------
# Per-method APIRequest subclasses (used by APIHelper)
# ---------------------------------------------------------------------------

import pydantic


class TestAPIRequestBase:
    """The base class itself isn't directly useful, but the contracts it
    sets — required ``url``, common fields, abstract ``execute`` — are."""

    def test_url_is_required_at_construction(self):
        with pytest.raises(pydantic.ValidationError):
            APIRequest()  # type: ignore[call-arg]

    def test_base_execute_raises_not_implemented(self):
        req = APIRequest(url="http://localhost/api/x")
        with pytest.raises(NotImplementedError):
            req.execute()


class TestGetRequest:
    def test_url_required(self):
        with pytest.raises(pydantic.ValidationError):
            GetRequest()  # type: ignore[call-arg]

    def test_no_body_field(self):
        """The type system rejects ``body`` on GET — the field literally
        isn't on the model. Catches misuse at construction."""
        assert "body" not in GetRequest.model_fields

    def test_execute_dispatches_to_get_request(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.get",
            _capture(captured),
        )
        GetRequest(
            url="http://localhost/api/systems",
            params={"f": "json"},
            headers={"Accept": "application/json"},
            auth=("u", "p"),
        ).execute()
        assert captured["url"] == "http://localhost/api/systems"
        assert captured["params"] == {"f": "json"}
        assert captured["headers"] == {"Accept": "application/json"}
        assert captured["auth"] == ("u", "p")


class TestPostRequest:
    def test_url_required(self):
        with pytest.raises(pydantic.ValidationError):
            PostRequest()  # type: ignore[call-arg]

    def test_no_params_field(self):
        """POST in this codebase carries body, not params — matches the
        ``post_request`` wrapper signature."""
        assert "params" not in PostRequest.model_fields

    def test_execute_with_str_body_routes_to_data(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        PostRequest(
            url="http://localhost/api/systems",
            body='{"name": "x"}',
        ).execute()
        assert captured["data"] == '{"name": "x"}'
        assert captured["json"] is None

    def test_execute_with_dict_body_routes_to_json(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        PostRequest(
            url="http://localhost/api/systems",
            body={"name": "x"},
        ).execute()
        assert captured["json"] == {"name": "x"}
        assert captured["data"] is None

    def test_execute_without_body_dispatches(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.post",
            _capture(captured),
        )
        PostRequest(url="http://localhost/api/x/actions/reset").execute()
        assert captured["called"] is True


class TestPutRequest:
    def test_url_required(self):
        with pytest.raises(pydantic.ValidationError):
            PutRequest()  # type: ignore[call-arg]

    def test_no_params_field(self):
        assert "params" not in PutRequest.model_fields

    def test_execute_with_body(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.put",
            _capture(captured),
        )
        PutRequest(
            url="http://localhost/api/systems/sys-1",
            body='{"name": "renamed"}',
        ).execute()
        assert captured["data"] == '{"name": "renamed"}'


class TestDeleteRequest:
    def test_url_required(self):
        with pytest.raises(pydantic.ValidationError):
            DeleteRequest()  # type: ignore[call-arg]

    def test_no_body_field(self):
        """The wrapper doesn't pass a body to ``requests.delete``; we
        match the wrapper rather than HTTP-allowed-but-unused shapes."""
        assert "body" not in DeleteRequest.model_fields

    def test_execute_dispatches_to_delete_request(self, monkeypatch):
        captured: dict = {}
        monkeypatch.setattr(
            "oshconnect.csapi4py.request_wrappers.requests.delete",
            _capture(captured),
        )
        DeleteRequest(
            url="http://localhost/api/systems/sys-1",
            auth=("u", "p"),
        ).execute()
        assert captured["url"] == "http://localhost/api/systems/sys-1"
        assert captured["auth"] == ("u", "p")
