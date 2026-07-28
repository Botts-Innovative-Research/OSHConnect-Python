#  =============================================================================
#  Copyright (c) 2026 Georobotix Innovative Research
#  Date: 2026/7/28
#  Author: Ian Patterson
#  Contact Email: ian.patterson@georobotix.us
#  =============================================================================

"""Typed exceptions raised by OSHConnect.

Every error the library raises deliberately descends from
`OSHConnectError`, so callers can wrap an OSHConnect operation and catch
*its* failures without also swallowing `KeyError`, `AttributeError`, and
every other bug in their own code::

    from oshconnect.exceptions import OSHConnectError, ResourceInsertError

    try:
        system.insert_self()
    except ResourceInsertError as e:
        if e.status_code == 507:      # server out of disk — worth retrying
            schedule_retry()
        else:
            raise

`OSHConnectError` subclasses the builtin `Exception`, so pre-existing
``except Exception:`` handlers keep working unchanged.

The HTTP free-function layer in `oshconnect.api_helpers` deliberately does
*not* raise these — it returns raw `requests.Response` objects and leaves
status interpretation to the caller. These exceptions come from the
wrapper layer (`System`, `Datastream`, `ControlStream`, `Node`,
`OSHConnect`), which does interpret responses on the caller's behalf.
"""
from __future__ import annotations


class OSHConnectError(Exception):
    """Base class for every error OSHConnect raises deliberately.

    Catch this to handle any OSHConnect-originated failure while letting
    genuine programming errors propagate.
    """


class ConfigurationError(OSHConnectError):
    """The library was asked to act on an object it hasn't been given.

    Raised for caller-side wiring mistakes that are detectable before any
    HTTP request is attempted — e.g. inserting a system into a `Node` that
    was never registered with the `OSHConnect` instance.
    """


class ResourceRequestError(OSHConnectError):
    """Base for failures tied to a specific CS API HTTP exchange.

    Carries whatever the response made available. Every field is optional
    because not every call site has all of them — check for ``None``
    rather than assuming.

    :param message: Human-readable description; becomes ``str(exc)``.
    :param status_code: HTTP status from the response, when there was one.
    :param response_text: Raw response body, when there was one.
    :param resource_type: The CS API resource involved, e.g. ``'system'``.
    :param resource_label: Caller-facing name of the specific resource,
        e.g. the system's label or the datastream's name.
    """

    def __init__(self, message: str, *, status_code: int = None,
                 response_text: str = None, resource_type: str = None,
                 resource_label: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.resource_type = resource_type
        self.resource_label = resource_label


class ResourceInsertError(ResourceRequestError):
    """A create (POST) of a CS API resource did not succeed.

    Raised when the server returns a non-OK response to a resource
    creation request — inserting a system, datastream, control stream, or
    observation.
    """


class MissingLocationHeaderError(ResourceInsertError):
    """A create POST succeeded but the server omitted ``Location``.

    The resource was very likely created; OSHConnect just cannot learn its
    server-assigned id, so the local wrapper cannot be linked to it. A
    distinct type because the remediation differs from an outright
    rejection: the caller may need to re-discover rather than re-POST, to
    avoid creating a duplicate. Subclasses `ResourceInsertError` so
    callers that don't care about the distinction can catch the broader
    type.
    """


class ResourceDiscoveryError(ResourceRequestError):
    """A listing / discovery (GET) request did not succeed.

    Distinguishes a genuine failure — server down, bad credentials, 5xx —
    from the legitimately-empty result that discovery otherwise returns.
    """


__all__ = [
    "OSHConnectError",
    "ConfigurationError",
    "ResourceRequestError",
    "ResourceInsertError",
    "MissingLocationHeaderError",
    "ResourceDiscoveryError",
]
