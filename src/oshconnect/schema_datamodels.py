#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
from __future__ import annotations

from datetime import datetime
from typing import Annotated, Union, List, Literal

from pydantic import BaseModel, Field, model_validator, HttpUrl, ConfigDict

from .api_utils import Link, URI
from .encoding import JSONEncoding
from .geometry import Geometry
from .swe_components import AnyComponent, check_named
from .timemanagement import TimeInstant


def _now_iso8601_z() -> str:
    """Per-call default for ``CommandJSON.issue_time``: a UTC timestamp with
    trailing ``Z`` (CS API Part 2 / SWE Common 3 expect a valid ISO8601
    with zone info — OSH 400s on the bare ``datetime.now().isoformat()``
    form because it has no zone designator)."""
    return TimeInstant.now_as_time_instant().get_iso_time()


def _dump_csapi(model: BaseModel) -> dict:
    """Internal: canonical CS API serialization (alias keys, exclude None, JSON-mode)."""
    return model.model_dump(by_alias=True, exclude_none=True, mode='json')


"""
In many of the top level resource models there is a "schema" field of some description. These models are meant to ease
the burden on the end user to create those.
"""


class CommandJSON(BaseModel):
    """
    A class to represent a command in JSON format
    """
    model_config = ConfigDict(populate_by_name=True)
    control_id: str = Field(None, serialization_alias="control@id")
    issue_time: Union[str, float] = Field(default_factory=_now_iso8601_z,
                                          serialization_alias="issueTime")
    sender: str = Field(None)
    # CS API Part 2 — and OSH — call this field ``parameters`` on the wire.
    # ``populate_by_name=True`` keeps the Python attribute readable as ``params``.
    params: Union[dict, list, int, float, str] = Field(None, alias="parameters")

    def to_csapi_dict(self) -> dict:
        """Render as the CS API `application/json` command body."""
        return _dump_csapi(self)

    @classmethod
    def from_csapi_dict(cls, data: dict) -> "CommandJSON":
        """Build from a CS API command JSON dict."""
        return cls.model_validate(data)


class CommandSchema(BaseModel):
    """
    Base class representation for control streams' command schemas
    """
    model_config = ConfigDict(populate_by_name=True)

    command_format: str = Field(..., alias='commandFormat')


class SWEJSONCommandSchema(CommandSchema):
    """
    SWE+JSON command schema
    """
    model_config = ConfigDict(populate_by_name=True)

    # Literal pin powers the discriminated `AnyCommandSchema` union below
    # and removes the need for a runtime field_validator.
    command_format: Literal["application/swe+json"] = Field(
        "application/swe+json", alias='commandFormat')
    # Concrete subclass instead of `SerializeAsAny[Encoding]` — `JSONEncoding`
    # is the only Encoding type used in practice, and a concrete type
    # serializes deterministically without `SerializeAsAny`. If/when more
    # encoding types arrive, migrate this to a discriminated Union on
    # `Encoding.type`.
    encoding: JSONEncoding = Field(...)
    record_schema: AnyComponent = Field(..., alias='recordSchema')

    @model_validator(mode="after")
    def _root_record_schema_requires_name(self):
        check_named(self.record_schema, "SWEJSONCommandSchema.recordSchema")
        return self

    def to_swejson_dict(self) -> dict:
        """Render as an `application/swe+json` command-schema document."""
        return _dump_csapi(self)

    @classmethod
    def from_swejson_dict(cls, data: dict) -> "SWEJSONCommandSchema":
        """Build from an `application/swe+json` command-schema dict."""
        return cls.model_validate(data, by_alias=True)


class JSONCommandSchema(CommandSchema):
    """
    JSON command schema
    """
    model_config = ConfigDict(populate_by_name=True)

    command_format: Literal["application/json"] = Field("application/json", alias='commandFormat')
    params_schema: AnyComponent = Field(..., alias='parametersSchema')
    result_schema: AnyComponent = Field(None, alias='resultSchema')
    feasibility_schema: AnyComponent = Field(None, alias='feasibilityResultSchema')

    @model_validator(mode="after")
    def _root_schemas_require_name(self):
        check_named(self.params_schema, "JSONCommandSchema.parametersSchema")
        if self.result_schema is not None:
            check_named(self.result_schema, "JSONCommandSchema.resultSchema")
        if self.feasibility_schema is not None:
            check_named(self.feasibility_schema, "JSONCommandSchema.feasibilityResultSchema")
        return self

    def to_json_dict(self) -> dict:
        """Render as an `application/json` command-schema document."""
        return _dump_csapi(self)

    @classmethod
    def from_json_dict(cls, data: dict) -> "JSONCommandSchema":
        """Build from an `application/json` command-schema dict."""
        return cls.model_validate(data, by_alias=True)


class DatastreamRecordSchema(BaseModel):
    """
    A class to represent the schema of a datastream
    """
    model_config = ConfigDict(populate_by_name=True)

    obs_format: str = Field(..., alias='obsFormat')


# `encoding` is required per CS API Part 2 §16.2.3 Requirement 109.B, but the
# OSH server omits it from /datastreams/{id}/schema responses. We accept it as
# optional to be able to parse what the server returns. See
# docs/osh_spec_deviations.md (swe-json-missing-encoding).
class SWEDatastreamRecordSchema(DatastreamRecordSchema):
    model_config = ConfigDict(populate_by_name=True)
    # Multi-Literal acts as the discriminator value(s) for AnyDatastreamRecordSchema
    # below. Replaces the previous runtime field_validator.
    obs_format: Literal[
        "application/swe+json",
        "application/swe+csv",
        "application/swe+text",
        "application/swe+binary",
    ] = Field(..., alias='obsFormat')
    encoding: JSONEncoding = Field(None)
    record_schema: AnyComponent = Field(..., alias='recordSchema')

    @model_validator(mode="after")
    def _root_record_schema_requires_name(self):
        check_named(self.record_schema, "SWEDatastreamRecordSchema.recordSchema")
        return self

    def to_swejson_dict(self) -> dict:
        """Render as an `application/swe+json` datastream-schema document."""
        return _dump_csapi(self)

    @classmethod
    def from_swejson_dict(cls, data: dict) -> "SWEDatastreamRecordSchema":
        """Build from an `application/swe+json` datastream-schema dict
        (e.g., a CS API ``/datastreams/{id}/schema`` response in SWE form)."""
        return cls.model_validate(data, by_alias=True)


class OMJSONDatastreamRecordSchema(DatastreamRecordSchema):
    """Datastream observation schema for the OM+JSON media type
    (`application/om+json`, also accepts `application/json` as a synonym
    on parse since OSH treats them equivalently for datastream schemas).

    Per CS API Part 2 §16.1.4, this form does not carry a SWE `encoding`
    block; structure is fully described by `resultSchema` (inline result)
    or `resultLink` (out-of-band). `parametersSchema` is optional.
    """
    model_config = ConfigDict(populate_by_name=True)

    # Multi-Literal — both wire forms are spec-equivalent for OM+JSON.
    obs_format: Literal[
        "application/om+json",
        "application/json",
    ] = Field("application/om+json", alias='obsFormat')
    result_schema: AnyComponent = Field(None, alias='resultSchema')
    parameters_schema: AnyComponent = Field(None, alias='parametersSchema')
    result_link: dict = Field(None, alias='resultLink')

    @model_validator(mode="after")
    def _root_schemas_require_name(self):
        if self.result_schema is not None:
            check_named(self.result_schema, "OMJSONDatastreamRecordSchema.resultSchema")
        if self.parameters_schema is not None:
            check_named(self.parameters_schema, "OMJSONDatastreamRecordSchema.parametersSchema")
        return self

    def to_omjson_dict(self) -> dict:
        """Render as an `application/om+json` datastream-schema document."""
        return _dump_csapi(self)

    @classmethod
    def from_omjson_dict(cls, data: dict) -> "OMJSONDatastreamRecordSchema":
        """Build from an `application/om+json` (or `application/json`)
        datastream-schema dict (e.g., a CS API ``/datastreams/{id}/schema``
        response in OM+JSON form)."""
        return cls.model_validate(data, by_alias=True)


class LogicalProperty(BaseModel):
    """One entry in `LogicalDatastreamRecordSchema.properties`.

    The logical schema is OSH's JSON-Schema-flavored representation of a
    SWE Common DataRecord. Each property is a JSON Schema field with
    OGC extension keywords (`x-ogc-definition`, `x-ogc-refFrame`,
    `x-ogc-unit`, `x-ogc-axis`) that carry the SWE Common metadata.

    Permissive: ``extra='allow'`` accepts JSON Schema fields we haven't
    modeled (e.g. ``description``, ``default``, ``minimum``, ``maximum``,
    nested ``items`` for arrays).
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    title: str = Field(None)
    type: str = Field(...)            # "string" | "number" | "integer" | "boolean" | "object" | "array"
    format: str = Field(None)         # e.g. "date-time"
    enum: list = Field(None)
    items: dict = Field(None)         # for type="array"
    properties: dict = Field(None)    # for type="object" (nested)

    # OGC SWE Common extensions (hyphenated keys → aliased)
    ogc_definition: str = Field(None, alias='x-ogc-definition')
    ogc_ref_frame: str = Field(None, alias='x-ogc-refFrame')
    ogc_unit: str = Field(None, alias='x-ogc-unit')
    ogc_axis: str = Field(None, alias='x-ogc-axis')


class LogicalDatastreamRecordSchema(BaseModel):
    """Logical schema document — OSH's `obsFormat=logical` representation.

    Returned by ``GET /datastreams/{id}/schema?obsFormat=logical``. Distinct
    from `SWEDatastreamRecordSchema` and `OMJSONDatastreamRecordSchema`:

    - No ``obsFormat`` envelope field
    - No ``recordSchema`` wrapper — the schema is the document
    - JSON Schema flavor (``type: "object"`` + ``properties``) instead of
      a SWE Common AnyComponent tree
    - Each property carries SWE Common metadata via ``x-ogc-*`` extension
      keywords

    OSH-specific (not in the OGC CS API spec) but useful for tooling that
    speaks JSON Schema natively. Permissive (``extra='allow'``) so future
    JSON Schema fields don't break parsing.
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    type: str = Field(...)            # always "object" for OSH datastream schemas
    title: str = Field(None)
    properties: dict[str, LogicalProperty] = Field(...)
    required: list[str] = Field(None)

    def to_logical_dict(self) -> dict:
        """Render as an OSH `obsFormat=logical` JSON Schema dict."""
        return _dump_csapi(self)

    @classmethod
    def from_logical_dict(cls, data: dict) -> "LogicalDatastreamRecordSchema":
        """Build from a logical schema dict (e.g., a CS API
        ``/datastreams/{id}/schema?obsFormat=logical`` response body)."""
        return cls.model_validate(data, by_alias=True)


class ObservationOMJSONInline(BaseModel):
    """
    A class to represent an observation in OM-JSON format
    """
    model_config = ConfigDict(populate_by_name=True)
    datastream_id: str = Field(None, alias="datastream@id")
    foi_id: str = Field(None, alias="foi@id")
    phenomenon_time: str = Field(None, alias="phenomenonTime")
    result_time: str = Field(datetime.now().isoformat(), alias="resultTime")
    parameters: dict = Field(None)
    result: Union[int, float, str, dict, list] = Field(...)
    result_links: List[Link] = Field(None, alias="result@links")

    def to_csapi_dict(self) -> dict:
        """Render as an `application/om+json` observation body."""
        return _dump_csapi(self)

    @classmethod
    def from_csapi_dict(cls, data: dict) -> "ObservationOMJSONInline":
        """Build from an `application/om+json` observation dict."""
        return cls.model_validate(data)


class SystemEventOMJSON(BaseModel):
    """
    A class to represent the schema of a system event
    """
    model_config = ConfigDict(populate_by_name=True)
    label: str = Field(...)
    description: str = Field(None)
    definition: HttpUrl = Field(...)
    identifiers: list = Field(None)
    classifiers: list = Field(None)
    contacts: list = Field(None)
    documentation: list = Field(None)
    time: str = Field(...)
    properties: list = Field(None)
    configuration: dict = Field(None)
    links: list[Link] = Field(None)


class SystemHistoryGeoJSON(BaseModel):
    """
    A class to represent the schema of a system history
    """
    model_config = ConfigDict(populate_by_name=True)
    type: str = Field(...)
    id: str = Field(None)
    properties: SystemHistoryProperties = Field(...)
    geometry: Geometry = Field(None)
    bbox: list = Field(None)
    links: list[Link] = Field(None)


class SystemHistoryProperties(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    feature_type: str = Field(...)
    uid: URI = Field(...)
    name: str = Field(...)
    description: str = Field(None)
    asset_type: str = Field(None)
    valid_time: list = Field(None)
    parent_system_link: str = Field(None, serialization_alias='parentSystem@link')
    procedure_link: str = Field(None, serialization_alias='procedure@link')


# Discriminated unions replace the earlier `SerializeAsAny[<base>]` pattern
# on resource models. Pydantic dispatches by the literal value of the
# discriminator field — `obsFormat` / `commandFormat` — so validate and
# dump round-trip without polymorphism quirks.
AnyDatastreamRecordSchema = Annotated[
    Union[SWEDatastreamRecordSchema, OMJSONDatastreamRecordSchema],
    Field(discriminator='obs_format'),
]
"""Public alias for `DatastreamResource.record_schema`. Discriminator: `obs_format`."""

AnyCommandSchema = Annotated[
    Union[SWEJSONCommandSchema, JSONCommandSchema],
    Field(discriminator='command_format'),
]
"""Public alias for `ControlStreamResource.command_schema`. Discriminator: `command_format`."""


# Defense-in-depth: rebuild every container model that forward-references
# `AnyComponent`. See the matching block in swe_components.py for the
# `MockValSer` rationale — same fault recurs here because each schema
# class threads `AnyComponent` through its body.
SWEJSONCommandSchema.model_rebuild(force=True)
JSONCommandSchema.model_rebuild(force=True)
SWEDatastreamRecordSchema.model_rebuild(force=True)
OMJSONDatastreamRecordSchema.model_rebuild(force=True)
