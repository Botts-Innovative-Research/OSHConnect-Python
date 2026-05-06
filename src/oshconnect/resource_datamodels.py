#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import json
from typing import List, TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, model_validator
from shapely import Point

from .api_utils import Link
from .geometry import Geometry
from .schema_datamodels import DatastreamRecordSchema, CommandSchema
from .timemanagement import TimeInstant, TimePeriod

if TYPE_CHECKING:
    from .swe_components import AnyComponent


class BoundingBox(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    lower_left_corner: Point = Field(..., description="The lower left corner of the bounding box.")
    upper_right_corner: Point = Field(..., description="The upper right corner of the bounding box.")
    min_value: float = Field(None, description="The minimum value of the bounding box.")
    max_value: float = Field(None, description="The maximum value of the bounding box.")

    # @model_validator(mode='before')
    # def validate_minmax(self) -> Self:
    #     if self.min_value > self.max_value:
    #         raise ValueError("min_value must be less than max_value")
    #     return self


class SecurityConstraints:
    constraints: list


class LegalConstraints:
    constraints: list


class Characteristics:
    characteristics: list


class Capabilities:
    capabilities: list


class Contact:
    contact: list


class Documentation:
    documentation: list


class HistoryEvent:
    history_event: list


class ConfigurationSettings:
    settings: list


class FeatureOfInterest:
    feature: list


class Input:
    input: list


class Output:
    output: list


class Parameter:
    parameter: list


class Mode:
    mode: list


class ProcessMethod:
    method: list


class BaseResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    id: str = Field(..., alias="id")
    name: str = Field(...)
    description: str = Field(None)
    type: str = Field(None)
    links: List[Link] = Field(None)


class SystemResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    feature_type: str = Field(None, alias="type")
    system_id: str = Field(None, alias="id")
    properties: dict = Field(None)
    geometry: Geometry | None = Field(None)
    bbox: BoundingBox = Field(None)
    links: List[Link] = Field(None)
    description: str = Field(None)
    uid: str = Field(None, alias="uniqueId")
    label: str = Field(None)
    lang: str = Field(None)
    keywords: List[str] = Field(None)
    identifiers: List[str] = Field(None)
    classifiers: List[str] = Field(None)
    valid_time: TimePeriod = Field(None, alias="validTime")
    security_constraints: List[SecurityConstraints] = Field(None, alias="securityConstraints")
    legal_constraints: List[LegalConstraints] = Field(None, alias="legalConstraints")
    characteristics: List[Characteristics] = Field(None)
    capabilities: List[Capabilities] = Field(None)
    contacts: List[Contact] = Field(None)
    documentation: List[Documentation] = Field(None)
    history: List[HistoryEvent] = Field(None)
    definition: str = Field(None)
    type_of: str = Field(None, alias="typeOf")
    configuration: ConfigurationSettings = Field(None)
    features_of_interest: List[FeatureOfInterest] = Field(None, alias="featuresOfInterest")
    inputs: List[Input] = Field(None)
    outputs: List[Output] = Field(None)
    parameters: List[Parameter] = Field(None)
    modes: List[Mode] = Field(None)
    method: ProcessMethod = Field(None)

    def to_smljson_dict(self) -> dict:
        """Render this system as an `application/sml+json` dict (SensorML JSON encoding).

        Sets ``feature_type = "PhysicalSystem"`` to match the SML discriminator
        before dumping. Output keys are camelCase per the CS API wire format.
        """
        self.feature_type = "PhysicalSystem"
        return self.model_dump(by_alias=True, exclude_none=True, mode='json')

    def to_smljson(self) -> str:
        """JSON-string variant of `to_smljson_dict`."""
        return json.dumps(self.to_smljson_dict())

    def to_geojson_dict(self) -> dict:
        """Render this system as an `application/geo+json` dict.

        Sets ``feature_type = "Feature"`` to match the GeoJSON discriminator
        before dumping. Useful when posting to endpoints that expect the
        GeoJSON Feature shape.
        """
        self.feature_type = "Feature"
        return self.model_dump(by_alias=True, exclude_none=True, mode='json')

    def to_geojson(self) -> str:
        """JSON-string variant of `to_geojson_dict`."""
        return json.dumps(self.to_geojson_dict())

    @classmethod
    def from_smljson_dict(cls, data: dict) -> "SystemResource":
        """Build a `SystemResource` from an `application/sml+json` dict
        (e.g., a CS API server response body for a system in SML form)."""
        return cls.model_validate(data, by_alias=True)

    @classmethod
    def from_geojson_dict(cls, data: dict) -> "SystemResource":
        """Build a `SystemResource` from an `application/geo+json` dict
        (e.g., a CS API server response body for a system in GeoJSON form)."""
        return cls.model_validate(data, by_alias=True)

    @classmethod
    def from_csapi_dict(cls, data: dict) -> "SystemResource":
        """Build a `SystemResource` from a CS API system dict, auto-dispatching
        on the ``type`` field: ``"PhysicalSystem"`` → SML+JSON path,
        ``"Feature"`` → GeoJSON path. Anything else falls through to a
        permissive validate.
        """
        feature_type = data.get("type")
        if feature_type == "PhysicalSystem":
            return cls.from_smljson_dict(data)
        if feature_type == "Feature":
            return cls.from_geojson_dict(data)
        return cls.model_validate(data, by_alias=True)


class DatastreamResource(BaseModel):
    """
    The DatastreamResource class is a Pydantic model that represents a datastream resource in the OGC SensorThings API.
    It contains all the necessary and optional properties listed in the OGC Connected Systems API documentation. Note
    that, depending on the format of the  request, the fields needed may differ. There may be derived models in a later
    release that will have different sets of required fields to ease the validation process for users.
    """
    model_config = ConfigDict(populate_by_name=True)

    ds_id: str = Field(..., alias="id")
    name: str = Field(...)
    description: str = Field(None)
    valid_time: TimePeriod = Field(..., alias="validTime")
    output_name: str = Field(None, alias="outputName")
    procedure_link: Link = Field(None, alias="procedureLink@link")
    deployment_link: Link = Field(None, alias="deploymentLink@link")
    feature_of_interest_link: Link = Field(None, alias="featureOfInterest@link")
    sampling_feature_link: Link = Field(None, alias="samplingFeature@link")
    parameters: dict = Field(None)
    phenomenon_time: TimePeriod = Field(None, alias="phenomenonTime")
    result_time: TimePeriod = Field(None, alias="resultTimeInterval")
    ds_type: str = Field(None, alias="type")
    result_type: str = Field(None, alias="resultType")
    formats: List[str] = Field(default_factory=list)
    observed_properties: List[dict] = Field(default_factory=list, alias="observedProperties")
    system_id: str = Field(None, alias="system@id")
    links: List[Link] = Field(None)
    record_schema: SerializeAsAny[DatastreamRecordSchema] = Field(None, alias="schema")

    @classmethod
    @model_validator(mode="before")
    def handle_aliases(cls, values):
        if isinstance(values, dict):
            if 'ds_id' not in values:
                for alias in ('id', 'datastream_id'):
                    if alias in values:
                        values['ds_id'] = values[alias]
                        break
            if 'valid_time' not in values:
                for alias in ('validTime', 'time_interval'):
                    if alias in values:
                        values['valid_time'] = values[alias]
                        break
        return values

    def to_csapi_dict(self) -> dict:
        """Render this datastream as the CS API `application/json` resource
        body. The embedded ``schema`` field is dumped polymorphically per
        whichever variant (`SWEDatastreamRecordSchema` /
        `OMJSONDatastreamRecordSchema`) it holds.
        """
        return self.model_dump(by_alias=True, exclude_none=True, mode='json')

    def to_csapi_json(self) -> str:
        """JSON-string variant of `to_csapi_dict`."""
        return json.dumps(self.to_csapi_dict())

    @classmethod
    def from_csapi_dict(cls, data: dict) -> "DatastreamResource":
        """Build a `DatastreamResource` from a CS API datastream dict
        (e.g., a server response body or an entry from a /datastreams
        listing)."""
        return cls.model_validate(data, by_alias=True)


class ObservationResource(BaseModel):
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    sampling_feature_id: str = Field(None, alias="samplingFeature@Id")
    procedure_link: Link = Field(None, alias="procedure@link")
    phenomenon_time: TimeInstant = Field(None, alias="phenomenonTime")
    result_time: TimeInstant = Field(..., alias="resultTime")
    parameters: dict = Field(None)
    result: dict = Field(...)
    result_link: Link = Field(None, alias="result@link")

    def to_omjson_dict(self, datastream_id: str | None = None) -> dict:
        """Render this observation as an `application/om+json` dict
        (the ``ObservationOMJSONInline`` shape).

        :param datastream_id: Optional ID to include as ``datastream@id``
            on the output. The CS API typically supplies this from URL
            context, so it's not required on the model itself.
        """
        from .schema_datamodels import ObservationOMJSONInline
        kwargs = {"result": self.result}
        if datastream_id is not None:
            kwargs["datastream_id"] = datastream_id
        if self.phenomenon_time:
            kwargs["phenomenon_time"] = self.phenomenon_time.get_iso_time()
        if self.result_time:
            kwargs["result_time"] = self.result_time.get_iso_time()
        if self.parameters is not None:
            kwargs["parameters"] = self.parameters
        wrapper = ObservationOMJSONInline(**kwargs)
        return wrapper.model_dump(by_alias=True, exclude_none=True, mode='json')

    def to_swejson_dict(self, schema: "AnyComponent" = None) -> dict:
        """Render this observation as an `application/swe+json` payload
        (the SWE Common JSON encoding of one record).

        SWE+JSON encodes a single observation as a flat JSON object whose
        keys are the schema field names; ``self.result`` is already that
        dict, so this is essentially a passthrough. The optional
        ``schema`` argument is accepted for forward compatibility (when
        we add field-order / encoding-aware emission).
        """
        # ``schema`` reserved for future encoding rules (vector-as-arrays,
        # JSONEncoding handling, etc.); current behavior is passthrough.
        del schema
        return dict(self.result) if self.result is not None else {}

    @classmethod
    def from_omjson_dict(cls, data: dict) -> "ObservationResource":
        """Build an `ObservationResource` from an `application/om+json` dict.

        Parses through `ObservationOMJSONInline` to validate the OM+JSON
        envelope, then strips the ``datastream@id`` / ``foi@id`` envelope
        fields (those live on the surrounding context, not the resource)
        and returns the inner observation.
        """
        from .schema_datamodels import ObservationOMJSONInline
        wrapper = ObservationOMJSONInline.model_validate(data)
        kwargs = {
            "result_time": TimeInstant.from_string(wrapper.result_time),
            "result": wrapper.result,
        }
        if wrapper.phenomenon_time:
            kwargs["phenomenon_time"] = TimeInstant.from_string(wrapper.phenomenon_time)
        if wrapper.parameters is not None:
            kwargs["parameters"] = wrapper.parameters
        return cls(**kwargs)

    @classmethod
    def from_swejson_dict(cls, data: dict, schema: "AnyComponent" = None,
                          result_time: str | None = None) -> "ObservationResource":
        """Build an `ObservationResource` from an `application/swe+json`
        observation payload.

        SWE+JSON observations don't carry an envelope (no ``resultTime`` /
        ``phenomenonTime`` fields); pass ``result_time`` explicitly when
        you have it, otherwise the current UTC time is used.

        :param data: The flat SWE+JSON record dict.
        :param schema: Optional schema, reserved for future per-field
            type coercion. Currently ignored.
        :param result_time: ISO 8601 timestamp for ``resultTime``;
            defaults to ``TimeInstant.now_as_time_instant().isoformat()``
            if omitted.
        """
        del schema  # future use
        rt = TimeInstant.from_string(result_time) if result_time is not None else TimeInstant.now_as_time_instant()
        return cls(result_time=rt, result=dict(data))


class ControlStreamResource(BaseModel):
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    cs_id: str = Field(None, alias="id")
    name: str = Field(...)
    description: str = Field(None)
    valid_time: TimePeriod = Field(None, alias="validTime")
    input_name: str = Field(None, alias="inputName")
    procedure_link: Link = Field(None, alias="procedureLink@link")
    deployment_link: Link = Field(None, alias="deploymentLink@link")
    feature_of_interest_link: Link = Field(None, alias="featureOfInterest@link")
    sampling_feature_link: Link = Field(None, alias="samplingFeature@link")
    issue_time: TimePeriod = Field(None, alias="issueTime")
    execution_time: TimePeriod = Field(None, alias="executionTime")
    live: bool = Field(None)
    asynchronous: bool = Field(True, alias="async")
    command_schema: SerializeAsAny[CommandSchema] = Field(None, alias="schema")
    links: List[Link] = Field(None)

    def to_csapi_dict(self) -> dict:
        """Render this control stream as the CS API `application/json`
        resource body. The embedded ``schema`` field is dumped
        polymorphically per whichever variant
        (`SWEJSONCommandSchema` / `JSONCommandSchema`) it holds.
        """
        return self.model_dump(by_alias=True, exclude_none=True, mode='json')

    def to_csapi_json(self) -> str:
        """JSON-string variant of `to_csapi_dict`."""
        return json.dumps(self.to_csapi_dict())

    @classmethod
    def from_csapi_dict(cls, data: dict) -> "ControlStreamResource":
        """Build a `ControlStreamResource` from a CS API control-stream dict
        (e.g., a server response body or an entry from a /controlstreams
        listing)."""
        return cls.model_validate(data, by_alias=True)
