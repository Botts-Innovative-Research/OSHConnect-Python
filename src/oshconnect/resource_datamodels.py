#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict, Field, SerializeAsAny, model_validator
from shapely import Point

from .api_utils import Link
from .geometry import Geometry
from .schema_datamodels import DatastreamRecordSchema, CommandSchema
from .timemanagement import TimeInstant, TimePeriod


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
    phenomenon_time: TimePeriod = Field(None, alias="phenomenonTimeInterval")
    result_time: TimePeriod = Field(None, alias="resultTimeInterval")
    ds_type: str = Field(None, alias="type")
    result_type: str = Field(None, alias="resultType")
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


class ObservationResource(BaseModel):
    model_config = ConfigDict(populate_by_name=True, arbitrary_types_allowed=True)

    sampling_feature_id: str = Field(None, alias="samplingFeature@Id")
    procedure_link: Link = Field(None, alias="procedure@link")
    phenomenon_time: TimeInstant = Field(None, alias="phenomenonTime")
    result_time: TimeInstant = Field(..., alias="resultTime")
    parameters: dict = Field(None)
    result: dict = Field(...)
    result_link: Link = Field(None, alias="result@link")


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
