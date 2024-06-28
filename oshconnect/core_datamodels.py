#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from __future__ import annotations

import uuid
from typing import List

from conSys4Py import DatastreamSchema, Geometry
from conSys4Py.datamodels.api_utils import Link
from pydantic import BaseModel, ConfigDict, Field
from shapely import Point

from oshconnect.timemanagement import DateTimeSchema, TimePeriod


class BoundingBox(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

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


class SystemResource(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    feature_type: str = Field(None, serialization_alias="type")
    system_id: str = Field(None, serialization_alias="id")
    properties: dict = Field(None)
    geometry: Geometry | None = Field(None)
    bbox: BoundingBox = Field(None)
    links: List[Link] = Field(None)
    description: str = Field(None)
    uid: uuid.UUID = Field(None)
    label: str = Field(None)
    lang: str = Field(None)
    keywords: List[str] = Field(None)
    identifiers: List[str] = Field(None)
    classifiers: List[str] = Field(None)
    valid_time: DateTimeSchema = Field(None, serialization_alias="validTime")
    security_constraints: List[SecurityConstraints] = Field(None, serialization_alias="securityConstraints")
    legal_constraints: List[LegalConstraints] = Field(None, serialization_alias="legalConstraints")
    characteristics: List[Characteristics] = Field(None)
    capabilities: List[Capabilities] = Field(None)
    contacts: List[Contact] = Field(None)
    documentation: List[Documentation] = Field(None)
    history: List[HistoryEvent] = Field(None)
    definition: str = Field(None)
    type_of: str = Field(None, serialization_alias="typeOf")
    configuration: ConfigurationSettings = Field(None)
    features_of_interest: List[FeatureOfInterest] = Field(None, alias="featuresOfInterest")
    inputs: List[Input] = Field(None)
    outputs: List[Output] = Field(None)
    parameters: List[Parameter] = Field(None)
    modes: List[Mode] = Field(None)
    method: ProcessMethod = Field(None)


class DatastreamResource(BaseModel):
    # model_config = ConfigDict(populate_by_name=True)

    ds_id: str = Field(..., alias="id")
    name: str = Field(...)
    description: str = Field(None)
    valid_time: TimePeriod = Field(..., alias="validTime")
    output_name: str = Field(None, alias="outputName")
    procedure_link: Link = Field(None, alias="procedureLink@link")
    deployment_link: Link = Field(None, alias="deploymentLink@link")
    ultimate_feature_of_interest_link: Link = Field(None, alias="ultimateFeatureOfInterest@link")
    sampling_feature_link: Link = Field(None, alias="samplingFeature@link")
    parameters: dict = Field(None)
    phenomenon_time: TimePeriod = Field(None, alias="phenomenonTimeInterval")
    result_time: TimePeriod = Field(None, alias="resultTimeInterval")
    ds_type: str = Field(None, alias="type")
    result_type: str = Field(None, alias="resultType")
    links: List[Link] = Field(None)
    schema: DatastreamSchema = Field(None)


class Observation(BaseModel):
    sampling_feature_id: str = Field(None, serialization_alias="samplingFeature@Id")
    procedure_link: Link = Field(None, serialization_alias="procedure@link")
    phenomenon_time: DateTimeSchema = Field(None, serialization_alias="phenomenonTime")
    result_time: DateTimeSchema = Field(..., serialization_alias="resultTime")
    parameters: dict = Field(None)
    result: dict = Field(...)
    result_link: Link = Field(None, serialization_alias="result@link")
