#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
import uuid
from typing import Any, List, Self

from conSys4Py import DatastreamSchema, Geometry
from conSys4Py.datamodels.api_utils import Link
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from shapely import Point

from oshconnect.timemanagement import TimePeriod


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


class DateTime(BaseModel):
    is_instant: bool = Field(True, description="Whether the date time is an instant or a period.")
    iso_date: str = Field(None, description="The ISO formatted date time.")
    time_period: tuple = Field(None, description="The time period of the date time.")

    @model_validator(mode='before')
    def valid_datetime_type(self) -> Self:
        if self.is_instant:
            if self.iso_date is None:
                raise ValueError("Instant date time must have a valid ISO8601 date.")
        return self

    @field_validator('iso_date')
    @classmethod
    def check_iso_date(cls, v) -> str:
        if not v:
            raise ValueError("Instant date time must have a valid ISO8601 date.")
        return v


class TimePeriod(BaseModel):
    start: str = Field(...)
    end: str = Field(...)

    @model_validator(mode='before')
    @classmethod
    def valid_time_period(cls, data) -> Any:
        if isinstance(data, list):
            if data[0] > data[1]:
                raise ValueError("Time period start must be before end.")
            return {"start": data[0], "end": data[1]}
        elif isinstance(data, dict):
            if data['start'] > data['end']:
                raise ValueError("Time period start must be before end.")
            return data

    def __repr__(self):
        return f'{[self.start, self.end]}'

    def does_timeperiod_overlap(self, checked_timeperiod: TimePeriod) -> bool:
        if checked_timeperiod.start < self.end and checked_timeperiod.end > self.start:
            return True
        else:
            return False


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
    valid_time: DateTime = Field(None, serialization_alias="validTime")
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
    phenomenon_time: DateTime = Field(None, serialization_alias="phenomenonTime")
    result_time: DateTime = Field(..., serialization_alias="resultTime")
    parameters: dict = Field(None)
    result: dict = Field(...)
    result_link: Link = Field(None, serialization_alias="result@link")
