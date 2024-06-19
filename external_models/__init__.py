#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/31
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic._internal import _repr
from shapely import Point
from typing_extensions import Self


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

    def __repr__(self):
        return f'{[self.start, self.end]}'



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
