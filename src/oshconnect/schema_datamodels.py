#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
from __future__ import annotations

from datetime import datetime
from typing import Union, List

from pydantic import BaseModel, Field, SerializeAsAny, field_validator, HttpUrl, ConfigDict

from .api_utils import Link, URI
from .csapi4py.constants import ObservationFormat
from .encoding import Encoding
from .geometry import Geometry
from .swe_components import AnyComponentSchema

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
    issue_time: Union[str, float] = Field(datetime.now().isoformat(), serialization_alias="issueTime")
    sender: str = Field(None)
    params: Union[dict, list, int, float, str] = Field(None)


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

    command_format: str = Field("application/swe+json", alias='commandFormat')
    encoding: SerializeAsAny[Encoding] = Field(...)
    record_schema: SerializeAsAny[AnyComponentSchema] = Field(..., serialization_alias='recordSchema')


class JSONCommandSchema(CommandSchema):
    """
    JSON command schema
    """
    model_config = ConfigDict(populate_by_name=True)

    command_format: str = Field("application/json", alias='commandFormat')
    params_schema: SerializeAsAny[AnyComponentSchema] = Field(..., alias='parametersSchema')
    result_schema: SerializeAsAny[AnyComponentSchema] = Field(None, alias='resultSchema')
    feasibility_schema: SerializeAsAny[AnyComponentSchema] = Field(None, alias='feasibilityResultSchema')


class DatastreamRecordSchema(BaseModel):
    """
    A class to represent the schema of a datastream
    """
    model_config = ConfigDict(populate_by_name=True)

    obs_format: str = Field(..., alias='obsFormat')


class SWEDatastreamRecordSchema(DatastreamRecordSchema):
    model_config = ConfigDict(populate_by_name=True)
    encoding: SerializeAsAny[Encoding] = Field(...)
    record_schema: SerializeAsAny[AnyComponentSchema] = Field(..., serialization_alias='recordSchema')

    @field_validator('obs_format')
    @classmethod
    def check_check_obs_format(cls, v):
        if v not in [ObservationFormat.SWE_JSON.value, ObservationFormat.SWE_CSV.value,
                     ObservationFormat.SWE_TEXT.value, ObservationFormat.SWE_BINARY.value]:
            raise ValueError('obsFormat must be on of the SWE formats')
        return v


class ObservationOMJSONInline(BaseModel):
    """
    A class to represent an observation in OM-JSON format
    """
    model_config = ConfigDict(populate_by_name=True)
    datastream_id: str = Field(None, serialization_alias="datastream@id")
    foi_id: str = Field(None, serialization_alias="foi@id")
    phenomenon_time: str = Field(None, serialization_alias="phenomenonTime")
    result_time: str = Field(datetime.now().isoformat(), serialization_alias="resultTime")
    parameters: dict = Field(None)
    result: Union[int, float, str, dict, list] = Field(...)
    result_links: List[Link] = Field(None, serialization_alias="result@links")


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
