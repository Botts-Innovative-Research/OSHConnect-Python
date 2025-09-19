from __future__ import annotations

from typing import Union

from pydantic import BaseModel, Field, SerializeAsAny

from src.oshconnect.datamodels.encoding import Encoding
from src.oshconnect.datamodels.swe_components import AnyComponentSchema


class ControlStreamJSONSchema(BaseModel):
    """
    A class to represent the schema of a control stream
    """
    id: str = Field(None)
    name: str = Field(...)
    description: str = Field(None)
    deployment_link: str = Field(None, serialization_alias='deployment@link')
    ultimate_feature_of_interest_link: str = Field(None, serialization_alias='ultimateFeatureOfInterest@link')
    sampling_feature_link: str = Field(None, alias='samplingFeature@link')
    valid_time: list = Field(None, serialization_alias='validTime')
    input_name: str = Field(None, serialization_alias='inputName')
    links: list = Field(None)
    control_stream_schema: SerializeAsAny[Union[SWEControlChannelSchema, JSONControlChannelSchema]] = Field(...,
                                                                                                            serialization_alias='schema')


class SWEControlChannelSchema(BaseModel):
    """
    A class to represent the schema of a control channel
    """
    command_format: str = Field("application/swe+json", serialization_alias='commandFormat')
    encoding: SerializeAsAny[Encoding] = Field(...)
    record_schema: SerializeAsAny[AnyComponentSchema] = Field(..., serialization_alias='recordSchema')


class JSONControlChannelSchema(BaseModel):
    command_format: str = Field("application/cmd+json", serialization_alias='commandFormat')
    params_schema: SerializeAsAny[AnyComponentSchema] = Field(..., serialization_alias='paramsSchema')
