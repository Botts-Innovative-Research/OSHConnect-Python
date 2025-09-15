from pydantic import BaseModel, Field, field_validator, SerializeAsAny

from oshconnect.csapi4py.constants import ObservationFormat
from oshconnect.datamodels.encoding import Encoding
from oshconnect.datamodels.swe_components import AnyComponentSchema


class DatastreamSchema(BaseModel):
    """
    A class to represent the schema of a datastream
    """
    obs_format: str = Field(..., serialization_alias='obsFormat')


class SWEDatastreamSchema(DatastreamSchema):
    encoding: SerializeAsAny[Encoding] = Field(...)
    record_schema: SerializeAsAny[AnyComponentSchema] = Field(..., serialization_alias='recordSchema')

    @field_validator('obs_format')
    @classmethod
    def check_check_obs_format(cls, v):
        if v not in [ObservationFormat.SWE_JSON.value, ObservationFormat.SWE_CSV.value,
                     ObservationFormat.SWE_TEXT.value, ObservationFormat.SWE_BINARY.value]:
            raise ValueError('obsFormat must be on of the SWE formats')
        return v
