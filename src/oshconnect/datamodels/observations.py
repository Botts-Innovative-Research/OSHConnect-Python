from datetime import datetime
from typing import Union, List

from pydantic import BaseModel, Field

from oshconnect.datamodels.api_utils import Link


class ObservationOMJSONInline(BaseModel):
    """
    A class to represent an observation in OM-JSON format
    """
    datastream_id: str = Field(None, serialization_alias="datastream@id")
    foi_id: str = Field(None, serialization_alias="foi@id")
    phenomenon_time: str = Field(None, serialization_alias="phenomenonTime")
    result_time: str = Field(datetime.now().isoformat(), serialization_alias="resultTime")
    parameters: dict = Field(None)
    result: Union[int, float, str, dict, list] = Field(...)
    result_links: List[Link] = Field(None, serialization_alias="result@links")
