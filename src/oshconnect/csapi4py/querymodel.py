from datetime import datetime
from typing import Union, Optional, List

from pydantic import BaseModel, StrictStr, Field, field_validator


class QueryModel(BaseModel):
    id: list = None
    bbox: list = None
    date_time: Union[StrictStr, datetime] = Field(None, alias='datetime')
    geom: dict = None
    q: list = Optional[List[str]]
    parent: list = None
    procedure: list = None
    foi: list = None
    observed_property: list = Field(None, serialization_alias='observedProperty')
    controlled_property: list = Field(None, serialization_alias='controlledProperty')
    recursive: bool = False
    limit: int = Field(10, ge=1, le=10000)

    @field_validator('q')
    def validate_q(cls, v):
        if v is not None:
            return v.split(',')
        return v
