from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field


class CommandJSON(BaseModel):
    """
    A class to represent a command in JSON format
    """
    control_id: str = Field(None, serialization_alias="control@id")
    issue_time: Union[str, float] = Field(datetime.now().isoformat(), serialization_alias="issueTime")
    sender: str = Field(None)
    params: Union[dict, list, int, float, str] = Field(None)
