from pydantic import BaseModel, Field


class Encoding(BaseModel):
    id: str = Field(None)
    type: str = Field(...)
    vector_as_arrays: bool = Field(False, alias='vectorAsArrays')


class JSONEncoding(Encoding):
    type: str = "JSONEncoding"
