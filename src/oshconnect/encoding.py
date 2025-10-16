from pydantic import BaseModel, Field, ConfigDict


class Encoding(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    id: str = Field(None)
    type: str = Field(...)
    vector_as_arrays: bool = Field(False, alias='vectorAsArrays')


class JSONEncoding(Encoding):
    type: str = "JSONEncoding"
