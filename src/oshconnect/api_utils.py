from __future__ import annotations
from pydantic import BaseModel, Field, HttpUrl, field_validator


class Link(BaseModel):
    href: HttpUrl = Field(...)
    rel: str = Field(None)
    type: str = Field(None)
    hreflang: str = Field(None)
    title: str = Field(None)
    uid: URI = Field(None)
    rt: URI = Field(None)
    interface: URI = Field(None, serialization_alias='if')


class UCUMCode(BaseModel):
    code: str = Field(...)
    label: str = Field(None)

    @field_validator('code', 'label')
    @classmethod
    def validate_string_fields(cls, v):
        if isinstance(v, str) and len(v) > 0:
            return v
        else:
            raise ValueError('code and label must be strings of length > 0')


class URI(BaseModel):
    href: HttpUrl = Field(...)
    label: str = Field(None)

    @field_validator('label')
    @classmethod
    def validate_label(cls, v):
        if isinstance(v, str) and len(v) > 0:
            return v
        else:
            raise ValueError('label must be strings of length > 0')
