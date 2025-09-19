from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl

from src.oshconnect.datamodels.api_utils import Link, URI
from src.oshconnect.datamodels.geometry import Geometry


class SystemEventOMJSON(BaseModel):
    """
    A class to represent the schema of a system event
    """
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
    type: str = Field(...)
    id: str = Field(None)
    properties: SystemHistoryProperties = Field(...)
    geometry: Geometry = Field(None)
    bbox: list = Field(None)
    links: list[Link] = Field(None)


class SystemHistoryProperties(BaseModel):
    feature_type: str = Field(...)
    uid: URI = Field(...)
    name: str = Field(...)
    description: str = Field(None)
    asset_type: str = Field(None)
    valid_time: list = Field(None)
    parent_system_link: str = Field(None, serialization_alias='parentSystem@link')
    procedure_link: str = Field(None, serialization_alias='procedure@link')
