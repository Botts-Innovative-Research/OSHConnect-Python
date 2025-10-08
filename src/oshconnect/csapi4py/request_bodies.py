from typing import Union

from pydantic import BaseModel, HttpUrl, Field, model_serializer, RootModel, SerializeAsAny

from .constants import DatastreamResultTypes
from oshconnect.datamodels.datastreams import DatastreamSchema
from oshconnect.datamodels.geometry import Geometry
from .sensor_ml.sml import TypeOf


# TODO: Consider some sort of Abstract Base Class for all valid request bodies to inherit from to reduce the complexity
#  of the final request body.

class GeoJSONBody(BaseModel):
    type: str
    id: str
    properties: dict = None
    geometry: Geometry = None
    bbox: list = None
    links: list = None


class SmlJSONBody(BaseModel):
    object_type: str = Field(None, serialization_alias='type')
    id: str = Field(None)
    description: str = Field(None)
    unique_id: str = Field(..., serialization_alias='uniqueId')
    label: str = Field(...)
    lang: str = None
    keywords: list = None
    identifiers: list = None
    classifiers: list = None
    valid_time: list = Field(None, serialization_alias='validTime')
    security_constraints: list = Field(None, serialization_alias='securityConstraints')
    legal_constraints: list = Field(None, serialization_alias='legalConstraints')
    characteristics: list = None
    capabilities: list = None
    contacts: list = None
    documents: list = None
    history: list = None
    definition: HttpUrl = None
    type_of: TypeOf = Field(None, serialization_alias='typeOf')
    configuration: HttpUrl = None
    features_of_interest: list = Field(None, serialization_alias='featuresOfInterest')
    inputs: list = None
    outputs: list = None
    parameters: list = None
    modes: list = None
    method: str = None
    position: list = None
    links: list = Field(None)


class OMJSONBody(BaseModel):
    datastream_id: str = Field(None, alias="datastream@id")
    foi_id: str = Field(None, alias="foi@id")
    phenomenon_time: str = Field(None, alias="phenomenonTime")
    result_time: str = Field(None, alias="resultTime")
    parameters: list = Field(None)
    result: dict = Field(None)
    result_links: list = Field(None, alias="result@links")


class DatastreamBodyJSON(BaseModel):
    """
    NOTES: though the spec does not require that outputName, and schema be present, they are required for the
    implementation of the API present on OSH
    """
    id: str = Field(None)
    name: str = Field(...)
    description: str = Field(None)
    deployment: HttpUrl = Field(None, serialization_alias='deployment@link')
    ultimate_feature_of_interest: HttpUrl = Field(None, serialization_alias='featureOfInterest@link')
    sampling_feature: HttpUrl = Field(None, serialization_alias='samplingFeature@link')
    valid_time: list = Field(None, serialization_alias='validTime')
    output_name: str = Field(..., serialization_alias='outputName')
    phenomenon_time_interval: str = Field(None, serialization_alias='phenomenonTimeInterval')
    result_time_interval: str = Field(None, serialization_alias='resultTimeInterval')
    result_type: DatastreamResultTypes = Field(None, serialization_alias='resultType')
    links: list = Field(None)
    datastream_schema: SerializeAsAny[DatastreamSchema] = Field(..., serialization_alias='schema')


class RequestBody(BaseModel):
    """
    Wrapper class to support different request json structures
    """
    json_structure: Union[GeoJSONBody, SmlJSONBody, OMJSONBody, DatastreamSchema] = Field(...,
                                                                                          serialization_alias='json')
    test_extra: str = Field("Hello, I am test", serialization_alias='testExtra')

    @model_serializer
    def ser_model(self):
        print("Serializing model...")
        return self.json_structure


class RequestBodyList(RootModel):
    root: list[Union[GeoJSONBody, SmlJSONBody, OMJSONBody, DatastreamSchema]] = Field(...)
