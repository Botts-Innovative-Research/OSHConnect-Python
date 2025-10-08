#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

from numbers import Real
from typing import Union, Any

from pydantic import BaseModel, Field, field_validator, SerializeAsAny

from .csapi4py.constants import GeometryTypes
from .api_utils import UCUMCode, URI
from .geometry import Geometry

"""
 NOTE: The following classes are used to represent the Record Schemas that are required for use with Datastreams
The names are likely to change to include a "Schema" suffix to differentiate them from the actual data structures.
The current scope of the project likely excludes conversion from received data to actual SWE Common data structures,
in the event this is added it will most likely be in a separate module as those structures have use cases outside of
the API solely
"""


# TODO: Add field validators that are missing
# TODO: valid string fields that are intended to represent time/date values
# TODO: Validate places where string fields are not allowed to be empty


class AnyComponentSchema(BaseModel):
    type: str = Field(...)
    id: str = Field(None)
    label: str = Field(None)
    description: str = Field(None)
    updatable: bool = Field(False)
    optional: bool = Field(False)
    definition: str = Field(None)


class DataRecordSchema(AnyComponentSchema):
    type: str = "DataRecord"
    fields: SerializeAsAny[list[AnyComponentSchema]] = Field(...)


class VectorSchema(AnyComponentSchema):
    label: str = Field(...)
    name: str = Field(...)
    type: str = "Vector"
    definition: str = Field(...)
    reference_frame: str = Field(...)
    local_frame: str = Field(None)
    # TODO: VERIFY might need to be moved further down when these are defined
    coordinates: SerializeAsAny[Union[list[CountSchema], list[QuantitySchema], list[TimeSchema]]] = Field(...)


class DataArraySchema(AnyComponentSchema):
    type: str = "DataArray"
    name: str = Field(...)
    element_count: dict | str | CountSchema = Field(..., serialization_alias='elementCount')  # Should type of Count
    element_type: SerializeAsAny[list[AnyComponentSchema]] = Field(..., serialization_alias='elementType')
    encoding: str = Field(...)  # TODO: implement an encodings class
    values: list = Field(None)


class MatrixSchema(AnyComponentSchema):
    type: str = "Matrix"
    element_count: dict | str | CountSchema = Field(..., serialization_alias='elementCount')  # Should be type of Count
    element_type: SerializeAsAny[list[AnyComponentSchema]] = Field(..., serialization_alias='elementType')
    encoding: str = Field(...)  # TODO: implement an encodings class
    values: list = Field(None)
    reference_frame: str = Field(None)
    local_frame: str = Field(None)


class DataChoiceSchema(AnyComponentSchema):
    type: str = "DataChoice"
    updatable: bool = Field(False)
    optional: bool = Field(False)
    choice_value: CategorySchema = Field(..., serialization_alias='choiceValue')  # TODO: Might be called "choiceValues"
    items: SerializeAsAny[list[AnyComponentSchema]] = Field(...)


class GeometrySchema(AnyComponentSchema):
    label: str = Field(...)
    type: str = "Geometry"
    updatable: bool = Field(False)
    optional: bool = Field(False)
    definition: str = Field(...)
    constraint: dict = Field(default_factory=lambda: {
        'geomTypes': [
            GeometryTypes.POINT.value,
            GeometryTypes.LINESTRING.value,
            GeometryTypes.POLYGON.value,
            GeometryTypes.MULTI_POINT.value,
            GeometryTypes.MULTI_LINESTRING.value,
            GeometryTypes.MULTI_POLYGON.value
        ]
    })
    nil_values: list = Field(None, serialization_alias='nilValues')
    srs: str = Field(...)
    value: Geometry = Field(None)


class AnySimpleComponentSchema(AnyComponentSchema):
    label: str = Field(...)
    description: str = Field(None)
    type: str = Field(...)
    updatable: bool = Field(False)
    optional: bool = Field(False)
    definition: str = Field(...)
    reference_frame: str = Field(None, serialization_alias='referenceFrame')
    axis_id: str = Field(None, serialization_alias='axisID')
    quality: Union[list[QuantitySchema], list[QuantityRangeSchema], list[CategorySchema], list[TextSchema]] = Field(
        None)  # TODO: Union[Quantity, QuantityRange, Category, Text]
    nil_values: list = Field(None, serialization_alias='nilValues')
    constraint: Any = Field(None)
    value: Any = Field(None)
    name: str = Field(...)


class AnyScalarComponentSchema(AnySimpleComponentSchema):
    """
    A base class for all scalar components. The structure is essentially that of AnySimpleComponent
    """
    pass


class BooleanSchema(AnyScalarComponentSchema):
    type: str = "Boolean"
    value: bool = Field(None)


class CountSchema(AnyScalarComponentSchema):
    type: str = "Count"
    value: int = Field(None)


class QuantitySchema(AnyScalarComponentSchema):
    type: str = "Quantity"
    value: Union[float, str] = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)

    @field_validator('value')
    @classmethod
    def validate_value(cls, v):
        if isinstance(v, Real):
            return v
        elif isinstance(v, str):
            if v in ['NaN', 'INFINITY', '+INFINITY', '-INFINITY']:
                return v
            else:
                raise ValueError(
                    'string representation of value must be one of the following: NaN, INFINITY, +INFINITY, -INFINITY')
        else:
            try:
                return float(v)
            except ValueError:
                raise ValueError('value must be a number or a string representing a special value '
                                 '[NaN, INFINITY, +INFINITY, -INFINITY]')


class TimeSchema(AnyScalarComponentSchema):
    type: str = "Time"
    value: str = Field(None)
    reference_time: str = Field(None, serialization_alias='referenceTime')
    local_frame: str = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class CategorySchema(AnyScalarComponentSchema):
    type: str = "Category"
    value: str = Field(None)
    code_space: str = Field(None, serialization_alias='codeSpace')


class TextSchema(AnyScalarComponentSchema):
    type: str = "Text"
    value: str = Field(None)


class CountRangeSchema(AnySimpleComponentSchema):
    type: str = "CountRange"
    value: list[int] = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class QuantityRangeSchema(AnySimpleComponentSchema):
    type: str = "QuantityRange"
    value: list[Union[float, str]] = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class TimeRangeSchema(AnySimpleComponentSchema):
    type: str = "TimeRange"
    value: list[str] = Field(None)
    reference_time: str = Field(None, serialization_alias='referenceTime')
    local_frame: str = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class CategoryRangeSchema(AnySimpleComponentSchema):
    type: str = "CategoryRange"
    value: list[str] = Field(None)
    code_space: str = Field(None, serialization_alias='codeSpace')
