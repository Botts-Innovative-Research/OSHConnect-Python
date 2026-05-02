#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/9/30
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from __future__ import annotations

import re
from numbers import Real
from typing import Union, Any, Literal, Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator, SerializeAsAny

from .csapi4py.constants import GeometryTypes
from .api_utils import UCUMCode, URI
from .geometry import Geometry

# SWE Common 3 NameToken: basicTypes.json#/$defs/NameToken
_NAME_TOKEN_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_\-]*$")


def check_named(component, location: str) -> None:
    """Validate that a component bound via SoftNamedProperty carries a NameToken `name`."""
    name = getattr(component, "name", None)
    if not name:
        raise ValueError(
            f"{location}: a component bound here must carry `name` (SWE Common 3 SoftNamedProperty)."
        )
    if not _NAME_TOKEN_RE.match(name):
        raise ValueError(
            f"{location}: `name` {name!r} does not match NameToken pattern "
            f"^[A-Za-z][A-Za-z0-9_\\-]*$."
        )


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
    model_config = ConfigDict(populate_by_name=True)
    type: str = Field(...)
    id: str = Field(None)
    # Wire-format flat carrier for SoftNamedProperty.name. Optional on the component
    # itself (per AbstractDataComponent.json); enforced as required by parent
    # binding-context validators (DataRecord/DataChoice/Vector/DataArray/Matrix and
    # the datastream/controlstream schema wrappers in schema_datamodels.py).
    name: str = Field(None)
    label: str = Field(None)
    description: str = Field(None)
    updatable: bool = Field(False)
    optional: bool = Field(False)
    definition: str = Field(None)


class DataRecordSchema(AnyComponentSchema):
    type: Literal["DataRecord"] = "DataRecord"
    # DataRecord.json: fields.minItems = 1
    fields: list["AnyComponent"] = Field(..., min_length=1)

    @model_validator(mode="after")
    def _fields_require_name(self):
        for i, f in enumerate(self.fields):
            check_named(f, f"DataRecord.fields[{i}]")
        return self


class VectorSchema(AnyComponentSchema):
    label: str = Field(...)
    type: Literal["Vector"] = "Vector"
    definition: str = Field(...)
    reference_frame: str = Field(..., alias='referenceFrame')
    local_frame: str = Field(None, alias='localFrame')
    # TODO: VERIFY might need to be moved further down when these are defined
    coordinates: SerializeAsAny[Union[list[CountSchema], list[QuantitySchema], list[TimeSchema]]] = Field(...)

    @model_validator(mode="after")
    def _coordinates_require_name(self):
        for i, c in enumerate(self.coordinates):
            check_named(c, f"Vector.coordinates[{i}]")
        return self


class DataArraySchema(AnyComponentSchema):
    type: Literal["DataArray"] = "DataArray"
    element_count: dict | str | CountSchema = Field(..., alias='elementCount')  # Should type of Count
    element_type: "AnyComponent" = Field(..., alias='elementType')
    encoding: str = Field(...)  # TODO: implement an encodings class
    values: list = Field(None)

    @model_validator(mode="after")
    def _element_type_requires_name(self):
        check_named(self.element_type, "DataArray.elementType")
        return self


class MatrixSchema(AnyComponentSchema):
    type: Literal["Matrix"] = "Matrix"
    element_count: dict | str | CountSchema = Field(..., alias='elementCount')  # Should be type of Count
    # TODO: spec defines Matrix.elementType as a single component (allOf SoftNamedProperty + AnyComponent),
    # not a list. Cardinality fix is out of scope for the name-validator change.
    element_type: list["AnyComponent"] = Field(..., alias='elementType')
    encoding: str = Field(...)  # TODO: implement an encodings class
    values: list = Field(None)
    reference_frame: str = Field(None)
    local_frame: str = Field(None)

    @model_validator(mode="after")
    def _element_type_requires_name(self):
        for i, et in enumerate(self.element_type):
            check_named(et, f"Matrix.elementType[{i}]")
        return self


class DataChoiceSchema(AnyComponentSchema):
    type: Literal["DataChoice"] = "DataChoice"
    updatable: bool = Field(False)
    optional: bool = Field(False)
    choice_value: CategorySchema = Field(..., alias='choiceValue')  # TODO: Might be called "choiceValues"
    items: list["AnyComponent"] = Field(...)

    @model_validator(mode="after")
    def _items_require_name(self):
        for i, item in enumerate(self.items):
            check_named(item, f"DataChoice.items[{i}]")
        return self


class GeometrySchema(AnyComponentSchema):
    label: str = Field(...)
    type: Literal["Geometry"] = "Geometry"
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
    nil_values: list = Field(None, alias='nilValues')
    srs: str = Field(...)
    value: Geometry = Field(None)


class AnySimpleComponentSchema(AnyComponentSchema):
    label: str = Field(...)
    description: str = Field(None)
    type: str = Field(...)
    updatable: bool = Field(False)
    optional: bool = Field(False)
    definition: str = Field(...)
    reference_frame: str = Field(None, alias='referenceFrame')
    axis_id: str = Field(None, alias='axisID')
    quality: list[Annotated[
        Union[QuantitySchema, QuantityRangeSchema, CategorySchema, TextSchema],
        Field(discriminator='type'),
    ]] = Field(None)
    nil_values: list = Field(None, alias='nilValues')
    constraint: Any = Field(None)
    value: Any = Field(None)


class AnyScalarComponentSchema(AnySimpleComponentSchema):
    """
    A base class for all scalar components. The structure is essentially that of AnySimpleComponent
    """
    pass


class BooleanSchema(AnyScalarComponentSchema):
    type: Literal["Boolean"] = "Boolean"
    value: bool = Field(None)


class CountSchema(AnyScalarComponentSchema):
    type: Literal["Count"] = "Count"
    value: int = Field(None)


class QuantitySchema(AnyScalarComponentSchema):
    type: Literal["Quantity"] = "Quantity"
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
    type: Literal["Time"] = "Time"
    value: str = Field(None)
    reference_time: str = Field(None, alias='referenceTime')
    local_frame: str = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class CategorySchema(AnyScalarComponentSchema):
    type: Literal["Category"] = "Category"
    value: str = Field(None)
    code_space: str = Field(None, alias='codeSpace')


class TextSchema(AnyScalarComponentSchema):
    type: Literal["Text"] = "Text"
    value: str = Field(None)


class CountRangeSchema(AnySimpleComponentSchema):
    type: Literal["CountRange"] = "CountRange"
    value: list[int] = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class QuantityRangeSchema(AnySimpleComponentSchema):
    type: Literal["QuantityRange"] = "QuantityRange"
    value: list[Union[float, str]] = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class TimeRangeSchema(AnySimpleComponentSchema):
    type: Literal["TimeRange"] = "TimeRange"
    value: list[str] = Field(None)
    reference_time: str = Field(None, alias='referenceTime')
    local_frame: str = Field(None)
    uom: Union[UCUMCode, URI] = Field(...)


class CategoryRangeSchema(AnySimpleComponentSchema):
    type: Literal["CategoryRange"] = "CategoryRange"
    value: list[str] = Field(None)
    code_space: str = Field(None, alias='codeSpace')


AnyComponent = Annotated[
    Union[
        DataRecordSchema, VectorSchema, DataArraySchema, MatrixSchema,
        DataChoiceSchema, GeometrySchema,
        BooleanSchema, CountSchema, QuantitySchema, TimeSchema,
        CategorySchema, TextSchema,
        CountRangeSchema, QuantityRangeSchema, TimeRangeSchema, CategoryRangeSchema,
    ],
    Field(discriminator="type"),
]
