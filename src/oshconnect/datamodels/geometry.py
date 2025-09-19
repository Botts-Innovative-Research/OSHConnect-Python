from pydantic import BaseModel, Field

from src.oshconnect.csapi4py.constants import GeometryTypes


# TODO: Add specific validations for each type
# TODO: determine if serializing 'shapely' objects gives valid JSON structures from our own serialization
class Geometry(BaseModel):
    """
    A class to represent the geometry of a feature
    """
    type: GeometryTypes = Field(...)
    coordinates: list
    bbox: list = None
