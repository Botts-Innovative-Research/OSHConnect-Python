from pydantic import BaseModel, Field

from oshconnect.csapi4py.constants import GeometryTypes


# TODO: Add specific validations for each type
# TODO: update to either use third party Geometry definitions or create a more robust definition
class Geometry(BaseModel):
    """
    A class to represent the geometry of a feature
    """
    type: GeometryTypes = Field(...)
    coordinates: list
    bbox: list = None