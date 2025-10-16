#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/1
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================

from pydantic import BaseModel, Field

from .csapi4py.constants import GeometryTypes


# TODO: Add specific validations for each type
# TODO: determine if serializing 'shapely' objects gives valid JSON structures from our own serialization
class Geometry(BaseModel):
    """
    A class to represent the geometry of a feature
    """
    type: GeometryTypes = Field(...)
    coordinates: list
    bbox: list = None
