#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from typing import Callable

from pydantic import BaseModel


class VisualizationDataLayer(BaseModel):
    """
    Represents the data portion of a particular visualization as well as its state information.
    """

    name: str
    description: str
    datasource_id: list[str]
    visible: bool
    timestamp: float
    _get_timestamp: Callable
    on_left_click: Callable
    on_right_click: Callable
    on_hover: Callable
    _id: str
    _type: str
    _filter: dict
    _datasources_to_fn: Callable
    _no_datasources_fn: Callable


class Styling:

    def __init__(self):
        pass
