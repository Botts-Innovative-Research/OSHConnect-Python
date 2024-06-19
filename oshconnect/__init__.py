#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import base64
import uuid
from abc import ABC
from dataclasses import dataclass, field
from enum import Enum
import swecommondm as swe_common
from conSys4Py import APIResourceTypes
from conSys4Py.core.default_api_helpers import APIHelper

from external_models.object_models import System as SystemResource


@dataclass(kw_only=True)
class Endpoints:
    root: str = "sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"




class TemporalModes(Enum):
    REAL_TIME = 0
    ARCHIVE = 1
    BATCH = 2
    RT_SYNC = 3
    ARCHIVE_SYNC = 4
