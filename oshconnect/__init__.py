#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

import base64
from dataclasses import dataclass
from enum import Enum


@dataclass(kw_only=True)
class Endpoints:
    root: str = "sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


class TemporalModes(Enum):
    REAL_TIME = "realtime"
    ARCHIVE = "archive"
    BATCH = "batch"
    RT_SYNC = "realtimesync"
    ARCHIVE_SYNC = "archivesync"


class Utilities:

    @staticmethod
    def convert_auth_to_base64(username: str, password: str) -> str:
        return base64.b64encode(f"{username}:{password}".encode()).decode()
