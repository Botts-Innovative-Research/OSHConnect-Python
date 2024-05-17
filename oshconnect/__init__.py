from abc import ABC
from dataclasses import dataclass
from enum import Enum
import oshdatacore as swe_common


@dataclass(kw_only=True)
class Endpoints:
    root: str = "/sensorhub"
    sos: str = f"{root}/sos"
    connected_systems: str = f"{root}/api"


@dataclass(kw_only=True)
class Node(ABC):
    address: str
    port: int
    endpoints: Endpoints
    is_secure: bool


class TemporalModes(Enum):
    REAL_TIME = 0
    ARCHIVE = 1
    BATCH = 2
    RT_SYNC = 3
    ARCHIVE_SYNC = 4


