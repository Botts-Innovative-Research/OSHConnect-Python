from enum import Enum


def is_defined(v):
    return v is not None


class Mode(Enum):
    REPLAY = "replay"
    BATCH = "batch"
    REAL_TIME = "realTime"

class DataSourceHandler:
    datasources
