
#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/5/28
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from enum import Enum


def is_defined(v):
    return v is not None


class Mode(Enum):
    REPLAY = "replay"
    BATCH = "batch"
    REAL_TIME = "realTime"
