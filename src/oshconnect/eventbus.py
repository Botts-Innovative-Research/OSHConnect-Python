#  =============================================================================
#  Copyright (c) 2025 Botts Innovative Research Inc.
#  Date: 2025/10/6
#  Author: Ian Patterson
#  Contact Email: ian@botts-inc.com
#  =============================================================================
import collections
from abc import ABC


class EventBus(ABC):
    """
    A base class for an event bus system.
    """
    _deque: collections.deque
