import time
from datetime import datetime, timezone
from enum import Enum
from conSys4Py.part_1 import systems


class TemporalMode(Enum):
    REAL_TIME = 0
    ARCHIVE = 1


class State(Enum):
    UNINITIALIZED = 0
    INTIALIZED = 1
    STOPPED = 2
    BUFFERING = 3
    PLAYING = 4
    FAST_FORWARDING = 5
    REWINDING = 6


class TimeUtils:
    @staticmethod
    def to_epoch_time(a_time: datetime | str) -> float:
        """
        Convert a datetime or string to epoch time
        :param a_time:
        :return:
        """
        if isinstance(a_time, str):
            return time.mktime(datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S").timetuple())
        elif isinstance(a_time, datetime):
            return time.mktime(a_time.timetuple())

    @staticmethod
    def to_utc_time(a_time: float | str) -> datetime:
        """
        Convert epoch time or string to UTC time object
        :param a_time:
        :return:
        """
        if isinstance(a_time, str):
            return datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        elif isinstance(a_time, float):
            return datetime.fromtimestamp(a_time, tz=timezone.utc)

    @staticmethod
    def current_epoch_time():
        """
        Get the current time in epoch format
        :return:
        """
        return time.time()

    @staticmethod
    def current_utc_time() -> datetime:
        """
        Get the current time in UTC timezone
        :return:
        """
        return datetime.now(timezone.utc)

    @staticmethod
    def time_to_iso(a_time: datetime | float) -> str:
        """
        Convert a datetime object to iso format
        :param a_time: datetime object in UTC timezone or epoch time (float)
        :return:
        """
        if isinstance(a_time, float):
            return datetime.fromtimestamp(a_time, tz=timezone.utc).isoformat() + "Z"
        elif isinstance(a_time, datetime):
            return a_time.isoformat() + "Z"


class Time:
    _epoch_time: float

    def __init__(self, epoch_time: float = None, utc_time: datetime = None):
        if epoch_time is not None:
            self._epoch_time = epoch_time
        elif utc_time is not None:
            self._epoch_time = TimeUtils.to_epoch_time(utc_time)

    @property
    def epoch_time(self):
        return self._epoch_time

    @epoch_time.setter
    def epoch_time(self, epoch_time: float):
        if hasattr(self, "_epoch_time"):
            raise AttributeError("Epoch time should not be changed once set")

    def get_epoch_time(self):
        return self._epoch_time

    def get_utc_time(self):
        return TimeUtils.to_utc_time(self._epoch_time)


class IndeterminateTime(Enum):
    NOW = "now"
    # LATEST = "latest"
    # FIRST = "first"


class TimePeriod:
    _start_time: Time | IndeterminateTime
    _end_time: Time | IndeterminateTime

    def __init__(self, start_time: Time | IndeterminateTime, end_time: Time | IndeterminateTime):
        if isinstance(start_time, Time) and isinstance(end_time, Time):
            if start_time.get_epoch_time() > end_time.get_epoch_time():
                raise ValueError("Start time cannot be later than end time")

        if isinstance(start_time, IndeterminateTime) and isinstance(end_time, IndeterminateTime):
            raise ValueError("Start time and end time cannot be indeterminate at the same time")

        self._start_time = start_time
        self._end_time = end_time

    def get_start_time(self):
        return self._start_time

    def get_end_time(self):
        return self._end_time

    def set_start_time(self, start_time: Time):
        self._start_time = start_time

    def set_end_time(self, end_time: Time):
        self._end_time = end_time

    def is_indeterminate_start_time(self):
        return isinstance(self._start_time, IndeterminateTime)

    def is_indeterminate_end_time(self):
        return isinstance(self._end_time, IndeterminateTime)


class Utilities:
    pass
     # @staticmethod
     # def parse_systems_result(result) -> System: