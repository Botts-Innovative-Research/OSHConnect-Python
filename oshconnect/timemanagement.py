#   ==============================================================================
#   Copyright (c) 2024 Botts Innovative Research, Inc.
#   Date:  2024/6/26
#   Author:  Ian Patterson
#   Contact Email:  ian@botts-inc.com
#   ==============================================================================

from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer, model_validator


class TemporalModes(Enum):
    REAL_TIME = "realtime"
    ARCHIVE = "archive"
    BATCH = "batch"
    RT_SYNC = "realtimesync"
    ARCHIVE_SYNC = "archivesync",


class State(Enum):
    UNINITIALIZED = 0
    INITIALIZED = 1
    STOPPED = 2
    BUFFERING = 3
    PLAYING = 4
    FAST_FORWARDING = 5
    REWINDING = 6


class TimeUtils:
    iso_format = '%Y-%m-%dT%H:%M:%S.%fZ'

    @staticmethod
    def to_epoch_time(a_time: datetime | str) -> float:
        """
        Convert a datetime or string to epoch time
        :param a_time:
        :return:
        """
        if isinstance(a_time, str):
            return time.mktime(
                datetime.strptime(a_time, "%Y-%m-%d %H:%M:%S.%fZ").timetuple())
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
            if re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.)(\d+)(Z)', a_time):
                return datetime.strptime(a_time, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            else:
                return datetime.strptime(a_time, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
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
            return datetime.fromtimestamp(a_time).strftime(TimeUtils.iso_format)
        elif isinstance(a_time, datetime):
            return a_time.strftime(TimeUtils.iso_format)

    @staticmethod
    def compare_time_instants_or_indeterminate(time1: TimeInstant | str, time2: TimeInstant | str) -> int:
        """
        Compare two time instants or indeterminate times. This coerces the indeterminate time 'now' to the current time.
        This may cause unexpected behavior if the times are very close together.
        :param time1: TimeInstant or IndeterminateTime
        :param time2: TimeInstant or IndeterminateTime
        :return: 0 if equal, -1 if time1 < time2, 1 if time1 > time2
        """
        if isinstance(time1, TimeInstant) and isinstance(time2, TimeInstant):
            if time1 < time2:
                return -1
            elif time1 > time2:
                return 1
            else:
                return 0
        elif isinstance(time1, TimeInstant) and isinstance(time2, str):
            if time2 == "now":
                now_as_time = TimeInstant.now_as_time_instant()
                return TimeUtils.compare_time_instants_or_indeterminate(time1, now_as_time)
            else:
                t2_as_time = TimeInstant.from_string(time2)
                return TimeUtils.compare_time_instants_or_indeterminate(time1, t2_as_time)
        elif isinstance(time1, str) and isinstance(time2, TimeInstant):
            if time1 == "now":
                now_as_ti = TimeInstant.now_as_time_instant()
                return TimeUtils.compare_time_instants_or_indeterminate(now_as_ti, time2)
            else:
                t1_as_ti = TimeInstant.from_string(time1)
                return TimeUtils.compare_time_instants_or_indeterminate(t1_as_ti, time2)
        elif isinstance(time1, str) and isinstance(time2, str):
            if time1 == "now" and time2 == "now":
                raise ValueError("Both times cannot be 'now'")
            elif time1 == "now":
                t1_as_ti = TimeInstant.now_as_time_instant()
                t2_as_ti = TimeInstant.from_string(time2)
            elif time2 == "now":
                t2_as_ti = TimeInstant.now_as_time_instant()
                t1_as_ti = TimeInstant.from_string(time1)
            else:
                t1_as_ti = TimeInstant.from_string(time1)
                t2_as_ti = TimeInstant.from_string(time2)
            return TimeUtils.compare_time_instants_or_indeterminate(t1_as_ti, t2_as_ti)


class TimeInstant:
    _epoch_time: float | None

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

    def has_epoch_time(self):
        return self._epoch_time is not None

    def get_utc_time(self):
        return TimeUtils.to_utc_time(self._epoch_time)

    def get_iso_time(self):
        return TimeUtils.time_to_iso(self._epoch_time)

    @staticmethod
    def from_string(utc_time: str):
        # TODO: handle timezones
        if re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.)(\d+)(Z)', utc_time):
            dt = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            dt = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")
        return TimeInstant(utc_time=dt)

    @staticmethod
    def now_as_time_instant():
        return TimeInstant(epoch_time=TimeUtils.current_epoch_time())

    def __lt__(self, other: TimeInstant) -> bool:
        return self.epoch_time < other.epoch_time

    def __gt__(self, other: TimeInstant) -> bool:
        return self.epoch_time > other.epoch_time

    def __eq__(self, other: TimeInstant) -> bool:
        return self.epoch_time == other.epoch_time

    def __le__(self, other: TimeInstant) -> bool:
        return self.epoch_time <= other.epoch_time

    def __ge__(self, other: TimeInstant) -> bool:
        return self.epoch_time >= other.epoch_time

    def __ne__(self, other: TimeInstant) -> bool:
        return self.epoch_time != other.epoch_time

    def __repr__(self):
        return f'{self.get_iso_time()}'


class DateTimeSchema(BaseModel):
    is_instant: bool = Field(True, description="Whether the date time is an instant or a period.")
    iso_date: str = Field(None, description="The ISO formatted date time.")
    time_period: tuple = Field(None, description="The time period of the date time.")

    @model_validator(mode='before')
    def valid_datetime_type(self) -> Self:
        if self.is_instant:
            if self.iso_date is None:
                raise ValueError("Instant date time must have a valid ISO8601 date.")
        return self

    @field_validator('iso_date')
    @classmethod
    def check_iso_date(cls, v) -> str:
        if not v:
            raise ValueError("Instant date time must have a valid ISO8601 date.")
        return v


class IndeterminateTime(Enum):
    NOW = "now"
    LATEST = "latest"
    FIRST = "first"


class TimePeriod(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    start: TimeInstant | str = Field(...)
    end: TimeInstant | str = Field(...)

    @model_validator(mode='before')
    @classmethod
    def valid_time_period(cls, data) -> Any:
        data_dict = {}

        if isinstance(data, list):
            data_dict['start'] = cls.check_mbr_type(data[0])
            data_dict['end'] = cls.check_mbr_type(data[1])
        elif isinstance(data, dict):
            data_dict['start'] = cls.check_mbr_type(data['start'])
            data_dict['end'] = cls.check_mbr_type(data['end'])

        if not cls.compare_start_lt_end(data_dict['start'], data_dict['end']):
            raise ValueError("Start time must be less than end time")

        return data_dict

    @model_serializer
    def ser_model(self):
        return [str(self.start), str(self.end)]

    @staticmethod
    def check_mbr_type(value):
        if isinstance(value, str):
            if value == "now":
                return value
            else:
                tp = TimeInstant.from_string(value)
                return tp
        elif isinstance(value, TimeInstant):
            return value

    @classmethod
    def compare_start_lt_end(cls, start: TimeInstant | str, end: TimeInstant | str) -> bool:
        if isinstance(start, TimeInstant) and isinstance(end, TimeInstant):
            return start < end
        elif isinstance(start, str) and isinstance(end, str):
            if start == "now" and end == "now":
                raise ValueError("Start and end cannot both be 'now'")
            else:
                raise ValueError("Check the time strings for validity. This should not occur.")
        elif isinstance(start, str) and start == "now" and isinstance(end, TimeInstant):
            return TimeInstant.now_as_time_instant() < end
        elif isinstance(start, TimeInstant) and isinstance(end, str) and end == "now":
            return start < TimeInstant.now_as_time_instant()

    def __repr__(self):
        return f'{[self.start, self.end]}'

    def does_timeperiod_overlap(self, checked_timeperiod: TimePeriod) -> bool:
        """
        Checks if the provided TimePeriod overlaps with the TimePeriod instance.

        **Note**: This method does not check for some edge cases, but the TimePeriods *should never* be valid
        in those situations.

        :param checked_timeperiod:
        :return: True if the TimePeriods overlap, False otherwise
        """
        # check that start of checked is not after end of this instance
        start_check_lt_end_inst = TimeUtils.compare_time_instants_or_indeterminate(checked_timeperiod.start, self.end)
        # check that end of checked is not before start of this instance
        end_check_gt_start_inst = TimeUtils.compare_time_instants_or_indeterminate(self.end, checked_timeperiod.start)
        if start_check_lt_end_inst == -1 and end_check_gt_start_inst == 1:
            return True


class Utilities:
    pass
    # @staticmethod
    # def parse_systems_result(result) -> System:


class TimeManagement:
    time_range: TimePeriod
    time_controller: TimeController

    def __init__(self, time_range: TimePeriod):
        self.time_range = time_range
        self.time_controller = TimeController()

    def get_time_range(self):
        return self.time_range


class TemporalMode:
    pass


class TimeController:
    _instance = None
    _temporal_mode: TemporalMode
    _status: str
    _playback_speed: int
    _timeline_begin: TimeInstant
    _timeline_end: TimeInstant
    _current_time: TimeInstant

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(TimeController, cls).__new__(cls)
        return cls._instance

    def set_temporal_mode(self, mode: TemporalMode):
        self._temporal_mode = mode

    def get_temporal_mode(self):
        return self._temporal_mode

    def start(self):
        pass

    def pause(self):
        pass

    def stop(self):
        pass

    def fast_forward(self, speed: int):
        self._playback_speed = speed

    def rewind(self, speed: int):
        self._playback_speed = speed

    def skip(self, a_time: TimeInstant):
        self._current_time = a_time

    def get_status(self):
        return self._status

    def set_timeline_start(self, a_time: TimeInstant):
        self._timeline_begin = a_time

    def set_timeline_end(self, a_time: TimeInstant):
        self._timeline_end = a_time

    def set_current_time(self, a_time: TimeInstant):
        if a_time < self._timeline_begin:
            self._current_time = self._timeline_begin
        elif a_time > self._timeline_end:
            self._timeline_end = a_time
        self._current_time = a_time

    def get_timeline_start(self):
        return self._timeline_begin

    def get_timeline_end(self):
        return self._timeline_end

    def get_current_time(self):
        return self._current_time

    def play_from_start(self):
        pass

    def skip_to_end(self):
        pass

    def add_listener(self, datastream, event_listener) -> str:
        pass

    def remove_listener(self, stream_id):
        pass

    def clear_streams(self):
        pass

    def reset(self):
        self.clear_streams()
        self._temporal_mode = None
        self._status = None
        self._playback_speed = None
        self._timeline_begin = None
        self._timeline_end = None
        self._current_time = None

    def set_buffer_time(self, time: int):
        pass

    def get_buffer_time(self):
        pass

    def _compute_time_range(self):
        pass


class Synchronizer:
    _buffer: any

    def synchronize(self, systems: list):
        pass
