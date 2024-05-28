from __future__ import annotations

import datetime

from oshdatacore import component_implementations as swe_common_components

from oshconnect.timemanagement import Time, TemporalMode


class TimeManagement:
    time = swe_common_components.TimeComponent()
    time_controller: TimeController


class TimeController:
    _instance = None
    _temporal_mode: TemporalMode
    _status: str
    _playback_speed: int
    _timeline_begin: Time
    _timeline_end: Time
    _current_time: Time

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

    def skip(self, a_time: Time):
        self._current_time = a_time

    def get_status(self):
        return self._status

    def set_timeline_start(self, a_time: Time):
        self._timeline_begin = a_time

    def set_timeline_end(self, a_time: Time):
        self._timeline_end = a_time

    def set_current_time(self, a_time: Time):
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
