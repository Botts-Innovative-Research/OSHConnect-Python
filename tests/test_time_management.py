"""TimePeriod / TimeInstant primitives from oshconnect.timemanagement."""
from oshconnect import TimeInstant, TimePeriod


def test_time_period_with_iso_strings_resolves_to_time_instants():
    tp = TimePeriod(start="2024-06-18T15:46:32Z", end="2024-06-18T20:00:00Z")
    assert isinstance(tp.start, TimeInstant)
    assert isinstance(tp.end, TimeInstant)
    assert tp.start.epoch_time == TimeInstant.from_string("2024-06-18T15:46:32Z").epoch_time
    assert tp.end.epoch_time == TimeInstant.from_string("2024-06-18T20:00:00Z").epoch_time


def test_time_period_now_sentinel_preserved_at_start():
    tp = TimePeriod(start="now", end="2099-06-18T20:00:00Z")
    assert tp.start == "now"
    assert tp.end.epoch_time == TimeInstant.from_string("2099-06-18T20:00:00Z").epoch_time


def test_time_period_now_sentinel_preserved_at_end():
    tp = TimePeriod(start="2024-06-18T20:00:00Z", end="now")
    assert tp.start.epoch_time == TimeInstant.from_string("2024-06-18T20:00:00Z").epoch_time
    assert tp.end == "now"