import time
from calendar import timegm
from datetime import datetime

from dateutil import parser


def _timestamp_string_to_epoch(tstamp, frmt):
    dt_obj = _timestamp_string_to_datetime(tstamp, frmt)
    return _timestamp_datetime_to_epoch(dt_obj)


def _timestamp_struct_time_to_epoch(ts):
    return timegm(ts)


def _timestamp_string_to_datetime(tstamp, frmt):
    if frmt:
        dt_obj = datetime.strptime(tstamp, frmt)
    else:
        dt_obj = parser.parse(tstamp)
    return dt_obj


def _timestamp_datetime_to_epoch(dt):
    return (timegm(dt.timetuple()) * 1e3 + dt.microsecond / 1e3) / 1e3


def get_epoch_from_timestamp(tstamp, format=None):
    # Suppressing warning due to bug in PyCharm mentioned here:
    # http://stackoverflow.com/questions/43357869/using-basestring-in-is-instance-type-mismatch-warning
    # noinspection PyTypeChecker
    if isinstance(tstamp, str):
        return _timestamp_string_to_epoch(tstamp, frmt=format)
    elif isinstance(tstamp, datetime):
        return _timestamp_datetime_to_epoch(tstamp)
    elif isinstance(tstamp, time.struct_time):
        return _timestamp_struct_time_to_epoch(tstamp)
    else:
        raise TypeError("Expected tstamp to be string, datetime or time.struct_time. Received: {}".format(type(tstamp)))


if __name__ == '__main__':
    print(get_epoch_from_timestamp(tstamp='2023-08-01'))
