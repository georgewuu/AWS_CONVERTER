import sys
from datetime import datetime
from delorean import Delorean

if sys.version_info > (3, 0):
    timestamp = datetime.timestamp
else:
    from backports.datetime_timestamp import timestamp


def get_timestamp_from_string(timestr, zone, fmt="%Y-%m-%d %H:%M:%S"):
    d = Delorean(datetime.strptime(timestr, fmt), timezone=zone)
    return timestamp(d.datetime)


get_timestamp = get_timestamp_from_string


def get_timestamp_from_datetime(dt, zone):
    d = Delorean(dt, timezone=zone)
    return timestamp(d.datetime)


def get_datetime_from_timestamp(unixtime, zone):
    d = Delorean(datetime.utcfromtimestamp(unixtime), "UTC")
    d.shift(zone)
    return d.datetime


def get_string_from_timestamp(unixtime, zone, fmt="%Y-%m-%d %H:%M:%S %Z"):
    return get_datetime_from_timestamp(unixtime, zone).strftime(fmt)
