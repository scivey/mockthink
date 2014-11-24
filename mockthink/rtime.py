import rethinkdb
import datetime

def to_date(dt, timezone=None):
    return datetime.datetime(dt.year, dt.month, dt.day, tzinfo=dt.tzinfo)

def time_of_day_seconds(dt):
    minutes = (dt.hour * 60) + dt.minute
    return (minutes * 60) + dt.second

def day_of_year(dt):
    raise NotImplementedError

def make_time(year, month, day, hour=0, minute=0, second=0, timezone=None):
    timezone = timezone or rethinkdb.make_timezone('00:00')
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=timezone)

def now():
    dtime = datetime.datetime.now()
    return dtime.replace(tzinfo=rethinkdb.make_timezone('00:00'))
