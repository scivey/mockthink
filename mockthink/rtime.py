import rethinkdb
import datetime
import time

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

def create_rql_timezone(timezone_string):
    if timezone_string == 'Z':
        return rethinkdb.make_timezone('00:00')
    else:
        raise NotImplementedError

def epoch_time(dt):
    #   there's definitely a better way to do this.
    jan1_1970 = datetime.datetime(1970, 1, 1, tzinfo=dt.tzinfo)
    return int((dt - jan1_1970).total_seconds())

def rql_compatible_time(year, month, day, *args):
    hour, minute, second = (0, 0, 0)
    arg_count = len(args)
    if arg_count == 1:
        timezone = args[0]
    elif arg_count == 2:
        hour, timezone = args
    elif arg_count == 3:
        hour, minute, timezone = args
    elif arg_count == 4:
        hour, minute, second, timezone = args
    timezone = create_rql_timezone(timezone)
    return datetime.datetime(year, month, day, hour, minute, second, tzinfo=timezone)
