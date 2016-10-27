import unittest
import datetime
import rethinkdb

from mockthink.test.common import assertEqual
from ... import rtime

class TestRTime(unittest.TestCase):
    def test_to_date(self):
        timezone = rethinkdb.make_timezone('00:00')
        dt = datetime.datetime(2014, 6, 3, 12, 5, 36, tzinfo=timezone)
        as_date = rtime.to_date(dt)
        assertEqual(2014, as_date.year)
        assertEqual(6, as_date.month)
        assertEqual(3, as_date.day)
        assertEqual(0, as_date.hour)
        assertEqual(0, as_date.minute)
        assertEqual(0, as_date.second)
        assertEqual(timezone, as_date.tzinfo)

    def test_time_of_day_seconds(self):
        dt = datetime.datetime(2014, 1, 1, 2, 10, 30)
        assertEqual(7830, rtime.time_of_day_seconds(dt))

    def test_make_time(self):
        dt = rtime.make_time(2014, 3, 2)
        assertEqual(2014, dt.year)
        assertEqual(3, dt.month)
        assertEqual(2, dt.day)
        self.assertTrue(isinstance(dt.tzinfo, rethinkdb.ast.RqlTzinfo))
