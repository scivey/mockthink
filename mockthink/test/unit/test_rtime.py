import mock
import unittest
import datetime
import rethinkdb

from ... import rtime

class TestRTime(unittest.TestCase):
    def test_year_of_time(self):
        dt = mock.Mock()
        dt.year = 'YEAR'
        self.assertEqual('YEAR', rtime.year_of_time(dt))

    def test_hours_of_time(self):
        dt = mock.Mock()
        dt.hour = 'HOUR'
        self.assertEqual('HOUR', rtime.hours_of_time(dt))

    def test_day_of_time(self):
        dt = mock.Mock()
        dt.day = 'DAY'
        self.assertEqual('DAY', rtime.day_of_time(dt))

    def test_weekday_of_time(self):
        dt = mock.Mock()
        dt.isoweekday.return_value = 'ISO-WEEKDAY'
        self.assertEqual('ISO-WEEKDAY', rtime.weekday_of_time(dt))

    def test_to_date(self):
        timezone = rethinkdb.make_timezone('00:00')
        dt = datetime.datetime(2014, 6, 3, 12, 5, 36, tzinfo=timezone)
        as_date = rtime.to_date(dt)
        self.assertEqual(2014, as_date.year)
        self.assertEqual(6, as_date.month)
        self.assertEqual(3, as_date.day)
        self.assertEqual(0, as_date.hour)
        self.assertEqual(0, as_date.minute)
        self.assertEqual(0, as_date.second)
        self.assertEqual(timezone, as_date.tzinfo)

    def test_time_of_day_seconds(self):
        dt = datetime.datetime(2014, 1, 1, 2, 10, 30)
        self.assertEqual(7830, rtime.time_of_day_seconds(dt))

    def test_make_time(self):
        dt = rtime.make_time(2014, 3, 2)
        self.assertEqual(2014, dt.year)
        self.assertEqual(3, dt.month)
        self.assertEqual(2, dt.day)
        self.assertTrue(isinstance(dt.tzinfo, rethinkdb.ast.RqlTzinfo))

