import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest

class TestBetween(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'zuul', 'first_name': 'Adam', 'last_name': 'Zuul'}

        ]
        return as_db_and_table('s', 'people', data)

    def test_between_id_default_range(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'zuul'
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_between_id_closed_right(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_id_open_left(self, conn):
        expected = [
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', left_bound='open'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_id_open_left_closed_right(self, conn):
        expected = [
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        result = r.db('s').table('people').between(
            'bob', 'tom', left_bound='open', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_default_range(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder', 'Smith', index='last_name'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_closed_right(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder', 'Smith', index='last_name', right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)

    def test_between_index_open_left(self, conn):
        expected = [
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').between(
            'Builder',
            'Smith',
            index='last_name',
            left_bound='open',
            right_bound='closed'
        ).run(conn)
        result = list(result)
        assertEqUnordered(expected, result)
