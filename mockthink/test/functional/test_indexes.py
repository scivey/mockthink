import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestIndexes(MockTest):
    def get_data(self):
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        return as_db_and_table('s', 'people', data)

    def test_field_index_create(self, conn):
        expected = ['first_name']
        r.db('s').table('people').index_create('first_name').run(conn)
        result = r.db('s').table('people').index_list().run(conn)

        self.assertEqUnordered(expected, list(result))

    def test_field_index_create_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create('first_name').run(conn)
        r.db('s').table('people').index_wait('first_name').run(conn)
        result = r.db('s').table('people').get_all('Bob', index='first_name').run(conn)
        result = list(result)
        pprint(result)
        self.assertEqUnordered(expected, result)

    def test_func_index_create(self, conn):
        expected = ['first_and_last']
        r.db('s').table('people').index_create(
            'first_and_last',
            lambda doc: doc['first_name'] + doc['last_name']
        ).run(conn)
        result = r.db('s').table('people').index_list().run(conn)

        self.assertEqUnordered(expected, list(result))

    def test_func_index_create_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'first_and_last',
            lambda doc: doc['first_name'] + doc['last_name']
        ).run(conn)
        result = r.db('s').table('people').get_all(
            'BobBuilder', 'TomGeneric',
            index='first_and_last'
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_index_drop_works(self, conn):
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        self.assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_drop(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        self.assertEqual([], indexes)


    def test_index_rename_works(self, conn):
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        self.assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_rename(
            'last_name', 'new_last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        self.assertEqual(['new_last_name'], indexes)


    def test_index_rename_works_2(self, conn):
        expected = [
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        self.assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_rename(
            'last_name', 'new_last_name'
        ).run(conn)
        result = r.db('s').table('people').get_all(
            'Generic',
            index='new_last_name'
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_index_wait_one_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait('last_name').run(conn)
        result = r.db('s').table('people').get_all(
            'Builder', index='last_name'
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_index_wait_all_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').get_all(
            'Builder', index='last_name'
        ).run(conn)
        self.assertEqual(expected, list(result))

