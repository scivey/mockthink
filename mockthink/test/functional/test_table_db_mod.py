import rethinkdb as r
from rethinkdb import RqlRuntimeError

from mockthink import util
from mockthink.test.common import as_db_and_table, assertEqual
from mockthink.test.functional.common import MockTest

class TestTableMod(MockTest):
    @staticmethod
    def get_data():
        return {
            'dbs': {
                'db_one': {
                    'tables': {
                        'one_x': [],
                        'one_y': []
                    }
                },
                'db_two': {
                    'tables': {
                        'two_x': [],
                        'two_y': []
                    }
                }
            }
        }

    def test_table_list_1(self, conn):
        expected = set(['one_x', 'one_y'])
        result = r.db('db_one').table_list().run(conn)
        assertEqual(expected, set(list(result)))

    def test_table_list_2(self, conn):
        expected = set(['two_x', 'two_y'])
        result = r.db('db_two').table_list().run(conn)
        assertEqual(expected, set(list(result)))

    def test_table_create_1(self, conn):
        expected_1 = set(['one_x', 'one_y', 'one_z'])
        expected_2 = set(['two_x', 'two_y'])
        r.db('db_one').table_create('one_z').run(conn)
        result_1 = r.db('db_one').table_list().run(conn)
        assertEqual(expected_1, set(list(result_1)))

        result_2 = r.db('db_two').table_list().run(conn)
        assertEqual(expected_2, set(list(result_2)))

    def test_table_create_2(self, conn):
        expected_1 = set(['one_x', 'one_y'])
        expected_2 = set(['two_x', 'two_y', 'two_z'])
        r.db('db_two').table_create('two_z').run(conn)
        result_1 = r.db('db_one').table_list().run(conn)
        assertEqual(expected_1, set(list(result_1)))

        result_2 = r.db('db_two').table_list().run(conn)
        assertEqual(expected_2, set(list(result_2)))

    def test_table_drop_1(self, conn):
        expected_1 = set(['one_x'])
        expected_2 = set(['two_x', 'two_y'])
        r.db('db_one').table_drop('one_y').run(conn)
        result_1 = r.db('db_one').table_list().run(conn)
        assertEqual(expected_1, set(list(result_1)))

        result_2 = r.db('db_two').table_list().run(conn)
        assertEqual(expected_2, set(list(result_2)))

    def test_table_drop_2(self, conn):
        expected_1 = set(['one_x', 'one_y'])
        expected_2 = set(['two_x'])
        r.db('db_two').table_drop('two_y').run(conn)
        result_1 = r.db('db_one').table_list().run(conn)
        assertEqual(expected_1, set(list(result_1)))

        result_2 = r.db('db_two').table_list().run(conn)
        assertEqual(expected_2, set(list(result_2)))


class TestDbMod(MockTest):
    @staticmethod
    def get_data():
        return {
            'dbs': {
                'db_one': {
                    'tables': {
                        'one_x': [],
                        'one_y': []
                    }
                },
                'db_two': {
                    'tables': {
                        'two_x': [],
                        'two_y': []
                    }
                }
            }
        }

    def test_db_list(self, conn):
        expected = set(['db_one', 'db_two'])
        result = self.db_list(conn)
        assertEqual(expected, result)

    def test_db_create(self, conn):
        expected = set(['db_one', 'db_two', 'db_three'])
        r.db_create('db_three').run(conn)
        result = self.db_list(conn)
        assertEqual(expected, result)

    def test_db_drop(self, conn):
        expected = set(['db_one'])
        r.db_drop('db_two').run(conn)
        result = self.db_list(conn)
        assertEqual(expected, result)

    def db_list(self, conn):
        # rethinkdb is special and always present; we don't care, for these tests
        return set(r.db_list().run(conn)) - {u'rethinkdb'}
