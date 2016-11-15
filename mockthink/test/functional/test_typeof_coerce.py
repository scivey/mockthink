import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestTypeOf(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'one',
                'null_attr': None,
                'list_attr': [5],
                'dict_attr': {'x': 10},
                'bool_attr': True,
                'num_attr': 3.2,
                'str_attr': 'text'
            }
        ]
        return as_db_and_table('a_db', 'types', data)

    def test_null(self, conn):
        expected = ['NULL']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['null_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))


    def test_num(self, conn):
        expected = ['NUMBER']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['num_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))

    def test_obj(self, conn):
        expected = ['OBJECT']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['dict_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))

    def test_bool(self, conn):
        expected = ['BOOL']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['bool_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))

    def test_array(self, conn):
        expected = ['ARRAY']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['list_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))

    def test_string(self, conn):
        expected = ['STRING']
        result = r.db('a_db').table('types').map(
            lambda doc: doc['str_attr'].type_of()
        ).run(conn)
        assertEqual(expected, list(result))

