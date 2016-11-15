import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqUnordered, assertEqual
from mockthink.test.functional.common import MockTest
from mockthink.util import DictableSet
from pprint import pprint

class TestDistinctTop(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob-id', 'first_name': 'Bob', 'last_name': 'Sanders', 'age': 35},
            {'id': 'sam-id', 'first_name': 'Sam', 'last_name': 'Fudd', 'age': 17},
            {'id': 'joe-id', 'first_name': 'Joe', 'last_name': 'Sanders', 'age': 62}
        ]
        return as_db_and_table('d', 'people', data)

    def test_distinct_table(self, conn):
        expected = [
            {'id': 'bob-id', 'first_name': 'Bob', 'last_name': 'Sanders', 'age': 35},
            {'id': 'sam-id', 'first_name': 'Sam', 'last_name': 'Fudd', 'age': 17},
            {'id': 'joe-id', 'first_name': 'Joe', 'last_name': 'Sanders', 'age': 62}
        ]

        result = r.db('d').table('people').distinct().run(conn)
        assertEqUnordered(expected, list(result))

    def test_distinct_secondary_index(self, conn):
        r.db('d').table('people').index_create('last_name').run(conn)
        r.db('d').table('people').index_wait().run(conn)
        result = r.db('d').table('people').distinct(index='last_name').run(conn)
        result = list(result)
        pprint({'result': result})
        assertEqual(2, len(result))
        assertEqual(set(['Sanders', 'Fudd']), set(result))


class TestDistinctNested(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'x-id', 'nums': [1, 5, 2, 5, 3, 2]},
            {'id': 'y-id', 'nums': [{'val': 1}, {'val': 5}, {'val': 2}, {'val': 5}, {'val': 3}, {'val': 2}]}
        ]
        return as_db_and_table('d', 'people', data)

    def test_distinct_nested(self, conn):
        ex1 = set([1, 2, 5, 3])
        ex2 = DictableSet([{'val': 1}, {'val': 2}, {'val': 5}, {'val': 3}])
        result = r.db('d').table('people').map(
            lambda doc: doc['nums'].distinct()
        ).run(conn)
        result = list(result)
        for elem in result:
            if isinstance(elem[0], dict):
                for dict_elem in elem:
                    assert(ex2.has(dict_elem))
            else:
                assertEqual(ex1, set(elem))
