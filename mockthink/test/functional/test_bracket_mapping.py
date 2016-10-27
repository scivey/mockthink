import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestBracketMapping(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 1, 'animals': ['frog', 'cow']},
            {'id': 2, 'animals': ['horse']}
        ]
        return as_db_and_table('x', 'farms', data)

    def test_simple(self, conn):
        res = r.db('x').table('farms').map(
            lambda doc: doc['animals'][0]
        ).run(conn)
        assertEqual(
            set(['frog', 'horse']),
            set(list(res))
        )

    def test_filter_by_bracket(self, conn):
        res = r.db('x').table('farms').filter(
            lambda doc: doc['id'] < 2
        ).run(conn)
        expected = [1]
        results = [doc['id'] for doc in res]
        assertEqual(expected, results)

    def test_order_by_bracket(self, conn):
        res = r.db('x').table('farms').order_by(
            lambda doc: doc['id']
        ).map(lambda doc: doc['id']).run(conn)
        expected = [1, 2]
        assertEqual(expected, list(res))
