import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from pprint import pprint


class TestGroup(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe', 'type': 'bro'},
            {'id': 'bill', 'type': 'hipster'},
            {'id': 'todd', 'type': 'hipster'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_group_by_field(self, conn):
        expected = {
            'bro': [
                {'id': 'joe', 'type': 'bro'},
            ],
            'hipster': [
                {'id': 'bill', 'type': 'hipster'},
                {'id': 'todd', 'type': 'hipster'}
            ]
        }
        result = r.db('x').table('people').group('type').run(conn)
        self.assertEqual(expected['bro'], result['bro'])
        self.assertEqUnordered(expected['hipster'], result['hipster'])
        self.assertEqual(set(['bro', 'hipster']), set(result.keys()))

    def test_group_by_func(self, conn):
        expected = {
            'bro': [
                {'id': 'joe', 'type': 'bro'},
            ],
            'hipster': [
                {'id': 'bill', 'type': 'hipster'},
                {'id': 'todd', 'type': 'hipster'}
            ]
        }
        result = r.db('x').table('people').group(lambda d: d['type']).run(conn)
        self.assertEqual(expected['bro'], result['bro'])
        self.assertEqUnordered(expected['hipster'], result['hipster'])
        self.assertEqual(set(['bro', 'hipster']), set(result.keys()))
