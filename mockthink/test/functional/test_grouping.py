import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqual, assertEqUnordered
from mockthink.test.functional.common import MockTest
from pprint import pprint


class TestGroup(MockTest):
    @staticmethod
    def get_data():
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
        assertEqual(expected['bro'], result['bro'])
        assertEqUnordered(expected['hipster'], result['hipster'])
        assertEqual(set(['bro', 'hipster']), set(result.keys()))

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
        assertEqual(expected['bro'], result['bro'])
        assertEqUnordered(expected['hipster'], result['hipster'])
        assertEqual(set(['bro', 'hipster']), set(result.keys()))


class TestUngroup(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe', 'type': 'bro'},
            {'id': 'bill', 'type': 'hipster'},
            {'id': 'todd', 'type': 'hipster'},
            {'id': 'sam', 'type': 'bro'},
            {'id': 'glenn', 'type': 'unknown'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_ungroup_grouped_by_field(self, conn):
        expected = [
            {
                'group': 'bro',
                'reduction': [
                    {'id': 'joe', 'type': 'bro'},
                    {'id': 'sam', 'type': 'bro'}
                ]
            },
            {
                'group': 'hipster',
                'reduction': [
                    {'id': 'bill', 'type': 'hipster'},
                    {'id': 'todd', 'type': 'hipster'}
                ]
            },
            {
                'group': 'unknown',
                'reduction': [
                    {'id': 'glenn', 'type': 'unknown'},
                ]
            }
        ]
        result = r.db('x').table('people').group('type').ungroup().run(conn)
        result = list(result)
        assertEqual(3, len(result))
        assertEqual(set(['bro', 'hipster', 'unknown']), set([doc['group'] for doc in result]))
        is_group = lambda group: lambda doc: doc['group'] == group
        for group in ('bro', 'hipster', 'unknown'):
            result_group = list(filter(is_group(group), result))[0]
            expected_group = list(filter(is_group(group), expected))[0]
            assertEqUnordered(expected_group['reduction'], result_group['reduction'])

    def test_ungroup_grouped_by_func(self, conn):
        expected = [
            {
                'group': 'bro',
                'reduction': [
                    {'id': 'joe', 'type': 'bro'},
                    {'id': 'sam', 'type': 'bro'}
                ]
            },
            {
                'group': 'hipster',
                'reduction': [
                    {'id': 'bill', 'type': 'hipster'},
                    {'id': 'todd', 'type': 'hipster'}
                ]
            },
            {
                'group': 'unknown',
                'reduction': [
                    {'id': 'glenn', 'type': 'unknown'},
                ]
            }
        ]
        result = r.db('x').table('people').group(lambda d: d['type']).ungroup().run(conn)
        result = list(result)
        assertEqual(3, len(result))
        assertEqual(set(['bro', 'hipster', 'unknown']), set([doc['group'] for doc in result]))
        is_group = lambda group: lambda doc: doc['group'] == group
        for group in ('bro', 'hipster', 'unknown'):
            result_group = list(filter(is_group(group), result))[0]
            expected_group = list(filter(is_group(group), expected))[0]
            assertEqUnordered(expected_group['reduction'], result_group['reduction'])
