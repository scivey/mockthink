import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqUnordered
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestMerge(MockTest):
    @staticmethod
    def get_data():
        data = [
            {
                'id': 'id-1',
                'x': {
                    'x-val': 'x-val-1'
                },
                'y': {
                    'y-val': 'y-val-1'
                }
            },
            {
                'id': 'id-2',
                'x': {
                    'x-val': 'x-val-2'
                },
                'y': {
                    'y-val': 'y-val-2'
                }
            }
        ]
        return as_db_and_table('jezebel', 'things', data)

    def test_merge_toplevel(self, conn):
        expected = [
            {
                'id': 'id-1',
                'x': {
                    'x-val': 'x-val-1'
                },
                'y': {
                    'y-val': 'y-val-1'
                },
                'z': 'Z-VALUE'
            },
            {
                'id': 'id-2',
                'x': {
                    'x-val': 'x-val-2'
                },
                'y': {
                    'y-val': 'y-val-2'
                },
                'z': 'Z-VALUE'
            }
        ]
        result = r.db('jezebel').table('things').merge({'z': 'Z-VALUE'}).run(conn)
        assertEqUnordered(expected, list(result))

    def test_merge_nested(self, conn):
        expected = [
            {
                'y-val': 'y-val-1',
                'extra-y-val': 'extra'
            },
            {
                'y-val': 'y-val-2',
                'extra-y-val': 'extra'
            }
        ]
        result = r.db('jezebel').table('things').map(
            lambda d: d['y'].merge({'extra-y-val': 'extra'})
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_merge_nested_with_prop(self, conn):
        expected = [
            {
                'x-val': 'x-val-1',
                'y-val': 'y-val-1'
            },
            {
                'x-val': 'x-val-2',
                'y-val': 'y-val-2'
            }
        ]
        result = r.db('jezebel').table('things').map(
            lambda d: d['x'].merge(d['y'])
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_merge_nested_with_prop2(self, conn):
        expected = [
            {
                'x-val': 'x-val-1',
                'nested': {
                    'y-val': 'y-val-1'
                }
            },
            {
                'x-val': 'x-val-2',
                'nested': {
                    'y-val': 'y-val-2'
                }
            }
        ]
        result = r.db('jezebel').table('things').map(
            lambda d: d['x'].merge({'nested': d['y']})
        ).run(conn)
        assertEqUnordered(expected, list(result))
