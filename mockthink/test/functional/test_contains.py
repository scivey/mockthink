import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from mockthink.util import DictableSet
from pprint import pprint

class TestContains(MockTest):
    def get_data(self):
        data = [
            {'id': 'bob-id', 'age': 32, 'nums': [5, 7]},
            {'id': 'sam-id', 'age': 45},
            {'id': 'joe-id', 'age': 36}
        ]
        return as_db_and_table('d', 'people', data)

    def test_contains_table_dict_true(self, conn):
        result = r.db('d').table('people').contains({
            'id': 'sam-id',
            'age': 45
        }).run(conn)
        self.assertEqual(True, result)

    def test_contains_table_dict_multi_true(self, conn):
        result = r.db('d').table('people').contains(
            {
                'id': 'sam-id',
                'age': 45
            },
            {
                'id': 'joe-id',
                'age': 36
            }
        ).run(conn)
        self.assertEqual(True, result)

    def test_contains_table_dict_false(self, conn):
        result = r.db('d').table('people').contains({
            'id': 'tara-muse-id',
            'age': 'timeless'
        }).run(conn)
        self.assertEqual(False, result)

    def test_contains_table_dict_multi_false(self, conn):
        result = r.db('d').table('people').contains(
            {
                'id': 'sam-id',
                'age': 45
            },
            {
                'id': 'tara-muse-id',
                'age': 'timeless'
            }
        ).run(conn)
        self.assertEqual(False, result)

    def test_contains_table_pred_true(self, conn):
        result = r.db('d').table('people').contains(
            lambda doc: doc['id'] == 'sam-id'
        ).run(conn)
        self.assertEqual(True, result)

    def test_contains_table_pred_multi_true(self, conn):
        result = r.db('d').table('people').contains(
            lambda doc: doc['id'] == 'sam-id',
            lambda doc: doc['id'] == 'joe-id'
        ).run(conn)
        self.assertEqual(True, result)

    def test_contains_table_pred_false(self, conn):
        result = r.db('d').table('people').contains(
            lambda doc: doc['id'] == 'tara-muse-id'
        ).run(conn)
        self.assertEqual(False, result)

    def test_contains_table_pred_multi_false(self, conn):
        result = r.db('d').table('people').contains(
            lambda doc: doc['id'] == 'sam-id',
            lambda doc: doc['id'] == 'tara-muse-id'
        ).run(conn)
        self.assertEqual(False, result)

