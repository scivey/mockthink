import rethinkdb as r
from rethinkdb import RqlRuntimeError

from mockthink import util
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest


class TestMax(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'age': 26,
                'hobbies': ['sand', 'water', 'cats']
            },
            {
                'id': 'bill',
                'age': 52,
                'hobbies': ['watermelon']
            },
            {
                'id': 'todd',
                'age': 35,
                'hobbies': ['citrus'],
                'nums': [100, 550, 40, 900, 800, 36],
                'nums2': [
                    {'val': 26},
                    {'val': 78},
                    {'val': 19},
                    {'val': 110},
                    {'val': 82}
                ]
            }
        ]
        return as_db_and_table('x', 'people', data)

    def test_max_of_table_field(self, conn):
        expected = {'id': 'bill', 'age': 52, 'hobbies': ['watermelon']}
        result = r.db('x').table('people').max('age').run(conn)
        self.assertEqual(expected, result)

    def test_max_of_sequence_field(self, conn):
        expected = [110]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].max('val')
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_max_of_table_Func(self, conn):
        expected = {'id': 'bill', 'age': 52, 'hobbies': ['watermelon']}
        result = r.db('x').table('people').max(
            lambda d: d['age']
        ).run(conn)
        self.assertEqual(expected, result)

    def test_max_of_sequence_func(self, conn):
        expected = [110]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].max(
                lambda num: num['val']
            )
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_max_of_left_seq_no_args(self, conn):
        expected = [900]
        result = r.db('x').table('people').filter(
            lambda doc: doc['id'] == 'todd'
        ).map(
            lambda doc: doc['nums'].max()
        ).run(conn)
        self.assertEqual(expected, list(result))


class TestMin(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'age': 26,
                'hobbies': ['sand', 'water', 'cats']
            },
            {
                'id': 'bill',
                'age': 52,
                'hobbies': ['watermelon']
            },
            {
                'id': 'todd',
                'age': 35,
                'hobbies': ['citrus'],
                'nums': [100, 550, 40, 900, 800],
                'nums2': [
                    {'val': 26},
                    {'val': 17},
                    {'val': 86}
                ]
            }
        ]
        return as_db_and_table('x', 'people', data)

    def test_min_of_table_field(self, conn):
        expected = {'id': 'joe', 'age': 26, 'hobbies': ['sand',  'water',  'cats']}
        result = r.db('x').table('people').min('age').run(conn)
        self.assertEqual(expected, result)

    def test_min_of_sequence_field(self, conn):
        expected = [17]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].min('val')
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_min_of_table_func(self, conn):
        expected = {'id': 'joe', 'age': 26, 'hobbies': ['sand',  'water',  'cats']}
        result = r.db('x').table('people').min(
            lambda doc: doc['age']
        ).run(conn)
        self.assertEqual(expected, result)

    def test_min_of_sequence_func(self, conn):
        expected = [17]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].min(
                lambda num: num['val']
            )
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_min_of_left_seq_no_args(self, conn):
        expected = [40]
        result = r.db('x').table('people').filter(
            lambda doc: doc['id'] == 'todd'
        ).map(
            lambda doc: doc['nums'].min()
        ).run(conn)
        self.assertEqual(expected, list(result))


class TestSum(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'age': 26,
                'hobbies': ['sand', 'water', 'cats']
            },
            {
                'id': 'bill',
                'age': 52,
                'hobbies': ['watermelon']
            },
            {
                'id': 'todd',
                'age': 35,
                'hobbies': ['citrus'],
                'nums': [100, 50, 400, 9],
                'nums2': [
                    {'val': 40},
                    {'val': 53}
                ]
            }
        ]
        return as_db_and_table('x', 'people', data)

    def test_sum_of_table_field(self, conn):
        expected = 113
        result = r.db('x').table('people').sum('age').run(conn)
        self.assertEqual(expected, result)

    def test_sum_of_seq_field(self, conn):
        expected = [93]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].sum('val')
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_sum_of_table_func(self, conn):
        expected = 113
        result = r.db('x').table('people').sum(
            lambda doc: doc['age']
        ).run(conn)
        self.assertEqual(expected, result)

    def test_sum_of_seq_func(self, conn):
        expected = [93]
        result = r.db('x').table('people').filter({
            'id': 'todd'
        }).map(
            lambda doc: doc['nums2'].sum(
                lambda num: num['val']
            )
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_sum_of_seq_no_args(self, conn):
        expected = [559]
        result = r.db('x').table('people').filter(
            lambda doc: doc['id'] == 'todd'
        ).map(
            lambda doc: doc['nums'].sum()
        ).run(conn)
        self.assertEqual(expected, list(result))


class TestAverage(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'age': 43,
                'hobbies': ['sand', 'water', 'cats']
            },
            {
                'id': 'bill',
                'age': 48,
                'hobbies': ['watermelon']
            },
            {
                'id': 'todd',
                'age': 29,
                'hobbies': ['citrus'],
                'nums': [76, 40, 100, 800],
                'nums2': [
                    {'val': 10},
                    {'val': 20}
                ]
            }
        ]
        return as_db_and_table('x', 'people', data)

    def test_avg_of_table_field(self, conn):
        expected = 40
        result = r.db('x').table('people').avg('age').run(conn)
        self.assertEqual(expected, result)

    def test_avg_of_sequence_field(self, conn):
        expected = [15]
        result = r.db('x').table('people').filter(
            {'id': 'todd'}
        ).map(
            lambda doc: doc['nums2'].avg('val')
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_avg_of_table_func(self, conn):
        expected = 40
        result = r.db('x').table('people').avg(
            lambda doc: doc['age']
        ).run(conn)
        self.assertEqual(expected, result)

    def test_avg_of_sequence_func(self, conn):
        expected = [15]
        result = r.db('x').table('people').filter(
            {'id': 'todd'}
        ).map(
            lambda doc: doc['nums2'].avg(
                lambda num: num['val']
            )
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_avg_of_left_seq_no_args(self, conn):
        expected = [254]
        result = r.db('x').table('people').filter(
            lambda doc: doc['id'] == 'todd'
        ).map(
            lambda doc: doc['nums'].avg()
        ).run(conn)
        self.assertEqual(expected, list(result))


class TestCount(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'age': 43,
                'hobbies': ['sand', 'water', 'cats']
            },
            {
                'id': 'bill',
                'age': 48,
                'hobbies': ['watermelon']
            },
            {
                'id': 'todd',
                'age': 29,
                'hobbies': ['citrus'],
                'nums': [40, 67, 40, 800, 900]
            }
        ]
        return as_db_and_table('x', 'people', data)

    def test_table_count(self, conn):
        expected = 3
        result = r.db('x').table('people').count().run(conn)
        self.assertEqual(expected, result)

    def test_sequence_count(self, conn):
        expected = [4]
        result = r.db('x').table('people').filter(
            {'id': 'todd'}
        ).map(
            lambda doc: doc['nums'].count()
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_table_eq_elem_count(self, conn):
        expected = 1
        result = r.db('x').table('people').count({
            'id': 'bill',
            'age': 48,
            'hobbies': ['watermelon']
        }).run(conn)
        self.assertEqual(expected, result)

    def test_sequence_eq_elem_count(self, conn):
        expected = [2]
        result = r.db('x').table('people').filter(
            {'id': 'todd'}
        ).map(
            lambda doc: doc['nums'].count(40)
        ).run(conn)
        self.assertEqual(expected, list(result))

    def test_table_func_count(self, conn):
        expected = 2
        result = r.db('x').table('people').count(
            lambda doc: doc['age'] > 40
        ).run(conn)
        self.assertEqual(expected, result)

    def test_sequence_func_count(self, conn):
        expected = [3]
        result = r.db('x').table('people').filter(
            {'id': 'todd'}
        ).map(
            lambda doc: doc['nums'].count(
                lambda num: num > 40
            )
        ).run(conn)
        self.assertEqual(expected, list(result))
