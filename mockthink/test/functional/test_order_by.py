import rethinkdb as r
from mockthink.test.functional.common import MockTest
from mockthink.test.common import as_db_and_table, assertEqual


class TestOrderByOne(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        return as_db_and_table('y', 'scores', data)

    def test_sort_1_attr(self, conn):
        expected = [
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        result = r.db('y').table('scores').order_by('age').run(conn)
        assertEqual(expected, list(result))


    def test_sort_1_attr_asc(self, conn):
        expected = [
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age')).run(conn)
        assertEqual(expected, list(result))

    def test_sort_1_attr_desc(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'joe', 'age': 26, 'score': 60}
        ]
        result = r.db('y').table('scores').order_by(r.desc('age')).run(conn)
        assertEqual(expected, list(result))

    def test_sort_1_attr_2(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
        ]
        result = r.db('y').table('scores').order_by('score').run(conn)
        assertEqual(expected, list(result))

    def test_sort_1_attr_2_asc(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
        ]
        result = r.db('y').table('scores').order_by(r.asc('score')).run(conn)
        assertEqual(expected, list(result))

    def test_sort_1_attr_2_desc(self, conn):
        expected = [
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'todd', 'age': 52, 'score': 15},
        ]
        result = r.db('y').table('scores').order_by(r.desc('score')).run(conn)
        assertEqual(expected, list(result))


class TestOrderByMulti(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'pale', 'age': 52, 'score': 30}
        ]
        return as_db_and_table('y', 'scores', data)

    def test_sort_multi_1(self, conn):
        expected = [
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30}
        ]
        result = r.db('y').table('scores').order_by('age', 'score').run(conn)
        assertEqual(expected, list(result))

    def test_sort_multi_1_asc(self, conn):
        expected = [
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age'), r.asc('score')).run(conn)
        assertEqual(expected, list(result))

    def test_sort_multi_1_desc_1(self, conn):
        expected = [
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age'), r.desc('score')).run(conn)
        assertEqual(expected, list(result))

    def test_sort_multi_1_desc_2(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60}
        ]
        result = r.db('y').table('scores').order_by(r.desc('age'), 'score').run(conn)
        assertEqual(expected, list(result))

    def test_sort_multi_2(self, conn):
        expected = [
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78}
        ]
        result = r.db('y').table('scores').order_by('score', 'age').run(conn)
        assertEqual(expected, list(result))
