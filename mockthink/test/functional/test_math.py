import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestMath(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'pt-1',
                'x': 10,
                'y': 25
            },
            {
                'id': 'pt-2',
                'x': 100,
                'y': 3
            }
        ]
        return as_db_and_table('math_db', 'points', data)

    def test_add_method(self, conn):
        expected = [35, 103]
        result = r.db('math_db').table('points').map(lambda t: t['x'].add(t['y'])).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_add_oper(self, conn):
        expected = [35, 103]
        result = r.db('math_db').table('points').map(lambda t: t['x'] + t['y']).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_method(self, conn):
        expected = [-15, 97]
        result = r.db('math_db').table('points').map(lambda t: t['x'].sub(t['y'])).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_oper(self, conn):
        expected = [-15, 97]
        result = r.db('math_db').table('points').map(lambda t: t['x'] - t['y']).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_mul_method(self, conn):
        expected = [250, 300]
        result = r.db('math_db').table('points').map(lambda t: t['x'].mul(t['y'])).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_mul_oper(self, conn):
        expected = [250, 300]
        result = r.db('math_db').table('points').map(lambda t: t['x'] * t['y']).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestMath2(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'pt-1',
                'x': 30,
                'y': 3,
                'z': 18
            },
            {
                'id': 'pt-2',
                'x': 24,
                'y': 6,
                'z': 10
            }
        ]
        return as_db_and_table('math_db', 'points', data)

    def test_div_method(self, conn):
        expected = set([10, 4])
        result = r.db('math_db').table('points').map(
            lambda t: t['x'].div(t['y'])
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

    def test_div_oper(self, conn):
        expected = set([10, 4])
        result = r.db('math_db').table('points').map(
            lambda t: t['x'] / t['y']
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

    def test_mod_method(self, conn):
        expected = set([12, 4])
        result = r.db('math_db').table('points').map(
            lambda t: t['x'].mod(t['z'])
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

    def test_mod_oper(self, conn):
        expected = set([12, 4])
        result = r.db('math_db').table('points').map(
            lambda t: t['x'] % t['z']
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

class TestRandom(MockTest):
    def get_data(self):
        data = [
            {'id': 'x', 'val': 12},
            {'id': 'y', 'val': 30}
        ]
        return as_db_and_table('things', 'pointless', data)

    def test_random_0(self, conn):
        result = r.random().run(conn)
        assert(result <= 1)
        assert(result >= 0)
        assert(type(result) == float)

    def test_random_1(self, conn):
        result = r.random(10).run(conn)
        assert(result <= 10)
        assert(result >= 0)
        assert(type(result) == int)

    def test_random_1_float(self, conn):
        result = r.random(10).run(conn)
        assert(result <= 10)
        assert(result >= 0)
        assert(type(result) == int)

    def test_random_2(self, conn):
        result = r.random(10, 20).run(conn)
        assert(result <= 20)
        assert(result >= 10)
        assert(type(result) == int)

    def test_random_2_float(self, conn):
        result = r.random(10, 20, float=True).run(conn)
        assert(result <= 20)
        assert(result >= 10)
        assert(type(result) == float)

