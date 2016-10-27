import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqUnordered, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestLogic1(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'joe', 'has_eyes': True, 'age': 22, 'hair_color': 'brown'},
            {'id': 'sam', 'has_eyes': True, 'age': 17, 'hair_color': 'bald'},
            {'id': 'angela', 'has_eyes': False, 'age': 26, 'hair_color': 'black'},
            {'id': 'johnson', 'has_eyes': False, 'age': 16, 'hair_color': 'blonde'}
        ]
        return as_db_and_table('pdb', 'p', data)

    def test_not(self, conn):
        expected = [
            {'id': 'johnson'},
            {'id': 'angela'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: ~doc['has_eyes']
        ).pluck('id').run(conn)
        assertEqUnordered(expected, list(result))

    def test_and(self, conn):
        expected = [
            {'id': 'sam'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['has_eyes'].and_(doc['age'].lt(20))
        ).pluck('id').run(conn)
        assertEqual(expected, list(result))

    def test_or(self, conn):
        expected = [
            {'id': 'sam'},
            {'id': 'angela'},
            {'id': 'joe'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['has_eyes'].or_(doc['age'].gt(20))
        ).pluck('id').run(conn)
        assertEqUnordered(expected, list(result))

    def test_gt(self, conn):
        expected = [
            {'id': 'joe'},
            {'id': 'angela'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['age'] > 20
        ).pluck('id').run(conn)
        assertEqUnordered(expected, list(result))

    def test_lt(self, conn):
        expected = [
            {'id': 'sam'},
            {'id': 'johnson'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['age'].lt(20)
        ).pluck('id').run(conn)
        assertEqUnordered(expected, list(result))

    def test_eq(self, conn):
        expected = [
            {'id': 'sam'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['hair_color'] == 'bald'
        ).pluck('id').run(conn)
        assertEqual(expected, list(result))

    def test_neq(self, conn):
        expected = [
            {'id': 'sam'},
            {'id': 'angela'},
            {'id': 'joe'}
        ]
        result = r.db('pdb').table('p').filter(
            lambda doc: doc['hair_color'] != 'blonde'
        ).pluck('id').run(conn)
        assertEqUnordered(expected, list(result))

