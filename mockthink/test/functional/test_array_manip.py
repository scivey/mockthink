import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestArrayManip(MockTest):
    def get_data(self):
        data = [
            {'id': 1, 'animals': ['frog', 'cow']},
            {'id': 2, 'animals': ['horse']}
        ]
        return as_db_and_table('x', 'farms', data)

    def test_insert_at(self, conn):
        expected = [
            ['frog', 'pig', 'cow'],
            ['horse', 'pig']
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].insert_at(1, 'pig')
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_splice_at(self, conn):
        expected = [
            ['frog', 'pig', 'chicken', 'cow'],
            ['horse', 'pig', 'chicken']
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].splice_at(1, ['pig', 'chicken'])
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_prepend(self, conn):
        expected = [
            ['pig', 'frog', 'cow'],
            ['pig', 'horse']
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].prepend('pig')
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_append(self, conn):
        expected = [
            ['frog', 'cow', 'pig'],
            ['horse', 'pig']
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].append('pig')
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_change_at(self, conn):
        expected = [
            ['wombat', 'cow'],
            ['wombat']
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].change_at(0, 'wombat')
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_delete_at(self, conn):
        expected = [
            ['cow'],
            []
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].delete_at(0)
        ).run(conn)
        res = list(result)
        self.assertEqUnordered(expected, res)

class TestUnion(MockTest):
    def get_data(self):
        things_1 = [
            {'id': 'thing1-1'},
            {'id': 'thing1-2'}
        ]
        things_2 = [
            {'id': 'thing2-1'},
            {'id': 'thing2-2'}
        ]
        return {
            'dbs': {
                'x': {
                    'tables': {
                        'things_1': things_1,
                        'things_2': things_2
                    }
                }
            }

        }

    def test_table_union(self, conn):
        expected = [
            {'id': 'thing1-1'},
            {'id': 'thing1-2'},
            {'id': 'thing2-1'},
            {'id': 'thing2-2'}
        ]
        result = r.db('x').table('things_1').union(
            r.db('x').table('things_2')
        ).run(conn)
        self.assertEqUnordered(expected, list(result))


class TestIndexesOf(MockTest):
    def get_data(self):
        things = [
            {'id': 'one', 'letters': ['c', 'c']},
            {'id': 'two', 'letters': ['a', 'b', 'a', ['q', 'q'], 'b']},
            {'id': 'three', 'letters': ['b', 'a', 'b', 'a']},
            {'id': 'four', 'letters': ['c', 'a', 'b', 'a', ['q', 'q']]}
        ]
        return as_db_and_table('scrumptious', 'cake', things)

    def test_indexes_of_val(self, conn):
        expected = [
            [],
            [1, 4],
            [0, 2],
            [2]
        ]
        result = r.db('scrumptious').table('cake').map(
            lambda doc: doc['letters'].indexes_of('b')
        ).run(conn)
        result = list(result)
        pprint(result)
        self.assertEqUnordered(expected, result)

    def test_indexes_of_array_val(self, conn):
        expected = [
            [],
            [3],
            [],
            [4]
        ]
        result = r.db('scrumptious').table('cake').map(
            lambda doc: doc['letters'].indexes_of(['q', 'q'])
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_indexes_of_func(self, conn):
        expected = [
            [],
            [1, 4],
            [0, 2],
            [2]
        ]
        result = r.db('scrumptious').table('cake').map(
            lambda doc: doc['letters'].indexes_of(
                lambda letter: letter == 'b'
            )
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestSample(MockTest):
    def get_data(self):
        data = [
            {'id': 'one', 'data': range(10, 20)},
            {'id': 'two', 'data': range(20, 30)},
            {'id': 'three', 'data': range(30, 40)}
        ]
        return as_db_and_table('db', 'things', data)

    def test_nested(self, conn):
        result = r.db('db').table('things').filter(
            {'id': 'one'}
        ).map(
            lambda doc: doc['data'].sample(3)
        ).run(conn)
        result = list(result)
        assert(len(result) == 1)
        result = result[0]
        assert(len(result) == 3)
        for num in result:
            assert(num <= 20)
            assert(num >= 10)

    def test_docs(self, conn):
        result = r.db('db').table('things').sample(2).run(conn)
        result = list(result)
        assert(len(result) == 2)
        doc1, doc2 = result
        assert(doc1 != doc2)
        ids = set(['one', 'two', 'three'])
        assert(doc1['id'] in ids)
        assert(doc2['id'] in ids)
