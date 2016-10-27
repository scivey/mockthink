import rethinkdb as r
from mockthink.test.common import as_db_and_table, assertEqUnordered, assertEqual
from mockthink.test.functional.common import MockTest
from pprint import pprint

class TestIndexes(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        return as_db_and_table('s', 'people', data)

    def test_field_index_create(self, conn):
        expected = ['first_name']
        r.db('s').table('people').index_create('first_name').run(conn)
        result = r.db('s').table('people').index_list().run(conn)

        assertEqUnordered(expected, list(result))

    def test_field_index_create_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create('first_name').run(conn)
        r.db('s').table('people').index_wait('first_name').run(conn)
        result = r.db('s').table('people').get_all('Bob', index='first_name').run(conn)
        result = list(result)
        pprint(result)
        assertEqUnordered(expected, result)

    def test_func_index_create(self, conn):
        expected = ['first_and_last']
        r.db('s').table('people').index_create(
            'first_and_last',
            lambda doc: doc['first_name'] + doc['last_name']
        ).run(conn)
        result = r.db('s').table('people').index_list().run(conn)

        assertEqUnordered(expected, list(result))

    def test_func_index_create_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'first_and_last',
            lambda doc: doc['first_name'] + doc['last_name']
        ).run(conn)
        result = r.db('s').table('people').get_all(
            'BobBuilder', 'TomGeneric',
            index='first_and_last'
        ).run(conn)
        assertEqUnordered(expected, list(result))

    def test_index_drop_works(self, conn):
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_drop(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        assertEqual([], indexes)


    def test_index_rename_works(self, conn):
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_rename(
            'last_name', 'new_last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        assertEqual(['new_last_name'], indexes)


    def test_index_rename_works_2(self, conn):
        expected = [
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        indexes = list(r.db('s').table('people').index_list().run(conn))
        assertEqual(['last_name'], indexes)
        r.db('s').table('people').index_rename(
            'last_name', 'new_last_name'
        ).run(conn)
        result = r.db('s').table('people').get_all(
            'Generic',
            index='new_last_name'
        ).run(conn)
        assertEqual(expected, list(result))

    def test_index_wait_one_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait('last_name').run(conn)
        result = r.db('s').table('people').get_all(
            'Builder', index='last_name'
        ).run(conn)
        assertEqual(expected, list(result))

    def test_index_wait_all_works(self, conn):
        expected = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'}
        ]

        r.db('s').table('people').index_create(
            'last_name'
        ).run(conn)
        r.db('s').table('people').index_wait().run(conn)
        result = r.db('s').table('people').get_all(
            'Builder', index='last_name'
        ).run(conn)
        assertEqual(expected, list(result))


class TestIndexUpdating(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'bob', 'first_name': 'Bob', 'last_name': 'Builder'},
            {'id': 'joe', 'first_name': 'Joseph', 'last_name': 'Smith'},
            {'id': 'tom', 'first_name': 'Tom', 'last_name': 'Generic'}
        ]
        return as_db_and_table('s', 'people', data)

    def test_index_update(self, conn):
        people = r.db('s').table('people')
        people.index_create(
            'last_name'
        ).run(conn)
        people.index_wait().run(conn)
        people.insert({
            'id': 'someone',
            'first_name': 'Some',
            'last_name': 'One'
        }).run(conn)
        result = list(people
            .get_all('One', index='last_name')
            .run(conn)
        )
        assertEqual(1, len(result))
        result = result[0]
        assertEqual('someone', result['id'])


class TestMoreIndices(MockTest):
    @staticmethod
    def get_data():
        data = [
            {'id': 'a-id', 'name': 'a', 'parent': None, 'parents': []},
            {'id': 'b-id', 'name': 'b', 'parent': 'a-id', 'parents': ['a-id']},
            {'id': 'c-id', 'name': 'c', 'parent': 'a-id', 'parents': ['a-id']},
            {'id': 'd-id', 'name': 'd', 'parent': 'c-id', 'parents': ['c-id', 'a-id']},
            {'id': 'e-id', 'name': 'e', 'parent': 'b-id', 'parents': ['b-id', 'a-id']}
        ]
        return as_db_and_table('s', 'spaces', data)

    def test_basic(self, conn):
        spaces = r.db('s').table('spaces')
        spaces.index_create('parents', multi=True).run(conn)
        spaces.index_wait().run(conn)
        children = list(spaces.get_all('a-id', index='parents').run(conn))
        assertEqual(4, len(children))

    # def test_query_against(self, conn):
    #     spaces = r.db('s').table('spaces')
    #     spaces.index_create('parents', multi=True).run(conn)
    #     spaces.index_wait().run(conn)
    #     query = spaces.get_all('b-id', index='parents')\
    #         .distinct()\
    #         .order_by(lambda doc: doc['parents'].count())\
    #         .map(lambda doc: doc['id'])
    #     result = list(query.run(conn))
    #     pprint(result)
    #     assertEqual(True, False)
    #         # for parent_id in (rethinkdb.table(Workspace.plural())
    #         #                            .get_all(*workspace_ids, index='parents')
    #         #                            .distinct()
    #         #                            .order_by(lambda doc: doc['parents'].count())
    #         #                            .map(lambda doc: doc['id'])
    #         #                            .run(Model.connection.local())):
