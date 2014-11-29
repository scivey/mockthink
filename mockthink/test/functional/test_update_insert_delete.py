import rethinkdb as r
from mockthink.test.common import as_db_and_table
from mockthink.test.functional.common import MockTest
from pprint import pprint


class TestReplace(MockTest):
    def get_data(self):
        data = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        return as_db_and_table('things', 'muppets', data)

    def test_replace_one(self, conn):
        expected = [
            {'id': 'kermit-id', 'name': 'Just Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        r.db('things').table('muppets').get('kermit-id').replace({'id': 'kermit-id', 'name': 'Just Kermit'}).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, result)


class TestUpdating(MockTest):
    def get_data(self):
        data = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        return as_db_and_table('things', 'muppets', data)

    def test_update_one(self, conn):
        expected = {'id': 'kermit-id', 'species': 'green frog', 'name': 'Kermit'}
        r.db('things').table('muppets').get('kermit-id').update({'species': 'green frog'}).run(conn)
        result = r.db('things').table('muppets').get('kermit-id').run(conn)
        self.assertEqual(expected, result)

    def test_update_sequence(self, conn):
        expected = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit', 'is_muppet': 'very'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy', 'is_muppet': 'very'}
        ]
        r.db('things').table('muppets').update({'is_muppet': 'very'}).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqual(expected, list(result))

    def test_insert_one(self, conn):
        expected = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'},
            {'id': 'elmo-id', 'species': 'methhead', 'name': 'Elmo'}
        ]
        r.db('things').table('muppets').insert({
            'id': 'elmo-id',
            'species': 'methhead',
            'name': 'Elmo'
        }).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_insert_array(self, conn):
        expected = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'},
            {'id': 'elmo-id', 'species': 'methhead', 'name': 'Elmo'},
            {'id': 'fonz-id', 'species': 'guido', 'name': 'The Fonz'}
        ]
        r.db('things').table('muppets').insert([
            {
                'id': 'elmo-id',
                'species': 'methhead',
                'name': 'Elmo'
            },
            {
                'id': 'fonz-id',
                'species': 'guido',
                'name': 'The Fonz'
            }
        ]).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_insert_one_no_id(self, conn):
        r.db('things').table('muppets').insert({
            'name': 'joe'
        }).run(conn)
        result = r.db('things').table('muppets').filter({
            'name': 'joe'
        }).run(conn)
        result = list(result)
        self.assertEqual(1, len(result))
        joe = result[0]
        assert(isinstance(joe['id'], unicode))

    def test_insert_array_no_ids(self, conn):
        r.db('things').table('muppets').insert([
            {
                'name': 'joe',
                'wanted': True
            },
            {
                'name': 'todd',
                'wanted': True
            }
        ]).run(conn)
        result = r.db('things').table('muppets').filter({
            'wanted': True
        }).run(conn)
        result = list(result)
        self.assertEqual(2, len(result))
        assert(isinstance(result[0]['id'], unicode))
        assert(isinstance(result[1]['id'], unicode))


class TestDelete(MockTest):
    def get_data(self):
        data = [
            {'id': 'sam-id', 'name': 'sam'},
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'tom-id', 'name': 'tom'},
            {'id': 'sally-id', 'name': 'sally'}
        ]
        return as_db_and_table('ephemeral', 'people', data)

    def test_delete_one(self, conn):
        expected = [
            {'id': 'sam-id', 'name': 'sam'},
            {'id': 'tom-id', 'name': 'tom'},
            {'id': 'sally-id', 'name': 'sally'}
        ]
        r.db('ephemeral').table('people').get('joe-id').delete().run(conn)
        result = r.db('ephemeral').table('people').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_delete_n(self, conn):
        expected = [
            {'id': 'sally-id', 'name': 'sally'},
            {'id': 'joe-id', 'name': 'joe'}
        ]
        r.db('ephemeral').table('people').get_all('sam-id', 'tom-id').delete().run(conn)
        result = r.db('ephemeral').table('people').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_delete_all_table_rows(self, conn):
        expected = []
        r.db('ephemeral').table('people').delete().run(conn)
        result = r.db('ephemeral').table('people').run(conn)
        self.assertEqUnordered(expected, list(result))
