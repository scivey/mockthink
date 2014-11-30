import rethinkdb as r
from rethinkdb import RqlRuntimeError
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

    def test_replace_selected(self, conn):
        expected = [
            {'id': 'kermit-id', 'name': 'Just Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        r.db('things').table('muppets').get('kermit-id').replace({'id': 'kermit-id', 'name': 'Just Kermit'}).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, result)

    def test_replace_one_from_table(self, conn):
        expected = [
            {'id': 'kermit-id', 'name': 'Just Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        r.db('things').table('muppets').replace({'id': 'kermit-id', 'name': 'Just Kermit'}).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, result)

    def test_replace_one_from_sequence(self, conn):
        expected = [
            {'id': 'kermit-id', 'name': 'Just Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        r.db('things').table('muppets').filter(
            lambda doc: True
        ).replace({'id': 'kermit-id', 'name': 'Just Kermit'}).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, result)



class TestInsert(MockTest):
    def get_data(self):
        data = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        return as_db_and_table('things', 'muppets', data)

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


class TestUpdate(MockTest):
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


class TestUpdateRql(MockTest):
    def get_data(self):
        data = [
            {'id': 'kermit-id', 'species': 'frog', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'pig', 'name': 'Ms. Piggy'}
        ]
        return as_db_and_table('things', 'muppets', data)

    def test_update_many(self, conn):
        expected = [
            {'id': 'kermit-id', 'species': 'unknown', 'name': 'Kermit'},
            {'id': 'piggy-id', 'species': 'unknown', 'name': 'Ms. Piggy'}
        ]
        r.db('things').table('muppets').update(
            r.row.merge({'species': 'unknown'})
        ).run(conn)
        result = r.db('things').table('muppets').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_update_one(self, conn):
        expected = {'id': 'kermit-id', 'species': 'unknown', 'name': 'Kermit'}
        r.db('things').table('muppets').get('kermit-id').update(
            r.row.merge({'species': 'unknown'})
        ).run(conn)
        result = r.db('things').table('muppets').get('kermit-id').run(conn)
        self.assertEqual(expected, result)

class TestNestedUpdateNotLit(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'one',
                'points': {
                    'pt1': {
                        'x': 'x-1',
                        'y': 'y-1'
                        }
                    }
            },
            {
                'id': 'two',
                'points': {
                    'pt1': {
                        'x': 'x-2',
                        'y': 'y-2'
                        }
                    }
            },
            {
                'id': 'three',
                'things': {
                    'x': [1, 3, 5]
                }
            }
        ]
        return as_db_and_table('things', 'points', data)

    def test_update_merge(self, conn):
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'x': 'x-1',
                    'y': 'y-1',
                    'z': 'z-1'
                    }
            }
        }

        r.db('things').table('points').filter({'id': 'one'}).update(
            r.row.merge({'points': {'pt1': {'z': 'z-1'}}})
        ).run(conn)
        result = r.db('things').table('points').get('one').run(conn)
        self.assertEqual(expected, result)

    def test_update_no_merge(self, conn):
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'x': 'x-1',
                    'y': 'y-1',
                    'z': 'z-1'
                    }
            }
        }

        r.db('things').table('points').filter({'id': 'one'}).update(
            {'points': {'pt1': {'z': 'z-1'}}}
        ).run(conn)
        result = r.db('things').table('points').get('one').run(conn)
        self.assertEqual(expected, result)

    def test_update_merge_deep(self, conn):
        # this behavior is pretty weird, but it's what rethink does
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'x': 'x-1',
                    'y': 'y-1'
                    }
            },
            'pt1': {
                'x': 'x-1',
                'y': 'y-1',
                'z': 'z-1'
            }
        }

        r.db('things').table('points').filter({'id': 'one'}).update(
            r.row['points'].merge({'pt1': {'z': 'z-1'}})
        ).run(conn)
        result = r.db('things').table('points').get('one').run(conn)
        pprint({'merge_deep': result})
        self.assertEqual(expected, result)

    def test_update_merge_array(self, conn):
        expected = {
            'id': 'three',
            'things': {
                'x': [1, 3, 5, 7, 9]
            }
        }
        r.db('things').table('points').filter({'id': 'three'}).update(
            r.row.merge({'things': {'x': [7, 9]}})
        ).run(conn)
        result = r.db('things').table('points').get('three').run(conn)
        pprint(result)
        self.assertEqUnordered(expected, result)

    def test_update_no_merge_array(self, conn):
        expected = {
            'id': 'three',
            'things': {
                'x': [1, 3, 5, 7, 9]
            }
        }
        r.db('things').table('points').filter({'id': 'three'}).update(
            {'things': {'x': [7, 9]}}
        ).run(conn)
        result = r.db('things').table('points').get('three').run(conn)
        pprint(result)
        self.assertEqUnordered(expected, result)

    def test_update_merge_array_deep(self, conn):
        expected = {
            'id': 'three',
            'things': {
                'x': [1, 3, 5]
            },
            'x': [1, 3, 5, 7, 9]
        }
        r.db('things').table('points').filter({'id': 'three'}).update(
            r.row['things'].merge({'x': [7, 9]})
        ).run(conn)
        result = r.db('things').table('points').get('three').run(conn)
        pprint(result)
        self.assertEqUnordered(expected, result)


class TestLiteral(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'one',
                'points': {
                    'pt1': {
                        'x': 'x-1',
                        'y': 'y-1'
                        }
                    }
            },
            {
                'id': 'two',
                'points': {
                    'pt1': {
                        'x': 'x-2',
                        'y': 'y-2'
                        }
                    }
            },
            {
                'id': 'three',
                'things': {
                    'x': [1, 3, 5]
                }
            }
        ]
        return as_db_and_table('things', 'points', data)

    def test_map_merge_no_literal(self, conn):
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'x': 'x-1',
                    'y': 'y-1',
                    'z': 'z-1'
                }
            }
        }

        result = r.db('things').table('points').filter({'id': 'one'}).map(
            lambda doc: doc.merge({'points': {'pt1': {'z': 'z-1'}}})
        ).run(conn)
        self.assertEqual(expected, list(result)[0])

    def test_map_merge_literal(self, conn):
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'z': 'z-1'
                }
            }
        }
        result = r.db('things').table('points').filter({'id': 'one'}).map(
            lambda doc: doc.merge({'points': {'pt1': r.literal({'z': 'z-1'})}})
        ).run(conn)
        self.assertEqual(expected, list(result)[0])

    def test_top_level_literal_throws_merge(self, conn):
        err = None
        try:
            result = r.db('things').table('points').filter({'id': 'one'}).map(
                lambda doc: doc.merge(r.literal({'points': {'pt1': {'z': 'z-1'}}}))
            ).run(conn)
        except RqlRuntimeError as e:
            err = e
        assert(isinstance(err, RqlRuntimeError))

    def test_nested_literal_throws_merge(self, conn):
        err = None
        result = None
        try:
            result = r.db('things').table('points').filter({'id': 'one'}).map(
                lambda doc: doc.merge({'points': r.literal({'pt1': r.literal({'z': 'z-1'})})})
            ).run(conn)
        except RqlRuntimeError as e:
            err = e
        pprint(err)
        pprint(result)
        assert(isinstance(err, RqlRuntimeError))

    def test_update_literal(self, conn):
        expected = {
            'id': 'one',
            'points': {
                'pt1': {
                    'z': 'z-1'
                }
            }
        }
        r.db('things').table('points').filter({'id': 'one'}).update(
            {'points': r.literal({'pt1': {'z': 'z-1'}})}
        ).run(conn)
        result = r.db('things').table('points').get('one').run(conn)
        self.assertEqual(expected, result)

    # def test_top_level_literal_on_update_does_nothing(self, conn):
    #     expected = {
    #         'id': 'one',
    #         'points': {
    #             'pt1': {
    #                 'x': 'x-1',
    #                 'y': 'y-1'
    #             }
    #         }
    #     }
    #     r.db('things').table('points').filter({'id': 'one'}).update(
    #         r.literal({'points': {'pt1': {'z': 'z-1'}}})
    #     ).run(conn)
    #     result = r.db('things').table('points').get('one').run(conn)
    #     self.assertEqual(expected, result)

    # def test_nested_literal_throws_update(self, conn):
    #     err = None
    #     try:
    #         r.db('things').table('points').filter({'id': 'one'}).update(
    #             {'points': r.literal({'pt1': r.literal({'z': 'z-1'})})}
    #         ).run(conn)
    #     except RqlRuntimeError as e:
    #         err = e
    #     assert(isinstance(err, RqlRuntimeError))


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

    def test_delete_one_from_sequence(self, conn):
        expected = [
            {'id': 'sam-id', 'name': 'sam'},
            {'id': 'tom-id', 'name': 'tom'},
            {'id': 'sally-id', 'name': 'sally'}
        ]
        r.db('ephemeral').table('people').filter({
            'id': 'joe-id'
        }).delete().run(conn)
        result = r.db('ephemeral').table('people').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_delete_get_all(self, conn):
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
