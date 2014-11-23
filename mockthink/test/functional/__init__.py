import argparse
import sys
from pprint import pprint
import rethinkdb as r
from mockthink.db import MockThink, MockThinkConn
from mockthink.test.common import make_test_registry, AssertionMixin
from mockthink.test.common import as_db_and_table
from mockthink import util

TESTS = {}
register_test = make_test_registry(TESTS)

class Meta(type):
    def __new__(cls, name, bases, attrs):
        result = super(Meta, cls).__new__(cls, name, bases, attrs)
        tests = [name for name in attrs.keys() if 'test' in name]
        register_test(result, result.__name__, tests)
        return result

class Base(object):
    __metaclass__ = Meta

class MockTest(Base, AssertionMixin):
    def get_data(self):
        return {
            'dbs': {
                'default': {
                    'tables': {}
                }
            }
        }


class TestGet(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_get_one_by_id(self, conn):
        result = r.db('x').table('people').get('bob-id').run(conn)
        self.assertEqual({'id': 'bob-id', 'name': 'bob'}, result)


class TestGetAll(MockTest):
    def get_data(self):
        data = [
            {'id': 'sam-id', 'name': 'sam'},
            {'id': 'anne-id', 'name': 'anne'},
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_get_all_by_id(self, conn):
        expected = [
            {'id': 'anne-id', 'name': 'anne'},
            {'id': 'joe-id', 'name': 'joe'},
        ]
        result = r.db('x').table('people').get_all('anne-id', 'joe-id').run(conn)
        pprint(result)
        self.assertEqUnordered(expected, result)

    def test_get_all_just_one(self, conn):
        expected = [
            {'id': 'bob-id', 'name': 'bob'},
        ]
        result = r.db('x').table('people').get_all('bob-id').run(conn)
        self.assertEqual(expected, result)



class TestFiltering(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'bill-id', 'name': 'bill', 'age': 35},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        return as_db_and_table('x', 'people', data)

    def test_filter_lambda_gt(self, conn):
        expected = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bill-id', 'name': 'bill', 'age': 35}
        ]
        result = r.db('x').table('people').filter(lambda p: p['age'] > 20).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_filter_lambda_lt(self, conn):
        expected = [
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        result = r.db('x').table('people').filter(lambda p: p['age'] < 20).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_filter_dict_match(self, conn):
        expected = [{'id': 'bill-id', 'name': 'bill', 'age': 35}]
        result = r.db('x').table('people').filter({'age': 35}).run(conn)
        self.assertEqual(expected, list(result))


class TestMapping(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe', 'age': 28},
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'bill-id', 'name': 'bill', 'age': 35},
            {'id': 'kimye-id', 'name': 'kimye', 'age': 17}
        ]
        return as_db_and_table('x', 'people', data)

    def test_map_gt(self, conn):
        expected = [
            True, False, True, False
        ]
        result = r.db('x').table('people').map(lambda p: p['age'] > 20).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestPlucking(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe', 'hobby': 'guitar'},
            {'id': 'bob-id', 'name': 'bob', 'hobby': 'pseudointellectualism'},
            {'id': 'bill-id', 'name': 'bill'},
            {'id': 'kimye-id', 'name': 'kimye', 'hobby': 'being kimye'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_pluck_missing_attr(self, conn):
        expected = [
            {'id': 'joe-id', 'hobby': 'guitar'},
            {'id': 'bob-id', 'hobby': 'pseudointellectualism'},
            {'id': 'bill-id'},
            {'id': 'kimye-id', 'hobby': 'being kimye'}
        ]
        result = r.db('x').table('people').pluck('id', 'hobby').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_pluck_missing_attr_list(self, conn):
        expected = [
            {'id': 'joe-id', 'hobby': 'guitar'},
            {'id': 'bob-id', 'hobby': 'pseudointellectualism'},
            {'id': 'bill-id'},
            {'id': 'kimye-id', 'hobby': 'being kimye'}
        ]
        result = r.db('x').table('people').pluck(['id', 'hobby']).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_pluck(self, conn):
        expected = [
            {'id': 'joe-id', 'hobby': 'guitar'},
            {'id': 'bob-id', 'hobby': 'pseudointellectualism'},
            {'id': 'bill-id'},
            {'id': 'kimye-id', 'hobby': 'being kimye'}
        ]
        result = r.db('x').table('people').map(lambda p: p.pluck('id', 'hobby')).run(conn)
        self.assertEqUnordered(expected, list(result))


class TestPlucking2(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'thing-1',
                'values': {
                    'a': 'a-1',
                    'b': 'b-1',
                    'c': 'c-1',
                    'd': 'd-1'
                }
            },
            {
                'id': 'thing-2',
                'values': {
                    'a': 'a-2',
                    'b': 'b-2',
                    'c': 'c-2',
                    'd': 'd-2'
                }
            },
        ]
        return as_db_and_table('some_db', 'things', data)

    def test_sub_sub(self, conn):
        expected = [
            {'a': 'a-1', 'd': 'd-1'},
            {'a': 'a-2', 'd': 'd-2'}
        ]
        result = r.db('some_db').table('things').map(lambda t: t['values'].pluck('a', 'd')).run(conn)
        pprint(result)
        self.assertEqUnordered(expected, list(result))

    def test_sub_sub_list(self, conn):
        expected = [
            {'a': 'a-1', 'd': 'd-1'},
            {'a': 'a-2', 'd': 'd-2'}
        ]
        result = r.db('some_db').table('things').map(lambda t: t['values'].pluck('a', 'd')).run(conn)
        self.assertEqUnordered(expected, list(result))



class TestWithout(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe', 'hobby': 'guitar'},
            {'id': 'bob-id', 'name': 'bob', 'hobby': 'pseudointellectualism'},
            {'id': 'bill-id', 'name': 'bill'},
            {'id': 'kimye-id', 'name': 'kimye', 'hobby': 'being kimye'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_without_missing_attr(self, conn):
        expected = [
            {'id': 'joe-id'},
            {'id': 'bob-id'},
            {'id': 'bill-id'},
            {'id': 'kimye-id'}
        ]
        result = r.db('x').table('people').without('name', 'hobby').run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_without_missing_attr_list(self, conn):
        expected = [
            {'id': 'joe-id'},
            {'id': 'bob-id'},
            {'id': 'bill-id'},
            {'id': 'kimye-id'}
        ]
        result = r.db('x').table('people').without(['name', 'hobby']).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_without(self, conn):
        expected = [
            {'id': 'joe-id'},
            {'id': 'bob-id'},
            {'id': 'bill-id'},
            {'id': 'kimye-id'}
        ]
        result = r.db('x').table('people').map(lambda p: p.without('name', 'hobby')).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_without_list(self, conn):
        expected = [
            {'id': 'joe-id'},
            {'id': 'bob-id'},
            {'id': 'bill-id'},
            {'id': 'kimye-id'}
        ]
        result = r.db('x').table('people').map(lambda p: p.without(['name', 'hobby'])).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestWithout2(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'thing-1',
                'values': {
                    'a': 'a-1',
                    'b': 'b-1',
                    'c': 'c-1',
                    'd': 'd-1'
                }
            },
            {
                'id': 'thing-2',
                'values': {
                    'a': 'a-2',
                    'b': 'b-2',
                    'c': 'c-2',
                    'd': 'd-2'
                }
            },
        ]
        return as_db_and_table('some_db', 'things', data)

    def test_sub_sub(self, conn):
        expected = [
            {'b': 'b-1', 'c': 'c-1'},
            {'b': 'b-2', 'c': 'c-2'}
        ]
        result = r.db('some_db').table('things').map(lambda t: t['values'].without('a', 'd')).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_sub_sub_list(self, conn):
        expected = [
            {'b': 'b-1', 'c': 'c-1'},
            {'b': 'b-2', 'c': 'c-2'}
        ]
        result = r.db('some_db').table('things').map(lambda t: t['values'].without(['a', 'd'])).run(conn)
        self.assertEqUnordered(expected, list(result))


class TestBracket(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'thing-1',
                'other_val': 'other-1',
                'values': {
                    'a': 'a-1',
                    'b': 'b-1',
                    'c': 'c-1',
                    'd': 'd-1'
                }
            },
            {
                'id': 'thing-2',
                'other_val': 'other-2',
                'values': {
                    'a': 'a-2',
                    'b': 'b-2',
                    'c': 'c-2',
                    'd': 'd-2'
                }
            },
        ]
        return as_db_and_table('some_db', 'things', data)

    def test_one_level(self, conn):
        expected = ['other-1', 'other-2']
        result = r.db('some_db').table('things').map(lambda t: t['other_val']).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_nested(self, conn):
        expected = ['c-1', 'c-2']
        result = r.db('some_db').table('things').map(lambda t: t['values']['c']).run(conn)
        self.assertEqUnordered(expected, list(result))

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

    def test_div_method(self, conn):
        expected = [250, 300]
        result = r.db('math_db').table('points').map(lambda t: t['x'].mul(t['y'])).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_mul_oper(self, conn):
        expected = [250, 300]
        result = r.db('math_db').table('points').map(lambda t: t['x'] * t['y']).run(conn)
        self.assertEqUnordered(expected, list(result))


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



class TestHasFields(MockTest):
    def get_data(self):
        people = [
            {'id': 'joe', 'first_name': 'Joe', 'age': 26},
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'phil', 'first_name': 'Phil', 'last_name': 'LastPhil'},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}

        ]
        return as_db_and_table('x', 'people', people)

    def test_has_fields_1(self, conn):
        expected = [
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}
        ]
        result = r.db('x').table('people').has_fields('last_name', 'age').run(conn)
        self.assertEqUnordered(expected, result)

    def test_has_fields_array(self, conn):
        expected = [
            {'id': 'todd', 'first_name': 'Todd', 'last_name': 'Last', 'age': 35},
            {'id': 'sam', 'first_name': 'Sam', 'last_name': 'SamLast', 'age': 31}
        ]
        result = r.db('x').table('people').has_fields(['last_name', 'age']).run(conn)
        self.assertEqUnordered(expected, result)


def common_join_data():
    people_data = [
        {'id': 'joe-id', 'name': 'Joe'},
        {'id': 'tom-id', 'name': 'Tom'},
        {'id': 'arnold-id', 'name': 'Arnold'}
    ]
    job_data = [
        {'id': 'lawyer-id', 'name': 'Lawyer'},
        {'id': 'nurse-id', 'name': 'Nurse'},
        {'id': 'semipro-wombat-id', 'name': 'Semi-Professional Wombat'}
    ]
    employee_data = [
        {'id': 'joe-emp-id', 'person': 'joe-id', 'job': 'lawyer-id'},
        {'id': 'arnold-emp-id', 'person': 'arnold-id', 'job': 'nurse-id'}
    ]
    data = {
        'dbs': {
            'jezebel': {
                'tables': {
                    'people': people_data,
                    'jobs': job_data,
                    'employees': employee_data
                }
            }
        }

    }
    return data

class TestEqJoin(MockTest):
    def get_data(self):
        return common_join_data()

    def test_eq_join_1(self, conn):
        expected = [
            {
                'left': {
                    'id': 'joe-emp-id',
                    'person': 'joe-id',
                    'job': 'lawyer-id'
                },
                'right': {
                    'id': 'joe-id',
                    'name': 'Joe'
                }
            },
            {
                'left': {
                    'id': 'arnold-emp-id',
                    'person': 'arnold-id',
                    'job': 'nurse-id'
                },
                'right': {
                    'id': 'arnold-id',
                    'name': 'Arnold'
                }
            }
        ]
        result = r.db('jezebel').table('employees').eq_join('person', r.db('jezebel').table('people')).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestInnerJoin(MockTest):
    def get_data(self):
        return common_join_data()

    def test_inner_join_1(self, conn):
        expected = [
            {
                'left': {
                    'id': 'joe-emp-id',
                    'person': 'joe-id',
                    'job': 'lawyer-id'
                },
                'right': {
                    'id': 'joe-id',
                    'name': 'Joe'
                }
            },
            {
                'left': {
                    'id': 'arnold-emp-id',
                    'person': 'arnold-id',
                    'job': 'nurse-id'
                },
                'right': {
                    'id': 'arnold-id',
                    'name': 'Arnold'
                }
            }
        ]
        result = r.db('jezebel').table('employees').inner_join(
            r.db('jezebel').table('people'),
            lambda employee, person: employee['person'] == person['id']
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

class TestOuterJoin(MockTest):
    def get_data(self):
        people = [
            {'id': 'sam-id', 'name': 'Sam'},
            {'id': 'miguel-id', 'name': 'Miguel'},
            {'id': 'mark-id', 'name': 'Mark'}
        ]
        pets = [
            {'id': 'pet1-id', 'name': 'Pet1', 'owner': 'miguel-id'},
            {'id': 'pet2-id', 'name': 'Pet2', 'owner': 'mark-id'},
            {'id': 'pet3-id', 'name': 'Pet3', 'owner': 'miguel-id'},
        ]
        return {
            'dbs': {
                'awesomesauce': {
                    'tables': {
                        'pets': pets,
                        'people': people
                    }
                }
            }
        }

    def test_outer_join_1(self, conn):
        expected = [
            {
                'left': {
                    'id': 'miguel-id',
                    'name': 'Miguel'
                },
                'right': {
                    'id': 'pet1-id',
                    'name': 'Pet1',
                    'owner': 'miguel-id'
                }
            },
            {
                'left': {
                    'id': 'miguel-id',
                    'name': 'Miguel'
                },
                'right': {
                    'id': 'pet3-id',
                    'name': 'Pet3',
                    'owner': 'miguel-id'
                }
            },
            {
                'left': {
                    'id': 'mark-id',
                    'name': 'Mark'
                },
                'right': {
                    'id': 'pet2-id',
                    'name': 'Pet2',
                    'owner': 'mark-id'
                }
            },
            {
                'left': {
                    'id': 'sam-id',
                    'name': 'Sam'
                }
            }
        ]
        result = r.db('awesomesauce').table('people').outer_join(
            r.db('awesomesauce').table('pets'),
            lambda person, pet: pet['owner'] == person['id']
        ).run(conn)
        self.assertEqUnordered(expected, list(result))


class TestMerge(MockTest):
    def get_data(self):
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
        self.assertEqUnordered(expected, list(result))

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
        self.assertEqUnordered(expected, list(result))

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
        pprint(result)
        self.assertEqUnordered(expected, list(result))

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
        self.assertEqUnordered(expected, list(result))


class TestIsEmpty(MockTest):
    def get_data(self):
        data = [
            {'id': 'id-1', 'things': []},
            {'id': 'id-2', 'things': ['x', 'y']}
        ]
        return as_db_and_table('some_db', 'some_table', data)

    def test_is_empty_nested(self, conn):
        expected = [
            {'id': 'id-1', 'things_empty': True, 'things': []},
            {'id': 'id-2', 'things_empty': False, 'things': ['x', 'y']}
        ]
        result = r.db('some_db').table('some_table').map(
            lambda d: d.merge({'things_empty': d['things'].is_empty()})
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_is_empty_toplevel_empty(self, conn):
        result = r.db('some_db').table('some_table').filter({
            'some_key': 'some-value'
        }).is_empty().run(conn)
        self.assertEqual(True, result)

    def test_is_empty_toplevel_not_empty(self, conn):
        result = r.db('some_db').table('some_table').has_fields('things').is_empty().run(conn)
        self.assertEqual(False, result)


class TestGroup(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe', 'type': 'bro'},
            {'id': 'bill', 'type': 'hipster'},
            {'id': 'todd', 'type': 'hipster'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_group_by_field(self, conn):
        expected = {
            'bro': [
                {'id': 'joe', 'type': 'bro'},
            ],
            'hipster': [
                {'id': 'bill', 'type': 'hipster'},
                {'id': 'todd', 'type': 'hipster'}
            ]
        }
        result = r.db('x').table('people').group('type').run(conn)
        self.assertEqual(expected['bro'], result['bro'])
        self.assertEqUnordered(expected['hipster'], result['hipster'])
        self.assertEqual(set(['bro', 'hipster']), set(result.keys()))

    def test_group_by_func(self, conn):
        expected = {
            'bro': [
                {'id': 'joe', 'type': 'bro'},
            ],
            'hipster': [
                {'id': 'bill', 'type': 'hipster'},
                {'id': 'todd', 'type': 'hipster'}
            ]
        }
        result = r.db('x').table('people').group(lambda d: d['type']).run(conn)
        self.assertEqual(expected['bro'], result['bro'])
        self.assertEqUnordered(expected['hipster'], result['hipster'])
        self.assertEqual(set(['bro', 'hipster']), set(result.keys()))


class TestMax(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe', 'age': 26, 'hobbies': ['sand', 'water', 'cats']},
            {'id': 'bill', 'age': 35, 'hobbies': ['watermelon']},
            {'id': 'todd', 'age': 52, 'hobbies': ['citrus']}
        ]
        return as_db_and_table('x', 'people', data)

    def test_max_of_field(self, conn):
        expected = {'id': 'todd', 'age': 52, 'hobbies': ['citrus']}
        result = r.db('x').table('people').max('age').run(conn)
        self.assertEqual(expected, result)

    def test_max_of_func_return_val(self, conn):
        expected = {'id': 'joe', 'age': 26, 'hobbies': ['sand', 'water', 'cats']}
        result = r.db('x').table('people').max(lambda d: d['hobbies'].count()).run(conn)
        self.assertEqual(expected, result)


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
        pprint(result)
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
        pprint(result)
        self.assertEqUnordered(expected, list(result))

class TestObjectManip(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'joe',
                'attributes': {
                    'face': 'bad',
                    'toes': 'fugly'
                },
                'joe-attr': True
            },
            {
                'id': 'sam',
                'attributes': {
                    'face': 'eh',
                    'blog': 'dry'
                },
                'sam-attr': True
            }
        ]
        return as_db_and_table('y', 'people', data)

    def test_keys_document(self, conn):
        expected = [
            ['id', 'attributes', 'joe-attr'],
            ['id', 'attributes', 'sam-attr']
        ]
        result = list(r.db('y').table('people').map(
            lambda d: d.keys()
        ).run(conn))
        self.assertEqual(3, len(result[0]))
        self.assertEqual(3, len(result[1]))
        key_set = set(util.cat(result[0], result[1]))
        self.assertEqual(set(['id', 'attributes', 'joe-attr', 'sam-attr']), key_set)


    def test_keys_nested(self, conn):
        expected = [
            ['face', 'toes'],
            ['face', 'blog']
        ]
        result = list(r.db('y').table('people').map(
            lambda d: d['attributes'].keys()
        ).run(conn))
        self.assertEqual(2, len(result[0]))
        self.assertEqual(2, len(result[1]))

        key_set = set(util.cat(result[0], result[1]))
        self.assertEqual(set(['face', 'toes', 'blog']), key_set)

class TestStrings(MockTest):
    def get_data(self):
        data = [
            {'id': 'a', 'text': 'something  with spaces'},
            {'id': 'b', 'text': 'some,csv,file'},
            {'id': 'c', 'text': 'someething'}
        ]
        return as_db_and_table('library', 'texts', data)

    def test_upcase(self, conn):
        expected = set([
            'SOMETHING  WITH SPACES',
            'SOME,CSV,FILE',
            'SOMEETHING'
        ])
        result =  r.db('library').table('texts').map(
            lambda doc: doc['text'].upcase()
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

    def test_downcase(self, conn):
        expected = set([
            'something  with spaces',
            'some,csv,file',
            'someething'
        ])
        result =  r.db('library').table('texts').map(
            lambda doc: doc['text'].downcase()
        ).run(conn)
        self.assertEqual(expected, set(list(result)))

    def test_split_1(self, conn):
        expected = [
            ['something', 'with', 'spaces'],
            ['some,csv,file'],
            ['someething']
        ]
        result = r.db('library').table('texts').map(
            lambda doc: doc['text'].split()
        ).run(conn)
        self.assertEqUnordered(expected, list(result))

    def test_split_2(self, conn):
        expected = [
            ['something  with spaces'],
            ['some', 'csv', 'file'],
            ['someething']
        ]
        result = r.db('library').table('texts').map(
            lambda doc: doc['text'].split(',')
        ).run(conn)
        self.assertEqUnordered(expected, result)

    def test_split_3(self, conn):
        expected = [
            ['som', 'thing  with spac', 's'],
            ['som', ',csv,fil', ''],
            ['som', '', 'thing']
        ]
        result = r.db('library').table('texts').map(
            lambda doc: doc['text'].split('e')
        ).run(conn)
        pprint(result)
        self.assertEqUnordered(expected, list(result))

    def test_split_4(self, conn):
        expected = [
            ['som', 'thing  with spaces'],
            ['som', ',csv,file'],
            ['som', 'ething']
        ]
        result = r.db('library').table('texts').map(
            lambda doc: doc['text'].split('e', 1)
        ).run(conn)
        pprint(result)
        self.assertEqUnordered(expected, list(result))



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

    # def test_delete_n(self, conn):
    #     expected = [
    #         {'id': 'sally-id', 'name': 'sally'},
    #         {'id': 'joe-id', 'name': 'joe'}
    #     ]
    #     r.db('ephemeral').table('people').get_all('sam-id', 'tom-id').delete().run(conn)
    #     result = r.db('ephemeral').table('people').run(conn)
    #     self.assertEqUnordered(expected, list(result))




def run_tests(conn, grep):
    for test_name, test_fn in TESTS.iteritems():
        if not grep or grep == 'ALL':
            test_fn(conn)
        elif grep in test_name:
            test_fn(conn)
        else:
            print 'skipping: %s' % test_name

def run_tests_with_mockthink(grep):
    think = MockThink(as_db_and_table('nothing', 'nothing', []))
    run_tests(think.get_conn(), grep)

def run_tests_with_rethink(grep):
    conn = r.connect('localhost', 28015)
    run_tests(conn, grep)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    runners = {
        'mockthink': run_tests_with_mockthink,
        'rethink': run_tests_with_rethink
    }

    parser.add_argument('--run', default='mockthink')
    parser.add_argument('--grep', default=None)
    args = parser.parse_args(sys.argv[1:])
    runners[args.run](args.grep)

