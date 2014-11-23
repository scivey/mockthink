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


class TestOrderByOne(MockTest):
    def get_data(self):
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
        self.assertEqual(expected, list(result))


    def test_sort_1_attr_asc(self, conn):
        expected = [
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age')).run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_1_attr_desc(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'joe', 'age': 26, 'score': 60}
        ]
        result = r.db('y').table('scores').order_by(r.desc('age')).run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_1_attr_2(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
        ]
        result = r.db('y').table('scores').order_by('score').run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_1_attr_2_asc(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
        ]
        result = r.db('y').table('scores').order_by(r.asc('score')).run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_1_attr_2_desc(self, conn):
        expected = [
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'todd', 'age': 52, 'score': 15},
        ]
        result = r.db('y').table('scores').order_by(r.desc('score')).run(conn)
        self.assertEqual(expected, list(result))


class TestOrderByMulti(MockTest):
    def get_data(self):
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
        self.assertEqual(expected, list(result))

    def test_sort_multi_1_asc(self, conn):
        expected = [
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age'), r.asc('score')).run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_multi_1_desc_1(self, conn):
        expected = [
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'todd', 'age': 52, 'score': 15}
        ]
        result = r.db('y').table('scores').order_by(r.asc('age'), r.desc('score')).run(conn)
        self.assertEqual(expected, list(result))

    def test_sort_multi_1_desc_2(self, conn):
        expected = [
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'bill', 'age': 35, 'score': 78},
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'joe', 'age': 26, 'score': 60}
        ]
        result = r.db('y').table('scores').order_by(r.desc('age'), 'score').run(conn)
        self.assertEqual(expected, list(result))



    def test_sort_multi_2(self, conn):
        expected = [
            {'id': 'glen', 'age': 26, 'score': 15},
            {'id': 'todd', 'age': 52, 'score': 15},
            {'id': 'pale', 'age': 52, 'score': 30},
            {'id': 'joe', 'age': 26, 'score': 60},
            {'id': 'bill', 'age': 35, 'score': 78}
        ]
        result = r.db('y').table('scores').order_by('score', 'age').run(conn)
        self.assertEqual(expected, list(result))


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

    def test_delete_at(self, conn):
        expected = [
            ['cow'],
            []
        ]
        result = r.db('x').table('farms').map(
            lambda d: d['animals'].delete_at(0)
        ).run(conn)
        res = list(result)
        pprint(res)
        self.assertEqUnordered(expected, res)

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
        self.assertEqUnordered(expected, result)


class TestIndexesOf(MockTest):
    def get_data(self):
        things = [
            {'id': 'one', 'letters': ['c', 'c']},
            {'id': 'two', 'letters': ['a', 'b', 'a', ['q', 'q'], 'b']},
            {'id': 'three', 'letters': ['b', 'a', 'b', 'a']},
            {'id': 'three', 'letters': ['c', 'a', 'b', 'a', ['q', 'q']]}
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
        pprint(result)
        self.assertEqUnordered(expected, list(result))

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
        pprint(result)
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
        pprint(result)
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


class TestDo(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'one',
                'name': 'One',
                'pets': ['dog', 'cat', 'bird']
            },
            {
                'id': 'two',
                'name': 'Two',
                'pets': ['fish', 'another fish']
            },
            {
                'id': 'three',
                'name': 'Three',
                'pets': ['toad', 'rabbit']
            }
        ]
        return as_db_and_table('generic', 'table', data)

    def test_do_simple_1(self, conn):
        result = r.db('generic').table('table').get('one').do(
            lambda d: d['name']
        ).run(conn)
        self.assertEqual('One', result)

    def test_do_simple_2(self, conn):
        result = r.do(lambda d: d['name'],
            r.db('generic').table('table').get('two')
        ).run(conn)
        self.assertEqual('Two', result)

    def test_do_two(self, conn):
        base = r.db('generic').table('table')
        result = r.do(base.get('one'), base.get('two'),
            lambda d1, d2: [d1['name'], d2['name']]
        ).run(conn)
        self.assertEqual(['One', 'Two'], result)

    def test_do_three(self, conn):
        base = r.db('generic').table('table')
        result = r.do(
            base.get('one'),
            base.get('two'),
            base.get('three'),
            lambda d1, d2, d3: [d1['name'], d2['name'], d3['name']]
        ).run(conn)
        self.assertEqual(['One', 'Two', 'Three'], result)


class TestZip(MockTest):
    def get_data(self):
        left = [
            {
                'id': 'one',
                'lname': 'One',
                'rval': 'r-one'
            },
            {
                'id': 'two',
                'lname': 'Two',
                'rval': 'r-two'
            }
        ]
        right = [
            {
                'id': 'r-one',
                'rname': 'RightOne'
            },
            {
                'id': 'r-two',
                'rname': 'RightTwo'
            }
        ]
        return {
            'dbs': {
                'x': {
                    'tables': {
                        'ltab': left,
                        'rtab': right
                    }
                }
            }


        }

    def test_zip_1(self, conn):
        expected = [
            {
                'id': 'r-one',
                'lname': 'One',
                'rname': 'RightOne',
                'rval': 'r-one'
            },
            {
                'id': 'r-two',
                'lname': 'Two',
                'rname': 'RightTwo',
                'rval': 'r-two'
            }
        ]
        result = r.db('x').table('ltab').eq_join(
            'rval', r.db('x').table('rtab')
        ).zip().run(conn)
        self.assertEqUnordered(expected, list(result))


class TestSets(MockTest):
    def get_data(self):
        data = [
            {
                'id': 'one',
                'simple': ['x', 'y'],
                'complex': [{'val': 10}, {'val': 16}]
            },
            {
                'id': 'two',
                'simple': ['x', 'z'],
                'complex': [{'val': 10}]
            }
        ]
        return as_db_and_table('z', 't', data)

    def test_set_insert(self, conn):
        expected = [
            set(['x', 'y']),
            set(['x', 'y', 'z'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_insert('y')
        ).run(conn)
        result = map(lambda d: set(d), result)
        self.assertEqUnordered(expected, result)

    def test_set_union(self, conn):
        expected = [
            set(['x', 'y', 'a']),
            set(['x', 'y', 'z', 'a'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_union(['y', 'a'])
        ).run(conn)
        result = map(lambda d: set(d), result)
        self.assertEqUnordered(expected, result)

    def test_set_intersection(self, conn):
        expected = [
            set(['x', 'y']),
            set(['x'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_intersection(['x', 'y'])
        ).run(conn)
        result = map(lambda d: set(d), result)
        self.assertEqUnordered(expected, result)

    def test_set_difference(self, conn):
        expected = [
            set(['x']),
            set(['x', 'z'])
        ]
        result = r.db('z').table('t').map(
            lambda doc: doc['simple'].set_difference(['y'])
        ).run(conn)
        result = list(result)
        pprint(result)
        result = map(lambda d: set(d), result)
        self.assertEqUnordered(expected, result)


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


class TestBranch(MockTest):
    def get_data(self):
        data = [
            {'id': 'one', 'value': 5},
            {'id': 'three', 'value': 22},
            {'id': 'two', 'value': 12},
            {'id': 'four', 'value': 31}
        ]
        return as_db_and_table('x', 't', data)

    def test_branch_1(self, conn):
        expected = [
            {'id': 'one', 'value': 5, 'over_20': False},
            {'id': 'three', 'value': 22, 'over_20': True},
            {'id': 'two', 'value': 12, 'over_20': False},
            {'id': 'four', 'value': 31, 'over_20': True}
        ]
        result = r.db('x').table('t').map(
            r.branch(
                r.row['value'] > 20,
                r.row.merge({'over_20': True}),
                r.row.merge({'over_20': False})
            )
        ).run(conn)
        result = list(result)
        pprint({'RESULT': result})
        self.assertEqUnordered(expected, list(result))

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

