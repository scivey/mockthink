import argparse
import sys
from pprint import pprint
import rethinkdb as r
from mockthink.db import MockThink, MockThinkConn
from mockthink.test.common import make_test_registry, AssertionMixin
from mockthink.test.common import as_db_and_table


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

class TestThings(MockTest):
    def get_data(self):
        people_data = [
            {'id': 'joe-id', 'name': 'joe', 'age': 26},
            {'id': 'bob-id', 'name': 'bob', 'age': 19},
            {'id': 'tim-id', 'name': 'tim', 'age': 53},
            {'id': 'todd-id', 'name': 'todd', 'age': 17}
        ]
        job_data = [
            {'id': 'lawyer-id', 'name': 'Lawyer'},
            {'id': 'nurse-id', 'name': 'Nurse'},
            {'id': 'assistant-id', 'name': 'Assistant'}
        ]
        employee_data = [
            {'id': 'joe-employee-id', 'person': 'joe-id', 'job': 'lawyer-id'},
            {'id': 'tim-employee-id', 'person': 'tim-id', 'job': 'nurse-id'},
            {'id': 'bob-employee-id', 'person': 'bob-id', 'job': 'assistant-id'},
            {'id': 'todd-employee-id', 'person': 'todd-id', 'job': 'lawyer-id'}
        ]
        return {
            'dbs': {
                'x': {
                    'tables': {
                        'people': people_data,
                        'jobs': job_data,
                        'employees': employee_data
                    }
                }
            }
        }

    def test_join_filter_map(self, conn):
        query = r.db('x').table('employees').eq_join(
            'person', r.db('x').table('people')
        ).filter(
            lambda p: p['right']['age'] > 20
        ).map(
            lambda d: d['left'].merge({'person': d['right']['name']})
        )
        expected = [
            {
                'id': 'joe-employee-id',
                'person': 'joe',
                'job': 'lawyer-id'
            },
            {
                'id': 'tim-employee-id',
                'person': 'tim',
                'job': 'nurse-id'
            }
        ]

        self.assertEqUnordered(expected, list(query.run(conn)))

    def test_index_getall_map_orderby(self, conn):
        r.db('x').table('people').index_create(
            'name_and_id',
            lambda doc: doc['name'] + doc['id']
        ).run(conn)

        r.db('x').table('people').index_wait().run(conn)

        query = r.db('x').table('people').get_all(
            'joejoe-id', 'timtim-id', index='name_and_id'
        ).map(
            lambda doc: doc.merge({
                'also_name': doc['name'],
                'age_plus_10': doc['age'] + 10,
                'age_times_2': doc['age'] * 2
            })
        ).order_by('name')

        expected = [
            {
                'id': 'joe-id',
                'name': 'joe',
                'also_name': 'joe',
                'age': 26,
                'age_plus_10': 36,
                'age_times_2': 52
            },
            {
                'id': 'tim-id',
                'name': 'tim',
                'also_name': 'tim',
                'age': 53,
                'age_plus_10': 63,
                'age_times_2': 106
            }
        ]
        self.assertEqual(expected, list(query.run(conn)))

    def test_multi_join(self, conn):
        query = r.db('x').table('employees').eq_join(
            'person', r.db('x').table('people')
        ).map(
            lambda d: d['left'].merge({'person': d['right']['name']})
        ).eq_join(
            'job', r.db('x').table('jobs')
        ).map(
            lambda d: d['left'].merge({'job': d['right']['name']})
        )
        expected = [
            {
                'id': 'joe-employee-id',
                'person': 'joe',
                'job': 'Lawyer'
            },
            {
                'id': 'tim-employee-id',
                'person': 'tim',
                'job': 'Nurse'
            },
            {
                'id': 'bob-employee-id',
                'person': 'bob',
                'job': 'Assistant'
            },
            {
                'id': 'todd-employee-id',
                'person': 'todd',
                'job': 'Lawyer'
            }
        ]
        self.assertEqUnordered(expected, list(query.run(conn)))


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
    conn = r.connect('localhost', 30000)
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

