# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import rethinkdb as r

from mockthink.test.common import assertEqUnordered, assertEqual
from . import MockTest


class TestThings(MockTest):
    @staticmethod
    def get_data():
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

        assertEqUnordered(expected, list(query.run(conn)))

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
        assertEqual(expected, list(query.run(conn)))

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
        assertEqUnordered(expected, list(query.run(conn)))

