import rethinkdb as r
from mockthink.test.functional.common import MockTest

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
