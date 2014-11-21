from pprint import pprint
import rethinkdb as r
from mockthink.db import MockThink, MockThinkConn

def real_stock_data_load(data, connection):
    for db in list(r.db_list().run(connection)):
        r.db_drop(db).run(connection)
    for db_name, db_data in data['dbs'].iteritems():
        r.db_create(db_name).run(connection)
        for table_name, table_data in db_data['tables'].iteritems():
            r.db(db_name).table_create(table_name).run(conn)
            r.db(db_name).table(table_name).insert(table_data)

def mock_stock_data_load(data, connection):
    connection.reset_data(data)

def load_stock_data(data, connection):
    if isinstance(connection, MockThinkConn):
        return mock_stock_data_load(data, connection)
    else:
        pass


TESTS = {}

def register_test(Constructor, class_name, tests):
    def test(connection):
        instance = Constructor()
        for one_test in tests:
            load_stock_data(instance.get_data(), connection)
            print '%s: %s' % (class_name, one_test)
            test_func = getattr(instance, one_test)
            test_func(connection)
    TESTS[class_name] = test

class Meta(type):
    def __new__(cls, name, bases, attrs):
        result = super(Meta, cls).__new__(cls, name, bases, attrs)
        tests = [name for name in attrs.keys() if 'test' in name]
        register_test(result, result.__name__, tests)
        return result

class Base(object):
    __metaclass__ = Meta

class MockTest(Base):
    def get_data(self):
        return {
            'dbs': {
                'default': {
                    'tables': {}
                }
            }
        }
    def assertEqual(self, x, y, msg=''):
        try:
            assert(x == y)
        except AssertionError as e:
            print 'AssertionError: expected %s to equal %s' % (x, y)
            raise e

    def assertEqUnordered(self, x, y, msg=''):
        return self.assertEqual(x, y, msg)

def as_db_and_table(db_name, table_name, data):
    return {
        'dbs': {
            db_name: {
                'tables': {
                    table_name: data
                }
            }
        }
    }


class TestGetting(MockTest):
    def get_data(self):
        data = [
            {'id': 'joe-id', 'name': 'joe'},
            {'id': 'bob-id', 'name': 'bob'}
        ]
        return as_db_and_table('x', 'people', data)

    def test_get_one_by_id(self, conn):
        result = r.db('x').table('people').get('bob-id').run(conn)
        self.assertEqual({'id': 'bob-id', 'name': 'bob'}, result)

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
        self.assertEqual(expected, list(result))

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


class TestUpdating(MockTest):
    pass


if __name__ == '__main__':
    think = MockThink(as_db_and_table('nothing', 'nothing', []))
    conn = think.get_conn()
    for test_name, test_fn in TESTS.iteritems():
        test_fn(conn)

