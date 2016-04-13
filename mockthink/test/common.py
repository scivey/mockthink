from pprint import pprint
import rethinkdb as r
from mockthink.db import MockThink, MockThinkConn
import mockthink.util as util
import unittest

def real_stock_data_load(data, connection):
    for db in list(r.db_list().run(connection)):
        if db != "rethinkdb":
            r.db_drop(db).run(connection)
    for db_name, db_data in data['dbs'].iteritems():
        r.db_create(db_name).run(connection)
        for table_name, table_data in db_data['tables'].iteritems():
            r.db(db_name).table_create(table_name).run(connection)
            r.db(db_name).table(table_name).insert(table_data).run(connection)

def mock_stock_data_load(data, connection):
    connection.reset_data(data)

def load_stock_data(data, connection):
    if isinstance(connection, MockThinkConn):
        return mock_stock_data_load(data, connection)
    elif isinstance(connection, r.net.Connection):
        return real_stock_data_load(data, connection)

def make_test_registry(test_dict):
    def register_test(Constructor, class_name, tests):
        def test(connection):
            instance = Constructor()
            for one_test in tests:
                load_stock_data(instance.get_data(), connection)
                print '%s: %s' % (class_name, one_test)
                test_func = getattr(instance, one_test)
                test_func(connection)
        test_dict[class_name] = test
    return register_test

def assertEqUnordered(x, y, msg=''):
    for x_elem in x:
        if x_elem not in y:
            msg = 'assertEqUnordered: match not found for %s' % x_elem
            print 'AssertionError: %s' % msg
            raise AssertionError(msg)



class AssertionMixin(object):
    def assertEqual(self, x, y, msg=''):
        try:
            assert(x == y)
        except AssertionError as e:
            print 'AssertionError: expected %r to equal %r' % (x, y)
            pprint(x)
            pprint(y)
            raise e

    def assertEqUnordered(self, x, y, msg=''):
        return assertEqUnordered(x, y, msg)

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

class TestCase(unittest.TestCase):
    def assertEqUnordered(self, x, y, msg=''):
        return assertEqUnordered(x, y, msg)

    def assert_key_equality(self, keys, dict1, dict2):
        pluck = util.pluck_with(*keys)
        self.assertEqual(pluck(dict1), pluck(dict2))
