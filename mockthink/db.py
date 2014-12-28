import rethinkdb
import datetime
import contextlib
from pprint import pprint
from . import util, ast, rtime
from .rql_rewrite import rewrite_query
from .scope import Scope

def replace_array_elems_by_id(existing, replace_with):
    elem_index_by_id = {}
    for index in xrange(0, len(existing)):
        elem = existing[index]
        elem_index_by_id[util.getter('id')(elem)] = index

    to_return = util.clone_array(existing)

    for elem in replace_with:
        index = elem_index_by_id[util.getter('id')(elem)]
        to_return[index] = elem

    return to_return

def remove_array_elems_by_id(existing, to_remove):
    existing = util.clone_array(existing)
    result = existing
    for elem in to_remove:
        if elem in result:
            result.remove(elem)
    return result

def insert_update_one_row(existing_row, new_row):
    assert(existing_row['id'] == new_row['id'])
    result = {}
    changes = {'id': existing_row['id']}
    result.update(existing_row)
    for k, v in new_row.iteritems():
        if (k not in existing_row) or (existing_row[k] != v):
            changes[k] = v
        result[k] = v
    return result, changes

def insert_replace_one_row(existing_row, new_row):
    assert(existing_row['id'] == new_row['id'])
    result = {}
    changes = {'id': existing_row['id']}
    for k, v in new_row.iteritems():
        result[k] = v
        if (k not in existing_row) or (existing_row[k] != v):
            changes[k] = v
    return result, changes

def insert_into_table_with_conflict_setting(existing, to_insert, conflict):
    # conflict: {'error', 'update', 'replace'}
    existing_by_id = {row['id']: row for row in existing}
    seen = set([])
    result = []
    result_report = {
        'errors': 0,
        'updated': 0,
        'inserted': 0,
        'replaced': 0,
        'changes': []
    }
    for doc in to_insert:
        if doc['id'] in existing_by_id:
            if conflict == 'error':
                result_report['errors'] += 1
                continue
            elif conflict == 'update':
                result_row, changes = insert_update_one_row(existing_by_id[doc['id']], doc)
                result_report['updated'] += 1
                seen.add(doc['id'])
            elif conflict == 'replace':
                result_row, changes = insert_replace_one_row(existing_by_id[doc['id']], doc)
                result_report['replaced'] += 1
                seen.add(doc['id'])
        else:
            result_row = doc
            changes = doc
        result.append(result_row)
        result_report['changes'].append(changes)
        result_report['inserted'] += 1

    not_updated = [row for row in existing if row['id'] not in seen]
    result = not_updated + result
    return result, result_report

class MockTableData(object):
    def __init__(self, rows, indexes):
        self.rows = rows
        self.indexes = indexes

    def replace_all(self, rows, indexes):
        return MockTableData(rows, indexes)

    def update_by_id(self, new_data):
        if not isinstance(new_data, list):
            new_data = [new_data]
        return MockTableData(replace_array_elems_by_id(self.rows, new_data), self.indexes)

    def insert(self, new_rows, conflict):
        if not isinstance(new_rows, list):
            new_rows = [new_rows]
        current_data = self.rows
        current_by_id = {row['id']: row for row in self.rows}
        return MockTableData(util.cat(self.rows, new_rows), self.indexes)

    def remove_by_id(self, to_remove):
        if not isinstance(to_remove, list):
            to_remove = [to_remove]
        return MockTableData(remove_array_elems_by_id(self.rows, to_remove), self.indexes)

    def get_rows(self):
        return self.rows

    def create_index(self, index_name, index_func):
        return MockTableData(self.rows, util.extend(self.indexes, {index_name: index_func}))

    def rename_index(self, old_name, new_name):
        new_indexes = util.without([old_name], self.indexes)
        new_indexes[new_name] = self.indexes[old_name]
        return MockTableData(self.rows, new_indexes)

    def drop_index(self, index_name):
        new_indexes = util.without([index_name], self.indexes)
        return MockTableData(self.rows, new_indexes)

    def list_indexes(self):
        return self.indexes.keys()

    def index_exists(self, index):
        return index in self.indexes

    def _index_values(self, index_name):
        func = self.get_index_func(index_name)
        out = [func(elem) for elem in self.rows]
        pprint({'func': func, 'out': out})
        return out

    def get_index_func(self, index):
        return self.indexes[index]

    def __iter__(self):
        for elem in self.rows:
            yield elem

    def __getitem__(self, index):
        return self.rows[index]

class MockDbData(object):
    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def create_table(self, table_name):
        return self.set_table(table_name, MockTableData([], {}))

    def list_tables(self):
        return self.tables_by_name.keys()

    def drop_table(self, table_name):
        return MockDbData(util.without([table_name], self.tables_by_name))

    def get_table(self, table_name):
        return self.tables_by_name[table_name]

    def set_table(self, table_name, new_table_instance):
        assert(isinstance(new_table_instance, MockTableData))
        tables = util.obj_clone(self.tables_by_name)
        tables[table_name] = new_table_instance
        return MockDbData(tables)

class MockDb(object):
    def __init__(self, dbs_by_name):
        self.dbs_by_name = dbs_by_name

    def get_db(self, db_name):
        return self.dbs_by_name[db_name]

    def set_db(self, db_name, db_data_instance):
        assert(isinstance(db_data_instance, MockDbData))
        dbs_by_name = util.obj_clone(self.dbs_by_name)
        dbs_by_name[db_name] = db_data_instance
        return MockDb(dbs_by_name)

    def create_table_in_db(self, db_name, table_name):
        new_db = self.get_db(db_name)
        new_db = new_db.create_table(table_name)
        return self.set_db(db_name, new_db)

    def drop_table_in_db(self, db_name, table_name):
        new_db = self.get_db(db_name)
        new_db = new_db.drop_table(table_name)
        return self.set_db(db_name, new_db)

    def list_tables_in_db(self, db_name):
        return self.get_db(db_name).list_tables()

    def create_db(self, db_name):
        return self.set_db(db_name, MockDbData({}))

    def drop_db(self, db_name):
        return MockDb(util.without([db_name], self.dbs_by_name))

    def list_dbs(self):
        return self.dbs_by_name.keys()

    def replace_table_in_db(self, db_name, table_name, table_data_instance):
        assert(isinstance(table_data_instance, MockTableData))
        db = self.get_db(db_name)
        new_db = db.set_table(table_name, table_data_instance)
        return self.set_db(db_name, new_db)

    def insert_into_table_in_db(self, db_name, table_name, elem_list, conflict):
        new_table_data = self.get_db(db_name).get_table(table_name).insert(elem_list, conflict)
        return self._replace_table(db_name, table_name, new_table_data)

    def update_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_table_data = self.get_db(db_name).get_table(table_name).update_by_id(elem_list)
        return self._replace_table(db_name, table_name, new_table_data)

    def _replace_table(self, db_name, table_name, new_table_data):
        new_db = self.get_db(db_name).set_table(table_name, new_table_data)
        return self.set_db(db_name, new_db)

    def remove_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_table_data = self.get_db(db_name).get_table(table_name).remove_by_id(elem_list)
        return self._replace_table(db_name, table_name, new_table_data)

    def create_index_in_table_in_db(self, db_name, table_name, index_name, index_func):
        new_table_data = self.get_db(db_name).get_table(table_name).create_index(index_name, index_func)
        return self._replace_table(db_name, table_name, new_table_data)

    def drop_index_in_table_in_db(self, db_name, table_name, index_name):
        new_table_data = self.get_db(db_name).get_table(table_name).drop_index(index_name)
        return self._replace_table(db_name, table_name, new_table_data)

    def rename_index_in_table_in_db(self, db_name, table_name, old_index_name, new_index_name):
        new_table_data = self.get_db(db_name).get_table(table_name).rename_index(old_index_name, new_index_name)
        return self._replace_table(db_name, table_name, new_table_data)

    def list_indexes_in_table_in_db(self, db_name, table_name):
        return self.get_db(db_name).get_table(table_name).list_indexes()

    def index_exists_in_table_in_db(self, db_name, table_name, index_name):
        return self.get_db(db_name).get_table(table_name).index_exists(index_name)

    def get_index_func_in_table_in_db(self, db_name, table_name, index_name):
        return self.get_db(db_name).get_table(table_name).get_index_func(index_name)

    def get_now_time(self):
        return self.mockthink.get_now_time()

def objects_from_pods(data):
    dbs_by_name = {}
    for db_name, db_data in data['dbs'].iteritems():
        tables_by_name = {}
        for table_name, table_data in db_data['tables'].iteritems():
            if isinstance(table_data, list):
                indexes = {}
            else:
                indexes = table_data.get('indexes', {})
                table_data = table_data.get('rows', [])
            tables_by_name[table_name] = MockTableData(table_data, indexes)
        dbs_by_name[db_name] = MockDbData(tables_by_name)
    return MockDb(dbs_by_name)

class MockThinkConn(object):
    def __init__(self, mockthink_parent):
        self.mockthink_parent = mockthink_parent
    def reset_data(self, data):
        self.mockthink_parent._modify_initial_data(data)
    def _start(self, rql_query, **global_optargs):
        return self.mockthink_parent.run_query(rewrite_query(rql_query))

class MockThink(object):
    def __init__(self, initial_data):
        self._modify_initial_data(initial_data)
        self.tzinfo = rethinkdb.make_timezone('00:00')

    def _modify_initial_data(self, new_data):
        self.initial_data = new_data
        self.reset()

    def run_query(self, query):
        temp_now_time = False

        # RethinkDB only evaluates `r.now()` once per query,
        # so it should have the same result each time within that query.
        # But we don't do anything if now_time has already been set.

        if not hasattr(self, 'now_time'):
            temp_now_time = True
            self.now_time = self.get_now_time()

        result = query.run(self.data, Scope({}))
        if isinstance(result, MockDb):
            self.data = result
        elif isinstance(result, MockTableData):
            result = result.get_rows()

        if temp_now_time:
            delattr(self, 'now_time')
        return result

    def pprint_query_ast(self, query):
        query = "%s" % query
        print query

    def reset(self):
        self.data = objects_from_pods(self.initial_data)
        self.data.mockthink = self

    def get_conn(self):
        conn = MockThinkConn(self)
        return conn

    def set_now_time(self, dtime):
        self.now_time = dtime

    def get_now_time(self):
        if hasattr(self, 'now_time'):
            return self.now_time
        else:
            return rtime.now()

    @contextlib.contextmanager
    def connect(self):
        conn = MockThinkConn(self)
        yield conn
        self.reset()
