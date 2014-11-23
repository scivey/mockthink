from . import util
from . import ast
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

    def insert(self, new_rows):
        if not isinstance(new_rows, list):
            new_rows = [new_rows]
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

    def __iter__(self):
        for elem in self.rows:
            yield elem

    def __getitem__(self, index):
        return self.rows[index]

class MockDbData(object):
    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def create_table(self, table_name):
        return self.set_table(table_name, MockTableData([]))

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

    def insert_into_table(self, table_name, elem_list):
        new_table = self.get_table(table_name).insert(elem_list)
        return self.set_table(table_name, new_table)

    def update_by_id_in_table(self, table_name, elem_list):
        new_table = self.get_table(table_name).update_by_id(elem_list)
        return self.set_table(table_name, new_table)

    def remove_by_id_in_table(self, table_name, elem_list):
        new_table = self.get_table(table_name).remove_by_id(elem_list)
        return self.set_table(table_name, new_table)

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

    def insert_into_table_in_db(self, db_name, table_name, elem_list):
        new_db_data = self.get_db(db_name).insert_into_table(table_name, elem_list)
        return self.set_db(db_name, new_db_data)

    def update_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_db_data = self.get_db(db_name).update_by_id_in_table(table_name, elem_list)
        return self.set_db(db_name, new_db_data)

    def remove_by_id_in_table_in_db(self, db_name, table_name, elem_list):
        new_db_data = self.get_db(db_name).remove_by_id_in_table(table_name, elem_list)
        return self.set_db(db_name, new_db_data)

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
        self.initial_data = initial_data
        self.data = objects_from_pods(initial_data)

    def _modify_initial_data(self, new_data):
        self.initial_data = new_data
        self.data = objects_from_pods(new_data)

    def run_query(self, query):
        result = query.run(self.data, Scope({}))
        if isinstance(result, MockDb):
            self.data = result
        elif isinstance(result, MockTableData):
            result = result.get_rows()
        return result

    def pprint_query_ast(self, query):
        foo = "%s" % query
        print foo

    def reset(self):
        self.data = objects_from_pods(self.initial_data)

    def get_conn(self):
        conn = MockThinkConn(self)
        return conn
