from . import util
from . import ast
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

class MockTableData(object):
    def __init__(self, data):
        self.data = data

    def replace_all(self, new_data):
        return MockTableData(new_data)

    def update_by_id(self, new_data):
        return MockTableData(replace_array_elems_by_id(self.data, new_data))

    def insert(self, new_rows):
        if not isinstance(new_rows, list):
            new_rows = [new_rows]
        return MockTableData(util.cat(self.data, new_rows))

    def get_rows(self):
        return self.data

    def __iter__(self):
        for elem in self.data:
            yield elem

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

def objects_from_pods(data):
    dbs_by_name = {}
    for db_name, db_data in data['dbs'].iteritems():
        tables_by_name = {}
        for table_name, table_data in db_data['tables'].iteritems():
            tables_by_name[table_name] = MockTableData(table_data)
        dbs_by_name[db_name] = MockDbData(tables_by_name)
    return MockDb(dbs_by_name)

class MockThink(object):
    def __init__(self, initial_data):
        self.initial_data = initial_data
        self.data = objects_from_pods(initial_data)

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
