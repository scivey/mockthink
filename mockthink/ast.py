import operator
from . import util
from .scope import Scope

from pprint import pprint
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

class RBase(object):
    def __init__(self, *args):
        pass

    def find_table_scope(self):
        result = None
        if isinstance(self, RTable):
            result = self.get_table_name()
        elif hasattr(self, 'left'):
            result = self.left.find_table_scope()
        return result

    def find_db_scope(self):
        result = None
        if isinstance(self, RDb):
            result = self.get_db_name()
        elif hasattr(self, 'left'):
            result = self.left.find_db_scope()
        return result

class RDatum(RBase):
    def __init__(self, val):
        self.val = val

    def run(self, arg, scope):
        return self.val


class RSym(RBase):
    def __init__(self, sym):
        self.sym = sym

    def run(self, arg, scope):
        return scope.get_sym(self.sym)

class MonExp(RBase):
    def __init__(self, left):
        self.left = left

    def do_run(self, left, arg, scope):
        pass

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        return self.do_run(left, arg, scope)

class RDb(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.get_db(db_name)

    def get_db_name(self):
        return self.left.run(None, Scope({}))

class RVar(MonExp):
    def do_run(self, symbol_name, arg, scope):
        return scope.get_sym(symbol_name)

class BinExp(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def do_run(self, left, right, arg, scope):
        pass

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        return self.do_run(left, right, arg, scope)

class RTable(BinExp):
    def get_table_name(self):
        return self.right.run(None, Scope({}))

    def do_run(self, data, table_name, arg, scope):
        return data.get_table(table_name)

class Bracket(BinExp):
    def do_run(self, thing, thing_attr, arg, scope):
        return thing[thing_attr]

class Get(BinExp):
    def do_run(self, left, right, arg, scope):
        res = None
        for elem in left:
            if util.getter('id', elem) == right:
                res = elem
                break
        return res


class BinCompr(BinExp):
    def do_run(self, left, right, arg, scope):
        return self.__class__.comparator(left, right)

class Gt(BinCompr):
    comparator = operator.gt

class Gte(BinCompr):
    comparator = operator.ge

class Lt(BinCompr):
    comparator = operator.lt

class Lte(BinCompr):
    comparator = operator.le

class Eq(BinCompr):
    comparator = operator.eq

class Neq(BinCompr):
    comparator = operator.ne

def db_data_extend(original_data, extend_with):
    to_return = util.obj_clone(original_data)
    to_return['dbs'] = util.obj_clone(to_return['dbs'])
    for one_db, one_db_data in extend_with['dbs'].iteritems():
        new_db_data = util.obj_clone(to_return['dbs'][one_db])
        for one_table, one_table_data in extend_with['dbs'][one_db]['tables'].iteritems():
            new_db_data['tables'][one_table] = one_table_data
        to_return['dbs'][one_db] = new_db_data
    print "\n\t[ EXTENDING DB!! ]\n"
    pprint(to_return)
    pprint(extend_with)
    print "\n\t [ /EXTENDING ]\n"
    return to_return

def set_db_table(db_data, db_name, table_name, table_data):
    ext_with = {
        'dbs': {
            db_name: {
                'tables': {
                    table_name: table_data
                }
            }
        }
    }
    return db_data_extend(db_data, ext_with)




class MockDbAction(object):
    pass

class MockDbTableUpdate(MockDbAction):
    def __init__(self, db_name, table_name, data):
        self.db_name = db_name
        self.table_name = table_name
        self.data = data

    # def __call__(self, db_data):
    #     original = db_data['dbs'][self.db_name]['tables'][self.table_name]
    #     replaced = replace_array_elems_by_id(original, self.data)
    #     result = extend(db_data, {
    #         'dbs':
    #     })


def map_with_scope(map_fn, scope, to_map):
    return map(lambda elem: map_fn(elem, scope), to_map)

def filter_with_scope(filter_fn, scope, to_filter):
    return filter(lambda elem: filter_fn(elem, scope), to_filter)

class UpdateBase(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right
    def run(self, arg, scope):
        updated = map_with_scope(self.get_update_fn(), scope, self.left.run(arg, scope))
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        return arg.update_by_id_in_table_in_db(current_db, current_table, updated)

class UpdateWithFunc(UpdateBase):
    def get_update_fn(self):
        def update_fn(elem, scope):
            return self.right.run([elem], scope)
        return update_fn

class UpdateWithObj(UpdateBase):
    def get_update_fn(self):
        ext_fn = util.extend_with(self.right)
        def update_fn(elem, scope):
            return ext_fn(elem)
        return update_fn

class RFunc(RBase):
    def __init__(self, param_names, body):
        self.param_names = param_names
        self.body = body

    def run(self, args, scope):
        bound = util.as_obj(zip(self.param_names, args))
        call_scope = scope.push(bound)
        return self.body.run(None, call_scope)

class FilterBase(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def get_filter_fn(self):
        pass

    def run(self, arg, scope):
        to_filter = self.left.run(arg, scope)
        out = []
        filt_fn = self.get_filter_fn()
        for elem in to_filter:
            if filt_fn(elem, scope):
                out.append(elem)
        return out

class FilterWithFunc(FilterBase):
    def get_filter_fn(self):
        def filt(elem, scope):
            return self.right.run([elem], scope)
        return filt

class FilterWithObj(FilterBase):
    def get_filter_fn(self):
        to_match = self.right
        def filt(elem, scope):
            return util.match_attrs(to_match)(elem)
        return filt

class MapBase(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def get_map_fn(self):
        pass

    def run(self, arg, scope):
        out = []
        map_fn = self.get_map_fn()
        for elem in self.left.run(arg, scope):
            out.append(map_fn(elem, scope))
        return out

class MapWithRFunc(MapBase):
    def get_map_fn(self):
        def map_fn(elem, scope):
            return self.right.run([elem], scope)
        return map_fn

class WithoutMap(MapBase):
    def get_map_fn(self):
        bad_attrs = self.right
        without_fn = util.without(bad_attrs)
        def map_fn(elem, scope):
            return without_fn(elem)
        return map_fn

class PluckMap(MapBase):
    def get_map_fn(self):
        pluck_fn = util.pluck_with(*self.right)
        def map_fn(elem, scope):
            return pluck_fn(elem)
        return map_fn
