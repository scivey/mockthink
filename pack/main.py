import operator
from pprint import pprint

def curry2(func):
    def out(x, *args):
        if len(args):
            return func(x, args[0])
        def out2(y):
            return func(x, y)
        return out2
    return out

def curry3(func):
    def out(x, *args):
        if len(args) == 2:
            return func(x, *args)
        def out2(y, *args):
            if len(args):
                return func(x, y, args[0])
            def out3(z):
                return func(x, y, z)
            return out3
        return out2
    return out

def extend(*dicts):
    out = {}
    for one_dict in dicts:
        out.update(one_dict)
    return out

@curry2
def extend_with(a_dict, to_extend):
    return extend(to_extend, a_dict)

@curry2
def map_with(fn, a_list):
    return map(fn, a_list)

@curry2
def maybe_map_with(fn, thing):
    if isinstance(thing, list):
        return map(fn, thing)
    return fn(thing)

def is_simple(x):
    return not (isinstance(x, (list, dict)))

@curry2
def nth(n, things):
    return things[n]

@curry2
def getter(key, thing):
    if isinstance(thing, dict):
        return thing.get(key, None)
    else:
        return getattr(thing, key, None)

@curry3
def match_attr(key, val, thing):
    return getter(key, thing) == val

@curry2
def match_attrs(to_match, to_test):
    match = True
    for k, v in to_match.iteritems():
        if getter(k)(to_test) != v:
            match = False
            break
    return match

@curry2
def filter_with(func, things):
    return filter(func, things)

@curry2
def find_first(pred, things):
    result = None
    for thing in things:
        if pred(thing):
            result = thing
            break
    return result

def pipeline(*funcs):
    def out(x):
        result = x
        for f in funcs:
            result = f(result)
        return result
    return out

def pluck_with(*attrs):
    def inner_pluck(thing):
        return {k: v for k,v in thing.iteritems() if k in attrs}
    return inner_pluck

def get_by_id(id):
    return find_first(match_attr('id', id))

def as_obj(pairs):
    return {p[0]: p[1] for p in pairs}

class NotInScopeErr(Exception):
    def __init__(self, msg):
        print msg
        self.msg = msg

class Scope(object):
    def __init__(self, values):
        self.values = values

    def get_sym(self, x):
        result = None
        if x in self.values:
            result = self.values[x]
        elif hasattr(self, 'parent'):
            result = self.parent.get_sym(x)
        if result == None:
            msg = "symbol not defined: %s" % x
            raise NotInScopeErr(msg)
        return result

    def push(self, vals):
        scope = Scope(vals)
        scope.parent = self
        return scope

    def get_flattened(self):
        vals = {k: v for k, v in self.values.iteritems()}
        if not hasattr(self, 'parent'):
            return vals
        parent_vals = self.parent.get_flattened()
        parent_vals.update(vals)
        return parent_vals

    def log(self):
        pprint(self.get_flattened())



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
        return arg['dbs'][db_name]

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
        return data['tables'][table_name]

class Bracket(BinExp):
    def do_run(self, thing, thing_attr, arg, scope):
        return thing[thing_attr]

class Get(BinExp):
    def do_run(self, left, right, arg, scope):
        res = None
        for elem in left:
            if getter('id', elem) == right:
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

def clone_array(x):
    return [elem for elem in x]

def replace_array_elems_by_id(existing, replace_with):
    elem_index_by_id = {}
    for index in xrange(0, len(existing)):
        elem = existing[index]
        elem_index_by_id[getter('id')(elem)] = index

    to_return = clone_array(existing)

    for elem in replace_with:
        index = elem_index_by_id[getter('id')(elem)]
        to_return[index] = elem

    return to_return

@curry2
def without(bad_attrs, thing):
    return {k: v for k, v in thing.iteritems() if k not in bad_attrs}

def obj_clone(a_dict):
    return {k: v for k, v in a_dict.iteritems()}

def db_data_extend(original_data, extend_with):
    to_return = obj_clone(original_data)
    to_return['dbs'] = obj_clone(to_return['dbs'])
    for one_db, one_db_data in extend_with['dbs'].iteritems():
        new_db_data = obj_clone(to_return['dbs'][one_db])
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


# class MockDbData(object):
#     def __init__(self, data):
#         self.data = data

#     def get_data(self):
#         return self.data

#     def _extend_with(self, data):
#         to_return = obj_clone(self.data)
#         to_return['dbs'] = obj_clone(to_return['dbs'])
#         for one_db, one_db_data in data['dbs'].iteritems():
#             new_db_data = obj_clone(to_return['dbs'][one_db])
#             for one_table, one_table_data in new_table_data.iteritems():
#                 new_db_data['tables'][one_table] = one_table_data
#             to_return['dbs'][one_db] = new_db_data
#         return MockDbData(to_return)

#     def get_db(self, db_name):
#         return self.data['dbs'][db_name]

#     def get_table(self, db_name, table_name):
#         return self.get_db(db_name)['tables'][table_name]

#     def set_table(self, db_name, table_name, table_data):
#         ext_with = {
#             'dbs': {
#                 db_name: {
#                     'tables': {
#                         table_name: table_data
#                     }
#                 }
#             }
#         }
#         return self._extend_with(ext_with)

#     def replace_table(self, db_name, table_name, table_data):
#         to_return = self.data
#         existing = self.get_table(db_name, table_name)


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

class UpdateWithObj(RBase):
    def __init__(self, left, update_with):
        self.left = left
        self.update_with = update_with

    def run(self, arg, scope):
        to_update = self.left.run(arg, scope)
        map_fn = extend_with(self.update_with)
        pprint(arg)
        updated = map(map_fn, to_update)
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        print "\n\t [ %s : %s ]\n" % (current_db, current_table)
        original = arg['dbs'][current_db]['tables'][current_table]
        replaced = replace_array_elems_by_id(original, updated)
        to_return = set_db_table(arg, current_db, current_table, replaced)
        return to_return

class RFunc(RBase):
    def __init__(self, param_names, body):
        self.param_names = param_names
        self.body = body

    def run(self, args, scope):
        bound = as_obj(zip(self.param_names, args))
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
            return match_attrs(to_match)(elem)
        return filt



# class Filter(BinExp):
#     def do_run(self, left, right, arg, scope):
#         out = []
#         for elem in left:
#             if right.run([elem], scope):
#                 out.append(elem)
#         return out


query1 = FilterWithFunc(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    RFunc(
        ['x'],
        Gt(
            Bracket(RVar(RDatum('x')), RDatum('age')),
            RDatum(20)
        )
    )
)

query2 = Get(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    RDatum('joe-id'),
)

query3 = FilterWithObj(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    {'name': 'smith'}
)

query4 = UpdateWithObj(
    RTable(
        RDb(RDatum('fonz')),
        RDatum('wabbits')
    ),
    {
        'is_wabbit': True
    }
)

class MockDb(object):
    def __init__(self, data):
        self.data = data

    def run_query(self, query):
        result = query.run(self.data, Scope({}))
        if hasattr(result, 'dbs'):
            self.data = result
        return result

data = {
    'dbs': {
        'fonz': {
            'tables': {
                'wabbits': [
                    {'id': 'steve-id', 'name': 'steve', 'age': 26},
                    {'id': 'joe-id', 'name': 'joe', 'age': 15},
                    {'id': 'todd-id', 'name': 'todd', 'age': 65},
                    {'id': 'smith-id', 'name': 'smith', 'age': 34},
                    {'id': 'tim-id', 'name': 'tim', 'age': 19}
                ]
            }
        }
    }
}
res = query1.run(data, Scope({}))
pprint(res)
pprint(query2.run(data, Scope({})))
pprint(query3.run(data, Scope({})))
pprint(query4.run(data, Scope({})))

pprint(query1.run(data, Scope({})))
