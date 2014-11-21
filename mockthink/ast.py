import operator
from . import util
from .scope import Scope

from pprint import pprint

def map_with_scope(map_fn, scope, to_map):
    return map(lambda elem: map_fn(elem, scope), to_map)

def filter_with_scope(filter_fn, scope, to_filter):
    return filter(lambda elem: filter_fn(elem, scope), to_filter)

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

    def __str__(self):
        return "<DATUM: %s>" % self.val

    def run(self, arg, scope):
        return self.val


class MonExp(RBase):
    def __init__(self, left):
        self.left = left

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: %s>" % (class_name, self.left)

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

class Not(MonExp):
    def do_run(self, left, arg, scope):
        return not (left.run(arg, scope))

class BinExp(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: (%s, %s)>" % (class_name, self.left, self.right)


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

class GetAll(BinExp):
    def do_run(self, left, right, arg, scope):
        res = []
        to_find = set(right)
        for elem in left:
            if util.getter('id', elem) in to_find:
                res.append(elem)
        return res

class BinOp(BinExp):
    def do_run(self, left, right, arg, scope):
        return self.__class__.binop(left, right)

class Gt(BinOp):
    binop = operator.gt

class Gte(BinOp):
    binop = operator.ge

class Lt(BinOp):
    binop = operator.lt

class Lte(BinOp):
    binop = operator.le

class Eq(BinOp):
    binop = operator.eq

class Neq(BinOp):
    binop = operator.ne

class Add(BinOp):
    binop = operator.add

class Sub(BinOp):
    binop = operator.sub

class Mul(BinOp):
    binop = operator.mul

class Div(BinOp):
    binop = operator.div

class Mod(BinOp):
    binop = operator.mod

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


class UpdateBase(RBase):

    def __str__(self):
        return "<Update: (%s, %s)>" % (self.left, self.right)

    def __init__(self, left, right):
        self.left = left
        self.right = right

    def run(self, arg, scope):
        update_fn = self.get_update_fn()
        left = self.left.run(arg, scope)
        pprint(left)
        if isinstance(left, dict):
            result = [update_fn(left, scope)]
        else:
            result = map_with_scope(update_fn, scope, left)
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        return arg.update_by_id_in_table_in_db(current_db, current_table, result)

class UpdateWithFunc(UpdateBase):
    def get_update_fn(self):
        def update_fn(elem, scope):
            return self.right.run([elem], scope)
        return update_fn

class UpdateWithObj(UpdateBase):
    def get_update_fn(self):
        pprint(self.right)
        ext_fn = util.extend_with(self.right)
        def update_fn(elem, scope):
            pprint(elem)
            return ext_fn(elem)
        return update_fn

class Delete(RBase):
    def __init__(self, left):
        self.left = left

    def run(self, arg, scope):
        to_remove = self.left.run(arg, scope)
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        return arg.remove_by_id_in_table_in_db(current_db, current_table, to_remove)

class RFunc(RBase):
    def __init__(self, param_names, body):
        self.param_names = param_names
        self.body = body

    def __str__(self):
        params = ", ".join(self.param_names)
        return "<RFunc: [%s] { %s }>" % (params, self.body)

    def run(self, args, scope):
        bound = util.as_obj(zip(self.param_names, args))
        call_scope = scope.push(bound)
        return self.body.run(None, call_scope)

class FilterBase(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __str__(self):
        return "<Filter: (%s, %s)>" % (self.left, self.right)

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

    def __str__(self):
        return "<Map: (%s, %s)>" % (self.left, self.right)

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

class WithoutPoly(RBase):
    def __init__(self, left, bad_attrs):
        self.left = left
        self.bad_attrs = bad_attrs

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        map_fn = util.without(self.bad_attrs)
        if isinstance(left, dict):
            return map_fn(left)
        elif util.is_iterable(left):
            return map(map_fn, left)
        else:
            pprint(left)
            raise Exception('unexpected type')

class PluckPoly(RBase):
    def __init__(self, left, attrs):
        self.left = left
        self.attrs = attrs

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        map_fn = util.pluck_with(*self.attrs)
        if isinstance(left, dict):
            return map_fn(left)
        elif util.is_iterable(left):
            return map(map_fn, left)
        else:
            pprint(left)
            raise Exception('unexpected type')


class MergePoly(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def run(self, arg, scope):
        extend_with = self.right.run(arg, scope)
        map_fn = lambda d: util.extend(d, extend_with)
        left = self.left.run(arg, scope)
        if isinstance(left, dict):
            return map_fn(left)
        return map(map_fn, left)


def do_eq_join(left_field, left, right_field, right):
    out = []
    for elem in left:
        lval = util.getter(left_field)(elem)
        match = util.find_first(lambda d: util.getter(right_field)(d) == lval, right)
        if match:
            out.append({'left': elem, 'right': match})
    return out

class EqJoin(RBase):
    def __init__(self, left, field_name, right):
        self.left = left
        self.field_name = field_name
        self.right = right
    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        pprint(left)
        pprint(right)
        return do_eq_join(self.field_name, left, 'id', right)

def do_inner_join(pred, left, right):
    out = []
    for left_elem in left:
        for right_elem in right:
            if pred(left_elem, right_elem):
                out.append({'left': left_elem, 'right': right_elem})
    return out

class InnerJoin(RBase):
    def __init__(self, left, pred, right):
        self.left = left
        self.right = right
        self.pred = pred

    def run(self, arg, scope):
        def inner_pred(x, y):
            return self.pred.run([x, y], scope)
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        return do_inner_join(inner_pred, left, right)

def do_outer_join(pred, left, right):
    out = []
    for left_elem in left:
        matches = []
        result = {'left': left_elem}
        for right_elem in right:
            if pred(left_elem, right_elem):
                matches.append(util.extend(result, {'right': right_elem}))
        if not matches:
            matches.append(result)
        out = util.cat(out, matches)
    return out

class OuterJoin(RBase):
    def __init__(self, left, pred, right):
        self.left = left
        self.right = right
        self.pred = pred

    def run(self, arg, scope):
        def outer_pred(x, y):
            return self.pred.run([x, y], scope)
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        return do_outer_join(outer_pred, left, right)

class MakeObj(RBase):
    def __init__(self, vals):
        self.vals = vals

    def run(self, arg, scope):
        result = {k: v.run(arg, scope) for k, v in self.vals.iteritems()}
        return result

class MakeArray(RBase):
    def __init__(self, vals):
        self.vals = vals

    def run(self, arg, scope):
        result = [elem.run(arg, scope) for elem in self.vals]
        return result
