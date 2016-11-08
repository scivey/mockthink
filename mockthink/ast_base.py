from future.utils import iteritems
from rethinkdb import RqlCompileError, RqlRuntimeError

from . import util


class AttrHaving(object):
    def __init__(self, attrs):
        for k, v in iteritems(attrs):
            setattr(self, k, v)


# #################
#   Base classes
# #################

class RBase(object):
    def __init__(self, *args):
        pass

    def find_table_scope(self):
        result = None
        if hasattr(self, 'left'):
            result = self.left.find_table_scope()
        return result

    def find_db_scope(self):
        result = None
        if hasattr(self, 'left'):
            result = self.left.find_db_scope()
        return result

    def has_table_scope(self):
        result = None
        for part in ('left', 'middle', 'right'):
            if hasattr(self, part):
                result = getattr(self, part).has_table_scope()
                if result:
                    break
        return result

    def find_index_func_for_scope(self, index_name, db_arg):
        table = self.find_table_scope()
        db = self.find_db_scope()
        db_scope = self.find_db_scope()
        table_scope = self.find_table_scope()
        func = db_arg.get_index_func_in_table_in_db(
            db_scope,
            table_scope,
            index_name
        )
        is_multi = db_arg.is_multi_index(
            db_scope, table_scope, index_name
        )
        return func, is_multi

    def raise_rql_runtime_error(self, msg):
        from rethinkdb import RqlRuntimeError
        # temporary jankiness to get it working
        # doing it this way means error messages won't
        # be properly printed
        term = AttrHaving({
            'args': (),
            'optargs': {},
            'compose': (lambda x,y: 'COMPOSED')
        })
        raise RqlRuntimeError(msg, term, [])

    def raise_rql_compile_error(self, msg):
        term = AttrHaving({
            'args': (),
            'optargs': {},
            'compose': (lambda x,y: 'COMPOSED')
        })
        raise RqlCompileError(msg, term, [])

    def set_mock_ref(self, other):
        if hasattr(self, 'mockdb_ref'):
            other.mockdb_ref = self.mockdb_ref

class RDatum(RBase):
    def __init__(self, val, optargs={}):
        self.val = val

    def __str__(self):
        return "<DATUM: %s>" % self.val

    def run(self, arg, scope):
        return self.val

class RFunc(RBase):
    def __init__(self, param_names, body, optargs={}):
        self.param_names = param_names
        self.body = body

    def has_table_scope(self):
        return self.body.has_table_scope()

    def __str__(self):
        params = ", ".join(self.param_names)
        return "<RFunc: [%s] { %s }>" % (params, self.body)

    def run(self, args, scope):
        self.set_mock_ref(self.body)
        if not isinstance(args, list):
            args = [args]
        bound = util.as_obj(zip(self.param_names, args))
        call_scope = scope.push(bound)
        return self.body.run(None, call_scope)

class MonExp(RBase):
    def __init__(self, left, optargs={}):
        self.left = left
        self.optargs = optargs

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: %s>" % (class_name, self.left)

    def do_run(self, left, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        self.set_mock_ref(self.left)
        left = self.left.run(arg, scope)
        return self.do_run(left, arg, scope)


class BinExp(RBase):
    def __init__(self, left, right, optargs={}):
        self.left = left
        self.right = right
        self.optargs = optargs

    def __str__(self):
        class_name = self.__class__.__name__
        return "<%s: (%s, %s)>" % (class_name, self.left, self.right)

    def do_run(self, left, right, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        self.set_mock_ref(self.left)
        self.set_mock_ref(self.right)
        left = self.left.run(arg, scope)
        right = self.right.run(arg, scope)
        return self.do_run(left, right, arg, scope)


class Ternary(RBase):
    def __init__(self, left, middle, right, optargs={}):
        self.left = left
        self.middle = middle
        self.right = right
        self.optargs = optargs

    def do_run(self, left, middle, right, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        for part in ('left', 'middle', 'right'):
            self.set_mock_ref(getattr(self, part))
        left = self.left.run(arg, scope)
        middle = self.middle.run(arg, scope)
        right = self.right.run(arg, scope)
        return self.do_run(left, middle, right, arg, scope)

class ByFuncBase(RBase):
    def __init__(self, left, right, optargs={}):
        self.left = left
        self.right = right
        self.optargs = optargs

    def do_run(self, left, map_fn, arg, scope):
        raise NotImplementedError("method do_run not defined in class %s" % self.__class__.__name__)

    def run(self, arg, scope):
        self.set_mock_ref(self.left)
        self.set_mock_ref(self.right)
        left = self.left.run(arg, scope)
        map_fn = lambda x: self.right.run(x, scope)
        return self.do_run(left, map_fn, arg, scope)

class MakeObj(RBase):
    def __init__(self, vals, **kwargs):
        self.vals = vals

    def run(self, arg, scope):
        out = {}
        for k, v in iteritems(self.vals):
            self.set_mock_ref(v)
            out[k] = v.run(arg, scope)
        return out

class MakeArray(RBase):
    def __init__(self, vals):
        self.vals = vals

    def run(self, arg, scope):
        out = []
        for elem in self.vals:
            self.set_mock_ref(elem)
            out.append(elem.run(arg, scope))
        return out

class LITERAL_OBJECT(dict):
    @staticmethod
    def from_dict(a_dict):
        out = LITERAL_OBJECT()
        for k, v in iteritems(a_dict):
            out[k] = v
        return out

class LITERAL_LIST(list):
    @staticmethod
    def from_list(a_list):
        return LITERAL_LIST([elem for elem in a_list])

def contains_literals(to_check):
    if is_literal(to_check):
        return True
    elif isinstance(to_check, dict):
        for k, v in iteritems(to_check):
            if is_literal(v):
                return True
            elif contains_literals(v):
                return True
        return False
    elif isinstance(to_check, list):
        for v in to_check:
            if is_literal(v):
                return True
            elif contains_literals(v):
                return True
        return False
    return False

def has_nested_literal(to_check):
    if isinstance(to_check, LITERAL_OBJECT):
        for k, v in iteritems(to_check):
            if contains_literals(v):
                return True
    elif isinstance(to_check, dict):
        for k, v in iteritems(to_check):
            if has_nested_literal(v):
                return True
    elif isinstance(to_check, LITERAL_LIST):
        for v in to_check:
            if contains_literals(v):
                return True
    elif isinstance(to_check, list):
        for v in to_check:
            if has_nested_literal(v):
                return True
    return False

def is_literal(x):
    return isinstance(x, LITERAL_OBJECT) or isinstance(x, LITERAL_LIST)


@util.curry2
def rql_merge_with(ext_with, to_extend):
    out = {}
    out.update(to_extend)
    if is_literal(ext_with):
        if has_nested_literal(ext_with):
            raise RqlRuntimeError('No nested r.literal()!')

    for k, v in iteritems(ext_with):
        if is_literal(v):
            if has_nested_literal(v):
                raise RqlRuntimeError('No nested r.literal()!')

        if k not in to_extend:
            out[k] = util.clone(v)
        else:
            d1_val = to_extend[k]
            if is_literal(v):
                out[k] = util.clone(v)
            else:
                if isinstance(d1_val, dict) and (isinstance(v, dict) or isinstance(v, LITERAL_OBJECT)):
                    out[k] = rql_merge_with(v, d1_val)
                elif isinstance(d1_val, list) and (isinstance(v, list) or isinstance(v, LITERAL_LIST)):
                    out[k] = util.cat(d1_val, v)
                else:
                    out[k] = util.clone(v)
    return out
