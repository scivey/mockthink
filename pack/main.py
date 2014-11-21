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


class RDatum(object):
    def __init__(self, val):
        self.val = val

    def run(self, arg, scope):
        return self.val

class RSym(object):
    def __init__(self, sym):
        self.sym = sym

    def run(self, arg, scope):
        return scope.get_sym(self.sym)

class MonExp(object):
    def __init__(self, left):
        self.left = left

    def do_run(self, left, arg, scope):
        pass

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        return self.do_run(left, arg, scope)

class RDb(MonExp):
    def do_run(self, left, arg, scope):
        return arg['dbs'][left]

class RVar(MonExp):
    def do_run(self, left, arg, scope):
        return scope.get_sym(left)

class BinExp(object):
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
    def do_run(self, left, right, arg, scope):
        return left['tables'][right]

class Bracket(BinExp):
    def do_run(self, left, right, arg, scope):
        return left[right]

class BinCompr(BinExp):
    def do_run(self, left, right, arg, scope):
        return self.__class__.comparator(left, right)

class Get(BinExp):
    def do_run(self, left, right, arg, scope):
        res = None
        for elem in left:
            if getter('id', elem) == right:
                res = elem
                break
        return res

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


class RFunc(object):
    def __init__(self, param_names, body):
        self.param_names = param_names
        self.body = body

    def run(self, args, scope):
        bound = as_obj(zip(self.param_names, args))
        call_scope = scope.push(bound)
        return self.body.run(None, call_scope)

class FilterBase(object):
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





