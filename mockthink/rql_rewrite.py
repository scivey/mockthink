from pprint import pprint
import rethinkdb.ast as r_ast
from . import ast as mt_ast
from . import util

class UnexpectedTermSequence(Exception):
    def __init__(self, msg=''):
        print msg
        self.msg = msg

RQL_TYPE_HANDLERS = {}

def type_dispatch(rql_node):
    return RQL_TYPE_HANDLERS[rql_node.__class__](rql_node)

@util.curry2
def handles_type(rql_type, func):
    def handler(node):
        assert(type(node) == rql_type)
        return func(node)
    RQL_TYPE_HANDLERS[rql_type] = handler
    return handler


def process_optargs(node):
    if hasattr(node, 'optargs') and node.optargs:
        return {k: plain_val_of_datum(v) for k, v in node.optargs.iteritems()}
    return {}

@util.curry2
def handle_generic_zerop(Mt_Constructor, node):
    return Mt_Constructor(optargs=process_optargs(node))

@util.curry2
def handle_generic_monop(Mt_Constructor, node):
    return Mt_Constructor(type_dispatch(node.args[0]), optargs=process_optargs(node))

@util.curry2
def handle_generic_binop(Mt_Constructor, node):
    return Mt_Constructor(
        type_dispatch(node.args[0]),
        type_dispatch(node.args[1]),
        optargs=process_optargs(node)
    )

@util.curry2
def handle_generic_binop_poly_2(mt_type_map, node):
    for r_type, m_type in mt_type_map.iteritems():
        if isinstance(node.args[1], r_type):
            Mt_Constructor = m_type
            break
    return Mt_Constructor(
        type_dispatch(node.args[0]),
        type_dispatch(node.args[1]),
        optargs=process_optargs(node)
    )

@util.curry2
def handle_generic_ternop(Mt_Constructor, node):
    assert(len(node.args) == 3)
    return Mt_Constructor(*[type_dispatch(arg) for arg in node.args], optargs=process_optargs(node))

GENERIC_BY_ARITY = {
    0: handle_generic_zerop,
    1: handle_generic_monop,
    2: handle_generic_binop,
    3: handle_generic_ternop
}

@util.curry2
def handle_n_ary(arity_type_map, node):
    arg_len = len(node.args)
    return GENERIC_BY_ARITY[arg_len](arity_type_map[arg_len], node)

def makearray_of_datums(datum_list):
    out = []
    for elem in datum_list:
        expected_types = (r_ast.Datum, r_ast.Asc, r_ast.Desc)
        if elem.__class__ not in expected_types:
            raise TypeError('unexpected elem type: %p' % elem)
        out.append(type_dispatch(elem))
    return mt_ast.MakeArray(out)

@util.curry2
def binop_splat(Mt_Constructor, node):
    args = node.args
    left = type_dispatch(args[0])
    if isinstance(args[1], r_ast.MakeArray):
        right = type_dispatch(args[1])
    else:
        right = makearray_of_datums(args[1:])
    return Mt_Constructor(left, right, optargs=process_optargs(node))



NORMAL_MONOPS = {
    r_ast.Var: mt_ast.RVar,
    r_ast.DB: mt_ast.RDb,
    r_ast.Delete: mt_ast.Delete,
    r_ast.IsEmpty: mt_ast.IsEmpty,
    r_ast.Count: mt_ast.Count,
    r_ast.Keys: mt_ast.Keys,
    r_ast.Downcase: mt_ast.StrDowncase,
    r_ast.Upcase: mt_ast.StrUpcase,
    r_ast.Asc: mt_ast.Asc,
    r_ast.Desc: mt_ast.Desc,
    r_ast.Zip: mt_ast.Zip,
    r_ast.Json: mt_ast.Json,
    r_ast.TypeOf: mt_ast.TypeOf
}

NORMAL_BINOPS = {
    r_ast.Ge: mt_ast.Gte,
    r_ast.Lt: mt_ast.Lt,
    r_ast.Le: mt_ast.Lte,
    r_ast.Eq: mt_ast.Eq,
    r_ast.Ne: mt_ast.Neq,
    r_ast.Gt: mt_ast.Gt,
    r_ast.Add: mt_ast.Add,
    r_ast.Sub: mt_ast.Sub,
    r_ast.Mul: mt_ast.Mul,
    r_ast.Div: mt_ast.Div,
    r_ast.Mod: mt_ast.Mod,
    r_ast.Bracket: mt_ast.Bracket,
    r_ast.Table: mt_ast.RTable,
    r_ast.Get: mt_ast.Get,
    r_ast.Map: mt_ast.MapWithRFunc,
    r_ast.Replace: mt_ast.Replace,
    r_ast.Merge: mt_ast.MergePoly,
    r_ast.Append: mt_ast.Append,
    r_ast.Prepend: mt_ast.Prepend,
    r_ast.Union: mt_ast.Union,
    r_ast.Sample: mt_ast.Sample,
    r_ast.SetUnion: mt_ast.SetUnion,
    r_ast.SetDifference: mt_ast.SetDifference,
    r_ast.SetInsert: mt_ast.SetInsert,
    r_ast.SetIntersection: mt_ast.SetIntersection,
    r_ast.Reduce: mt_ast.Reduce,
    r_ast.Insert: mt_ast.Insert
}

BINOPS_BY_ARG_2_TYPE = {
    r_ast.Group: {
        r_ast.Datum: mt_ast.GroupByField,
        r_ast.Func: mt_ast.GroupByFunc
    },
    r_ast.Max: {
        r_ast.Datum: mt_ast.MaxByField,
        r_ast.Func: mt_ast.MaxByFunc
    },
    r_ast.Filter: {
        r_ast.MakeObj: mt_ast.FilterWithObj,
        r_ast.Func: mt_ast.FilterWithFunc
    },
    r_ast.Update: {
        r_ast.MakeObj: mt_ast.UpdateWithObj,
        r_ast.Func: mt_ast.UpdateByFunc
    }
}

SPLATTED_BINOPS = {
    r_ast.Pluck: mt_ast.PluckPoly,
    r_ast.HasFields: mt_ast.HasFields,
    r_ast.Without: mt_ast.WithoutPoly,
    r_ast.GetAll: mt_ast.GetAll,
    r_ast.OrderBy: mt_ast.OrderBy,
    r_ast.DeleteAt: mt_ast.DeleteAt
}

NORMAL_TERNOPS = {
    r_ast.EqJoin: mt_ast.EqJoin,
    r_ast.InnerJoin: mt_ast.InnerJoin,
    r_ast.OuterJoin: mt_ast.OuterJoin,
    r_ast.InsertAt: mt_ast.InsertAt,
    r_ast.SpliceAt: mt_ast.SpliceAt,
    r_ast.ChangeAt: mt_ast.ChangeAt,
    r_ast.Branch: mt_ast.Branch
}

OPS_BY_ARITY = {
    r_ast.Split: {
        1: mt_ast.StrSplitDefault,
        2: mt_ast.StrSplitOn,
        3: mt_ast.StrSplitOnLimit
    },
    r_ast.Random: {
        0: mt_ast.Random0,
        1: mt_ast.Random1,
        2: mt_ast.Random2
    }
}

for r_type, mt_type in NORMAL_MONOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_monop(mt_type)

for r_type, mt_type in NORMAL_BINOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop(mt_type)

for r_type, arg_2_map in BINOPS_BY_ARG_2_TYPE.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop_poly_2(arg_2_map)

for r_type, mt_type in SPLATTED_BINOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = binop_splat(mt_type)

for r_type, mt_type in NORMAL_TERNOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_ternop(mt_type)

for r_type, mt_type in OPS_BY_ARITY.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_n_ary(mt_type)

@handles_type(r_ast.Datum)
def handle_datum(node):
    return mt_ast.RDatum(node.data)

def plain_val_of_datum(datum_node):
    return datum_node.data

def plain_list_of_make_array(make_array_instance):
    assert(isinstance(make_array_instance, r_ast.MakeArray))
    return map(plain_val_of_datum, make_array_instance.args)

@handles_type(r_ast.MakeArray)
def handle_make_array(node):
    return mt_ast.MakeArray([type_dispatch(elem) for elem in node.args])

@handles_type(r_ast.MakeObj)
def handle_make_obj(node):
    return mt_ast.MakeObj({k: type_dispatch(v) for k, v in node.optargs.iteritems()})

def contains_ivar(node):
    return r_ast._ivar_scan(node)

def is_ivar(node):
    return isinstance(node, r_ast.ImplicitVar)

def replace_implicit_vars(arg_symbol, node):
    for index in range(0, len(node.args)):
        elem = node.args[index]
        if is_ivar(elem):
            node.args[index] = r_ast.Var(r_ast.Datum(arg_symbol))
        else:
            replace_implicit_vars(arg_symbol, elem)
    for key, val in node.optargs.iteritems():
        if is_ivar(val):
            node.optargs[key] = r_ast.Var(r_ast.Datum(arg_symbol))
        else:
            replace_implicit_vars(arg_symbol, node.optargs[key])


@handles_type(r_ast.Func)
def handle_func(node):
    func_params = plain_list_of_make_array(node.args[0])
    func_body = node.args[1]
    if contains_ivar(func_body):
        replace_implicit_vars(func_params[0], func_body)
    func_body = type_dispatch(func_body)
    return mt_ast.RFunc(func_params, func_body)

@handles_type(r_ast.OrderBy)
def handle_order_by(node):
    left = type_dispatch(node.args[0])
    right = []
    for elem in node.args[1:]:
        if isinstance(elem, r_ast.Datum):
            right.append(mt_ast.Asc(type_dispatch(elem)))
        else:
            accepted = (r_ast.Desc, r_ast.Asc)
            assert(elem.__class__ in accepted)
            right.append(type_dispatch(elem))
    right = mt_ast.MakeArray(right)
    return mt_ast.OrderBy(left, right)

@handles_type(r_ast.IndexesOf)
def handle_indexes_of(node):
    left = type_dispatch(node.args[0])
    right = type_dispatch(node.args[1])
    if isinstance(node.args[1], r_ast.Func):
        return mt_ast.IndexesOfFunc(left, right)
    else:
        return mt_ast.IndexesOfValue(left, right)

@handles_type(r_ast.FunCall)
def handle_funcall(node):
    if isinstance(node.args[0], r_ast.Func):
        func = type_dispatch(node.args[0])
        rest = node.args[1:]
    else:
        last = len(node.args) - 1
        func = type_dispatch(node.args[last])
        rest = node.args[0:last]
    rest = mt_ast.MakeArray([type_dispatch(elem) for elem in rest])
    return mt_ast.Do(rest, func)

def rewrite_query(query):
    return type_dispatch(query)

