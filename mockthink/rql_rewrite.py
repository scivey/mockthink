from pprint import pprint
import rethinkdb.ast as r_ast
from . import ast as mt_ast
# from rethinkdb import ql2_pb2 as rql_proto
# pTerm = rql_proto.Term.TermType
# pQuery = rql_proto.Query.QueryType
# pDatum = rql_proto.Datum.DatumType
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

@util.curry2
def handle_generic_binop(Mt_Constructor, node):
    return Mt_Constructor(type_dispatch(node.args[0]), type_dispatch(node.args[1]))

@util.curry2
def handle_generic_monop(Mt_Constructor, node):
    return Mt_Constructor(type_dispatch(node.args[0]))

NORMAL_BINOPS = {
    r_ast.Ge: mt_ast.Gte,
    r_ast.Lt: mt_ast.Lt,
    r_ast.Le: mt_ast.Lte,
    r_ast.Eq: mt_ast.Eq,
    r_ast.Ne: mt_ast.Neq,
    r_ast.Gt: mt_ast.Gt,
    r_ast.Bracket: mt_ast.Bracket,
    r_ast.Table: mt_ast.RTable,
    r_ast.Get: mt_ast.Get,
    r_ast.Map: mt_ast.MapWithRFunc
}

for r_type, mt_type in NORMAL_BINOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop(mt_type)

NORMAL_MONOPS = {
    r_ast.Var: mt_ast.RVar,
    r_ast.DB: mt_ast.RDb
}

for r_type, mt_type in NORMAL_MONOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_monop(mt_type)


@handles_type(r_ast.Datum)
def handle_datum(node):
    return mt_ast.RDatum(node.data)

@handles_type(r_ast.Without)
def handle_without(node):
    pass

def plain_val_of_var(var_node):
    return var_node.args[0].data

def plain_val_of_datum(datum_node):
    return datum_node.data

def plain_list_of_make_array(make_array_instance):
    assert(isinstance(make_array_instance, r_ast.MakeArray))
    return map(plain_val_of_datum, make_array_instance.args)

def plain_obj_of_make_obj(make_obj_instance):
    assert(isinstance(make_obj_instance, r_ast.MakeObj))
    return {k: plain_val_of_datum(v) for k, v in make_obj_instance.optargs.iteritems()}

@handles_type(r_ast.Func)
def handle_func(node):
    func_params = plain_list_of_make_array(node.args[0])
    func_body = type_dispatch(node.args[1])
    return mt_ast.RFunc(func_params, func_body)

@handles_type(r_ast.Filter)
def handle_filter(node):
    args = node.args
    if isinstance(args[1], r_ast.Func):
        return mt_ast.FilterWithFunc(type_dispatch(args[0]), type_dispatch(args[1]))
    elif isinstance(args[1], r_ast.MakeObj):
        filter_obj = plain_obj_of_make_obj(args[1])
        left_seq = type_dispatch(args[0])
        return mt_ast.FilterWithObj(left_seq, filter_obj)
    else:
        raise UnexpectedTermSequence('unknown sequence for FILTER -> %s' % args[1])

@handles_type(r_ast.Update)
def handle_update(node):
    args = node.args
    if isinstance(args[1], r_ast.Func):
        return handle_generic_binop(mt_ast.UpdateWithFunc, node)
    elif isinstance(args[1], r_ast.MakeObj):
        update_obj = plain_obj_of_make_obj(args[1])
        left_seq = type_dispatch(args[0])
        return mt_ast.UpdateWithObj(left_seq, update_obj)
    else:
        raise UnexpectedTermSequence('unknown sequence for UPDATE -> %s' % args[1])

@handles_type(r_ast.Pluck)
def handle_pluck(node):
    args = node.args
    if isinstance(args[1], r_ast.MakeArray):
        attrs = plain_list_of_make_array(args[1])
    else:
        assert(isinstance(args[1], r_ast.Datum))
        attrs = [plain_val_of_datum(datum) for datum in args[1:]]

    left = type_dispatch(args[0])
    return mt_ast.PluckPoly(left, attrs)


def rewrite_query(query):
    return type_dispatch(query)

