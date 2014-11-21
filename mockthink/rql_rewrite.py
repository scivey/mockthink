from pprint import pprint
import rethinkdb.ast as r_ast
from . import ast as mt_ast
# from rethinkdb import ql2_pb2 as rql_proto
# pTerm = rql_proto.Term.TermType
# pQuery = rql_proto.Query.QueryType
# pDatum = rql_proto.Datum.DatumType
from . import util

def rql_query_to_tree(query):
    current_query = query
    current_val = None
    if isinstance(query, r_ast.Datum):
        current_val = query.data
    elif isinstance(query, r_ast.MakeObj):
        current_val = {k: rql_query_to_tree(v) for k, v in query.iteritems()}
    elif hasattr(current_query, 'args'):
        current_val = map(rql_query_to_tree, current_query.args)
        if len(current_val) == 1:
            current_val = current_val[0]
    # return [current_query.__class__.__name__, current_val]
    return [current_query, current_val]

RQL_TYPE_HANDLERS = {}

COUNT = {
    'n': 0
}
def type_dispatch(rql_node):
    # if COUNT['n'] >= 5:
    #     return
    COUNT['n'] += 1
    print 'TYPE_DISPATCH: %s' % rql_node.__class__.__name__
    handler = RQL_TYPE_HANDLERS[rql_node.__class__]
    print handler.func_name
    pprint(rql_node)
    if hasattr(rql_node, 'args'):
        pprint(rql_node.args)
    result = handler(rql_node)
    pprint(result)
    return result

@util.curry2
def handles_type(rql_type, func):
    def handler(node):
        assert(type(node) == rql_type)
        return func(node)
    RQL_TYPE_HANDLERS[rql_type] = handler
    return handler

def binop_map(Mt_Constructor, node):
    return Mt_Constructor(type_dispatch(node.args[0]), type_dispatch(node.args[1]))

@handles_type(r_ast.Var)
def handle_var(node):
    return mt_ast.RVar(type_dispatch(node.args[0]))

@handles_type(r_ast.Datum)
def handle_datum(node):
    return mt_ast.RDatum(node.data)

@util.curry2
def handle_generic_binop(Mt_Constructor, node):
    return Mt_Constructor(type_dispatch(node.args[0]), type_dispatch(node.args[1]))


NORMAL_BINOPS = {
    r_ast.Ge: mt_ast.Gte,
    r_ast.Lt: mt_ast.Lt,
    r_ast.Le: mt_ast.Lte,
    r_ast.Eq: mt_ast.Eq,
    r_ast.Ne: mt_ast.Neq,
    r_ast.Gt: mt_ast.Gt,
    r_ast.Bracket: mt_ast.Bracket,
    r_ast.Table: mt_ast.RTable
}

for r_type, mt_type in NORMAL_BINOPS.iteritems():
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop(mt_type)




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

@handles_type(r_ast.Func)
def handle_func(node):
    func_params = plain_list_of_make_array(node.args[0])
    func_body = type_dispatch(node.args[1])
    return mt_ast.RFunc(func_params, func_body)

@handles_type(r_ast.Filter)
def handle_filter(node):
    print 'HANDLE_FILTER'
    args = node.args
    if isinstance(args[1], r_ast.Func):
        pprint(args[1])
        return mt_ast.FilterWithFunc(type_dispatch(args[0]), type_dispatch(args[1]))

    if type(parts[1]) == r_ast.Func:
        pprint(parts)
    return {'FILTER_TYPE_UNKNOWN': node}

@handles_type(r_ast.DB)
def handle_db(node):
    return mt_ast.RDb(type_dispatch(node.args[0]))

def rewrite_query(query):
    pprint(query)
    pprint(query.args)
    return type_dispatch(query)

    # return type_dispatch(rql_query_to_tree(query)[0])
