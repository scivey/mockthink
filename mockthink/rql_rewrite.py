import rethinkdb.ast as r_ast
from future.utils import iteritems
from past.builtins import map

from . import ast as mt_ast
from . import util

def rewrite_query(query):
    """Rewrite a ReQL query from `r_ast` types into corresponding `mt_ast` terms."""
    return type_dispatch(query)

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
        return {k: plain_val_of_datum(v) for k, v in iteritems(node.optargs)}
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
    for r_type, m_type in iteritems(mt_type_map):
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

@util.curry2
def handle_generic_aggregation(mt_type_map, node):
    optargs = process_optargs(node)
    if len(node.args) == 1:
        Mt_Constructor = mt_type_map[1]
        return Mt_Constructor(
            type_dispatch(node.args[0]),
            optargs=optargs
        )
    else:
        for r_type, m_type in iteritems(mt_type_map[2]):
            if isinstance(node.args[1], r_type):
                Mt_Constructor = m_type
                break
    return Mt_Constructor(
        type_dispatch(node.args[0]),
        type_dispatch(node.args[1]),
        optargs=optargs
    )

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
        expected_types = (r_ast.Datum, r_ast.Asc, r_ast.Desc, r_ast.Func, r_ast.MakeArray, r_ast.MakeObj)
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


#
#   ReQL functions have an arity one greater than they seem to.
#
#   In the query `r.db('some_db').table('some_table')`, `table`'s arguments are really:
#       table(r.db('some_db'), 'some_table')
#
#   Likewise, `r.db('some_Db').table('some_table').get('doc-id')` is really:
#       get(r.table(r.db('some_db'), 'some_table'), 'doc-id')
#
#   i.e. the immediately preceding term is really a term's first argument, and so the deepest
#   term in the AST is really the first term in the query.
#

#   0-ary reql terms which don't need any special handling
NORMAL_ZEROPS = {
    r_ast.Now: mt_ast.Now,
    r_ast.DbList: mt_ast.DbList
}


#   1-ary reql terms which don't need any special handling
NORMAL_MONOPS = {
    r_ast.Var: mt_ast.RVar,
    r_ast.DB: mt_ast.RDb,
    r_ast.Delete: mt_ast.Delete,
    r_ast.IsEmpty: mt_ast.IsEmpty,
    r_ast.Keys: mt_ast.Keys,
    r_ast.Downcase: mt_ast.StrDowncase,
    r_ast.Upcase: mt_ast.StrUpcase,
    r_ast.Asc: mt_ast.Asc,
    r_ast.Desc: mt_ast.Desc,
    r_ast.Zip: mt_ast.Zip,
    r_ast.Json: mt_ast.Json,
    r_ast.TypeOf: mt_ast.TypeOf,
    r_ast.IndexList: mt_ast.IndexList,
    r_ast.Sync: mt_ast.Sync,
    r_ast.Ungroup: mt_ast.UnGroup,
    r_ast.Not: mt_ast.Not,
    r_ast.TableList: mt_ast.TableList,
    r_ast.DbDrop: mt_ast.DbDrop,
    r_ast.DbCreate: mt_ast.DbCreate,
    r_ast.Year: mt_ast.Year,
    r_ast.Month: mt_ast.Month,
    r_ast.Day: mt_ast.Day,
    r_ast.Hours: mt_ast.Hours,
    r_ast.Minutes: mt_ast.Minutes,
    r_ast.Seconds: mt_ast.Seconds,
    r_ast.TimeOfDay: mt_ast.TimeOfDay,
    r_ast.DayOfWeek: mt_ast.DayOfWeek,
    r_ast.Date: mt_ast.Date,
    r_ast.ToEpochTime: mt_ast.ToEpochTime,
    r_ast.Literal: mt_ast.Literal,
    r_ast.Distinct: mt_ast.Distinct,
    r_ast.ISO8601: mt_ast.ISO8601
}

#   2-ary reql terms which don't need any special handling
NORMAL_BINOPS = {
    r_ast.And: mt_ast.And,
    r_ast.Or: mt_ast.Or,
    r_ast.Ge: mt_ast.Gte,
    r_ast.Lt: mt_ast.Lt,
    r_ast.Le: mt_ast.Lte,
    r_ast.Eq: mt_ast.Eq,
    r_ast.Ne: mt_ast.Neq,
    r_ast.Gt: mt_ast.Gt,
    r_ast.Nth: mt_ast.Nth,
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
    r_ast.Insert: mt_ast.Insert,
    r_ast.IndexDrop: mt_ast.IndexDrop,
    r_ast.TableCreate: mt_ast.TableCreate,
    r_ast.TableDrop: mt_ast.TableDrop,
    r_ast.Default: mt_ast.RDefault,
    r_ast.CoerceTo: mt_ast.CoerceTo
}


#   2-ary terms which we handle with different mt_ast terms depending
#   on the type of their second argument.  This allows us to avoid some type-checking
#   at evaluation time, though we still need to branch on evaluate type of first argument
#   in many cases.
BINOPS_BY_ARG_2_TYPE = {
    r_ast.Group: {
        r_ast.Datum: mt_ast.GroupByField,
        r_ast.Func: mt_ast.GroupByFunc
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

#   ReQL represents these as varargs functions, which can take an array as second arg or
#   a sequence of datums for args 1...N.
#
#   e.g. these are both allowed:
#       r.db('x').table('y').pluck('id', 'name')
#       r.db('x').table('y').pluck(['id', 'name'])
#
#   We check arg count and type and normalize to 2-ary functions of (arg0, [varargs])
#   so they can be uniformly handled at evaluation.
SPLATTED_BINOPS = {
    r_ast.Pluck: mt_ast.PluckPoly,
    r_ast.HasFields: mt_ast.HasFields,
    r_ast.Without: mt_ast.WithoutPoly,
    r_ast.GetAll: mt_ast.GetAll,
    r_ast.DeleteAt: mt_ast.DeleteAt
}

#   3-ary reql terms which don't need any special handling
NORMAL_TERNOPS = {
    r_ast.EqJoin: mt_ast.EqJoin,
    r_ast.InnerJoin: mt_ast.InnerJoin,
    r_ast.OuterJoin: mt_ast.OuterJoin,
    r_ast.InsertAt: mt_ast.InsertAt,
    r_ast.SpliceAt: mt_ast.SpliceAt,
    r_ast.ChangeAt: mt_ast.ChangeAt,
    r_ast.Branch: mt_ast.Branch,
    r_ast.IndexRename: mt_ast.IndexRename,
    r_ast.Between: mt_ast.Between,
    r_ast.During: mt_ast.During
}

#   We can determine a lot about these functions' behavior based on arg count.
#   Since we already know so much, there's no need to handle the branching logic at evaluation time.
#   Instead, we represent them as different `mt_ast` types.
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
    },
    r_ast.IndexCreate: {
        2: mt_ast.IndexCreateByField,
        3: mt_ast.IndexCreateByFunc
    },
    r_ast.IndexWait: {
        1: mt_ast.IndexWaitAll,
        2: mt_ast.IndexWaitOne
    },
    r_ast.UserError: {
        0: mt_ast.RError0,
        1: mt_ast.RError1
    }
}

NORMAL_AGGREGATIONS = {
    r_ast.Min: {
        1: mt_ast.Min1,
        2: {
            r_ast.Datum: mt_ast.MinByField,
            r_ast.Func: mt_ast.MinByFunc
        }
    },
    r_ast.Max: {
        1: mt_ast.Max1,
        2: {
            r_ast.Datum: mt_ast.MaxByField,
            r_ast.Func: mt_ast.MaxByFunc
        }
    },
    r_ast.Avg: {
        1: mt_ast.Avg1,
        2: {
            r_ast.Datum: mt_ast.AvgByField,
            r_ast.Func: mt_ast.AvgByFunc
        }
    },
    r_ast.Sum: {
        1: mt_ast.Sum1,
        2: {
            r_ast.Datum: mt_ast.SumByField,
            r_ast.Func: mt_ast.SumByFunc
        }
    }
}

for r_type, mt_type in iteritems(NORMAL_ZEROPS):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_zerop(mt_type)

for r_type, mt_type in iteritems(NORMAL_MONOPS):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_monop(mt_type)

for r_type, mt_type in iteritems(NORMAL_BINOPS):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop(mt_type)

for r_type, arg_2_map in iteritems(BINOPS_BY_ARG_2_TYPE):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_binop_poly_2(arg_2_map)

for r_type, mt_type in iteritems(SPLATTED_BINOPS):
    RQL_TYPE_HANDLERS[r_type] = binop_splat(mt_type)

for r_type, mt_type in iteritems(NORMAL_TERNOPS):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_ternop(mt_type)

for r_type, mt_type in iteritems(OPS_BY_ARITY):
    RQL_TYPE_HANDLERS[r_type] = handle_n_ary(mt_type)

for r_type, type_map in iteritems(NORMAL_AGGREGATIONS):
    RQL_TYPE_HANDLERS[r_type] = handle_generic_aggregation(type_map)

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
    return mt_ast.MakeObj({k: type_dispatch(v) for k, v in iteritems(node.optargs)})

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
    optargs = process_optargs(node)
    left = type_dispatch(node.args[0])
    right = []
    for elem in node.args[1:]:
        if isinstance(elem, r_ast.Datum):
            right.append(mt_ast.Asc(type_dispatch(elem)))
        else:
            accepted = (r_ast.Desc, r_ast.Asc, r_ast.Func)
            assert(elem.__class__ in accepted)
            right.append(type_dispatch(elem))
    if isinstance(right[0], mt_ast.RFunc):
        right = right[0]
        return mt_ast.OrderByFunc(left, right, optargs=optargs)
    right = mt_ast.MakeArray(right)
    return mt_ast.OrderByKeys(left, right, optargs=optargs)

@handles_type(r_ast.OffsetsOf)
def handle_offsets_of(node):
    optargs = process_optargs(node)
    left = type_dispatch(node.args[0])
    right = type_dispatch(node.args[1])
    if isinstance(node.args[1], r_ast.Func):
        return mt_ast.OffsetsOfFunc(
            left, right, optargs=optargs
        )
    else:
        return mt_ast.OffsetsOfValue(
            left, right, optargs=optargs
        )

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

@handles_type(r_ast.Time)
def handle_time(node):
    arg = makearray_of_datums(node.args)
    return mt_ast.Time(arg)

@util.curry2
def is_instance_of_any(type_tuple, to_test):
    result = False
    for one_type in type_tuple:
        if isinstance(to_test, one_type):
            result = True
            break
    return result

@handles_type(r_ast.Count)
def handle_count(node):
    optargs = process_optargs(node)
    left = type_dispatch(node.args[0])
    if len(node.args) == 1:
        return mt_ast.Count1(
            left,
            optargs=optargs
        )
    else:
        right = type_dispatch(node.args[1])
        if is_instance_of_any((r_ast.MakeObj, r_ast.Datum, r_ast.MakeArray), node.args[1]):
            return mt_ast.CountByEq(
                left,
                right,
                optargs=optargs
            )
        elif isinstance(node.args[1], r_ast.Func):
            return mt_ast.CountByFunc(
                left,
                right,
                optargs=optargs
            )
    raise TypeError


@handles_type(r_ast.Contains)
def handle_contains(node):
    sequence = type_dispatch(node.args[0])
    optargs = process_optargs(node)
    rest = makearray_of_datums(node.args[1:])
    if isinstance(node.args[1], r_ast.Func):
        return mt_ast.ContainsFuncs(sequence, rest, optargs=optargs)
    else:
        return mt_ast.ContainsElems(sequence, rest, optargs=optargs)



#   ImplicitVar handling.
#   `ImplicitVar`s show up in ReQL expressions as `r.row`, e.g.:
#       r.db('x').table('y').map(
#           r.row('value').add(10)
#       )
#   ReQL rewrites this to a `Func` term but does not replace ivars
#   in the client (handled server-side).  We handle these by taking
#   the first parameter symbol of the `r_ast.Func`, recursing through its
#   body, and replacing any ImplicitVar instances with Var(Datum(symbol)).
#   Once that's done, the `Func` can be evaluated in the same way as any other.

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
    for key, val in iteritems(node.optargs):
        if is_ivar(val):
            node.optargs[key] = r_ast.Var(r_ast.Datum(arg_symbol))
        else:
            replace_implicit_vars(arg_symbol, node.optargs[key])
