import operator
import random
import uuid
import json
from pprint import pprint

from . import util, joins, rtime
from .scope import Scope

class AttrHaving(object):
    def __init__(self, attrs):
        for k, v in attrs.iteritems():
            setattr(self, k, v)


# #################
#   Base classes
# #################


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

    def find_index_func_for_scope(self, index_name, db_arg):
        table = self.find_table_scope()
        db = self.find_db_scope()
        return db_arg.get_index_func_in_table_in_db(
            self.find_db_scope(),
            self.find_table_scope(),
            index_name
        )

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

    def __str__(self):
        params = ", ".join(self.param_names)
        return "<RFunc: [%s] { %s }>" % (params, self.body)

    def run(self, args, scope):
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
        left = self.left.run(arg, scope)
        map_fn = lambda x: self.right.run(x, scope)
        return self.do_run(left, map_fn, arg, scope)

class MakeObj(RBase):
    def __init__(self, vals):
        self.vals = vals

    def run(self, arg, scope):
        return {k: v.run(arg, scope) for k, v in self.vals.iteritems()}

class MakeArray(RBase):
    def __init__(self, vals):
        self.vals = vals

    def run(self, arg, scope):
        return [elem.run(arg, scope) for elem in self.vals]

# #################
#   Query handlers
# #################



class RError0(RBase):
    def __init__(self, *args):
        pass

    def run(self, arg, conn):
        self.raise_rql_runtime_error('DEFAULT MESSAGE')

class RError1(MonExp):
    def do_run(self, msg, arg, scope):
        self.raise_rql_runtime_error(msg)


class Uuid(RBase):
    def run(self, arg, scope):
        return uuid.uuid4()

class RDb(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.get_db(db_name)

    def get_db_name(self):
        return self.left.run(None, Scope({}))

class TypeOf(MonExp):
    def do_run(self, val, arg, scope):
        type_map = {
            str: 'STRING',
            dict: 'OBJECT',
            int: 'NUMBER',
            float: 'NUMBER',
            bool: 'BOOL'
        }
        if val == None:
            return 'NULL'
        else:
            val_type = type(val)
            if val_type in type_map:
                return type_map[val_type]
            elif util.is_iterable(val):
                return 'ARRAY'
        raise TypeError


class Zip(MonExp):
    def do_run(self, sequence, arg, scope):
        out = []
        for elem in sequence:
            out.append(util.extend(elem['left'], elem['right']))
        return out

class IsEmpty(MonExp):
    def do_run(self, left, arg, scope):
        return (len(left) == 0)

class RVar(MonExp):
    def do_run(self, symbol_name, arg, scope):
        return scope.get_sym(symbol_name)

class Not(MonExp):
    def do_run(self, left, arg, scope):
        return (not left)

class Count(MonExp):
    def do_run(self, left, arg, scope):
        return len(left)

class Keys(MonExp):
    def do_run(self, left, arg, scope):
        return left.keys()

class Asc(MonExp):
    def do_run(self, left, arg, scope):
        return (left, 'ASC')

class Desc(MonExp):
    def do_run(self, left, arg, scope):
        return (left, 'DESC')

class Json(MonExp):
    def do_run(self, json_str, arg, scope):
        return json.loads(json_str)

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
        return util.find_first(util.match_attr('id', right), left)

class GetAll(BinExp):
    def do_run(self, left, right, arg, scope):
        if 'index' in self.optargs and self.optargs['index'] != 'id':
            index_func = self.find_index_func_for_scope(
                self.optargs['index'],
                arg
            )
            if isinstance(index_func, RFunc):
                map_fn = lambda d: index_func.run([d], scope)
            else:
                map_fn = index_func
            result = []
            left = list(left)
            pprint({'foo': left})
            for elem in left:
                if map_fn(elem) in right:
                    result.append(elem)
            return result
        else:
            return filter(util.match_attr_multi('id', right), left)

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

class And(BinOp):
    binop = operator.and_

class Or(BinOp):
    binop = operator.or_

class Reduce(ByFuncBase):
    def do_run(self, sequence, reduce_fn, arg, scope):
        first, second = sequence[0:2]
        result = reduce_fn([first, second])
        for elem in sequence[2:]:
            result = reduce_fn([elem, result])
        return result


class UpdateBase(object):
    def __init__(self, *args):
        pass

    def update_table(self, result_sequence, arg, scope):
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        result_sequence = util.ensure_list(result_sequence)
        return arg.update_by_id_in_table_in_db(current_db, current_table, result_sequence)

class UpdateByFunc(ByFuncBase, UpdateBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return self.update_table(map(map_fn, sequence), arg, scope)

class UpdateWithObj(BinExp, UpdateBase):
    def do_run(self, sequence, to_update, arg, scope):
        return self.update_table(util.maybe_map(util.extend_with(to_update), sequence), arg, scope)

class Replace(BinExp, UpdateBase):
    def do_run(self, left, right, arg, scope):
        return self.update_table(right, arg, scope)

class Delete(MonExp):
    def do_run(self, sequence, arg, scope):
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        return arg.remove_by_id_in_table_in_db(current_db, current_table, list(sequence))

def ensure_id(elem):
    if 'id' not in elem:
        elem = util.extend(elem, {'id': uuid.uuid4()})
    return elem

class Insert(BinExp):
    def do_run(self, sequence, to_insert, arg, scope):
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        if isinstance(to_insert, dict):
            to_insert = [to_insert]
        to_insert = map(ensure_id, list(to_insert))
        return arg.insert_into_table_in_db(current_db, current_table, to_insert)

class FilterWithFunc(ByFuncBase):
    def do_run(self, sequence, filt_fn, arg, scope):
        return filter(filt_fn, sequence)

class FilterWithObj(BinExp):
    def do_run(self, sequence, to_match, arg, scope):
        return filter(util.match_attrs(to_match), sequence)

class MapWithRFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return map(map_fn, sequence)

class WithoutPoly(BinExp):
    def do_run(self, left, attrs, arg, scope):
        return util.maybe_map(util.without(attrs), left)

class PluckPoly(BinExp):
    def do_run(self, left, attrs, arg, scope):
        return util.maybe_map(util.pluck_with(*attrs), left)

class MergePoly(BinExp):
    def do_run(self, left, ext_with, arg, scope):
        return util.maybe_map(util.extend_with(ext_with), left)

class HasFields(BinExp):
    def do_run(self, left, fields, arg, scope):
        return util.maybe_filter(util.has_attrs(fields), left)

class WithFields(BinExp):
    def do_run(self, sequence, keys, arg, scope):
        return [elem for elem in sequence if util.has_attrs(keys, elem)]

class ConcatMap(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.cat(*[util.map_with(map_fn, elem) for elem in sequence])

class Skip(BinExp):
    def do_run(self, sequence, num, arg, scope):
        return util.drop(num)(sequence)

class Limit(BinExp):
    def do_run(self, sequence, num, arg, scope):
        return util.take(num)(sequence)

class Slice(BinExp):
    def do_run(self, sequence, indices, arg, scope):
        start, end = indices
        return util.slice_with(start, end)(sequence)

class Nth(BinExp):
    def do_run(self, sequence, n, arg, scope):
        return util.nth(n)(sequence)

class SumByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.safe_sum([util.getter(field)(elem) for elem in sequence])

class SumByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.safe_sum(map(map_fn, sequence))

class MaxByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.max_mapped(util.getter(field), sequence)

class MaxByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.max_mapped(map_fn, sequence)

class AvgByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.safe_average(map(util.getter(field), sequence))

class AvgByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.safe_average(map(map_fn, sequence))

class MinByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.safe_min(map(util.getter(field), sequence))

class MinByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.safe_min(map(map_fn, sequence))

class GroupByField(BinExp):
    def do_run(self, elems, field, arg, scope):
        return util.group_by_func(util.getter(field), elems)

class GroupByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.group_by_func(map_fn, sequence)

class Append(BinExp):
    def do_run(self, sequence, value, arg, scope):
        return util.append(value, sequence)

class Prepend(BinExp):
    def do_run(self, sequence, value, arg, scope):
        return util.prepend(value, sequence)

class OrderBy(BinExp):
    def do_run(self, sequence, keys, arg, scope):
        return util.sort_by_many(keys, sequence)

class Random0(RBase):
    def __init__(self, optargs={}):
        self.optargs = optargs

    def run(self, arg, scope):
        return random.random()

class Random1(MonExp):
    def do_run(self, max_num, arg, scope):
        if 'float' in self.optargs and self.optargs['float']:
            return random.uniform(0, max_num)
        else:
            return random.randint(0, max_num)

class Random2(BinExp):
    def do_run(self, min_num, max_num, arg, scope):
        if 'float' in self.optargs and self.optargs['float']:
            return random.uniform(min_num, max_num)
        else:
            return random.randint(min_num, max_num)

class Union(BinExp):
    def do_run(self, left, right, arg, scope):
        return list(left) + list(right)

class Sample(BinExp):
    def do_run(self, sequence, sample_n, arg, scope):
        return random.sample(list(sequence), sample_n)

class IndexesOfValue(BinExp):
    def do_run(self, sequence, test_val, arg, scope):
        return util.indices_of_passing(util.eq(test_val), list(sequence))

class IndexesOfFunc(ByFuncBase):
    def do_run(self, sequence, test_fn, arg, scope):
        return util.indices_of_passing(test_fn, list(sequence))



class SetInsert(BinExp):
    def do_run(self, left, right, arg, scope):
        return list(set(util.append(right, list(left))))

class SetUnion(BinExp):
    def do_run(self, left, right, arg, scope):
        return list(set(list(left)).union(set(list(right))))

class SetIntersection(BinExp):
    def do_run(self, left, right, arg, scope):
        return list(set(list(left)).intersection(set(list(right))))

class SetDifference(BinExp):
    def do_run(self, left, right, arg, scope):
        return list(set(list(left)) - set(list(right)))


class Do(ByFuncBase):
    def do_run(self, left, func, arg, scope):
        return func(left)

class UnGroup(MonExp):
    def do_run(self, grouped_seq, arg, scope):
        for group_name, group_vals in grouped_seq.iteritems():
            yield {
                'group': group_name,
                'reduction': group_vals
            }

class Branch(RBase):
    def __init__(self, test, if_true, if_false, optargs={}):
        self.test = test
        self.if_true = if_true
        self.if_false = if_false

    def run(self, arg, scope):
        test = self.test.run(arg, scope)
        if test == False or test == None:
            return self.if_false.run(arg, scope)
        else:
            return self.if_true.run(arg, scope)

class Difference(BinExp):
    def do_run(self, sequence, to_remove, arg, scope):
        to_remove = set(to_remove)
        for elem in sequence:
            if elem not in to_remove:
                yield elem


#   #################################
#     Index manipulation functions
#   #################################

class IndexCreateByField(BinExp):
    def do_run(self, sequence, field_name, arg, scope):
        index_func = util.getter(field_name)
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        return arg.create_index_in_table_in_db(
            current_db,
            current_table,
            field_name,
            index_func
        )

class IndexCreateByFunc(RBase):
    def __init__(self, left, middle, right, optargs={}):
        self.left = left
        self.middle = middle
        self.right = right

    def run(self, arg, scope):
        sequence = self.left.run(arg, scope)
        index_name = self.middle.run(arg, scope)
        index_func = self.right
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        return arg.create_index_in_table_in_db(
            current_db,
            current_table,
            index_name,
            index_func
        )

class IndexRename(Ternary):
    def do_run(self, sequence, old_name, new_name, arg, scope):
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()

        exists = arg.index_exists_in_table_in_db(
            current_db,
            current_table,
            new_name
        )
        if exists:
            if not self.optargs.get('overwrite', False):
                raise Exception('tried to overwrite existing index!')

        return arg.rename_index_in_table_in_db(
            current_db,
            current_table,
            old_name,
            new_name
        )

class IndexDrop(BinExp):
    def do_run(self, sequence, index_name, arg, scope):
        assert(isinstance(self.left, RTable))
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()

        return arg.drop_index_in_table_in_db(
            current_db,
            current_table,
            index_name
        )

class IndexList(MonExp):
    def do_run(self, table, arg, scope):
        assert(isinstance(self.left, RTable))

        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        return arg.list_indexes_in_table_in_db(
            current_db,
            current_table
        )


class IndexWaitAll(MonExp):
    def do_run(self, table, arg, scope):
        assert(isinstance(self.left, RTable))
        return table

class IndexWaitOne(BinExp):
    def do_run(self, table, index_name, arg, scope):
        assert(isinstance(self.left, RTable))
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        exists = arg.index_exists_in_table_in_db(
            current_db,
            current_table,
            index_name
        )
        assert(exists)
        return table

class Sync(MonExp):
    def do_run(self, table, arg, scope):
        assert(isinstance(self.left, RTable))
        return table


#   ####################
#     String functions
#   ####################


class StrUpcase(MonExp):
    def do_run(self, string, arg, scope):
        return string.upper()

class StrDowncase(MonExp):
    def do_run(self, string, arg, scope):
        return string.lower()

class StrSplitDefault(MonExp):
    def do_run(self, string, arg, scope):
        return string.split()

class StrSplitOn(BinExp):
    def do_run(self, string, split_on, arg, scope):
        return util.rql_str_split(string, split_on)

class StrSplitOnLimit(Ternary):
    def do_run(self, string, split_on, limit, arg, scope):
        return util.rql_str_split(string, split_on, limit)






def operators_for_bounds(left_bound, right_bound):
    if left_bound == 'closed':
        left_oper = operator.ge
    else:
        left_oper = operator.gt

    if right_bound == 'closed':
        right_oper = operator.le
    else:
        right_oper = operator.lt

    return left_oper, right_oper




class Between(Ternary):
    def do_run(self, table, lower_key, upper_key, arg, scope):
        defaults = {
            'left_bound': 'closed',
            'right_bound': 'open',
            'index': 'id'
        }
        options = util.extend(defaults, self.optargs)

        if options['index'] == 'id':
            map_fn = util.getter('id')
        else:
            map_fn = self.find_index_func_for_scope(
                options['index'],
                arg
            )

        left_test, right_test = operators_for_bounds(
            options['left_bound'], options['right_bound']
        )
        for document in table:
            doc_val = map_fn(document)
            if left_test(doc_val, lower_key) and right_test(doc_val, upper_key):
                yield document

class InsertAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.insert_at(value, index, sequence)

class SpliceAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.splice_at(value, index, sequence)

class ChangeAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.change_at(value, index, sequence)

class DeleteAt(BinExp):
    def do_run(self, sequence, indices, arg, scope):
        return list(util.without_indices(indices, sequence))


# ###########
#   Joins
# ###########

class EqJoin(Ternary):
    def do_run(self, left, field, right, arg, scope):
        return joins.do_eq_join(field, left, 'id', right)

class InnerOuterJoinBase(RBase):
    def __init__(self, left, middle, right, optargs={}):
        self.left = left
        self.middle = middle
        self.right = right

    def run(self, arg, scope):
        left_seq = self.left.run(arg, scope)
        right_seq = self.middle.run(arg, scope)
        pred = lambda x, y: self.right.run([x, y], scope)
        return self.do_run(left_seq, right_seq, pred, arg, scope)

class InnerJoin(InnerOuterJoinBase):
    def do_run(self, left, right, pred, arg, scope):
        return joins.do_inner_join(pred, left, right)


class OuterJoin(InnerOuterJoinBase):
    def do_run(self, left, right, pred, arg, scope):
        return joins.do_outer_join(pred, left, right)










# ############
#   Time
# ############

class Year(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.year

class Month(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.month

class Day(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.day

class Hours(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.hour

class Minutes(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.minute

class Seconds(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.second

class Date(MonExp):
    def do_run(self, dtime, arg, scope):
        return rtime.to_date(dtime)

class TimeOfDay(MonExp):
    def do_run(self, dtime, arg, scope):
        return rtime.time_of_day_seconds(dtime)

class DayOfWeek(MonExp):
    def do_run(self, dtime, arg, scope):
        return dtime.isoweekday()

class Now(RBase):
    def __init__(self, optsargs={}):
        self.optargs = optargs

    def run(self, db, scope):
        return db.get_now_time()

class Time(MonExp):
    def do_run(self, parts, arg, scope):
        return rtime.make_time(*parts)

class During(Ternary):
    def do_run(self, to_test, left, right, arg, scope):
        defaults = {
            'left_bound': 'closed',
            'right_bound': 'open'
        }
        options = util.extend(defaults, self.optargs)
        left_test, right_test = operators_for_bounds(
            options['left_bound'], options['right_bound']
        )
        return (left_test(to_test, left) and right_test(to_test, right))


class Contains(RBase):
    pass

class Literal(RBase):
    pass

class StrMatch(RBase):
    pass

class Args(RBase):
    pass

class Binary(RBase):
    pass

class ForEach(RBase):
    pass

class RDefault(RBase):
    pass

class RExpr(RBase):
    pass

class Js(RBase):
    pass

class CoerceTo(RBase):
    pass

class Info(RBase):
    pass

class Http(RBase):
    pass

class Distinct(RBase):
    pass
