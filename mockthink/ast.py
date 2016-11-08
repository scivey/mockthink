from __future__ import unicode_literals, absolute_import, print_function, division

from rethinkdb import RqlRuntimeError, RqlDriverError, RqlCompileError
import operator
import random
import uuid
import json
import dateutil.parser
from pprint import pprint
from future.utils import iteritems, text_type
from past.utils import old_div

from . import util, joins, rtime
from .scope import Scope

from . import ast_base
from .ast_base import RBase, MonExp, BinExp, Ternary, ByFuncBase
from .ast_base import LITERAL_OBJECT, LITERAL_LIST, RDatum, RFunc, MakeObj, MakeArray
from past.builtins import filter



# #################
#   Query handlers
# #################


class Literal(MonExp):
    def do_run(self, obj, arg, scope):
        pprint({'literal': obj})
        return LITERAL_OBJECT.from_dict(obj)

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
        if hasattr(self, 'mockdb_ref'):
            db = self.mockdb_ref
        else:
            db = arg
        return db.get_db(db_name)

    def find_db_scope(self):
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


class Distinct(MonExp):
    def do_run(self, table_or_seq, arg, scope):
        if 'index' in self.optargs:
            # table
            table_or_seq = table_or_seq._index_values(self.optargs['index'])
        return list(util.dictable_distinct(table_or_seq))

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
    def find_table_scope(self):
        return self.right.run(None, Scope({}))

    def has_table_scope(self):
        return True

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
            index_func, is_multi = self.find_index_func_for_scope(
                self.optargs['index'],
                arg
            )
            if isinstance(index_func, RFunc):
                map_fn = lambda d: index_func.run([d], scope)
            else:
                map_fn = index_func

            result = []
            left = list(left)
            if is_multi:
                seen_ids = set([])
                for elem in left:
                    indexed = map_fn(elem)
                    if not isinstance(indexed, (tuple, list)):
                        indexed = [indexed]
                    indexed = set(indexed)
                    for match_item in right:
                        if match_item in indexed:
                            if elem['id'] not in seen_ids:
                                seen_ids.add(elem['id'])
                                result.append(elem)
                            break
            else:
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
    binop = staticmethod(old_div)

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

    def get_update_settings(self):
        defaults = {
            'durability': 'hard',
            'return_changes': False,
            'non_atomic': False
        }
        return util.extend(defaults, self.optargs)

    def validate_nested_query_status(self):
        if self.right.has_table_scope() and (not self.get_update_settings()['non_atomic']):
            self.raise_rql_runtime_error('attempted nested query in update without non-atomic flag')

    def update_table(self, result_sequence, arg, scope):
        settings = self.get_update_settings()
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        result_sequence = util.ensure_list(result_sequence)
        result, report = arg.update_by_id_in_table_in_db(current_db, current_table, result_sequence)
        if not settings['return_changes']:
            del report['changes']
        return result, report

class UpdateByFunc(ByFuncBase, UpdateBase):
    def do_run(self, sequence, map_fn, arg, scope):
        self.validate_nested_query_status()
        def mapper(doc):
            ext_with = map_fn(doc)
            return ast_base.rql_merge_with(ext_with, doc)
        return self.update_table(util.maybe_map(mapper, sequence), arg, scope)

class UpdateWithObj(BinExp, UpdateBase):
    def do_run(self, sequence, to_update, arg, scope):
        self.validate_nested_query_status()
        return self.update_table(util.maybe_map(ast_base.rql_merge_with(to_update), sequence), arg, scope)

class Replace(BinExp, UpdateBase):
    def do_run(self, left, right, arg, scope):
        return self.update_table(right, arg, scope)

class Delete(MonExp):
    def get_delete_settings(self):
        defaults = {
            'durability': 'hard',
            'return_changes': False
        }
        return util.extend(defaults, self.optargs)

    def do_run(self, sequence, arg, scope):
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        if isinstance(sequence, dict):
            sequence = [sequence]
        else:
            sequence = list(sequence)
        result, report = arg.remove_by_id_in_table_in_db(current_db, current_table, sequence)
        if not self.get_delete_settings()['return_changes']:
            del report['changes']
        return result, report

class Insert(BinExp):
    def get_insert_settings(self):
        defaults = {
            'durability': 'hard',
            'return_changes': False,
            'conflict': 'error'
        }
        return util.extend(defaults, self.optargs)

    def do_run(self, sequence, to_insert, arg, scope):
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        if isinstance(to_insert, dict):
            to_insert = [to_insert]
        generated_keys = list()

        def ensure_id(elem):
            if (u'id' not in elem) or (elem[u'id'] is None):
                uid = text_type(uuid.uuid4())
                elem = util.extend(elem, {'id': uid})
                generated_keys.append(uid)
            return elem

        to_insert = list(map(ensure_id, list(to_insert)))
        settings = self.get_insert_settings()
        result, report = arg.insert_into_table_in_db(current_db, current_table, to_insert, conflict=settings['conflict'])
        if not settings['return_changes']:
            del report['changes']
        if generated_keys:
            report['generated_keys'] = generated_keys
        return result, report

class FilterWithFunc(ByFuncBase):
    def do_run(self, sequence, filt_fn, arg, scope):
        return filter(filt_fn, sequence)

class FilterWithObj(BinExp):
    def do_run(self, sequence, to_match, arg, scope):
        return filter(util.match_attrs(to_match), sequence)

class MapWithRFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        try:
            result = list(map(map_fn, sequence))
        except KeyError as k:
            message = "Missing field '%s'" % text_type(k)
            self.raise_rql_runtime_error(message)
        return result

class WithoutPoly(BinExp):
    def do_run(self, left, attrs, arg, scope):
        return util.maybe_map(util.without(attrs), left)

class PluckPoly(BinExp):
    def do_run(self, left, attrs, arg, scope):
        pprint({
            'left': left,
            'attrs': attrs
        })
        return util.maybe_map(util.pluck_with(*attrs), left)

class MergePoly(BinExp):
    def do_run(self, left, ext_with, arg, scope):
        if ast_base.is_literal(ext_with):
            self.raise_rql_runtime_error('invalid top-level r.literal()')
        elif ast_base.has_nested_literal(ext_with):
            self.raise_rql_runtime_error('invalid nested r.literal()')

        return util.maybe_map(ast_base.rql_merge_with(ext_with), left)


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

class Sum1(MonExp):
    def do_run(self, sequence, arg, scope):
        return util.safe_sum(sequence)

class SumByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.safe_sum([util.getter(field)(elem) for elem in sequence])

class SumByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.safe_sum(map(map_fn, sequence))

class Max1(MonExp):
    def do_run(self, sequence, arg, scope):
        return max(list(sequence))

class MaxByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.max_mapped(util.getter(field), sequence)

class MaxByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.max_mapped(map_fn, sequence)

class Avg1(MonExp):
    def do_run(self, sequence, arg, scope):
        return util.safe_average(list(sequence))

class AvgByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.safe_average(map(util.getter(field), sequence))

class AvgByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.safe_average(map(map_fn, sequence))

class Count1(MonExp):
    def do_run(self, sequence, arg, scope):
        return len(list(sequence))

class CountByEq(BinExp):
    def do_run(self, sequence, to_match, arg, scope):
        return len([elem for elem in sequence if elem == to_match])

class CountByFunc(ByFuncBase):
    def do_run(self, sequence, filter_fn, arg, scope):
        return len(filter(filter_fn, list(sequence)))

class Min1(MonExp):
    def do_run(self, sequence, arg, scope):
        return min(list(sequence))

class MinByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.min_mapped(util.getter(field), sequence)

class MinByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.min_mapped(map_fn, sequence)

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

class OrderByFunc(ByFuncBase):
    def do_run(self, sequence, func, arg, scope):
        tups = [(item, func(item)) for item in sequence]
        tups.sort(key=lambda x: x[1])
        return [item[0] for item in tups]

class OrderByKeys(BinExp):
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

class OffsetsOfValue(BinExp):
    def do_run(self, sequence, test_val, arg, scope):
        return util.indices_of_passing(util.eq(test_val), list(sequence))

class OffsetsOfFunc(ByFuncBase):
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
        for group_name, group_vals in iteritems(grouped_seq):
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


class ContainsElems(BinExp):
    def do_run(self, sequence, test_for, arg, scope):
        sequence = list(sequence)
        result = True
        for elem in test_for:
            if elem not in sequence:
                result = False
                break
        return result

class ContainsFuncs(RBase):
    def __init__(self, left, right, optargs={}):
        assert(isinstance(right, MakeArray))
        self.left = left
        self.right = right
        self.optargs = optargs

    def iter_preds(self, scope):
        for pred in self.right.vals:
            yield (lambda doc: pred.run([doc], scope))

    def run(self, arg, scope):
        sequence = list(self.left.run(arg, scope))
        result = True
        for pred in self.iter_preds(scope):
            if not util.any_passing(pred, sequence):
                result = False
                break
        return result


#   #################################
#     Table and database manipulation
#   #################################

class TableCreate(BinExp):
    def do_run(self, left, table_name, arg, scope):
        db_name = self.find_db_scope()
        return arg.create_table_in_db(db_name, table_name)

class TableDrop(BinExp):
    def do_run(self, db, table_name, arg, scope):
        db_name = self.find_db_scope()
        return arg.drop_table_in_db(db_name, table_name)

class TableList(MonExp):
    def do_run(self, db, arg, scope):
        db_name = self.find_db_scope()
        return arg.list_tables_in_db(db_name)


class DbCreate(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.create_db(db_name)

class DbDrop(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.drop_db(db_name)

class DbList(RBase):
    def __init__(self, *args, **kwargs):
        pass
    def run(self, arg, scope):
        return arg.list_dbs()

#   #################################
#     Index manipulation functions
#   #################################

class IndexCreateByField(BinExp):
    def do_run(self, sequence, field_name, arg, scope):
        index_func = util.getter(field_name)
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        multi = self.optargs.get('multi', False)
        return arg.create_index_in_table_in_db(
            current_db,
            current_table,
            field_name,
            index_func,
            multi=multi
        )

class IndexCreateByFunc(RBase):
    def __init__(self, left, middle, right, optargs=None):
        self.left = left
        self.middle = middle
        self.right = right
        self.optargs = optargs or {}

    def run(self, arg, scope):
        sequence = self.left.run(arg, scope)
        index_name = self.middle.run(arg, scope)
        index_func = self.right
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        multi = self.optargs.get('multi', False)
        return arg.create_index_in_table_in_db(
            current_db,
            current_table,
            index_name,
            index_func,
            multi=multi
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
            map_fn, _ = self.find_index_func_for_scope(
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

class ToEpochTime(MonExp):
    def do_run(self, dtime, arg, scope):
        return rtime.epoch_time(dtime)

class ISO8601(MonExp):
    def do_run(self, left, arg, scope):
        if not isinstance(left, basestring):
            left = left.run(arg, scope)
        return dateutil.parser.parse(left)


class Time(MonExp):
    def do_run(self, parts, arg, scope):
        parts = list(parts)
        if len(parts) < 4:
            self.raise_rql_compile_error("Expected between 4 and 7 arguments, got 3")
        return rtime.rql_compatible_time(*parts)

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

class StrMatch(RBase):
    pass

class Args(RBase):
    pass

class Binary(RBase):
    pass

class ForEach(RBase):
    pass

class RDefault(BinExp):
    def do_run(self, left, right, arg, scope):
        result = self.left.run(arg, scope)
        if result is None:
            return self.right.run(arg, scope)
        return result

class RExpr(RBase):
    pass

class Js(RBase):
    pass

class CoerceTo(BinExp):
    def do_run(self, left, right, arg, scope):
        res = self.left.run(arg, scope)
        rname = self.right.run(arg, scope)
        if rname.upper() == 'ARRAY':
            if isinstance(res, dict):
                return list(res.items())
            return list(res)
        return res

class Info(RBase):
    pass

class Http(RBase):
    pass
