import operator
import random
from . import util, joins
from .scope import Scope

from pprint import pprint

def map_with_scope(map_fn, scope, to_map):
    return map(lambda elem: map_fn(elem, scope), to_map)

def filter_with_scope(filter_fn, scope, to_filter):
    return filter(lambda elem: filter_fn(elem, scope), to_filter)



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
        pprint({
            'args': args,
            'params': self.param_names
        })
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
        pass

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
        pass

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
        raise NotImplemented()

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
        pass

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


class RDb(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.get_db(db_name)

    def get_db_name(self):
        return self.left.run(None, Scope({}))


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
        return arg.remove_by_id_in_table_in_db(current_db, current_table, sequence)

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

# class Do(RBase):
#     def __init__(self, left, right, optargs={}):
#         self.left = left
#         self.right = right

#     def run(self, arg, scope):
#         pred = lambda x: self.right.run(x, scope)
#         sequence = self.left.run(arg, scope)
#         return pred(sequence)


class Branch(RBase):
    def __init__(self, test, if_true, if_false, optargs={}):
        self.test = test
        self.if_true = if_true
        self.if_false = if_false

    def run(self, arg, scope):
        test = self.test.run(arg, scope)
        if test == False or test == None:
            return self.if_true.run(arg, scope)
        else:
            return self.if_false.run(arg, scope)




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











class Between(Ternary):
    def do_run(self, table, lower_key, upper_key, arg, scope):
        out = []
        for document in table:
            doc_id = util.getter('id')(document)
            if doc_id < upper_key and doc_id > lower_key:
                out.append(document)
        return out

class InsertAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.insert_at(value, index, sequence)

class SpliceAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.splice_at(value, index, sequence)

class ChangeAt(Ternary):
    def do_run(self, sequence, index, value, arg, scope):
        return util.change_at(value, index, sequence)


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





class UnGroup(RBase):
    pass

class Contains(RBase):
    pass

class Reduce(RBase):
    pass



class Row(RBase):
    pass


class Difference(RBase):
    pass

class DeleteAt(RBase):
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

class RError(RBase):
    pass

class RDefault(RBase):
    pass

class RExpr(RBase):
    pass

class Js(RBase):
    pass

class CoerceTo(RBase):
    pass

class TypeOf(RBase):
    pass

class Info(RBase):
    pass

class Json(RBase):
    pass

class Http(RBase):
    pass

class Uuid(RBase):
    pass

class Distinct(RBase):
    pass
