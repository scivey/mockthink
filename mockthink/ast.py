import operator
from . import util
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
    def __init__(self, val):
        self.val = val

    def __str__(self):
        return "<DATUM: %s>" % self.val

    def run(self, arg, scope):
        return self.val

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

class Ternary(RBase):
    def __init__(self, left, middle, right):
        self.left = left
        self.middle = middle
        self.right = right

    def do_run(self, left, middle, right, arg, scope):
        raise NotImplemented()

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        middle = self.middle.run(arg, scope)
        right = self.right.run(arg, scope)
        return self.do_run(left, middle, right, arg, scope)

class ByFuncBase(RBase):
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def do_run(self, left, map_fn, arg, scope):
        pass

    def run(self, arg, scope):
        left = self.left.run(arg, scope)
        map_fn = lambda x: self.right.run([x], scope)
        return self.do_run(left, map_fn, arg, scope)

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

# #################
#   Query handlers
# #################


class RDb(MonExp):
    def do_run(self, db_name, arg, scope):
        return arg.get_db(db_name)

    def get_db_name(self):
        return self.left.run(None, Scope({}))

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

class Replace(BinExp):
    def do_run(self, left, right, arg, scope):
        current_table = self.find_table_scope()
        current_db = self.find_db_scope()
        return arg.update_by_id_in_table_in_db(current_db, current_table, right)

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


class UpdateBase(object):
    def __init__(self, *args):
        pass

    def update_table(self, result_sequence, arg):
        current_db = self.find_db_scope()
        current_table = self.find_table_scope()
        return arg.update_by_id_in_table_in_db(current_db, current_table, result_sequence)

class UpdateByFunc(ByFuncBase, UpdateBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return self.update_table(map(map_fn, sequence), arg)

class UpdateWithObj(BinExp, UpdateBase):
    def do_run(self, sequence, to_update, arg, scope):
        map_fn = util.extend_with(to_update)
        if isinstance(sequence, dict):
            result = [map_fn(sequence)]
        else:
            result = map(map_fn, sequence)
        return self.update_table(result, arg)

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
    def do_run(self, sequence, start_and_end, arg, scope):
        start, end = start_and_end
        return util.slice_with(start, end)(sequence)

class Nth(BinExp):
    def do_run(self, sequence, n, arg, scope):
        return util.nth(n)(sequence)

class SumByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        mapped = [util.getter(field)(elem) for elem in sequence]
        nums = [elem for elem in mapped if util.is_num(elem)]
        return sum(nums)

class SumByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        nums = [elem for elem in map(map_fn, sequence) if util.is_num(elem)]
        return sum(nums)

class MaxByField(BinExp):
    def do_run(self, sequence, field, arg, scope):
        return util.max_mapped(util.getter(field), sequence)

class MaxByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.max_mapped(map_fn, sequence)

class GroupByField(BinExp):
    def do_run(self, elems, field, arg, scope):
        return util.group_by_func(util.getter(field), elems)

class GroupByFunc(ByFuncBase):
    def do_run(self, sequence, map_fn, arg, scope):
        return util.group_by_func(map_fn, sequence)







class Between(Ternary):
    def do_run(self, table, lower_key, upper_key, arg, scope):
        out = []
        for document in table:
            doc_id = util.getter('id')(document)
            if doc_id < upper_key and doc_id > lower_key:
                out.append(document)
        return out





# ###########
#   Joins
# ###########

def do_eq_join(left_field, left, right_field, right):
    out = []
    for elem in left:
        lval = util.getter(left_field)(elem)
        match = util.find_first(lambda d: util.getter(right_field)(d) == lval, right)
        if match:
            out.append({'left': elem, 'right': match})
    return out

class EqJoin(Ternary):
    def do_run(self, left, field, right, arg, scope):
        return do_eq_join(field, left, 'id', right)

class InnerOuterJoinBase(RBase):
    def __init__(self, left, middle, right):
        self.left = left
        self.middle = middle
        self.right = right

    def run(self, arg, scope):
        left_seq = self.left.run(arg, scope)
        right_seq = self.middle.run(arg, scope)
        pred = lambda x, y: self.right.run([x, y], scope)
        return self.do_run(left_seq, right_seq, pred, arg, scope)

def do_inner_join(pred, left, right):
    out = []
    for left_elem in left:
        for right_elem in right:
            if pred(left_elem, right_elem):
                out.append({'left': left_elem, 'right': right_elem})
    return out

class InnerJoin(InnerOuterJoinBase):
    def do_run(self, left, right, pred, arg, scope):
        return do_inner_join(pred, left, right)

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

class OuterJoin(InnerOuterJoinBase):
    def do_run(self, left, right, pred, arg, scope):
        return do_outer_join(pred, left, right)








class Zip(RBase):
    pass


class OrderBy(RBase):
    pass

class IndexesOf(RBase):
    pass

class Union(RBase):
    pass

class Sample(RBase):
    pass

class UnGroup(RBase):
    pass

class Contains(RBase):
    pass

class Reduce(RBase):
    pass


class Avg(RBase):
    pass

class Min(RBase):
    pass

class Max(RBase):
    pass

class Row(RBase):
    pass

class Append(RBase):
    pass

class Prepend(RBase):
    pass

class Difference(RBase):
    pass

class SetInsert(RBase):
    pass

class SetUnion(RBase):
    pass

class SetIntersection(RBase):
    pass

class SetDifference(RBase):
    pass

class InsertAt(RBase):
    pass

class SpliceAt(RBase):
    pass

class DeleteAt(RBase):
    pass

class ChangeAt(RBase):
    pass

class Keys(RBase):
    pass

class Literal(RBase):
    pass

class StrMatch(RBase):
    pass

class StrSplit(RBase):
    pass

class StrUpcase(RBase):
    pass

class StrDowncase(RBase):
    pass

class Random(RBase):
    pass

class And(RBase):
    pass

class Or(RBase):
    pass

class Args(RBase):
    pass

class Binary(RBase):
    pass

class Do(RBase):
    pass

class Branch(RBase):
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
