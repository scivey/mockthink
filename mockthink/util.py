from collections import defaultdict
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
        if not len(args):
            return curry2(lambda y, z: func(x, y, z))
        elif len(args) == 2:
            return func(x, *args)
        else:
            return curry2(lambda a, b: func(x, a, b))(args[0])
    return out

def extend(*dicts):
    out = {}
    pprint({'extend': dicts})
    for one_dict in dicts:
        out.update(one_dict)
    return out

def cat(*lists):
    out = []
    for one_list in lists:
        out.extend(one_list)
    return out

@curry2
def append(elem, a_list):
    return cat(a_list, [elem])

@curry2
def prepend(elem, a_list):
    return cat([elem], a_list)

@curry3
def splice_at(to_splice, index, a_list):
    return cat(a_list[0:index], to_splice, a_list[index:])

@curry3
def insert_at(val, index, a_list):
    return splice_at([val], index, a_list)

@curry3
def change_at(val, index, a_list):
    right_start = index + 1
    return cat(a_list[0:index], [val], a_list[right_start:])

@curry2
def extend_with(a_dict, to_extend):
    return extend(to_extend, a_dict)

@curry2
def map_with(fn, a_list):
    return map(fn, a_list)

@curry2
def maybe_map(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    elif is_iterable(thing):
        return map(fn, thing)
    else:
        return fn(thing)

@curry2
def maybe_filter(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    elif is_iterable(thing):
        return filter(fn, thing)
    else:
        return fn(thing)

def is_simple(x):
    return not (isinstance(x, (list, dict)))

@curry2
def has_attrs(attr_list, thing):
    result = True
    for attr in attr_list:
        if attr not in thing:
            result = False
            break
    return result

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

@curry3
def match_attr_multi(key, good_vals, thing):
    thing_val = getter(key, thing)
    result = False
    for val in good_vals:
        if thing_val == val:
            result = True
            break
    return result


def ensure_list(x):
    if not isinstance(x, list):
        x = [x]
    return x

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

def clone_array(x):
    return [elem for elem in x]

@curry2
def without(bad_attrs, thing):
    return {k: v for k, v in thing.iteritems() if k not in bad_attrs}

def obj_clone(a_dict):
    return {k: v for k, v in a_dict.iteritems()}

def is_iterable(x):
    return hasattr(x, '__iter__')

@curry2
def drop(n, a_list):
    return a_list[n:]

@curry2
def take(n, a_list):
    return a_list[0:n]

@curry3
def slice_with(start, end, a_list):
    return a_list[start:end]

@curry2
def max_mapped(func, sequence):
    current = (func(sequence[0]), sequence[0])
    for elem in sequence[1:]:
        val = func(elem)
        if is_num(val) and val > current[0]:
            current = (val, elem)
    return current[1]

@curry2
def group_by_func(func, sequence):
    output = defaultdict(lambda: [])
    for elem in sequence:
        output[func(elem)].append(elem)
    return output

def is_num(x):
    return isinstance(x, int) or isinstance(x, float)

def safe_sum(nums):
    return sum(filter(is_num, nums))

def safe_average(nums):
    actual_nums = filter(is_num, nums)
    return sum(actual_nums) / (len(actual_nums) + 0.0)

def safe_max(nums):
    return max(filter(is_num, nums))

def safe_min(nums):
    return min(filter(is_num, nums))
