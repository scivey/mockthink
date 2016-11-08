from __future__ import absolute_import, division, print_function, unicode_literals

from collections import defaultdict

from future.utils import iteritems, old_div


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
    for one_dict in dicts:
        out.update(one_dict)
    return out

def clone(x):
    if isinstance(x, dict):
        return obj_clone(x)
    elif isinstance(x, list):
        return clone_array(x)
    else:
        return x

def deep_extend_pair(dict1, dict2):
    out = {}
    out.update(dict1)
    for k, v in iteritems(dict2):
        if k not in out:
            out[k] = clone(v)
        else:
            d1_val = dict1[k]
            if isinstance(d1_val, dict) and isinstance(v, dict):
                out[k] = deep_extend_pair(d1_val, v)
            elif isinstance(d1_val, list) and isinstance(v, list):
                out[k] = cat(d1_val, v)
            else:
                out[k] = clone(v)
    return out

def deep_extend(*dicts):
    out = {}
    for one_dict in dicts:
        out = deep_extend_pair(out, one_dict)
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
    return list(map(fn, a_list))

@curry2
def maybe_map(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    elif is_iterable(thing):
        return list(map(fn, thing))
    else:
        return fn(thing)

@curry2
def maybe_filter(fn, thing):
    if isinstance(thing, dict):
        return fn(thing)
    elif is_iterable(thing):
        return list(filter(fn, thing))
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
    for k, v in iteritems(to_match):
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
        return {k: v for k,v in iteritems(thing) if k in attrs}
    return inner_pluck

def get_by_id(id):
    return find_first(match_attr('id', id))

def as_obj(pairs):
    return {p[0]: p[1] for p in pairs}

def clone_array(x):
    return [elem for elem in x]

@curry2
def without(bad_attrs, thing):
    return {k: v for k, v in iteritems(thing) if k not in bad_attrs}

def obj_clone(a_dict):
    return {k: v for k, v in iteritems(a_dict)}

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
def min_mapped(func, sequence):
    current = (func(sequence[0]), sequence[0])
    for elem in sequence[1:]:
        val = func(elem)
        if is_num(val) and val < current[0]:
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
    actual_nums = list(filter(is_num, nums))
    return old_div(sum(actual_nums), (len(actual_nums) + 0.0))

def safe_max(nums):
    return max(filter(is_num, nums))

def safe_min(nums):
    return min(filter(is_num, nums))

def array_of_string(string):
    return [char for char in string]

def rql_str_split(string, split_on, limit=-1):
    if not split_on:
        if isinstance(split_on, str):
            # rql's string.split mimics python's except for splitting on empty string.
            # in that case python throws an error, while rql converts to char array
            return array_of_string(string)
        else:
            # pythons string.split() seems not to allow a limit with default split_on
            return string.split()
    return string.split(split_on, limit)

def sort_by_one(sort_key, sequence, reverse=False):
    out = clone_array(sequence)
    kwargs = {
        'key': lambda doc: getter(sort_key, doc)
    }
    if reverse:
        kwargs['reverse'] = True
    out.sort(**kwargs)
    return out

def sort_by_many(keys_and_dirs, sequence):
    # keys_and_dirs is a list of tuples and orders:
    # [('name', 'ASC'), ('weight', 'DESC')]

    # this probably isn't all that efficient, but
    # we can figure that out later.
    key_for_pass = keys_and_dirs[0]
    current_pass = sort_by_one(key_for_pass[0], sequence, reverse=(key_for_pass[1] == 'DESC'))
    if len(keys_and_dirs) == 1:
        return current_pass
    else:
        result = []
        chunk = []
        current_key = None
        def handle_chunk():
            result.extend(sort_by_many(keys_and_dirs[1:], chunk))
        for elem in current_pass:
            next_key = getter(key_for_pass[0], elem)
            if next_key != current_key:
                if chunk:
                    handle_chunk()
                chunk = [elem]
            else:
                chunk.append(elem)
            current_key = next_key
        handle_chunk()
        return result

def indices_of_passing(pred, sequence):
    out = []
    for index in range(0, len(list(sequence))):
        if pred(sequence[index]):
            out.append(index)
    return out

def without_indices(indices, sequence):
    indices = set(indices)
    for index in range(0, len(sequence)):
        if index not in indices:
            yield sequence[index]

@curry2
def eq(x, y):
    return x == y

def sorted_iteritems(a_dict):
    keys = a_dict.keys()
    for k in sorted(keys):
        yield k, a_dict[k]

def sorted_list(a_list):
    a_list = clone_array(a_list)
    a_list.sort()
    return a_list

def make_hashable(x):
    if is_simple(x):
        return x
    elif isinstance(x, list):
        return tuple(make_hashable(elem) for elem in sorted_list(x))
    elif isinstance(x, dict):
        out = []
        for k, v in sorted_iteritems(x):
            out.append((k, make_hashable(v)))
        return tuple(elem for elem in out)

class DictableSet(set):
    def __init__(self, elems):
        elems = map(make_hashable, elems)
        super(DictableSet, self).__init__(elems)

    def add(self, elem):
        elem = make_hashable(elem)
        super(DictableSet, self).add(elem)

    def has(self, elem):
        return (make_hashable(elem) in self)


def dictable_distinct(sequence):
    seen = DictableSet([])
    for elem in sequence:
        if not seen.has(elem):
            seen.add(elem)
            yield elem


@curry2
def any_passing(pred, sequence):
    result = False
    for elem in sequence:
        if pred(elem):
            result = True
            break
    return result

