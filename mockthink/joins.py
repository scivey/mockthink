from pprint import pprint
from . import util

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

def do_inner_join(pred, left, right):
    out = []
    for left_elem in left:
        for right_elem in right:
            if pred(left_elem, right_elem):
                out.append({'left': left_elem, 'right': right_elem})
    return out

def do_eq_join(left_field, left, right_field, right):
    out = []
    for elem in left:
        lval = util.getter(left_field)(elem)
        match = util.find_first(lambda d: util.getter(right_field)(d) == lval, right)
        if match:
            out.append({'left': elem, 'right': match})
    return out
