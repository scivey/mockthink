import unittest

from mockthink.test.common import assertEqual
from ... import ast_base


class TestAst(unittest.TestCase):
    def test_rql_merge_with(self):
        to_ext = {
            'x': {
                'x1': {
                    'val': 'x1-val'
                },
                'x2': {
                    'val': 'x2-val'
                }
            },
            'y': {
                'ykey': True
            },
            'nums1': [1, 3, 5],
            'nums2': [1, 3, 5]
        }
        ext_with = {
            'x': {
                'x1': {
                    'val2': 'x1-val-2'
                },
                'x2': ast_base.LITERAL_OBJECT.from_dict({
                    'val2': 'x2-val-2'
                })
            },
            'y': 'new-y-val',
            'nums1': [7, 9],
            'nums2': ast_base.LITERAL_LIST.from_list([7, 9])
        }
        expected = {
            'x': {
                'x1': {
                    'val': 'x1-val',
                    'val2': 'x1-val-2'
                },
                'x2': {
                    'val2': 'x2-val-2'
                },
            },
            'y': 'new-y-val',
            'nums1': [1, 3, 5, 7, 9],
            'nums2': [7, 9]
        }
        assertEqual(expected, ast_base.rql_merge_with(ext_with, to_ext))

    def test_contains_literals_obj(self):
        thing = {
            'x': {
                'val1': ast_base.LITERAL_OBJECT({'is_val_1': True})
            }
        }
        self.assertTrue(ast_base.contains_literals(thing))
        self.assertTrue(ast_base.contains_literals(thing['x']))

    def test_contains_literals_list(self):
        thing = {
            'x': {
                'val1': ast_base.LITERAL_LIST([1, 2, 3])
            }
        }
        self.assertTrue(ast_base.contains_literals(thing))
        self.assertTrue(ast_base.contains_literals(thing['x']))

    def test_has_nested_literal_has_nested_obj(self):
        thing = ast_base.LITERAL_OBJECT({
            'x': {
                'val1': ast_base.LITERAL_OBJECT({'is_val_1': True})
            }
        })
        self.assertTrue(ast_base.has_nested_literal(thing))

    def test_has_nested_literal_has_nested_obj_2(self):
        thing = ast_base.LITERAL_LIST([{
            'x': {
                'val1': ast_base.LITERAL_OBJECT({'is_val_1': True})
            }
        }])
        self.assertTrue(ast_base.has_nested_literal(thing))

    def test_has_nested_literal_happy(self):
        thing = ast_base.LITERAL_OBJECT({
            'x': {
                'val1': {'is_val_1': True}
            }
        })
        self.assertFalse(ast_base.has_nested_literal(thing))

    def test_has_nested_literal_has_nested_list(self):
        thing = ast_base.LITERAL_OBJECT({
            'x': {
                'val1': ast_base.LITERAL_LIST([1, 2, 3])
            }
        })
        self.assertTrue(ast_base.has_nested_literal(thing))

    def test_has_nested_literal_has_nested_list_2(self):
        thing = ast_base.LITERAL_LIST([
            {
                'x': {
                    'val1': ast_base.LITERAL_LIST([1, 2, 3])
                }
            }
        ])
        self.assertTrue(ast_base.has_nested_literal(thing))
