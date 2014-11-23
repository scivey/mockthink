import mock
import unittest
from ... import joins

class TestJoins(unittest.TestCase):
    def test_eq_join(self):
        left = [
            {
                'id': 'left-1',
                'rel_field': 'right-1-name'
            },
            {
                'id': 'left-2',
                'rel_field': 'right-4-name'
            }
        ]
        right = [
            {
                'id': 'right-1',
                'name': 'right-1-name'
            },
            {
                'id': 'right-2',
                'name': 'right-2-name'
            },
            {
                'id': 'right-3',
                'name': 'right-3-name'
            },
            {
                'id': 'right-4',
                'name': 'right-4-name'
            }
        ]
        expected = [
            {
                'left': {
                    'id': 'left-1',
                    'rel_field': 'right-1-name'
                },
                'right': {
                    'id': 'right-1',
                    'name': 'right-1-name'
                }
            },
            {
                'left': {
                    'id': 'left-2',
                    'rel_field': 'right-4-name'
                },
                'right': {
                    'id': 'right-4',
                    'name': 'right-4-name'
                }
            }
        ]
        self.assertEqual(
            expected,
            joins.do_eq_join('rel_field', left, 'name', right)
        )

    def test_inner_join(self):
        left = range(1, 5)
        right = range(1, 5)
        expected = [
            {'left': 2, 'right': 1},
            {'left': 3, 'right': 1},
            {'left': 3, 'right': 2},
            {'left': 4, 'right': 1},
            {'left': 4, 'right': 2},
            {'left': 4, 'right': 3}
        ]
        pred = lambda x, y: x > y
        self.assertEqual(
            expected,
            joins.do_inner_join(pred, left, right)
        )

    def test_outer_join(self):
        left = range(1, 6)
        right = range(1, 6)
        expected = [
            {'left': 1},
            {'left': 2},
            {'left': 3, 'right': 1},
            {'left': 3, 'right': 2},
            {'left': 4},
            {'left': 5, 'right': 1},
            {'left': 5, 'right': 2},
            {'left': 5, 'right': 3},
            {'left': 5, 'right': 4}
        ]
        def pred(x, y):
            return (x % 2 == 1) and (x > y)
        self.assertEqual(
            expected,
            joins.do_outer_join(pred, left, right)
        )
