import mock
import unittest
from pprint import pprint
from ... import db

class TestDbRowInsert(unittest.TestCase):
    def test_insert_update_one_row(self):
        existing = {'id': 'x', 'name': 'john', 'age': 26}
        expected_result = {'id': 'x', 'name': 'john', 'nickname': 'johnny', 'age': 30}
        expected_changes = {'nickname': 'johnny', 'age': 30}
        result, changes = db.insert_update_one_row(existing, {'id': 'x', 'nickname': 'johnny', 'age': 30})
        self.assertEqual(expected_result, result)
        self.assertEqual(expected_changes, changes)

    def test_insert_replace_one_row(self):
        existing = {'id': 'x', 'name': 'john', 'age': 26}
        expected_result = {'id': 'x', 'nickname': 'johnny', 'age': 30}
        expected_changes = {'nickname': 'johnny', 'age': 30}
        result, changes = db.insert_replace_one_row(existing, {'id': 'x', 'nickname': 'johnny', 'age': 30})
        self.assertEqual(expected_result, result)
        self.assertEqual(expected_changes, changes)
