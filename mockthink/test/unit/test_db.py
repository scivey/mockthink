import mock
import unittest
from pprint import pprint
from ..common import TestCase
from ... import db, util

class TestDbRowInsert(TestCase):
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

def db_insert_starting_data():
    return [
        {'id': 'a', 'name': 'a-name', 'age': 'a-age'},
        {'id': 'b', 'name': 'b-name', 'age': 'b-age'},
        {'id': 'c', 'name': 'c-name', 'age': 'c-age'}
    ]


class TestDbInsertWithConflictSettings(TestCase):
    def test_error(self):
        expected_result = [
            {'id': 'a', 'name': 'a-name', 'age': 'a-age'},
            {'id': 'b', 'name': 'b-name', 'age': 'b-age'},
            {'id': 'c', 'name': 'c-name', 'age': 'c-age'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        expected_report = {
            'replaced': 0,
            'updated': 0,
            'inserted': 1,
            'errors': 1,
            'changes': [{'id': 'd', 'name': 'deshawn'}]
        }
        to_insert = [
            {'id': 'c', 'something': 'someval'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        result, report = db.insert_into_table_with_conflict_setting(
            db_insert_starting_data(),
            to_insert,
            conflict='error'
        )
        self.assertEqual(expected_result, result)
        keys = ('replaced', 'updated', 'inserted', 'errors', 'changes')
        self.assert_key_equality(keys, expected_report, report)


