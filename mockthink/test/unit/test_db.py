import mock
import unittest
from pprint import pprint
from ..common import TestCase
from ... import db, util

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
            'inserted': 1,
            'errors': 1,
            'changes': [{
                'old_val': None,
                'new_val': {
                    'id': 'd',
                    'name': 'deshawn'
                }
            }]
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
        keys = ('replaced', 'inserted', 'errors', 'changes')
        self.assert_key_equality(keys, expected_report, report)

    def test_update(self):
        expected_result = [
            {'id': 'a', 'name': 'a-name', 'age': 'a-age'},
            {'id': 'b', 'name': 'b-name', 'age': 'b-age'},
            {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val', 'age': 'c-age'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        expected_report = {
            'replaced': 1,
            'inserted': 1,
            'errors': 0,
            'changes': [
                {
                    'old_val': {'id': 'c', 'name': 'c-name', 'age': 'c-age'},
                    'new_val': {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val', 'age': 'c-age'}
                },
                {
                    'old_val': None,
                    'new_val': {'id': 'd', 'name': 'deshawn'}
                }
            ]
        }
        to_insert = [
            {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        result, report = db.insert_into_table_with_conflict_setting(
            db_insert_starting_data(),
            to_insert,
            conflict='update'
        )
        self.assertEqual(expected_result, result)
        keys = ('replaced', 'inserted', 'errors', 'changes')
        self.assert_key_equality(keys, expected_report, report)

    def test_replace(self):
        expected_result = [
            {'id': 'a', 'name': 'a-name', 'age': 'a-age'},
            {'id': 'b', 'name': 'b-name', 'age': 'b-age'},
            {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        expected_report = {
            'replaced': 1,
            'inserted': 1,
            'errors': 0,
            'changes': [
                {
                    'old_val': {'id': 'c', 'name': 'c-name', 'age': 'c-age'},
                    'new_val': {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val'}
                },
                {
                    'old_val': None,
                    'new_val': {'id': 'd', 'name': 'deshawn'}
                }
            ]
        }
        to_insert = [
            {'id': 'c', 'name': 'new c name', 'new_c_key': 'new_c_val'},
            {'id': 'd', 'name': 'deshawn'}
        ]
        result, report = db.insert_into_table_with_conflict_setting(
            db_insert_starting_data(),
            to_insert,
            conflict='replace'
        )
        self.assertEqual(expected_result, result)
        keys = ('replaced', 'inserted', 'errors', 'changes')
        self.assert_key_equality(keys, expected_report, report)

