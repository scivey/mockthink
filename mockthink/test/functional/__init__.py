import argparse
import sys

from mockthink.test.functional import test_array_manip, test_between, test_grouping
from mockthink.test.functional import test_grouping, test_indexes, test_joins
from mockthink.test.functional import test_math, test_merge, test_misc, test_order_by
from mockthink.test.functional import test_pluck, test_strings, test_time, test_typeof_coerce
from mockthink.test.functional import test_update_insert_delete, test_without
from mockthink.test.functional import test_aggregation, test_logic, test_table_db_mod


from mockthink.test.functional.common import run_tests_with_rethink, run_tests_with_mockthink

if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    runners = {
        'mockthink': run_tests_with_mockthink,
        'rethink': run_tests_with_rethink
    }

    parser.add_argument('--run', default='mockthink')
    parser.add_argument('--grep', default=None)
    args = parser.parse_args(sys.argv[1:])
    runners[args.run](args.grep)
