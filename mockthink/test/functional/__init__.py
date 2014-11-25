import argparse
import sys
import time
import datetime
from pprint import pprint
import rethinkdb as r
from rethinkdb import RqlRuntimeError, RqlDriverError, RqlCompileError
from rethinkdb.ast import RqlTzinfo
from mockthink.db import MockThink, MockThinkConn
from mockthink.test.common import make_test_registry, AssertionMixin
from mockthink.test.common import as_db_and_table
from mockthink import util, rtime

from mockthink.test.functional import tests
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
