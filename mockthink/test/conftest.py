# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import pytest
import rethinkdb

from mockthink import MockThink
from mockthink.test.common import as_db_and_table, load_stock_data


def pytest_addoption(parser):
    group = parser.getgroup("mockthink", "Mockthink Testing")
    group._addoption("--run", dest="conn_type", default="mockthink", action="store",
                     choices=["mockthink", "rethink"],
                     help="Select whether tests are run on a mockthink connection or rethink connection or both")


@pytest.fixture(scope="function")
def conn(request):
    cfg = request.config
    conn_type = cfg.getvalue("conn_type")
    if conn_type == "rethink":
        try:
            conn = rethinkdb.connect('localhost', 30000)  # TODO add config
        except rethinkdb.errors.ReqlDriverError:
            pytest.exit("Unable to connect to rethink")
    elif conn_type == "mockthink":
        conn = MockThink(as_db_and_table('nothing', 'nothing', [])).get_conn()
    else:
        pytest.exit("Unknown mockthink test connection type: " + conn_type)
    data = request.instance.get_data()
    load_stock_data(data, conn)
    # request.cls.addCleanup(load_stock_data, data, conn)
    return conn
