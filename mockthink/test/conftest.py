# coding: utf-8

from __future__ import absolute_import, division, print_function, unicode_literals

import pytest
import rethinkdb
from pytest_server_fixtures.rethink import rethink_server, rethink_server_sess

from mockthink import MockThink
from mockthink.test.common import as_db_and_table, load_stock_data

import logging
logging.basicConfig()

def pytest_addoption(parser):
    group = parser.getgroup("mockthink", "Mockthink Testing")
    group._addoption("--run", dest="conn_type", default="mockthink", action="store",
                     choices=["mockthink", "rethink"],
                     help="Select whether tests are run on a mockthink connection or rethink connection or both")


@pytest.fixture(scope="session")
def conn_sess(request):
    cfg = request.config
    conn_type = cfg.getvalue("conn_type")
    if conn_type == "rethink":
        try:
            server = rethink_server_sess(request)
            conn = server.conn
        except rethinkdb.errors.ReqlDriverError:
            pytest.exit("Unable to connect to rethink")
        except OSError:
            pytest.exit("No rethinkdb binary found")
    elif conn_type == "mockthink":
        conn = MockThink(as_db_and_table('nothing', 'nothing', [])).get_conn()
    else:
        pytest.exit("Unknown mockthink test connection type: " + conn_type)
    return conn


@pytest.fixture(scope="function")
def conn(request, conn_sess):
    data = request.instance.get_data()
    load_stock_data(data, conn_sess)
    return conn_sess
