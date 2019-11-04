import sys
import psycopg2
import pytest
from tests import models as m
from pyracmon import declare_models
from pyracmon.connection import connect
from pyracmon.dialect import postgresql


def test_declare_models_postgresql():
    db = connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )

    declare_models(postgresql, db, 'tests.models')
    try:
        assert hasattr(m, "t1")
        assert hasattr(m, "t2")
        assert hasattr(m, "t3")
        assert hasattr(m, "t4")
    finally:
        del sys.modules['tests.models'].__dict__["t1"]
        del sys.modules['tests.models'].__dict__["t2"]
        del sys.modules['tests.models'].__dict__["t3"]
        del sys.modules['tests.models'].__dict__["t4"]
