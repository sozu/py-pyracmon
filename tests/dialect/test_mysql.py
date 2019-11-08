import pymysql
import pytest
from pyracmon.connection import connect
from pyracmon.mixin import CRUDMixin
from pyracmon.model import define_model
from pyracmon.dialect.mysql import *


def _connect():
    return connect(
        pymysql,
        db = "pyracmon_test",
        user = "root",
        password = "root",
        host = "mysql",
        port = 3306,
    )

@pytest.mark.filterwarnings("ignore")
class TestReadSchame:
    def test_read(self):
        with _connect() as db:
            tables = sorted(read_schema(db), key = lambda t: t.name)

            assert len(tables) == 4

            _assert_scheme(tables[0], "t1", [
                dict(name = "c11", pk = True, incremental = True),
                dict(name = "c12", pk = False, incremental = None),
                dict(name = "c13", pk = False, incremental = None),
            ])
            _assert_scheme(tables[1], "t2", [
                dict(name = "c21", pk = True, incremental = None),
                dict(name = "c22", pk = True, incremental = None),
                dict(name = "c23", pk = False, incremental = None),
            ])
            _assert_scheme(tables[2], "t3", [
                dict(name = "c31", pk = True, incremental = None),
                dict(name = "c32", pk = False, incremental = None),
                dict(name = "c33", pk = False, incremental = None),
            ])
            _assert_scheme(tables[3], "t4", [
                dict(name = "c41", pk = True, incremental = None),
                dict(name = "c42", pk = True, incremental = None),
                dict(name = "c43", pk = True, incremental = None),
            ])


def _assert_scheme(actual, t, cs):
    assert t == actual.name
    assert len(cs) == len(actual.columns)

    cs = sorted(cs, key = lambda c: c['name'])
    acs = sorted(actual.columns, key = lambda c: c.name)

    for c, a in zip(cs, acs):
        assert c['name'] == a.name
        assert c['pk'] == a.pk
        assert c['incremental'] == a.incremental


@pytest.mark.filterwarnings("ignore")
class TestLastSequences:
    def test_last_sequences(self):
        with _connect() as db:
            tables = read_schema(db, includes=["t1"])
            m = define_model(tables[0], mixins=[MySQLMixin, CRUDMixin])
            c = db.cursor()
            c.execute("DELETE FROM t1")
            c.execute("ALTER TABLE t1 AUTO_INCREMENT = 1")
            c.execute(
                "INSERT INTO t1 (c12, c13) VALUES (%s, %s), (%s, %s), (%s, %s)",
                [1, "abc", 2, "def", 3, "ghi"]
            )
            assert [(m.columns[0], 3)] == m.last_sequences(db, 3)


@pytest.mark.filterwarnings("ignore")
class TestFunctions:
    def test_found_rows(self):
        with _connect() as db:
            c = db.cursor()
            c.execute("DELETE FROM t1")
            c.execute(
                "INSERT INTO t1 (c12, c13) VALUES (%s, %s), (%s, %s), (%s, %s)",
                [1, "abc", 2, "def", 3, "ghi"]
            )
            c.execute("SELECT SQL_CALC_FOUND_ROWS * FROM t1")
            assert found_rows(db) == 3