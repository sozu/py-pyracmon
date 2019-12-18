import psycopg2
import pytest
from pyracmon.connection import connect
from pyracmon.mixin import CRUDMixin
from pyracmon.model import define_model
from pyracmon.dialect.postgresql import *


def _connect():
    return connect(
        psycopg2,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )

class TestReadSchame:
    def test_read(self):
        with _connect() as db:
            tables = sorted(read_schema(db), key = lambda t: t.name)

            assert len(tables) == 4

            _assert_scheme(tables[0], "t1", [
                dict(name = "c11", type = int, pk = True, fk = False, incremental = "t1_c11_seq"),
                dict(name = "c12", type = int, pk = False, fk = False, incremental = None),
                dict(name = "c13", type = str, pk = False, fk = False, incremental = None),
            ])
            _assert_scheme(tables[1], "t2", [
                dict(name = "c21", type = int, pk = True, fk = False, incremental = None),
                dict(name = "c22", type = int, pk = True, fk = False, incremental = None),
                dict(name = "c23", type = str, pk = False, fk = False, incremental = None),
            ])
            _assert_scheme(tables[2], "t3", [
                dict(name = "c31", type = int, pk = True, fk = True, incremental = None),
                dict(name = "c32", type = int, pk = False, fk = False, incremental = "t3_c32_seq"),
                dict(name = "c33", type = str, pk = False, fk = False, incremental = None),
            ])
            _assert_scheme(tables[3], "t4", [
                dict(name = "c41", type = int, pk = True, fk = True, incremental = None),
                dict(name = "c42", type = int, pk = True, fk = True, incremental = None),
                dict(name = "c43", type = int, pk = True, fk = True, incremental = None),
            ])


def _assert_scheme(actual, t, cs):
    assert t == actual.name
    assert len(cs) == len(actual.columns)

    cs = sorted(cs, key = lambda c: c['name'])
    acs = sorted(actual.columns, key = lambda c: c.name)

    for c, a in zip(cs, acs):
        assert c['name'] == a.name
        assert c['type'] == a.ptype
        if 'info' in c:
            assert c['info'] == a.type_info
        assert c['pk'] == a.pk
        assert c['fk'] == a.fk
        assert c['incremental'] == a.incremental


class TestLastSequences:
    def test_last_sequences(self):
        with _connect() as db:
            tables = read_schema(db, includes=["t1"])
            m = define_model(tables[0], mixins=[PostgreSQLMixin, CRUDMixin])
            c = db.cursor()
            c.execute("TRUNCATE t1 RESTART IDENTITY CASCADE")
            c.execute(
                "INSERT INTO t1 (c12, c13) VALUES (%s, %s), (%s, %s), (%s, %s)",
                [1, "abc", 2, "def", 3, "ghi"]
            )
            assert [(m.columns[0], 3)] == m.last_sequences(db, 3)

