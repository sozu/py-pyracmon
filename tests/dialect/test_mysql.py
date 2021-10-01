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


class TestReadSchema:
    def test_read(self):
        db = _connect()

        tables = read_schema(db)

        assert len(tables) == 5

        table_map = {t.name:t for t in tables}

        _assert_schema(table_map["t1"], "t1", "comment of t1", [
            dict(name="c11", type=int, pk=True, fk=None, incremental=True, nullable=False, comment="comment of c11"),
            dict(name="c12", type=int, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c12"),
            dict(name="c13", type=str, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c13"),
        ])
        _assert_schema(table_map["t2"], "t2", "", [
            dict(name="c21", type=int, pk=True, fk=None, incremental=None, nullable=False, comment=""),
            dict(name="c22", type=int, pk=True, fk=None, incremental=None, nullable=False, comment=""),
            dict(name="c23", type=str, pk=False, fk=None, incremental=None, nullable=False, comment=""),
        ])
        _assert_schema(table_map["t3"], "t3", "", [
            dict(name="c31", type=int, pk=True, fk=[(table_map["t1"], "c11")], incremental=None, nullable=False, comment=""),
            dict(name="c32", type=int, pk=False, fk=None, incremental=None, nullable=False, comment=""),
            dict(name="c33", type=str, pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])
        _assert_schema(table_map["t4"], "t4", "", [
            dict(name="c41", type=int, pk=True, fk=[(table_map["t1"], "c11")], incremental=None, nullable=False, comment=""),
            dict(name="c42", type=int, pk=True, fk=[(table_map["t2"], "c21")], incremental=None, nullable=False, comment=""),
            dict(name="c43", type=int, pk=True, fk=[(table_map["t2"], "c22")], incremental=None, nullable=False, comment=""),
        ])
        _assert_schema(table_map["v1"], "v1", "", [
            dict(name="c11", type=int, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c11"),
            dict(name="c12", type=int, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c12"),
            dict(name="c31", type=int, pk=False, fk=None, incremental=None, nullable=False, comment=""),
            dict(name="c32", type=int, pk=False, fk=None, incremental=None, nullable=False, comment=""),
        ])

    def test_excludes(self):
        db = _connect()

        tables = sorted(read_schema(db, excludes=["t1", "t3"]), key = lambda t: t.name)

        assert len(tables) == 3
        assert {t.name for t in tables} == {"t2", "t4", "v1"}

    def test_includes(self):
        db = _connect()

        tables = sorted(read_schema(db, includes=["t2", "t4"]), key = lambda t: t.name)

        assert len(tables) == 2
        assert {t.name for t in tables} == {"t2", "t4"}

    def test_type_mapping(self):
        class C:
            pass

        db = _connect()
        db.context.configure(type_mapping = lambda t: C if t in {"int"} else None)

        tables = read_schema(db)

        table_map = {t.name:t for t in tables}

        _assert_schema(table_map["t1"], "t1", "comment of t1", [
            dict(name="c11", type=C, pk=True, fk=None, incremental=True, nullable=False, comment="comment of c11"),
            dict(name="c12", type=C, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c12"),
            dict(name="c13", type=str, pk=False, fk=None, incremental=None, nullable=False, comment="comment of c13"),
        ])


def _assert_schema(actual, t, tcm, cs):
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
        if c['fk']:
            assert len(a.fk.constraints) == len(c['fk'])
            for act, exp in zip(a.fk.constraints, c['fk']):
                tt, ct = exp[0], exp[0].find(exp[1])
                assert (act.table, act.column) == (tt, ct)
        else:
            assert None is a.fk
        assert c['incremental'] == a.incremental
        assert c['nullable'] is a.nullable
        assert c['comment'] == a.comment


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