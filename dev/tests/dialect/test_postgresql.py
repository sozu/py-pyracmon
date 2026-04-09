import pytest
import logging
import psycopg2
from datetime import date, datetime, time, timedelta
from typing import Any
from uuid import UUID
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


class TestReadSchema:
    def test_read(self):
        db = _connect()

        tables = read_schema(db)

        assert len(tables) == 8

        table_map = {t.name:t for t in tables}

        _assert_schema(table_map["t1"], "t1", "comment of t1", [
            dict(name="c11", type=int, pk=True, fk=None, incremental="t1_c11_seq", nullable=False, comment="comment of c11"),
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
            dict(name="c32", type=int, pk=False, fk=None, incremental="t3_c32_seq", nullable=False, comment=""),
            dict(name="c33", type=str, pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])
        _assert_schema(table_map["t4"], "t4", "", [
            dict(name="c41", type=int, pk=True, fk=[(table_map["t1"], "c11")], incremental=None, nullable=False, comment=""),
            dict(name="c42", type=int, pk=True, fk=[(table_map["t2"], "c21")], incremental=None, nullable=False, comment=""),
            dict(name="c43", type=int, pk=True, fk=[(table_map["t2"], "c22")], incremental=None, nullable=False, comment=""),
        ])
        _assert_schema(table_map["types"], "types", "", [
            dict(name="bool_", type=bool, udt="bool", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="double_", type=float, udt="float4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="int_", type=int, udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="string_", type=str, udt="text", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="bytes_", type=bytes, udt="bytea", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="date_", type=date, udt="date", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="datetime_", type=datetime, udt="timestamptz", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="time_", type=time, udt="time", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="delta_", type=timedelta, udt="interval", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="uuid_", type=UUID, udt="uuid", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="enum_", type=object, udt="t_enum", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="record_", type=object, udt="t_record", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="array_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="deeparray_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="json_", type=dict, udt="json", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="jsonb_", type=dict, udt="jsonb", pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])
        _assert_schema(table_map["v1"], "v1", "comment of v1", [
            dict(name="c11", type=int, pk=False, fk=None, incremental=None, nullable=True, comment="comment of c11 in v1"),
            dict(name="c12", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="c31", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="c32", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])
        _assert_schema(table_map["mv1"], "mv1", "comment of mv1", [
            dict(name="c11", type=int, pk=False, fk=None, incremental=None, nullable=True, comment="comment of c11 in mv1"),
            dict(name="c12", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="c31", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="c32", type=int, pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])
        _assert_schema(table_map["mv2"], "mv2", "", [
            dict(name="bool_", type=bool, udt="bool", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="double_", type=float, udt="float4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="int_", type=int, udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="string_", type=str, udt="text", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="bytes_", type=bytes, udt="bytea", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="date_", type=date, udt="date", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="datetime_", type=datetime, udt="timestamptz", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="time_", type=time, udt="time", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="delta_", type=timedelta, udt="interval", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="uuid_", type=UUID, udt="uuid", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="enum_", type=object, udt="t_enum", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="record_", type=object, udt="t_record", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="array_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="deeparray_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="json_", type=dict, udt="json", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="jsonb_", type=dict, udt="jsonb", pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])

    def test_excludes(self):
        db = _connect()

        tables = sorted(read_schema(db, excludes=["t1", "t3"]), key = lambda t: t.name)

        assert len(tables) == 6
        assert {t.name for t in tables} == {"t2", "t4", "types", "v1", "mv1", "mv2"}

    def test_includes(self):
        db = _connect()

        tables = sorted(read_schema(db, includes=["t2", "t4"]), key = lambda t: t.name)

        assert len(tables) == 2
        assert {t.name for t in tables} == {"t2", "t4"}

    def test_type_mapping(self):
        class C:
            pass

        def map_types(t, udt_name, **kwargs):
            if t in {"boolean", "date"}:
                return C
            elif udt_name == "t_enum":
                return C
            else:
                return None

        db = _connect()
        db.context.configure(type_mapping = map_types)

        tables = sorted(read_schema(db), key = lambda t: t.name)

        table_map = {t.name:t for t in tables}

        _assert_schema(table_map["types"], "types", "", [
            dict(name="bool_", type=C, udt="bool", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="double_", type=float, udt="float4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="int_", type=int, udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="string_", type=str, udt="text", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="bytes_", type=bytes, udt="bytea", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="date_", type=C, udt="date", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="datetime_", type=datetime, udt="timestamptz", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="time_", type=time, udt="time", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="delta_", type=timedelta, udt="interval", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="uuid_", type=UUID, udt="uuid", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="enum_", type=C, udt="t_enum", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="record_", type=object, udt="t_record", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="array_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="deeparray_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="json_", type=dict, udt="json", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="jsonb_", type=dict, udt="jsonb", pk=False, fk=None, incremental=None, nullable=True, comment=""),
        ])



def _assert_schema(actual, t, tcm, cs):
    assert t == actual.name
    assert tcm == actual.comment
    assert len(cs) == len(actual.columns)

    cs = sorted(cs, key = lambda c: c['name'])
    acs = sorted(actual.columns, key = lambda c: c.name)

    for c, a in zip(cs, acs):
        assert c['name'] == a.name
        assert c['type'] == a.ptype
        if 'udt' in c:
            assert c['udt'] == a.type_info[1]
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
        db = _connect()

        tables = read_schema(db, includes=["t1"])
        m = define_model(tables[0], mixins=[PostgreSQLMixin, CRUDMixin])

        c = db.cursor()
        c.execute("TRUNCATE t1 RESTART IDENTITY CASCADE")
        c.execute(
            "INSERT INTO t1 (c12, c13) VALUES (%s, %s), (%s, %s), (%s, %s)",
            [1, "abc", 2, "def", 3, "ghi"]
        )

        assert [(m.columns[0], 3)] == m.last_sequences(db, 3)