import pytest
import psycopg
from datetime import date, datetime, time, timedelta
from typing import Any
from uuid import UUID
from pyracmon.connection import aconnect
from pyracmon.mixin_async import AsyncCRUDMixin
from pyracmon.model import define_model
from pyracmon.dialect.postgresql import *


async def _connect():
    return await aconnect(
        psycopg.AsyncConnection.connect,
        api = psycopg,
        dbname = "pyracmon_test",
        user = "postgres",
        password = "postgres",
        host = "postgres",
        port = 5432,
    )


class TestReadSchema:
    @pytest.mark.asyncio
    async def test_read(self):
        db = await _connect()

        tables = await read_schema_async(db)

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
            dict(name="enum_", type=Any, udt="t_enum", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="record_", type=Any, udt="t_record", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="array_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="deeparray_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="json_", type=Any, udt="json", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="jsonb_", type=Any, udt="jsonb", pk=False, fk=None, incremental=None, nullable=True, comment=""),
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
            dict(name="enum_", type=Any, udt="t_enum", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="record_", type=Any, udt="t_record", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="array_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="deeparray_", type=list[int], udt="int4", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="json_", type=Any, udt="json", pk=False, fk=None, incremental=None, nullable=True, comment=""),
            dict(name="jsonb_", type=Any, udt="jsonb", pk=False, fk=None, incremental=None, nullable=True, comment=""),
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
    @pytest.mark.asyncio
    async def test_last_sequences(self):
        db = await _connect()

        tables = await read_schema_async(db, includes=["t1"])
        m = define_model(tables[0], mixins=[AsyncPostgreSQLMixin, AsyncCRUDMixin], model_type=AsyncCRUDMixin)

        c = db.cursor()
        await c.execute("TRUNCATE t1 RESTART IDENTITY CASCADE")
        await c.execute(
            "INSERT INTO t1 (c12, c13) VALUES (%s, %s), (%s, %s), (%s, %s)",
            [1, "abc", 2, "def", 3, "ghi"]
        )

        assert [(m.columns[0], 3)] == await m.last_sequences(db, 3)