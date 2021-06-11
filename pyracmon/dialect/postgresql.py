import re
from decimal import Decimal
from datetime import date, datetime, time, timedelta
from uuid import UUID
from itertools import groupby
from pyracmon.model import Table, Column
from pyracmon.dialect.shared import MultiInsertMixin
from pyracmon.query import Q, where, holders


SequencePattern = re.compile(r"nextval\(\'([a-zA-Z0-9_]+)\'(\:\:regclass)?\)")


def read_schema(db, excludes=None, includes=None):
    """
    Collect tables in current database.

    Parameters
    ----------
    excludes: [str]
        Excluding table names.
    includes: [str]
        Including table names. If not specified, all tables are collected.

    Returns
    -------
    [Table]
        Tables.
    """
    q = Q(excludes = excludes, includes = includes)

    cond = Q.of("c.table_catalog = current_catalog") & Q.eq("c", table_schema="public") & Q.in_("t", table_type=["BASE TABLE", "VIEW"]) \
        & q.excludes.not_in("c.table_name") & q.includes.in_("c.table_name")

    w, params = where(cond)

    cursor = db.stmt().execute(f"""\
        SELECT
            c.table_name, c.column_name, c.data_type, c.udt_name, c.is_nullable,
            e.data_type, e.udt_name, k.constraint_type, c.column_default, c.ordinal_position
        FROM
            information_schema.columns AS c
            INNER JOIN information_schema.tables AS t ON c.table_name = t.table_name
            LEFT JOIN (
                SELECT
                    tc.table_name, k.column_name, string_agg(tc.constraint_type, ',') AS constraint_type
                FROM
                    information_schema.key_column_usage AS k
                    INNER JOIN information_schema.table_constraints AS tc ON k.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'PRIMARY KEY' OR tc.constraint_type = 'FOREIGN KEY'
                GROUP BY
                    tc.table_name, k.column_name
            ) AS k ON t.table_name = k.table_name AND c.column_name = k.column_name
            LEFT JOIN information_schema.element_types AS e
                ON ((c.table_catalog, c.table_schema, c.table_name, 'TABLE', c.dtd_identifier) =
                    (e.object_catalog, e.object_schema, e.object_name, e.object_type, e.collection_type_identifier))
        {w}
        ORDER BY c.table_name ASC, c.ordinal_position ASC
        """, *params)

    def map_types(t, udt):
        base = db.context.config.type_mapping
        ptype = base and base(t, udt_name=udt)
        return ptype or _map_types(t)

    def column_of(n, t, udt, nullable, et, eudt, constraint, default, pos):
        m = SequencePattern.match(default or "")
        cs = (constraint or "").split(',')
        seq = m.group(1) if m else None
        null = nullable == 'YES'
        ptype = map_types(t, udt) if t != 'ARRAY' else [map_types(et, eudt)]
        info = (t, udt) if t != 'ARRAY' else (et, eudt)
        return Column(n, ptype, info, 'PRIMARY KEY' in cs, 'FOREIGN KEY' in cs, seq, null)

    tables = []
    column_positions = {}

    for t, cols in groupby(cursor.fetchall(), lambda row: row[0]):
        cols = list(cols)
        columns = [column_of(*c[1:]) for c in cols]
        tables.append(Table(t, columns))
        column_positions[t] = {c[1]:c[-1] for c in cols}

    cursor.close()

    # Materialized views
    cond = Q.eq("c", relkind = "m") & Q.ge("a", attnum = 1) \
        & q.excludes.not_in("c.relname") & q.includes.in_("c.relname")

    w, params = where(cond)

    cursor = db.stmt().execute(f"""\
        SELECT
            c.relname, a.attname, a.attnotnull, t.typname, et.typname, a.attnum
        FROM
            pg_class AS c
            INNER JOIN pg_attribute AS a ON c.oid = a.attrelid
            INNER JOIN pg_type AS t ON a.atttypid = t.oid
            LEFT JOIN pg_type AS et ON t.typelem = et.oid
        {w}
        ORDER BY
            c.oid ASC, a.attnum ASC
        """, *params)

    def mv_column_of(n, not_null, udt, eudt, pos):
        ptype = map_types(_map_alternates(udt), udt) if eudt is None else [map_types(_map_alternates(eudt), eudt)]
        info = (_map_alternates(udt), udt) if eudt is None else (_map_alternates(eudt), eudt)
        return Column(n, ptype, info, False, False, None, not not_null)

    for t, cols in groupby(cursor.fetchall(), lambda row: row[0]):
        cols = list(cols)
        columns = [mv_column_of(*c[1:]) for c in cols]
        tables.append(Table(t, columns))
        column_positions[t] = {c[1]:c[-1] for c in cols}

    cursor.close()

    if len(tables) == 0:
        return tables

    cursor = db.stmt().execute(f"""\
        SELECT
            relname, oid
        FROM
            pg_class
        WHERE
            relname IN ({holders(len(tables))})
        """, *[t.name for t in tables])

    table_oids = {}
    for n, oid in cursor.fetchall():
        table_oids[n] = oid

    for t in tables:
        cc = db.stmt().execute(f"SELECT col_description($_, 0)", *[table_oids[t.name]])
        t.comment = cc.fetchone()[0] or ""

        for i, col in enumerate(t.columns):
            m = db.helper.marker()
            cc = db.stmt().execute(f"SELECT col_description($_, $_)", *[table_oids[t.name], column_positions[t.name][col.name]])
            col.comment = cc.fetchone()[0] or ""

        cc.close()

    cursor.close()

    return tables


def _map_types(t):
    if t == "boolean":
        return bool
    elif t == "real" or t == "double precision":
        return float
    elif t == "smallint" or t == "integer" or t == "bigint":
        return int
    elif t == "numeric" or t == "decimal":
        return Decimal
    elif t == "character varying" or t == "text" or t == "character":
        return str
    elif t == "bytea":
        return bytes
    elif t == "date":
        return date
    elif t.startswith("timestamp "):
        return datetime
    elif t == "time" or t.startswith("time "):
        return time
    elif t == "interval":
        return timedelta
    elif t == "uuid":
        return UUID
    else:
        return object


def _map_alternates(n):
    if n == "int2":
        return "smallint"
    elif n == "int" or n == "int4":
        return "integer"
    elif n == "int8":
        return "bigint"
    elif n == "float4":
        return "real"
    elif n == "float8":
        return "double precision"
    elif n == "decimal":
        return "numeric"
    elif n == "bool":
        return "boolean"
    elif n == "char":
        return "character"
    elif n == "varchar":
        return "character varying"
    elif n == "timetz":
        return "time with time zone"
    elif n == "timestamptz":
        return "timestamp with time zone"
    else:
        return n


class PostgreSQLMixin(MultiInsertMixin):
    @classmethod
    def last_sequences(cls, db, num):
        cols = [c for c in cls.columns if c.incremental]

        if len(cols) > 0:
            sequences = []
            d = db.cursor()
            for c in cols:
                d.execute(f"SELECT currval('{c.incremental}')")
                sequences.append((c, d.fetchone()[0]))
            d.close()
            return sequences
        else:
            return []

    @classmethod
    def truncate(cls, db):
        db.cursor().execute(f"TRUNCATE {cls.name} RESTART IDENTITY CASCADE")


mixins = [PostgreSQLMixin]