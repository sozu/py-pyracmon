import re
from decimal import Decimal
from datetime import date, datetime, time, timedelta
from uuid import UUID
from itertools import groupby
from pyracmon.model import Table, Column
from pyracmon.dialect.shared import MultiInsertMixin

SequencePattern = re.compile(r"nextval\(\'([a-zA-Z0-9_]+)\'(\:\:regclass)?\)")

def read_schema(db, excludes = [], includes = []):
    c = db.cursor()

    ex_cond = "" if len(excludes) == 0 else f"AND c.table_name NOT IN ({db.helper.holders(len(excludes))})"
    in_cond = "" if len(includes) == 0 else f"AND c.table_name IN ({db.helper.holders(len(includes), start = len(excludes))})"

    c.execute(f"""\
        SELECT
            c.table_name, c.column_name, c.data_type, c.udt_name, e.data_type, e.udt_name, k.constraint_type, c.column_default
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
        WHERE
            c.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
            {ex_cond}
            {in_cond}
        ORDER BY c.table_name ASC, c.ordinal_position ASC
        """, excludes + includes)

    def column_of(n, t, udt, et, eudt, constraint, default):
        m = SequencePattern.match(default or "")
        cs = (constraint or "").split(',')
        seq = m.group(1) if m else None
        ptype = _map_types(t) if t != 'ARRAY' else [_map_types(et)]
        info = (t, udt) if t != 'ARRAY' else (et, eudt)
        return Column(n, ptype, info, 'PRIMARY KEY' in cs, 'FOREIGN KEY' in cs, seq)

    tables = []

    for t, cols in groupby(c.fetchall(), lambda row: row[0]):
        columns = [column_of(*c[1:]) for c in cols]
        tables.append(Table(t, columns))

    if len(tables) == 0:
        return tables

    c.execute(f"""\
        SELECT
            relname, oid
        FROM
            pg_class
        WHERE
            relname IN ({db.helper.holders(len(tables))})
        """, [t.name for t in tables])

    table_oids = {}
    for n, oid in c.fetchall():
        table_oids[n] = oid

    for t in tables:
        c.execute(f"SELECT col_description({db.helper.marker()()}, 0)", [table_oids[t.name]])
        t.comment = c.fetchone()[0] or ""

        for i, col in enumerate(t.columns):
            m = db.helper.marker()
            c.execute(f"SELECT col_description({m()}, {m()})", [table_oids[t.name], i+1])
            col.comment = c.fetchone()[0] or ""

    c.close()

    return tables


def _map_types(t):
    # TODO Actually, this mapping depends on connection module.
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
    elif t.startswith("time "):
        return time
    elif t == "interval":
        return timedelta
    elif t == "uuid":
        return UUID
    else:
        return object


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


mixins = [PostgreSQLMixin]