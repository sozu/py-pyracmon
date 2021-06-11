from itertools import groupby
from decimal import Decimal
from enum import Enum
from datetime import date, datetime, time, timedelta
from pyracmon.model import Table, Column
from pyracmon.dialect.shared import MultiInsertMixin
from pyracmon.query import Q, where, holders


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
    try:
        db.stmt().execute(f"DROP TEMPORARY TABLE IF EXISTS kcu")
        db.stmt().execute(f"""\
            CREATE TEMPORARY TABLE kcu
            SELECT * FROM information_schema.key_column_usage WHERE table_schema = DATABASE()
            """)

        q = Q(excludes = excludes, includes = includes)

        cond = Q.of("c.table_schema = DATABASE()") & q.excludes.not_in("c.table_name") & q.includes.in_("c.table_name")

        w, params = where(cond)

        cursor = db.stmt().execute(f"""\
            SELECT
                c.table_name, c.column_name, c.data_type, c.is_nullable, c.column_type, c.column_key,
                k.referenced_table_name, k.referenced_column_name, c.extra, c.column_comment
            FROM
                information_schema.columns AS c
                LEFT JOIN kcu AS k
                    ON c.table_catalog = k.table_catalog
                        AND c.table_schema = k.table_schema
                        AND c.table_name = k.table_name
                        AND c.column_name = k.column_name
                        AND k.referenced_table_name IS NOT NULL
            {w}
            ORDER BY
                c.table_name, c.ordinal_position ASC
            """, *params)

        def map_types(t):
            base = db.context.config.type_mapping
            ptype = base and base(t)
            return ptype or _map_types(t)

        def column_of(n, t, nullable, ct, key, rt, rc, extra, comment):
            return Column(n, map_types(t), ct, key == "PRI", bool(rt), True if extra == "auto_increment" else None, nullable == "YES", comment or "")

        tables = []

        for t, cols in groupby(cursor.fetchall(), lambda row: row[0]):
            tables.append(Table(t, [column_of(*c[1:]) for c in cols]))

        cursor.close()

        if len(tables) == 0:
            return []

        cursor = db.stmt().execute(f"""\
            SELECT
                table_name, table_comment
            FROM
                information_schema.tables
            WHERE
                table_name IN ({holders(len(tables))})
            """, *[t.name for t in tables])

        table_map = {t.name: t for t in tables}

        for n, cmt in cursor.fetchall():
            if n in table_map:
                table_map[n].comment = cmt or ""

        cursor.close()

        return tables
    finally:
        db.stmt().execute(f"DROP TEMPORARY TABLE IF EXISTS kcu")


def _map_types(t):
    if t == "tinyint" or t == "smallint" or t == "mediumint" or t == "int" or t == "bigint":
        return int
    elif t == "decimal":
        return Decimal
    elif t == "float" or t == "double":
        return float
    elif t == "bit":
        return int
    elif t == "char" or t == "varchar" or t == "binary" or t == "varbinary" or t == "text":
        return str
    elif t == "blob":
        return bytes
    elif t == "enum":
        return Enum
    elif t == "date":
        return date
    elif t == "datetime" or t == "timestamp":
        return datetime
    else:
        return object


class MySQLMixin(MultiInsertMixin):
    @classmethod
    def last_sequences(cls, db, num):
        cols = [c for c in cls.columns if c.incremental]

        if len(cols) > 1:
            raise ValueError(f"MySQL allows tables having only an auto-increment column.")
        elif len(cols) == 1:
            d = db.cursor()
            d.execute(f"SELECT LAST_INSERT_ID()")
            sequence = d.fetchone()[0] + num - 1
            d.close()
            return [(cols[0], sequence)]
        else:
            return []


mixins = [MySQLMixin]


def found_rows(db):
    with db.cursor() as c:
        c.execute("SELECT FOUND_ROWS()")
        return c.fetchone()[0]