"""
A dialect module for MySQL.
"""
from itertools import groupby
from decimal import Decimal
from enum import Enum
from datetime import date, datetime, time, timedelta
from typing import Optional
from pyracmon.connection import Connection
from pyracmon.model import Table, Column, Relations, ForeignKey
from pyracmon.dialect.shared import MultiInsertMixin, TruncateMixin
from pyracmon.query import Q, where
from pyracmon.clause import holders


def read_schema(db, excludes: Optional[list[str]] = None, includes: Optional[list[str]] = None) -> list[Table]:
    """
    Collect tables in current database.

    Args:
        excludes: Excluding table names.
        includes: Including table names. If not specified, all tables are collected.
    Returns:
        Table schemas.
    """
    q = Q(excludes = excludes, includes = includes)

    cond = Q.of("c.table_schema = DATABASE()") & q.excludes.not_in("c.table_name") & q.includes.in_("c.table_name")

    w, params = where(cond)

    cursor = db.stmt().execute(f"""\
        SELECT
            c.table_name, c.column_name, c.data_type, c.is_nullable, c.column_type, c.column_key, c.extra, c.column_comment
        FROM
            information_schema.columns AS c
        {w}
        ORDER BY
            c.table_name, c.ordinal_position ASC
        """, *params)

    def map_types(t):
        base = db.context.config.type_mapping
        ptype = base and base(t)
        return ptype or _map_types(t)

    def column_of(n, t, nullable, ct, key, extra, comment):
        return Column(n, map_types(t), ct, key == "PRI", None, True if extra == "auto_increment" else None, nullable == "YES", comment or "")

    tables = []

    for t, cols in groupby(cursor.fetchall(), lambda row: row[0]):
        tables.append(Table(t, [column_of(*c[1:]) for c in cols]))

    cursor.close()

    if len(tables) == 0:
        return []

    cursor = db.stmt().execute(f"""\
        SELECT
            table_name, column_name, referenced_table_name, referenced_column_name
        FROM
            information_schema.key_column_usage
        WHERE
            table_schema = DATABASE() AND referenced_table_name IS NOT NULL
        """)

    table_map = {t.name:t for t in tables}

    for row in cursor.fetchall():
        table_from = table_map.get(row[0], None)
        col_from = table_from.find(row[1]) if table_from else None

        if col_from:
            table_to = table_map.get(row[2], None)
            col_to = table_to.find(row[3]) if table_to else None
            col_from.fk = col_from.fk or Relations()
            col_from.fk.add(ForeignKey(table_to or row[2], col_to or row[3]))

    cursor.close()

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


class MySQLMixin(MultiInsertMixin, TruncateMixin):
    """
    Model mixin whose methods are available in MySQL.
    """
    @classmethod
    def last_sequences(cls, db: Connection, num: int) -> list[tuple[Column, int]]:
        cols = [c for c in cls.columns if c.incremental]

        if len(cols) > 1:
            raise ValueError(f"MySQL allows tables having only an auto-increment column.")
        elif len(cols) == 1:
            d = db.cursor()
            d.execute(f"SELECT LAST_INSERT_ID()")
            sequence = d.fetchone()[0] + num - 1 # type: ignore
            d.close()
            return [(cols[0], sequence)]
        else:
            return []

    @classmethod
    def truncate(cls, db: Connection):
        db.cursor().execute(f"DELETE FROM {cls.name}")
        db.cursor().execute(f"ALTER TABLE {cls.name} auto_increment = 1")


mixins = [MySQLMixin]


def found_rows(db):
    with db.cursor() as c:
        c.execute("SELECT FOUND_ROWS()")
        return c.fetchone()[0]