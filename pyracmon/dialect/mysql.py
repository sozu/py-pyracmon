from itertools import groupby
from decimal import Decimal
from enum import Enum
from datetime import date, datetime, time, timedelta
from pyracmon.model import Table, Column
from pyracmon.dialect.shared import MultiInsertMixin


def read_schema(db, excludes = [], includes = []):
    c = db.cursor()

    ex_cond = "" if len(excludes) == 0 else f"AND c.table_name NOT IN ({db.helper.holders(len(excludes))})"
    in_cond = "" if len(includes) == 0 else f"AND c.table_name IN ({db.helper.holders(len(includes), start = len(excludes))})"

    c.execute(f"""\
        SELECT
            c.table_name, c.column_name, c.data_type, c.column_type, c.column_key, k.referenced_table_name, k.referenced_column_name, c.extra, c.column_comment
        FROM
            information_schema.columns AS c
            LEFT JOIN information_schema.key_column_usage AS k
                ON c.table_catalog = k.table_catalog
                    AND c.table_schema = k.table_schema
                    AND c.table_name = k.table_name
                    AND c.column_name = k.column_name
                    AND k.referenced_table_name IS NOT NULL
        WHERE
            c.table_schema = DATABASE()
            {ex_cond}
            {in_cond}
        ORDER BY
            c.table_name, c.ordinal_position ASC
        """, excludes + includes)

    def column_of(n, t, ct, key, rt, rc, extra, comment):
        return Column(n, _map_types(t), ct, key == "PRI", bool(rt), True if extra == "auto_increment" else None, comment or "")

    tables = []

    for t, cols in groupby(c.fetchall(), lambda row: row[0]):
        tables.append(Table(t, [column_of(*c[1:]) for c in cols]))

    c.execute(f"""\
        SELECT
            table_name, table_comment
        FROM
            information_schema.tables
        WHERE
            table_name IN ({db.helper.holders(len(tables))})
        """, [t.name for t in tables])

    table_map = {t.name: t for t in tables}

    for n, cmt in c.fetchall():
        if n in table_map:
            table_map[n].comment = cmt or ""

    c.close()

    return tables


def _map_types(t):
    # TODO Actually, this mapping depends on connection module.
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