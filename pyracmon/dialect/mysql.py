from itertools import groupby
from pyracmon.model import Table, Column
from pyracmon.dialect.shared import MultiInsertMixin


def read_schema(db, excludes = [], includes = []):
    c = db.cursor()

    ex_cond = "" if len(excludes) == 0 else f"AND c.table_name NOT IN ({db.helper.holders(len(excludes))})"
    in_cond = "" if len(includes) == 0 else f"AND c.table_name IN ({db.helper.holders(len(includes), start = len(excludes))})"

    c.execute(f"""\
        SELECT
            c.table_name, c.column_name, c.column_key, c.extra
        FROM
            information_schema.columns AS c
        WHERE
            c.table_schema = DATABASE()
            {ex_cond}
            {in_cond}
        ORDER BY
            c.table_name, c.ordinal_position ASC
        """, excludes + includes)

    def column_of(n, key, extra):
        return Column(n, key == "PRI", True if extra == "auto_increment" else None)

    tables = []

    for t, cols in groupby(c.fetchall(), lambda row: row[0]):
        tables.append(Table(t, [column_of(*c[1:]) for c in cols]))

    c.close()

    return tables


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