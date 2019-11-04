import re
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
            c.table_name, c.column_name, k.constraint_type IS NOT NULL, c.column_default
        FROM
            information_schema.columns AS c
            INNER JOIN information_schema.tables AS t ON c.table_name = t.table_name
            LEFT JOIN (
                SELECT
                    tc.table_name, k.column_name, tc.constraint_type
                FROM
                    information_schema.key_column_usage AS k
                    INNER JOIN information_schema.table_constraints AS tc ON k.constraint_name = tc.constraint_name
                WHERE
                    tc.constraint_type = 'PRIMARY KEY'
            ) AS k ON t.table_name = k.table_name AND c.column_name = k.column_name
        WHERE
            c.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
            {ex_cond}
            {in_cond}
        ORDER BY c.table_name ASC, c.ordinal_position ASC
        """, excludes + includes)

    def column_of(n, pk, default):
        m = SequencePattern.match(default or "")
        seq = m.group(1) if m else None
        return Column(n, pk, seq)

    tables = []

    for t, cols in groupby(c.fetchall(), lambda row: row[0]):
        columns = [column_of(*c[1:]) for c in cols]
        tables.append(Table(t, columns))

    c.close()

    return tables


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