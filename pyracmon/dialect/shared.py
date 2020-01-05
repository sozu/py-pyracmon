from functools import reduce
from pyracmon.util import split_dict, index_qualifier, model_values

class MultiInsertMixin:
    """
    This class provides methods to execute queries which is not standard SQL but common to some RDBMS.
    """
    @classmethod
    def inserts(cls, db, rows, qualifier = {}, rows_per_insert = 1000):
        """
        Insert multiple records.

        Parameters
        ----------
        db: Connection
            DB connection.
        values: [{str: object}] | [model]
            Rows to insert. Each item should be a dictionary of columns and values or model object.
        qualifier: {str: str -> str}
            A mapping from column name to a function converting holder marker into another SQL expression.
        rows_per_insert: int
            Maximum number of rows to insert in one query execution.

        Returns
        -------
        int
            The number of inserted rows.
        """
        if len(rows) == 0:
            return 0

        dict_rows = [model_values(cls, r) for r in rows]

        col_names = list(cls._check_columns(dict_rows[0]))
        qualifier = index_qualifier(qualifier, col_names)

        c = db.cursor()
        remainders = dict_rows

        offset = 0
        sql_full = f"INSERT INTO {cls.name} ({', '.join(col_names)}) VALUES {db.helper.values(len(col_names), rows_per_insert, qualifier)}"

        def insert(cursor, targets, index):
            num = len(targets)
            values = sum([[r[c] for c in col_names] for r in targets], [])
            sql = sql_full if num == rows_per_insert else \
                f"INSERT INTO {cls.name} ({', '.join(col_names)}) VALUES {db.helper.values(len(col_names), num, qualifier)}"
            cursor.execute(sql, values)
            for c, v in cls.last_sequences(db, num):
                for i, r in enumerate(rows[index:index+num]):
                    if isinstance(r, cls):
                        setattr(r, c.name, v - (num - i - 1))

        while len(remainders) >= rows_per_insert:
            insert(c, remainders[0:rows_per_insert], offset)
            remainders = remainders[rows_per_insert:]
            offset += rows_per_insert

        if len(remainders) > 0:
            insert(c, remainders, offset)

        return len(rows)