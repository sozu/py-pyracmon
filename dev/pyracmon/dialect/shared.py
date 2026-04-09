"""
This module exports model mixin types having model methods available in some RDBMS.
"""
from collections.abc import Sequence
from typing import Union, Any
from typing_extensions import Self
from ..connection import Connection
from ..clause import values
from ..model import model_values, check_columns
from ..mixin import CRUDInternalMeta
from ..util import key_to_index, Qualifier


class MultiInsertMixin(CRUDInternalMeta):
    """
    This class provides methods to execute queries which is not standard SQL but common to some RDBMS.
    """
    @classmethod
    def inserts(
        cls,
        db: Connection,
        rows: Sequence[Union[Self, dict[str, Any]]],
        qualifier: dict[str, Qualifier] = {},
        rows_per_insert: int = 1000,
    ) -> int:
        """
        Insert multiple records.

        Args:
            db: DB connection.
            rows: Rows to insert. Each item should be a model object or dictionary of columns and values.
            qualifier: Functions qualifying placeholder markers.
            rows_per_insert: Maximum number of rows to insert in one query execution.
        Returns:
            The number of inserted rows.
        """
        if len(rows) == 0:
            return 0

        dict_rows = [model_values(cls, r) for r in rows]

        for v in dict_rows:
            check_columns(cls, v)

        cols = list(dict_rows[0].keys())
        ordered_qs = key_to_index(qualifier, cols)

        offset = 0
        remainders = dict_rows

        sql_full = f"INSERT INTO {cls.name} ({', '.join(cols)}) VALUES {values(len(cols), rows_per_insert, ordered_qs)}"

        def insert(targets, index):
            num = len(targets)
            vals = sum([list(t.values()) for t in targets], [])

            sql = sql_full if num == rows_per_insert else \
                f"INSERT INTO {cls.name} ({', '.join(cols)}) VALUES {values(len(cols), num, ordered_qs)}"

            db.stmt().execute(sql, *vals)

            for c, v in cls.last_sequences(db, num):
                for i, r in enumerate(rows[index:index+num]):
                    if isinstance(r, cls):
                        setattr(r, c.name, v - (num - i - 1))

        while len(remainders) >= rows_per_insert:
            insert(remainders[0:rows_per_insert], offset)
            remainders = remainders[rows_per_insert:]
            offset += rows_per_insert

        if len(remainders) > 0:
            insert(remainders, offset)

        return len(rows)


class TruncateMixin:
    @classmethod
    def truncate(cls, db: Connection):
        raise NotImplementedError()