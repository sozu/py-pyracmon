"""
This module exports model mixin types that provide methods available in certain RDBMSs.
"""
from collections.abc import Sequence, Mapping
from typing import Any, TypeVar
from ..connection import Connection
from ..clause import values
from ..model import model_values, check_columns
from ..mixin import CRUDMixinBase
from ..util import key_to_index, Qualifier


M = TypeVar('M', bound='MultiInsertMixin')


class MultiInsertMixin(CRUDMixinBase):
    """
    This class provides methods to execute queries that are not standard SQL but are common across several RDBMSs.
    """
    @classmethod
    def inserts(
        cls: type[M],
        db: Connection,
        rows: Sequence[M | dict[str, Any]],
        qualifier: Mapping[str, Qualifier] = {},
        rows_per_insert: int = 1000,
    ) -> int:
        """
        Insert multiple records.

        Args:
            db: The DB connection.
            rows: The rows to insert. Each item should be a model object or a dictionary of columns and values.
            qualifier: A mapping from column names to functions that qualify their placeholder markers.
            rows_per_insert: The maximum number of rows to insert per query execution.
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
        """
        Truncate the table associated with this model.

        Args:
            db: The DB connection.
        """
        raise NotImplementedError()