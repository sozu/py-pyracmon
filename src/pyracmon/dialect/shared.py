"""
This module exports model mixin types that provide methods available in certain RDBMSs.
"""
from collections.abc import Sequence, Mapping
from typing import Any, TypeVar
from itertools import batched, chain
from ..dbapi import cursor, acursor
from ..connection import Connection, AsyncConnection
from ..mixin import CRUDMixinBase
from ..mixin_async import AsyncCRUDMixinBase
from .._mixin import _insert, _render_many
from ..util import Qualifier


M = TypeVar('M', bound='MultiInsertMixin')
AM = TypeVar('AM', bound='AsyncMultiInsertMixin')


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

        offset = 0
        for page_rows in batched(rows, rows_per_insert):
            num = len(page_rows)

            models, expression, all_params = _render_many(cls, page_rows, qualifier)
            sql = _insert(cls, expression, length=num)

            with cursor(db.stmt().execute(sql, *chain.from_iterable(all_params))) as c:
                for c, v in cls.last_sequences(db, num):
                    for i, r in enumerate(rows[offset:offset+num]):
                        if isinstance(r, cls):
                            setattr(r, c.name, v - (num - i - 1))

            offset += num

        return len(rows)


class AsyncMultiInsertMixin(AsyncCRUDMixinBase):
    """
    This class provides methods to execute queries that are not standard SQL but are common across several RDBMSs.
    """
    @classmethod
    async def inserts(
        cls: type[AM],
        db: AsyncConnection,
        rows: Sequence[AM | dict[str, Any]],
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

        offset = 0
        for page_rows in batched(rows, rows_per_insert):
            num = len(page_rows)

            models, expression, all_params = _render_many(cls, page_rows, qualifier)
            sql = _insert(cls, expression, length=num)

            async with acursor(await db.stmt().execute(sql, *chain.from_iterable(all_params))) as c:
                for c, v in await cls.last_sequences(db, num):
                    for i, r in enumerate(rows[offset:offset+num]):
                        if isinstance(r, cls):
                            setattr(r, c.name, v - (num - i - 1))

            offset += num

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


class AsyncTruncateMixin:
    @classmethod
    async def truncate(cls, db: AsyncConnection):
        """
        Truncate the table associated with this model.

        Args:
            db: The DB connection.
        """
        raise NotImplementedError()