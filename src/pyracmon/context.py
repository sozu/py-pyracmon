"""
This module provides the context type that controls query execution according to its configuration.
"""
from collections.abc import Sequence
import logging
from typing import Any, Literal, Self, overload
from .config import default_config
from . import dbapi


type PARAMS = list[Any] | dict[str, Any]
"""Type alias for query parameters.

The type of the marker determines whether a `list` or `dict` should be used.
"""


class ConnectionContext:
    """
    This class represents a context for query execution.

    By default, the context bound to the global configuration is used.
    A custom context can be used by setting an instance of it on the connection via the `Connection.use` method.
    """
    def __init__(self, identifier: str | None = None, **configurations):
        #: Identifier of this context. `None` by default.
        self.identifier = identifier
        #: Configuration used in this context.
        self.config = default_config().derive(**configurations)

    def _message(self, message):
        return f"({self.identifier}) {message}" if self.identifier else message

    def _log_query(self, sql: str, params: PARAMS, is_many: bool = False) -> None:
        if logger := _logger(self.config):
            sql_log = sql if len(sql) <= self.config.sql_log_length else f"{sql[0:self.config.sql_log_length]}..."

            logger.log(self.config.log_level, self._message(sql_log))

            if self.config.parameter_log:
                if is_many:
                    for ps in params:
                        logger.log(self.config.log_level, self._message(f"Parameters: {ps}"))
                else:
                    logger.log(self.config.log_level, self._message(f"Parameters: {params}"))


    def configure(self, **configurations: Any) -> Self:
        """
        Changes the configuration of this context.

        Changes made by this method never affect the global configuration, even if this context is based on it.

        Args:
            configurations: New configuration values. See `pyracmon.config` for the available keys.
        Returns:
            This instance.
        """
        self.config.set(**configurations)
        return self

    def execute(self, cursor: dbapi.Cursor, sql: str, params: PARAMS) -> dbapi.Cursor:
        """
        Executes a query on a cursor.

        Args:
            cursor: The cursor on which to execute the query.
            sql: The query string.
            params: The query parameters.
        Returns:
            The given cursor object. Its internal state may have changed as a result of executing the query.
        """
        return self._execute(cursor, sql, params, False)

    def executemany(self, cursor: dbapi.Cursor, sql: str, seq_of_params: Sequence[PARAMS]) -> dbapi.Cursor:
        """
        Repeats a query on a cursor for a sequence of parameters.

        This method works similarly to `execute`, but invokes `executemany` instead.

        Args:
            cursor: The cursor on which to execute the query.
            sql: The query string.
            seq_of_params: A sequence of parameter sets, one for each execution of the query.
        Returns:
            The given cursor object. Its internal state may have changed as a result of executing the query.
        """
        return self._execute(cursor, sql, seq_of_params, True)

    @overload
    def _execute(self, cursor: dbapi.Cursor, sql: str, params: PARAMS, is_many: Literal[False] = False) -> dbapi.Cursor: ...
    @overload
    def _execute(self, cursor: dbapi.Cursor, sql: str, params: Sequence[PARAMS], is_many: Literal[True] = True) -> dbapi.Cursor: ...
    def _execute(
        self,
        cursor: dbapi.Cursor,
        sql: str,
        params,
        is_many: bool = False,
    ) -> dbapi.Cursor:
        self._log_query(sql, params, is_many)

        if is_many:
            cursor.executemany(sql, params)
        else:
            cursor.execute(sql, params)

        return cursor


class AsyncConnectionContext(ConnectionContext):
    """
    Asyncio version of `ConnectionContext`.
    """
    async def execute(self, cursor: dbapi.AsyncCursor, sql: str, params: PARAMS) -> dbapi.AsyncCursor:
        """
        Executes a query on a cursor.

        Args:
            cursor: The cursor on which to execute the query.
            sql: The query string.
            params: The query parameters.
        Returns:
            The given cursor object. Its internal state may have changed as a result of executing the query.
        """
        return await self._execute(cursor, sql, params, False)

    async def executemany(self, cursor: dbapi.AsyncCursor, sql: str, seq_of_params: Sequence[PARAMS]) -> dbapi.AsyncCursor:
        """
        Repeats a query on a cursor for a sequence of parameters.

        This method works similarly to `execute`, but invokes `executemany` instead.

        Args:
            cursor: The cursor on which to execute the query.
            sql: The query string.
            seq_of_params: A sequence of parameter sets, one for each execution of the query.
        Returns:
            The given cursor object. Its internal state may have changed as a result of executing the query.
        """
        return await self._execute(cursor, sql, seq_of_params, True)

    @overload
    async def _execute(self, cursor: dbapi.AsyncCursor, sql: str, params: PARAMS, is_many: Literal[False] = False) -> dbapi.AsyncCursor: ...
    @overload
    async def _execute(self, cursor: dbapi.AsyncCursor, sql: str, params: Sequence[PARAMS], is_many: Literal[True] = True) -> dbapi.AsyncCursor: ...
    async def _execute(
        self,
        cursor: dbapi.AsyncCursor,
        sql: str,
        params,
        is_many: bool = False,
    ) -> dbapi.AsyncCursor:
        self._log_query(sql, params, is_many)

        if is_many:
            await cursor.executemany(sql, params)
        else:
            await cursor.execute(sql, params)

        return cursor


def _logger(config) -> logging.Logger | None:
    if isinstance(config.logger, logging.Logger):
        return config.logger
    elif isinstance(config.logger, str):
        return logging.getLogger(config.logger)
    else:
        return None