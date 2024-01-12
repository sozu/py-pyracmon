"""
This module provides the context type which controls query execution as configured.
"""
from collections.abc import Sequence
import logging
from typing import Any, Union, Optional, Literal, overload
try:
    from typing import TypeAlias
except:
    from typing_extensions import TypeAlias
from .config import default_config
from . import dbapi


PARAMS: TypeAlias = Union[list[Any], dict[str, Any]]


class ConnectionContext:
    """
    This class represents a context of query execution.

    You don't need to care this object in most cases except for when you want to change the configuration at a query execution.
    """
    def __init__(self, identifier: Optional[str] = None, **configurations):
        self.identifier = identifier
        self.config = default_config().derive(**configurations)

    def _message(self, message):
        return f"({self.identifier}) {message}" if self.identifier else message

    def configure(self, **configurations: Any) -> 'ConnectionContext':
        """
        Change configurations of this context.

        Args:
            configurations: Configurations. See `pyracmon.config` to know available keys.
        Returns:
            This instance.
        """
        self.config.set(**configurations)
        return self

    def execute(self, cursor: dbapi.Cursor, sql: str, params: PARAMS) -> dbapi.Cursor:
        """
        Executes a query on a cursor.

        Query logging is also done in this method according to the configuration.

        This method is invoked from `ConnectionContext.execute` internally.
        When you intend to change behaviors of query executions,
        inherit this class, overwrite this method and set factory method for the class by `Connection.use` .

        Args:
            cursor: Cursor object.
            sql: Query string.
            params: Query parameters.
        Returns:
            Given cursor object. Internal state may be changed by the execution of the query.
        """
        return self._execute(cursor, sql, params, False)

    def executemany(self, cursor: dbapi.Cursor, sql: str, params: Sequence[PARAMS]) -> dbapi.Cursor:
        """
        Repeats query on a cursor for sequencee of parameters.

        This method works similar to `execute` but invoke `executemany` instead.

        Args:
            cursor: Cursor object.
            sql: Query string.
            params: Sequence of query parameters.
        Returns:
            Given cursor object. Internal state may be changed by the execution of the query.
        """
        return self._execute(cursor, sql, params, True)

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
        logger = _logger(self.config)

        if logger:
            sql_log = sql if len(sql) <= self.config.sql_log_length else f"{sql[0:self.config.sql_log_length]}..."

            logger.log(self.config.log_level, self._message(sql_log))

            if self.config.parameter_log:
                if is_many:
                    for ps in params:
                        logger.log(self.config.log_level, self._message(f"Parameters: {ps}"))
                else:
                    logger.log(self.config.log_level, self._message(f"Parameters: {params}"))

        if is_many:
            cursor.executemany(sql, params)
        else:
            cursor.execute(sql, params)

        return cursor


def _logger(config):
    if isinstance(config.logger, logging.Logger):
        return config.logger
    elif isinstance(config.logger, str):
        return logging.getLogger(config.logger)
    else:
        return None