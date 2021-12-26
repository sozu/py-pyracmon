import logging
from typing import *
from .config import default_config


class ConnectionContext:
    """
    This class represents a context of query execution.

    You don't need to care this object in most cases except for when you want to change the configuration at a query execution.
    """
    def __init__(self, identifier=None, **configurations):
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

    def execute(self, cursor: 'Cursor', sql: str, params: List[Any]) -> 'Cursor':
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
        logger = _logger(self.config)

        if logger:
            sql_log = sql if len(sql) <= self.config.sql_log_length else f"{sql[0:self.config.sql_log_length]}..."

            logger.log(self.config.log_level, self._message(sql_log))

            if self.config.parameter_log:
                logger.log(self.config.log_level, self._message(f"Parameters: {params}"))

        cursor.execute(sql, params)

        return cursor


def _logger(config):
    if isinstance(config.logger, logging.Logger):
        return config.logger
    elif isinstance(config.logger, str):
        return logging.getLogger(config.logger)
    else:
        return None