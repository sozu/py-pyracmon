import logging
from .config import default_config


class ConnectionContext:
    """
    This class represents a context where DB operation is done on the configuration it has.

    By default, each instance has the local copy of global configuration which can be changed via `configure()` 
    """
    def __init__(self, identifier=None, **configurations):
        self.identifier = identifier
        self.config = default_config().derive(**configurations)

    def _message(self, message):
        return f"({self.identifier}) {message}" if self.identifier else message

    def configure(self, **configurations):
        """
        Change the configuration of this context.

        Parameters
        ----------
        configurations: {str: object}
            Pairs of configuration name and its value.

        Returns
        -------
        ConnectionContext
            This instance.
        """
        self.config.set(**configurations)
        return self

    def execute(self, cursor, sql, params):
        """
        Executes a query on a cursor.

        Parameters
        ----------
        cursor: Cursor
            Cursor object.
        sql: str
            Query string.
        params: [object]
            Query parameters.

        Returns
        -------
        Cursor
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