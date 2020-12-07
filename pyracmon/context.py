import logging
import threading
from .config import default_config


class ConnectionContext:
    _local = threading.local()

    @classmethod
    def get(cls, identifier, factory=None):
        if not hasattr(cls._local, "stack"):
            cls._local.stack = {}

        if identifier not in cls._local.stack:
            cls._local.stack[identifier] = (factory or ConnectionContext)()

        return cls._local.stack[identifier]

    @classmethod
    def reset(cls, identifier):
        if identifier in getattr(cls._local, "stack", {}):
            del cls._local.stack[identifier]

    def __init__(self, **configurations):
        self.config = default_config().derive(**configurations)

    def configure(self, **configurations):
        self.config.set(**configurations)

    def execute(self, cursor, sql, params):
        logger = _logger(self.config)

        if logger:
            sql_log = sql if len(sql) <= self.config.sql_log_length else f"{sql[0:self.config.sql_log_length]}..."

            logger.log(self.config.log_level, sql_log)

            if self.config.parameter_log:
                logger.log(self.config.log_level, f"Parameters: {params}")

        cursor.execute(sql, params)

        return cursor


def _logger(config):
    if isinstance(config.logger, logging.Logger):
        return config.logger
    elif isinstance(config.logger, str):
        return logging.getLogger(config.logger)
    else:
        return None