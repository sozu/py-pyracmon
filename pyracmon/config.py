import logging
from functools import wraps


class PyracmonConfiguration:
    def __init__(
        self,
        name = None,
        logger = None,
        log_level = None,
        sql_log_length = None,
        parameter_log = None,
        paramstyle = None,
        type_mapping = None,
    ):
        self.name = name
        self.logger = logger
        self.log_level = log_level
        self.sql_log_length = sql_log_length
        self.parameter_log = parameter_log
        self.paramstyle = paramstyle
        self.type_mapping = type_mapping

    def derive(self, **kwargs):
        return DerivingConfiguration(self, **kwargs)


class DerivingConfiguration(PyracmonConfiguration):
    def __init__(self, base, **kwargs):
        super().__init__(**kwargs)
        self.base = base

    def __getattribute__(self, key):
        value = object.__getattribute__(self, key)
        return value if value is not None else getattr(self.base, key)

    def set(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self.base, k):
                setattr(self, k, v)
            else:
                raise KeyError(f"Unknown configuration key: {k}")
        return self


def default_config(config=PyracmonConfiguration(
    name = "default",
    logger = None,
    log_level = logging.DEBUG,
    sql_log_length = 4096,
    parameter_log = False,
    paramstyle = None,
    type_mapping = None,
)):
    return config


def pyracmon(**kwargs):
    class Configurable:
        def __init__(self):
            self.config = PyracmonConfiguration()

        def __enter__(self):
            return self.config

        def __exit__(self, exc_type, exc_value, traceback):
            if not exc_value:
                target = default_config()

                for k in vars(target):
                    v = getattr(self.config, k)
                    if v is not None:
                        setattr(target, k, v)
            return False

    return Configurable()

