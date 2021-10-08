"""
This module exports types and functions for configurations.

`PyracmonConfiguration` is a class exposing configurable attributes.
An instance of the class held in this module is treated as global configuration.  `pyracmon` is an only awy to change it.
Changes done in ``with`` block is activated after the block finished without error.

>>> assert default_config().name == "default"
>>> with pyracmon() as cfg:
>>>     cfg.name = "my_config"
>>> assert default_config().name == "my_config"

See the attributes of `PyracmonConfiguration` to know all configuration keys and their effects.
"""
import logging
from functools import wraps
from typing import *
from .model import Table, Column
from .model_graph import ConfigurableSpec
from .util import Configurable


class PyracmonConfiguration:
    """
    A class to hold all configurable values in attributes.

    :param name: Name of this configuration. This value has no effect on any behavior of modules. Default is ``default`` .
    :param logger: Logger or the name of logger used for internal logs such as query logging. Defalut is ``None`` .
    :param log_level: Logging level of internal logs. Default is `logging.DEBUG` .
    :param sql_log_length: Maximum length of query log. Queries longer than this value are output being trimmed. Default is ``4096`` .
    :param parameter_log: Flag to log query parameters also. Default is ``False`` .
    :param paramstyle: Parameter style defined in DB-API 2.0. This value overwrites the style obtained via DB module. Default is ``None`` .
    :param type_mapping: Function estimating python type from type name in database and optional DBMS dependent keys. Default is ``None`` .
    :param graph_spec: Graph specification used as default. Default is ``None`` .
    :param fixture_mapping: Function generating fixture value for a column and an index. Default is ``None`` .
    :param fixture_tz_aware: Flag to make fixture datetime being aware of timezone. Default is ``True`` .
    :param fixture_ignore_fk: Flag not to generate fixuture value on foreign key columns. Default is ``True`` .
    :param fixture_ignore_nullable: Flag not to generate fixuture value on nullable columns. Default is ``True`` .
    :param timedelta_unit: Default keyword arguments to pass ``datetime.timedelta()`` used in `near` matcher.
    """
    def __init__(
        self,
        name: str = None,
        logger: Union[str, logging.Logger] = None,
        log_level: int = None,
        sql_log_length: int = None,
        parameter_log: bool = None,
        paramstyle: str = None,
        type_mapping: Callable[[str], type] = None,
        graph_spec: ConfigurableSpec = None,
        fixture_mapping: Callable[[Table, Column, int], Any] = None,
        fixture_tz_aware: bool = None,
        fixture_ignore_fk: bool = None,
        fixture_ignore_nullable: bool = None,
        timedelta_unit: Dict[str, Any] = None,
    ):
        self.name = name
        self.logger = logger
        self.log_level = log_level
        self.sql_log_length = sql_log_length
        self.parameter_log = parameter_log
        self.paramstyle = paramstyle
        self.type_mapping = type_mapping
        self.graph_spec = graph_spec or ConfigurableSpec.create()
        self.fixture_mapping = fixture_mapping
        self.fixture_tz_aware = fixture_tz_aware
        self.fixture_ignore_fk = fixture_ignore_fk
        self.fixture_ignore_nullable = fixture_ignore_nullable
        self.timedelta_unit = timedelta_unit

    def derive(self, **kwargs: Any) -> 'PyracmonConfiguration':
        """
        Creates new configuration instance deriving this configuration.

        Keys of arguments are attribute names of this class.
        You don't need to supply value to every key. Values beging untouched or set to ``None`` inherit deriving configuration.
        """
        def attr(k):
            v = getattr(self, k)
            return v.clone() if isinstance(v, Configurable) else None
        attrs = {k:attr(k) for k in vars(self) if attr(k) is not None}
        attrs.update(kwargs)
        return DerivingConfiguration(self, **attrs)


class DerivingConfiguration(PyracmonConfiguration):
    """
    :meta private:
    """
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
    graph_spec = None,
    fixture_mapping = None,
    fixture_tz_aware = True,
    fixture_ignore_fk = True,
    fixture_ignore_nullable = True,
    timedelta_unit = dict(seconds=1),
)):
    return config


def pyracmon(**kwargs: Any) -> PyracmonConfiguration:
    """
    Starts `with` block to change global configurations.

    Returning object is not an instance of `PyracmonConfiguration` but allows the access to equivalent attributes.
    Changes done in the block is activated after the block finished without error.

    >>> assert default_config().name == "default"
    >>> with pyracmon() as cfg:
    >>>     cfg.name = "my_config"
    >>>     ...
    >>> assert default_config().name == "my_config"
    """
    class Configurable:
        def __init__(self):
            self.config = default_config().derive()

        def __enter__(self):
            return self.config

        def __exit__(self, exc_type, exc_value, traceback):
            if not exc_value:
                target = default_config()

                for k in vars(target):
                    v = getattr(self.config, k)
                    if isinstance(v, Configurable):
                        getattr(target, k).replace(v)
                    elif v is not None:
                        setattr(target, k, v)
            return False

    return Configurable()

