import logging
from functools import wraps
from .model_graph import ConfigurableSpec
from .util import Configurable


class PyracmonConfiguration:
    """
    Configurations for various modules.

    Attributes
    ----------
    name: str
        Name of this configuration. This value has no effect on any behavior of modules.
    logger: str | logging.Logger
        Logger or the name of logger used for internal logs such as query logging.
    log_level: int
        Logging level of internal logs.
    sql_log_length: int
        Maximum length for query log. Queries longer than this value are output with being trimmed.
    parameter_log: bool
        Flag to log query parameters also.
    paramstyle: str
        Parameter style defined in DB-API 2.0. This value overwrites the style obtained via DB module.
    type_mapping: (str, **) -> type
        Function estimating python type from type name in database and optional DBMS dependent keys.
    graph_spec: ConfigurableSpec
        Graph specification used as default.
    fixture_mapping: (Table, Column, int) -> object
        Function generating fixture value for a column and an index.
    fixture_tz_aware: bool
        Flag to make fixture datetime being aware of timezone.
    fixture_ignore_fk: bool
        Flag not to generate fixuture value on foreign key columns.
    fixture_ignore_nullable: bool
        Flag not to generate fixuture value on nullable columns.
    timedelta_unit: dict
        Default keyword arguments to pass `timedelta()` to compare actual `date`/`datetime` with expected one in `near` matcher.
    """
    def __init__(
        self,
        name = None,
        logger = None,
        log_level = None,
        sql_log_length = None,
        parameter_log = None,
        paramstyle = None,
        type_mapping = None,
        graph_spec = None,
        fixture_mapping = None,
        fixture_tz_aware = None,
        fixture_ignore_fk = None,
        fixture_ignore_nullable = None,
        timedelta_unit = None,
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

    def derive(self, **kwargs):
        def attr(k):
            v = getattr(self, k)
            return v.clone() if isinstance(v, Configurable) else None
        attrs = {k:attr(k) for k in vars(self) if attr(k) is not None}
        attrs.update(kwargs)
        return DerivingConfiguration(self, **attrs)


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
    graph_spec = None,
    fixture_mapping = None,
    fixture_tz_aware = True,
    fixture_ignore_fk = True,
    fixture_ignore_nullable = True,
    timedelta_unit = dict(seconds=1),
)):
    return config


def pyracmon(**kwargs):
    """
    Generates an object which starts `with` context, where global configuration can be changed.

    An object obtained by `as` keyword in `with` clause is an instance of `PyracmonConfiguration`,
    and changes to it done in the `with` context are copied into global configuration at the end of the context if no exception raises.

    >>> with pyracmon() as cfg:
    >>>     cfg.name = "my_config"
    >>>     ...
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

