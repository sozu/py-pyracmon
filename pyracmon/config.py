"""
This module exports types and functions for configurations.

Attributes of `PyracmonConfiguration` are the complete set of configurations which control the behaviors of this library.
"""
import logging
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, fields
from typing import Any, Union, Optional, Callable
from typing_extensions import Self, TypeAlias, Unpack, TypeVarTuple
from .model import Table, Column
from .model_graph import ConfigurableSpec


__all__ = [
    "default_config",
    "PyracmonConfiguration",
]


#Ts = TypeVarTuple("Ts")
#TypeMap: TypeAlias = Callable[[str, Unpack[Ts]], Optional[type]]
TypeMap: TypeAlias = Callable[..., Optional[type]]
"""Signature of a function which takes at least a type name and returns a python type if possible.

According to DBMS, the function will be called with additional arguments.
It is recommended to add keyword arguments to the function even if you don't need them.
"""


@dataclass
class PyracmonConfiguration:
    """
    A dataclass whose attirubutes are the complete set of configurations.
    """
    name: str = "default"
    """Name of this configuration. This value has no effect on any behavior of modules. """
    logger: Union[str, logging.Logger, None] = None
    """Logger or the name of logger used for internal logs such as query logging."""
    log_level: int = logging.DEBUG
    """Logging level of internal logs."""
    sql_log_length: int = 4096
    """Maximum length of query log. Queries longer than this value are output being trimmed."""
    parameter_log: bool = False
    """Flag to log query parameters also."""
    paramstyle: Optional[str] = None
    """Parameter style defined in DB-API 2.0. This value overwrites the style obtained via DB module."""
    type_mapping: Optional[TypeMap] = None
    """Function estimating python type from type name in database and optional arguments dependent on DBMS."""
    graph_spec: ConfigurableSpec = ConfigurableSpec.create()
    """Graph specification used as default."""
    fixture_mapping: Optional[Callable[[Table, Column, int], Any]] = None
    """Function generating fixture value for a column and an index."""
    fixture_tz_aware: bool = True
    """Flag to make fixture datetime being aware of timezone."""
    fixture_ignore_fk: bool = True
    """Flag not to generate fixuture value on foreign key columns."""
    fixture_ignore_nullable: bool = True
    """Flag not to generate fixuture value on nullable columns."""
    timedelta_unit: dict[str, Any] = field(default_factory=lambda: dict(seconds=1))
    """Default keyword arguments to pass `datetime.timedelta` used in `near` matcher."""

    def _copy_to(self, other: 'PyracmonConfiguration', **kwargs: Any):
        for f in fields(self):
            val = kwargs[f.name] if f.name in kwargs else deepcopy(getattr(self, f.name))
            setattr(other, f.name, val)

    def _check_fields(self, **kwargs: Any):
        names = {f.name for f in fields(self)}
        invalid = [k for k in kwargs.keys() if k not in names]
        if len(invalid) > 0:
            raise KeyError(f"Invalid configuration keys are found: {', '.join(invalid)}")

    def derive(self, **kwargs: Any) -> 'PyracmonConfiguration':
        """
        Creates new configuration instance deriving this configuration.

        Each keyword argument overwrites corresponding configuration value unless it is `None`.

        Args:
            kwargs: New configuration values. Each key must be a valid configuration key.
        Returns:
            Derived configuration.
        """
        self._check_fields(**kwargs)
        derived = PyracmonConfiguration()
        self._copy_to(derived, **kwargs)
        return derived

    def set(self, **kwargs: Any) -> None:
        """
        Updates this configuration by setting given configuration values.

        Args:
            kwargs: New configuration values. Each key must be a valid configuration key.
        """
        self._check_fields(**kwargs)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __enter__(self) -> 'PyracmonConfiguration':
        derived = self.derive()
        return derived

    def __exit__(self, exc_type, exc_value, traceback):
        pass


def contextualConfiguration(
    config_var: Callable[[], ContextVar[PyracmonConfiguration]],
    base: Optional[PyracmonConfiguration] = None
) -> PyracmonConfiguration:
    @dataclass
    class contextual(PyracmonConfiguration):
        def __enter__(self) -> 'PyracmonConfiguration':
            derived = contextual()
            self._copy_to(derived)
            config_var().set(derived)
            return derived

        def __exit__(self, exc_type, exc_value, traceback):
            config_var().set(self)

    cfg = contextual()
    if base:
        base._copy_to(cfg)
    return cfg


config: ContextVar[PyracmonConfiguration] = ContextVar('config', default=contextualConfiguration(lambda: config))


def default_config() -> PyracmonConfiguration:
    """
    Returns a global configuration.

    Global configuration is managed in *context* provided by `contextvars` module.
    Update on the returned object will change the behaviors of library modules globally.

    The object works as a context manager by `with` block where another object can be used as global configuration.

    ```python
    with default_config() as cfg:
        # Updates to cfg are not affect the global configurations.
        cfg.name = "another"
        assert default_config().name == "another"
    # Updates inside with block is no longer valid.
    assert default_config().name == "default"
    ```

    Returns:
        Global configuration.
    """
    return config.get()