"""
This module exports types and functions for configuration.

The attributes of `PyracmonConfiguration` form the complete set of settings that control the behavior of this library.
"""
import logging
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field, fields
from typing import Any, Callable
from .model import Table, Column
from .model_graph import ConfigurableSpec


__all__ = [
    "default_config",
    "PyracmonConfiguration",
]


type TypeMap = Callable[..., type | None]
"""Signature of a function that takes at least a type name and returns a Python type if possible.

Depending on the DBMS, the function may be called with additional arguments.
For example, in PostgreSQL, the function is called with a `udt_name` keyword argument, which is the user-defined type name.
"""


@dataclass
class PyracmonConfiguration:
    """
    A dataclass whose attributes form the complete set of configuration values.
    """
    name: str = "default"
    """Name of this configuration. This value does not affect the behavior of any module."""
    logger: str | logging.Logger | None = None
    """A `Logger` instance, or the name of a logger, used for internal logs such as query logging."""
    log_level: int = logging.DEBUG
    """The logging level for internal logs."""
    sql_log_length: int = 4096
    """Maximum length of a logged query. Queries longer than this value are trimmed before being output."""
    parameter_log: bool = False
    """Flag to also log query parameters."""
    paramstyle: str | None = None
    """Parameter style defined in DB-API 2.0. This value overwrites the style obtained from the DB module."""
    type_mapping: TypeMap | None = None
    """Function that estimates a Python type from a type name in the database, plus optional arguments that depend on the DBMS."""
    graph_spec: ConfigurableSpec = ConfigurableSpec.create()
    """The graph specification used by default."""
    fixture_mapping: Callable[[Table, Column, int], Any] | None = None
    """Function that generates a fixture value for a column and an index."""
    fixture_tz_aware: bool = True
    """Flag to make fixture datetime values timezone-aware."""
    fixture_ignore_fk: bool = True
    """Flag not to generate fixture values for foreign key columns."""
    fixture_ignore_nullable: bool = True
    """Flag not to generate fixture values for nullable columns."""
    timedelta_unit: dict[str, Any] = field(default_factory=lambda: dict(seconds=1))
    """Default keyword arguments to pass to `datetime.timedelta`, used in the `near` matcher."""

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
        Creates a new configuration instance derived from this configuration.

        Each keyword argument overwrites the corresponding configuration value unless it is `None`.

        Args:
            kwargs: New configuration values. Each key must be a valid configuration key.
        Returns:
            The derived configuration.
        """
        self._check_fields(**kwargs)
        derived = PyracmonConfiguration()
        self._copy_to(derived, **kwargs)
        return derived

    def set(self, **kwargs: Any) -> None:
        """
        Updates this configuration by setting the given configuration values.

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
    base: PyracmonConfiguration | None = None
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
    Returns the global configuration.

    The global configuration is managed in the *context* provided by the `contextvars` module.
    Updates to the returned object will change the behavior of library modules globally.

    The object works as a context manager: inside a `with` block, a different object is used as the global configuration.

    ```python
    with default_config() as cfg:
        # Updates to cfg do not affect the global configuration.
        cfg.name = "another"
        assert default_config().name == "another"
    # Updates inside the with block are no longer valid.
    assert default_config().name == "default"
    ```

    Returns:
        The global configuration.
    """
    return config.get()