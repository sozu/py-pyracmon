"""
This module provides an abstraction mechanism for creating markers used to embed parameters in a query.

DB-API 2.0 defines several `paramstyle`s, and which one is available depends on the DB driver.
However, some of these styles are not convenient for writing queries in Python code.
This library adopts a unified marker, `$_`, instead of those styles to keep things simple.

Everything in this module is used internally in most cases, so users typically do not need to know the details.
"""
from typing import Any


class Marker:
    """
    Base class that enables a unified marker in place of the markers defined by DB-API 2.0.

    Each subclass corresponds to a `paramstyle` and implements the methods to convert the unified marker into that style.
    """
    @classmethod
    def of(cls, paramstyle: str) -> 'Marker':
        """
        Creates an instance for the given `paramstyle`.

        Args:
            paramstyle: The name of a `paramstyle` defined in DB-API 2.0.
        Returns:
            The created instance.
        """
        if paramstyle == 'qmark':
            return QMarker()
        elif paramstyle == 'numeric':
            return NumericMarker()
        elif paramstyle == 'named':
            return NamedMarker()
        elif paramstyle == 'format':
            return FormatMarker()
        elif paramstyle == 'pyformat':
            return PyformatMarker()
        else:
            raise ValueError(f"Unknown parameter style: {paramstyle}")

    def __call__(self, key: int | str | None = None) -> str:
        """
        Renders a marker string for the key.

        Each invocation changes the internal state of this instance.

        Args:
            key: A key that indicates the parameter. The type of the key depends on the style.
        Returns:
            The rendered marker string.
        """
        if key is None:
            return self.default()
        elif isinstance(key, int) and key > 0:
            return self.indexed(key)
        elif isinstance(key, str) and key != "":
            return self.keyed(key)
        else:
            raise ValueError("Argument of marker invocation must be a positive int or str.")

    def default(self) -> str:
        """
        Generates a marker string that is neither indexed nor keyed.

        Returns:
            The rendered marker string.
        """
        raise NotImplementedError()

    def indexed(self, index: int) -> str:
        """
        Generates an indexed marker string.

        Args:
            index: The index of the parameter.
        Returns:
            The rendered marker string.
        """
        raise NotImplementedError()

    def keyed(self, key: str) -> str:
        """
        Generates a keyed marker string.

        Args:
            key: The key of the parameter.
        Returns:
            The rendered marker string.
        """
        raise NotImplementedError()

    def reset(self):
        """
        Resets the internal state.
        """
        pass

    def params(self, *args: Any, **kwargs: Any) -> list[Any] | dict[str, Any]:
        """
        Generates parameters in the form required for query execution.

        This method adjusts the form of the given parameters according to the internal state,
        which is updated on each marker generation.

        Args:
            args: Parameters for indexed markers.
            kwargs: Parameters for keyed markers.
        Returns:
            A list or dictionary of parameters.
        """
        raise NotImplementedError()


class ListMarker(Marker):
    def __init__(self, start=0):
        self.index = start
        self.param_keys = []

    def render(self):
        raise NotImplementedError()

    def default(self):
        self.param_keys.append(self.index)
        self.index += 1
        return self.render()

    def indexed(self, index):
        self.param_keys.append(index-1)
        return self.render()

    def keyed(self, key):
        self.param_keys.append(key)
        return self.render()

    def reset(self):
        self.index = 0
        self.param_keys = []

    def params(self, *args, **kwargs):
        def get(k):
            if isinstance(k, int):
                return args[k]
            else:
                return kwargs[k]

        return [get(k) for k in self.param_keys]


class DictMarker(Marker):
    def __init__(self, start=0):
        self.index = start
        self.param_keys = {}

    def render(self, key):
        raise NotImplementedError()

    def new_key(self, index):
        return f"param{index}"

    def default(self):
        key = self.param_keys.setdefault(self.index, self.new_key(self.index+1))
        self.index += 1
        return self.render(key)

    def indexed(self, index):
        key = self.param_keys.setdefault(index-1, self.new_key(index))
        return self.render(key)

    def keyed(self, key):
        self.param_keys[key] = key
        return self.render(key)

    def reset(self):
        self.index = 0
        self.param_keys = {}

    def params(self, *args, **kwargs):
        def get(k, v):
            if isinstance(k, int):
                return (v, args[k])
            else:
                return (v, kwargs[k])

        return dict([get(k, v) for k, v in self.param_keys.items()])


class QMarker(ListMarker):
    """
    This marker renders `?` for any parameter.
    """
    def render(self) -> str:
        return '?'


class NumericMarker(ListMarker):
    """
    This marker renders `:x` for the parameter at index `x` (starting from 1).

    Automatic numbering is used when invoked with no `key`, otherwise the specified position is selected.
    The index obtained by the next automatic numbering is one greater than the last obtained index.
    """
    def render(self) -> str:
        return f":{len(self.param_keys)}"


class NamedMarker(DictMarker):
    """
    This marker renders `:key` for the parameter bound to `key`.

    Automatic key selection is used when invoked with no `key`, otherwise the specified key is selected.
    Each automatic key has the form `paramN`, where `N` is either the specified index or the position of this key among the automatically generated keys.
    """
    def render(self, key: str) -> str:
        return f":{key}"


class FormatMarker(ListMarker):
    """
    This marker renders `%s` for any parameter.
    """
    def render(self) -> str:
        return '%s'


class PyformatMarker(DictMarker):
    """
    This marker renders `%(key)s` for the parameter bound to `key`.

    This works similarly to `NamedMarker`.
    """
    def render(self, key: str) -> str:
        return f"%({key})s"
