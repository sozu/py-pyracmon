"""
This module provides the abstration mechanism for marker creation used to embed parameters in a query.

DB-API 2.0 defines some `paramstyle`s and available style depends on DB driver.
In addition, some styles are not suitable for query construction in python code.
This library adopts unified marker `$_` instead of the styles to make things simple.

Everything in this module is used internally in most cases, thus user does not need to know the detail.
"""
from typing import *


class Marker:
    """
    Base class to enable unified marker instead of markers defined in DB-API 2.0.

    Each subclass corresponds to a `paramstyle` and implements methods to convert unified marker into its style.
    """
    @classmethod
    def of(cls, paramstyle: str) -> 'Marker':
        """
        Creates an instance for given `paramstyle`.

        Args:
            paramstyle: The name of `paramstyle` defined in DB-API 2.0.
        Returns:
            Created instance.
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

    def __call__(self, key: Optional[Union[int, str]] = None) -> str:
        """
        Renders a marker string for the key.

        Each invocation changes internal state of this instance.

        Args:
            key: A key which indicates the parameter. Type of the key depends on the style.
        Returns:
            Marker string.
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
        Generates a string of marker which is not indexed and keyed.

        Returns:
            Marker string.
        """
        raise NotImplementedError()

    def indexed(self, index: int) -> str:
        """
        Generates a string of indexed marker.

        Returns:
            Marker string.
        """
        raise NotImplementedError()

    def keyed(self, key: str) -> str:
        """
        Generates a string of keyed marker.

        Returns:
            Marker string.
        """
        raise NotImplementedError()

    def reset(self):
        """
        Resets the internal state.
        """
        pass

    def params(self, *args: Any, **kwargs: Any) -> Union[List[Any], Dict[str, Any]]:
        """
        Generates parameters in the form which is available for query execution.

        This method adjusts the form of given parameters according to the internal state
        which has been changed on each marker generation.

        Args:
            args: Parameters for indexed markers.
            kwargs: Parameters for keyed markers.
        Returns:
            List or dictionary of parameters.
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

    Returns:
        Marker string.
    """
    def render(self) -> str:
        return '?'


class NumericMarker(ListMarker):
    """
    This marker renders `:x` for the parameter at index `x` (starting from 1).

    Automatic numbering is used when invoked with no `key`, otherwise specified position is selected.
    The index obtained by next automatic numbering is an incremented value of the last obtained index.

    Returns:
        Marker string.
    """
    def render(self) -> str:
        return f":{len(self.param_keys)}"


class NamedMarker(DictMarker):
    """
    This marker renders `:key` for the parameter bound to `key`.

    Automatic key selection is used when invoked with no `key`, otherwise specified key is selected.
    Each automatic key is in the form of `keyn` where `n` is specified index or its order in generated keys.

    Returns:
        Marker string.
    """
    def render(self, key: str) -> str:
        return f":{key}"


class FormatMarker(ListMarker):
    """
    This marker renders `%s` for any parameter.

    Returns:
        Marker string.
    """
    def render(self) -> str:
        return '%s'


class PyformatMarker(DictMarker):
    """
    This marker renders `%(key)s` for the parameter bound to `key`.

    This works similarly to `NamedMarker`.

    Returns:
        Marker string.
    """
    def render(self, key: str) -> str:
        return f"%({key})s"
