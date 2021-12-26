"""
Utility types and functions for internal use.
"""
from typing import *


# type aliases
Qualifier = Dict[str, Callable[[str], str]]


class Configurable:
    """
    Interface for classes of configuration objects.
    """
    def clone(self):
        raise NotImplementedError()

    def replace(self, another):
        raise NotImplementedError()


def key_to_index(values: Dict[Union[str, int], Any], ordered_keys: List[Union[str, int]]) -> Dict[int, Any]:
    """
    Replace keys of a `dict` with its index in ordered list.

    Args:
        values: A dictionary.
        ordered_keys: Ordered keys.
    Returns:
        New dictionary where keys are replaced.
    """
    def index(k):
        if isinstance(k, int):
            return k
        else:
            return ordered_keys.index(k)

    return {index(k):v for k, v in values.items()}