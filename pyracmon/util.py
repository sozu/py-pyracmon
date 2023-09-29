"""
Utility types and functions for internal use.
"""
from collections.abc import Mapping, Sequence, Callable
from typing import Any, Union, TypeVar, TypeAlias


T = TypeVar('T')


#----------------------------------------------------------------
# Utility Types
#----------------------------------------------------------------
Qualifier: TypeAlias = Callable[[str], str]
"""Qualifier function."""

PKS = Union[Any, dict[str, Any]]
"""Primary key(s)."""


#----------------------------------------------------------------
# Utility Functions
#----------------------------------------------------------------
def key_to_index(values: Mapping[str, T], ordered_keys: Sequence[str]) -> dict[int, T]:
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