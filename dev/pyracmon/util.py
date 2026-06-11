"""
Utility types and functions for internal use.
"""
from collections.abc import Mapping, Sequence, Callable
from typing import Any, TypeVar


T = TypeVar('T')
CI = TypeVar('CI', bound=str | int, covariant=True)


#----------------------------------------------------------------
# Type aliases
#----------------------------------------------------------------
#: Type alias for a qualifier function that qualifies placeholder markers.
type Qualifier = Callable[[str], str]

#: Type alias for primary key value(s).
type PKS = Any | dict[str, Any]


#----------------------------------------------------------------
# Utility Functions
#----------------------------------------------------------------
def key_to_index(values: Mapping[CI, T], ordered_keys: Sequence[str]) -> dict[int, T]:
    """
    Replace the keys of a `dict` with their indices in an ordered list of keys.

    Args:
        values: The dictionary whose keys are replaced.
        ordered_keys: The keys that define the index for each string key in `values`.
    Returns:
        A new dictionary with the keys replaced by their indices.
    """
    def index(k):
        if isinstance(k, int):
            return k
        else:
            return ordered_keys.index(k)

    return {index(k):v for k, v in values.items()}