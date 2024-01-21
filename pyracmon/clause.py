"""
This module provides functions to generate miscellaneous clauses in query.
"""
from collections.abc import Mapping, Sequence, Callable
from typing import Any, Union, Optional
try:
    from typing import TypeAlias
except:
    from typing_extensions import TypeAlias
from .select import AliasedColumn
from .query import Expression
from .util import Qualifier


ORDER: TypeAlias = Union[bool, tuple[bool, bool], str]
"""Column order.

Boolean represents `ASC` or `DESC` by itself or as the first item of tuple. 
The second item of tuple represents `NULLS FIRST` or `NULLS LAST` .
`str` value is used as is.
"""

HolderKeys: TypeAlias = Union[str, int, None, Expression]


def order_by(columns: Mapping[Union[str, AliasedColumn], ORDER], **defaults: ORDER) -> str:
    """
    Generates `ORDER BY` clause from columns and directions.

    Args:
        columns: Columns and directions. Iteration order is kept in rendered clause.
        defaults: Column names and directions appended to the clause if the column is not contained in `columns` .
    Returns:
        `ORDER BY` clause.
    """
    columns = dict(columns, **{c:v for c,v in defaults.items() if c not in columns})
    def col(cd):
        if isinstance(cd[1], bool):
            return f"{cd[0]} ASC" if cd[1] else f"{cd[0]} DESC"
        elif isinstance(cd[1], str):
            return f"{cd[0]} {cd[1]}"
        elif isinstance(cd[1], tuple) and len(cd[1]) == 2:
            return f"{cd[0]} {'ASC' if cd[1][0] else 'DESC'} NULLS {'FIRST' if cd[1][1] else 'LAST'}"
        else:
            raise ValueError(f"Directions must be specified by bool, pair of bools or string: {cd[1]}")
    return '' if len(columns) == 0 else f"ORDER BY {', '.join(map(col, columns.items()))}"


def ranged_by(limit: Optional[int] = None, offset: Optional[int] = None) -> tuple[str, list[Any]]:
    """
    Generates `LIMIT OFFSET` clause using marker.

    Args:
        limit: Limit value. `None` means no limitation.
        offset: Offset value. `None` means `0`.
    Returns:
        `LIMIT OFFSET` clause and its parameters.
    """
    clause, params = [], []

    if limit is not None:
        clause.append("LIMIT $_")
        params.append(limit)

    if offset is not None:
        clause.append("OFFSET $_")
        params.append(offset)

    return ' '.join(clause) if clause else '', params


def holders(length_or_keys: Union[int, Sequence[HolderKeys]], qualifier: Optional[Mapping[int, Qualifier]] = None) -> str:
    """
    Generates partial query string containing placeholder markers separated by comma.

    Args:
        length_or_keys: The number of placeholders or list of placeholder keys.
        qualifier: Qualifying function for each index.
    Returns:
        Query string.
    """
    if isinstance(length_or_keys, int):
        hs = ["${_}"] * length_or_keys
    else:
        def key(k):
            if isinstance(k, Expression):
                return k.expression
            elif isinstance(k, int):
                return f"${{_{k}}}"
            elif k:
                return f"${{{k}}}"
            else:
                return "${_}"
        hs = [key(k) for k in length_or_keys]

    if qualifier:
        hs = [qualifier.get(i, _noop)(h) for i, h in enumerate(hs)]

    return ', '.join(hs)


def values(length_or_key_gen: Union[int, Sequence[Callable[[int], HolderKeys]]], rows: int, qualifier: Optional[Mapping[int, Qualifier]] = None) -> str:
    """
    Generates partial query string for `VALUES` clause in insertion query.

    Args:
        length_or_key_gen: The number of placeholders or list of functions taking row index and returning key for each placeholder.
        rows: The number of rows to insert.
        qualifier: Qualifying function for each index.
    Returns:
        Query string.
    """
    if isinstance(length_or_key_gen, int):
        lok = lambda i: length_or_key_gen
    else:
        lok = lambda i: [g(i) for g in length_or_key_gen] # type: ignore

    return ', '.join([f"({holders(lok(i), qualifier)})" for i in range(rows)])


def _noop(x):
    return x