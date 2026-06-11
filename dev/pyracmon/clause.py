"""
This module provides functions to generate miscellaneous clauses in a query.
"""
from collections.abc import Mapping, Sequence, Callable
from typing import Any
from .select import AliasedColumn
from .query import Expression
from .util import Qualifier


type ORDER = bool | tuple[bool, bool] | str
"""The specification of column order.

A boolean represents `ASC` or `DESC`, either by itself or as the first item of a tuple.
The second item of the tuple represents `NULLS FIRST` or `NULLS LAST`.
A `str` value is used as is.
"""

type HolderKeys = str | int | None | Expression
"""The specification of a placeholder in a query string.

Placeholders in a query string are represented in `string.Template` format such as `${key}`.
They are converted into placeholders in the actual query string according to the parameter style of the DB driver.

A `HolderKeys` value determines two things based on its type: how the placeholder is rendered, and which parameter is used for it.

- `str` corresponds to the keyword in keyword parameters.
- `int` corresponds to the index in positional parameters.
- `None` is used for the default correspondence.
- `Expression` is used as is; it determines the correspondence between the parameter and the placeholder by itself.
"""


def order_by[K: (str, AliasedColumn)](columns: Mapping[K, ORDER], **defaults: ORDER) -> str:
    """
    Generates an `ORDER BY` clause from columns and orders.

    Args:
        columns: Columns and orders. Iteration order is kept in the rendered clause.
        defaults: Column names and orders appended to the clause if the column is not contained in `columns`.
    Returns:
        An `ORDER BY` clause.
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


def ranged_by(limit: int | None = None, offset: int | None = None) -> tuple[str, list[Any]]:
    """
    Generates a `LIMIT OFFSET` clause using the unified marker `$_`.

    Args:
        limit: Limit value. `None` means no limitation.
        offset: Offset value. `None` means `0`.
    Returns:
        A `LIMIT OFFSET` clause and its parameters.
    """
    clause, params = [], []

    if limit is not None:
        clause.append("LIMIT $_")
        params.append(limit)

    if offset is not None:
        clause.append("OFFSET $_")
        params.append(offset)

    return ' '.join(clause) if clause else '', params


def holders(length_or_keys: int | Sequence[HolderKeys], qualifier: Mapping[int, Qualifier] | None = None) -> str:
    """
    Generates a partial query string containing placeholder markers separated by commas.

    Args:
        length_or_keys: The number of placeholders, or a list of placeholder keys.
        qualifier: Qualifying function for each index.
    Returns:
        The generated query string.
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


def values(length_or_key_gen: int | Sequence[Callable[[int], HolderKeys]], rows: int, qualifier: Mapping[int, Qualifier] | None = None) -> str:
    """
    Generates a partial query string for the `VALUES` clause of an insertion query.

    Args:
        length_or_key_gen: The number of placeholders, or a list of functions that take a row index and return the key for each placeholder.
        rows: The number of rows to insert.
        qualifier: Qualifying function for each index.
    Returns:
        The generated query string.
    """
    if isinstance(length_or_key_gen, int):
        lok = lambda i: length_or_key_gen
    else:
        lok = lambda i: [g(i) for g in length_or_key_gen] # type: ignore

    return ', '.join([f"({holders(lok(i), qualifier)})" for i in range(rows)])


def _noop(x):
    return x