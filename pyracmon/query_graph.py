from collections.abc import Callable, Iterable
from typing import Any, Union
from pyracmon.dbapi import Cursor
from pyracmon.select import Selection, Consumable, RowValues, read_row
from pyracmon.graph import Graph


def append_rows(cursor: Cursor, exp: Iterable[Union[Consumable, Any]], graph: Graph, /, **assign: Union[Selection, Any]) -> Graph:
    """
    Adds all rows in cursor into the graph.

    Values in `assign` are `Selection` or any kind of objects.
    If `Selection` is passed, the corresponding value in row is selected.
    In this case, the `Selection` must be contained in `exp` , otherwise `ValueError` is raised.

    ```python
    exp = ...
    c = db.stmt().execute(...)
    graph = add_all(c, exp, new_graph(SomeGraph), a=exp.a, b=exp.b, c=0)
    ```

    Args:
        cursor: Cursor obtained by query.
        exp: Expressions used in the query.
        graph: Graph to append rows.
        assign: Mapping from graph property name to `Selection` or arbitrary value.
    Returns:
        The same graph as passed one. 
    """
    def get(r: RowValues, k: str) -> Any:
        v = assign[k]
        if isinstance(v, Consumable):
            return getattr(r, v.name or "")
        else:
            return v

    for row in cursor.fetchall():
        r = read_row(row, *exp)
        graph.append(**{k:get(r, k) for k in assign.keys()})

    return graph