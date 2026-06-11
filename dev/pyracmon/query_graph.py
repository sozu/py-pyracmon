from collections.abc import Iterable
from typing import Any
from pyracmon.dbapi import Cursor
from pyracmon.select import Selection, Consumable, RowValues, read_row
from pyracmon.graph import Graph


def append_rows(cursor: Cursor, exp: Iterable[Consumable | Any], graph: Graph, /, **assign: Selection | Any) -> Graph:
    """
    Adds all rows from the cursor into the graph.

    Values in `assign` are either a `Selection` or an arbitrary object.
    If a `Selection` is passed, the corresponding value in the row is selected.
    In that case, the `Selection` must be contained in `exp`, otherwise a `ValueError` is raised.

    ```python
    exp = ...
    c = db.stmt().execute(...)
    graph = append_rows(c, exp, new_graph(SomeGraph), a=exp.a, b=exp.b, c=0)
    ```

    Args:
        cursor: A cursor obtained from executing the query.
        exp: Expressions used in the query.
        graph: The graph that rows are appended to.
        assign: A mapping from a graph property name to a `Selection` or an arbitrary value.
    Returns:
        The same graph instance that was passed in.
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