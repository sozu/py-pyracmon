from collections.abc import Callable
from typing import Any, Union
from pyracmon.dbapi import Cursor
from pyracmon.select import Selection, FieldExpressions, RowValues, read_row
from pyracmon.graph import Graph


def add_all(cursor: Cursor, exp: FieldExpressions, graph: Graph, /, **assign: Union[Selection, Any]) -> Graph:
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
    prop_exp: dict[str, Callable[[RowValues], Any]] = {}
    for k, v in assign.items():
        if isinstance(v, Selection):
            if getattr(exp, v.name) is v:
                prop_exp[k] = lambda r: getattr(r, v.name)
            else:
                raise ValueError(f"Passed selection is not contained in passed FieldExpression.")
        else:
            prop_exp[k] = lambda r: v

    for row in cursor.fetchall():
        r = read_row(row, *exp)
        graph.append(**{k:f(r) for k, f in prop_exp.items()})

    return graph