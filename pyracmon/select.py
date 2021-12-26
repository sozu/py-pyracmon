"""
This module exports types and functions used for `SELECT` queries.

Main purpose is providing a type which contains information of selecting columns,
i.e. which columns are selected and how they are expressed in the query.
Using the same instance of the type in both of query genration and reading results enables consistent reconstruction of model objects.

In most cases, classes of this module should not be used directly.
The use of `SelectMixin.select` and `read_row` is sufficient way to benefit from this module.
"""
from typing import *
from .model import Model


class Selection:
    """
    A representation of table and its columns used in query.

    This class is designed to be a bridge from query generation to reading results.
    String expression of the instance is comma-separated column names prepended with alias, which can be embedded in the select query.

    Due to `SelectMixin`, factory method is available on every model type.

    >>> s1 = table1.select("t1", includes = ["col11", "col12"])
    >>> s2 = table2.select("t2")
    >>> str(s1)
    't1.col11, t1.col12'
    >>> str(s2)
    't2.col21, t2.col22, t2.col23'

    The instances of this class are used in `read_row` to reconstruct model objects from each obtained row.

    >>> c.execute(f"SELECT {s1}, {s2} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, s1, s2)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)

    Args:
        table: Model type.
        alias: An alias string of this table.
    Returns:
        columns: Names of columns to select.
    """
    def __init__(self, table: Model, alias: str, columns: List[str]):
        self.table = table
        self.alias = alias
        self.columns = columns

    @property
    def name(self) -> str:
        """
        Returns alias or name of the table.
        """
        return self.alias if self.alias else self.table.name

    def __len__(self):
        return len(self.columns)

    def __repr__(self):
        a = f"{self.alias}." if self.alias else ""
        return ', '.join([f"{a}{c.name}" for c in self.columns])

    def __add__(self, other):
        return FieldExpressions() + self + other

    def __iter__(self):
        return iter([self])

    def consume(self, values: List[Any]) -> 'Model':
        """
        Construct a model object from a row.

        Args:
            values: Values of row. The length must be equal to the number of columns in this.
        Returns:
            Model object where column values obtained from the row are set. 
        """
        return self.table(**dict([(c.name, v) for c, v in zip(self.columns, values)]))


class FieldExpressions:
    """
    The instance of this class works as the composition of selections.

    `+` operation on `Selection` s creates an instance of `FieldExpressions`. Each selection is available via attributes of its name.
    Also, `FieldExpression` can be extended by `+=`.

    >>> exp = table1.select("t1", includes=["col11", "col12"]) + table2.select("t2")
    >>> c.execute(f"SELECT {exp} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, *exp)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)

    Here, selection means not only `Selection` instance but empty tuple and string.
    They are replaced with index arguments (tuple) or keywords arguments (string) each other by the direct invocation of the instance.

    >>> exp = table1.select("t1", includes=["col11", "col12"]) + () + "a" + () + "b"
    >>> f"{exp("t2.col21", "t2.col23", a="t2.col22", b="t2.col24")}"
    t1.col11, t1.col12, t2.col21, t2.col22, t2.col23, t2.col24
    """
    def __init__(self):
        self.__selections = []
        self.__keys = {}

    def __add__(self, other):
        exp = FieldExpressions()
        exp += self
        exp += other
        return exp

    def __iadd__(self, other):
        if isinstance(other, Selection):
            self.__selections.append(other)
            self.__keys[other.name] = other
        elif isinstance(other, FieldExpressions):
            self.__selections += other.__selections
            self.__keys.update(other.__keys)
        elif isinstance(other, str):
            self.__selections.append(other)
            self.__keys[other] = other
        elif other == ():
            self.__selections.append(other)
        else:
            raise ValueError(f"Operand of + for FieldExpressions must be a Selection or FieldExpressions but {type(other)} is given.")
        return self

    def __getattr__(self, key):
        return self.__keys[key]

    def __iter__(self):
        return iter(self.__selections)

    class Instance:
        def __init__(self, exp, *args, **kwargs):
            self.exp = exp
            self.args = args
            self.kwargs = kwargs

        def __repr__(self):
            args = list(self.args)
            def _repr(s):
                if isinstance(s, Selection):
                    return s.__repr__()
                elif isinstance(s, str):
                    return self.kwargs.get(s, s)
                elif s == ():
                    return args.pop(0)
            return ', '.join(map(_repr, self.exp))

    def __call__(self, *args, **kwargs):
        return FieldExpressions.Instance(self, *args, **kwargs)

    def __repr__(self):
        return self().__repr__()


class RowValues:
    """
    This class provides attribute access to each row in query result.

    Each instance behaves as if it is a list of values created by holding `Selection` s.
    Index access returns the value at the index and iteration yields values in order.

    >>> exp = table1.select("t1"), table2.select()
    >>> r = read_row(row, *exp)
    >>> r[0]
    ...
    >>> [v for v in r]
    ...

    It also exposes attributes whose name is the alias (if exists) or the table name.

    >>> r.t1
    ...
    >>> r.table2
    ...

    Args:
        selections: List of selections which assign each value in row to a column.
    """
    def __init__(self, selections: List[Union[Selection, str, Tuple[()]]]):
        self.key_map = dict([(s, i) for i, s in enumerate(map(self._key_of, selections)) if s is not None])
        self.values = []

    def _key_of(self, s):
        if isinstance(s, Selection):
            return s.name
        elif isinstance(s, str):
            return s
        else:
            return None

    def __len__(self):
        return len(self.values)

    def __iter__(self):
        return iter(self.values)

    def __getitem__(self, index):
        return self.values[index]

    def __getattr__(self, key):
        index = self.key_map.get(key, None)
        if index is None:
            raise AttributeError(f"No selection is found whose table name or alias is '{key}'")
        return self.values[index]

    def append(self, value: Any):
        """
        Appends a value in the row.

        Args:
            value: A value in the row.
        """
        self.values.append(value)


def read_row(row, *selections: Union[Selection, str, Tuple[()]], allow_redundancy: bool = False) -> RowValues:
    """
    Read values in a row according to given selections.

    This function returns `RowValues` where each value is created by each selection respectively.
    The type of the selection determines how values in the row are handled:

    - `Selection` consumes as many values as the number of columns in it and creates a model instance.
    - Empty tuple or a string consumes a value, which is stored in `RowValues` as it is.

    Args:
        selections: List of selections.
        allow_redundancy: If `False`, `ValueError` is thrown when not all values in a row are consumed.
    Returns:
        Values read from the row accoding to the selections.
    """
    result = RowValues(selections)

    for s in selections:
        if isinstance(s, Selection):
            result.append(s.consume(row))
            row = row[len(s):]
        elif callable(s):
            result.append(s(row[0]))
            row = row[1:]
        elif s == () or isinstance(s, str):
            result.append(row[0])
            row = row[1:]
        else:
            raise ValueError("Unavailable value is given to read_row().")

    if not allow_redundancy and len(row) > 0:
        raise ValueError("Not all elements in row is consumed.")

    return result


class SelectMixin:
    @classmethod
    def select(cls, alias: str = "", includes: List[str] = [], excludes: List[str] = []) -> Selection:
        """
        Default mixin class of every model type providing method to generate `Selection` by Selecting columns with alias.

        Args:
            alias: An alias string of this table.
            includes: Column names to use. All columns are selected if empty.
            excludes: Column names not to use.
        Returns:
            Selection object.
        """
        columns = [c for c in cls.columns if c.name not in excludes] \
            if not bool(includes) else \
                [c for c in cls.columns if c.name not in excludes and c.name in includes]
        return Selection(cls, alias, columns)
