"""
This module exports types and functions used for `SELECT` queries.

In most cases, classes of this module should not be used directly.
The use of `SelectMixin.select` and `read_row` is a sufficient way to benefit from this module.
"""
from collections.abc import Iterator
from typing import Any, Literal, cast, overload
from typing_extensions import Self
from .model import Model, Column
from .query import Q, QueryableT


class AliasedColumn(QueryableT[Any]): # type: ignore
    """
    A representation of a column and the alias of the table it belongs to.

    An instance of this class also works as an attribute of `Q`.
    i.e., a condition on the column can be generated similarly to `Q`, via methods like `eq`.

    ```python
    >>> c = AliasedColumn("t", "col")
    >>> c.eq(3)
    Condition: 't.col = $_' -- [3]
    ```
    """
    def __init__(self, alias: str, column: Column | str) -> None:
        #: The alias string.
        self.alias = alias
        #: The column name or schema.
        self.column = column

    def __hash__(self) -> int:
        return hash(self.alias) + hash(self.column)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AliasedColumn) and self.alias == other.alias and self.column is other.column

    def __str__(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        """
        The aliased column name. If the alias is empty, the column name is returned as is.
        """
        if isinstance(self.column, Column):
            return f"{self.alias}.{self.column.name}" if self.alias else self.column.name
        else:
            return f"{self.alias}.{self.column}" if self.alias else self.column

    def __getattr__(self, key):
        method = getattr(Q, key)
        def invoke(value, *args, **kwargs):
            kwargs.update({self.name: value})
            return method(*args, **kwargs)
        return invoke


class Aliased[M: Model]:
    """
    A wrapper of a model type with an alias for it.
    """
    def __init__(self, alias: str, model: type[M]) -> None:
        #: The alias string.
        self.alias = alias
        #: The model type.
        self.model = model

    def __getattr__(self, key: str) -> AliasedColumn:
        """
        Returns an aliased column for the given column name.

        Args:
            key: The name of the column.
        Returns:
            An `AliasedColumn` instance.
        Raises:
            KeyError: If the column name is not valid for the model.
        """
        try:
            col = next(filter(lambda c: c.name == key, self.model.columns))
            return AliasedColumn(self.alias, col)
        except StopIteration:
            raise KeyError(f"{key} is not a valid column name of {self.model.name}.")

    def select(self, includes: list[str] = [], excludes: list[str] = []) -> 'Selection[M]':
        """
        Creates a selection object containing the selected columns of the model.

        Args:
            includes: The column names to select. If empty, all columns except those specified in `excludes` are selected.
            excludes: The column names not to select.
        Returns:
            A `Selection` object.
        """
        columns = (
            [c for c in self.model.columns if c.name not in excludes]
            if not bool(includes) else
            [c for c in self.model.columns if c.name not in excludes and c.name in includes]
        )
        return cast(Selection[M], Selection(self.model, self.alias, columns))


class Consumable:
    @staticmethod
    def to_consumable(value: Any) -> 'Consumable':
        if isinstance(value, Consumable):
            return value
        elif isinstance(value, str):
            return StrConsumable(value)
        else:
            return EmptyConsumable()

    def __len__(self) -> int: ...
    @property
    def name(self) -> str | None: ...
    def consume(self, values: list[Any]) -> Any: ...


class StrConsumable(Consumable):
    def __init__(self, key: str) -> None:
        self.key = key

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StrConsumable) and self.key == other.key

    def __len__(self) -> int:
        return 1

    @property
    def name(self) -> str | None:
        return self.key

    def consume(self, values: list[Any]) -> Any:
        return values[0]


class EmptyConsumable(Consumable):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, EmptyConsumable)

    def __len__(self) -> int:
        return 1

    @property
    def name(self) -> str | None:
        return None

    def consume(self, values: list[Any]) -> Any:
        return values[0]


class Selection[S](Consumable):
    """
    A representation of a table and its columns, used in a query.

    This class is designed to be a bridge from query generation to reading results.
    The string expression of the instance is a comma-separated list of column names prepended with the alias, which can be embedded in the SELECT query.

    Due to `SelectMixin`, a factory method is available on every model type.

    ```python
    >>> s1 = table1.select("t1", includes = ["col11", "col12"])
    >>> s2 = table2.select("t2")
    >>> str(s1)
    't1.col11, t1.col12'
    >>> str(s2)
    't2.col21, t2.col22, t2.col23'
    ```

    The instances of this class are also used in `read_row` to reconstruct model objects from each obtained row.

    ```python
    >>> c.execute(f"SELECT {s1}, {s2} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, s1, s2)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)
    ```
    """
    def __init__(self, table: type[S], alias: str, columns: list[Column]):
        #: The model type.
        self.table = table
        #: An alias.
        self.alias = alias
        #: The columns to select.
        self.columns = columns

    @property
    def name(self) -> str:
        """
        The alias, if set; otherwise the name of the table.
        """
        return self.alias if self.alias else cast(type[Model], self.table).name

    def __len__(self) -> int:
        return len(self.columns)

    def __repr__(self) -> str:
        a = f"{self.alias}." if self.alias else ""
        return ', '.join([f"{a}{c.name}" for c in self.columns])

    def __add__(self, other) -> 'FieldExpressions':
        return FieldExpressions() + self + other

    def __iter__(self):
        return iter([self])

    def __getattr__(self, key) -> AliasedColumn:
        try:
            return AliasedColumn(self.alias, next(filter(lambda c: c.name == key, self.columns)))
        except StopIteration:
            raise KeyError(f"{key} is not found from selected columns.")

    def consume(self, values: list[Any]) -> S:
        """
        Construct a model object from a row.

        Args:
            values: The values from a row. The length must equal the number of columns in this selection.
        Returns:
            A model object with the column values from the row set on it.
        """
        return self.table(**dict([(c.name, v) for c, v in zip(self.columns, values)]))


class FieldExpressions:
    """
    The composition of `Selection` objects, used in both query construction and reading results.

    A `FieldExpressions` instance is created by adding (`+`) `Selection` objects.
    It can be embedded in the SELECT query directly.

    ```python
    >>> exp: FieldExpressions = table1.select("t1", includes=["col11", "col12"]) + table2.select("t2")
    >>> c.execute(f"SELECT {exp} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    ```

    Also, an empty tuple (`()`) and a string can be added as well.
    These items are replaced, respectively, with the positional and keyword arguments used to invoke the instance.
    An arbitrary string can be used for each argument, and it is embedded in the query as is.

    ```python
    >>> exp = table1.select("t1", includes=["col11", "col12"]) + () + "a" + () + "b"
    >>> f"{exp("t2.col21", "t2.col23", a="t2.col22", b="t2.col24")}"
    t1.col11, t1.col12, t2.col21, t2.col22, t2.col23, t2.col24
    ```

    The rows obtained by the query can be read according to the internal `Selection` objects, by passing the instance to `read_row`,
    which returns a `RowValues` instance that exposes the values corresponding to the `Selection` objects as attributes named after them.

    ```python
    >>> exp = table1.select("t1", includes=["col11", "col12"]) + table2.select("t2") + "now"
    >>> c.execute(f"SELECT {exp(now='now()')} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r: RowValues = read_row(row, *exp)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)
    >>>     assert isinstance(r.now, datetime)
    ```

    The example above shows the usage of `FieldExpressions` in both query construction and reading results.
    Because it knows the structure of the selected rows, it can reconstruct model objects by consuming as many values as needed.
    Like `Selection` objects, items added as strings are also available via the attributes, and their types are determined by how the DB driver returns them.
    """
    def __init__(self):
        self.__selections: list[Consumable] = []
        self.__keys = {}

    def __add__(self, other) -> Self:
        exp = FieldExpressions()
        exp += self
        exp += other
        return exp

    def __iadd__(self, other: 'Selection | FieldExpressions | str | tuple') -> Self:
        if isinstance(other, Selection):
            self.__selections.append(other)
            self.__keys[other.name] = other
        elif isinstance(other, FieldExpressions):
            self.__selections += other.__selections
            self.__keys.update(other.__keys)
        elif isinstance(other, str):
            cons = StrConsumable(other)
            self.__selections.append(cons)
            self.__keys[other] = cons
        elif other == ():
            self.__selections.append(EmptyConsumable())
        else:
            raise ValueError(f"Operand of + for FieldExpressions must be a Selection, FieldExpressions, str, or tuple but {type(other)} is given.")
        return self

    def __getitem__(self, index: int) -> Consumable:
        return self.__selections[index]

    def __getattr__(self, key: str) -> Consumable:
        return self.__keys[key]

    def __iter__(self) -> Iterator[Consumable]:
        return iter(self.__selections)

    class Instance:
        def __init__(self, exp: 'FieldExpressions', *args, **kwargs):
            self.exp = exp
            self.args = args
            self.kwargs = kwargs

        def __repr__(self):
            args = list(self.args)
            def _repr(s: Consumable) -> str:
                if isinstance(s, Selection):
                    return s.__repr__()
                elif isinstance(s, StrConsumable):
                    return self.kwargs.get(s.key, s.key)
                elif isinstance(s, EmptyConsumable):
                    return args.pop(0)
                else:
                    raise ValueError(f"Unexpected expression type: {s}")
            return ', '.join(map(_repr, self.exp))

    def __call__(self, *args, **kwargs):
        return FieldExpressions.Instance(self, *args, **kwargs)

    def __repr__(self):
        return self().__repr__()


class RowValues:
    """
    Holds the values of a row in a query result and provides attribute access to reconstructed values.

    A `RowValues` instance basically works as a list of the values in a row, and provides basic list operations like iteration and indexing.
    Additionally, its attributes are reconstructed values corresponding to the `Selection` objects.
    See `FieldExpressions` for details about the feature.
    """
    def __init__(self, selections: list[Consumable]):
        """
        Initializes the instance with the given selections.

        Args:
            selections: A list of selections used to construct the query.
        """
        self._key_map = dict([(s.name, i) for i, s in enumerate(selections) if s.name is not None])
        self._values = []

    def __len__(self):
        return len(self._values)

    def __iter__(self):
        return iter(self._values)

    def __getitem__(self, index):
        return self._values[index]

    def __getattr__(self, key) -> Any:
        index = self._key_map.get(key, None)
        if index is None:
            raise AttributeError(f"No selection is found whose table name or alias is '{key}'")
        return self._values[index]

    def append(self, value: Any):
        """
        Appends a value to the row.

        Args:
            value: The value to append.
        """
        self._values.append(value)


def read_row(row, *selections: Consumable | str | tuple, allow_redundancy: bool = False) -> RowValues:
    """
    Read the values from a row according to the given selections.

    This function returns a `RowValues` instance, in which each value is created by the corresponding selection.
    The type of the selection determines how the values in the row are handled:

    - A `Selection` consumes as many values as the number of columns in it, and creates a model instance.
    - An empty tuple or a string consumes a value, which is stored in the result as is.

    Args:
        row: A row of the query result.
        selections: A list of selections or their equivalents.
        allow_redundancy: If `False`, a `ValueError` is raised when not all values in the row are consumed.
    Returns:
        The values read from the row according to the selections.
    """
    consumables = [Consumable.to_consumable(s) for s in selections]

    result = RowValues(consumables)

    for s in consumables:
        result.append(s.consume(row))
        row = row[len(s):]

    if not allow_redundancy and len(row) > 0:
        raise ValueError("Not all elements in row is consumed.")

    return result


class SelectMixin:
    """
    A mixin class that provides every model type with a method to select its columns.
    """
    @overload
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: Literal[False] = False) -> FieldExpressions: ...
    @overload
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: Literal[True] = True) -> Selection[Self]: ...
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: bool = False):
        """
        Generates a `Selection` for the columns of this model type, with the given alias.

        Args:
            alias: An alias string of this table.
            includes: The column names to select. If empty, all columns except those specified in `excludes` are selected.
            excludes: The column names not to select.
            single: If `True`, only a `Selection` of this table is returned. Otherwise, the `Selection` is wrapped in `FieldExpressions`.
        Returns:
            A `FieldExpressions` instance, or a single `Selection`.
        """
        if single:
            return Aliased(alias, cast(type, cls)).select(includes, excludes)
        else:
            return FieldExpressions() + Aliased(alias, cast(type, cls)).select(includes, excludes)