"""
This module exports types and functions used for `SELECT` queries.

Main purpose is providing a type which contains information of selecting columns,
i.e. which columns are selected and how they are rendered in the query.
Using the same instance of the type in both of query genration and reading results enables consistent reconstruction of model objects.

In most cases, classes of this module should not be used directly.
The use of `SelectMixin.select` and `read_row` is sufficient way to benefit from this module.
"""
from collections.abc import Iterator
from typing import Any, Union, TypeVar, Generic, Optional, Literal, Protocol, cast, overload
from typing_extensions import Self
from .model import Model, Column
from .query import Q, QueryableT


S = TypeVar('S')
M = TypeVar('M', bound=Model)


class AliasedColumn(QueryableT[Any]): # type: ignore
    """
    The representation of column and the alias of its belonging table.

    The instance of this class works as `Q` 's attribute as well.
    i.e. Condition on the column can be generated similarly to 'Q' via methods like `eq` .

    ```python
    >>> c = AliasedColumn("t", "col")
    >>> c.eq(3)
    Condition: 't.col = $_' -- [3]
    ```
    """
    def __init__(self, alias: str, column: Union[Column, str]) -> None:
        #: Alias string.
        self.alias = alias
        #: Column name or schema.
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
        Aliased column name. If alias is empty, column name is returns as it is.
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


class Aliased(Generic[M]):
    """
    A wrapper of a model type with an alias for it.
    """
    def __init__(self, alias: str, model: type[M]) -> None:
        #: Alias string.
        self.alias = alias
        #: Model type.
        self.model = model

    def __getattr__(self, key: str) -> AliasedColumn:
        try:
            col = next(filter(lambda c: c.name == key, self.model.columns))
            return AliasedColumn(self.alias, col)
        except StopIteration:
            raise KeyError(f"{key} is not a valid column name of {self.model.name}.")

    def select(self, includes: list[str] = [], excludes: list[str] = []) -> 'Selection[M]':
        """
        Creates a selection object containing selected columns in the model.

        Args:
            includes: Column names to select. All columns except specified in `excludes` are selected if empty.
            excludes: Column names not to select.
        Returns:
            Selection object.
        """
        columns = [c for c in self.model.columns if c.name not in excludes] \
            if not bool(includes) else \
                [c for c in self.model.columns if c.name not in excludes and c.name in includes]
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
    def name(self) -> Optional[str]: ...
    def consume(self, values: list[Any]) -> Any: ...


class StrConsumable(Consumable):
    def __init__(self, key: str) -> None:
        self.key = key

    def __eq__(self, other: object) -> bool:
        return isinstance(other, StrConsumable) and self.key == other.key

    def __len__(self) -> int:
        return 1

    @property
    def name(self) -> Optional[str]:
        return self.key

    def consume(self, values: list[Any]) -> Any:
        return values[0]


class EmptyConsumable(Consumable):
    def __eq__(self, other: object) -> bool:
        return isinstance(other, EmptyConsumable)

    def __len__(self) -> int:
        return 1

    @property
    def name(self) -> Optional[str]:
        return None

    def consume(self, values: list[Any]) -> Any:
        return values[0]


class Selection(Consumable, Generic[S]):
    """
    A representation of table and its columns used in query.

    This class is designed to be a bridge from query generation to reading results.
    String expression of the instance is comma-separated column names prepended with alias, which can be embedded in the select query.

    Due to `SelectMixin`, factory method is available on every model type.

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
        #: Model type.
        self.table = table
        #: An alias.
        self.alias = alias
        #: Columns to select.
        self.columns = columns

    @property
    def name(self) -> str:
        """
        Returns alias or name of the table.
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
            values: Values of row. The length must be equal to the number of columns in this.
        Returns:
            Model object where column values obtained from the row are set. 
        """
        return self.table(**dict([(c.name, v) for c, v in zip(self.columns, values)]))


class FieldExpressions:
    """
    The instance of this class works as the composition of selections.

    `+` operation on `Selection` s creates an instance of `FieldExpressions`. Each selection can be accessed via attributes of its name.
    Also, `FieldExpression` can be extended by `+=`.

    ```python
    >>> exp = table1.select("t1", includes=["col11", "col12"]) + table2.select("t2")
    >>> c.execute(f"SELECT {exp} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, *exp)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)
    ```

    Here, empty tuple and string are also available instead of `Selection` instance.
    They are replaced with index arguments (tuple) or keywords arguments (string) respectively by the invocation of the instance.

    ```python
    >>> exp = table1.select("t1", includes=["col11", "col12"]) + () + "a" + () + "b"
    >>> f"{exp("t2.col21", "t2.col23", a="t2.col22", b="t2.col24")}"
    t1.col11, t1.col12, t2.col21, t2.col22, t2.col23, t2.col24
    ```
    """
    def __init__(self):
        self.__selections: list[Consumable] = []
        self.__keys = {}

    def __add__(self, other) -> Self:
        exp = FieldExpressions()
        exp += self
        exp += other
        return exp

    def __iadd__(self, other) -> Self:
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
            raise ValueError(f"Operand of + for FieldExpressions must be a Selection or FieldExpressions but {type(other)} is given.")
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
    This class provides attribute access to each row in query result.

    Each instance returned by `read_row` behaves as if it is a list of consumed values of containing `Selection` s.
    Index access returns the value at the index and iteration yields values in order.

    ```python
    >>> exp = table1.select("t1"), table2.select()
    >>> r = read_row(row, *exp)
    >>> r[0]
    ...
    >>> [v for v in r]
    ...
    ```

    It also exposes attributes returns a `Selection` by its alias or table name.

    ```python
    >>> r.t1
    ...
    >>> r.table2
    ...
    ```

    Args:
        selections: List of selections which assign each value in row to a column.
    """
    def __init__(self, selections: list[Consumable]):
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
        Appends a value in the row.

        Args:
            value: A value in the row.
        """
        self._values.append(value)


def read_row(row, *selections: Union[Consumable, str, tuple], allow_redundancy: bool = False) -> RowValues:
    """
    Read values in a row according to given selections.

    This function returns `RowValues` where each value is created by each selection respectively.
    The type of the selection determines how values in the row are handled:

    - `Selection` consumes as many values as the number of columns in it and creates a model instance.
    - Empty tuple or a string consumes a value, which is stored in `RowValues` as it is.

    Args:
        selections: List of selections or their equivalents.
        allow_redundancy: If `False`, `ValueError` is thrown when not all values in a row are consumed.
    Returns:
        Values read from the row accoding to the selections.
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
    @overload
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: Literal[False] = False) -> FieldExpressions: ...
    @overload
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: Literal[True] = True) -> Selection[Self]: ...
    @classmethod
    def select(cls, alias: str = "", includes: list[str] = [], excludes: list[str] = [], single: bool = False):
        """
        Default mixin class of every model type providing method to generate `Selection` by Selecting columns with alias.

        Args:
            alias: An alias string of this table.
            includes: Column names to select. All columns except specified in `excludes` are selected if empty.
            excludes: Column names not to select.
        Returns:
            Selection object.
        """
        if single:
            return Aliased(alias, cast(type, cls)).select(includes, excludes)
        else:
            return FieldExpressions() + Aliased(alias, cast(type, cls)).select(includes, excludes)