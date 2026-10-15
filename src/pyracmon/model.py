from collections.abc import Iterator, Callable
from typing import Any, Callable, Protocol, Self, get_origin, get_args, cast, TYPE_CHECKING, dataclass_transform
from .util import PKS


# ----------------------------------------------------------------
# Pseudo types used only for type hinting.
# ----------------------------------------------------------------
class Mixins[*Mxs]:
    """
    A container for mixin types, used to pass them to `define_model` as a typed argument.
    """
    pass


class ModelColumns:
    """
    A list of columns used as an attribute of model types.

    This class provides both sequence-like access to the columns and attribute access by column name.
    The iteration order matches the declared order of the columns in the table schema.

    >>> class AModel: # model type
    >>>    ...
    >>>
    >>> first_column: Column = AModel.columns[0]
    >>> column_by_name: Column = AModel.columns.c1
    """

    def __init__(self, columns: list['Column']) -> None:
        self.__columns = columns

    def __iter__(self) -> Iterator['Column']:
        """
        Iterates over the columns in declared order.

        Returns:
            An iterator that yields the columns of the model type.
        """
        return iter(self.__columns)

    def __getitem__(self, index: int) -> 'Column':
        """
        Gets a column by index.

        Args:
            index: The index of the column.
        Returns:
            The column at the specified index.
        """
        return self.__columns[index]

    def __getattr__(self, key: str) -> 'Column':
        """
        Gets a column by name.

        Args:
            key: The name of the column.
        Returns:
            The column with the specified name.
        Raises:
            AttributeError: If the column with the specified name does not exist.
        """
        if c := next(filter(lambda c: c.name == key, self.__columns), None):
            return c
        else:
            raise AttributeError(f"{key} is not a column of the model.")


if TYPE_CHECKING:
    @dataclass_transform(kw_only_default=True)
    class Meta(type):
        #: The table name.
        name: str
        #: The `Table` object.
        table: 'Table'
        #: A list of `Column` objects.
        columns: ModelColumns

        def __matmul__[M](self: type[M], cond: Any) -> Callable[[M | None], bool]:
            """
            Returns a function that checks whether a model instance matches the given condition.

            When `cond` is a dictionary,
            the returned function checks whether the model instance has attributes with the same names and values as those in the dictionary.
            When `cond` is a tuple or any other type,
            the returned function checks whether the model instance has primary key values that match those in `cond`.

            Args:
                cond: A dictionary of attribute names and values, or a tuple of primary key values, or a single primary key value.
            Returns:
                A function that takes a model instance and returns `True` if it matches the condition.
            """
            ...

    class IModel(Protocol, metaclass=Meta):
        def __init__(self, **kwargs) -> None: ...  # for typing
        def __getitem__(self, key: str) -> Any: ...
        def __contains__(self, key) -> bool: ...
        def __iter__(self) -> Iterator[tuple['Column', Any]]: ...

    class Model(IModel):
        """
        The base type of model types.

        This class only works as a marker for model types and provides no functionality of its own.
        """

        def __init__(self, **kwargs) -> None: ...  # for typing

        def __getitem__(self, key: str | Column) -> Any:
            """
            Gets a column value by column name or column schema.

            Args:
                key: A column name or column schema.
            Returns:
                The column value.
            Raises:
                ValueError: If `key` is a column schema that is not a column of the model type.
                AttributeError: If the column value is not set on the model instance.
            """
            ...

        def __contains__(self, key: str | Column) -> bool:
            """
            Checks whether a column value is set on the model instance.

            Args:
                key: A column name or column schema.
            Returns:
                `True` if the column value is set on the model instance, otherwise `False`.
            Raises:
                ValueError: If `key` is a column schema that is not a column of the model type.
            """
            ...

        def __iter__(self) -> Iterator[tuple[Column, Any]]:
            """
            Iterates over the columns and their values in the model instance.

            Returns:
                An iterator that yields pairs of a column schema and its value.
            """
            ...
else:
    class Meta:
        def __matmul__(self, cond: Any) -> Callable[[Any | None], bool]:
            def check(value: Any | None) -> bool:
                if value is None:
                    return False
                elif isinstance(cond, dict):
                    return all(getattr(value, n, ...) == v for n, v in cond.items())
                else:
                    pks = extract_pks(self, value)
                    if isinstance(cond, tuple):
                        return len(pks) == len(cond) and all(v == c for v, c in zip(pks.values(), cond))
                    else:
                        return len(pks) == 1 and next(iter(pks.values())) == cond
            return check

    class IModel(Protocol):
        pass

    class Model:
        pass
# ----------------------------------------------------------------
#: A model object or `dict` corresponding to a record.
Record = IModel | dict[str, Any]


class ForeignKey:
    """
    A foreign key constraint describing a relationship between two columns.
    """
    def __init__(self, table: 'Table | str', column: 'str | Column') -> None:
        #: The referenced table model, or the table name if the table is not modeled.
        self.table = table
        #: The referenced column model, or the column name if the column is not modeled.
        self.column = column


class Relations:
    """
    The foreign key constraints on a column.
    """
    def __init__(self, constraints: list[ForeignKey] | None = None) -> None:
        #: The foreign key constraints on the column.
        self.constraints: list[ForeignKey] = constraints or []

    def add(self, fk: ForeignKey):
        """
        Adds a constraint.

        Args:
            fk: The foreign key constraint to add.
        """
        self.constraints.append(fk)


class Column:
    """
    A model of a column.

    The data types of `type_info` and `incremental` depend on the DBMS and its driver. See the `dialect` package for details.
    """
    def __init__(
        self,
        name: str,
        ptype: type,
        type_info: Any | None,
        pk: bool,
        fk: Relations | None,
        incremental: Any | None,
        nullable: bool,
        comment: str = "",
    ):
        #: The column name.
        self.name = name
        #: The data type in Python.
        self.ptype = ptype
        #: Type information obtained from the DB.
        self.type_info = type_info
        #: Is this column a primary key?
        self.pk = pk
        #: Foreign key constraints on this column, or `None` if there are none.
        self.fk = fk
        #: If this column is auto-incremental, this attribute holds information about that, otherwise it is `None`.
        self.incremental = incremental
        #: Can this column contain `NULL`?
        self.nullable = nullable
        #: The column's comment.
        self.comment = comment


class Table:
    """
    A model of a table.
    """
    def __init__(self, name: str, columns: list[Column], comment: str = ""):
        #: The table name.
        self.name = name
        #: The columns in the table.
        self.columns = columns
        #: The table's comment.
        self.comment = comment

    def find(self, name: str) -> Column | None:
        """
        Finds a column by name.

        Args:
            name: The column name.
        Returns:
            The column if it exists, otherwise `None`.
        """
        return next(filter(lambda c: c.name == name, self.columns), None)


def define_model[M: IModel, MXT: Mixins](table_: Table, mixins: type[MXT] | list[type] | None = None, model_type: type[M] | None = Model) -> type[M]:
    """
    Creates a model type representing a table.

    The model type inherits all types in `mixins`, in order.
    When the same attribute is defined in multiple mixin types, the former overrides the latter.

    Every model type has the following class attributes:

    |name|type|description|
    |:---|:---|:---|
    |name|`str`|The name of the table.|
    |table|`Table`|The table schema.|
    |columns|`ModelColumns`|A list of column schemas.|

    ```python
    >>> # CREATE TABLE t1 (col1 int, col2 text, col3 text not null);
    >>> t1 = Table("t1", ...)
    >>> T1 = define_model(t1)
    >>> T1.name
    't1'
    ```

    The model type has a dataclass-like constructor that accepts keyword arguments corresponding to column names and their values.
    Unlike a dataclass, the constructor does not require all of the columns.
    Omitted columns are excluded from queries used by operations such as `CRUDMixin.insert`.
    As a result, the operation fails at runtime if such a column is required by the table schema, e.g., due to a `NOT NULL` constraint.

    ```python
    >>> m1 = T1(col1=1, col2="a") # OK, col3 is not set as an attribute of `m1`.
    >>> m1.insert(db) # NG, the insertion query does not contain col3 and no default value is set.
    ```

    Attributes are also assignable via the normal attribute setter. If the attribute name is not a valid column name, a `TypeError` is raised.

    ```python
    >>> m1.col3 = "b"
    ```

    A model instance supports iteration, which yields pairs of a column schema and its value.

    ```python
    >>> for c, v in m1:
    >>>     print(f"{c.name} = {v}")
    col1 = 1
    col2 = a
    col3 = b
    ```

    Args:
        table_: The table schema.
        mixins: Mixin types that provide class methods to the model type.
        model_type: Used only for type hinting, to determine the returned model type.
    Returns:
        The created model type.
    """
    column_names = {c.name for c in table_.columns}

    class _Meta(Meta, type):
        name = table_.name
        table = table_
        columns = ModelColumns(table_.columns)

    class Base(Model, metaclass=_Meta):
        pass

    mixin_types: list[type] = []

    if isinstance(mixins, list):
        mixin_types = mixins
    elif get_origin(mixins) is not None:
        mixin_types = cast(list[type], list(get_args(mixins)))
    elif mixins is not None:
        raise ValueError(f"Model mixin types should be specified by Mixins or a list of types.")

    def check_column(c: Column) -> bool:
        return c in table_.columns

    class _Model(type("ModelBase", tuple([Base] + mixin_types), {})):
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def __repr__(self):
            cls = cast(type[Base], type(self))
            return f"{cls.name}({', '.join([f'{c.name}={repr(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

        def __str__(self):
            cls = cast(type[Base], type(self))
            return f"{cls.name}({', '.join([f'{c.name}={str(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

        def __iter__(self) -> Iterator[tuple[Column, Any]]:
            cls = cast(type[Base], type(self))
            return map(lambda c: (c, getattr(self, c.name)), filter(lambda c: hasattr(self, c.name), cls.columns))

        def __setattr__(self, key, value):
            cls = cast(type[Base], type(self))
            if key not in column_names:
                raise TypeError(f"{key} is not a column of {cls.name}")
            object.__setattr__(self, key, value)

        def __getitem__(self, key: str | Column):
            if isinstance(key, Column):
                if not check_column(key):
                    raise ValueError(f"{key.name} is not a column of {table_.name}")
                key = key.name
            return getattr(self, key)

        def __contains__(self, key: str | Column):
            if isinstance(key, Column):
                if not check_column(key):
                    return False
                key = key.name
            return hasattr(self, key)

        def __eq__(self, other):
            cls = type(self)
            if cls != type(other):
                return False
            for k in column_names:
                if hasattr(self, k) ^ hasattr(other, k):
                    return False
                if getattr(self, k, None) != getattr(other, k, None):
                    return False
            return True

    return cast(type[M], _Model)


def parse_pks(model: Meta, pks: PKS) -> tuple[list[str], list[Any]]:
    """
    Parse polymorphic primary key values into a pair of column names and their values.

    Args:
        model: The model type.
        pks: The primary key value(s), in polymorphic form.
    Returns:
        A pair of column names and their values.
    Raises:
        ValueError: If `pks` is not a valid primary key value for the model.
    """
    if isinstance(pks, dict):
        ordered = check_columns(model, pks, lambda c: c.pk, True)
        return [v[0] for v in ordered], [v[1] for v in ordered]
    else:
        cols = [c.name for c in model.columns if c.pk]
        if len(cols) != 1:
            raise ValueError(f"The number of primary key columns in {model.name} is not 1.")
        return ([cols[0]], [pks])


def extract_pks(model: Meta, record: Record) -> dict[str, Any]:
    """
    Extract primary key values from a record.

    Args:
        model: The model type.
        record: The record to extract primary key values from.
    Returns:
        The primary key values as a dictionary..
    Raises:
        ValueError: If some primary key values are not contained in the given record.
    """
    pk_columns = [c.name for c in model.columns if c.pk]
    if isinstance(record, dict):
        pks = dict((c, record[c]) for c in pk_columns if c in record)
    else:
        pks = dict((c, getattr(record, c)) for c in pk_columns if hasattr(record, c))
    if len(pks) != len(pk_columns):
        missing = set(pk_columns) - set(pks.keys())
        raise ValueError(f"Some primary keys are not contained in passed record: {missing}")
    return pks


def check_columns(
    model: Meta,
    col_map: dict[str, Any],
    condition: Callable[[Column], bool] = lambda c: True,
    requires_all: bool = False,
) -> list[tuple[str, Any]]:
    """
    Checks that a set of names matches the selected column names of a model.

    Args:
        model: The model type.
        col_map: A dictionary whose keys are column names.
        condition: A function that selects columns from the model.
        requires_all: If `True`, the keys of `col_map` must exactly match the selected column names.
            If `False`, they need only be a subset of them.
    Returns:
        A list of `(name, value)` pairs from `col_map`, ordered according to the column declarations.
    Raises:
        ValueError: If the keys of `col_map` are not a subset of the selected column names, or if `requires_all`
            is `True` and they do not match exactly.
    """
    names = [c.name for c in model.columns if condition(c)]
    name_set = set(names)
    targets = set(col_map.keys())
    if not name_set >= targets:
        raise ValueError(f"Columns {targets - name_set} are not specified columns of '{model.name}'.")
    if requires_all and not name_set == targets:
        raise ValueError(f"Required columns {name_set - targets} in '{model.name}' are not found.")
    return [(n, col_map[n]) for n in names if n in col_map]


def model_values(model: Meta, record: Record, excludes_pk: bool = False) -> dict[str, Any]:
    """
    Generates a dictionary of column names and values from a record.

    When `record` contains extra keys that are not column names of the model,
    those keys are ignored and excluded from the returned dictionary.

    Args:
        model: The model type.
        record: The record to extract values from.
        excludes_pk: If `True`, the values of PK columns are excluded from the returned dictionary.
    Returns:
        A dictionary mapping column names to their values.
    """
    if isinstance(record, dict):
        includes = {c.name for c in model.columns if (not excludes_pk) or (not c.pk)}
        return {k: v for k, v in record.items() if k in includes}
    elif isinstance(record, model):
        return {cv[0].name: cv[1] for cv in record if (not excludes_pk) or (not cv[0].pk)}
    else:
        raise TypeError(f"Given record should be a dict or a model instance, but got {type(record)}.")
