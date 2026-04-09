from collections import OrderedDict
from collections.abc import Iterator, Sequence
from typing import Any, Union, Optional, Callable, Generic, TypeVar, get_origin, get_args, cast, TYPE_CHECKING
from typing_extensions import TypeVarTuple, Unpack, Self, dataclass_transform
from .util import PKS


#----------------------------------------------------------------
# Pseudo types used only for type hinting.
#----------------------------------------------------------------
M = TypeVar('M')
MXS = TypeVarTuple('MXS')
COLUMN = NotImplemented


class Mixins(Generic[Unpack[MXS]]):
    pass
MXT = TypeVar('MXT', bound=Mixins)


if TYPE_CHECKING:
    @dataclass_transform(kw_only_default=True)
    class Meta(type):
        #: Table name.
        name: str
        #: `Table` object.
        table: 'Table'
        #: A list of `Column` s.
        columns: list['Column']
        #: An object exposing `Column` object via the attribute of its name.
        column: Any

        def __iter__(self) -> Iterator[tuple['Column', Any]]: ...
        def __getitem__(self, key: str) -> Any: ...
        def __contains__(self, key) -> bool: ...

        @classmethod
        def shrink(cls: type[M], excludes, includes=None) -> M: ...


    class Model(Mixins[Unpack[MXS]], metaclass=Meta):
        """
        Base type of model types.

        This class only works as a marker of model types and gives no functionalities to them.
        """
        def __init__(self, **kwargs) -> None: ... # for typing
else:
    class Meta:
        pass
    class Model:
        pass
#----------------------------------------------------------------
Record = Union[Meta, dict[str, Any]]
"""Model object or dict corresponding to a table row."""


class ForeignKey:
    """
    This class represents a foreign key constraint.
    """
    def __init__(self, table: Union['Table', str], column: Union[str, 'Column']) -> None:
        #: Referenced table model, table name is set alternatively when the table is not modelled.
        self.table = table
        #: Referenced column model, column name is set alternatively when the column is not modelled.
        self.column = column


class Relations:
    """
    This class represents foreign key constraints on a column.
    """
    def __init__(self) -> None:
        #: Foreign key constraints on a column.
        self.constraints: list[ForeignKey] = []

    def add(self, fk: ForeignKey):
        """
        Adds a constraint.

        Args:
            fk: Foreign key constraint.
        """
        self.constraints.append(fk)


class Column:
    """
    This class represents a schema of a column.
    """
    def __init__(
        self,
        name: str,
        ptype: type,
        type_info: Optional[Any],
        pk: bool,
        fk: Optional[Relations],
        incremental: Optional[Any],
        nullable: bool,
        comment: str = "",
    ):
        #: Column name.
        self.name = name
        #: Data type in python.
        self.ptype = ptype
        #: Type informations obtained from DB.
        self.type_info = type_info
        #: Is this column a primary key?
        self.pk = pk
        #: Foreign key constraints.
        self.fk = fk
        #: If this column is auto-incremental, this object contains the information of the feature, otherwise, `None`.
        self.incremental = incremental
        #: Can this column contain null?
        self.nullable = nullable
        #: Comment of the column.
        self.comment = comment


class Table:
    """
    This class represents a schema of a table.
    """
    def __init__(self, name: str, columns: list[Column], comment: str = ""):
        #: Table name.
        self.name = name
        #: Columns in the table.
        self.columns = columns
        #: Comment of the table.
        self.comment = comment

    def find(self, name: str) -> Optional[Column]:
        """
        Find a column by name.

        Args:
            name: Column name.
        Returns:
            The column if exists, otherwise `None`.
        """
        return next(filter(lambda c: c.name == name, self.columns), None)


def define_model(table_: Table, mixins: Union[type[MXT], list[type], None] = None, model_type: Optional[type[M]] = Model) -> type[M]:
    """
    Create a model type representing a table.

    Model type inherits all types in `mixins` in order.
    When the same attribute is defined in multiple mixin types, the former overrides the latter.

    Every model type has following attributes:

    |name|type|description|
    |:---|:---|:---|
    |name|`str`|Name of the table.|
    |table|`Table`|Table schema.|
    |columns|`List[Column]`|List of column schemas.|
    |column|`Any`|An object whose attribute exposes of column schema of its name.|

    Model instances are created by passing the constructor keyword arguments composed of column names and values like builtin dataclass.
    Unlike dataclass, the constructor does not require all of columns.
    Omitted columns don't affect predefined operations such as `CRUDMixin.insert` .
    If `not null` constraint exists on the column, insertion will be denied at runtime and exception will be thrown.

    ```python
    >>> # CREATE TABLE t1 (col1 int, col2 text, col3 text);
    >>> table = define_model("t1")
    >>> model = table(col1=1, col2="a")
    ```

    Attributes are also assignable by normal setter. If attribute name is not a valid column name, `TypeError` raises.

    ```python
    >>> model.col3 = "b"
    ```

    Model instance supports iteration which yields pairs of assigned column schema and its value.

    ```python
    >>> for c, v in model:
    >>>     print(f"{c.name} = {v}")
    col1 = 1
    col2 = a
    col3 = b
    ```

    Args:
        table__: Table schema.
        mixin: Mixin types providing class methods to the model type.
        model_type: Use this just for type hinting to determine returned model type.
    Returns:
        Model type.
    """
    column_names = {c.name for c in table_.columns}

    class Columns:
        def __init__(self):
            for c in table_.columns:
                setattr(self, c.name, c)

    class _Meta(Meta, type):
        name = table_.name
        table = table_
        columns = table_.columns
        column = Columns()

        @classmethod
        def shrink(cls, excludes: list[str], includes: Optional[list[str]] = None) -> 'Meta':
            """
            Creates new model type containing subset of columns.

            Args:
                excludes: Column names to exclude.
                includes: Column names to include.
            Returns:
                model type.
            """
            cols = [c for c in cls.columns if (not includes or c.name in includes) and c.name not in excludes]
            return define_model(Table(cls.name, cols, cls.table.comment), mixins) # type: ignore

    class Base(Model, metaclass=_Meta):
        pass

    mixin_types: list[type] = []

    if isinstance(mixins, list):
        mixin_types = mixins
    elif get_origin(mixins) is not None:
        mixin_types = cast(list[type], list(get_args(mixins)))
    elif mixins is not None:
        raise ValueError(f"Model mixin types should be specified by Mixins or a list of types.")

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

        def __getitem__(self, key):
            return getattr(self, key)

        def __contains__(self, key):
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


def parse_pks(model: type[Meta], pks: PKS) -> tuple[list[str], list[Any]]:
    """
    Generates a pair of PK columns names and their values from polymorphic input.

    Args:
        model: Model class.
        pks: A dictionary of PK column name and their values or an object of single PK column.
    Returns:
        Names of PK columns and their values.
    """
    if isinstance(pks, dict):
        ordered = check_columns(model, pks, lambda c: c.pk, True)
        return [v[0] for v in ordered], [v[1] for v in ordered]
    else:
        cols = [c.name for c in model.columns if c.pk]
        if len(cols) != 1:
            raise ValueError(f"The number of primary key columns in {model.name} is not 1.")
        return ([cols[0]], [pks])


def extract_pks(model: type[Meta], record: Record) -> PKS:
    """
    Extract all primary keys from a record.

    Args:
        model: Model class.
        record: A record.
    Returns:
        Primary keys.
    """
    pk_columns = [c.name for c in model.columns if c.pk]
    if isinstance(record, dict):
        pks = dict((c, record[c]) for c in pk_columns if c in record)
    else:
        pks = dict((c, getattr(record, c)) for c in pk_columns if hasattr(record, c))
    if len(pks) != len(pk_columns):
        missing = set(pk_columns) - set(pks.keys())
        raise ValueError(f"Some primary keys are not contained in passed values and auto-increment values: {missing}")
    return pks


def check_columns(
    model: type[Meta],
    col_map: dict[str, Any],
    condition: Callable[[Column], bool] = lambda c: True,
    requires_all: bool = False,
) -> list[tuple[str, Any]]:
    """
    Checks keys of given `dict` match columns selected by a condition from a model.

    Args:
        model: Model class.
        col_map: Dictionary whose keys are column names.
        condition: A function which selects columns from the model.
        requires_all: If `True`, `ValueError` raises when the dictionary does not contain keys of all selected columns.
    """
    names = [c.name for c in model.columns if condition(c)]
    name_set = set(names)
    targets = set(col_map.keys())
    if not name_set >= targets:
        raise ValueError(f"Columns {targets - name_set} are not specified columns of '{model.name}'.")
    if requires_all and not name_set == targets:
        raise ValueError(f"Required columns {name_set - targets} in '{model.name}' are not found.")
    return [(n, col_map[n]) for n in names if n in col_map]


def model_values(model: type[Meta], values: Record, excludes_pk: bool = False) -> dict[str, Any]:
    """
    Generates a dictionary whose items are pairs of column name and column value.

    Args:
        model: Model class.
        values: Dictionary from column name to column value or a model instance.
        excludes_pk: If `True`, item of PK column is not contained in returned dictionary.
    Returns:
        A dictionary from column name to column value.
    """
    if isinstance(values, (dict, OrderedDict)):
        includes = {c.name for c in model.columns if (not excludes_pk) or (not c.pk)}
        return {k:v for k, v in values.items() if k in includes}
    elif isinstance(values, model):
        return {cv[0].name:cv[1] for cv in values if (not excludes_pk) or (not cv[0].pk)}
    else:
        raise TypeError(f"Required column value is not contained in the dictionary or model.")