from collections import OrderedDict


class Column:
    """
    This class represents a schema of a column.

    Attributes
    ----------
    name: str
        Column name.
    ptype: type
        Data type in python.
    type_info: str
        Type informations obtained from DB.
    pk: bool
        Is this column a PK?
    fk: object
        An informative object if this column is a foreign key, otherwise a value evaluated as `False` in boolean context.
    incremental: object
        If this column is auto-incremental, this object contains the information of the feature, otherwise, None.
    nullable: bool
        Can this column contain null?
    comment: str
        Comment of the column.
    """
    def __init__(self, name, ptype, type_info, pk, fk, incremental, nullable, comment=""):
        self.name = name
        self.ptype = ptype
        self.type_info = type_info
        self.pk = pk
        self.fk = fk
        self.incremental = incremental
        self.nullable = nullable
        self.comment = comment


class Table:
    """
    This class represents a schema of a table.

    Attributes
    ----------
    name: str
        Table name.
    columns: [Column]
        Columns in the table.
    comment: str
        Comment of the table.
    """
    def __init__(self, name, columns, comment=""):
        self.name = name
        self.columns = columns
        self.comment = ""


def define_model(table_, mixins=[]):
    """
    Create a model type representing the table.

    Types in `mixins` argument are base types which created type inherits.
    When the same attribute is defined in multiple mixin types, the former overwrites the latter.

    Model type always has following attributes.

    - name: str
        Name of the table.
    - table: Table
        Table schema.
    - columns: [Column]
        The list of column schemas.
    - column: object
        An object exposing schemas of columns via attributes of their names.

    Model instances are created by passing column name and its value as keyword arguments to the constructor.
    They are plain python objects representing a DB record by holding the subset of its columns as attributes.

    >>> # CREATE TABLE t1 (col1 int, col2 text, col3 text);
    >>> table = define_model("t1")
    >>> model = table(col1=1, col2="a")

    Attributes are also assignable by normal setter. In any case, attribute name must be a one of column names, otherwise `TypeError` raises.

    >>> model.col3 = "b"

    Model instance supports iteration which yields pairs of assigned column schema and its value.

    >>> for c, v in model:
    >>>     print(f"{c.name} = {v}")
    >>>
    col1 = 1
    col2 = a
    col3 = b

    Parameters
    ----------
    table_: Table
        Schema of table.
    mixins: [type]
        Types the created model type inherits.

    Returns
    -------
    type
        Created model type.
    """
    column_names = {c.name for c in table_.columns}

    class Meta(type):
        name = table_.name
        table = table_
        columns = table_.columns

        @classmethod
        def shrink(cls, excludes, includes=None):
            """
            Creates new model type containing subset of columns.

            Parameters
            ----------
            excludes: [str]
                Column names to exclude.
            includes: [str]
                Column names to include.

            Returns
            -------
            type
                Created model type.
            """
            cols = [c for c in cls.columns if (not includes or c.name in includes) and c.name not in excludes]
            return define_model(Table(cls.name, cols, cls.table.comment), mixins)

    class Columns:
        def __init__(self):
            for c in table_.columns:
                setattr(self, c.name, c)

    setattr(Meta, "column", Columns())

    class Base(metaclass=Meta):
        pass

    class Model(type("ModelBase", tuple([Base] + mixins), {})):
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

        def __repr__(self):
            cls = type(self)
            return f"{cls.name}({', '.join([f'{c.name}={repr(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

        def __str__(self):
            cls = type(self)
            return f"{cls.name}({', '.join([f'{c.name}={str(getattr(self, c.name))}' for c in cls.columns if hasattr(self, c.name)])})"

        def __iter__(self):
            cls = type(self)
            return map(lambda c: (c, getattr(self, c.name)), filter(lambda c: hasattr(self, c.name), cls.columns))

        def __setattr__(self, key, value):
            if key not in column_names:
                raise TypeError(f"{key} is not a column of {type(self).name}")
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

    return Model


def parse_pks(model, pks):
    """
    Generates a pair of PK columns names and their values from polymorphic input.

    Parameters
    ----------
    model: type
        Model class.
    pks: object | {str: object}
        A dictionary of PK column name and their values or an object of single PK column.

    Returns
    -------
    [str]
        Names of PK columns.
    [object]
        Values of PK columns.
    """
    if isinstance(pks, dict):
        check_columns(model, pks, lambda c: c.pk, True)
        return list(pks.keys()), list(pks.values())
    else:
        cols = [c.name for c in model.columns if c.pk]
        if len(cols) != 1:
            raise ValueError(f"The number of primary key columns in {model.name} is not 1.")
        return ([cols[0]], [pks])


def check_columns(model, col_map, condition=lambda c: True, requires_all=False):
    """
    Checks keys of given `dict` match columns selected by a condition from a model.

    Parameters
    ----------
    model: type
        Model class.
    col_map: {str: object}
        Dictionary whose keys are column names.
    condition: Column -> boolean
        A Function which selects columns from the model.
    requires_all: boolean
        If `True`, `ValueError` raises when the dictionary does not contain keys of all selected columns.
    """
    names = set([c.name for c in model.columns if condition(c)])
    targets = set(col_map.keys())
    if not names >= targets:
        raise ValueError(f"Columns {targets - names} are not columns of '{model.name}'.")
    if requires_all and not names == targets:
        raise ValueError(f"Required columns {names - targets} in '{model.name}' are not found.")


def model_values(model, values, excludes_pk=False):
    """
    Generates a dictionary whose items are pairs of column name and column value.

    Parameters
    ----------
    model: type
        Model class.
    values: dict | Model
        Dictionary from column name to column value or a model instance.
    excludes_pk: boolean
        If `True`, item of PK column is not contained in returned dictionary.

    Returns
    -------
    dict
        A dictionary from column name to column value.
    """
    if isinstance(values, (dict, OrderedDict)):
        includes = {c.name for c in model.columns if (not excludes_pk) or (not c.pk)}
        return {k:v for k, v in values.items() if k in includes}
    elif isinstance(values, model):
        return {cv[0].name:cv[1] for cv in values if (not excludes_pk) or (not cv[0].pk)}
    else:
        raise TypeError(f"Values to insert or update must be a dictionary or model.")