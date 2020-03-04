from collections import OrderedDict
from pyracmon.util import split_dict

class Column:
    def __init__(self, name, ptype, type_info, pk, fk, incremental, comment = ""):
        """
        Create a column schema.

        Parametes
        ---------
        name: str
            Column name.
        ptype: type
            Data type in python.
        type_info: str
            Type informations obtained from DB.
        pk: bool
            Is this column a PK?
        fk: object
            An informative object if this column is a foreign key, otherwise None.
        incremental: object
            If this column is auto-incremental, this object contains the information of the feature, otherwise, None.
        """
        self.name = name
        self.ptype = ptype
        self.type_info = type_info
        self.pk = pk
        self.fk = fk
        self.incremental = incremental
        self.comment = comment


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns
        self.comment = ""


def define_model(table, mixins = []):
    """
    Create a model type representing the table.

    When the same attribute is defined in multiple mixin types, the former one has a priority.

    Parameters
    ----------
    table: Table
        Schema of table.
    mixins: [type]
        Types the created model type inherits.

    Returns
    -------
    type
        Created model type.
    """
    class Model(type("ModelBase", tuple(mixins), {})):
        name = table.name
        columns = table.columns

        def __init__(self, **kwargs):
            cls = type(self)
            column_names = [c.name for c in cls.columns]
            for k, v in kwargs.items():
                if k not in column_names:
                    raise TypeError(f"{k} is not a column of {cls.name}")
                setattr(self, k, v)

        def __iter__(self):
            cls = type(self)
            return map(lambda c: (c, getattr(self, c.name)), filter(lambda c: hasattr(self, c.name), cls.columns))

        def __eq__(self, other):
            cls = type(self)
            if cls != type(other):
                return False
            for k in [c.name for c in cls.columns]:
                if hasattr(self, k) ^ hasattr(other, k):
                    return False
                if getattr(self, k, None) != getattr(other, k, None):
                    return False
            return True

        @classmethod
        def _parse_pks(cls, pks):
            if isinstance(pks, dict):
                cls._check_columns(pks, lambda c: c.pk, True)
                return split_dict(pks)
            else:
                cols = [c.name for c in cls.columns if c.pk]
                if len(cols) != 1:
                    raise ValueError(f"The number of primary key columns is not 1.")
                return ([cols[0]], [pks])

        @classmethod
        def _check_columns(cls, col_map, condition = lambda c: True, requires_all = False):
            names = set([c.name for c in cls.columns if condition(c)])
            targets = set(col_map.keys())
            if not names >= targets:
                raise ValueError(f"Columns {targets - names} are not columns of '{cls.name}'.")
            if requires_all and not names == targets:
                raise ValueError(f"Required columns {names - targets} in '{cls.name}' are not found.")
            return col_map.keys()

    return Model
