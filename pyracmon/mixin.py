from functools import reduce
from itertools import zip_longest
from collections import OrderedDict
from pyracmon.util import split_dict, index_qualifier, model_values


class Selection:
    def __init__(self, table, alias, columns):
        self.table = table
        self.alias = alias
        self.columns = columns

    def __len__(self):
        return len(self.columns)

    def __repr__(self):
        a = f"{self.alias}." if self.alias else ""
        return ', '.join([f"{a}{c.name}" for c in self.columns])

    def consume(self, values):
        return self.table(**dict([(c.name, v) for c, v in zip(self.columns, values)]))


def read_row(row, *selections, allow_redundancy = False):
    result = []

    for s in selections:
        if isinstance(s, Selection):
            result.append(s.consume(row))
            row = row[len(s):]
        elif callable(s):
            result.append(s(row[0]))
            row = row[1:]
        elif s == ():
            result.append(row[0])
            row = row[1:]
        else:
            raise ValueError("Unavailable value is given to read_row().")

    if not allow_redundancy and len(row) > 0:
        raise ValueError("Not all elements in row is consumed.")

    return result


class CRUDMixin:
    @classmethod
    def select(cls, alias = "", includes = [], excludes = []):
        """
        Select columns to use in a query with an alias of this table.

        Parameters
        ----------
        alias: str
            An alias string of this table.
        includes: [str]
            Column names to use. All columns are selected if empty.
        excludes: [str]
            Column names not to use.

        Returns
        -------
        Selection
            An object which has selected columns.
        """
        columns = [c for c in cls.columns if c.name not in excludes] \
            if not bool(includes) else \
                [c for c in cls.columns if c.name not in excludes and c.name in includes]
        return Selection(cls, alias, columns)

    @classmethod
    def insert(cls, db, values, qualifier = {}):
        value_dict = model_values(cls, values)
        cls._check_columns(value_dict)
        column_values = split_dict(value_dict)
        qualifier = index_qualifier(qualifier, column_values[0])

        sql = f"INSERT INTO {cls.name} ({', '.join(column_values[0])}) VALUES {db.helper.values(len(column_values[1]), 1, qualifier)}"
        result = db.cursor().execute(sql, column_values[1])

        if isinstance(values, cls):
            for c, v in cls.last_sequences(db, 1):
                setattr(values, c.name, v)

        return result

    @classmethod
    def update(cls, db, pks, values, qualifier = {}):
        value_dict = model_values(cls, values)
        cls._check_columns(value_dict)
        where_values = cls._parse_pks(pks)
        column_values = split_dict(value_dict)
        qualifier = index_qualifier(qualifier, column_values[0])

        m = db.helper.marker()
        setters = [f"{n} = {qualifier.get(i, lambda x: x)(m())}" for i, n in enumerate(column_values[0])]
        where = ' AND '.join([f"{n} = {m()}" for n in where_values[0]])
        return db.cursor().execute(f"UPDATE {cls.name} SET {', '.join(setters)} WHERE {where}", column_values[1] + where_values[1])

    @classmethod
    def delete(cls, db, pks):
        where_values = cls._parse_pks(pks)

        m = db.helper.marker()
        where = ' AND '.join([f"{n} = {m()}" for n in where_values[0]])
        return db.cursor().execute(f"DELETE FROM {cls.name} WHERE {where}", where_values[1])

    @classmethod
    def last_sequences(cls, db, num):
        return []