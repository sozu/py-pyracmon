from functools import reduce
from itertools import zip_longest
from collections import OrderedDict
from pyracmon.query import Q, where, order_by, ranged_by
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
    def count(cls, db, gen_condition = lambda m: Q.condition('', [])):
        c = db.cursor()
        m = db.helper.marker()
        wc, wp = where(gen_condition(m))
        c.execute(f"SELECT COUNT(*) FROM {cls.name} {wc}", m.params(wp))
        return c.fetchone()[0]

    @classmethod
    def fetch(cls, db, pks, lock = None):
        where_values = cls._parse_pks(pks)
        c = db.cursor()
        m = db.helper.marker()
        s = cls.select()
        where = ' AND '.join([f"{n} = {m()}" for n in where_values[0]])
        c.execute(f"SELECT {s} FROM {cls.name} WHERE {where}", m.params(where_values[1]))
        row = c.fetchone()
        return read_row(row, s)[0] if row else None

    @classmethod
    def fetch_where(cls, db, condition = lambda m: Q.condition('', []), orders = [], limit = None, offset = None, lock = None):
        def spacer(s):
            return (" " + s) if s else ""
        c = db.cursor()
        m = db.helper.marker()
        s = cls.select()
        wc, wp = where(condition(m))
        rc, rp = ranged_by(m, limit, offset)
        c.execute(f"SELECT {s} FROM {cls.name}{spacer(wc)}{spacer(order_by(orders))}{spacer(rc)}", m.params(wp + rp))
        return [read_row(row, s)[0] for row in c.fetchall()]

    @classmethod
    def insert(cls, db, values, qualifier = {}):
        value_dict = model_values(cls, values)
        cls._check_columns(value_dict)
        column_values = split_dict(value_dict)
        qualifier = index_qualifier(qualifier, column_values[0])

        m = db.helper.marker()
        sql = f"INSERT INTO {cls.name} ({', '.join(column_values[0])}) VALUES {db.helper.values(len(column_values[1]), 1, qualifier, marker = m)}"
        result = db.cursor().execute(sql, m.params(column_values[1]))

        if isinstance(values, cls):
            for c, v in cls.last_sequences(db, 1):
                setattr(values, c.name, v)

        return result

    @classmethod
    def update(cls, db, pks, values, qualifier = {}):
        def gen_condition(m):
            cols, vals = cls._parse_pks(pks)
            return reduce(lambda acc, x: acc & x, [Q.condition(f"{c} = {m()}", v) for c, v in zip(cols, vals)])
        return cls.update_where(db, values, gen_condition, qualifier, False)

    @classmethod
    def update_where(cls, db, values, gen_condition, qualifier = {}, allow_all = False):
        setters, params, m = _update(cls, db, values, qualifier)

        wc, wp = where(gen_condition(m))
        if wc == "" and not allow_all:
            raise ValueError("By default, update_where does not allow empty condition.")

        return db.cursor().execute(f"UPDATE {cls.name} SET {', '.join(setters)} {wc}", m.params(params + wp))

    @classmethod
    def delete(cls, db, pks):
        cols, vals = cls._parse_pks(pks)
        gen_condition = lambda m: reduce(lambda acc, x: acc & x, [Q.condition(f"{c} = {m()}", v) for c, v in zip(cols, vals)])

        return cls.delete_where(db, gen_condition)

    @classmethod
    def delete_where(cls, db, gen_condition, allow_all = False):
        m = db.helper.marker()
        wc, wp = where(gen_condition(m))
        if wc == "" and not allow_all:
            raise ValueError("By default, delete_where does not allow empty condition.")

        return db.cursor().execute(f"DELETE FROM {cls.name} {wc}", m.params(wp))

    @classmethod
    def last_sequences(cls, db, num):
        return []


def _update(cls, db, values, qualifier):
    value_dict = model_values(cls, values)
    cls._check_columns(value_dict)
    column_values = split_dict(value_dict)
    qualifier = index_qualifier(qualifier, column_values[0])

    m = db.helper.marker()
    setters = [f"{n} = {qualifier.get(i, lambda x: x)(m())}" for i, n in enumerate(column_values[0])]

    return setters, column_values[1], m