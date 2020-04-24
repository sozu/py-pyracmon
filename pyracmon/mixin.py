from functools import reduce
from itertools import zip_longest
from collections import OrderedDict
from pyracmon.query import Q, where, order_by, ranged_by
from pyracmon.util import split_dict, index_qualifier, model_values


class Selection:
    """
    The representation of table and its columns used in SQL.

    This class is designed to be a bridge from query generation to reading results.
    String expression of the instance is comma-separated column names with alias, which can be embedded in the select query.

    >>> s1 = table1.select("t1", includes = ["col11", "col12"])
    >>> s2 = table2.select("t2")
    >>> str(s1)
    't1.col11, t1.col12'
    >>> str(s2)
    't2.col21, t2.col22, t2.col23'

    The instances of this class are used in `read_row()` to read model object from obtained row.

    >>> c.execute(f"SELECT {s1}, {s2} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, s1, s2)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)
    """
    def __init__(self, table, alias, columns):
        self.table = table
        self.alias = alias
        self.columns = columns

    @property
    def name(self):
        return self.alias if self.alias else self.table.name

    def __len__(self):
        return len(self.columns)

    def __repr__(self):
        a = f"{self.alias}." if self.alias else ""
        return ', '.join([f"{a}{c.name}" for c in self.columns])

    def __add__(self, other):
        return Expressions() + self + other

    def consume(self, values):
        return self.table(**dict([(c.name, v) for c, v in zip(self.columns, values)]))


class Expressions:
    """
    The instance of this class works as the composition of `Selection`s which provides attributes to access each `Selection`.

    The main purpose of this class is avoiding a flood of occurrence of `Selection` variables. 
    Just applying + operator to them creates an instance of `Expressions`, by which all `Selection` are available via attributes of their names.

    >>> exp = table1.select("t1", includes = ["col11", "col12"]) + table2.select("t2")
    >>> c.execute(f"SELECT {exp.t1}, {exp.t2} FROM table1 AS t1 INNER JOIN table2 AS t2 ON ...")
    >>> for row in c.fetchall():
    >>>     r = read_row(row, *exp)
    >>>     assert isinstance(r.t1, table1)
    >>>     assert isinstance(r.t2, table2)
    """
    def __init__(self):
        self.__selections = []
        self.__keys = {}

    def __add__(self, other):
        if isinstance(other, Selection):
            self.__selections.append(other)
            self.__keys[other.name] = other
        elif isinstance(other, Expressions):
            self.__selections += other.__selections
            self.__keys.update(other.__keys)
        elif isinstance(other, str):
            self.__selections.append(other)
            self.__keys[other] = other
        elif other is ():
            self.__selections.append(other)
        else:
            raise ValueError(f"Operand of + for Expressions must be a Selection or Expressions but {type(other)} is given.")
        return self

    def __iadd__(self, selection):
        if isinstance(selection, Selection):
            self.__selections.append(selection)
            self.__keys[selection.name] = selection
        elif isinstance(other, str):
            self.__selections.append(other)
            self.__keys[other] = other
        elif other is ():
            self.__selections.append(other)
        else:
            raise ValueError(f"Operand of += for Expressions must be a Selection or Expressions object but {type(selection)} is given.")
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
        return Expressions.Instance(self, *args, **kwargs)

    def __repr__(self):
        return self().__repr__()


def expressions(*exps):
    """
    Creates a sequence of expressions.

    Parameters
    ----------
    exps: [Selection | str | ...]
        Sequence of values each of which corresponds to an SQL expression.

    Returns
    -------
    Expressions
        Sequence of expressions.
    """
    exp = Expressions()
    for x in exps:
        exp += x
    return exp


class RowValues:
    """
    This class provides attribute access for each row in query result.

    Values converted by `read_row()` can be accessed as if this instance is the list of them by using index or iteration.
    They can also be accessed via the attribute whose name is the alias (if exists) or the table name.

    >>> sels = table1.select("t1"), table2.select()
    >>> v = read_row(row, *sels)
    >>> v.t1
    >>> v.table2
    """
    def __init__(self, selections):
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

    def append(self, value):
        self.values.append(value)


def read_row(row, *selections, allow_redundancy = False):
    """
    Read values in a row according to `selections`.

    This function returns `RowValues` where each value is created by the item of `selections` respectively.
    The type of the item determines how values in the row is handled:

    - Selection consumes as many values as the number of columns in it and creates a model instance.
    - Callable consumes a value and returns another value.
    - Empty tuple or a string consumes a value, which is stored in `RowValues` as it is.

    Parameters
    ----------
    row: object
        An object representing a row returned by fetchone() / fetchall().
    selections: [Selection | S -> T | () | str]
        various type of objects determining the way to handle values in the row.
    allow_redundancy: bool
        If `False`, an exception is thrown when not all values in a row are consumed.

    Returns
    -------
    RowValues
        Values read from the row.
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


class CRUDMixin:
    """
    This class provides class methods available on all model classes.

    Every method takes the DB connection object as its first argument.

    Also, arguments listed below are commonly used in some methods of this mixin class:

    - `pks` represents value(s) of primary key(s).
        - If `pks` is a dictionary, each item is considered to be a pair of column name and value for primary keys respectively.
        - Otherwise, the model must have a primary key and `pks` is considered as its value.
    - `gen_condition` represents a condition or a function generating a condition with marker object.
        - This polymorphism enables the use of marker especially if it is stateful.
    - `qualifier` is a dictionary whose key is a column name and the value is a function generating SQL expression around the placeholder for the column.
        - By default in insert and update query, the expressions generated by a marker is used for column values.
        - This argument is used to override the expression.
        - Because the function takes default expression as an argument, you can generate another expression by qualifying it.
    """
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
    def count(cls, db, gen_condition = lambda m: Q.of('', [])):
        """
        Count rows which fulfill the condition.

        Parameters
        ----------
        db: Connection
            DB connection.
        gen_condition: Q.C | Marker -> Q.C
            Condition or a function creating a condition with a marker.

        Returns
        -------
        int
            The number of rows.
        """
        c = db.cursor()
        m = db.helper.marker()
        wc, wp = _where(gen_condition, m)
        c.execute(f"SELECT COUNT(*) FROM {cls.name} {wc}", m.params(wp))
        return c.fetchone()[0]

    @classmethod
    def fetch(cls, db, pks, lock = None):
        """
        Fetch a record by primary key(s).

        Parameters
        ----------
        db: Connection
            DB connection.
        pks: object | {str: object}
            A primary key or a mapping from column name to a value of primary keys.
        lock: object
            An object whose string representation is a valid locking statement.

        Returns
        -------
        cls
            A model of the record.
        """
        def spacer(s):
            return (" " + str(s)) if s else ""
        where_values = cls._parse_pks(pks)
        c = db.cursor()
        m = db.helper.marker()
        s = cls.select()
        where = ' AND '.join([f"{n} = {m()}" for n in where_values[0]])
        c.execute(f"SELECT {s} FROM {cls.name} WHERE {where}{spacer(lock)}", m.params(where_values[1]))
        row = c.fetchone()
        return read_row(row, s)[0] if row else None

    @classmethod
    def fetch_where(cls, db, gen_condition = lambda m: Q.of('', []), orders = {}, limit = None, offset = None, lock = None):
        """
        Fetch records which fulfill a condition.

        Parameters
        ----------
        db: Connection
            DB connection.
        gen_condition: Q.C | Marker -> Q.C
            Condition or a function creating a condition with a marker.
        orders: {str: bool}
            Ordered dict composed of column names and their ordering method. `True` means `ASC` and `False` means `DESC`.
        limit: int
            The number of rows to fetch. If `None`, all rows are obtained.
        offset: int
            The number of rows to skip.
        lock: object
            An object whose string representation is a valid locking statement.

        Returns
        -------
        [cls]
            Models of records.
        """
        def spacer(s):
            return (" " + str(s)) if s else ""
        c = db.cursor()
        m = db.helper.marker()
        s = cls.select()
        wc, wp = _where(gen_condition, m)
        rc, rp = ranged_by(m, limit, offset)
        c.execute(f"SELECT {s} FROM {cls.name}{spacer(wc)}{spacer(order_by(orders))}{spacer(rc)}{spacer(lock)}", m.params(wp + rp))
        return [read_row(row, s)[0] for row in c.fetchall()]

    @classmethod
    def insert(cls, db, values, qualifier = {}):
        """
        Insert a record.

        Parameters
        ----------
        db: Connection
            DB connection.
        values: {str: object} | model
            Columns and values to insert.
        qualifier: {str: str -> str}
            A mapping from column name to a function converting holder marker into another SQL expression.

        Returns
        -------
        object
            An object returned by `execute()` of DB-API 2.0 module.
        """
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
        """
        Update a record by primary key(s).

        Parameters
        ----------
        db: Connection
            DB connection.
        pks: object | {str: object}
            A primary key or a mapping from column name to a value of primary keys.
        values: {str: object} | model
            Columns and values to update.
        qualifier: {str: str -> str}
            A mapping from column name to a function converting holder marker into another SQL expression.

        Returns
        -------
        object
            An object returned by `execute()` of DB-API 2.0 module.
        """
        def gen_condition(m):
            cols, vals = cls._parse_pks(pks)
            return reduce(lambda acc, x: acc & x, [Q.of(f"{c} = {m()}", v) for c, v in zip(cols, vals)])
        return cls.update_where(db, values, gen_condition, qualifier, False)

    @classmethod
    def update_where(cls, db, values, gen_condition, qualifier = {}, allow_all = True):
        """
        Update records which fulfill a condition.

        Parameters
        ----------
        db: Connection
            DB connection.
        values: {str: object} | model
            Columns and values to update.
        gen_condition: Q.C | Marker -> Q.C
            Condition or a function creating a condition with a marker.
        qualifier: {str: str -> str}
            A mapping from column name to a function converting holder marker into another SQL expression.
        allow_all: bool
            Empty condition raises an exception if this is `False`.

        Returns
        -------
        object
            An object returned by `execute()` of DB-API 2.0 module.
        """
        setters, params, m = _update(cls, db, values, qualifier)

        wc, wp = _where(gen_condition, m)
        if wc == "" and not allow_all:
            raise ValueError("By default, update_where does not allow empty condition.")

        return db.cursor().execute(f"UPDATE {cls.name} SET {', '.join(setters)} {wc}", m.params(params + wp))

    @classmethod
    def delete(cls, db, pks):
        """
        Delete a record by primary key(s).

        Parameters
        ----------
        db: Connection
            DB connection.
        pks: object | {str: object}
            A primary key or a mapping from column name to a value of primary keys.

        Returns
        -------
        object
            An object returned by `execute()` of DB-API 2.0 module.
        """
        cols, vals = cls._parse_pks(pks)
        gen_condition = lambda m: reduce(lambda acc, x: acc & x, [Q.of(f"{c} = {m()}", v) for c, v in zip(cols, vals)])

        return cls.delete_where(db, gen_condition)

    @classmethod
    def delete_where(cls, db, gen_condition, allow_all = True):
        """
        Delete records which fulfill a condition.

        Parameters
        ----------
        db: Connection
            DB connection.
        gen_condition: Q.C | Marker -> Q.C
            Condition or a function creating a condition with a marker.
        allow_all: bool
            Empty condition raises an exception if this is `False`.

        Returns
        -------
        object
            An object returned by `execute()` of DB-API 2.0 module.
        """
        m = db.helper.marker()
        wc, wp = _where(gen_condition, m)
        if wc == "" and not allow_all:
            raise ValueError("By default, delete_where does not allow empty condition.")

        return db.cursor().execute(f"DELETE FROM {cls.name} {wc}", m.params(wp))

    @classmethod
    def last_sequences(cls, db, num):
        """
        Returns latest auto generated numbers in this table.

        Parameters
        ----------
        db: Connection
            DB connection.
        num: int
            The number of records inserted by the latest insert query.

        Returns
        -------
        [(Column, int)]
            A list of pairs of column and the generated number.
        """
        return []


def _update(cls, db, values, qualifier):
    value_dict = model_values(cls, values, excludes_pk=True)
    cls._check_columns(value_dict)
    column_values = split_dict(value_dict)
    qualifier = index_qualifier(qualifier, column_values[0])

    m = db.helper.marker()
    setters = [f"{n} = {qualifier.get(i, lambda x: x)(m())}" for i, n in enumerate(column_values[0])]

    return setters, column_values[1], m


def _where(gen_condition, marker):
    return where(gen_condition if isinstance(gen_condition, Q.C) else gen_condition(marker))