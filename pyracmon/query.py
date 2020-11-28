from functools import reduce
from .sql import Sql


class Q:
    """
    The instance of this class holds parameters to build query conditions.

    Each parameter passed by the constructor becomes an instance method of created instance,
    which takes a condition clause including placeholders which will accept the parameter in query execution.

    >>> q = Q(a = 1)
    >>> q.a("a = %s")
    Condition: 'a = %s' -- (1,)

    Method whose name is not passed by the constructor returns empty condition which has no effect on the query.

    >>> q.b("b = %s")
    Condition: '' -- ()

    Those features simplifies a query construction in case some parameters can be absent.
    A function taking Q instance and constructing a query by using it enables caller to control conditions at runtime.

    >>> def search(db, q):
    >>>     w, params = where(q.a("a = %s") & q.b("b = %s"))
    >>>     db.cursor().execute(f"SELECT * FROM table {w}", params)
    >>> 
    >>> search(Q(a = 1))        # SELECT * FROM table WHERE a = 1
    >>> search(Q(a = 1, b = 2)) # SELECT * FROM table WHERE a = 1 AND b = 2
    >>> search(Q())             # SELECT * FROM table

    Additionally, this class provides utility `classmethod` s creating condition or condition generation function directly.

    Using `of()` is the most simple way to create a condition clause.

    >>> Q.of("a = %s", 1)
    Condition: 'a = %s' -- (1,)

    Other utility methods correspond to basic operators defined in SQL.
    They create condition clause by applying the operator to pairs of column and some value(s) obtained from optional keyword arguments.

    >>> Q.eq(a = 1)
    Condition: 'a = %s' -- (1,)
    >>> Q.eq(a = 1, b = 2)
    Condition: 'a = %s AND b = %s' -- (1, 2)
    >>> Q.in_(a = [1, 2, 3])
    Condition: 'a IN (%s, %s, %s)' -- (1, 2, 3)
    >>> Q.like(a = "abc")
    Condition: 'a LIKE %s' -- ("%abc%",)
    """
    class Attribute:
        def __init__(self, value):
            self.value = value

        def __call__(self, clause, convert=None):
            """
            Craetes conditional object composed of given clause and the attribute value as parameters.

            Parameters
            ----------
            clause: str | object -> str
                A clause or a function generating a clause by taking the attribute value.
            convert: object -> object
                A function converting the attribute value to parameters.
                If this function returns a value which is not a list, a list having only the value is used.

            Returns
            -------
            Conditional
                Conditional object.
            """
            clause = clause if isinstance(clause, str) else clause(self.value)
            params = convert(self.value) if convert else [self.value]

            return Conditional(clause, params if isinstance(params, list) else [params])

        @property
        def all(self):
            return Q.CompositeAttribute(self.value, True)

        @property
        def any(self):
            return Q.CompositeAttribute(self.value, False)

        def __getattr__(self, key):
            method = getattr(Q, key)
            def invoke(col, convert=None):
                return method(**{col: convert(self.value) if convert else self.value})
            return invoke

    class CompositeAttribute(Attribute):
        def __init__(self, value, and_):
            super().__init__(value)
            self._and = and_

        def __call__(self, clause, convert=None):
            conds = [Q.Attribute(v)(clause, convert) for v in self.value]
            return Conditional.all(conds) if self._and else Conditional.any(conds)

        def __getattr__(self, key):
            method = getattr(Q, key)
            def invoke(col, convert=None):
                conds = [method(**{col: convert(v) if convert else v}) for v in self.value]
                return Conditional.all(conds) if self._and else Conditional.any(conds)
            return invoke

    class NoAttribute:
        def __call__(self, clause, holder=lambda x:x):
            return Conditional()

        @property
        def all(self):
            return self

        @property
        def any(self):
            return self

        def __getattr__(self, key):
            method = getattr(Q, key)
            def invoke(col, convert=None):
                return Conditional()
            return invoke

    def __init__(self, __include_none=False, **kwargs):
        self.attributes = dict([(k, v) for k, v in kwargs.items() if __include_none or v is not None])

    def __getattr__(self, key):
        if key in self.attributes:
            return Q.Attribute(self.attributes[key])
        else:
            return Q.NoAttribute()

    @classmethod
    def of(cls, clause="", params=[]):
        """
        Utility method to create condition object directly from where clause and the parameters.

        Parameters
        ----------
        clause: str
            Condition clause.
        params: [object]
            Parameters used in the condition.

        Returns
        -------
        Conditional
            Conditional object.
        """
        return Conditional(clause, params)

    @classmethod
    def eq(cls, __alias=None, __and=True, **kwargs):
        """
        Creates a function which generates a condition checking a column value equals to a value.

        Parameters
        ----------
        __alias: str
            Alias prepended to the column.
        __and: bool
            If `True`, conditions are concatenated by `&`, otherwise `|`.
        kwargs: {str:object}
            Mapping from columns to values.

        Returns
        -------
        Conditional
            Conditional object.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NULL", []
            return None
        return _conditional("=", __and, kwargs, is_null, __alias)

    @classmethod
    def neq(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value does NOT equal to a value.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NOT NULL", []
            return None
        return _conditional("!=", __and, kwargs, is_null, __alias)

    @classmethod
    def in_(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is one of list items using `IN` operator.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "1 = 0", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} IN ({holder})", val
        return _conditional("IN", __and, kwargs, in_list, __alias)

    @classmethod
    def match(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value matches a string escaped for `LIKE` operator.
        """
        return _conditional("LIKE", __and, kwargs, None, __alias)

    @classmethod
    def like(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is a sub-string of a value using `LIKE` operator.
        """
        return _conditional("LIKE", __and, {k: f"%{escape_like(v)}%" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def startswith(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is a prefix of a value using `LIKE` operator.
        """
        return _conditional("LIKE", __and, {k: f"{escape_like(v)}%" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def endswith(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is a postfix of a value using `LIKE` operator.
        """
        return _conditional("LIKE", __and, {k: f"%{escape_like(v)}" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def lt(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is less than a value using `<` operator.
        """
        return _conditional("<", __and, kwargs, None, __alias)

    @classmethod
    def le(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is less than or equal to a value using `<=` operator.
        """
        return _conditional("<=", __and, kwargs, None, __alias)

    @classmethod
    def gt(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is greater than a value using `>` operator.
        """
        return _conditional(">", __and, kwargs, None, __alias)

    @classmethod
    def ge(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but checks a column value is greater than or equal to a value using `>=` operator.
        """
        return _conditional(">=", __and, kwargs, None, __alias)


def _conditional(op, and_, column_values, gen=None, alias=None):
    cond = Conditional()

    def concat(c):
        nonlocal cond
        if and_:
            cond &= c
        else:
            cond |= c

    for col, val in column_values.items():
        col = f"{alias}.{col}" if alias else col

        if gen:
            r = gen(col, val)
            if r is not None:
                concat(Conditional(r[0], r[1]))
                continue

        concat(Conditional(f"{col} {op} $_", [val]))

    return cond


class Conditional:
    """
    Represents a single condition composed of a clause and parameters used for place holders in the clause.

    Parameters must be a list where the index of each parameter matches the index of place holder for it.
    The clause accepts only the automatic numbering template parameter, that is `$_`.
    """
    @classmethod
    def all(cls, conditionals):
        """
        Concatenates conditional objects with `AND`.

        Parameters
        ----------
        conditionals: [Conditional]
            Conditional objects.

        Returns
        -------
        Conditional
            Concatenated conditional object
        """
        return reduce(lambda acc, c: acc & c, conditionals, Conditional())

    @classmethod
    def any(cls, conditionals):
        """
        Concatenates conditional objects with `OR`.

        Parameters
        ----------
        conditionals: [Conditional]
            Conditional objects.

        Returns
        -------
        Conditional
            Concatenated conditional object
        """
        return reduce(lambda acc, c: acc | c, conditionals, Conditional())

    def __init__(self, clause="", params=None):
        self.clause = clause
        self.params = list(params or [])

    def __repr__(self):
        return f"Condition: '{self.clause}' -- {self.params}"

    def __call__(self, marker):
        """
        Deprecated.
        """
        return self

    def __and__(self, other):
        clause = ""
        if self.clause and other.clause:
            clause = f"({self.clause}) AND ({other.clause})"
        elif self.clause:
            clause = self.clause
        elif other.clause:
            clause = other.clause

        return Conditional(clause, self.params + other.params)

    def __or__(self, other):
        clause = ""
        if self.clause and other.clause:
            clause = f"({self.clause}) OR ({other.clause})"
        elif self.clause:
            clause = self.clause
        elif other.clause:
            clause = other.clause

        return Conditional(clause, self.params + other.params)

    def __invert__(self):
        if self.clause:
            return Conditional(f"NOT ({self.clause})", self.params)
        else:
            return Conditional(f"1 = 0", [])


class _Conditional:
    """
    This class wraps the function which takes a marker and returns a condition and parameters.

    Instances can be composed by boolean operators.
    """
    def __init__(self, gen):
        self.gen = gen

    def __call__(self, marker):
        return self.gen(marker)

    def __and__(self, other):
        def _gen(m):
            return self(m) & other(m)
        return _Conditional(_gen)

    def __or__(self, other):
        def _gen(m):
            return self(m) | other(m)
        return _Conditional(_gen)

    def __invert__(self):
        def _gen(m):
            return ~(self(m))
        return _Conditional(_gen)


def escape_like(v):
    def esc(c):
        if c == "\\":
            return r"\\\\"
        elif c == "%":
            return r"\%"
        elif c == "_":
            return r"\_"
        else:
            return c
    return ''.join(map(esc, v))


def where(condition):
    """
    Generates a where clause representing given condition.

    Parameters
    ----------
    condition: Q.C
        A condition built by `Q` and concatenated with operators.

    Returns
    -------
    str
        A where clause starting from `WHERE` or empty string if the condition is empty.
    """
    return ('', []) if condition.clause == '' else (f'WHERE {condition.clause}', list(condition.params))


def order_by(columns):
    """
    Generates ORDER BY clause from columns and directions.

    Parameters
    ----------
    columns: { str: bool }
        An ordered dictionary mapping column name to its direction, where `True` denotes `ASC` and `False` denotes `DESC`.

    Returns
    -------
    str
        ORDER BY clause.
    """
    def col(cd):
        return f"{cd[0]} ASC" if cd[1] else f"{cd[0]} DESC"
    return '' if len(columns) == 0 else f"ORDER BY {', '.join(map(col, columns.items()))}"


def ranged_by(marker, limit = None, offset = None):
    """
    Generates LIMIT and OFFSET clause using marker.

    Parameters
    ----------
    limit: int
        Limit value or `None`.
    offset: int
        OFfset value or `None`.

    Returns
    -------
    str
        LIMIT and OFFSET clause.
    """
    clause, params = [], []
    if limit is not None:
        clause.append(f"LIMIT {marker()}")
        params.append(limit)
    if offset is not None:
        clause.append(f"OFFSET {marker()}")
        params.append(offset)
    return ' '.join(clause), params


class QueryHelper:
    """
    This class provides methods helping query construction.

    The instance can be obtained via `helper` attribute in `pyracmon.connection.Connection`.
    """
    def __init__(self, api):
        self.api = api

    def marker(self):
        """
        Create new marker.

        Returns
        -------
        Marker
            Created marker.
        """
        return _marker_of(self.api.paramstyle)

    def holders(self, keys, qualifier = None, start = 0, marker = None):
        """
        Generates partial query string containing place holder markers with comma.

        >>> # when db.api.paramstyle == "format"
        >>> db.helper.holders(5)
        '%s, %s, %s, %s, %s'

        >>> # when db.api.paramstyle == "numeric"
        >>> db.helper.holders(5, start=3)
        ':3, :4, :5, :6, :7'

        >>> # when db.api.paramstyle == "named"
        >>> db.helper.holders(['a', 'b', 'c', 'd', 'e'])
        ':a, :b, :c, :d, :e'

        Parameters
        ----------
        keys: int / [str]
            The number of holders / Keys of holders.
        qualifier: {int: str -> str}
            Functions for each index converting the marker into another expression.
        start: int
            First index to calculate integral marker parameter.
        marker: Marker
            A marker object used in this method. If `None`, new marker instance is created and used.

        Returns
        -------
        str
            Generated string.
        """
        key_map = dict([(i, start + i + 1) for i in range(0, keys)]) if isinstance(keys, int) \
            else dict([(i, k) for i, k in enumerate(keys)])
        qualifier = qualifier or {}
        m = marker or self.marker()
        return ', '.join([qualifier.get(i, _noop)(m(key_map[i])) for i in range(0, len(key_map))])

    def values(self, keys, rows, qualifier = None, start = 0, marker = None):
        """
        Generates partial query string corresponding `VALUES` clause in insertion query.

        >>> # when db.api.paramstyle == "format"
        >>> db.helper.values(5, 3)
        '(%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s), (%s, %s, %s, %s, %s)'

        Parameters
        ----------
        keys: int / [str]
            The number of holders / Keys of holders.
        rows: int
            The number of rows to insert.
        qualifier: {int: str -> str}
            Functions for each index converting the marker into another expression.
        start: int
            First index to calculate integral marker parameter.
        marker: Marker
            A marker object used in this method. If `None`, new marker instance is created and used.

        Returns
        -------
        str
            Generated string.
        """
        num = keys if isinstance(keys, int) else len(keys)
        m = marker or self.marker()
        return ', '.join([f"({self.holders(keys, qualifier, start + num * i, m)})" for i in range(0, rows)])


def _noop(x):
    return x
