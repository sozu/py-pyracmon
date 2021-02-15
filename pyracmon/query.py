from functools import reduce
from .sql import Sql
from .marker import Marker


class Q:
    """
    The instance of this class holds parameters to build query conditions.

    Each parameter passed by the constructor becomes an instance method of created instance,
    which takes a condition clause including placeholders which will accept the parameter in query execution.
    `$_` in the clause will be replaced with correct placeholder marker in query execution by `Statement`.

    >>> q = Q(a=1)
    >>> q.a("a = $_")
    Condition: 'a = $_' -- [1]

    Method whose name is not passed by the constructor returns empty condition which has no effect on the query.

    >>> q.b("b = $_")
    Condition: '' -- []

    This feature simplifies a query construction in case some parameters can be absent.

    >>> def search(db, q):
    >>>     w, params = where(q.a("a = $_") & q.b("b = $_"))
    >>>     db.stmt().execute(f"SELECT * FROM table {w}", *params)
    >>> 
    >>> search(db, Q(a=1))        # SELECT * FROM table WHERE a = 1
    >>> search(db, Q(a=1, b=2)) # SELECT * FROM table WHERE a = 1 AND b = 2
    >>> search(db, Q())             # SELECT * FROM table

    Additionally, this class provides utility class methods creating conditions.

    Using `of()` is the most simple way to create a condition clause.

    >>> Q.of("a = $_", 1)
    Condition: 'a = $_' -- [1]

    Other utility methods correspond to basic operators defined in SQL.
    They takes keyword arguments and create conditions by applying operator to each item respectively.

    >>> Q.eq(a=1)
    Condition: 'a = %s' -- [1]
    >>> Q.eq(a=1, b=2)
    Condition: 'a = %s AND b = %s' -- [1, 2]
    >>> Q.in_(a=[1, 2, 3])
    Condition: 'a IN (%s, %s, %s)' -- [1, 2, 3]
    >>> Q.like(a="abc")
    Condition: 'a LIKE %s' -- ["%abc%"]
    """
    class Attribute:
        def __init__(self, value):
            self.value = value

        def __call__(self, expression, convert=None):
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
                Condition.
            """
            expression = expression if isinstance(expression, str) else expression(self.value)
            params = convert(self.value) if convert else [self.value]

            return Conditional(expression, params if isinstance(params, list) else [params])

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

        def __call__(self, expression, convert=None):
            conds = [Q.Attribute(v)(expression, convert) for v in self.value]
            return Conditional.all(conds) if self._and else Conditional.any(conds)

        def __getattr__(self, key):
            method = getattr(Q, key)
            def invoke(col, convert=None):
                conds = [method(**{col: convert(v) if convert else v}) for v in self.value]
                return Conditional.all(conds) if self._and else Conditional.any(conds)
            return invoke

    class NoAttribute:
        def __call__(self, expression, holder=lambda x:x):
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
    def of(cls, expression="", params=[]):
        """
        Creates a condition directly from an expression and parameters.

        Parameters
        ----------
        expression: str
            Condition expression.
        params: [object]
            Parameters used in the condition.

        Returns
        -------
        Conditional
            Condition.
        """
        if isinstance(params, list):
            pass
        elif isinstance(params, tuple):
            params = list(params)
        else:
            params = [params]
        return Conditional(expression, params)

    @classmethod
    def eq(cls, __alias=None, __and=True, **kwargs):
        """
        Creates a condition applying '=' operator to columns.

        Parameters
        ----------
        __alias: str
            Alias prepended to the column.
        __and: bool
            If `True`, conditions are concatenated by `AND`, otherwise `OR`.
        kwargs: {str:object}
            Mapping from columns to values.

        Returns
        -------
        Conditional
            Condition.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NULL", []
            elif val is True:
                return f"{col}", []
            elif val is False:
                return f"NOT {col}", []
            return None
        return _conditional("=", __and, kwargs, is_null, __alias)

    @classmethod
    def neq(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies `!=`.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NOT NULL", []
            elif val is True:
                return f"NOT {col}", []
            elif val is False:
                return f"{col}", []
            return None
        return _conditional("!=", __and, kwargs, is_null, __alias)

    @classmethod
    def in_(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies `IN`.

        If parameters are empty, this method returns a condition being never satisfied.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "1 = 0", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} IN ({holder})", val
        return _conditional("IN", __and, kwargs, in_list, __alias)

    @classmethod
    def not_in(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies `NOT IN`.

        If parameters are empty, this method returns a condition being always satisfied.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} NOT IN ({holder})", val
        return _conditional("NOT IN", __and, kwargs, in_list, __alias)

    @classmethod
    def match(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies 'LIKE'. Parameter is passed to query without being modified.
        """
        return _conditional("LIKE", __and, kwargs, None, __alias)

    @classmethod
    def like(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies 'LIKE'. Parameter is escaped and enclosed with wildcards (%) to execute partial match.
        """
        return _conditional("LIKE", __and, {k: f"%{escape_like(v)}%" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def startswith(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies 'LIKE'. Parameter is escaped and appended with wildcards (%) to execute prefix match.
        """
        return _conditional("LIKE", __and, {k: f"{escape_like(v)}%" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def endswith(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies 'LIKE'. Parameter is escaped and prepended with wildcards (%) to execute backward match.
        """
        return _conditional("LIKE", __and, {k: f"%{escape_like(v)}" for k, v in kwargs.items()}, None, __alias)

    @classmethod
    def lt(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies '<'.
        """
        return _conditional("<", __and, kwargs, None, __alias)

    @classmethod
    def le(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies '<='.
        """
        return _conditional("<=", __and, kwargs, None, __alias)

    @classmethod
    def gt(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies '>'.
        """
        return _conditional(">", __and, kwargs, None, __alias)

    @classmethod
    def ge(cls, __alias=None, __and=True, **kwargs):
        """
        Works like `eq`, but applies '>='.
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


class Expression:
    def __init__(self, expression, params):
        self.expression = expression
        self.params = params


class Conditional(Expression):
    """
    Represents a single condition composed of an expression and parameters used for placeholders in the expression.

    Parameters must be a list where the index of each parameter matches the index of placeholder for it.
    The expression accepts only the automatic numbering template parameter `$_`.

    Logical operators such as `&`, `|` and `~` is available to generate new condition.

    >>> c1 = Q.of("a = $_", 0)
    >>> c2 = Q.of("b < $_", 1)
    >>> c3 = Q.of("c > $_", 2)
    >>> c = ~(c1 & c2 | c3)
    >>> c
    Condition: NOT (((a = $_) AND (b < $_)) OR (c > $_)) -- [0, 1, 2]
    """
    @classmethod
    def all(cls, conditionals):
        """
        Concatenates conditional objects with `AND`.

        Parameters
        ----------
        conditionals: [Conditional]
            Condition.

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
            Condition.

        Returns
        -------
        Conditional
            Concatenated conditional object
        """
        return reduce(lambda acc, c: acc | c, conditionals, Conditional())

    def __init__(self, expression="", params=None):
        super().__init__(expression, params or [])

    def __repr__(self):
        return f"Condition: '{self.expression}' -- {self.params}"

    def __call__(self, marker):
        """
        Deprecated.
        """
        c, p = Sql(marker, self.expression).render(*self.params)
        if not isinstance(p, list):
            raise ValueError(f"Only list style marker is available.")
        return Conditional(c, p)

    def __and__(self, other):
        expression = ""
        if self.expression and other.expression:
            expression = f"({self.expression}) AND ({other.expression})"
        elif self.expression:
            expression = self.expression
        elif other.expression:
            expression = other.expression

        return Conditional(expression, self.params + other.params)

    def __or__(self, other):
        expression = ""
        if self.expression and other.expression:
            expression = f"({self.expression}) OR ({other.expression})"
        elif self.expression:
            expression = self.expression
        elif other.expression:
            expression = other.expression

        return Conditional(expression, self.params + other.params)

    def __invert__(self):
        if self.expression:
            return Conditional(f"NOT ({self.expression})", self.params)
        else:
            return Conditional(f"1 = 0", [])


def escape_like(v):
    """
    Escape characters for the use in `LIKE` condition.

    Parameters
    ----------
    v: str
        A string.

    Returns
    -------
    str
        Escaped string.
    """
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
    condition: Conditional
        Condition.

    Returns
    -------
    str
        A where clause starting from `WHERE` or empty string if the condition is empty.
    [object]
        Parameters for placeholders in where clause.
    """
    return ('', []) if condition.expression == '' else (f'WHERE {condition.expression}', condition.params)


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


def ranged_by(limit=None, offset=None):
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
    [object]
        Parameters for placeholders in LIMIT and OFFSET clause.
    """
    clause, params = [], []

    if limit is not None:
        clause.append("LIMIT $_")
        params.append(limit)

    if offset is not None:
        clause.append("OFFSET $_")
        params.append(offset)

    return ' '.join(clause) if clause else '', params


def holders(length_or_keys, qualifier=None):
    """
    Generates partial query string containing placeholders separated by comma.

    Parameters
    ----------
    length_or_keys: int | [str]
        The number of placeholders or keys assigned to them.
    qualifier: {int: str -> str}
        Mapping from indexes to functions. Each function converts placeholder string at paired index.

    Returns
    -------
    str
        Query string.
    """
    if isinstance(length_or_keys, int):
        hs = ["${_}"] * length_or_keys
    else:
        def key(k):
            if isinstance(k, int):
                return f"${{_{k}}}"
            elif k:
                return f"${{{k}}}"
            else:
                return "${_}"
        hs = [key(k) for k in length_or_keys]

    if qualifier:
        hs = [qualifier.get(i, _noop)(h) for i, h in enumerate(hs)]

    return ', '.join(hs)


def values(length_or_key_gen, rows, qualifier=None):
    """
    Generates partial query string for `VALUES` clause in insertion query.

    Parameters
    ----------
    length_or_key_gen: int | [int -> str]
        The number of placeholders or functions which takes a row index and returns a key.
    rows: int
        The number of rows to insert.
    qualifier: {int: str -> str}
        Mapping from indexes to functions. Each function converts placeholder string at paired index.

    Returns
    -------
    str
        Query string.
    """
    if isinstance(length_or_key_gen, int):
        lok = (lambda i: length_or_key_gen)
    else:
        lok = (lambda i: [g(i) for g in length_or_key_gen])

    return ', '.join([f"({holders(lok(i), qualifier)})" for i in range(rows)])


class QueryHelper:
    """
    Deprecated.
    """
    def __init__(self, api, config):
        self.api = api
        self.config = config

    def marker(self):
        """
        Deprecated. Don't create marker.
        """
        # TODO read paramstyle from config.
        return Marker.of(self.api.paramstyle)

    def holders(self, keys, qualifier = None, start = 0, marker = None):
        """
        Deprecated. Use global `holders` instead.
        """
        m = marker or self.marker()
        qualifier = qualifier or {}

        if isinstance(keys, int):
            return ', '.join([qualifier.get(i, _noop)(m(i + start)) for i in range(keys)])
        else:
            return ', '.join([qualifier.get(i, _noop)(m(k)) for i, k in enumerate(keys)])

    def values(self, keys, rows, qualifier = None, start = 0, marker = None):
        """
        Deprecated. Use global `values` instead.
        """
        num = keys if isinstance(keys, int) else len(keys)
        m = marker or self.marker()
        return ', '.join([f"({self.holders(keys, qualifier, start + num * i, m)})" for i in range(0, rows)])


def _noop(x):
    return x
