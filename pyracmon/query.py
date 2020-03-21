from functools import reduce

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
    class C:
        """
        This class represents a condition, which can be composed of many conditions.

        Some bitwise operators are applicable to create another condition by logical operator.

        - `&` concatenates two conditions by AND.
        - `|` concatenates two conditions by OR.
        - `~` creates inverted condition by NOT.
        """
        def __init__(self, clause, params):
            self.clause = clause
            self.params = params if isinstance(params, tuple) \
                else tuple(params) if isinstance(params, list) \
                else (params,)

        def __repr__(self):
            return f"Condition: '{self.clause}' -- {self.params}"

        @classmethod
        def concat(cls, c1, c2, op):
            """
            Concatenate two conditions with an operator.

            Parameters
            ----------
            c1: Q.C
                First condition,
            c2: Q.C
                Second condition,
            c3: str
                An operator.

            Returns
            -------
            Q.C
                Concatenated condition.
            """
            if c1.clause and c2.clause:
                return cls('({}) {} ({})'.format(c1.clause, op, c2.clause), c1.params + c2.params)
            elif c1.clause:
                return c1
            elif c2.clause:
                return c2
            else:
                return cls('', ())

        def __and__(self, other):
            return Q.C.concat(self, other, 'AND')

        def __or__(self, other):
            return Q.C.concat(self, other, 'OR')

        def __invert__(self):
            return Q.C(f"NOT ({self.clause})", self.params)

    class Attribute:
        def __init__(self, value):
            self.value = value

        def __call__(self, clause, holder=lambda x:x):
            if callable(clause):
                clause = clause(self.value)
            elif not isinstance(clause, str):
                raise ValueError(f"The first argument for query attribute method must be a string or callable taking a parameter.")

            if callable(holder):
                holder = holder(self.value)

            return Q.C(clause, holder)

        def all(self, clause, holder=lambda x: x):
            conds = [Q.Attribute(v)(clause, holder) for v in self.value]
            return reduce(lambda acc, c: acc & c, conds, Q.C('', ()))

        def any(self, clause, holder=lambda x: x):
            conds = [Q.Attribute(v)(clause, holder) for v in self.value]
            return reduce(lambda acc, c: acc | c, conds, Q.C('', ()))

    def __init__(self, **kwargs):
        self.params = dict([(k, v) for k, v in kwargs.items() if v is not None])

    def __getattr__(self, key):
        if key in self.params:
            return Q.Attribute(self.params[key])
        else:
            def true(*args, **kwargs):
                return Q.C('', ())
            return true

    @classmethod
    def of(cls, clause="", params=[]):
        """
        Utility method to create condition object directly from where clause and the parameters.

        Parameters
        ----------
        clause: str
            Where clause.
        params: [object]
            Parameters used in the condition.

        Returns
        -------
        Q.C
            Created condition.
        """
        return Q.C(clause, params)

    @classmethod
    def eq(cls, __and=True, __key=None, **kwargs):
        """
        Creates a function which generates a condition checking a column value equals to a value.

        Parameters
        ----------
        __and: bool
            Specifies concatenating operator of conditions. `True` means `AND` whereas `False` means `OR`.
        __key: str | int | [str]
            Value(s) passed to marker.
        kwargs: {str:object}
            Mapping from a column name to a value.

        Returns
        -------
        Marker -> Q.C
            A function which takes a marker and then returns a condition.
        """
        null_handler = lambda c: (f"{c} IS NULL", [])
        return _queries("=", __key, __and, kwargs.items(), null_handler)

    @classmethod
    def neq(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value does NOT equal to a value.
        """
        null_handler = lambda c: (f"{c} IS NOT NULL", [])
        return _queries("!=", __key, __and, kwargs.items(), null_handler)

    @classmethod
    def in_(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is one of list items using `IN` operator.
        """
        return _queries("IN", __key, __and, [(k, vs) for k, vs in kwargs.items() if vs])

    @classmethod
    def like(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is a sub-string of a value using `LIKE` operator.
        """
        return _queries("LIKE", __key, __and, [(k, f"%{_escape_like(v)}%") for k, v in kwargs.items()])

    @classmethod
    def prefix(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is a prefix of a value using `LIKE` operator.
        """
        return _queries("LIKE", __key, __and, [(k, f"{_escape_like(v)}%") for k, v in kwargs.items()])

    @classmethod
    def postfix(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is a postfix of a value using `LIKE` operator.
        """
        return _queries("LIKE", __key, __and, [(k, f"%{_escape_like(v)}") for k, v in kwargs.items()])

    @classmethod
    def lt(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is less than a value using `<` operator.
        """
        return _queries("<", __key, __and, kwargs.items())

    @classmethod
    def le(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is less than or equal to a value using `<=` operator.
        """
        return _queries("<=", __key, __and, kwargs.items())

    @classmethod
    def gt(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is greater than a value using `>` operator.
        """
        return _queries(">", __key, __and, kwargs.items())

    @classmethod
    def ge(cls, __and=True, __key=None, **kwargs):
        """
        Works like `eq`, but checks a column value is greater than or equal to a value using `>=` operator.
        """
        return _queries(">=", __key, __and, kwargs.items())


def _queries(op, key, and_, column_values, null_handler=None):
    def gen(m):
        concat = " AND " if and_ else " OR "
        queries = []
        values = []
        for col, val in column_values:
            if null_handler and val is None:
                q, vs = null_handler(col)
                queries.append(q)
                values += vs
            else:
                if isinstance(val, list):
                    num = len(val)
                    keys = key if isinstance(key, list) \
                        else [key+i for i in range(num)] if isinstance(key, int) \
                        else [None] * num
                    queries.append(f"{col} {op} ({','.join([m(keys[i]) for i in range(num)])})")
                    values += val
                else:
                    queries.append(f"{col} {op} {m(key)}")
                    values.append(val)
        return Q.of(concat.join(queries), values)
    return gen


def _escape_like(v):
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


class Marker:
    """
    This class provides the abstration mechanism for marker creation used to embed parameters in a query.

    The instance is obtained by invoking `db.helper.marker()`.
    In many cases, it's enough to get string representation of the instance, because the marker manges it state by its own.

    >>> m = db.helper.marker()
    >>> # when db.api.paramstyle == "format"
    >>> f"SELECT * FROM table1 WHERE col11 = {m()} AND col2 = {m()}"
    'SELECT * FROM table1 WHERE col11 = %s AND col2 = %s'
    """
    def reset(self):
        """
        Reset the internal state.
        """
        pass

    def params(self, ps):
        if isinstance(ps, (list, tuple)):
            return list(ps)
        else:
            raise ValueError(f"Parameters argument must be a list or tuple.")

class QMarker(Marker):
    def __call__(self, x = None):
        return '?'

class NumericMarker(Marker):
    def __init__(self):
        self.index = 0
    def __call__(self, index = None):
        if index is None:
            self.index += 1
        else:
            self.index = index
        return f":{self.index}"
    def reset(self):
        self.index = 0

class NamedMarker(Marker):
    def __init__(self):
        self.keys = []
    def __call__(self, name = None):
        if name is not None and name != "":
            self.keys.append(str(name))
            return f":{name}"
        else:
            key = f"key{len(self.keys)}"
            if key in self.keys:
                raise ValueError(f"Explicit key '{key}' prevented key generation for named marker.")
            self.keys.append(key)
            return f":{key}"
    def reset(self):
        self.keys = []
    def params(self, ps):
        if isinstance(ps, dict):
            return ps
        elif isinstance(ps, (list, tuple)):
            return dict(zip(self.keys, ps))
        else:
            raise ValueError(f"Parameter argument must be a dict, list or tuple.")

class FormatMarker(Marker):
    def __call__(self, x = None):
        return '%s'

class PyformatMarker(Marker):
    def __init__(self):
        self.keys = []
        self.is_named = None
    def __call__(self, name = None):
        named = name is not None and isinstance(name, str)
        if self.is_named is None:
            self.is_named = named
        elif self.is_named and not named:
            name = f"key{len(self.keys)}"
        elif not self.is_named and named:
            raise ValueError(f"Mixed usage of %s and %(name)s is not allowed.")
        if named:
            self.keys.append(name)
            return f"%({name})s"
        else:
            return f"%({name})s" if self.is_named else '%s'
    def reset(self):
        self.is_named = None
    def params(self, ps):
        if self.is_named is True:
            if isinstance(ps, dict):
                pass
            elif isinstance(ps, (list, tuple)):
                ps = dict(zip(self.keys, ps))
            else:
                raise ValueError(f"Pyformat with named parameters requires dict parameters.")
        elif self.is_named is False and not isinstance(ps, (list, tuple)):
            raise ValueError(f"Pyformat without parameter names requires list or tuple parameters.")
        return list(ps) if self.is_named is False else ps

def _noop(x):
    return x

def _marker_of(paramstyle):
    if paramstyle == 'qmark':
        return QMarker()
    elif paramstyle == 'numeric':
        return NumericMarker()
    elif paramstyle == 'named':
        return NamedMarker()
    elif paramstyle == 'format':
        return FormatMarker()
    elif paramstyle == 'pyformat':
        return PyformatMarker()
    else:
        raise ValueError(f"Unknown parameter style: {paramstyle}")