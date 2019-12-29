class Q:
    """
    The instance of this class holds parameters to build query conditions.

    Each parameter passed by the constructor with its name behaves as a method
    which takes a condition clause including parameter holders which will accept the parameter in query execution.

    > >>> q = Q(a = 1)
    > >>> q.a("a = %s")
    > Condition: 'a = %s' -- (1,)

    Method whose name is not passed by the constructor returns empty condition which has no effect on the query.

    > >>> q.b("b = %s")
    > Condition: '' -- ()

    Those features simplifies a query construction in case each parameter may be absent.
    A function taking Q instance and constructing a query by using it enables caller to control conditions at runtime.

    > def search(db, q):
    >     w, params = where(q.a("a = %s") & q.b("b = %s"))
    >     db.cursor().execute(f"SELECT * FROM table {w}", params)
    >
    > search(Q(a = 1))        # SELECT * FROM table WHERE a = 1
    > search(Q(a = 1, b = 2)) # SELECT * FROM table WHERE a = 1 AND b = 2
    > search(Q())             # SELECT * FROM table
    """
    class C:
        def __init__(self, clause, params):
            self.clause = clause
            self.params = params if isinstance(params, tuple) \
                else tuple(params) if isinstance(params, list) \
                else (params,)

        def __repr__(self):
            return f"Condition: '{self.clause}' -- {self.params}"

        @classmethod
        def concat(cls, c1, c2, op):
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

    def __init__(self, **kwargs):
        self.params = dict([(k, v) for k, v in kwargs.items() if v is not None])

    def __getattr__(self, key):
        def attr(c, f=lambda x:x):
            return Q.C(c, f(self.params[key])) if key in self.params else Q.C('', ())
        return attr

    @classmethod
    def of(cls, clause, params):
        return Q.C(clause, params)


def where(condition):
    """
    Generates a condition clause representing given condition.

    Parameters
    ----------
    condition: Q.C
        A condition built by `Q` and concatenated with operators.

    Returns
    -------
    str
        A condition clause starting from `WHERE` or empty string if the condition is empty.
    """
    return ('', []) if condition.clause == '' else (f'WHERE {condition.clause}', list(condition.params))


def order_by(columns):
    """
    Generates ORDER BY clause from columns and directions.

    Parameters
    ----------
    columns: { str: bool }
        An ordered dictionary mapping column name to its direction.
    """
    def col(cd):
        return f"{cd[0]} ASC" if cd[1] else f"{cd[0]} DESC"
    return '' if len(columns) == 0 else f"ORDER BY {', '.join(map(col, columns.items()))}"


def ranged_by(marker, limit = None, offset = None):
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
    This class provides methods helping query construction available for any kind of DB-API 2.0 module.
    """
    def __init__(self, api):
        self.api = api

    def marker(self):
        return _marker_of(self.api.paramstyle)

    def holders(self, keys, qualifier = None, start = 0, marker = None):
        """
        Generates partial query string containing place holder markers.

        Parameters
        ----------
        keys: int / [str]
            The number of holders / Keys of holders.
        qualifier: {int: str -> str}
            Functions for each index converting the marker into another expression.
        start: int
            First index to calculate integral marker parameter.

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

        Returns
        -------
        str
            Generated string.
        """
        num = keys if isinstance(keys, int) else len(keys)
        m = marker or self.marker()
        return ', '.join([f"({self.holders(keys, qualifier, start + num * i, m)})" for i in range(0, rows)])


class Marker:
    def reset(self):
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