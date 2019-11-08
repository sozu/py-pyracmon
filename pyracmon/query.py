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
    return ('', ()) if condition.clause == '' else (f'WHERE {condition.clause}', condition.params)


class QueryHelper:
    """
    This class provides methods helping query construction available for any kind of DB-API 2.0 module.
    """
    def __init__(self, api):
        self.api = api

    def marker(self):
        return _marker_of(self.api.paramstyle)

    def holders(self, keys, qualifier = None, start = 0):
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
        m = self.marker()
        return ', '.join([qualifier.get(i, _noop)(m(key_map[i])) for i in range(0, len(key_map))])

    def values(self, keys, rows, qualifier = None, start = 0):
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
        return ', '.join([f"({self.holders(keys, qualifier, start + num * i)})" for i in range(0, rows)])


class QMarker:
    def __call__(self, x = None):
        return '?'

class NumericMarker:
    def __init__(self):
        self.index = 0
    def __call__(self, index = None):
        if index is None:
            self.index += 1
        else:
            self.index = index
        return f":{self.index}"

class NamedMarker:
    def __call__(self, name):
        return f":{name}"

class FormatMarker:
    def __call__(self, x = None):
        return '%s'

class PyformatMarker:
    def __call__(self, name = None):
        if name and not isinstance(name, int):
            return f"%({name})s"
        else:
            return '%s'

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