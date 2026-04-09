"""
This module exports types and functions for query construction.

`Q` is the factory class constructing query condition, that is, `WHERE` clause.
Class methods on `Q` are designed to concatenate conditions in conjunction with query before `WHERE` .

Constructed condition results in `Conditional` object and `where` extracts `WHERE` clause and parameters from it.
Due to that, query operation code can be divided into condition construction phase and query formatting phase clearly.

```python
cond = Q.eq("t", c1=1) & Q.lt("t", c2=2)
w, params = where(cond)
db.stmt().execute("SELECT * FROM table AS t {w} LIMIT $_ OFFSET $_", *params, 10, 5)
# SQL: SELECT * FROM table AS t WHERE t.c1 = 1 AND t.c2 < 2 LIMIT 10 OFFSET 5
```
"""
from collections.abc import Sequence, Mapping
from functools import reduce
from itertools import chain
from typing import Any, Callable, Union, Generic, Optional, Protocol, TYPE_CHECKING
from typing_extensions import Never, Self, TypeVarTuple, Unpack, NotRequired


QA = TypeVarTuple('QA')
if TYPE_CHECKING:
    # TODO: There are no correct hinting expression for optional arguments in python <= 3.10.
    class Queryable(Protocol, Generic[Unpack[QA]]):
        def eq(self, *args: Unpack[QA]) -> 'Conditional': ...
        def neq(self, *args: Unpack[QA]) -> 'Conditional': ...
        def in_(self, *args: Unpack[QA]) -> 'Conditional': ...
        def not_in(self, *args: Unpack[QA]) -> 'Conditional': ...
        def match(self, *args: Unpack[QA]) -> 'Conditional': ...
        def like(self, *args: Unpack[QA]) -> 'Conditional': ...
        def startswith(self, *args: Unpack[QA]) -> 'Conditional': ...
        def endswith(self, *args: Unpack[QA]) -> 'Conditional': ...
        def lt(self, *args: Unpack[QA]) -> 'Conditional': ...
        def le(self, *args: Unpack[QA]) -> 'Conditional': ...
        def gt(self, *args: Unpack[QA]) -> 'Conditional': ...
        def ge(self, *args: Unpack[QA]) -> 'Conditional': ...

    class QueryableT(Generic[Unpack[QA]]):
        def eq(self, *args: Unpack[QA]) -> 'Conditional': ...
        def neq(self, *args: Unpack[QA]) -> 'Conditional': ...
        def in_(self, *args: Unpack[QA]) -> 'Conditional': ...
        def not_in(self, *args: Unpack[QA]) -> 'Conditional': ...
        def match(self, *args: Unpack[QA]) -> 'Conditional': ...
        def like(self, *args: Unpack[QA]) -> 'Conditional': ...
        def startswith(self, *args: Unpack[QA]) -> 'Conditional': ...
        def endswith(self, *args: Unpack[QA]) -> 'Conditional': ...
        def lt(self, *args: Unpack[QA]) -> 'Conditional': ...
        def le(self, *args: Unpack[QA]) -> 'Conditional': ...
        def gt(self, *args: Unpack[QA]) -> 'Conditional': ...
        def ge(self, *args: Unpack[QA]) -> 'Conditional': ...
else:
    class Queryable(Protocol, Generic[Unpack[QA]]):
        pass
    class QueryableT(Generic[Unpack[QA]]):
        pass


class Q:
    """
    This class provides utility class methods creating conditions.

    Using `of()` is the most simple way to create a condition clause with parameters.

    ```python
    >>> Q.of("a = $_", 1)
    Condition: 'a = $_' -- [1]
    ```

    Other utility methods correspond to basic operators defined in SQL.
    They takes keyword arguments and create conditions by applying operator to each item respectively.

    ```python
    >>> Q.eq(a=1)
    Condition: 'a = %s' -- [1]
    >>> Q.in_(a=[1, 2, 3])
    Condition: 'a IN (%s, %s, %s)' -- [1, 2, 3]
    >>> Q.like(a="abc")
    Condition: 'a LIKE %s' -- ["%abc%"]
    ```

    Multiple arguments generates a condition which concatenates conditions with logical operator, by default `AND` .

    ```python
    >>> Q.eq(a=1, b=2)
    Condition: 'a = %s AND b = %s' -- [1, 2]
    ```

    Those methods also accept table alias which is prepended to columns.

    ```python
    >>> Q.eq("t", a=1, b=2)
    Condition: 't.a = %s AND t.b = %s'
    ```

    Additionally, the instance of this class has its own functionality to generate condition.

    Each parameter passed to the constructor becomes an instance method of the instance,
    which takes a condition clause including placeholders which will take parameters in query execution phase.
    `pyracmon.connection.Statement.execute` allows unified marker `$_` in spite of DB driver.

    ```python
    >>> q = Q(a=1)
    >>> q.a("a = $_")
    Condition: 'a = $_' -- [1]
    ```

    Method whose name is not passed to the constructor renders empty condition which has no effect on the query.

    ```python
    >>> q.b("b = $_")
    Condition: '' -- []
    ```

    By default, `None` is equivalent to not being passed. Giving `True` at the first argument in constructor changes the behavior.

    ```python
    >>> q = Q(a=1, b=None)
    >>> q.b("b = $_")
    Condition: '' -- []
    >>> q = Q(True, a=1, b=None)
    >>> q.b("b = $_")
    Condition: 'b = $_' -- [None]
    ```

    This feature simplifies a query construction in cases some parameters are absent.

    ```python
    >>> def search(db, q):
    >>>     w, params = where(q.a("a = $_") & q.b("b = $_"))
    >>>     db.stmt().execute(f"SELECT * FROM table {w}", *params)
    >>> 
    >>> search(db, Q(a=1))      # SELECT * FROM table WHERE a = 1
    >>> search(db, Q(a=1, b=2)) # SELECT * FROM table WHERE a = 1 AND b = 2
    >>> search(db, Q())         # SELECT * FROM table
    ```
    """
    class Attribute(QueryableT[str, Unpack[tuple[Any, ...]]]): # type: ignore
        def __init__(self, value):
            self.value = value

        def __call__(
            self,
            expression: Union[str, Callable[[Any], str]],
            convert: Optional[Union[Callable[[Any], Any], Any]] = None,
        ) -> 'Conditional':
            """
            Creates conditional object composed of given expression and the attribute value as parameters.

            Args:
                expression: A clause or a function generating a clause by taking the attribute value.
                convert: A function converting the attribute value to parameters.
                    If this function returns a value which is not a list, a list having only the value is used.
            Returns:
                Condition.
            """
            expression = expression if isinstance(expression, str) else expression(self.value)

            if callable(convert):
                params = convert(self.value)
            elif convert is not None:
                params = convert
            else:
                params = [self.value]

            return Conditional(expression, params if isinstance(params, list) else [params])

        @property
        def all(self) -> 'Q.Attribute':
            """
            Returns composite attribute which applies conditions to every values iterated from attribute value and join them with `AND`.

            Returns:
                Composite attribute.
            """
            return Q.CompositeAttribute(self.value, True)

        @property
        def any(self) -> 'Q.Attribute':
            """
            Returns composite attribute which applies conditions to every values iterated from attribute value and join them with `OR`.

            Returns:
                Composite attribute.
            """
            return Q.CompositeAttribute(self.value, False)

        def __bool__(self):
            return True

        def __and__(self, other: 'Conditional') -> 'Conditional':
            return other if bool(self.value) else Conditional()

        def __or__(self, other: 'Conditional') -> 'Conditional':
            return other if not bool(self.value) else Conditional()

        def __getattr__(self, key):
            """
            Exposes a method which works similarly to 'Q' 's utility method of the same name.

            ```python
            >>> q = Q(a = 1)
            >>> q.a.eq("col")
            Condition: 'col = $_' -- [1]
            >>>
            >>> q.a.eq("col", None, "t")
            Condition: 't.col = $_' -- [1]
            >>>
            >>> q.a.eq("col", lambda x: x*2, "t")
            Condition: 't.col = $_' -- [2]
            ```
            """
            method = getattr(Q, key)
            def invoke(col, convert=None, *args, **kwargs):
                if callable(convert):
                    value = convert(self.value)
                else:
                    value = convert if convert is not None else self.value
                kwargs.update({col: value})
                return method(*args, **kwargs)
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
            def invoke(col, convert=None, *args, **kwargs):
                def conv(v):
                    if callable(convert):
                        return convert(v)
                    else:
                        # REVIEW Replacing every parameter in the list with the same value is meaningless?
                        return convert if convert is not None else v
                conds = [method(*args, **dict(chain(kwargs.items(), [(col, conv(v))]))) for v in self.value]
                return Conditional.all(conds) if self._and else Conditional.any(conds)
            return invoke

    class NoAttribute(Attribute):
        def __init__(self):
            super().__init__(None)

        def __call__(self, expression, holder=lambda x:x):
            return Conditional()

        @property
        def all(self):
            return self

        @property
        def any(self):
            return self

        def __bool__(self):
            return False

        def __and__(self, other: 'Conditional') -> 'Conditional':
            return Conditional()

        def __or__(self, other: 'Conditional') -> 'Conditional':
            return Conditional()

        def __getattr__(self, key):
            method = getattr(Q, key)
            def invoke(col, convert=None, *args):
                return Conditional()
            return invoke

    def __init__(self, _include_none_: bool = False, **kwargs: Any):
        """
        Initializes an instance.

        Args:
            _include_none_: Whether include attributes whose value is `None`.
            kwargs: Denotes pairs of attribute name and parameter.
        """
        self.attributes = dict([(k, v) for k, v in kwargs.items() if _include_none_ or v is not None])

    def __getattr__(self, key) -> Attribute:
        if key in self.attributes:
            return Q.Attribute(self.attributes[key])
        else:
            return Q.NoAttribute()

    @classmethod
    def of(cls, expression: str = "", *params: Any) -> 'Conditional':
        """
        Creates a condition directly from an expression and parameters.

        Args:
            expression: Condition expression.
            params: Parameters used in the condition.
        Returns:
            Condition object.
        """
        return Conditional(expression, list(params))

    @classmethod
    def eq(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Creates a condition applying `=` operator to columns.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NULL", []
            elif val is True:
                return f"{col}", []
            elif val is False:
                return f"NOT {col}", []
            return None
        return _conditional("=", _and_, kwargs, is_null, _alias_)

    @classmethod
    def neq(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `!=`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        def is_null(col, val):
            if val is None:
                return f"{col} IS NOT NULL", []
            elif val is True:
                return f"NOT {col}", []
            elif val is False:
                return f"{col}", []
            return None
        return _conditional("!=", _and_, kwargs, is_null, _alias_)

    @classmethod
    def in_(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Sequence[Any]) -> 'Conditional':
        """
        Works like `eq`, but applies `IN`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "1 = 0", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} IN ({holder})", val
        return _conditional("IN", _and_, kwargs, in_list, _alias_)

    @classmethod
    def not_in(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Sequence[Any]) -> 'Conditional':
        """
        Works like `eq`, but applies `NOT IN`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} NOT IN ({holder})", val
        return _conditional("NOT IN", _and_, kwargs, in_list, _alias_)

    @classmethod
    def match(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. Given parameters will be passed to query without being escaped or enclosed.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("LIKE", _and_, kwargs, None, _alias_)

    @classmethod
    def like(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. Given parameters will be escaped and enclosed with wildcards (%) to execute partial match.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("LIKE", _and_, {k: f"%{escape_like(v)}%" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def startswith(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. Given parameters will be escaped and appended with wildcards (%) to execute prefix match.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("LIKE", _and_, {k: f"{escape_like(v)}%" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def endswith(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. Given parameters will be escaped and prepended with wildcards (%) to execute backward match.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("LIKE", _and_, {k: f"%{escape_like(v)}" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def lt(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `<`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("<", _and_, kwargs, None, _alias_)

    @classmethod
    def le(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `<=`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional("<=", _and_, kwargs, None, _alias_)

    @classmethod
    def gt(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `>`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional(">", _and_, kwargs, None, _alias_)

    @classmethod
    def ge(cls, _alias_: Optional[str] = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `>=`.

        Args:
            _alias_: Table alias.
            _and_: Specifies concatenating logical operator is `AND` or `OR`.
            kwargs: Column names and parameters.
        Returns:
            Condition object.
        """
        return _conditional(">=", _and_, kwargs, None, _alias_)


def _conditional(op, and_, column_values, gen=None, alias=None) -> 'Conditional':
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
    """
    Abstraction of expression in any query.
    """
    def __init__(self, expression: str, params: list[Any]):
        #: Expression string.
        self.expression = expression
        #: Parameters corresponding to placeholders in the expression.
        self.params = params


class Conditional(Expression):
    """
    Represents a query condition composed of an expression and parameters.

    Parameters must be a list where the index of each parameter matches the index of placeholder for it.
    The expression accepts only the unified marker `$_`.

    Applying logical operators such as `&`, `|` and `~` generates new condition.

    ```python
    >>> c1 = Q.of("a = $_", 0)
    >>> c2 = Q.of("b < $_", 1)
    >>> c3 = Q.of("c > $_", 2)
    >>> c = ~(c1 & c2 | c3)
    >>> c
    Condition: NOT (((a = $_) AND (b < $_)) OR (c > $_)) -- [0, 1, 2]
    ```
    """
    @classmethod
    def all(cls, conditionals: Sequence['Conditional']) -> 'Conditional':
        """
        Concatenates condition objects with `AND`.

        Args:
            conditionals: Condition objects.
        Returns:
            Concatenated condition object.
        """
        return reduce(lambda acc, c: acc & c, conditionals, Conditional())

    @classmethod
    def any(cls, conditionals: Sequence['Conditional']) -> 'Conditional':
        """
        Concatenates condition objects with `OR`.

        Args:
            conditionals: Condition objects.
        Returns:
            Concatenated condition object.
        """
        if len(conditionals) == 0:
            return Conditional("1 = 0")
        return reduce(lambda acc, c: acc | c, conditionals, Conditional())

    def __init__(self, expression="", params=None):
        super().__init__(expression, params or [])

    def __repr__(self):
        return f"Condition: '{self.expression}' -- {self.params}"

    def __and__(self, other) -> 'Conditional':
        expression = ""
        if self.expression and other.expression:
            expression = f"({self.expression}) AND ({other.expression})"
        elif self.expression:
            expression = self.expression
        elif other.expression:
            expression = other.expression

        return Conditional(expression, self.params + other.params)

    def __or__(self, other) -> 'Conditional':
        expression = ""
        if self.expression and other.expression:
            expression = f"({self.expression}) OR ({other.expression})"
        elif self.expression:
            expression = self.expression
        elif other.expression:
            expression = other.expression

        return Conditional(expression, self.params + other.params)

    def __invert__(self) -> 'Conditional':
        if self.expression:
            return Conditional(f"NOT ({self.expression})", self.params)
        else:
            return Conditional(f"1 = 0", [])


def escape_like(v: str) -> str:
    """
    Escape a string for the use in `LIKE` condition.

    Args:
        v: A string.
    Returns:
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


def where(condition: 'Conditional') -> tuple[str, list[Any]]:
    """
    Generates a `WHERE` clause and parameters representing given condition.

    If the condition is empty, returned clause is an empty string which does not contain `WHERE` keyword.

    Args:
        condition: Condition object.
    Returns:
        Tuple of `WHERE` clause and parameters.
    """
    return ('', []) if condition.expression == '' else (f'WHERE {condition.expression}', condition.params)