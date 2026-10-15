"""
This module exports types and functions for query construction.

`Q` is the factory class for query conditions, that is, the `WHERE` clause.
Class methods on `Q` are designed to be combined with each other to build the condition for the `WHERE` clause.

A condition created via a `Q` method is a `Conditional` object, which contains an expression and parameters for a `WHERE` clause.
As a result, query operation code can be clearly divided into a condition-construction phase and a query-formatting phase.

```python
cond = Q.eq("t", c1=1) & Q.lt("t", c2=2)
w, params = where(cond)
db.stmt().execute("SELECT * FROM table AS t {w} LIMIT $_ OFFSET $_", *params, 10, 5)
# SQL: SELECT * FROM table AS t WHERE t.c1 = 1 AND t.c2 < 2 LIMIT 10 OFFSET 5
```
"""
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import reduce
from itertools import chain
from typing import Any, Generic, Protocol, TypeVarTuple, Unpack, TYPE_CHECKING


QA = TypeVarTuple('QA')
if TYPE_CHECKING:
    # TODO: There are no correct hinting expression for optional arguments in python <= 3.10.
    class Queryable(Protocol, Generic[Unpack[QA]]):
        def eq(self, *args: Unpack[QA]) -> Conditional: ...
        def neq(self, *args: Unpack[QA]) -> Conditional: ...
        def in_(self, *args: Unpack[QA]) -> Conditional: ...
        def not_in(self, *args: Unpack[QA]) -> Conditional: ...
        def match(self, *args: Unpack[QA]) -> Conditional: ...
        def like(self, *args: Unpack[QA]) -> Conditional: ...
        def startswith(self, *args: Unpack[QA]) -> Conditional: ...
        def endswith(self, *args: Unpack[QA]) -> Conditional: ...
        def lt(self, *args: Unpack[QA]) -> Conditional: ...
        def le(self, *args: Unpack[QA]) -> Conditional: ...
        def gt(self, *args: Unpack[QA]) -> Conditional: ...
        def ge(self, *args: Unpack[QA]) -> Conditional: ...

    class QueryableT(Generic[Unpack[QA]]):
        def eq(self, *args: Unpack[QA]) -> Conditional: ...
        def neq(self, *args: Unpack[QA]) -> Conditional: ...
        def in_(self, *args: Unpack[QA]) -> Conditional: ...
        def not_in(self, *args: Unpack[QA]) -> Conditional: ...
        def match(self, *args: Unpack[QA]) -> Conditional: ...
        def like(self, *args: Unpack[QA]) -> Conditional: ...
        def startswith(self, *args: Unpack[QA]) -> Conditional: ...
        def endswith(self, *args: Unpack[QA]) -> Conditional: ...
        def lt(self, *args: Unpack[QA]) -> Conditional: ...
        def le(self, *args: Unpack[QA]) -> Conditional: ...
        def gt(self, *args: Unpack[QA]) -> Conditional: ...
        def ge(self, *args: Unpack[QA]) -> Conditional: ...
else:
    class Queryable(Protocol, Generic[Unpack[QA]]):
        pass
    class QueryableT(Generic[Unpack[QA]]):
        pass


class Q:
    """
    This class provides utility class methods that create conditions.

    Using `of()` is the simplest way to create a condition with parameters.

    ```python
    >>> Q.of("a = $_", 1)
    Condition: 'a = $_' -- [1]
    ```

    Other utility methods correspond to basic operators defined in SQL.
    They take keyword arguments and create a condition by applying the operator to each of them.

    ```python
    >>> Q.eq(a=1)
    Condition: 'a = %s' -- [1]
    >>> Q.in_(a=[1, 2, 3])
    Condition: 'a IN (%s, %s, %s)' -- [1, 2, 3]
    >>> Q.like(a="abc")
    Condition: 'a LIKE %s' -- ["%abc%"]
    ```

    Multiple arguments generate a single condition that concatenates the conditions for each argument with a logical operator, `AND` by default.

    ```python
    >>> Q.eq(a=1, b=2)
    Condition: 'a = %s AND b = %s' -- [1, 2]
    ```

    Those methods also accept a table alias, which is prepended to the columns.

    ```python
    >>> Q.eq("t", a=1, b=2)
    Condition: 't.a = %s AND t.b = %s'
    ```

    Additionally, an instance of this class has its own functionality for generating conditions.

    Each parameter passed to the constructor is exposed as a method on the instance,
    which takes a condition clause containing placeholders that will be filled with parameters during query execution.
    `pyracmon.connection.Statement.execute` allows the unified marker `$_` to be used regardless of the DB driver.

    ```python
    >>> q = Q(a=1)
    >>> q.a("a = $_")
    Condition: 'a = $_' -- [1]
    ```

    A method whose name was not passed to the constructor renders an empty condition that has no effect on the query.

    ```python
    >>> q.b("b = $_")
    Condition: '' -- []
    ```

    By default, `None` is equivalent to the parameter not being passed. Passing `True` as the first argument to the constructor changes this behavior.

    ```python
    >>> q = Q(a=1, b=None)
    >>> q.b("b = $_")
    Condition: '' -- []
    >>> q = Q(True, a=1, b=None)
    >>> q.b("b = $_")
    Condition: 'b = $_' -- [None]
    ```

    This feature simplifies query construction in cases where some parameters are absent.

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
        def __init__(self, value: Any):
            self.value = value

        def __call__(
            self,
            expression: str | Callable[[Any], str],
            convert: Callable[[Any], Any] | Any | None = None,
        ) -> 'Conditional':
            """
            Creates a `Conditional` object composed of the given expression and the attribute value as its parameters.

            Args:
                expression: A clause, or a function that generates a clause from the attribute value.
                convert: A function that converts the attribute value into parameters.
                    If this function returns a value that is not a list, a list containing only that value is used.
            Returns:
                A `Conditional` object.
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
            Returns a composite attribute that applies a condition to every value iterated from the attribute value, and joins them with `AND`.

            Returns:
                A composite attribute.
            """
            return Q.CompositeAttribute(self.value, True)

        @property
        def any(self) -> 'Q.Attribute':
            """
            Returns a composite attribute that applies a condition to every value iterated from the attribute value, and joins them with `OR`.

            Returns:
                A composite attribute.
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
            Exposes a method that works similarly to `Q`'s utility method of the same name.

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
            _include_none_: Whether to include attributes whose value is `None`.
            kwargs: Pairs of attribute names and their parameter values.
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
            expression: The condition expression.
            params: The parameters used in the condition.
        Returns:
            A `Conditional` object.
        """
        return Conditional(expression, list(params))

    @classmethod
    def eq(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Creates a condition that applies the `=` operator to columns.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        def is_null(col: str, val: Any) -> tuple[str, list[Any]] | None:
            if val is None:
                return f"{col} IS NULL", []
            elif val is True:
                return f"{col}", []
            elif val is False:
                return f"NOT {col}", []
            return None
        return _conditional("=", _and_, kwargs, is_null, _alias_)

    @classmethod
    def neq(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `!=`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        def is_null(col: str, val: Any) -> tuple[str, list[Any]] | None:
            if val is None:
                return f"{col} IS NOT NULL", []
            elif val is True:
                return f"NOT {col}", []
            elif val is False:
                return f"{col}", []
            return None
        return _conditional("!=", _and_, kwargs, is_null, _alias_)

    @classmethod
    def in_(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Sequence[Any]) -> 'Conditional':
        """
        Works like `eq`, but applies `IN`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "1 = 0", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} IN ({holder})", val
        return _conditional("IN", _and_, kwargs, in_list, _alias_)

    @classmethod
    def not_in(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Sequence[Any]) -> 'Conditional':
        """
        Works like `eq`, but applies `NOT IN`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        def in_list(col, val):
            if len(val) == 0:
                return "", []
            else:
                holder = ', '.join(['$_'] * len(val))
                return f"{col} NOT IN ({holder})", val
        return _conditional("NOT IN", _and_, kwargs, in_list, _alias_)

    @classmethod
    def match(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. The given parameters are passed to the query without being escaped or enclosed.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("LIKE", _and_, kwargs, None, _alias_)

    @classmethod
    def like(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. The given parameters are escaped and enclosed with wildcards (`%`) to perform a partial match.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("LIKE", _and_, {k: f"%{escape_like(v)}%" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def startswith(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. The given parameters are escaped and appended with a wildcard (`%`) to perform a prefix match.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("LIKE", _and_, {k: f"{escape_like(v)}%" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def endswith(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: str) -> 'Conditional':
        """
        Works like `eq`, but applies `LIKE`. The given parameters are escaped and prepended with a wildcard (`%`) to perform a suffix match.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("LIKE", _and_, {k: f"%{escape_like(v)}" for k, v in kwargs.items()}, None, _alias_)

    @classmethod
    def lt(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `<`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("<", _and_, kwargs, None, _alias_)

    @classmethod
    def le(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `<=`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional("<=", _and_, kwargs, None, _alias_)

    @classmethod
    def gt(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `>`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional(">", _and_, kwargs, None, _alias_)

    @classmethod
    def ge(cls, _alias_: str | None = None, _and_: bool = True, **kwargs: Any) -> 'Conditional':
        """
        Works like `eq`, but applies `>=`.

        Args:
            _alias_: An optional table alias prepended to the column names.
            _and_: Specifies whether the concatenating logical operator is `AND` or `OR`.
            kwargs: The column names and their parameter values.
        Returns:
            A `Conditional` object.
        """
        return _conditional(">=", _and_, kwargs, None, _alias_)


def _conditional(
    op: str,
    and_: bool,
    column_values: dict[str, Any],
    gen: Callable[[str, Any], tuple[str, list[Any]] | None] | None = None,
    alias: str | None = None,
) -> 'Conditional':
    cond = Conditional()

    def concat(c):
        nonlocal cond
        if and_:
            cond &= c
        else:
            cond |= c

    for col, val in column_values.items():
        if gen:
            acol = f"{alias}.{col}" if alias else col

            r = gen(acol, val)
            if r is not None:
                concat(Conditional(r[0], r[1]))
                continue

        acol = f"{alias}.{col}" if alias else col

        concat(Conditional(f"{acol} {op} $_", [val]))

    return cond


class Expression(ABC):
    """
    An abstraction of an expression in a query.
    """
    @property
    @abstractmethod
    def expression(self) -> str:
        """
        The expression string.
        """
        ...

    @property
    @abstractmethod
    def params(self) -> list[Any]:
        """
        The parameters corresponding to the placeholders in the expression.
        """
        ...


class Conditional(Expression):
    """
    Represents a query condition composed of an expression and its parameters.

    The parameters must be a list where the index of each parameter matches the index of the placeholder for it.
    The expression accepts only the unified marker `$_`.

    Applying logical operators such as `&`, `|`, and `~` generates a new condition.

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
        Concatenates the given condition objects with `AND`.

        If `conditionals` is empty, an empty condition that has no effect on the query is returned.

        Args:
            conditionals: The condition objects to concatenate.
        Returns:
            The concatenated condition object.
        """
        return reduce(lambda acc, c: acc & c, conditionals, Conditional())

    @classmethod
    def any(cls, conditionals: Sequence['Conditional']) -> 'Conditional':
        """
        Concatenates the given condition objects with `OR`.

        If `conditionals` is empty, a condition that is always false (`1 = 0`) is returned.

        Args:
            conditionals: The condition objects to concatenate.
        Returns:
            The concatenated condition object.
        """
        if len(conditionals) == 0:
            return Conditional("1 = 0")
        return reduce(lambda acc, c: acc | c, conditionals, Conditional())

    def __init__(self, expression: str = "", params: list[Any] | None = None):
        super().__init__()
        self._expression = expression
        self._params: list[Any] = params or []

    @property
    def expression(self) -> str:
        return self._expression

    @property
    def params(self) -> list[Any]:
        return self._params

    def __repr__(self):
        return f"Condition: '{self.expression}' -- {self.params}"

    def __and__(self, other: 'Conditional') -> 'Conditional':
        expression = ""
        if self.expression and other.expression:
            expression = f"({self.expression}) AND ({other.expression})"
        elif self.expression:
            expression = self.expression
        elif other.expression:
            expression = other.expression

        return Conditional(expression, self.params + other.params)

    def __or__(self, other: 'Conditional') -> 'Conditional':
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
    Escape a string for use in a `LIKE` condition.

    Args:
        v: The string to escape.
    Returns:
        The escaped string.
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


def where(condition: Conditional) -> tuple[str, list[Any]]:
    """
    Generates a `WHERE` clause and its parameters from the given condition.

    If the condition is empty, the returned clause is an empty string that does not contain the `WHERE` keyword.

    Args:
        condition: The condition object.
    Returns:
        A tuple of the `WHERE` clause and its parameters.
    """
    return ('', []) if condition.expression == '' else (f'WHERE {condition.expression}', condition.params)