"""
This module provides types and functions for DB connections.
"""
from collections.abc import Sequence, Callable
import secrets
import string
import threading
import types
from typing import Any, Callable, Optional, Union
from typing_extensions import Self
from . import dbapi
from .sql import Sql
from .marker import Marker
from .context import ConnectionContext, PARAMS


def connect(api: types.ModuleType, *args: Any, **kwargs: Any) -> 'Connection':
    """
    Connects to DB by passing arguments to DB-API 2.0 module.

    Every optional argument is passed to `api.connect` and returns the `Connection` object which wraps obtained DB connection.

    ```python
    import psycopg2
    from pyracmon import connect
    db = connect(psycopg2, host="localhost", port=5432, dbname="pyracmon", user="postgres", password="postgres")
    c = db.stmt().execute("SELECT 1")
    assert c.fetchone()[0] == 1
    ```

    Args:
        api: DB-API 2.0 module which exports `connect` function.
        args: Positional arguments passed to `api.connect`.
        kwargs: Keyword arguments passed to `api.connect`.
    Returns:
        Wrapper of DB-API 2.0 connection.
    """
    return Connection(api, api.connect(*args, **kwargs), None)


class Connection(dbapi.Connection):
    """
    Wrapper class of DB-API 2.0 Connection.

    Every instance works as the proxy object to original connection, therefore any attribute in it is still available.
    """
    _characters = string.ascii_letters + string.digits + ".="

    def __init__(self, api, conn: dbapi.Connection, context_factory: Optional[Callable[[], ConnectionContext]] = None):
        #: A string which identifies a connection.
        self.identifier = self._gen_identifier()
        #: DB-API 2.0 module.
        self.api = api
        #: Original connection object.
        self.conn = conn
        self._context_factory = context_factory
        self._context = None

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def __enter__(self):
        if hasattr(self.conn, "__enter__"):
            self.conn.__enter__() # type: ignore
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self.conn, "__exit__"):
            self.conn.__exit__(exc_type, exc_value, traceback) # type: ignore
        else:
            if exc_value is None:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

    def _gen_identifier(self):
        return threading.current_thread().name + "-" + secrets.token_hex(4)

    @property
    def context(self) -> ConnectionContext:
        """
        Context object used for this connection.
        """
        if not self._context:
            self._context = (self._context_factory or ConnectionContext)()
            self._context.identifier = self.identifier
        return self._context

    def close(self) -> None:
        return self.conn.close()

    def commit(self) -> None:
        return self.conn.commit()

    def rollback(self) -> None:
        return self.conn.rollback()

    def cursor(self) -> dbapi.Cursor:
        return self.conn.cursor()

    def use(self, factory: Callable[[], ConnectionContext]) -> Self:
        """
        Sets factory function of `ConnectionContext` to use custom context.

        When the context is already set, it will be replaced with new one.

        Args:
            factory: Function returning custom context.
        Returns:
            This instance.
        """
        self._context_factory = factory
        self._context = None
        return self

    def stmt(self, context: Optional[ConnectionContext] = None) -> 'Statement':
        """
        Creates new `Statement` which executes queries on this connection.

        Args:
            context: Context object used in the statement. If `None`, the context of this connection is used.
        Returns:
            Created statement.
        """
        return Statement(self, context or self.context)


class Statement:
    """
    This class has methods to execute query on containing connection and context.

    Be sure to execute queries on this class to benefit from:

    - Query formatting using unified marker `$_`.
    - Query logging.
    """
    def __init__(self, conn: Connection, context: ConnectionContext):
        self.conn = conn
        self.context = context

    def prepare(self, sql: str, *args: Any, **kwargs: Any) -> tuple[str, PARAMS]:
        """
        Generates formatted query and a list of parameters.

        This method is invoked internally from `execute` to generate actual query and parameters.

        Args:
            sql: Query template which can contain unified marker.
            args: Positional parameters of query.
            kwargs: Keyword parameters of query.
        Returns:
            Formatted query and parameters.
        """
        paramstyle = self.context.config.paramstyle or self.conn.api.paramstyle

        return Sql(Marker.of(paramstyle), sql).render(*args, **kwargs)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> dbapi.Cursor:
        """
        Executes a query and returns a cursor object.

        Args:
            sql: Query template which can contain unified marker.
            args: Positional parameters of query.
            kwargs: Keyword parameters of query.
        Returns:
            Cursor object used for the query execution.
        """
        sql, params = self.prepare(sql, *args, **kwargs)

        c = self.conn.cursor()

        return self.context.execute(c, sql, params)

    def executemany(self, sql: str, seq_of_args: Sequence[PARAMS]) -> dbapi.Cursor:
        """
        Executes a query for each parameters in `seq_of_args` and returns a cursor object.

        Args:
            sql: Query template which can contain unified marker.
            seq_of_args: A sequence of parameters of the query.
        Returns:
            Cursor object used for the query execution.
        """
        def prepare(ps: Union[list[Any], dict[str, Any]]):
            args = list(ps) if isinstance(ps, (list, tuple)) else []
            kwargs = ps if isinstance(ps, dict) else {}
            return self.prepare(sql, *args, **kwargs)

        rendered, params = prepare(seq_of_args[0])
        seq_of_params: list[PARAMS] = [params]

        for i, ps in enumerate(seq_of_args[1:]):
            _, params = prepare(ps)
            seq_of_params.append(params)

        c = self.conn.cursor()

        return self.context.executemany(c, rendered, seq_of_params)