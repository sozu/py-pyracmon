"""
This module provides types and functions for DB connections.
"""
from abc import ABC, abstractmethod
from collections.abc import Sequence, Callable
import secrets
import string
import threading
import types
from typing import Any, Self
from . import dbapi
from .sql import Sql
from .marker import Marker
from .context import ConnectionContext, AsyncConnectionContext, PARAMS


#----------------------------------------------------------------
# Connection
#----------------------------------------------------------------
def connect(api: types.ModuleType, *args: Any, **kwargs: Any) -> 'Connection':
    """
    Connects to a DB by passing arguments to a DB-API 2.0 module.

    All `args` and `kwargs` are passed to `api.connect`, and its result is wrapped in the returned `Connection` object.

    ```python
    import psycopg2
    from pyracmon import connect
    db = connect(psycopg2, host="localhost", port=5432, dbname="pyracmon", user="postgres", password="postgres")
    c = db.stmt().execute("SELECT 1")
    assert c.fetchone()[0] == 1
    ```

    Args:
        api: A DB-API 2.0 module that exports a `connect` function.
        args: Positional arguments passed to `api.connect`.
        kwargs: Keyword arguments passed to `api.connect`.
    Returns:
        A wrapper around the DB-API 2.0 connection.
    """
    return Connection(api, api.connect(*args, **kwargs), None)


async def aconnect(connector: Any, *args: Any, api: types.ModuleType | None = None, **kwargs: Any) -> 'AsyncConnection':
    """
    Asyncio version of `connect`.

    Args:
        connector: Any kind of object which has the `connect` function returning an awaitable asynchronous connection.
        api: A DB-API 2.0 module if `connector` is not a DB-API 2.0 module. If `None`, `connector` is assumed to be a DB-API 2.0 module.
        args: Positional arguments passed to `api.connect`.
        kwargs: Keyword arguments passed to `api.connect`.
    Returns:
        A wrapper around the asynchronous connection.
    """
    conn = await connector(*args, **kwargs)
    return AsyncConnection(api or connector, conn, None)


class ConnectionBase[CONN, CXT: ConnectionContext](ABC):
    _characters = string.ascii_letters + string.digits + ".="

    def __init__(self, api, conn: CONN, context_factory: Callable[[], CXT] | None = None):
        #: A string that identifies this connection.
        self.identifier = self._gen_identifier()
        #: The DB-API 2.0 module.
        self.api = api
        #: The original connection object.
        self.conn = conn
        self._context_factory = context_factory
        self._context = None

    @classmethod
    @abstractmethod
    def _gen_context(cls) -> CXT:
        ...

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def _gen_identifier(self):
        return threading.current_thread().name + "-" + secrets.token_hex(4)

    @property
    def context(self) -> CXT:
        """
        The context object used for this connection.
        """
        if not self._context:
            self._context = (self._context_factory or self._gen_context)()
            self._context.identifier = self.identifier
        return self._context

    def use(self, factory: Callable[[], CXT]) -> Self:
        """
        Sets the factory function for `ConnectionContext` to use a custom context.

        When the context is already set, it is replaced with a new one.

        Args:
            factory: A function that returns a custom context.
        Returns:
            This instance.
        """
        self._context_factory = factory
        self._context = None
        return self


class Connection(ConnectionBase[dbapi.Connection, ConnectionContext], dbapi.Connection):
    """
    A wrapper class for a DB-API 2.0 `Connection`.

    Every instance works as a proxy for the original connection, so any attribute of the original connection is still accessible.
    """
    @classmethod
    def _gen_context(cls) -> ConnectionContext:
        return ConnectionContext()

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

    def close(self) -> None:
        return self.conn.close()

    def commit(self) -> None:
        return self.conn.commit()

    def rollback(self) -> None:
        return self.conn.rollback()

    def cursor(self) -> dbapi.Cursor:
        return self.conn.cursor()

    def stmt(self, context: ConnectionContext | None = None) -> 'Statement':
        """
        Creates a new `Statement` that executes queries on this connection.

        Args:
            context: The context object used by the statement. If `None`, the context of this connection is used.
        Returns:
            The created statement.
        """
        return Statement(self, context or self.context)


class AsyncConnection(ConnectionBase[dbapi.AsyncConnection, AsyncConnectionContext], dbapi.AsyncConnection):
    """
    Asyncio version of `Connection`.
    """
    @classmethod
    def _gen_context(cls) -> AsyncConnectionContext:
        return AsyncConnectionContext()

    async def close(self) -> None:
        return await self.conn.close()

    async def commit(self) -> None:
        return await self.conn.commit()

    async def rollback(self) -> None:
        return await self.conn.rollback()

    def cursor(self) -> dbapi.AsyncCursor:
        return self.conn.cursor()

    def stmt(self, context: AsyncConnectionContext | None = None) -> 'AsyncStatement':
        """
        Creates a new `Statement` that executes queries on this connection.

        Args:
            context: The context object used by the statement. If `None`, the context of this connection is used.
        Returns:
            The created statement.
        """
        return AsyncStatement(self, context or self.context)


#----------------------------------------------------------------
# Statement
#----------------------------------------------------------------
class StatementBase[CONN: Connection | AsyncConnection, CXT: ConnectionContext](ABC):
    """
    This class provides methods to execute queries on its connection, using its context.

    Executing queries via this class provides the following benefits:

    - Query formatting using the unified marker `$_`.
    - Query logging.
    """
    def __init__(self, conn: CONN, context: CXT):
        self.conn = conn
        self.context = context

    def _prepare_params(self, sql: str, params: PARAMS):
        args = list(params) if isinstance(params, (list, tuple)) else []
        kwargs = params if isinstance(params, dict) else {}
        return self.prepare(sql, *args, **kwargs)

    def _prepare_query(self, sql: str, seq_of_args: Sequence[PARAMS]) -> tuple[str, list[PARAMS]]:
        rendered, params = self._prepare_params(sql, seq_of_args[0])
        seq_of_params: list[PARAMS] = [params]

        for i, ps in enumerate(seq_of_args[1:]):
            _, params = self._prepare_params(sql, ps)
            seq_of_params.append(params)

        return rendered, seq_of_params

    def prepare(self, sql: str, *args: Any, **kwargs: Any) -> tuple[str, PARAMS]:
        """
        Generates a formatted query and a list of parameters.

        This method is invoked internally by `execute` to generate the actual query and its parameters.

        Args:
            sql: A query template that can contain unified markers.
            args: Positional parameters of the query.
            kwargs: Keyword parameters of the query.
        Returns:
            The formatted query and its parameters.
        """
        paramstyle = self.context.config.paramstyle or self.conn.api.paramstyle

        return Sql(Marker.of(paramstyle), sql).render(*args, **kwargs)


class Statement(StatementBase[Connection, ConnectionContext]):
    """
    This class provides methods to execute queries on its connection, using its context.

    Executing queries via this class provides the following benefits:

    - Query formatting using the unified marker `$_`.
    - Query logging.
    """
    def execute(self, sql: str, *args: Any, **kwargs: Any) -> dbapi.Cursor:
        """
        Executes a query and returns a cursor object.

        Args:
            sql: A query template that can contain unified markers.
            args: Positional parameters of the query.
            kwargs: Keyword parameters of the query.
        Returns:
            The cursor object used for the query execution.
        """
        sql, params = self.prepare(sql, *args, **kwargs)

        c = self.conn.cursor()

        return self.context.execute(c, sql, params)

    def executemany(self, sql: str, seq_of_args: Sequence[PARAMS]) -> dbapi.Cursor:
        """
        Executes the query once for each set of parameters in `seq_of_args` and returns a cursor object.

        Args:
            sql: A query template that can contain unified markers.
            seq_of_args: A sequence of parameter sets, one for each execution of the query.
        Returns:
            The cursor object used for the query execution.
        """
        rendered, seq_of_params = self._prepare_query(sql, seq_of_args)

        c = self.conn.cursor()

        return self.context.executemany(c, rendered, seq_of_params)


class AsyncStatement(StatementBase[AsyncConnection, AsyncConnectionContext]):
    """
    Asyncio version of `Statement`.
    """
    async def execute(self, sql: str, *args: Any, **kwargs: Any) -> dbapi.AsyncCursor:
        """
        Executes a query and returns a cursor object.

        Args:
            sql: A query template that can contain unified markers.
            args: Positional parameters of the query.
            kwargs: Keyword parameters of the query.
        Returns:
            The cursor object used for the query execution.
        """
        sql, params = self.prepare(sql, *args, **kwargs)

        c = self.conn.cursor()

        return await self.context.execute(c, sql, params)

    async def executemany(self, sql: str, seq_of_args: Sequence[PARAMS]) -> dbapi.AsyncCursor:
        """
        Executes the query once for each set of parameters in `seq_of_args` and returns a cursor object.

        Args:
            sql: A query template that can contain unified markers.
            seq_of_args: A sequence of parameter sets, one for each execution of the query.
        Returns:
            The cursor object used for the query execution.
        """
        rendered, seq_of_params = self._prepare_query(sql, seq_of_args)

        c = self.conn.cursor()

        return await self.context.executemany(c, rendered, seq_of_params)
