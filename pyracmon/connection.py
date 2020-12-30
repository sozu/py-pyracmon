import string
import threading
from datetime import datetime
from .query import QueryHelper
from .sql import Sql
from .marker import Marker
from .context import ConnectionContext


def connect(api, *args, **kwargs):
    """
    Connect to DB by passing arguments to DB-API 2.0 module.

    Every optional argument is passed to `api.connect()` and returns the `Connection` object which wraps obtained DB connection.

    Parameters
    ----------
    api: module
        DB-API 2.0 module which should export `connect()` function.

    Returns
    -------
    Connection
        Wrapper of DB-API 2.0 connection.
    """
    return Connection(api, api.connect(*args, **kwargs), None)


class Connection:
    """
    Wrapper class of DB-API 2.0 Connection.

    Every instance works as the proxy object to original connection, therefore any attribute in it is still available.
    """
    _characters = string.ascii_letters + string.digits + ".="

    def __init__(self, api, conn, context_factory=None):
        self.identifier = self._gen_identifier()
        self.api = api
        self.conn = conn
        self.context_factory = context_factory
        self._context = None

    def __getattr__(self, name):
        return getattr(self.conn, name)

    def __enter__(self):
        if hasattr(self.conn, "__enter__"):
            self.conn.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self.conn, "__exit__"):
            self.conn.__exit__(exc_type, exc_value, traceback)
        else:
            if exc_value is None:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()

    def _gen_identifier(self):
        t = int(datetime.now().timestamp() * 1000)

        def gen(x):
            base = len(Connection._characters)
            while x >= base:
                x, r = divmod(x, base)
                yield Connection._characters[r]
            yield Connection._characters[x]

        return threading.current_thread().name + "-" + ''.join(gen(t))[::-1]

    @property
    def helper(self):
        """
        Deprecated. Returns `QueryHelper` object.
        """
        return QueryHelper(self.api, None)

    @property
    def context(self):
        """
        `ConnectionContext` used for this connection.

        Returns
        -------
        ConnectionContext
            `ConnectionContext` used for this connection.
        """
        if not self._context:
            self._context = (self.context_factory or ConnectionContext)()
            self._context.identifier = self.identifier
        return self._context

    def use(self, factory):
        """
        Set factory function of `ConnectionContext`.

        Parameters
        ----------
        factory: () -> ConnectionContext
            A factory function.

        Returns
        -------
        Connection
            This instance.
        """
        self.context_factory = factory
        return self

    def stmt(self, context=None):
        """
        Creates new statement which provides methods to execute query.

        Parameters
        ----------
        context: ConnectionContext
            `ConnectionContext` used in the statement. If `None`, the context of this connection is used.

        Returns
        -------
        Statement
            Created statement.
        """
        return Statement(self, context or self.context)


class Statement:
    """
    This class has the functionality to execute query on containing connection and context.
    """
    def __init__(self, conn, context):
        self.conn = conn
        self.context = context

    def prepare(self, sql, *args, **kwargs):
        """
        Generates a prepared SQL statement and its parameters.

        Parameters
        ----------
        sql: str
            SQL template.
        args: [object]
            Indexed parameters in the SQL.
        kwargs: {str: object}
            Keyword parameters in the SQL.

        Returns
        -------
        str
            Prepared SQL statement.
        [object] | {str:object}
            Paremeters for created statement.
        """
        paramstyle = self.context.config.paramstyle or self.conn.api.paramstyle

        sql = Sql(Marker.of(paramstyle), sql)

        return sql.render(*args, **kwargs)

    def execute(self, sql, *args, **kwargs):
        """
        Executes a query and returns a cursor object used for the execution.

        Parameters
        ----------
        sql: str
            SQL template.
        args: [object]
            Indexed parameters in the SQL.
        kwargs: {str: object}
            Keyword parameters in the SQL.

        Returns
        -------
        Cursor
            Cursor object which has been used for the query execution.
        """
        sql, params = self.prepare(sql, *args, **kwargs)

        c = self.conn.cursor()

        return self.context.execute(c, sql, params)