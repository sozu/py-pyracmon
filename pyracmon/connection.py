import string
import threading
import types
from typing import *
from datetime import datetime
from .query import QueryHelper
from .sql import Sql
from .marker import Marker
from .context import ConnectionContext


def connect(api: types.ModuleType, *args: Any, **kwargs: Any) -> 'Connection':
    """
    Connects to DB by passing arguments to DB-API 2.0 module.

    Every optional argument is passed to ``api.connect()`` and returns the `Connection` object which wraps obtained DB connection.

    Here shows an example connecting to database and executing query.

    >>> import psycopg2
    >>> from pyracmon import connect
    >>> db = connect(psycopg2, host="localhost", port=5432, dbname="pyracmon", user="postgres", password="postgres")
    >>> c = db.stmt().execute("SELECT 1")
    >>> assert c.fetchone()[0] == 1

    :param api: DB-API 2.0 module which exports `connect()` function.
    :param args: Arguments passed to ``api.connect()`` .
    :param kwargs: Keyword arguments passed to ``api.connect()`` .
    :returns: Wrapper of DB-API 2.0 connection.
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
    def context(self) -> ConnectionContext:
        """
        Returns context object used for this connection.

        :getter: Context object used for this connection.
        """
        if not self._context:
            self._context = (self.context_factory or ConnectionContext)()
            self._context.identifier = self.identifier
        return self._context

    def use(self, factory: Callable[[], ConnectionContext]) -> 'Connection':
        """
        Set factory function of `ConnectionContext`.

        Use this method to use your custom context object.

        :param factory: Function returning custom context object.
        :returns: This instance.
        """
        self.context_factory = factory
        return self

    def stmt(self, context: Optional[ConnectionContext] = None) -> 'Statement':
        """
        Creates new statement which provides methods to execute query.

        :param context: Context object used in the statement. If `None`, the context of this connection is used.
        :returns: Created statement.
        """
        return Statement(self, context or self.context)

    @property
    def helper(self) -> QueryHelper:
        """
        .. deprecated:: 1.0.0
        """
        return QueryHelper(self.api, None)


class Statement:
    """
    This class has method to execute query on containing connection and context.

    Be sure to execute queries on this class to benefit from:

    - Query formatting using unified marker ``$_``.
    - Query logging.
    """
    def __init__(self, conn, context):
        self.conn = conn
        self.context = context

    def prepare(self, sql: str, *args: Any, **kwargs: Any) -> Tuple[str, Union[List[Any], Dict[str, Any]]]:
        """
        Generates formatted query and a list of parameters.

        This method is invoked internally during `execute` to generate arguments for cursor object.
        It's meaningless to use this method except for the debugging purpose.

        :param sql: Query template which can contain unified marker.
        :param args: Positional parameters of query.
        :param kwargs: Keyword parameters of query.
        :returns: Formatted query and parameters.
        """
        paramstyle = self.context.config.paramstyle or self.conn.api.paramstyle

        sql = Sql(Marker.of(paramstyle), sql)

        return sql.render(*args, **kwargs)

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> 'Cursor':
        """
        Executes a query and returns a cursor object.

        Cursor is defined in DB-API 2.0 and it provides methods to access results of query execution (ex. ``fetchall`` ``fetchone`` ).
        Those methods and other methods defined by using DB driver are available as they are.

        :param sql: Query template which can contain unified marker.
        :param args: Positional parameters of query.
        :param kwargs: Keyword parameters of query.
        :returns: Cursor object used for the query execution.
        """
        sql, params = self.prepare(sql, *args, **kwargs)

        c = self.conn.cursor()

        return self.context.execute(c, sql, params)