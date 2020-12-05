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
    return Connection(api, api.connect(*args, **kwargs))


class Connection:
    """
    Wrapper class of DB-API 2.0 Connection.

    Every instance works as the proxy object to original connection, therefore any attribute in it is still available.
    """
    def __init__(self, api, conn):
        self.api = api
        self.conn = conn

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

    @property
    def helper(self):
        return QueryHelper(self.api, None)

    def stmt(self, context=None):
        return Statement(self, context)


class Statement:
    """
    Statement object executes a query on provided configuration.
    """
    def __init__(self, conn, context):
        self.conn = conn
        self.context = context

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
        sql = Sql(Marker.of(self.conn.api.paramstyle), sql)

        sql, params = sql.render(*args, **kwargs)

        c = self.conn.cursor()

        c.execute(sql, params)

        return c