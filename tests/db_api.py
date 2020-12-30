import logging
from pyracmon.query import QueryHelper
from pyracmon.connection import Connection


class PseudoAPI:
    def __init__(self, paramstyle="qmark"):
        self.apilevel = '1.0'
        self.threadsafety = 1
        self.paramstyle = paramstyle

    def connect(self, **kwargs):
        return PseudoConnection(self)


class PseudoConnection(Connection):
    class Inner:
        def close(self):
            pass

    def __init__(self, api, **kwargs):
        super().__init__(api, PseudoConnection.Inner(), **kwargs)
        self.query_list = []
        self.params_list = []
        self.rows_list = []
        self.closed = False

    def reserve(self, rows):
        if not isinstance(rows, list):
            raise ValueError("Reserving row set must be a list of rows.")
        elif len(rows) > 0 and not isinstance(rows[0], list):
            raise ValueError("Each row must be a list.")
        self.rows_list.append(rows)

    def clear(self):
        self.query_list = []
        self.params_list = []
        self.rows_list = []

    def cursor(self):
        return PseudoCursor(self)

    def close(self):
        self.closed = True
        super(PseudoConnection, self).close()


class PseudoCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, params = []):
        self.conn.query_list.append(query)
        self.conn.params_list.append(params)

    def fetchone(self):
        rows = self.conn.rows_list.pop(0)
        return rows[0] if len(rows) > 0 else None

    def fetchall(self):
        return self.conn.rows_list.pop(0)

    @property
    def rowcount(self):
        return -1


class PseudoLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        self.messages = []

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            self.messages.append(msg)