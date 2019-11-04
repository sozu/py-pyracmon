from pyracmon.query import QueryHelper


class PseudoAPI:
    def __init__(self):
        self.apilevel = '1.0'
        self.threadsafety = 1
        self.paramstyle = 'qmark'

    def connect(self, **kwargs):
        return PseudoConnection(self)


class PseudoConnection:
    def __init__(self, api):
        self.api = api
        self.query_list = []
        self.params_list = []
        self._rows_list = []

    def reserve(self, rows):
        if not isinstance(rows, list):
            raise ValueError("Reserving row set must be a list of rows.")
        elif not isinstance(rows[0], list):
            raise ValueError("Each row must be a list.")
        self._rows_list.append(rows)

    def clear(self):
        self.query_list = []
        self.params_list = []
        self._rows_list = []

    @property
    def helper(self):
        return QueryHelper(self.api)

    def cursor(self):
        return PseudoCursor(self)


class PseudoCursor:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, params = []):
        self.conn.query_list.append(query)
        self.conn.params_list.append(params)

    def fetchone(self):
        rows = self.conn._rows_list.pop(0)
        return rows[0]

    def fetchall(self):
        return self.conn._rows_list.pop(0)
