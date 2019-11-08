from pyracmon.query import QueryHelper


def connect(api, *args, **kwargs):
    return Connection(api, api.connect(*args, **kwargs))


class Connection:
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
        return QueryHelper(self.api)
