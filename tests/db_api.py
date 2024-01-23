from collections.abc import Sequence
import logging
from typing import Any, Optional
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
        def close(self): pass
        def commit(self): pass
        def rollback(self): pass
        def cursor(self) -> 'PseudoCursor': ...

    def __init__(self, api, **kwargs):
        super().__init__(api, PseudoConnection.Inner(), **kwargs)
        self.query_list = []
        self.params_list = []
        self.rows_list: list[Sequence[Any]] = []
        self.closed = False
        self.rowcount = -1

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

    def cursor(self) -> 'PseudoCursor':
        return PseudoCursor(self)

    def close(self):
        self.closed = True
        super(PseudoConnection, self).close()


class PseudoCursor:
    def __init__(self, conn: PseudoConnection):
        self.conn = conn

    @property
    def description(self): return "PseudoCursor"
    @property
    def rowcount(self): return self.conn.rowcount
    @property
    def arraysize(self): return 0

    def close(self): pass
    def setinputsizes(self, sizes: Sequence[Any]): pass
    def setoutputsize(self, size: int, column: int): pass

    def execute(self, operation: str, *args, **kwargs) -> Any:
        self.conn.query_list.append(operation)
        self.conn.params_list.append(args[0])

    def executemany(self, operation: str, seq_of_parameters: Sequence[Any]) -> Any:
        for ps in seq_of_parameters:
            self.execute(operation, ps)

    def fetchone(self) -> Optional[Sequence[Any]]:
        rows = self.conn.rows_list.pop(0)
        return rows[0] if len(rows) > 0 else None

    def fetchmany(self, size: int = 0) -> Sequence[Sequence[Any]]:
        return self.fetchall()

    def fetchall(self) -> Sequence[Sequence[Any]]:
        return self.conn.rows_list.pop(0) if self.conn.rows_list else []


class PseudoLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        self.messages = []

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            self.messages.append(msg)