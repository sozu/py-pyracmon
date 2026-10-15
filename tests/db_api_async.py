import asyncio
from collections.abc import Sequence
import logging
from typing import Any, Optional
from pyracmon.connection import AsyncConnection


class PseudoAPI:
    def __init__(self, paramstyle="qmark"):
        self.apilevel = '1.0'
        self.threadsafety = 1
        self.paramstyle = paramstyle

    async def connect(self, **kwargs):
        await asyncio.sleep(0.01)
        return PseudoConnection(self)


class PseudoConnection(AsyncConnection):
    class Inner:
        async def close(self): pass
        async def commit(self): pass
        async def rollback(self): pass
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

    async def close(self):
        self.closed = True
        await super(PseudoConnection, self).close()


class PseudoCursor:
    def __init__(self, conn: PseudoConnection):
        self.conn = conn

    @property
    def description(self): return "PseudoCursor"
    @property
    def rowcount(self): return self.conn.rowcount
    @property
    def arraysize(self): return 0

    async def close(self): pass
    async def setinputsizes(self, sizes: Sequence[Any]): pass
    async def setoutputsize(self, size: int, column: int): pass

    async def execute(self, operation: str, *args, **kwargs) -> Any:
        await asyncio.sleep(0.01)
        self.conn.query_list.append(operation)
        self.conn.params_list.append(args[0])

    async def executemany(self, operation: str, seq_of_parameters: Sequence[Any]) -> Any:
        for ps in seq_of_parameters:
            await self.execute(operation, ps)

    async def fetchone(self) -> Optional[Sequence[Any]]:
        await asyncio.sleep(0.01)
        rows = self.conn.rows_list.pop(0)
        return rows[0] if len(rows) > 0 else None

    async def fetchmany(self, size: int = 0) -> Sequence[Sequence[Any]]:
        return await self.fetchall()

    async def fetchall(self) -> Sequence[Sequence[Any]]:
        await asyncio.sleep(0.01)
        return self.conn.rows_list.pop(0) if self.conn.rows_list else []


class PseudoLogger(logging.Logger):
    def __init__(self, name):
        super().__init__(name)
        self.messages = []

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            self.messages.append(msg)