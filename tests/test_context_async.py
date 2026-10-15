import pytest
import logging
from pyracmon.config import default_config
from pyracmon.context import AsyncConnectionContext
from .db_api_async import *


class TestConfigure:
    def test_configure(self):
        cxt = AsyncConnectionContext("a")

        assert cxt.config is not default_config()

        cxt.configure(name="context", log_level=-1)

        assert cxt.config.name == "context"
        assert cxt.config.log_level == -1
        assert default_config().name == "default"
        assert default_config().log_level == logging.DEBUG


class TestExecute:
    @pytest.mark.asyncio
    async def test_execute(self):
        cursor = PseudoCursor(PseudoConnection(PseudoAPI()))
        logger = PseudoLogger("test")

        cxt = AsyncConnectionContext("a")

        cxt.configure(logger=logger, sql_log_length=10)
        c1 = await cxt.execute(cursor, "SELECT", [1, 2, 3])

        cxt.configure(sql_log_length=4, parameter_log=True)
        c2 = await cxt.execute(cursor, "UPDATE", [4, 5, 6])

        assert c1 is c2
        assert c1 is cursor

        assert cursor.conn.query_list == ["SELECT", "UPDATE"]
        assert cursor.conn.params_list == [[1, 2, 3], [4, 5, 6]]

        assert logger.messages == ["(a) SELECT", "(a) UPDA...", "(a) Parameters: [4, 5, 6]"]


class TestExecuteMany:
    @pytest.mark.asyncio
    async def test_execute(self):
        cursor = PseudoCursor(PseudoConnection(PseudoAPI()))
        logger = PseudoLogger("test")

        cxt = AsyncConnectionContext("a")

        cxt.configure(logger=logger, parameter_log=True)
        c1 = await cxt.executemany(cursor, "SELECT", [[1, 2, 3], [4, 5, 6], [7, 8, 9]])

        assert c1 is cursor

        assert cursor.conn.query_list == ["SELECT", "SELECT", "SELECT"]
        assert cursor.conn.params_list == [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

        assert logger.messages == [
            "(a) SELECT",
            "(a) Parameters: [1, 2, 3]",
            "(a) Parameters: [4, 5, 6]",
            "(a) Parameters: [7, 8, 9]",
        ]