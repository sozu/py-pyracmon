import pytest
from .db_api_async import *


class TestStatement:
    @pytest.mark.asyncio
    async def test_execute(self):
        conn = PseudoConnection(PseudoAPI())

        stmt = conn.stmt()

        cursor = await stmt.execute("abc $_ $a $_ $b", 1, 2, a=3, b=4)

        assert isinstance(cursor, PseudoCursor)
        assert cursor.conn.query_list == ["abc ? ? ? ?"]
        assert cursor.conn.params_list == [[1, 3, 2, 4]]

    @pytest.mark.asyncio
    async def test_executemany(self):
        conn = PseudoConnection(PseudoAPI())

        stmt = conn.stmt()

        c1 = await stmt.executemany("abc $_ $_", [[1, 2], [3, 4], [5, 6]])
        c2 = await stmt.executemany("def $a $b", [dict(a=7, b=8), dict(a=9, b=10)])

        assert isinstance(c1, PseudoCursor)
        assert isinstance(c2, PseudoCursor)

        assert c1.conn.query_list == ["abc ? ?", "abc ? ?", "abc ? ?", "def ? ?", "def ? ?"]
        assert c1.conn.params_list == [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]