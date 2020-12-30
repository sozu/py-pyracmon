import pytest
import re
import threading
from functools import reduce
from pyracmon.connection import *
from pyracmon.context import ConnectionContext
from tests.db_api import *


class TestContext:
    def test_default(self):
        conn = PseudoConnection(PseudoAPI())

        cxt = conn.context
        assert cxt.identifier == conn.identifier

    def test_factory(self):
        class TestContext(ConnectionContext):
            def __init__(self, v):
                super().__init__()
                self.value = v

        conn = PseudoConnection(PseudoAPI()).use(lambda: TestContext(1))

        cxt = conn.context
        assert isinstance(cxt, TestContext)
        assert cxt.value == 1


class TestStatement:
    def test_prepare(self):
        conn = PseudoConnection(PseudoAPI())

        stmt = conn.stmt()

        assert stmt.prepare("abc $_ $_", 1, 2) == ("abc ? ?", [1, 2])
        assert stmt.prepare("abc $a $b", a=1, b=2) == ("abc ? ?", [1, 2])

    def test_prepare_paramstyle(self):
        conn = PseudoConnection(PseudoAPI())

        cxt = ConnectionContext(paramstyle="pyformat")

        stmt = conn.stmt(cxt)

        assert stmt.prepare("abc $_ $_", 1, 2) == ("abc %(param1)s %(param2)s", {"param1": 1, "param2": 2})
        assert stmt.prepare("abc $a $b", a=1, b=2) == ("abc %(a)s %(b)s", {"a": 1, "b": 2})

    def test_execute(self):
        conn = PseudoConnection(PseudoAPI())

        stmt = conn.stmt()

        cursor = stmt.execute("abc $_ $a $_ $b", 1, 2, a=3, b=4)

        assert isinstance(cursor, PseudoCursor)
        assert cursor.conn.query_list == ["abc ? ? ? ?"]
        assert cursor.conn.params_list == [[1, 3, 2, 4]]