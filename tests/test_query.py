import pytest
from pyracmon.query import *
from tests.db_api import PseudoAPI

class TestCondition:
    def test_and(self):
        a = Q.C("a = %s", 1)
        b = Q.C("b = %s", 2)
        c = a & b
        assert c.clause == "(a = %s) AND (b = %s)"
        assert c.params == (1, 2)

    def test_or(self):
        a = Q.C("a = %s", 1)
        b = Q.C("b = %s", 2)
        c = a | b
        assert c.clause == "(a = %s) OR (b = %s)"
        assert c.params == (1, 2)

    def test_not(self):
        a = Q.C("a = %s", 1)
        c = ~a
        assert c.clause == "NOT (a = %s)"
        assert c.params == (1,)

    def test_nest(self):
        a = Q.C("a = %s", 1)
        b = Q.C("b = %s + %s", [2, 3])
        c = Q.C("c = %s * %s", [4, 5])
        d = (a | b) & ~c
        assert d.clause == "((a = %s) OR (b = %s + %s)) AND (NOT (c = %s * %s))"
        assert d.params == (1, 2, 3, 4, 5)


class TestQuery:
    def test_empty(self):
        q = Q()
        c = q.a("a = %s")
        assert c.clause == ""
        assert c.params == ()

    def test_params(self):
        q = Q(a = 1, b = 2)
        c = q.a("a = %s") & q.b("b = %s")
        assert c.clause == "(a = %s) AND (b = %s)"
        assert c.params == (1, 2)

    def test_partial(self):
        q = Q(a = 1)
        c = q.a("a = %s") & q.b("b = %s")
        assert c.clause == "a = %s"
        assert c.params == (1,)

    def test_nest(self):
        q = Q(a = 1, c = 3)
        c = (q.a("a = %s") | q.b("b = %s")) & ~q.c("c = %s")
        assert c.clause == "(a = %s) AND (NOT (c = %s))"
        assert c.params == (1, 3)

    def test_convert(self):
        q = Q(a = 1, b = "abc")
        c = q.a("a = %s", lambda x: x * 2) & q.b("b = %s", len)
        assert c.clause == "(a = %s) AND (b = %s)"
        assert c.params == (2, 3)


class TestMarker:
    api = PseudoAPI()

    def test_qmark(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(), m(), m()] == ["?", "?", "?"]

    def test_numeric(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(0), m(1), m(3)] == [":0", ":1", ":3"]

    def test_numeric_counter(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(), m(), m()] == [":1", ":2", ":3"]
        m(1)
        assert m.index == 1
        assert [m(), m(1), m()] == [":2", ":1", ":2"]

    def test_named(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m("a"), m("b"), m("c")] == [":a", ":b", ":c"]

    def test_format(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(), m(), m()] == ["%s", "%s", "%s"]

    def test_pyformat(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(), m("b"), m()] == ["%s", "%(b)s", "%s"]


class TestHolders:
    api = PseudoAPI()

    def test_qmark(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        assert h.holders(3) == "?, ?, ?"

    def test_numeric(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        assert h.holders(3) == ":1, :2, :3"

    def test_named(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        assert h.holders(["a", "b", "c"]) == ":a, :b, :c"

    def test_format(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        assert h.holders(3) == "%s, %s, %s"

    def test_pyformat(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        assert h.holders(["a", None, "c"]) == "%(a)s, %s, %(c)s"

    def test_pyformat_intkey(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        assert h.holders(3) == "%s, %s, %s"

    def test_qualifier(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        assert h.holders(3, { 1: lambda x: f"{x}::text"}) == "?, ?::text, ?"


class TestValues:
    api = PseudoAPI()

    def test_qmark(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        assert h.values(2, 3) == "(?, ?), (?, ?), (?, ?)"

    def test_numeric(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        assert h.values(2, 3) == "(:1, :2), (:3, :4), (:5, :6)"

    def test_named(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        assert h.values(["a", "b"], 3) == "(:a, :b), (:a, :b), (:a, :b)"

    def test_format(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        assert h.values(2, 3) == "(%s, %s), (%s, %s), (%s, %s)"

    def test_pyformat(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        assert h.values(["a", None], 3) == "(%(a)s, %s), (%(a)s, %s), (%(a)s, %s)"

    def test_pyformat(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        assert h.values(2, 3) == "(%s, %s), (%s, %s), (%s, %s)"

    def test_qualifier(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        assert h.values(2, 3, { 1: lambda x: f"{x}::text"}) == "(?, ?::text), (?, ?::text), (?, ?::text)"


