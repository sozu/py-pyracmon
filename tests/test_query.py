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

    def test_func(self):
        q = Q(a = 1, b = "abc")
        c = q.a(lambda v: f"a{v} = %s") & q.b(lambda v: f"b = {len(v)}", lambda x: ())
        assert c.clause == "(a1 = %s) AND (b = 3)"
        assert c.params == (1,)


class TestQueryEach:
    def test_and(self):
        q = Q(a = [1,2,3])
        c = q.a.all("a = %s")
        assert c.clause == "((a = %s) AND (a = %s)) AND (a = %s)"
        assert c.params == (1, 2, 3)

    def test_or(self):
        q = Q(a = [1,2,3])
        c = q.a.any("a = %s")
        assert c.clause == "((a = %s) OR (a = %s)) OR (a = %s)"
        assert c.params == (1, 2, 3)

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

    def test_gen_name(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m("a"), m(), m("")] == [":a", ":key1", ":key2"]

    def test_format(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m(), m(), m()] == ["%s", "%s", "%s"]

    def test_pyformat(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m("a"), m("b"), m("c")] == ["%(a)s", "%(b)s", "%(c)s"]

    def test_pyformat_mixed_ok(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        assert [m("a"), m(), m("c")] == ["%(a)s", "%(key1)s", "%(c)s"]

    def test_pyformat_mixed_ng(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        with pytest.raises(ValueError):
            ms = [m(), m("b"), m("c")]

    def test_qmark_params(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        assert m.params((1,2,3)) == [1,2,3]

    def test_qmark_params_ng(self):
        self.api.paramstyle = 'qmark'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        with pytest.raises(ValueError):
            m.params({0:1, 1:2, 2:3})

    def test_numeric_params(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        assert m.params((1,2,3)) == [1,2,3]

    def test_numeric_params_ng(self):
        self.api.paramstyle = 'numeric'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        with pytest.raises(ValueError):
            m.params({0:1, 1:2, 2:3})

    def test_named_params(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        m = h.marker()
        m("a"); m("b"); m("c")
        assert m.params(dict(a=1, b=2, c=3)) == dict(a=1, b=2, c=3)
        assert m.params((1, 2, 3)) == dict(a=1, b=2, c=3)

    def test_named_gen_params(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        m = h.marker()
        m("a"); m(); m()
        assert m.params(dict(a=1, b=2, c=3)) == dict(a=1, b=2, c=3)
        assert m.params((1, 2, 3)) == dict(a=1, key1=2, key2=3)

    def test_named_params_ng(self):
        self.api.paramstyle = 'named'
        h = QueryHelper(self.api)
        m = h.marker()
        m("a"); m("b"); m("c")
        with pytest.raises(ValueError):
            m.params("abc")

    def test_format_params(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        assert m.params((1, 2, 3)) == [1, 2, 3]

    def test_format_params_ng(self):
        self.api.paramstyle = 'format'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        with pytest.raises(ValueError):
            m.params({0:1, 1:2, 2:3})

    def test_pyformat_params(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        assert m.params((1, 2, 3)) == [1, 2, 3]

    def test_pyformat_params_ng(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        m(); m(); m()
        with pytest.raises(ValueError):
            m.params({0:1, 1:2, 2:3})

    def test_pyformat_named_params(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        m("a"); m("b"); m("c")
        assert m.params(dict(a=1, b=2, c=3)) == dict(a=1, b=2, c=3)

    def test_pyformat_named_params_ng(self):
        self.api.paramstyle = 'pyformat'
        h = QueryHelper(self.api)
        m = h.marker()
        m("a"); m("b"); m("c")
        assert m.params((1, 2, 3)) == dict(a=1, b=2, c=3)


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
        assert h.holders(["a", "b", "c"]) == "%(a)s, %(b)s, %(c)s"

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


class TestEq:
    def test_eq(self):
        m = PyformatMarker()
        c = Q.eq(a = 1)(m)
        assert c.clause == "a = %s"
        assert c.params == (1,)

    def test_eqs(self):
        m = PyformatMarker()
        c = Q.eq(a = 1, b = 2)(m)
        assert c.clause == "a = %s AND b = %s"
        assert c.params == (1,2)

    def test_eq_null(self):
        m = PyformatMarker()
        c = Q.eq(a = None)(m)
        assert c.clause == "a IS NULL"
        assert c.params == ()

    def test_eqs_or(self):
        m = PyformatMarker()
        c = Q.eq(False, a = 1, b = 2)(m)
        assert c.clause == "a = %s OR b = %s"
        assert c.params == (1,2)


class TestNeq:
    def test_neq(self):
        m = PyformatMarker()
        c = Q.neq(a = 1)(m)
        assert c.clause == "a != %s"
        assert c.params == (1,)

    def test_neqs(self):
        m = PyformatMarker()
        c = Q.neq(a = 1, b = 2)(m)
        assert c.clause == "a != %s AND b != %s"
        assert c.params == (1,2)

    def test_neq_null(self):
        m = PyformatMarker()
        c = Q.neq(a = None)(m)
        assert c.clause == "a IS NOT NULL"
        assert c.params == ()

    def test_neqs_or(self):
        m = PyformatMarker()
        c = Q.neq(False, a = 1, b = 2)(m)
        assert c.clause == "a != %s OR b != %s"
        assert c.params == (1,2)


class TestIn:
    def test_in(self):
        m = PyformatMarker()
        c = Q.in_(a = [1,2])(m)
        assert c.clause == "a IN (%s,%s)"
        assert c.params == (1,2)

    def test_ins(self):
        m = PyformatMarker()
        c = Q.in_(a = [1,2], b = [3,4])(m)
        assert c.clause == "a IN (%s,%s) AND b IN (%s,%s)"
        assert c.params == (1,2,3,4)

    def test_in_empty(self):
        m = PyformatMarker()
        c = Q.in_(a = [])(m)
        assert c.clause == "1 = 0"
        assert c.params == ()

    def test_in_empty_one(self):
        m = PyformatMarker()
        c = Q.in_(a = [], b = [1,2])(m)
        assert c.clause == "1 = 0 AND b IN (%s,%s)"
        assert c.params == (1,2)

    def test_ins_or(self):
        m = PyformatMarker()
        c = Q.in_(False, a = [1,2], b = [3,4])(m)
        assert c.clause == "a IN (%s,%s) OR b IN (%s,%s)"
        assert c.params == (1,2,3,4)


class TestLike:
    def test_like(self):
        m = PyformatMarker()
        c = Q.like(a = "abc")(m)
        assert c.clause == "a LIKE %s"
        assert c.params == ("%abc%",)

    def test_likes(self):
        m = PyformatMarker()
        c = Q.like(a = "abc", b = "def")(m)
        assert c.clause == "a LIKE %s AND b LIKE %s"
        assert c.params == ("%abc%", "%def%")

    def test_likes_or(self):
        m = PyformatMarker()
        c = Q.like(False, a = "abc", b = "def")(m)
        assert c.clause == "a LIKE %s OR b LIKE %s"
        assert c.params == ("%abc%", "%def%")

    def test_like_escape(self):
        m = PyformatMarker()
        c = Q.like(a = "%abc_")(m)
        assert c.clause == "a LIKE %s"
        assert c.params == ("%\\%abc\\_%",)

    def test_prefix(self):
        m = PyformatMarker()
        c = Q.prefix(a = "abc")(m)
        assert c.clause == "a LIKE %s"
        assert c.params == ("abc%",)

    def test_postfix(self):
        m = PyformatMarker()
        c = Q.postfix(a = "abc")(m)
        assert c.clause == "a LIKE %s"
        assert c.params == ("%abc",)


class TestCompare:
    def test_lt(self):
        m = PyformatMarker()
        c = Q.lt(a = 1)(m)
        assert c.clause == "a < %s"
        assert c.params == (1,)

    def test_le(self):
        m = PyformatMarker()
        c = Q.le(a = 1)(m)
        assert c.clause == "a <= %s"
        assert c.params == (1,)

    def test_gt(self):
        m = PyformatMarker()
        c = Q.gt(a = 1)(m)
        assert c.clause == "a > %s"
        assert c.params == (1,)

    def test_ge(self):
        m = PyformatMarker()
        c = Q.ge(a = 1)(m)
        assert c.clause == "a >= %s"
        assert c.params == (1,)

    def test_lts(self):
        m = PyformatMarker()
        c = Q.lt(a = 1, b = 2)(m)
        assert c.clause == "a < %s AND b < %s"
        assert c.params == (1, 2)

    def test_lts_or(self):
        m = PyformatMarker()
        c = Q.lt(False, a = 1, b = 2)(m)
        assert c.clause == "a < %s OR b < %s"
        assert c.params == (1, 2)


class TestConditional:
    def test_and(self):
        m = PyformatMarker()
        c1 = Q.by(lambda m: f"a = {m()}", 1)
        c2 = Q.eq(b = 2)
        c = c1 & c2
        assert where(c(m)) == ("WHERE (a = %s) AND (b = %s)", [1, 2])

    def test_or(self):
        m = PyformatMarker()
        c1 = Q.by(lambda m: f"a = {m()}", 1)
        c2 = Q.eq(b = 2)
        c = c1 | c2
        assert where(c(m)) == ("WHERE (a = %s) OR (b = %s)", [1, 2])

    def test_not(self):
        m = PyformatMarker()
        c = ~Q.by(lambda m: f"a = {m()}", 1)
        assert where(c(m)) == ("WHERE NOT (a = %s)", [1])

    def test_and_c(self):
        m = PyformatMarker()
        c1 = Q.eq(a = 2)
        c2 = Q.of("b = %s", 3)
        c = c1 & c2
        assert where(c(m)) == ("WHERE (a = %s) AND (b = %s)", [2, 3])

    def test_or_c(self):
        m = PyformatMarker()
        c1 = Q.eq(a = 2)
        c2 = Q.of("b = %s", 3)
        c = c1 | c2
        assert where(c(m)) == ("WHERE (a = %s) OR (b = %s)", [2, 3])

    def test_c_and(self):
        m = PyformatMarker()
        c1 = Q.eq(a = 2)
        c2 = Q.of("b = %s", 3)
        c = c2 & c1
        assert where(c(m)) == ("WHERE (b = %s) AND (a = %s)", [3, 2])

    def test_c_or(self):
        m = PyformatMarker()
        c1 = Q.eq(a = 2)
        c2 = Q.of("b = %s", 3)
        c = c2 | c1
        assert where(c(m)) == ("WHERE (b = %s) OR (a = %s)", [3, 2])


