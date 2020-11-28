import pytest
from pyracmon.query import *
from pyracmon.marker import *
from tests.db_api import PseudoAPI


class TestConditional:
    def test_and(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 & c2
        assert (c.clause, c.params) == ("(a = ? + ?) AND (b = ? * ?)", [1, 2, 3, 4])

    def test_and_left(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("", [])
        c = c1 & c2
        assert (c.clause, c.params) == ("a = ? + ?", [1, 2])

    def test_and_right(self):
        c1 = Conditional("", [])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 & c2
        assert (c.clause, c.params) == ("b = ? * ?", [3, 4])

    def test_and_empty(self):
        c1 = Conditional("", [])
        c2 = Conditional("", [])
        c = c1 & c2
        assert (c.clause, c.params) == ("", [])

    def test_or(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 | c2
        assert (c.clause, c.params) == ("(a = ? + ?) OR (b = ? * ?)", [1, 2, 3, 4])

    def test_or_left(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("", [])
        c = c1 | c2
        assert (c.clause, c.params) == ("a = ? + ?", [1, 2])

    def test_or_right(self):
        c1 = Conditional("", [])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 | c2
        assert (c.clause, c.params) == ("b = ? * ?", [3, 4])

    def test_or_empty(self):
        c1 = Conditional("", [])
        c2 = Conditional("", [])
        c = c1 | c2
        assert (c.clause, c.params) == ("", [])

    def test_not(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c = ~c1
        assert (c.clause, c.params) == ("NOT (a = ? + ?)", [1, 2])

    def test_not_empty(self):
        c1 = Conditional("", [])
        c = ~c1
        assert (c.clause, c.params) == ("1 = 0", [])


class TestQuery:
    def test_empty(self):
        q = Q()
        c = q.a("a = %s")
        assert (c.clause, c.params) == ("", [])

    def test_params(self):
        q = Q(a = 1, b = 2)
        c = q.a("a = %s") & q.b("b = %s")
        assert (c.clause, c.params) == ("(a = %s) AND (b = %s)", [1, 2])

    def test_partial(self):
        q = Q(a = 1)
        c = q.a("a = %s") & q.b("b = %s")
        assert (c.clause, c.params) == ("a = %s", [1])

    def test_nest(self):
        q = Q(a = 1, c = 3)
        c = (q.a("a = %s") | q.b("b = %s")) & ~q.c("c = %s")
        assert (c.clause, c.params) == ("(a = %s) AND (NOT (c = %s))", [1, 3])

    def test_convert(self):
        q = Q(a = 1, b = "abc")
        c = q.a("a = %s", lambda x: x * 2) & q.b("b = %s", len)
        assert (c.clause, c.params) == ("(a = %s) AND (b = %s)", [2, 3])

    def test_func_clause(self):
        q = Q(a = 1, b = "abc")
        c = q.a(lambda v: f"a{v} = %s") & q.b(lambda v: f"b = {len(v)}", lambda x: [])
        assert (c.clause, c.params) == ("(a1 = %s) AND (b = 3)", [1])


class TestQueryComposite:
    def test_all(self):
        q = Q(a = [1,2,3])
        c = q.a.all("a = %s")
        assert (c.clause, c.params) == ("((a = %s) AND (a = %s)) AND (a = %s)", [1, 2, 3])

    def test_any(self):
        q = Q(a = [1,2,3])
        c = q.a.any("a = %s")
        assert (c.clause, c.params) == ("((a = %s) OR (a = %s)) OR (a = %s)", [1, 2, 3])

    def test_all_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.all("a = %s", lambda x: x*2)
        assert (c.clause, c.params) == ("((a = %s) AND (a = %s)) AND (a = %s)", [2, 4, 6])

    def test_any_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.any("a = %s", lambda x: x*2)
        assert (c.clause, c.params) == ("((a = %s) OR (a = %s)) OR (a = %s)", [2, 4, 6])

    def test_method(self):
        q = Q(a = [1,2,3])
        c = q.a.all.eq("a")
        assert (c.clause, c.params) == ("((a = $_) AND (a = $_)) AND (a = $_)", [1, 2, 3])

    def test_method_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.all.eq("a", lambda x:x*2)
        assert (c.clause, c.params) == ("((a = $_) AND (a = $_)) AND (a = $_)", [2, 4, 6])

    def test_empty(self):
        q = Q()
        c = q.a.all("a = %s")
        assert (c.clause, c.params) == ("", [])

    def test_empty_method(self):
        q = Q()
        c = q.a.eq("a")
        assert (c.clause, c.params) == ("", [])


class TestQueryMethod:
    def test_eq(self):
        q = Q(a = 1)
        c = q.a.eq("a")
        assert (c.clause, c.params) == ("a = $_", [1])

    def test_in(self):
        q = Q(a = [1, 2, 3])
        c = q.a.in_("a")
        assert (c.clause, c.params) == ("a IN ($_, $_, $_)", [1, 2, 3])

    def test_convert(self):
        q = Q(a = 1)
        c = q.a.eq("a", lambda x: x*2)
        assert (c.clause, c.params) == ("a = $_", [2])

    def test_convert_list(self):
        q = Q(a = [1, 2, 3])
        c = q.a.in_("a", lambda x: [v*2 for v in x])
        assert (c.clause, c.params) == ("a IN ($_, $_, $_)", [2, 4, 6])


class TestEq:
    def test_one(self):
        c = Q.eq(a = 1)
        assert (c.clause, c.params) == ("a = $_", [1])

    def test_and(self):
        c = Q.eq(a = 1, b = 2)
        assert (c.clause, c.params) == ("(a = $_) AND (b = $_)", [1, 2])

    def test_or(self):
        c = Q.eq(None, False, a = 1, b = 2)
        assert (c.clause, c.params) == ("(a = $_) OR (b = $_)", [1, 2])

    def test_alias(self):
        c = Q.eq("t", a = 1, b = 2)
        assert (c.clause, c.params) == ("(t.a = $_) AND (t.b = $_)", [1, 2])

    def test_null(self):
        c = Q.eq(a = None)
        assert (c.clause, c.params) == ("a IS NULL", [])


class TestNeq:
    def test_one(self):
        c = Q.neq(a = 1)
        assert (c.clause, c.params) == ("a != $_", [1])

    def test_and(self):
        c = Q.neq(a = 1, b = 2)
        assert (c.clause, c.params) == ("(a != $_) AND (b != $_)", [1, 2])

    def test_or(self):
        c = Q.neq(None, False, a = 1, b = 2)
        assert (c.clause, c.params) == ("(a != $_) OR (b != $_)", [1, 2])

    def test_alias(self):
        c = Q.neq("t", a = 1, b = 2)
        assert (c.clause, c.params) == ("(t.a != $_) AND (t.b != $_)", [1, 2])

    def test_null(self):
        c = Q.neq(a = None)
        assert (c.clause, c.params) == ("a IS NOT NULL", [])


class TestIn:
    def test_one(self):
        c = Q.in_(a = [1, 2])
        assert (c.clause, c.params) == ("a IN ($_, $_)", [1, 2])

    def test_and(self):
        c = Q.in_(a = [1, 2], b = [3, 4])
        assert (c.clause, c.params) == ("(a IN ($_, $_)) AND (b IN ($_, $_))", [1, 2, 3, 4])

    def test_or(self):
        c = Q.in_(None, False, a = [1, 2], b = [3, 4])
        assert (c.clause, c.params) == ("(a IN ($_, $_)) OR (b IN ($_, $_))", [1, 2, 3, 4])

    def test_alias(self):
        c = Q.in_("t", a = [1, 2], b = [3, 4])
        assert (c.clause, c.params) == ("(t.a IN ($_, $_)) AND (t.b IN ($_, $_))", [1, 2, 3, 4])

    def test_empty(self):
        c = Q.in_(a = [])
        assert (c.clause, c.params) == ("1 = 0", [])


class TestLike:
    def test_like(self):
        c = Q.like(a = "abc")
        assert (c.clause, c.params) == ("a LIKE $_", ["%abc%"])

    def test_startswith(self):
        c = Q.startswith(a = "abc")
        assert (c.clause, c.params) == ("a LIKE $_", ["abc%"])

    def test_endswith(self):
        c = Q.endswith(a = "abc")
        assert (c.clause, c.params) == ("a LIKE $_", ["%abc"])

    def test_match(self):
        c = Q.match(a = "_a%c_")
        assert (c.clause, c.params) == ("a LIKE $_", ["_a%c_"])

    def test_and(self):
        c = Q.like(a = "abc", b = "def")
        assert (c.clause, c.params) == ("(a LIKE $_) AND (b LIKE $_)", ["%abc%", "%def%"])

    def test_or(self):
        c = Q.like(None, False, a = "abc", b = "def")
        assert (c.clause, c.params) == ("(a LIKE $_) OR (b LIKE $_)", ["%abc%", "%def%"])

    def test_alias(self):
        c = Q.like("t", a = "abc", b = "def")
        assert (c.clause, c.params) == ("(t.a LIKE $_) AND (t.b LIKE $_)", ["%abc%", "%def%"])

    def test_escape(self):
        c = Q.like(a = r"_\a%\_%")
        assert (c.clause, c.params) == ("a LIKE $_", [r"%\_\\\\a\%\\\\\_\%%"])


class TestCompare:
    def test_lt(self):
        c = Q.lt(a = 1)
        assert (c.clause, c.params) == ("a < $_", [1])

    def test_le(self):
        c = Q.le(a = 1)
        assert (c.clause, c.params) == ("a <= $_", [1])

    def test_gt(self):
        c = Q.gt(a = 1)
        assert (c.clause, c.params) == ("a > $_", [1])

    def test_ge(self):
        c = Q.ge(a = 1)
        assert (c.clause, c.params) == ("a >= $_", [1])

    def test_and(self):
        c = Q.lt(a = 1, b = 2)
        assert (c.clause, c.params) == ("(a < $_) AND (b < $_)", [1, 2])

    def test_or(self):
        c = Q.lt(None, False, a = 1, b = 2)
        assert (c.clause, c.params) == ("(a < $_) OR (b < $_)", [1, 2])

    def test_alias(self):
        c = Q.lt("t", a = 1, b = 2)
        assert (c.clause, c.params) == ("(t.a < $_) AND (t.b < $_)", [1, 2])


#class TestConditional:
#    def test_and(self):
#        m = PyformatMarker()
#        c1 = Q.by(lambda m: f"a = {m()}", 1)
#        c2 = Q.eq(b = 2)
#        c = c1 & c2
#        assert where(c(m)) == ("WHERE (a = %s) AND (b = %s)", [1, 2])
#
#    def test_or(self):
#        m = PyformatMarker()
#        c1 = Q.by(lambda m: f"a = {m()}", 1)
#        c2 = Q.eq(b = 2)
#        c = c1 | c2
#        assert where(c(m)) == ("WHERE (a = %s) OR (b = %s)", [1, 2])
#
#    def test_not(self):
#        m = PyformatMarker()
#        c = ~Q.by(lambda m: f"a = {m()}", 1)
#        assert where(c(m)) == ("WHERE NOT (a = %s)", [1])
#
#    def test_and_c(self):
#        m = PyformatMarker()
#        c1 = Q.eq(a = 2)
#        c2 = Q.of("b = %s", 3)
#        c = c1 & c2
#        assert where(c(m)) == ("WHERE (a = %s) AND (b = %s)", [2, 3])
#
#    def test_or_c(self):
#        m = PyformatMarker()
#        c1 = Q.eq(a = 2)
#        c2 = Q.of("b = %s", 3)
#        c = c1 | c2
#        assert where(c(m)) == ("WHERE (a = %s) OR (b = %s)", [2, 3])
#
#    def test_c_and(self):
#        m = PyformatMarker()
#        c1 = Q.eq(a = 2)
#        c2 = Q.of("b = %s", 3)
#        c = c2 & c1
#        assert where(c(m)) == ("WHERE (b = %s) AND (a = %s)", [3, 2])
#
#    def test_c_or(self):
#        m = PyformatMarker()
#        c1 = Q.eq(a = 2)
#        c2 = Q.of("b = %s", 3)
#        c = c2 | c1
#        assert where(c(m)) == ("WHERE (b = %s) OR (a = %s)", [3, 2])
#
#    def test_many(self):
#        m = PyformatMarker()
#        c1 = Q.eq(a = 2)
#        c2 = Q.of("b = %s", 3)
#        c3 = Q.in_(c = [1, 2, 3])
#        c = c1 & c2 & c3
#        assert where(c(m)) == ("WHERE ((a = %s) AND (b = %s)) AND (c IN (%s,%s,%s))", [2, 3, 1, 2, 3])

