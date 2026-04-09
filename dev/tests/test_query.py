import pytest
from pyracmon.query import *


class TestConditional:
    def test_and(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 & c2
        assert (c.expression, c.params) == ("(a = ? + ?) AND (b = ? * ?)", [1, 2, 3, 4])

    def test_and_left(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("", [])
        c = c1 & c2
        assert (c.expression, c.params) == ("a = ? + ?", [1, 2])

    def test_and_right(self):
        c1 = Conditional("", [])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 & c2
        assert (c.expression, c.params) == ("b = ? * ?", [3, 4])

    def test_and_empty(self):
        c1 = Conditional("", [])
        c2 = Conditional("", [])
        c = c1 & c2
        assert (c.expression, c.params) == ("", [])

    def test_or(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 | c2
        assert (c.expression, c.params) == ("(a = ? + ?) OR (b = ? * ?)", [1, 2, 3, 4])

    def test_or_left(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c2 = Conditional("", [])
        c = c1 | c2
        assert (c.expression, c.params) == ("a = ? + ?", [1, 2])

    def test_or_right(self):
        c1 = Conditional("", [])
        c2 = Conditional("b = ? * ?", [3, 4])
        c = c1 | c2
        assert (c.expression, c.params) == ("b = ? * ?", [3, 4])

    def test_or_empty(self):
        c1 = Conditional("", [])
        c2 = Conditional("", [])
        c = c1 | c2
        assert (c.expression, c.params) == ("", [])

    def test_not(self):
        c1 = Conditional("a = ? + ?", [1, 2])
        c = ~c1
        assert (c.expression, c.params) == ("NOT (a = ? + ?)", [1, 2])

    def test_not_empty(self):
        c1 = Conditional("", [])
        c = ~c1
        assert (c.expression, c.params) == ("1 = 0", [])


class TestQuery:
    def test_empty(self):
        q = Q()
        c = q.a("a = %s")
        assert (c.expression, c.params) == ("", [])
        assert bool(q.a) is False

    def test_params(self):
        q = Q(a = 1, b = 2)
        c = q.a("a = %s") & q.b("b = %s")
        assert (c.expression, c.params) == ("(a = %s) AND (b = %s)", [1, 2])
        assert bool(q.a) is True

    def test_partial(self):
        q = Q(a = 1)
        c = q.a("a = %s") & q.b("b = %s")
        assert (c.expression, c.params) == ("a = %s", [1])

    def test_nest(self):
        q = Q(a = 1, c = 3)
        c = (q.a("a = %s") | q.b("b = %s")) & ~q.c("c = %s")
        assert (c.expression, c.params) == ("(a = %s) AND (NOT (c = %s))", [1, 3])

    def test_convert_func(self):
        q = Q(a = 1, b = "abc")
        c = q.a("a = %s", lambda x: x * 2) & q.b("b = %s", len)
        assert (c.expression, c.params) == ("(a = %s) AND (b = %s)", [2, 3])

    def test_convert_param(self):
        q = Q(a = 1, b = "abc")
        c = q.a("a = %s", 3) & q.b("b = %s", [1,2])
        assert (c.expression, c.params) == ("(a = %s) AND (b = %s)", [3, 1, 2])

    def test_func_expression(self):
        q = Q(a = 1, b = "abc")
        c = q.a(lambda v: f"a{v} = %s") & q.b(lambda v: f"b = {len(v)}", lambda x: [])
        assert (c.expression, c.params) == ("(a1 = %s) AND (b = 3)", [1])

    def test_and(self):
        q = Q(a = True, b = False)
        c = q.a & Conditional("a", [1, 2])
        assert (c.expression, c.params) == ("a", [1, 2])
        c = q.b & Conditional("b", [1, 2])
        assert (c.expression, c.params) == ("", [])
        c = q.c & Conditional("c", [1, 2])
        assert (c.expression, c.params) == ("", [])

    def test_or(self):
        q = Q(a = True, b = False)
        c = q.a | Conditional("a", [1, 2])
        assert (c.expression, c.params) == ("", [])
        c = q.b | Conditional("b", [1, 2])
        assert (c.expression, c.params) == ("b", [1, 2])
        c = q.c & Conditional("c", [1, 2])
        assert (c.expression, c.params) == ("", [])


class TestQueryComposite:
    def test_all(self):
        q = Q(a = [1,2,3])
        c = q.a.all("a = %s")
        assert (c.expression, c.params) == ("((a = %s) AND (a = %s)) AND (a = %s)", [1, 2, 3])

    def test_any(self):
        q = Q(a = [1,2,3])
        c = q.a.any("a = %s")
        assert (c.expression, c.params) == ("((a = %s) OR (a = %s)) OR (a = %s)", [1, 2, 3])

    def test_all_empty(self):
        q = Q(a = [])
        c = q.a.all("a = %s")
        assert (c.expression, c.params) == ("", [])

    def test_any_empty(self):
        q = Q(a = [])
        c = q.a.any("a = %s")
        assert (c.expression, c.params) == ("1 = 0", [])

    def test_all_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.all("a = %s", lambda x: x*2)
        assert (c.expression, c.params) == ("((a = %s) AND (a = %s)) AND (a = %s)", [2, 4, 6])

    def test_any_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.any("a = %s", lambda x: x*2)
        assert (c.expression, c.params) == ("((a = %s) OR (a = %s)) OR (a = %s)", [2, 4, 6])

    def test_method(self):
        q = Q(a = [1,2,3])
        c = q.a.all.eq("a")
        assert (c.expression, c.params) == ("((a = $_) AND (a = $_)) AND (a = $_)", [1, 2, 3])

    def test_method_convert(self):
        q = Q(a = [1,2,3])
        c = q.a.all.eq("a", lambda x:x*2)
        assert (c.expression, c.params) == ("((a = $_) AND (a = $_)) AND (a = $_)", [2, 4, 6])

    def test_empty(self):
        q = Q()
        c = q.a.all("a = %s")
        assert (c.expression, c.params) == ("", [])

    def test_empty_method(self):
        q = Q()
        c = q.a.eq("a")
        assert (c.expression, c.params) == ("", [])


class TestQueryMethod:
    def test_eq(self):
        q = Q(a = 1)
        c = q.a.eq("a")
        assert (c.expression, c.params) == ("a = $_", [1])

    def test_in(self):
        q = Q(a = [1, 2, 3])
        c = q.a.in_("a")
        assert (c.expression, c.params) == ("a IN ($_, $_, $_)", [1, 2, 3])

    def test_convert(self):
        q = Q(a = 1)
        c = q.a.eq("a", lambda x: x*2)
        assert (c.expression, c.params) == ("a = $_", [2])

    def test_convert_value(self):
        q = Q(a = 1)
        c = q.a.eq("a", 5)
        assert (c.expression, c.params) == ("a = $_", [5])

    def test_convert_list(self):
        q = Q(a = [1, 2, 3])
        c = q.a.in_("a", lambda x: [v*2 for v in x])
        assert (c.expression, c.params) == ("a IN ($_, $_, $_)", [2, 4, 6])

    def test_use_alias(self):
        q = Q(a = 1)
        c = q.a.eq("b.a")
        assert (c.expression, c.params) == ("b.a = $_", [1])


class TestEq:
    def test_one(self):
        c = Q.eq(a = 1)
        assert (c.expression, c.params) == ("a = $_", [1])

    def test_and(self):
        c = Q.eq(a = 1, b = 2)
        assert (c.expression, c.params) == ("(a = $_) AND (b = $_)", [1, 2])

    def test_or(self):
        c = Q.eq(None, False, a = 1, b = 2)
        assert (c.expression, c.params) == ("(a = $_) OR (b = $_)", [1, 2])

    def test_alias(self):
        c = Q.eq("t", a = 1, b = 2)
        assert (c.expression, c.params) == ("(t.a = $_) AND (t.b = $_)", [1, 2])

    def test_null(self):
        c = Q.eq(a = None)
        assert (c.expression, c.params) == ("a IS NULL", [])

    def test_true(self):
        c = Q.eq(a = True)
        assert (c.expression, c.params) == ("a", [])

    def test_false(self):
        c = Q.eq(a = False)
        assert (c.expression, c.params) == ("NOT a", [])


class TestNeq:
    def test_one(self):
        c = Q.neq(a = 1)
        assert (c.expression, c.params) == ("a != $_", [1])

    def test_and(self):
        c = Q.neq(a = 1, b = 2)
        assert (c.expression, c.params) == ("(a != $_) AND (b != $_)", [1, 2])

    def test_or(self):
        c = Q.neq(None, False, a = 1, b = 2)
        assert (c.expression, c.params) == ("(a != $_) OR (b != $_)", [1, 2])

    def test_alias(self):
        c = Q.neq("t", a = 1, b = 2)
        assert (c.expression, c.params) == ("(t.a != $_) AND (t.b != $_)", [1, 2])

    def test_null(self):
        c = Q.neq(a = None)
        assert (c.expression, c.params) == ("a IS NOT NULL", [])

    def test_true(self):
        c = Q.neq(a = True)
        assert (c.expression, c.params) == ("NOT a", [])

    def test_false(self):
        c = Q.neq(a = False)
        assert (c.expression, c.params) == ("a", [])


class TestIn:
    def test_one(self):
        c = Q.in_(a = [1, 2])
        assert (c.expression, c.params) == ("a IN ($_, $_)", [1, 2])

    def test_and(self):
        c = Q.in_(a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(a IN ($_, $_)) AND (b IN ($_, $_))", [1, 2, 3, 4])

    def test_or(self):
        c = Q.in_(None, False, a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(a IN ($_, $_)) OR (b IN ($_, $_))", [1, 2, 3, 4])

    def test_alias(self):
        c = Q.in_("t", a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(t.a IN ($_, $_)) AND (t.b IN ($_, $_))", [1, 2, 3, 4])

    def test_empty(self):
        c = Q.in_(a = [])
        assert (c.expression, c.params) == ("1 = 0", [])


class TestNotIn:
    def test_one(self):
        c = Q.not_in(a = [1, 2])
        assert (c.expression, c.params) == ("a NOT IN ($_, $_)", [1, 2])

    def test_and(self):
        c = Q.not_in(a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(a NOT IN ($_, $_)) AND (b NOT IN ($_, $_))", [1, 2, 3, 4])

    def test_or(self):
        c = Q.not_in(None, False, a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(a NOT IN ($_, $_)) OR (b NOT IN ($_, $_))", [1, 2, 3, 4])

    def test_alias(self):
        c = Q.not_in("t", a = [1, 2], b = [3, 4])
        assert (c.expression, c.params) == ("(t.a NOT IN ($_, $_)) AND (t.b NOT IN ($_, $_))", [1, 2, 3, 4])

    def test_empty(self):
        c = Q.not_in(a = [])
        assert (c.expression, c.params) == ("", [])


class TestLike:
    def test_like(self):
        c = Q.like(a = "abc")
        assert (c.expression, c.params) == ("a LIKE $_", ["%abc%"])

    def test_startswith(self):
        c = Q.startswith(a = "abc")
        assert (c.expression, c.params) == ("a LIKE $_", ["abc%"])

    def test_endswith(self):
        c = Q.endswith(a = "abc")
        assert (c.expression, c.params) == ("a LIKE $_", ["%abc"])

    def test_match(self):
        c = Q.match(a = "_a%c_")
        assert (c.expression, c.params) == ("a LIKE $_", ["_a%c_"])

    def test_and(self):
        c = Q.like(a = "abc", b = "def")
        assert (c.expression, c.params) == ("(a LIKE $_) AND (b LIKE $_)", ["%abc%", "%def%"])

    def test_or(self):
        c = Q.like(None, False, a = "abc", b = "def")
        assert (c.expression, c.params) == ("(a LIKE $_) OR (b LIKE $_)", ["%abc%", "%def%"])

    def test_alias(self):
        c = Q.like("t", a = "abc", b = "def")
        assert (c.expression, c.params) == ("(t.a LIKE $_) AND (t.b LIKE $_)", ["%abc%", "%def%"])

    def test_escape(self):
        c = Q.like(a = r"_\a%\_%")
        assert (c.expression, c.params) == ("a LIKE $_", [r"%\_\\\\a\%\\\\\_\%%"])


class TestCompare:
    def test_lt(self):
        c = Q.lt(a = 1)
        assert (c.expression, c.params) == ("a < $_", [1])

    def test_le(self):
        c = Q.le(a = 1)
        assert (c.expression, c.params) == ("a <= $_", [1])

    def test_gt(self):
        c = Q.gt(a = 1)
        assert (c.expression, c.params) == ("a > $_", [1])

    def test_ge(self):
        c = Q.ge(a = 1)
        assert (c.expression, c.params) == ("a >= $_", [1])

    def test_and(self):
        c = Q.lt(a = 1, b = 2)
        assert (c.expression, c.params) == ("(a < $_) AND (b < $_)", [1, 2])

    def test_or(self):
        c = Q.lt(None, False, a = 1, b = 2)
        assert (c.expression, c.params) == ("(a < $_) OR (b < $_)", [1, 2])

    def test_alias(self):
        c = Q.lt("t", a = 1, b = 2)
        assert (c.expression, c.params) == ("(t.a < $_) AND (t.b < $_)", [1, 2])


class TestEscapeLike:
    def test_escape(self):
        r = escape_like(r"a\b_\%_c%\\")
        assert r == r"a\\\\b\_\\\\\%\_c\%\\\\\\\\"


class TestWhere:
    def test_where(self):
        c, p = where(Q.of("a = $_ AND b = $_", 1, 2))
        assert (c, p) == ("WHERE a = $_ AND b = $_", [1, 2])

    def test_empty(self):
        c, p = where(Q.of("", 1, 2))
        assert (c, p) == ("", [])