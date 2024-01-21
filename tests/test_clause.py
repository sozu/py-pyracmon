import pytest
from pyracmon.model import Column
from pyracmon.query import Expression
from pyracmon.clause import *


class TestOrderBy:
    def test_single(self):
        r = order_by(dict(a = True))
        assert r == "ORDER BY a ASC"

    def test_multiple(self):
        r = order_by(dict(a = True, b = False))
        assert r == "ORDER BY a ASC, b DESC"

    def test_empty(self):
        r = order_by({})
        assert r == ""

    def test_defaults(self):
        r = order_by(dict(a=True, b=False), a=False, c=True)
        assert r == "ORDER BY a ASC, b DESC, c ASC"

    def test_nulls(self):
        r = order_by(dict(a = (True, False)))
        assert r == "ORDER BY a ASC NULLS LAST"

    def test_string(self):
        r = order_by(dict(a = "RANDOM()"))
        assert r == "ORDER BY a RANDOM()"

    def test_aliased(self):
        ac1 = AliasedColumn("t1", Column("c1", int, None, True, None, "seq", False))
        ac2 = AliasedColumn("t2", "c2")
        r = order_by({ac1: True, ac2: False})
        assert r == "ORDER BY t1.c1 ASC, t2.c2 DESC"


class TestRangedBy:
    def test_limit_offset(self):
        c, p = ranged_by(10, 5)
        assert (c, p) == ("LIMIT $_ OFFSET $_", [10, 5])

    def test_limit(self):
        c, p = ranged_by(10)
        assert (c, p) == ("LIMIT $_", [10])

    def test_offset(self):
        c, p = ranged_by(None, 5)
        assert (c, p) == ("OFFSET $_", [5])


class TestHolders:
    def test_by_length(self):
        assert holders(3) == "${_}, ${_}, ${_}"

    def test_by_keys(self):
        assert holders(["a", 1, None]) == "${a}, ${_1}, ${_}"

    def test_qualifier(self):
        assert holders(3, {1: lambda h: f"__{h}__"}) == "${_}, __${_}__, ${_}"

    def test_expression(self):
        assert holders(["a", Expression("now()", []), Expression("$_ + 1", [3])]) == "${a}, now(), $_ + 1"


class TestValues:
    def test_by_length(self):
        assert values(3, 2) == "(${_}, ${_}, ${_}), (${_}, ${_}, ${_})"

    def test_by_key_gens(self):
        assert values([lambda i:f"a{i}", lambda i:i, lambda i:None], 2) == "(${a0}, ${_0}, ${_}), (${a1}, ${_1}, ${_})"

    def test_qualifier(self):
        assert values(3, 2, {1: lambda h: f"__{h}__"}) == "(${_}, __${_}__, ${_}), (${_}, __${_}__, ${_})"

    def test_expression(self):
        assert values([lambda i:i, lambda i:Expression("now()", []), lambda i:Expression(f"$_ + {i}", [i])], 2) \
            == "(${_0}, now(), $_ + 0), (${_1}, now(), $_ + 1)"