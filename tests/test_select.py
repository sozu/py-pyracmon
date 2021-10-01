import pytest
from pyracmon.model import Table, Column, define_model
from pyracmon.select import *


table1 = Table("t1", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, False, None, None, False),
    Column("c3", int, None, False, None, None, True),
])

table2 = Table("t2", [
    Column("c1", int, None, True, None, "seq", False),
    Column("c2", int, None, True, None, None, True),
    Column("c3", int, None, False, None, None, False),
])

model1 = define_model(table1, [SelectMixin])
model2 = define_model(table2, [SelectMixin])


class TestSelection:
    def test_name_table(self):
        s = Selection(model1, "", table1.columns)
        assert s.name == "t1"

    def test_name_alias(self):
        s = Selection(model1, "a", table1.columns)
        assert s.name == "a"

    def test_repr_table(self):
        s = Selection(model1, "", [table1.columns[0], table1.columns[2]])
        assert str(s) == "c1, c3"

    def test_repr_alias(self):
        s = Selection(model1, "a", [table1.columns[0], table1.columns[2]])
        assert str(s) == "a.c1, a.c3"

    def test_add(self):
        s1 = Selection(model1, "a", [table1.columns[0], table1.columns[2]])
        s2 = Selection(model2, "", table2.columns)
        ex = s1 + s2
        assert isinstance(ex, FieldExpressions)
        assert list(ex) == [s1, s2]

    def test_iter(self):
        s = Selection(model1, "a", table1.columns)
        assert list(iter(s)) == [s]

    def test_consume(self):
        s = Selection(model1, "", [table1.columns[0], table1.columns[2]])
        t = s.consume([1, 2, 3])
        assert (t.c1, t.c3) == (1, 2)
        assert not hasattr(t, "c2")


class TestSelect:
    def test_all_columns(self):
        s = model1.select()
        assert str(s) == "c1, c2, c3"
        v = s.consume([1, 2, 3])
        assert isinstance(v, model1)
        assert (v.c1, v.c2, v.c3) == (1, 2, 3)

    def test_alias(self):
        s = model1.select("t")
        assert str(s) == "t.c1, t.c2, t.c3"
        v = s.consume([1, 2, 3])
        assert isinstance(v, model1)
        assert (v.c1, v.c2, v.c3) == (1, 2, 3)

    def test_includes(self):
        s = model1.select("t", ["c1", "c3"])
        assert str(s) == "t.c1, t.c3"
        v = s.consume([1, 3])
        assert isinstance(v, model1)
        assert (v.c1, v.c3) == (1, 3)

    def test_excludes(self):
        s = model1.select("t", excludes = ["c2"])
        assert str(s) == "t.c1, t.c3"
        v = s.consume([1, 3])
        assert isinstance(v, model1)
        assert (v.c1, v.c3) == (1, 3)

    def test_includes_excludes(self):
        s = model1.select("t", ["c1", "c2"], ["c2"])
        assert str(s) == "t.c1"
        v = s.consume([1, 3])
        assert isinstance(v, model1)
        assert (v.c1,) == (1,)


class TestFieldExpressions:
    def test_add_selection(self):
        exp = FieldExpressions()

        a1 = model1.select("a1")
        a2 = model1.select("a2", ["c2"])
        a3 = model1.select()

        exp += a1; exp += a2; exp += a3

        assert (exp.a1, exp.a2, exp.t1) == (a1, a2, a3)
        assert list(exp) == [a1, a2, a3]
        assert str(exp) == "a1.c1, a1.c2, a1.c3, a2.c2, c1, c2, c3"

    def test_add_expressions(self):
        exp1 = FieldExpressions()
        exp2 = FieldExpressions()

        a1 = model1.select("a1")
        a2 = model1.select("a2", ["c2"])
        a3 = model1.select()

        exp1 += a1; exp1 += a2; exp1 += a3

        b1 = model1.select("b1", ["c1"])
        b2 = model1.select("b2", ["c3"])
        b3 = model1.select(excludes=["c1"])

        exp2 += b1; exp2 += b2; exp2 += b3

        exp = exp1 + exp2

        assert (exp.a1, exp.a2, exp.b1, exp.b2, exp.t1) == (a1, a2, b1, b2, b3)
        assert list(exp) == [a1, a2, a3, b1, b2, b3]
        assert str(exp) == "a1.c1, a1.c2, a1.c3, a2.c2, c1, c2, c3, b1.c1, b2.c3, c2, c3"

    def test_add_key(self):
        a1 = model1.select("a1")
        a2 = model1.select("a2", ["c2"])
        a3 = model1.select()

        exp = a1 + a2 + a3

        exp += "b"; exp += "a2"

        assert (exp.a1, exp.a2, exp.t1, exp.b) == (a1, "a2", a3, "b")
        assert list(exp) == [a1, a2, a3, "b", "a2"]
        assert str(exp) == "a1.c1, a1.c2, a1.c3, a2.c2, c1, c2, c3, b, a2"
        assert str(exp(b="b1.c1", a2="d2")) == "a1.c1, a1.c2, a1.c3, a2.c2, c1, c2, c3, b1.c1, d2"

    def test_add_empty(self):
        a1 = model1.select("a1")
        a2 = model1.select("a2", ["c2"])
        a3 = model1.select()

        exp = a1 + a2 + () + a3 + ()

        assert (exp.a1, exp.a2, exp.t1) == (a1, a2, a3)
        assert list(exp) == [a1, a2, (), a3, ()]
        with pytest.raises(IndexError):
            str(exp)
        assert str(exp("b1.c1", "d2")) == "a1.c1, a1.c2, a1.c3, a2.c2, b1.c1, c1, c2, c3, d2"

    def test_mixture(self):
        exp = model1.select("a1", ["c1", "c2"]) + "b" + () + model1.select("a2", ["c1", "c2", "c3"]) + "d"

        assert all([hasattr(exp, k) for k in ["a1", "b", "a2", "d"]])
        assert len(list(exp)) == 5
        assert str(exp("c", b="b1.c1", d="d2")) == "a1.c1, a1.c2, b1.c1, c, a2.c1, a2.c2, a2.c3, d2"


class TestRowValues:
    def test_attributes(self):
        a1 = model1.select("a1", ["c1", "c2"])
        a2 = model2.select("a2", ["c1", "c2", "c3"])

        rv = RowValues([a1, (), "b", a2, "d", ()])

        rv.append(model1(c1 = 1))
        rv.append(2)
        rv.append(3)
        rv.append(model2(c1 = 4))
        rv.append(5)
        rv.append(6)

        assert len(rv) == 6
        assert isinstance(rv.a1, model1) and (rv[0] is rv.a1) and rv.a1.c1 == 1
        assert rv[1] == 2
        assert (rv[2] is rv.b) and rv.b == 3
        assert isinstance(rv.a2, model2) and (rv[3] is rv.a2) and rv.a2.c1 == 4
        assert (rv[4] is rv.d) and rv.d == 5
        assert rv[5] == 6


class TestReadRow:
    def _values(self):
        selections = [
            model1.select("a1", ["c1", "c2"]),
            (), "b",
            model2.select("a2", ["c1", "c2", "c3"]),
            "d", (),
        ]

        row = [
            1, 2,
            3, 4,
            5, 6, 7,
            8, 9,
        ]

        return row, selections

    def test_read_row(self):
        row, selections = self._values()

        rv = read_row(row, *selections)

        assert isinstance(rv, RowValues)
        assert len(rv) == 6
        assert isinstance(rv.a1, model1) and (rv[0] is rv.a1) and rv.a1.c1 == 1 and rv.a1.c2 == 2
        assert rv[1] == 3
        assert (rv[2] is rv.b) and rv.b == 4
        assert isinstance(rv.a2, model2) and (rv[3] is rv.a2) and rv.a2.c1 == 5 and rv.a2.c2 == 6 and rv.a2.c3 == 7
        assert (rv[4] is rv.d) and rv.d == 8
        assert rv[5] == 9

    def test_deny_redundancy(self):
        row, selections = self._values()
        row.append(10)

        with pytest.raises(ValueError):
            rv = read_row(row, *selections)

    def test_allow_redundancy(self):
        row, selections = self._values()
        row.append(10)

        rv = read_row(row, *selections, allow_redundancy=True)

        assert isinstance(rv, RowValues)
        assert len(rv) == 6
        assert isinstance(rv.a1, model1) and (rv[0] is rv.a1) and rv.a1.c1 == 1 and rv.a1.c2 == 2
        assert rv[1] == 3
        assert (rv[2] is rv.b) and rv.b == 4
        assert isinstance(rv.a2, model2) and (rv[3] is rv.a2) and rv.a2.c1 == 5 and rv.a2.c2 == 6 and rv.a2.c3 == 7
        assert (rv[4] is rv.d) and rv.d == 8
        assert rv[5] == 9
